package com.beicai.mapreduce.recommend;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beicai.mapreduce.recommend.Step3.MyMapper;

/**
 * 计算推荐结果列表
 * 
 * @author Administrator
 *
 */
public class Step4 {
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();
		private String filename;// 读取的文件名称

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			filename = split.getPath().getParent().getName();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(value.toString());
			if (st.hasMoreElements()) {
				if (filename.equals("step2")) {// 同现矩阵
					String[] v1 = st.nextToken().split(":");
					String item1 = v1[0];//电影名1
					String item2 = v1[1];//电影名2
					String num = st.nextToken();//同现次数
					k.set(item1);
					v.set("A:" + item2 + "," + num);
					context.write(k, v);
				} else if (filename.equals("step3")) {// 评分矩阵
					String item = st.nextToken();//电影名
					String info = st.nextToken();
					String user = info.split(":")[0];//用户id
					String pref = info.split(":")[1];//评分
					k.set(item);
					v.set("B:" + user + "," + pref);
					context.write(k, v);
				}

			}
		}
	}

	static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// mapA 同现矩阵 <item , num>
			Map<String, String> mapA = new HashMap<String, String>();
			// mapB 评分矩阵 <userID , pref>
			Map<String, String> mapB = new HashMap<String, String>();
			for (Text value : values) {
				String src = value.toString();
				if (src.startsWith("A:")) {
					String[] info = src.substring(2).split(",");
					mapA.put(info[0], info[1]);
				} else if (src.startsWith("B:")) {
					String[] info = src.substring(2).split(",");
					mapB.put(info[0], info[1]);
				}
			}
			float result = 0;
			Iterator<String> iter = mapA.keySet().iterator();
			while (iter.hasNext()) {
				String item = iter.next();// itemID
				int num = Integer.parseInt(mapA.get(item));
				Iterator<String> iter2 = mapB.keySet().iterator();
				while (iter2.hasNext()) {
					String user = iter2.next();
					float pref = Float.parseFloat(mapB.get(user));
					result = num * pref; // 矩阵乘法计算
					k.set(user);
					v.set(item + "," + result);
					context.write(k, v);
				}
			}
		}

	}

	public Job getJob(Configuration conf) throws Exception {
		String input1 = "hdfs://hadoop01:8020/mr/input/recommend/step2";
		String input2 = "hdfs://hadoop01:8020/mr/input/recommend/step3";
		String output = "hdfs://hadoop01:8020/mr/input/recommend/step4";

		Job job = Job.getInstance(conf, this.getClass().getName());
		job.setJarByClass(this.getClass());
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 输入输出类型
		FileInputFormat.addInputPath(job, new Path(input1));
		FileInputFormat.addInputPath(job, new Path(input2));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job;
	}

}
