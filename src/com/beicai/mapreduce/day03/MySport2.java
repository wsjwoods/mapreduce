package com.beicai.mapreduce.day03;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 1 张三 20160101 8974 10768 717.92 天宫院 2 李四 20160101 8661 10393 692.88 大臧村 3 王五
 * 20160101 4312 5174 344.96 大臧村
 * 
 * 每个人每个月共消耗多少
 * 
 * @author Administrator
 *
 */
public class MySport2 extends ToolRunner implements Tool {
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer st = new StringTokenizer(line);
			if (st.hasMoreTokens()) {
				String id = st.nextToken();
				String name = st.nextToken();
				String date = st.nextToken().substring(4, 6);
				String info = st.nextToken() + "_" + st.nextToken() + "_" + st.nextToken();
				String location = st.nextToken();
				context.write(new Text(date + "月:" + id + "-" + name), new Text(info));
			}
		}
	}

	static class MyReducer extends Reducer<Text, Text, Text, FloatWritable> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			float kcal = 0;
			for (Text value : values) {
				String info = value.toString();
				kcal += (float) (Float.parseFloat(info.split("_")[0]) * 0.5)
						+ (float) (Float.parseFloat(info.split("_")[1]) * 0.85)
						+ (float) (Float.parseFloat(info.split("_")[2]) * 0.25);
			}
			context.write(key, new FloatWritable(kcal));
		}

	}

	public static void main(String[] args) throws Exception {
		// 获取配置信息
		Configuration conf = new Configuration();
		String[] arguments = new GenericOptionsParser(conf, args).getRemainingArgs();
		int status = ToolRunner.run(conf, new MySport2(), arguments);
		System.exit(status);
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job(getConf());
		job.setJobName("my----");
		// 运行
		job.setJarByClass(MySport2.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		// 输入输出类型
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 输出结果的key和value的类型
		job.setOutputKeyClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);

		// 提交任务
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
}
