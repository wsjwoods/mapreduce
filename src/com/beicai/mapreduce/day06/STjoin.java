package com.beicai.mapreduce.day06;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * 单表联查 目的：对原始数据所包含的信息进行挖掘 -- 数据挖掘
 * 
 * 
 * @author Administrator
 *
 */
public class STjoin extends ToolRunner implements Tool {
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String childname = "";
			String parentname = "";
			String relationtype = "";//标识符  1 2
			String lineValue = value.toString();
            
			if(lineValue.contains("child")){
				return ;
			}		
			
			StringTokenizer st = new StringTokenizer(lineValue);
			if (st.hasMoreElements()) {
				childname = st.nextToken();
				parentname = st.nextToken();
			}
			/**
			 * 1: key:父母 value:这个家族 
			 * 2: key:孩子 value:这个家族
			 */
			// 输出左表
			relationtype = "1";
			String str1 = relationtype + "+" + childname + "+" + parentname;
			context.write(new Text(parentname), new Text(str1));

			// 输出右表
			relationtype = "2";
			String str2 = relationtype + "+" + childname + "+" + parentname;
			context.write(new Text(childname), new Text(str2));

		}
	}

	static class MyReducer extends Reducer<Text, Text, Text, Text> {
		//为了在输出时只输出一次的列标
		private static int time = 0;

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if (time == 0) {
				context.write(new Text("grandchild"), new Text("grandparent"));
				time++;
			}
			int grandchildNum = 0;
			String[] grandchild = new String[10];
			int grandparentNum = 0;
			String[] grandparent = new String[10];
			for (Text value : values) {
				String record = value.toString();
				int len = record.length();
				int i = 2;
				if (len == 0) {
					continue;
				}
				// 取得左右表标识
				char relationtype = record.charAt(0);
				// 获取values list中value的child
				String childname = "";
				String parentname = "";
				while (record.charAt(i) != '+') {
					childname += record.charAt(i++);
				}
				i++;
				while (len > i) {
					parentname += record.charAt(i++);
				}
				// 左表 取出child放入grandchild
				if (relationtype == '1') {
					grandchild[grandchildNum++] = childname;
				}
				// 右表 取出parent放入grandparent
				if (relationtype == '2') {
					grandparent[grandparentNum++] = parentname;
				}
				// grandchild和grandparent数组求笛卡尔积
				if (grandchildNum != 0 && grandparentNum != 0) {
					for (int j = 0; j < grandparentNum; j++) {
						for (int k = 0; k < grandchildNum; k++) {
							context.write(new Text(grandchild[k]), new Text(grandparent[j]));
						}
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {		
		// 获取配置信息
		Configuration conf = new Configuration();
		String[] arguments = new GenericOptionsParser(conf, args).getRemainingArgs();
		int status = ToolRunner.run(conf, new STjoin(), arguments);
		System.exit(status);
	}

	@Override
	public Configuration getConf() {
		Configuration conf = new Configuration();
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {

	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = parseInputAndOutput(this, getConf(), args);

		// 运行
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		// 输出结果的key和value的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 提交任务
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}

	public Job parseInputAndOutput(Tool tool, Configuration conf, String[] args) throws Exception {
		Job job = Job.getInstance(conf, this.getClass().getName());
		job.setJarByClass(this.getClass());

		// 输入输出类型
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setNumReduceTasks(0);
		return job;
	}
}
