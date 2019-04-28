package com.beicai.mapreduce.day02;

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


public class MyWordCount2 extends ToolRunner implements Tool{
     static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text word = new Text();
        final static IntWritable one = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    String lineValue = value.toString();
		    StringTokenizer token = new StringTokenizer(lineValue, ",");
		    while (token.hasMoreElements()) {
				String str = token.nextToken();
				word.set(str);
				context.write(word, one);
			}
		}
     }
     
     static class MyReducer extends Reducer<Text,IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
			
		}
    	 
    	 
     }
     
     public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
     	int status = ToolRunner.run(conf, new MyWordCount2(), arguments);
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
     	job.setJobName("mywordcount2");
     	//运行
     	job.setJarByClass(MyWordCount2.class);
     	job.setMapperClass(MyMapper.class);
     	job.setReducerClass(MyReducer.class);
     	
     	//输入输出类型
     	FileInputFormat.addInputPath(job, new Path(args[0]));
     	FileOutputFormat.setOutputPath(job, new Path(args[1]));
     	
     	//输出结果的key和value的类型
     	job.setOutputKeyClass(Text.class);
     	job.setMapOutputValueClass(IntWritable.class);
     	
     	//提交任务
     	boolean isSuccess = job.waitForCompletion(true);
		return isSuccess? 0 : 1;
	}
}
