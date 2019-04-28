package com.beicai.mapreduce.day01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Reducer;

public class MyUpperCase {
      static class MyMapper extends Mapper<LongWritable, Text,Text,IntWritable>{
			static int y=0;
			private Text word = new Text();
			final static IntWritable one = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
				String lineValue = value.toString(); 
				word.set(lineValue);
				context.write(word, one);
			
		}  	  
      }
      
      static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			 IntWritable result = new IntWritable();
			 Text text = new Text();
			 int sum = 0;
			 for(IntWritable value : values){
				 sum += value.get();
			 }
			 String line = key.toString();
			 String upperCase = line.toUpperCase();
			 text.set(upperCase);
			 result.set(sum);
			 context.write(text, result);
		}
    	  
      }
      
      public static void main(String[] args) throws Exception {
  		if(args.length!=2){
  			System.out.println("参数数量不正确 <input> <output>");
  			System.exit(0);
  		}
      	
      	//获取配置信息
      	Configuration conf = new Configuration();
      	Job job = new Job(conf, "myUpperCase");
      	//运行
      	job.setJarByClass(MyUpperCase.class);
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
      	System.out.println(isSuccess? 0 : 1);
      	System.exit(isSuccess? 0 : 1);
  	}
}
