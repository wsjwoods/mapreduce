package com.beicai.mapreduce.recommend;


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beicai.mapreduce.recommend.Step1.MyMapper;
import com.beicai.mapreduce.recommend.Step1.MyReducer;

/**
 * 对物品组合列表进行计数，建立物品的同现矩阵
 * 对Step1的结果进行处理 （inputPath = "hdfs://hadoop01:8020/mr/input/recommend/step1"）
 * 
 * @author Administrator
 *
 */
public class Step2{
     static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    	 private Text k = new Text();
    	 private static final IntWritable one = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    StringTokenizer st1 = new StringTokenizer(value.toString(), "\t");
		    if (st1.hasMoreElements()) {
		    	st1.nextToken();
		    	String[] tokens = st1.nextToken().split(",");
		    	for (int i = 0; i < tokens.length; i++) {
					for (int j = 0; j < tokens.length; j++) {
						k.set(tokens[i].split(":")[0] + ":" + tokens[j].split(":")[0]);
						context.write(k, one);
					}
				}
		    }
		}
     }
     
     static class MyReducer extends Reducer<Text,IntWritable, Text, IntWritable>{
    	 private IntWritable v = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			v.set(sum);
			context.write(key, v);
		}
     }
     
     public Job getJob(Configuration conf) throws Exception{
      	String input = "hdfs://hadoop01:8020/mr/input/recommend/step1";
      	String output = "hdfs://hadoop01:8020/mr/input/recommend/step2";
 	
 		Job job = Job.getInstance(conf,this.getClass().getName());
 		job.setJarByClass(this.getClass());
 		job.setMapperClass(MyMapper.class);
      	job.setReducerClass(MyReducer.class);
      
      	job.setMapOutputKeyClass(Text.class);
     	job.setMapOutputValueClass(IntWritable.class);
    	job.setOutputKeyClass(Text.class);
     	job.setOutputValueClass(IntWritable.class);
     	   	
 		//输入输出类型
      	FileInputFormat.addInputPath(job, new Path(input));
      	FileOutputFormat.setOutputPath(job, new Path(output));
      	
 		return job;
 	}
	
	
}


