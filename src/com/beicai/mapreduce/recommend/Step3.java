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

import com.beicai.mapreduce.recommend.Step2.MyMapper;
import com.beicai.mapreduce.recommend.Step2.MyReducer;

/**
 * 合并同现矩阵和评分矩阵
 * @author Administrator
 *
 */
public class Step3{
     static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
    	 private Text k = new Text();
    	 private Text v = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    StringTokenizer st1 = new StringTokenizer(value.toString());
		    if(st1.hasMoreElements()){
		    	String id = st1.nextToken();
		    	String items = st1.nextToken();
		    	StringTokenizer st2 = new StringTokenizer(items.toString(),",");
		    	while(st2.hasMoreElements()){
		    		String info = st2.nextToken();
		    		k.set(info.split(":")[0]);
		    		v.set(id + ":" + info.split(":")[1]);
		    		context.write(k, v);
		    	}
		    }
		}
     }
     
     public Job getJob(Configuration conf) throws Exception{
       	String input = "hdfs://hadoop01:8020/mr/input/recommend/step1";
       	String output = "hdfs://hadoop01:8020/mr/input/recommend/step3";
  	
  		Job job = Job.getInstance(conf,this.getClass().getName());
  		job.setJarByClass(this.getClass());
  		job.setMapperClass(MyMapper.class);
  		
     	job.setMapOutputKeyClass(Text.class);
     	job.setMapOutputValueClass(Text.class);
    	job.setOutputKeyClass(Text.class);
     	job.setOutputValueClass(Text.class);
      	   	
  		//输入输出类型
       	FileInputFormat.addInputPath(job, new Path(input));
       	FileOutputFormat.setOutputPath(job, new Path(output));
       	
  		return job;
  	} 

	
}


