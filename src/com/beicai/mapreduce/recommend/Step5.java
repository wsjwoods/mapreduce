package com.beicai.mapreduce.recommend;


import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beicai.mapreduce.recommend.Step4.MyMapper;
import com.beicai.mapreduce.recommend.Step4.MyReducer;

/**
 * 合并结果列表
 * @author Administrator
 *
 */
public class Step5{
     static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
    	 private IntWritable k = new IntWritable();
    	 private Text v = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    StringTokenizer st = new StringTokenizer(value.toString());
		    if(st.hasMoreElements()){
		    	k.set(Integer.parseInt(st.nextToken()));
		    	v.set(st.nextToken());
		    	context.write(k, v);
		    }
		}
     }
     
     static class MyReducer extends Reducer<IntWritable,Text, IntWritable, Text>{
    	 private Text v = new Text();
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Map<String,Float> map = new HashMap<String,Float>();
			for (Text value : values) {
				String[] strs = value.toString().split(",");
				String item = strs[0];
				float score = Float.parseFloat(strs[1]);
				if(map.containsKey(item))
					map.put(item, map.get(item) + score);
				else
					map.put(item, score);
			}
			
			Iterator<String> iter = map.keySet().iterator();
			while(iter.hasNext()){
				String item = iter.next();
				float score = map.get(item);
				v.set(item + "," + score);
				context.write(key, v);
			}
		}
     }
     
  
     public Job getJob(Configuration conf) throws Exception{
     	String input = "hdfs://hadoop01:8020/mr/input/recommend/step4";
     	String output = "hdfs://hadoop01:8020/mr/input/recommend/step5";
	
		Job job = Job.getInstance(conf,this.getClass().getName());
		job.setJarByClass(this.getClass());
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
	   	job.setMapOutputKeyClass(IntWritable.class);
	   	job.setMapOutputValueClass(Text.class);
	  	job.setOutputKeyClass(IntWritable.class);
	   	job.setOutputValueClass(Text.class);
    	   	
		//输入输出类型
     	FileInputFormat.addInputPath(job, new Path(input));
     	FileOutputFormat.setOutputPath(job, new Path(output));
     	
		return job;
	} 
}


