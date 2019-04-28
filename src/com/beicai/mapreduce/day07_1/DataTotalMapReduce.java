package com.beicai.mapreduce.day07_1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DataTotalMapReduce extends Configured implements Tool{
   /**
    * Mapper��
    * 
    */
	public static class DataTotalMapper extends Mapper<LongWritable, Text,Text,DataWritable>{
		private Text mapOutputKey = new Text();
		private DataWritable mapOutputValue = new DataWritable();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String lineValue = value.toString();
			String[] strs = lineValue.split("\t");
			//validate
			String phoneNum = strs[1];
			int upPackNum = Integer.valueOf(strs[6]);
			int downPackNum = Integer.valueOf(strs[7]);
			int upPayLoad = Integer.valueOf(strs[8]);
			int downPayLoad = Integer.valueOf(strs[9]);
			mapOutputKey.set(phoneNum);
			mapOutputValue.set(downPackNum, upPackNum, downPayLoad, upPayLoad);
			context.write(mapOutputKey, mapOutputValue);
		}
	}
	
	/**
	 * Reducer��
	 */
	public static class DataTotalReducer extends Reducer<Text, DataWritable, Text, DataWritable>{
       private DataWritable outputValue = new DataWritable();

		@Override
		protected void reduce(Text key, Iterable<DataWritable> values, Context context) throws IOException, InterruptedException {
			int upPackNum =0;
			int downPackNum = 0;
			int upPayLoad = 0;
			int downPayLoad = 0;
			for(DataWritable value :values){
				upPackNum+=value.getUpPackNum();
				downPackNum+=value.getDownPackNum();
				upPayLoad+=value.getUpPayLoad();
				downPayLoad+=value.getDownPayLoad();
			}
			outputValue.set(downPackNum, upPackNum, downPayLoad, upPayLoad);
			context.write(key, outputValue);
		}
	}

       public Job pareseInputAndOutput(Tool tool,Configuration conf,String[] args) throws Exception{
    	 if(args.length!=2){
    		 System.err.println("Usage"+tool.getClass().getSimpleName()+"is probrelm!");
    		 return null;
    	 }
    	   
		Job job = Job.getInstance(conf, this.getClass().getName());
		job.setJarByClass(tool.getClass());
		Path path = new Path(args[0]);
		FileInputFormat.addInputPath(job, path);
		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		
		return job;
       }
	
	/**
	 * Driver��
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = this.pareseInputAndOutput(this,conf,args);
		job.setMapperClass(DataTotalMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataWritable.class);
		
		job.setReducerClass(DataTotalReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataWritable.class);
		
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess?0:1;
	}
	
	  public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 int status = ToolRunner.run(conf,new DataTotalMapReduce(), args);
		 System.exit(status);
	}
}
