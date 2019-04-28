package com.beicai.mapreduce.day07;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WCOldMapReduce extends Configured implements Tool{

	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
		private Text outputKey = new Text();
		private IntWritable one = new IntWritable(1);
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String lineValue = value.toString();
			StringTokenizer st = new StringTokenizer(lineValue);
			while (st.hasMoreElements()) {
				String val = st.nextToken();
				outputKey.set(val);
				output.collect(outputKey, one);
			}
			
		}
          		
	}
	
	public static class MyReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable outputValue = new IntWritable();
		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			int sum = 0;
			while(values.hasNext()){
				sum += values.next().get();
			}
			outputValue.set(sum);
			output.collect(key, outputValue);
		}
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		JobConf job = new JobConf(conf);
		job.setJobName(this.getClass().getName());
		job.setJarByClass(this.getClass());
		
		Path inpath = new Path(args[0]);
		Path outpath = new Path(args[1]);
		FileInputFormat.addInputPath(job, inpath);
		FileOutputFormat.setOutputPath(job, outpath);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		RunningJob rj = JobClient.runJob(job);
		
		
		return rj.getJobState();
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int status = ToolRunner.run(conf,new WCOldMapReduce(),args);
		System.exit(status);
		
	}
	
	
	
	
}
