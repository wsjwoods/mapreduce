package com.beicai.mapreduce.day05;


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
 * 
 * 使用 WCWritable  传递参数
 * @author Administrator
 *
 */
public class NewWordCount extends ToolRunner implements Tool{
     static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    String line = value.toString();
		    StringTokenizer st = new StringTokenizer(line);
		    while (st.hasMoreElements()) {
				context.write(new Text(st.nextToken()), new IntWritable(1));
				
			}
		}
		
     }
     
     static class MyReducer extends Reducer<Text, IntWritable, WCWritable,NullWritable>{
		
    	private TreeSet<WCWritable> ts = new TreeSet<>();
		public static final int KEY =10;
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum=0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			WCWritable mword = new WCWritable(key.toString(),sum);
			ts.add(mword);
			if(ts.size() > KEY)
				ts.remove(ts.first());
		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, WCWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (WCWritable mword : ts) {
				context.write(mword, NullWritable.get());
			}
		}
		
		
		
    	 
     }
     
     public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
     	int status = ToolRunner.run(conf, new NewWordCount(), arguments);
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
		
		Job job = this.parseInputAndOutput(this, this.getConf(), args);
		   	
     	//运行
 
     	job.setMapperClass(MyMapper.class);
     	job.setReducerClass(MyReducer.class);
     	
     	//输出结果的key和value的类型
 
     	job.setMapOutputKeyClass(Text.class);
     	job.setMapOutputValueClass(IntWritable.class);
    	job.setOutputKeyClass(WCWritable.class);
     	job.setOutputValueClass(NullWritable.class);
     	
     	
     	//提交任务
     	boolean isSuccess = job.waitForCompletion(true);
		return isSuccess? 0 : 1;
	}
	
	public Job parseInputAndOutput(Tool tool,Configuration conf,String[] args) throws Exception{
		Job job = Job.getInstance(conf,this.getClass().getName());
		job.setJarByClass(this.getClass());
		
     	//输入输出类型
		FileInputFormat.addInputPath(job, new Path(args[0]));
     	FileOutputFormat.setOutputPath(job, new Path(args[1]));
     	
		return job;
	}
}


