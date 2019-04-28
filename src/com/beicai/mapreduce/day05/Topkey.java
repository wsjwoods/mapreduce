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
 * top10 统计前10名 
 * wordcount   单词 数量     倒序输出
 * @author Administrator
 *
 */
public class Topkey extends ToolRunner implements Tool{
     static class MyMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable>{
        private TreeSet<Long> treeSet = new TreeSet<>();
        public static final int KEY = 10;
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    String line = value.toString();
		    if(line == null){
		    	return ;
		    }
			long tmpValue = Long.valueOf(line);
			treeSet.add(tmpValue);
			if(KEY < treeSet.size()){
				treeSet.remove(treeSet.first());
			}
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (Long l : treeSet) {
				context.write(new LongWritable(l),NullWritable.get());
			}
			
		}
     }
     
     static class MyReducer extends Reducer<LongWritable,NullWritable, IntWritable,LongWritable>{
		private TreeSet<Long> top = new TreeSet<>();
    	private static final int KEY = 10;
    	
		@Override
		protected void reduce(LongWritable key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			top.add(key.get());
			if(KEY < top.size()){
				top.remove(top.first());
			}
			
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int i = 10;
			for (Long l : top) {
				context.write(new IntWritable(i--), new LongWritable(l));
			}
			
		}
		
    	 
     }
     
     public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
     	int status = ToolRunner.run(conf, new Topkey(), arguments);
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
		Job job = this.parseInputAndOutput(this, this.getConf(), args);
		   	
     	//运行
 
     	job.setMapperClass(MyMapper.class);
     	job.setReducerClass(MyReducer.class);
     	
     	//输入输出类型
     	FileInputFormat.addInputPath(job, new Path(args[0]));
     	FileOutputFormat.setOutputPath(job, new Path(args[1]));
     	
     	//输出结果的key和value的类型
 
     	job.setMapOutputKeyClass(LongWritable.class);
     	job.setMapOutputValueClass(NullWritable.class);
    	job.setOutputKeyClass(IntWritable.class);
     	job.setOutputValueClass(LongWritable.class);
     	
     	
     	//提交任务
     	boolean isSuccess = job.waitForCompletion(true);
		return isSuccess? 0 : 1;
	}
	
	public Job parseInputAndOutput(Tool tool,Configuration conf,String[] args) throws Exception{
		Job job = Job.getInstance(conf,this.getClass().getName());
		job.setJarByClass(this.getClass());
		
		return job;
		
	}
}


