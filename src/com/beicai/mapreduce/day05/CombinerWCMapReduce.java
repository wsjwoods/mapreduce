package com.beicai.mapreduce.day05;


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.hash.Hashing;

/**
 * 分区 合并  wordcount
 * 按照首字符的相同单词放入到一个文件中
 * a 90
 * ab 78
 * b 6
 * bad 89
 * 
 * 
 * @author Administrator
 *
 */
public class CombinerWCMapReduce extends ToolRunner implements Tool{
     
	
	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		public static Text outputKey  = new Text();
		public static final IntWritable outputValue  = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    StringTokenizer st = new StringTokenizer(value.toString());
		    while (st.hasMoreElements()) {
		    	outputKey.set(st.nextToken());
				context.write(outputKey, outputValue);
			}
		}
     }
     
     static class MyReducer extends Reducer<Text,IntWritable, Text, IntWritable>{
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			
		}

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			IntWritable outputValue = new IntWritable();
			for (IntWritable value : values) {
				sum += value.get();
			}
			outputValue.set(sum);
					
			context.write(key, outputValue);
			
		}
    	 
		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
    	 
     }
     
     public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
     	int status = ToolRunner.run(conf, new CombinerWCMapReduce(), arguments);
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
		Job job = parseInputAndOutput(this, getConf(), args);

     	//运行

     	job.setMapperClass(MyMapper.class);
     	job.setReducerClass(MyReducer.class);
   	    	
     	//输出结果的key和value的类型
     	job.setMapOutputKeyClass(Text.class);
     	job.setMapOutputValueClass(IntWritable.class);
    	job.setOutputKeyClass(Text.class);
     	job.setOutputValueClass(IntWritable.class);
     	
     	//指定reduce数量
     	job.setNumReduceTasks(4);
     	
     	//默认分区  HashPartitioner
     	//job.setPartitionerClass(HashPartitioner.class);
     	
     	//指定自定义分区
     	job.setPartitionerClass(WCPatitioner.class);
     		
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


