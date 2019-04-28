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

/**
 * 2016030110
 * @author Administrator
 *
 */
public class Temperature extends ToolRunner implements Tool{
	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text word = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    String lineValue = value.toString();
		    String date = lineValue.substring(0, 8);
		    int temp = Integer.parseInt(lineValue.substring(8));
		    context.write(new Text(date), new IntWritable(temp));
		
		}
     }
     
     static class MyReducer extends Reducer<Text,IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int max = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				max = Math.max(max, value.get());
			}
			context.write(key, new IntWritable(max));
		}    	 
     }
	
     public static void main(String[] args) throws Exception {  	
      	Configuration conf = new Configuration();
      	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
      	int status = ToolRunner.run(conf, new Temperature(), arguments);
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
      	job.setJobName("mytemp");
      	//运行
      	job.setJarByClass(Temperature.class);
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
