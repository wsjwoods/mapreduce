package com.beicai.mapreduce.day01;

import java.io.IOException;

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


public class MyWordCount {
	/**
	 * 
	 * 
	 * Mapper区域  阶段
	 * @author Administrator
	 * 
	 */
    static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1); 
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
	            String lineValue = value.toString();
	            String[] str = lineValue.split(" ");
	            for(int i=0;i<str.length;i++){
	            	String wordValue = str[i];
	            	word.set(wordValue);
	            	context.write(word, one);
	            }
		}	
    }
    /**
     * 接收KEY类型    	接收value    	输出的key 	输出的value
     * KEYIN,    	VALUEIN, 	KEYOUT, 	VALUEOUT
     * @author Administrator
     *
     */
    static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
    	private int sum;
    	private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			for(IntWritable value:values){
				sum += value.get();
			}
			result.set(sum);
			String str = key.toString();
			str = removePoint(str);
			
			context.write(new Text(str), result);
		}   	
		
		//去除末尾的.或，
		public static String removePoint(String str){
	    	String ss = str;
	    	int index = str.indexOf('.');
	    	if(index!=-1){
	    		ss = str.substring(0, index);
	    	}
	    	index = str.indexOf(',');
	    	if(index!=-1){
	    		ss = str.substring(0, index);
	    	}
	    	return ss;
	    }
    }
    
    /**
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
		if(args.length!=2){
			System.out.println("参数数量不正确 wordcount<input> <output>");
			System.exit(0);
		}
    	
    	//获取配置信息
    	Configuration conf = new Configuration();
    	Job job = new Job(conf, "mywordcount");
    	//运行
    	job.setJarByClass(MyWordCount.class);
    	job.setMapperClass(MyMapper.class);
    	job.setReducerClass(MyReduce.class);
    	
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
