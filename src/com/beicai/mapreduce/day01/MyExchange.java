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


public class MyExchange {
    /**
     * 数据源
     * abc bbc aab ccb
     * 
     */
	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		static int y=0;
		private Text word = new Text();
		final static IntWritable one = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString(); 
			String[] str = lineValue.split(" ");
			for(int i = 0;i<str.length;i++){
				word.set(str[i]);
				context.write(word, one);
			}	
		}
	}
	
	static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			Text charword = new Text();
			IntWritable sumW = null;
			String tmp = key.toString();
			char ch = myReg(tmp);
			if(ch != '0'){
				for(IntWritable value:values){
					sum += value.get();
				}
				charword.set(ch+"");
				sumW = new IntWritable(sum);
				context.write(charword, sumW);
			}
		
			
		}
		
		
		//正则判断返回合规的字符
				public char myReg(String str){
				    
					for(int i=0;i<str.length()-1;i++){
						if(str.charAt(i)==str.charAt(i+1)){
							return str.charAt(i);
						}
					}	
					return '0';
				}	
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length!=2){
			System.out.println("参数数量不正确 <input> <output>");
			System.exit(0);
		}
    	
    	//获取配置信息
    	Configuration conf = new Configuration();
    	Job job = new Job(conf, "myExchange");
    	//运行
    	job.setJarByClass(MyExchange.class);
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
