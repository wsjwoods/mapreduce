package com.beicai.mapreduce.note;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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


public class NodeMapReduce extends Configured implements Tool{
     static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    StringTokenizer st = new StringTokenizer(value.toString(), "\t");
		    if(st.hasMoreElements()){
		    	String k = st.nextToken();
		    	st.nextToken();st.nextToken();st.nextToken();
		    	String time = StringUtils.getTime(st.nextToken());
		    	String v = st.nextToken();
		    	context.write(new Text(k + "," + time), new Text(v));
		    }
		}
     }
     
     static class MyReducer extends Reducer<Text,Text, Text, NullWritable>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for (Text value : values) {
				sb.append(value.toString());
			}
			String k = StringUtils.getStr(sb.toString());
			if(k.length()>=1)
				context.write(new Text(key.toString() + "," + k), NullWritable.get());
		}
     }
     
     public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
     	int status = ToolRunner.run(conf, new NodeMapReduce(), arguments);
     	System.exit(status);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = parseInputAndOutput(this, getConf(), args);

     	//运行
     	job.setMapperClass(MyMapper.class);
     	job.setReducerClass(MyReducer.class);
     	
     	job.setMapOutputKeyClass(Text.class);
     	job.setMapOutputValueClass(Text.class);
    	job.setOutputKeyClass(Text.class);
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


