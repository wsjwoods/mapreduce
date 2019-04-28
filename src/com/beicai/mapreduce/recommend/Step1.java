package com.beicai.mapreduce.recommend;


import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 按用户分组，计算所有物品出现的组合列表，得到用户对物品的评分矩阵
 * @author Administrator
 *
 */
public class Step1 {
     static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
    	 private IntWritable k = new IntWritable();
    	 private Text v = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String linevalue = value.toString();
			StringTokenizer st = new StringTokenizer(linevalue);
			if (st.hasMoreElements()) {
				k.set(Integer.parseInt(st.nextToken()));
				v.set(st.nextToken() + ":" + st.nextToken());
				context.write(k, v);
			}
		}
     }
     
     static class MyReducer extends Reducer<IntWritable, Text, IntWritable,Text>{
    	 private Text v = new Text();
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for (Text value : values) {
				sb.append("," + value);
			}
			v.set(sb.toString().replaceFirst(",", ""));
			context.write(key , v);
		}
    	 
    	 
     }
     
   

	
	public Job getJob(Configuration conf) throws Exception{

     	String input = "hdfs://hadoop01:8020/mr/input/recommend/items.txt";
     	String output = "hdfs://hadoop01:8020/mr/input/recommend/step1";
	
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


