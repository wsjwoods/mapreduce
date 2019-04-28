package com.beicai.mapreduce.day02;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 赵一|95
 * 求平均值
 * 
 * 
 * @author Administrator
 *
 */
public class AvgScore extends ToolRunner implements Tool{
	static class MyMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
        private Text word = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    String lineValue = value.toString();
		    StringTokenizer st = new StringTokenizer(lineValue);
		    if (st.hasMoreElements()) {
				String name = st.nextToken();
				String score = st.nextToken();
				
				//获取读取的文件的文件名
				InputSplit inputSplit = context.getInputSplit();
				String filename=((FileSplit)inputSplit).getPath().getName();
				
				filename = filename.substring(0,filename.indexOf('.'));
				context.write(new Text(filename), new FloatWritable(Float.parseFloat(score)));
			}
		}
     }
     
     static class MyReducer extends Reducer<Text,FloatWritable, Text, FloatWritable>{
		@Override
		protected void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float sum = 0;
			int num = 0;
			for (FloatWritable value : values) {
				sum += value.get();
				num++;
			}
			context.write(key, new FloatWritable((float)(sum/num)));
				
		}    	 
     }
	
     public static void main(String[] args) throws Exception {  	
      	Configuration conf = new Configuration();
      	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
      	int status = ToolRunner.run(conf, new AvgScore(), arguments);
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
      	job.setJobName("myavgscore");
      	//运行
      	job.setJarByClass(AvgScore.class);
      	job.setMapperClass(MyMapper.class);
      	job.setReducerClass(MyReducer.class);
      	
      	//输入输出类型
      	FileInputFormat.addInputPath(job, new Path(args[0]));
      	FileOutputFormat.setOutputPath(job, new Path(args[1]));
      	
      	//输出结果的key和value的类型
      	job.setOutputKeyClass(Text.class);
      	job.setMapOutputValueClass(FloatWritable.class);
      	
      	//提交任务
      	boolean isSuccess = job.waitForCompletion(true);
 		return isSuccess? 0 : 1;
 	}
    
}
