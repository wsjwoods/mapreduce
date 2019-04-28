package com.beicai.mapreduce.day02;

import java.io.IOException;
import java.util.Iterator;
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
 *  小明 语文 92
	小明 数学 91
	小明 英语 70
	小强 语文 89
	小强 数学 66
	小强 英语 87
	小红 语文 93
	小红 数学 85
	小红 英语 95
	
	求每人平均分
 * @author Administrator
 *
 */
public class AvgScore2 extends ToolRunner implements Tool{
	static class MyMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
        private Text word = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    String lineValue = value.toString();
		    StringTokenizer st = new StringTokenizer(lineValue);
		    if (st.hasMoreElements()) {
				String name = st.nextToken();
				String subject = st.nextToken();
				String score = st.nextToken();
				
				context.write(new Text(name), new FloatWritable(Float.parseFloat(score)));
			}
		}
     }
     
     static class MyReducer extends Reducer<Text,FloatWritable, Text, FloatWritable>{
		@Override
		protected void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			Iterator<FloatWritable> it = values.iterator();
			float sum = 0;
			int num = 0;
			while(it.hasNext()){
				sum += it.next().get();
				num++;
			}
			context.write(key, new FloatWritable((float)(sum/num)));		
		}    	 
     }
	
     public static void main(String[] args) throws Exception {  	
      	Configuration conf = new Configuration();
      	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
      	int status = ToolRunner.run(conf, new AvgScore2(), arguments);
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
      	job.setJarByClass(AvgScore2.class);
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
