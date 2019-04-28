package com.beicai.mapreduce.day03;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
 *  1 张三 20160101 8974 10768 717.92 天宫院
	2  李四 20160101 8661 10393 692.88 大臧村
	3  王五 20160101 4312 5174 344.96 大臧村
 * 
 * 每天的排行
 * @author Administrator
 *
 */
public class MySport4 extends ToolRunner implements Tool{
     static class MyMapper extends Mapper<LongWritable, Text, FloatWritable, Text>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    String line = value.toString();
		    StringTokenizer st = new StringTokenizer(line);
		    if(st.hasMoreTokens()){
		    	String id = st.nextToken();
		    	String name = st.nextToken();
		    	String date = st.nextToken().substring(4,6);
		    	float sum = (float) (Float.parseFloat(st.nextToken())*0.5+Float.parseFloat(st.nextToken())*0.85+Float.parseFloat(st.nextToken())*0.25);
		    	String location = st.nextToken();
		    	context.write(new FloatWritable(sum), new Text(id+"-"+name));
		    }
		}
     }
     
     static class MyReducer extends Reducer<FloatWritable,Text, IntWritable, Text>{
        int i=1;
    	
		@Override
		protected void reduce(FloatWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				float sum = key.get();
				context.write(new IntWritable(i++), new Text(value.toString()+" "+sum));
			}
		} 	 
     }
     
     public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
     	int status = ToolRunner.run(conf, new MySport4(), arguments);
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
     	job.setJobName("my----");
     	//运行
     	job.setJarByClass(MySport4.class);
     	job.setMapperClass(MyMapper.class);
     	job.setReducerClass(MyReducer.class);
     	
     	//输入输出类型
     	FileInputFormat.addInputPath(job, new Path(args[0]));
     	FileOutputFormat.setOutputPath(job, new Path(args[1]));
     	
     	//输出结果的key和value的类型

     	job.setMapOutputKeyClass(FloatWritable.class);
     	job.setMapOutputValueClass(Text.class);
     	job.setOutputKeyClass(IntWritable.class);
     	job.setOutputValueClass(Text.class);
     	
     	
     	//提交任务
     	boolean isSuccess = job.waitForCompletion(true);
		return isSuccess? 0 : 1;
	}
}


