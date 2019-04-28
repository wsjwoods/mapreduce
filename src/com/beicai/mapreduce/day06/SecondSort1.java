package com.beicai.mapreduce.day06;


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
 *  3	2    排序后     1	5
 *  3	1		1	6
 *  1	5		2	2
 *  2	5		2	5
 *  2	2		3	1
 *  1	6		3	2
 *  3	3		3	3
 *  
 *  第一种  实现默认排序（ value值按先后顺序输出   不能实现想要效果 ）
 * @author Administrator
 *
 */
public class SecondSort1 extends ToolRunner implements Tool{
     static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    StringTokenizer st = new StringTokenizer(value.toString(),"\t");
		    while (st.hasMoreElements()) {
				IntWritable k2 = new IntWritable(Integer.parseInt(st.nextToken()));
				IntWritable v2 = new IntWritable(Integer.parseInt(st.nextToken()));
				context.write(k2, v2);
			}
		    
		}
     }
     
     static class MyReducer extends Reducer<IntWritable,IntWritable, IntWritable, IntWritable>{
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			for (IntWritable value : values) {
				context.write(key, value);
			}
		}
     }
     
     public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
     	int status = ToolRunner.run(conf, new SecondSort1(), arguments);
     	System.exit(status);
	}

	@Override
	public Configuration getConf() {
		
		Configuration conf = new Configuration();
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		
		
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = parseInputAndOutput(this, getConf(), args);

     	//运行
     	job.setMapperClass(MyMapper.class);
     	job.setReducerClass(MyReducer.class);
     	
     	
     	//输出结果的key和value的类型
     	job.setMapOutputKeyClass(Text.class);
     	job.setMapOutputValueClass(Text.class);
    	job.setOutputKeyClass(Text.class);
     	job.setOutputValueClass(IntWritable.class);
     	   	
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


