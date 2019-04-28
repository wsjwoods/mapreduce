package com.beicai.mapreduce.day06;


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
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

/**
 *  3	2    排序后     1	5
 *  3	1		1	6
 *  1	5		2	2
 *  2	5		2	5
 *  2	2		3	1
 *  1	6		3	2
 *  3	3		3	3
 *  
 *  第二种   需要自定义可序列化对象  NewK2 
 * @author Administrator
 *
 */
public class SecondSort2 extends ToolRunner implements Tool{
     static class MyMapper extends Mapper<LongWritable, Text, NewK2, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    StringTokenizer st = new StringTokenizer(value.toString(),"\t");
		    if (st.hasMoreElements()) {
				int k2 = Integer.parseInt(st.nextToken());
				int v2 = Integer.parseInt(st.nextToken());
				NewK2 n = new NewK2(k2, v2);
				context.write(n,new IntWritable(v2));
			}
		}
     }
     
     static class MyReducer extends Reducer<NewK2,IntWritable, IntWritable, IntWritable>{
		@Override
		protected void reduce(NewK2 key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			for (IntWritable value : values) {
				context.write(new IntWritable(key.getFitst()), value);
			}
		}
     }
     
     
     
     public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
     	int status = ToolRunner.run(conf, new SecondSort2(), arguments);
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
     	job.setMapOutputKeyClass(NewK2.class);
     	job.setMapOutputValueClass(IntWritable.class);
    	job.setOutputKeyClass(IntWritable.class);
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


