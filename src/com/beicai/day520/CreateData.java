package com.beicai.day520;


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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CreateData extends Configured implements Tool{
     static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
    	 public Text k = new Text();
    	 
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    String[] strs = value.toString().split("\t");
		    StringBuffer sb = new StringBuffer();
		    for (int i = 0; i < strs.length; i++) {
				if(i==0){
					strs[i] = "id:" + strs[i] + "www.beicai";
				}
				if(i==1){
					strs[i] = strs[i] + "人";
				}
				if(i==2){
					strs[i] = strs[i] + "年";
				}
				if(i==3){
					strs[i] = "学历:" + strs[i];
				}
				if(i==4){
					strs[i] = "工作:" + strs[i];
				}
				if(i==5){
					strs[i] = "工资:" + strs[i];
				}
				if(i==6){
					strs[i] = "省份:" + strs[i];
				}
				if(i==7){
					strs[i] = "城市:" + strs[i];
				}
				if(i==8){
					strs[i] = "是否是城市:" + strs[i];
				}
			}
		    for (int i = 0; i < strs.length; i++) {
				sb.append(strs[i] + "\t");
			}
		    k.set(sb.toString());
		    context.write(k, NullWritable.get());
		}
     }
     
     
     public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
     	int status = ToolRunner.run(conf, new CreateData(), arguments);
     	System.exit(status);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = parseInputAndOutput(this, getConf(), args);

     	//运行
     	job.setMapperClass(MyMapper.class);
     	
     	//输出结果的key和value的类型
     	job.setMapOutputKeyClass(Text.class);
     	job.setMapOutputValueClass(NullWritable.class);
     	   	
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


