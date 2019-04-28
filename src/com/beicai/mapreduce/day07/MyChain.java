package com.beicai.mapreduce.day07;


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeSet;
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
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 多个map reduce 
 * 链表
 * @author Administrator
 *
 */
public class MyChain extends Configured implements Tool{
     static class TopNumMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    StringTokenizer st = new StringTokenizer(value.toString());
		    String strKey = st.nextToken();
		    String strValue = st.nextToken();
		    int intValue = Integer.parseInt(strValue);
		    context.write(new Text(strKey), new IntWritable(intValue));
		    
		}
     }
     
     static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

 		@Override
 		protected void map(LongWritable key, Text value, Context context)
 				throws IOException, InterruptedException {
 		    StringTokenizer st = new StringTokenizer(value.toString());
 		    while(st.hasMoreTokens()){
 		    	context.write(new Text(st.nextToken()), new IntWritable(1));
 		    }
 		}
      }
      
     
     static class WCReducer extends Reducer<Text,IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) 
				sum += value.get();
			context.write(new Text(key), new IntWritable(sum));
		}
     }
     
     static class TopNumReducer extends Reducer<Text,IntWritable, WordWritable, NullWritable>{
        public TreeSet<WordWritable> ts = new TreeSet<>();
        public static final int KEY = 20;
 		@Override
 		protected void reduce(Text key, Iterable<IntWritable> values,
 				Context context) throws IOException, InterruptedException {
 			String name = key.toString();
 			int count = values.iterator().next().get();
 			WordWritable word = new WordWritable(name,count);
 			ts.add(word);
 			
 			if(ts.size() > KEY)
 				ts.remove(ts.last());
 		}
		@Override
		protected void cleanup(Reducer<Text, IntWritable, WordWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (WordWritable word : ts) {
				context.write(word, NullWritable.get());
			}
		}
     	 
     	 
      }
     
     public static void main(String[] args) throws Exception {  	
     	MyChain mc = new MyChain();
     	int status = ToolRunner.run(mc.getConf(), mc,args);
     	System.exit(status);
	}


	@Override
	public int run(String[] args) throws Exception {
//设置wcjob   first job		
		Job wcjob = getJob(this);
		wcjob.setJarByClass(this.getClass());

     	//运行
     	wcjob.setMapperClass(WCMapper.class);
     	wcjob.setReducerClass(WCReducer.class); 
     	
     	wcjob.setPartitionerClass(WordPatitioner.class);
     	wcjob.setNumReduceTasks(4);
     	
     	//输出结果的key和value的类型
     	wcjob.setMapOutputKeyClass(Text.class);
     	wcjob.setMapOutputValueClass(IntWritable.class);
    	wcjob.setOutputKeyClass(Text.class);
     	wcjob.setOutputValueClass(IntWritable.class);
     	
     	Path inPath1 = new Path("hdfs://192.168.254.11:8020/mr/input/a");
     	FileInputFormat.addInputPath(wcjob, inPath1);
     	Path outPath1 = new Path("hdfs://192.168.254.11:8020/mr/input/b");
     	FileOutputFormat.setOutputPath(wcjob, outPath1);

//设置tnjob   second job	
     	Job tnjob = getJob(this);
     	tnjob.setJarByClass(this.getClass());

     	//运行
     	tnjob.setMapperClass(TopNumMapper.class);
     	tnjob.setReducerClass(TopNumReducer.class); 
     	
     	tnjob.setPartitionerClass(WordPatitioner.class);
     	tnjob.setNumReduceTasks(4);
     	
     	//输出结果的key和value的类型
     	tnjob.setMapOutputKeyClass(Text.class);
     	tnjob.setMapOutputValueClass(IntWritable.class);
     	tnjob.setOutputKeyClass(WordWritable.class);
     	tnjob.setOutputValueClass(NullWritable.class);
     	
     	Path inPath2 = new Path("hdfs://192.168.254.11:8020/mr/input/b");
     	FileInputFormat.addInputPath(tnjob, inPath2);
     	Path outPath2 = new Path("hdfs://192.168.254.11:8020/mr/input/c");
     	FileOutputFormat.setOutputPath(tnjob, outPath2);
     	
     	//封装2个job 控制器
     	ControlledJob cjob1 = new ControlledJob(wcjob.getConfiguration());
     	ControlledJob cjob2 = new ControlledJob(tnjob.getConfiguration());
     	
     	//cjob2要依赖于cjob1
     	cjob2.addDependingJob(cjob1);
     	
     	//定义多个job的执行流程
     	JobControl mjc = new JobControl("wc and tn jobs");
     	mjc.addJob(cjob1);
     	mjc.addJob(cjob2);
     	
     	//创建线程对象
     	Thread mjcThread = new Thread(mjc);
     	
     	mjcThread.start();
     	while(!mjc.allFinished()){
     		Thread.sleep(3000);
     	}
     	mjc.stop();
     	
		return 0;
	}
	
	@SuppressWarnings("static-access")
	public Job getJob(Tool tool){
		Configuration conf = tool.getConf();
		Job job = null;
		try {
			job = job.getInstance(conf, tool.getClass().getSimpleName());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return job;
	}
}


