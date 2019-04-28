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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 排序
 * @author Administrator
 *
 */
public class Sort extends ToolRunner implements Tool{
     static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
        private static IntWritable data =new IntWritable();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    String line = value.toString();
		    data.set(Integer.parseInt(line));
		    context.write(data, new IntWritable(1));
		}
     }
     
     static class MyReducer extends Reducer<IntWritable,IntWritable, IntWritable, IntWritable>{
        private int i = 1;
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			for (IntWritable value : values) {
				context.write(new IntWritable(i++), key);
			}
		}
     }
     
     /**
      * 自定义分区
      * 指定一个当前数据中最大值/当前分区数 = 分区边界线
      * 根据边界线让这个数据去哪个分区；
      * @author Administrator
      *
      */
     static class MyPartitioner extends Partitioner<IntWritable,IntWritable>{

		@Override
		public int getPartition(IntWritable key, IntWritable value, int numPatition) {
			int Maxnumber = 8000;
			int bound = Maxnumber/numPatition;
			int keyNum = key.get();
			for (int i = 0; i < numPatition; i++) {
				if(keyNum < bound*(i+1)&&keyNum>=bound){
					return i;
				}
			}
			return 0;
			
		}
    	 
     }
     
     public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
     	int status = ToolRunner.run(conf, new Sort(), arguments);
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
		Job job = parseInputAndOutput(this, getConf(), args);

     	//运行
     	job.setMapperClass(MyMapper.class);
     	job.setReducerClass(MyReducer.class);
     	
     	job.setNumReduceTasks(3);
     	job.setPartitionerClass(MyPartitioner.class);
     	
     	//输出结果的key和value的类型
     	job.setMapOutputKeyClass(IntWritable.class);
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


