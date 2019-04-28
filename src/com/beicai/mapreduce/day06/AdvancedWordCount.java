package com.beicai.mapreduce.day06;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
 * 从一个文件当中过滤无意义的单词
 * 过滤无意义的单词（a an the等）之后的文本词频统计
 * 流程：将事先定义的无意义单词保存到一个词典中，这个词典放在hdfs上  然后去运行程序
 * @author Administrator
 *
 */
public class AdvancedWordCount extends Configured implements Tool{
     static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private static final IntWritable one = new IntWritable(1);
    	private Text word = new Text();
    	private HashSet<String> keyWord;
    	private Path[] localFiles;
    	
    	@SuppressWarnings("deprecation")
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			keyWord = new HashSet<>();
			Configuration conf = context.getConfiguration();
			localFiles = DistributedCache.getLocalCacheFiles(conf);
			for (int i = 0; i < localFiles.length; i++) {
				String aKeyWord;
				BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));
				while((aKeyWord = br.readLine())!=null){
					keyWord.add(aKeyWord);
				}
				br.close();
			}
		}
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    StringTokenizer st = new StringTokenizer(value.toString());
		    while (st.hasMoreElements()) {
				String aword = st.nextToken();
				if(!keyWord.contains(aword)){
					word.set(aword);
					context.write(word, one);
				}
			}
		}
     }
     
     static class MyReducer extends Reducer<Text,IntWritable, Text, IntWritable>{
    	 private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
    	 
    	 
     }
     
 	
     @SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	DistributedCache.addCacheFile(new URI("hdfs://192.168.254.11:8020/mr/input/dictionary.txt"), conf);
     	
     	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
     	int status = ToolRunner.run(conf, new AdvancedWordCount(), arguments);
     	System.exit(status);
	}

	

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = parseInputAndOutput(this, this.getConf(), args);

     	//运行
     	job.setMapperClass(MyMapper.class);
     	job.setCombinerClass(MyReducer.class);
     	job.setReducerClass(MyReducer.class);
     	
     	//默认分区 == 1各分区  00000
     	job.setPartitionerClass(HashPartitioner.class);
     	//默认reduce数量
     	job.setNumReduceTasks(1);
     	
     	//默认Combiner
     	
     	
     	//默认分组比较器
     	//job.setSortComparatorClass();
     	
     	//输出结果的key和value的类型
     	job.setMapOutputKeyClass(Text.class);
     	job.setMapOutputValueClass(IntWritable.class);
    	job.setOutputKeyClass(Text.class);
     	job.setOutputValueClass(IntWritable.class);
     	   	
     	//提交任务
     	boolean isSuccess = job.waitForCompletion(true);
		return isSuccess? 0 : 1;
	}
	
	public Job parseInputAndOutput(Tool tool,Configuration conf,String[] args) throws Exception{
		Job job = Job.getInstance(conf,this.getClass().getName());
		job.setJarByClass(tool.getClass());
		
		//输入输出类型
     	FileInputFormat.addInputPath(job, new Path(args[0]));
     	FileOutputFormat.setOutputPath(job, new Path(args[1]));
     	
		return job;
	}
}


