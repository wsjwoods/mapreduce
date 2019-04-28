package com.beicai.mapreduce.day06;


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 反向索引  倒排索引 
 * 
 * file1 : MapReduce is simple
 * file2 : MapReduce is powerful is simple
 * file3 : Hello MapReduce bye MapReduce
 * 结果:
 *      MapReduce      file1.txt:1;file2.txt:1;file3.txt:2;
		is        　　 file1.txt:1;file2.txt:2;
		simple         file1.txt:1;file2.txt:1;
		powerful   　　file2.txt:1;
		Hello       　 file3.txt:1;
		bye       　　 file3.txt:1;
 * 正向搜索: 给key的url值 
 * 反向搜索: 给正向搜索提供分词的方法
 * @author Administrator
 *
 */
public class InvertedIndex extends ToolRunner implements Tool{
     static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
    	 private Text keyInfo = new Text();
    	 private Text valueInfo = new Text();
    	 private FileSplit split;
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    //获得<key,value>对所属的FileSplit对象
			split = (FileSplit) context.getInputSplit();
		    StringTokenizer st = new StringTokenizer(value.toString());
		    while (st.hasMoreElements()) {
		    	//int splitinfo = split.getPath().toString().indexOf("file");
		    	//keyInfo.set(st.nextToken() + ":" + split.getPath().toString().substring(splitinfo));
		    	keyInfo.set(st.nextToken() + ":" + split.getPath().getName());
		    }
		    //词频初始化
		    valueInfo.set("1");
		    context.write(keyInfo, valueInfo);
		}
     }
     
     static class MyReducer extends Reducer<Text,Text, Text, Text>{
    	private Text result = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			//生成文档列表
			String fileList = "";
			for (Text value : values) {
				fileList += value.toString() + ";";
			}
			result.set(fileList);
			context.write(key, result);
			
		}
    	 
    	 
     }
     
     public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
     	int status = ToolRunner.run(conf, new InvertedIndex(), arguments);
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
     	
     	//默认分区 == 1各分区  00000
     	job.setPartitionerClass(HashPartitioner.class);
     	//默认reduce数量
     	job.setNumReduceTasks(1);
     	//设置combiner
     	job.setCombinerClass(MyCombiner.class);
     	//默认分组比较器
     	//job.setSortComparatorClass();
     	
     	//输出结果的key和value的类型
     	job.setMapOutputKeyClass(Text.class);
     	job.setMapOutputValueClass(Text.class);
    	job.setOutputKeyClass(Text.class);
     	job.setOutputValueClass(Text.class);
     	   	
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


