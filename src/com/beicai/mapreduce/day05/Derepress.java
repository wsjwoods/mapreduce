package com.beicai.mapreduce.day05;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * 多个文件
 * file1:
 *   2012-03-01 北京
 *   2012-03-02 辽宁
 * file2:
 *   2012-03-01 北京
 *   2012-03-02 山西
 * result:
 * 	 2012-03-01   北京
 *   2012-03-02 辽宁
 *   2012-03-02 山西
 *   
 * 为什么做去重？
 *   掌握并利用并行化思想对数据进行有意义的筛选
 *   统计大数据集上的数据种类个数，从网站日志中计算访问地点等看似很庞杂的任务，都会涉及数据去重
 * @author Administrator
 */
public class Derepress extends ToolRunner implements Tool{
    static class MyMapper extends Mapper<Object, Text, Text, Text>{
        private static Text line = new Text();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			line = value;
			context.write(line, new Text(""));
		}
    }
    
    static class MyReducer extends Reducer<Text,Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			context.write(key, new Text(""));
		}
    }
    
    public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
     	int status = ToolRunner.run(conf, new Derepress(), arguments);
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
   	    //job.setCombinerClass(MyReducer.class);	
     	
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
