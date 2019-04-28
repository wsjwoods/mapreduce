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
 * 多表联查
 * 输入两个文件，一个代表工厂，包含工厂名   地址ID
 *          另一个代表地址，包含地址名  地址ID
 * 要求输出 工厂名  地址名
 * 
 * file1
 * Beijing Red Star		1
 * 
 * file2
 * 1 	beijing
 * 
 * 输出    Beijing Red Star		北京
 * @author Administrator
 *
 */
public class MTjoin extends ToolRunner implements Tool{
     static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
    	
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString() ;
		    String realitontype = "";
		    if(lineValue.contains("factoryname") || lineValue.contains("addressname")){
		    	return ; 
		    }
		    StringTokenizer st = new StringTokenizer(lineValue, "\t");
		    String mapKey = "";
		    String mapValue = "";
		    int i = 0;
		    while (st.hasMoreElements()) {
				String token = st.nextToken();
				if(token.charAt(0) >= '0' && token.charAt(0)<='9'){ //如果是 地址id   则放入key
					mapKey = token;
					if(i>0){
						realitontype ="1";
					}else{
						realitontype ="2";
					}
					continue;
				}
				//存工厂名称
				mapValue += token;
				i++;
			}
		    /**
		     * K:地址ID  V:工厂名
		     * K:地址ID  V:地址名
		     * 
		     */
		    context.write(new Text(mapKey), new Text(realitontype + "+" + mapValue));
		}
     }
     
     static class MyReducer extends Reducer<Text,Text, Text, Text>{
    	 private boolean flag = true;
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			if(flag){
				context.write(new Text("factory"), new Text("addressd"));
				flag = false;
			}
			int factorynum = 0;
			String[] factory = new String[10];
			int addressnum = 0;
			String[] address = new String[10];
			
			for (Text value : values) {
				String record = value.toString();
				int len = record.length();
				int i = 2;
				if(len==0){
					continue;
				}
				//取得左右表标识
				char relationType = record.charAt(0);
				//左表
				if(relationType == '1'){
					factory[factorynum++] = record.substring(i);
				}
				//右表
				if(relationType == '2'){
					address[addressnum++] = record.substring(i);
				}
				if(factorynum!=0&&addressnum!=0){
					for (int j = 0; j < factorynum; j++) {
						for (int k = 0; k < addressnum; k++) {
							context.write(new Text(factory[j]), new Text(address[k]));
						}
					}
				}
				
			}
			
		}
    	 
    	 
     }
     
     public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
     	int status = ToolRunner.run(conf, new MTjoin(), arguments);
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


