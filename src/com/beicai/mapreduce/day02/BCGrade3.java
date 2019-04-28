package com.beicai.mapreduce.day02;


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 	武世建	103	java初级	90	91	92	93
	任中超	103	java初级	89	88	84	81
	邓春雷	103	java初级	92	88	90	86
	刘晋元	104	java初级	88	84	81	79
	郭林		104	java初级	85	82	81	89
	宋鑫		104	java初级	89	81	72	71
	卫恒		105	java初级	81	85	81	74
	徐海涛	105	java初级	79	76	75	90
	张金磊	105	java初级	78	71	74	75
	
	各个人的总成绩（笔试 *1.1 机试 *0.9  [项目+面试]/1.2）
 * @author Administrator
 *
 */
public class BCGrade3 extends ToolRunner implements Tool{
     static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
        private Text word = new Text();
        final static IntWritable one = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    String lineValue = value.toString();
		    
		    InputSplit inputSplit = context.getInputSplit();
			String filename=((FileSplit)inputSplit).getPath().getName();		
			filename = filename.substring(0,filename.indexOf('.'));
		    
		    StringTokenizer st = new StringTokenizer(lineValue,"\t");
		    StringBuffer sb = new StringBuffer();
		    if (st.hasMoreElements()) {
				String name = st.nextToken();
				String bcclass = st.nextToken();
				String subject = st.nextToken();
				sb.append(st.nextToken()+"_"+st.nextToken()+"_"+st.nextToken()+"_"+st.nextToken());	
				context.write(new Text(filename+":"+name), new Text(sb.toString()));
			}
		    
		}
     }
     
     static class MyReducer extends Reducer<Text,Text, Text, FloatWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			float sum = 0;
			float[] weights = {1.1F,0.9F,1.2F};
			for (Text value : values) {
				String str = value.toString();
				String[] scores = str.split("\\_");
				if(scores.length<1){
					continue;
				}
				for(int i=0;i<scores.length;i++){
					if(i==2){
						sum += (Float.parseFloat(scores[i])+Float.parseFloat(scores[i+1]))/weights[i];
						break;
					}
					sum += Float.parseFloat(scores[i])*weights[i];
				}			
			}
			context.write(key, new FloatWritable(sum));
		}
     }
     
     public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	String[] arguments = new GenericOptionsParser(conf,args).getRemainingArgs();
     	int status = ToolRunner.run(conf, new BCGrade3(), arguments);
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
     	job.setJobName("mybcgrade");
     	//运行
     	job.setJarByClass(BCGrade3.class);
     	job.setMapperClass(MyMapper.class);
     	job.setReducerClass(MyReducer.class);
     	
     	//输入输出类型
     	FileInputFormat.addInputPath(job, new Path(args[0]));
     	FileOutputFormat.setOutputPath(job, new Path(args[1]));
     	
     	//输出结果的key和value的类型
     	job.setMapOutputKeyClass(Text.class);
     	job.setMapOutputValueClass(Text.class);
     	job.setOutputKeyClass(Text.class);
     	job.setOutputValueClass(FloatWritable.class);
     	
     	//提交任务
     	boolean isSuccess = job.waitForCompletion(true);
		return isSuccess? 0 : 1;
	}
}


