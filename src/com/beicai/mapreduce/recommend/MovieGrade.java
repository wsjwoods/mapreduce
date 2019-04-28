package com.beicai.mapreduce.recommend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 每部電影的平均評分并排名
 * @author Administrator
 *
 */
public class MovieGrade extends Configured implements Tool{
     static class MyMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    StringTokenizer st = new StringTokenizer(value.toString());
		    if (st.hasMoreElements()) {
				st.nextToken();
				String movie = st.nextToken();
				String score = st.nextToken();
				context.write(new Text(movie), new FloatWritable(Float.parseFloat(score)));
			}
		}
     }
     
     static class MyReducer extends Reducer<Text,FloatWritable, IntWritable,Movie>{
    	static Set<Movie> set = new TreeSet<>();
		@Override
		protected void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float sum = 0;
			int count = 0;
			for (FloatWritable value : values) {
				sum += value.get();
				count++;
			}
			Movie movie = new Movie(key.toString(),sum/count);
			set.add(movie);
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Iterator<Movie> it = set.iterator();
			int i = 1;
			while (it.hasNext()) {
				context.write(new IntWritable(i++), it.next());
			}
		}
		
		
     }
     
     public static void main(String[] args) throws Exception {  	
     	//获取配置信息
     	Configuration conf = new Configuration();
     	String[] arguments = new String[2];
     	arguments[0] = "hdfs://hadoop01:8020/mr/input/recommend/items.txt";
     	arguments[1] = "hdfs://hadoop01:8020/mr/input/recommend/moviegrade";;
     	int status = ToolRunner.run(conf, new MovieGrade(), arguments);
     	System.exit(status);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = parseInputAndOutput(this, getConf(), args);

     	//运行
     	job.setMapperClass(MyMapper.class);
     	job.setReducerClass(MyReducer.class);
     	
     	//默认分区 == 1各分区  00000
     	//job.setPartitionerClass(HashPartitioner.class);
     	//默认reduce数量
     	//job.setNumReduceTasks(1);  	
     	//默认Combiner
     	//job.setCombinerClass(Reducer.class);	
     	//默认分组比较器
     	//job.setSortComparatorClass();
     	
     	//输出结果的key和value的类型
     	job.setMapOutputKeyClass(Text.class);
     	job.setMapOutputValueClass(FloatWritable.class);
    	job.setOutputKeyClass(Movie.class);
     	job.setOutputValueClass(NullWritable.class);
     	   	
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
	
	static class Movie implements WritableComparable<Movie>{
		private String movie;
		private float score;
		public String getMovie() {
			return movie;
		}
		public void setMovie(String movie) {
			this.movie = movie;
		}
		public float getScore() {
			return score;
		}
		public void setScore(float score) {
			this.score = score;
		}
		public Movie(String movie, float score) {
			super();
			this.movie = movie;
			this.score = score;
		}
		@Override
		public int hashCode() {
			// TODO Auto-generated method stub
			return super.hashCode();
		}
		@Override
		public boolean equals(Object obj) {
			// TODO Auto-generated method stub
			return super.equals(obj);
		}
		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return movie + "\t" + score;
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(movie);
			out.writeFloat(score);
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			movie = in.readUTF();
			score = in.readFloat();
		}
		@Override
		public int compareTo(Movie o) {
			return -(int)(this.score*1000 - o.score*1000);
		}
		
	}
}


