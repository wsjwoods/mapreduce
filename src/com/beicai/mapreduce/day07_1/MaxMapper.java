package com.beicai.mapreduce.day07_1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxMapper extends MapReduceBase implements Mapper<LongWritable ,Text,Text,IntWritable>{
  private Logger log = LoggerFactory.getLogger(MaxMapper.class);
  private String delimiter=null;
  @Override
  public void configure(JobConf conf){
    delimiter=conf.get("delimiter");
    log.info("delimiter:"+delimiter);
    log.info("This is the begin of MaxMapper");
  }
  
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> out, Reporter reporter) throws IOException {
    String[] values= value.toString().split(delimiter);
    log.info(values[0]+"-->"+values[1]);
    out.collect(new Text(values[0]), new IntWritable(Integer.parseInt(values[1])));
    
  }
  public void close(){
    log.info("This is the end of MaxMapper");
  }
}