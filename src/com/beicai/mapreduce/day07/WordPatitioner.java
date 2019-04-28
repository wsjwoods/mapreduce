package com.beicai.mapreduce.day07;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPatitioner extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int num) {
		String val = key.toString().substring(0, 1);
		if(val.matches("[a-i]"))
			return 0%num;
		else if(val.matches("[j-s]"))
		    return 1%num;
		else if(val.matches("[t-z]"))
			return 2%num;
		return 3%num;
	}

}
