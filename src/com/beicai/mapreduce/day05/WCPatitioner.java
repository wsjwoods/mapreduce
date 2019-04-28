package com.beicai.mapreduce.day05;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WCPatitioner extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numPatitioner) {
		String lineValue = key.toString();
		String firstChar = lineValue.substring(0, 1);
		if(firstChar.matches("[a-z]")){
			return 0%numPatitioner;
		}
		else if(firstChar.matches("[A-Z]")){
			return 1%numPatitioner;
		}
		else if(firstChar.matches("[0-9]")){
			return 2%numPatitioner;
		}else{
			return 3%numPatitioner;
		}
		
	}

}
