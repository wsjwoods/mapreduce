package com.beicai.mapreduce.day07;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WordWritable implements WritableComparable<WordWritable>{
    private String word;
    private int count;
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(word);
		out.writeInt(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word = in.readUTF();
		count = in.readInt();
	}

	@Override
	public String toString() {
		return this.word + "\t" + this.count;
	}

	@Override
	public int compareTo(WordWritable o) {
		if(this.count == o.count)
			return this.word.compareTo(o.word);
		return this.count-o.count;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public WordWritable(String word, int count) {
		super();
		this.word = word;
		this.count = count;
	}

	public WordWritable() {
		super();
	}
     
}
