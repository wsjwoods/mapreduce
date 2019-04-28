package com.beicai.mapreduce.day05;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WCWritable implements WritableComparable<WCWritable>{
    private String word;
	private int count;
	
	public WCWritable() {
	}

	public WCWritable(String word, int count) {
		this.word = word;
		this.count = count;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(word);
		out.writeInt(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		word = in.readUTF();
		count = in.readInt();
	}

	@Override
	public int compareTo(WCWritable o) {
		// TODO Auto-generated method stub
		if(this.count == o.count)
			return this.word.compareTo(o.word);
		
		return this.count-o.count;
	}
	
	
	@Override
	public String toString() {
		return this.word + "\t" + this.count;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + count;
		result = prime * result + ((word == null) ? 0 : word.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WCWritable other = (WCWritable) obj;
		if (count != other.count)
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

}
