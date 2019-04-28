package com.beicai.mapreduce.day07_1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataWritable implements Writable{
	private int downPackNum;
	private int upPackNum;
	private int downPayLoad;
	private int upPayLoad;

	public int getDownPackNum() {
		return downPackNum;
	}

	public void setDownPackNum(int downPackNum) {
		this.downPackNum = downPackNum;
	}

	public int getUpPackNum() {
		return upPackNum;
	}

	public void setUpPackNum(int upPackNum) {
		this.upPackNum = upPackNum;
	}

	public int getDownPayLoad() {
		return downPayLoad;
	}

	public void setDownPayLoad(int downPayLoad) {
		this.downPayLoad = downPayLoad;
	}

	public int getUpPayLoad() {
		return upPayLoad;
	}

	public void setUpPayLoad(int upPayLoad) {
		this.upPayLoad = upPayLoad;
	}
	public void set(int downPackNum,int upPackNum,
			int downPayLoad,int upPayLoad){
		this.setDownPackNum(downPackNum);
		this.setUpPackNum(upPackNum);
		this.setDownPayLoad(downPayLoad);
		this.setUpPayLoad(upPayLoad);
	}
    
	public DataWritable() {
		super();
	}
    
	public DataWritable(int downPackNum, int upPackNum, int downPayLoad,
			int upPayLoad) {
		set(downPackNum, upPackNum, downPayLoad, upPayLoad);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(downPackNum);
		out.writeInt(upPackNum);
		out.writeInt(downPayLoad);
		out.writeInt(upPayLoad);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.downPackNum=in.readInt();
		this.upPackNum=in.readInt();
		this.downPayLoad=in.readInt();
		this.upPayLoad=in.readInt();
		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + downPackNum;
		result = prime * result + downPayLoad;
		result = prime * result + upPackNum;
		result = prime * result + upPayLoad;
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
		DataWritable other = (DataWritable) obj;
		if (downPackNum != other.downPackNum)
			return false;
		if (downPayLoad != other.downPayLoad)
			return false;
		if (upPackNum != other.upPackNum)
			return false;
		if (upPayLoad != other.upPayLoad)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return downPackNum+"\t"+upPackNum+"\t"+downPayLoad+"\t"+upPayLoad;
	}

}
