package com.beicai.mapreduce.day06;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class NewK2 implements WritableComparable<NewK2> {
     int first;
     int second;
     
     //这个方法规定了自定义参数类型在输出时的样子
     @Override
		public String toString() {
			// TODO Auto-generated method stub
			return first + "\t" + second;
		}
     
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(first);
		out.writeInt(second);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readInt();
		second = in.readInt();
		
	}
	/**
	 * 需要重写可比较的方法
	 * 有两个数比较
	 * 第一个数 比较 !=0 返回 
	 * 第一个数 比较 ==0 则比较第二个数 
	 * 
	 */
	@Override
	//从小到大排列
	public int compareTo(NewK2 o) {
		int tmp = this.first - o.first;
		if(tmp != 0){
			return tmp;
		} 
		return this.second-o.second;
	}
	
	public int getFitst() {
		return first;
	}
	public void setFitst(int fitst) {
		this.first = fitst;
	}
	public int getSecond() {
		return second;
	}
	public void setSecond(int second) {
		this.second = second;
	}
	public NewK2(int fitst, int second) {
		super();
		this.first = fitst;
		this.second = second;
	}
	public NewK2() {
		super();
	}
	
     
}
