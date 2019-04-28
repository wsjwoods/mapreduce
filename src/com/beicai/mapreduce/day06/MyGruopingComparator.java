package com.beicai.mapreduce.day06;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义比较器
 * @author Administrator
 *
 */
public class MyGruopingComparator implements RawComparator<NewK3>{

	@Override
	public int compare(NewK3 o1, NewK3 o2) {
		// TODO Auto-generated method stub
		return o1.first - o2.first;
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		// TODO Auto-generated method stub
		return WritableComparator.compareBytes(b1, s1, 8, b2, s2, 8);
	}
   
}
