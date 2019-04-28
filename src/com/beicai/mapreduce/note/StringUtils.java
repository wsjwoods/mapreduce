package com.beicai.mapreduce.note;

import java.util.StringTokenizer;

public class StringUtils {
	public static String getStr(String str){
		StringBuffer sb = new StringBuffer();
		StringTokenizer st = new StringTokenizer(str,"【[《");
		while(st.hasMoreTokens()){
			String b = st.nextToken();
			int index = -2;
			if((index = b.indexOf("]"))!=-1){
				sb.append("*[" + b.substring(0,index)+"]");
			}
			if((index = b.indexOf("】"))!=-1){
				sb.append("*【" + b.substring(0,index)+"】");
			}
			if((index = b.indexOf("》"))!=-1){
				sb.append("*《" + b.substring(0,index)+"》");
			}
		}
		return sb.replace(0, 1, "").toString();
	}
	
	public static String getTime(String str){
		return str.split(" ")[0];
	}
	
}
