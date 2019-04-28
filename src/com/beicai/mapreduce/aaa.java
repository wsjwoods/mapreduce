package com.beicai.mapreduce;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class aaa {
  
	public static void main(String[] args) {
		StringBuffer str = new StringBuffer();
		String a = "啥地方了【科技】未来、[方法]法律及科技网[呜呜]【得到】";
		StringTokenizer st = new StringTokenizer(a,"【[");
		while(st.hasMoreTokens()){
			String b = st.nextToken();
			int index = -2;
			if((index = b.indexOf("]"))!=-1){
				str.append("*" + b.substring(0,index));
			}
			if((index = b.indexOf("】"))!=-1){
				str.append("*" + b.substring(0,index));
			}
		}
		System.out.println(str.replace(0, 1, ""));
	}
   
}
