package makedate;

import java.io.File;

import java.io.FileOutputStream;

import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import freemarker.template.SimpleDate;


public class MakeData {
	String[] locations = {"天宫院","生物医药基地","黄村","大臧村","西红门"};
	String[] names ={"张三","李四","王五","陈六","孙七","赵八","周九"};
	int timecount = 0;
	/**
	 * 
	 * @param filename 文件名格式 20160101  or  201601
	 * @param sp   数据格式       id 姓名 时间 步数 心跳 位置 热量  
	 * @throws Exception 
	 */
     public void makeData(String filename) throws Exception{
    	 File file = new File("D:\\大数据\\mapreduce\\data\\"+filename);
  
    	 FileOutputStream fos = new FileOutputStream(file);
    	 OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
    	 for(int i = 0;i < names.length;i++){
    		 osw.write(setOneData(names[i], i+1,filename)+"\r\n");
    	 }
    	 osw.close();
     }
     
     public String setOneData(String name,int id,String date) throws Exception{
    	 Sports sp = new Sports();
    	 int count = (int)(Math.random()*(9999-100));
         String time = date.substring(0,date.indexOf("."));
    	 sp.setId(id);
    	 sp.setCount(count);
    	 sp.setLocation(locations[(int)(Math.random()*5)]);
    	 sp.setName(name);
    	 sp.setHeartbeat((int) (count*1.2));
    	 sp.setHeat((float) ((count/0.75)*60/1000));
    	 sp.setTime(time);
    	 return sp.toString();
     }
     
     public static void main(String[] args) throws Exception {
    	 MakeData md = new MakeData();
    	 //md.makeData("20160101.txt");
    	 for(int i=1;i<=30;i++){
    		 String date = "";
    		 if(i<10){
    			 date = "0"+i;
    		 } else {
    			 date = i+"";
    		 }
    		 md.makeData("201601"+date+".txt");
    	 }
    	 
    	 
 	}
}
