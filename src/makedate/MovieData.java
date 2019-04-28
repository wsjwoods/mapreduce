package makedate;

import java.io.File;

import java.io.FileOutputStream;

import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import freemarker.template.SimpleDate;

/**
 * 用戶名  電影名  評分（1-10）
 * 1	教父	10
 * @author Administrator
 *
 */
public class MovieData {
	static int user = 50;
	static String[] movies = { "肖申克的救赎", "教父", "禁闭岛", "盗梦空间", "王的盛宴", "沉默的羔羊", "华尔街之狼", "碟中谍", "007", "星球大战" };
	static Random rm = new Random();

	public static void main(String[] args) throws Exception {
		File file = new File("D:\\大数据\\mapreduce\\data\\items.txt");

		FileOutputStream fos = new FileOutputStream(file);
		OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
		int[] numsuser = new int[user];
		
		for (int i = 0; i < numsuser.length; i++) {
			numsuser[i] = rm.nextInt(6) + 3;
		}
		
		for (int i = 0; i < user; i++) {
			int[] numsmv = getRandom(numsuser[i]);
			for (int j = 0; j < numsuser[i]; j++) {
				String str = (i+1) + "\t" + movies[numsmv[j]] + "\t" + (float)(rm.nextInt(8)+3)/2;
				osw.write(str + "\r\n");
			}
		}
		osw.close();
	}
	
	public static int[] getRandom(int num){
		int showIn = num;//一共抽取個數
		int sumIn = 10;//總個數
		int[] nums = new int[showIn];
		for(int i=0;i<showIn;i++){
			nums[i] = (int)(Math.random()*sumIn);
			for(int j=0;j<i;j++){
				if(nums[i]==nums[j]){
					i--;
					break;
				}
			}
		}
		return nums;
	}
}
