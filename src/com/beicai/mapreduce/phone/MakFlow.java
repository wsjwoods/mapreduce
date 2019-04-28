package com.beicai.mapreduce.phone;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Random;

public class MakFlow {
	private static final String PROJECT_DIR = "files";
	private static long[] phoneNum = { 18211575960l, 18211575961l, 18211575962l, 18211575963l, 18211575964l,
			18211575965l, 18211575966l, 18211575967l, 18211575968l, 18211575969l, 18211575970l, 18211575971l,
			18211575972l, 18211575973l, 18211575974l, 18211575975l, 18211575976l, 18211575977l, 18211575978l,
			18211575979l };
	private static String[] mac = { "14-71-AC-CD-E6-18:CMCC-EASY", "24-71-AC-CD-E6-18:CMCC-EASY",
			"34-71-AC-CD-E6-18:CMCC-EASY", "44-71-AC-CD-E6-18:CMCC-EASY", "54-71-AC-CD-E6-18:CMCC-EASY",
			"64-71-AC-CD-E6-18:CMCC-EASY", "77-71-AC-CD-E6-18:CMCC-EASY", "84-71-AC-CD-E6-18:CMCC-EASY",
			"99-71-AC-CD-E6-18:CMCC-EASY", "A1-71-AC-CD-E6-18:CMCC-EASY", "A2-71-AC-CD-E6-18:CMCC-EASY",
			"B1-71-AC-CD-E6-18:CMCC-EASY", "BB-71-AC-CD-E6-18:CMCC-EASY", "CC-71-AC-CD-E6-18:CMCC-EASY",
			"DE-71-AC-CD-E6-18:CMCC-EASY", };
	private static String[] ip = { "120.196.100.90", "120.196.100.91", "120.196.100.92", "120.196.100.93",
			"120.196.100.94", "120.196.100.95", "120.196.100.96", "120.196.100.97", "120.196.100.98",
			"120.196.100.99", };
	private static String[] url = { "iface.qiyi.com", "www.baidu.com", "www.126.com", "www.sina.com.cn", "www.aaa.com",
			"www.bbb.com", "www.ccc.com", "www.ddd.com", "www.eee.com", "www.fff.com", };
	private static String[] urlType = { "爱奇艺", "百度", "126网", "新浪", "a视频网", "a视频网", "c视频网", "d视频网", "e视频网",
			"f视频网", };
	private static Random random = new Random();
	private static String timer = "20160301";
	private static long id = 1363157993044l;

	public static void main(String[] args) throws Exception {
		// String strStart = "20151112000000"; //2015��11��12��0ʱ0��0�� ��ʼģ�� ��ģ��һ������
		// (1447257600000 long��ʱ��)
		// String strEnd = "20151113235959"; //2015��11��13��23ʱ59��59��
		// ����ģ��(1447430399000 long��ʱ��)
		// SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
		// long millionSeconds = sdf.parse(strStart).getTime();
		// long millionSeconds = sdf.parse(strEnd).getTime();
		// System.out.println(millionSeconds);//

		for (long i = 1447257600000l; i <= 1447430399000l; i = i + 1000l) {
			doWrite(phoneNum, i);
		}
		System.out.println("complete!");
	}

	public static void doWrite(long[] phoneNum, long time) throws Exception, FileNotFoundException {
		/**
		 * id 手机号  MAC IP URL TYPE ״̬1/״̬2/��������/��������/״̬3/ʱ�� 1363157993044
		 * 18211575961 94-71-AC-CD-E6-18:CMCC-EASY 120.196.100.99 iface.qiyi.com
		 * ��Ƶ��վ 15 12 1527 2106 200 1196413077278
		 */
		File file = new File(PROJECT_DIR + "/" + timer + ".txt");
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8");

		for (int i = 0; i < phoneNum.length; i++) {
			int[] ranNum = { random.nextInt(mac.length), random.nextInt(ip.length), random.nextInt(url.length),
					random.nextInt(201), random.nextInt(201), random.nextInt(20001), random.nextInt(50001),
					random.nextInt(2), };
			osw.append(id++ + "\t");
			osw.append(phoneNum[i] + "" + "\t");
			osw.append(mac[ranNum[0]] + "\t");
			osw.append(ip[ranNum[1]] + "\t");
			osw.append(url[ranNum[2]] + "\t");
			osw.append(urlType[ranNum[2]] + "\t");
			osw.append(ranNum[3] + "\t");
			osw.append(ranNum[4] + "\t");
			osw.append(ranNum[5] + "\t");
			osw.append(ranNum[6] + "\t");
			if (ranNum[7] == 0) {
				osw.append("100" + "\t");
			} else {
				osw.append("200" + "\t");
			}
			osw.append(time + "");
			osw.append('\n');
		}
		osw.close();
	}
}
