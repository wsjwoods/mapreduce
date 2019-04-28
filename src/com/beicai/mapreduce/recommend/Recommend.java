package com.beicai.mapreduce.recommend;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Recommend  extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		Recommend rm = new Recommend();
     	int status = ToolRunner.run(rm.getConf(), rm,args);
     	System.exit(status);
	}

	@Override
	public int run(String[] args) throws Exception {
		Step1 s1 = new Step1();
		Step2 s2 = new Step2();
		Step3 s3 = new Step3();
		Step4 s4 = new Step4();
		Step5 s5 = new Step5();
		//封装2个job 控制器
     	ControlledJob cjob1 = new ControlledJob(s1.getJob(getConf()).getConfiguration());
     	ControlledJob cjob2 = new ControlledJob(s2.getJob(getConf()).getConfiguration());
     	ControlledJob cjob3 = new ControlledJob(s3.getJob(getConf()).getConfiguration());
     	ControlledJob cjob4 = new ControlledJob(s4.getJob(getConf()).getConfiguration());
     	ControlledJob cjob5 = new ControlledJob(s5.getJob(getConf()).getConfiguration());
     	//cjob2要依赖于cjob1
     	cjob2.addDependingJob(cjob1);
     	cjob3.addDependingJob(cjob1);
     	cjob4.addDependingJob(cjob2);
     	cjob4.addDependingJob(cjob3);
     	cjob5.addDependingJob(cjob4);
     	
     	//定义多个job的执行流程
     	JobControl mjc = new JobControl("wc and tn jobs");
     	mjc.addJob(cjob1);
     	mjc.addJob(cjob2);
     	mjc.addJob(cjob3);
     	mjc.addJob(cjob4);
     	mjc.addJob(cjob5);
     	
     	//创建线程对象
     	Thread mjcThread = new Thread(mjc);
     	
     	mjcThread.start();
     	while(!mjc.allFinished()){
     		Thread.sleep(3000);
     	}
     	mjc.stop();
     	
		return 0;
	}
}
