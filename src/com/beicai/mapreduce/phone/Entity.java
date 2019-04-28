package com.beicai.mapreduce.phone;

public class Entity {
	private String strId,strPhone,strMac,strIp,strURL,strType,strStatus1;
	private String strStatus2,strUpflow,strDownflow,strStatus3,strTime;
	private String[] fileds;
	
	public void mySplit1(String lineValue){
		fileds = lineValue.split("\t");
		this.setStrId(fileds[0]);
		this.setStrPhone(fileds[1]);
		this.setStrMac(fileds[2]);
		this.setStrIp(fileds[3]);
		this.setStrURL(fileds[4]);
		this.setStrType(fileds[5]);
		this.setStrStatus1(fileds[6]);
		this.setStrStatus2(fileds[7]);
		this.setStrUpflow(fileds[8]);
		this.setStrDownflow(fileds[9]);
		this.setStrStatus3(fileds[10]);
		this.setStrTime(fileds[11]);
	}
	
	
	public String getStrId() {
		return strId;
	}
	public void setStrId(String strId) {
		this.strId = strId;
	}
	public String getStrPhone() {
		return strPhone;
	}
	public void setStrPhone(String strPhone) {
		this.strPhone = strPhone;
	}
	public String getStrMac() {
		return strMac;
	}
	public void setStrMac(String strMac) {
		this.strMac = strMac;
	}
	public String getStrIp() {
		return strIp;
	}
	public void setStrIp(String strIp) {
		this.strIp = strIp;
	}
	public String getStrURL() {
		return strURL;
	}
	public void setStrURL(String strURL) {
		this.strURL = strURL;
	}
	public String getStrType() {
		return strType;
	}
	public void setStrType(String strType) {
		this.strType = strType;
	}
	public String getStrStatus1() {
		return strStatus1;
	}
	public void setStrStatus1(String strStatus1) {
		this.strStatus1 = strStatus1;
	}
	public String getStrStatus2() {
		return strStatus2;
	}
	public void setStrStatus2(String strStatus2) {
		this.strStatus2 = strStatus2;
	}
	public String getStrUpflow() {
		return strUpflow;
	}
	public void setStrUpflow(String strUpflow) {
		this.strUpflow = strUpflow;
	}
	public String getStrDownflow() {
		return strDownflow;
	}
	public void setStrDownflow(String strDownflow) {
		this.strDownflow = strDownflow;
	}
	public String getStrStatus3() {
		return strStatus3;
	}
	public void setStrStatus3(String strStatus3) {
		this.strStatus3 = strStatus3;
	}
	public String getStrTime() {
		return strTime;
	}
	public void setStrTime(String strTime) {
		this.strTime = strTime;
	}
	
}
