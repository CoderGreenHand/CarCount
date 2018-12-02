package com.bfd.carcount.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CarInfoWritable implements Writable,Comparable<CarInfoWritable>,Cloneable {
	private String deviceid;//车辆id
	private Long date;//时间戳
	private String province;//省
	private String city;//市
	private String region;//区域
	
	

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getDeviceid() {
		return deviceid;
	}

	public void setDeviceid(String deviceid) {
		this.deviceid = deviceid;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public Long getDate() {
		return date;
	}

	public void setDate(Long date) {
		this.date = date;
	}

	@Override
	public String toString() {
		return deviceid+"\t"+region;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(deviceid);
		out.writeUTF(province);
		out.writeUTF(city);
		out.writeUTF(region);
		out.writeLong(date);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.deviceid = in.readUTF();
		this.province = in.readUTF();
		this.city = in.readUTF();
		this.region = in.readUTF();
		this.date = in.readLong();
		
	}

	@Override
	public int compareTo(CarInfoWritable carInfo) {
		//int returnData = this.date >carInfo.getDate()?1:-1;
		if(this.date>carInfo.getDate()){
			return 1;
		}else if(this.date<carInfo.getDate()){
			return -1;
		}else {
			return 0;
		}
	}
	@Override
	public CarInfoWritable clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return (CarInfoWritable) super.clone();
	}

}
