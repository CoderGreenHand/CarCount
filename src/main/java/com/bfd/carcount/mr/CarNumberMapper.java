package com.bfd.carcount.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bfd.carcount.pojo.CarInfoWritable;

import sun.tools.tree.LengthExpression;

/**
 * 
 * @author jiaze.yuan
 * 读取hive中从kafka接入的原始数据
 * 将deviceid，timestamp，region读取出来
 * 以deviceid作为map的key
 * reduce中按照同一辆车的timestamp进行排序
 *
 */

public class CarNumberMapper extends Mapper<LongWritable, Text, Text, CarInfoWritable>{
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		
		CarInfoWritable carInfo = new CarInfoWritable();
		String[] line = value.toString().split("\t");
		//System.out.println(value.toString()+":"+line.length);
		int length = line.length;
		switch (length) {
		case 7:
			carInfo.setDeviceid(line[0]);
			carInfo.setDate(Long.valueOf(line[1]));
			carInfo.setProvince(line[6]);
			carInfo.setCity("");
			carInfo.setRegion("");
			break;
		case 8:
			carInfo.setDeviceid(line[0]);
			carInfo.setDate(Long.valueOf(line[1]));
			carInfo.setProvince(line[6]);
			carInfo.setCity(line[7]);
			carInfo.setRegion("");
			break;
		case 9:
			carInfo.setDeviceid(line[0]);
			carInfo.setDate(Long.valueOf(line[1]));
			carInfo.setProvince(line[6]);
			carInfo.setCity(line[7]);
			carInfo.setRegion(line[8]);
			break;
		default:
			System.out.println(value.toString()+"未解析成功......");
			break;
		}
		if(carInfo.getDeviceid()!=null){
			Text outKey = new Text(carInfo.getDeviceid());
			context.write(outKey, carInfo);
		}
		
	}
}
