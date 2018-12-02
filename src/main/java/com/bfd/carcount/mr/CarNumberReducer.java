package com.bfd.carcount.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

import com.bfd.carcount.pojo.CarInfoWritable;

/**
 * 
 * @author jiaze.yuan
 * @see   将map同一个deviceid的信息进行合并，
 *  然后按照timestamp进行排序
 * 	同一辆车的上个时间点和下个时间点进行比较
 * 	如何两次的区域相同则跳过，如果不同，则将两次的区域对应的map<k,v>
 *	的value进行+1操作
 */

public class CarNumberReducer extends Reducer<Text, CarInfoWritable, Text, IntWritable> {
	/*private static Map<String,Integer> carCountPro = new HashMap<String,Integer>();
	private static Map<String,Integer> carCountCity = new HashMap<String,Integer>();
	private static Map<String,Integer> carCountRegion = new HashMap<String,Integer>();*/
	private static Map<String,Integer> carCountMap = new HashMap<String,Integer>();
	
	@Override
	protected void reduce(Text key, Iterable<CarInfoWritable> values, Context context)
			throws IOException, InterruptedException {
		String province = "";//存放临时省
		String city = "";//存放临时市
		String region = "";//存放临时区域
		List<CarInfoWritable> list = new ArrayList<CarInfoWritable>();
		for (CarInfoWritable carInfoWritable : values) {
			CarInfoWritable carInfo = WritableUtils.clone(carInfoWritable, context.getConfiguration());
			list.add(carInfo);
		}
		Collections.sort(list);//对同一辆车的value按时间进行排序
		//遍历集合中的元素，判断两个连续时间点的车是否在同一区域
		for (CarInfoWritable carInfo : list) {
			
			//----------------------------------------------------------省
			if (carInfo.getProvince()!=null && !carInfo.getProvince().equals("")) {
				if ("".equals(province)) {
					province = carInfo.getProvince();
				}else if(!province.equals(carInfo.getProvince())){
					
					//两次相邻时间所对应的区域不相等的时候，将region更新为本次
					//循环对应的区域，然后两个区域对应的次数+1
					//判断map中是否已经存在这两个region对应的值，如果存在则对
					//这两个key的value进行分别+1操作，并对region进行重新赋值
					if (carCountMap.containsKey(province)) {
						carCountMap.put(province,carCountMap.get(province)+1);
						
					}else{
						carCountMap.put(province, 1);
						
					}
					if(carCountMap.containsKey(carInfo.getProvince())){
						//判断下一个时间点的region是否在map<k,v>中，如果在，同样+1，
						//否则添加
						carCountMap.put(carInfo.getProvince(), carCountMap.get(carInfo.getProvince())+1);
					}else{
						carCountMap.put(carInfo.getProvince(), 1);
					}
					province = carInfo.getProvince();//更新region	
				}else if (province.equals(carInfo.getProvince())) {
					//context.write(new Text(region), new IntWritable(1));
					//continue; //两次相邻时间所对应的区域相等的时候，跳过本次循环
				}
				
			}
			
			
			//----------------------------------------------------------市
			if (carInfo.getCity()!=null && !carInfo.getCity().equals("")) {
				if ("".equals(city)) {
					city = carInfo.getCity();
				}else if(!city.equals(carInfo.getCity())){
					
					//两次相邻时间所对应的区域不相等的时候，将region更新为本次
					//循环对应的区域，然后两个区域对应的次数+1
					//判断map中是否已经存在这两个region对应的值，如果存在则对
					//这两个key的value进行分别+1操作，并对region进行重新赋值
					if (carCountMap.containsKey(city)) {
						carCountMap.put(city,carCountMap.get(city)+1);
						
					}else{
						carCountMap.put(city, 1);
						
					}
					if(carCountMap.containsKey(carInfo.getCity())){
						//判断下一个时间点的region是否在map<k,v>中，如果在，同样+1，
						//否则添加
						carCountMap.put(carInfo.getCity(), carCountMap.get(carInfo.getCity())+1);
					}else{
						carCountMap.put(carInfo.getCity(), 1);
					}
					city = carInfo.getCity();//更新region	
				}else if (city.equals(carInfo.getCity())) {
					//context.write(new Text(region), new IntWritable(1));
					//continue; //两次相邻时间所对应的区域相等的时候，跳过本次循环
				}
				
			}
			
			//----------------------------------------------------------------------------区
			if (carInfo.getRegion()!=null && !carInfo.getRegion().equals("")) {
				if ("".equals(region)) {
					region = carInfo.getRegion();
				}else if(!region.equals(carInfo.getRegion())){
					
					//两次相邻时间所对应的区域不相等的时候，将region更新为本次
					//循环对应的区域，然后两个区域对应的次数+1
					//判断map中是否已经存在这两个region对应的值，如果存在则对
					//这两个key的value进行分别+1操作，并对region进行重新赋值
					if (carCountMap.containsKey(region)) {
						carCountMap.put(region,carCountMap.get(region)+1);
						
					}else{
						carCountMap.put(region, 1);
						
					}
					if(carCountMap.containsKey(carInfo.getRegion())){
						//判断下一个时间点的region是否在map<k,v>中，如果在，同样+1，
						//否则添加
						carCountMap.put(carInfo.getRegion(), carCountMap.get(carInfo.getRegion())+1);
					}else{
						carCountMap.put(carInfo.getRegion(), 1);
					}
					region = carInfo.getRegion();//更新region	
				}else if (region.equals(carInfo.getRegion())) {
					//context.write(new Text(region), new IntWritable(1));
					continue; //两次相邻时间所对应的区域相等的时候，跳过本次循环
				}
				
			}
			//context.write(new Text(carInfo.toString()), new IntWritable(1));
			
		}

		
	}
	
	@Override
	protected void cleanup(Reducer<Text, CarInfoWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//一次将所有map的统计结果输出，21w辆车
		for (Map.Entry<String, Integer> entry: carCountMap.entrySet()) {
			context.write(new Text(entry.getKey()),new IntWritable(entry.getValue()));
		}
		super.cleanup(context);
	}
	/*public static void main(String[] args) throws CloneNotSupportedException {
		List<CarInfoWritable> list = new ArrayList<>();
		CarInfoWritable carInfoWritable1 = new CarInfoWritable();
		carInfoWritable1.setDeviceid("111112");
		carInfoWritable1.setDate(1234567l);
		carInfoWritable1.setRegion("张家口");
		CarInfoWritable carInfoWritable5 = new CarInfoWritable();
		carInfoWritable5.setDeviceid("111112");
		carInfoWritable5.setDate(1234569l);
		carInfoWritable5.setRegion("张家口");
		CarInfoWritable carInfoWritable2 = new CarInfoWritable();
		carInfoWritable2.setDeviceid("111112");
		carInfoWritable2.setDate(1334567l);
		carInfoWritable2.setRegion("廊坊");
		CarInfoWritable carInfoWritable3 = new CarInfoWritable();
		carInfoWritable3.setDeviceid("111112");
		carInfoWritable3.setDate(234567l);
		carInfoWritable3.setRegion("秦皇岛");
		CarInfoWritable carInfoWritable4 = new CarInfoWritable();
		carInfoWritable4.setDeviceid("111112");
		carInfoWritable4.setDate(1234587l);
		carInfoWritable4.setRegion("石家庄");
		CarInfoWritable carInfoWritable6 = carInfoWritable3.clone();
		list.add(carInfoWritable1);
		list.add(carInfoWritable2);
		list.add(carInfoWritable3);
		list.add(carInfoWritable4);
		list.add(carInfoWritable5);
		list.add(carInfoWritable6);
		Collections.sort(list);
		
		String region="";
		for (CarInfoWritable carInfo : list) {
			System.out.println(carInfo.toString()+"\t"+carInfo.getDate());
			if (carInfo.getRegion()!=null && !carInfo.getRegion().equals("")) {
				if ("".equals(region)) {
					region = carInfo.getRegion();
				}else if(!region.equals(carInfo.getRegion())){
					
					//两次相邻时间所对应的区域不相等的时候，将region更新为本次
					//循环对应的区域，然后两个区域对应的次数+1
					//判断map中是否已经存在这两个region对应的值，如果存在则对
					//这两个key的value进行分别+1操作，并对region进行重新赋值
					if (carCountMap.containsKey(region)) {
						carCountMap.put(region,carCountMap.get(region)+1);
						
					}else{
						carCountMap.put(region, 1);
						
					}
					if(carCountMap.containsKey(carInfo.getRegion())){
						//判断下一个时间点的region是否在map<k,v>中，如果在，同样+1，
						//否则添加
						carCountMap.put(carInfo.getRegion(), carCountMap.get(carInfo.getRegion())+1);
					}else{
						carCountMap.put(carInfo.getRegion(), 1);
					}
					region = carInfo.getRegion();//更新region	
				}else if (region.equals(carInfo.getRegion())) {
					//context.write(new Text(region), new IntWritable(1));
					continue; //两次相邻时间所对应的区域相等的时候，跳过本次循环
				}
			}
		}
		System.out.println("----------------------------------------");
		for (Map.Entry<String, Integer> entry: carCountMap.entrySet()) {
			System.out.println(entry.getKey()+":"+entry.getValue());
		}
	}*/

}
