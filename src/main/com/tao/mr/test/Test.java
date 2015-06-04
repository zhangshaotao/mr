package main.com.tao.mr.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Test {

	public Test() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		String line = "0067011990999991950051507004888888889999999N9+00001+9999999999999999999999";
		
		String year = line.substring(15,19);
		String temperature = line.substring(45,50);
		if(temperature.indexOf("9999") != -1)
		{
			System.out.println("temperature is error!");
			return;
		}
		String quality = line.substring(50,51);
		if(!quality.matches("[01459]"))
		{
			System.out.println("temperature is exception");
			return;
		}
		if(temperature.startsWith("+") || temperature.startsWith("-"))
		{
			System.out.println(year+""+temperature);
		}	

	}

}
