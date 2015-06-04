package main.com.tao.mr.maxtemperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
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
			context.write(new Text(year), new IntWritable(Integer.parseInt(temperature)));
		}	
	}

}
