package main.com.tao.mr.maxtemperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text,IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
//		int maxTemperature = 0;
//		int minTemperature = 0;
//		
//		Iterator iterator = values.iterator();
//		while(iterator.hasNext())
//		{
//			int temp = ((IntWritable)iterator.next()).get();
//			if(temp > maxTemperature)	
//			{	
//				maxTemperature = temp;
//			}	
//		}
		
		int maxTemperature = Integer.MIN_VALUE;
		
		for(IntWritable val:values)
		{
			maxTemperature = Math.max(maxTemperature, val.get());
		}

		context.write(key, new IntWritable(maxTemperature));
	}

}
