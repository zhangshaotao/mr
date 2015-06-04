package main.com.tao.mr.maxtemperature;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperatureMain {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(args.length < 2)
		{
			System.exit(0);
		}
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance();
		job.setJobName("map_temperature");
		
		job.setJarByClass(MaxTemperatureMain.class);
		
		job.setMapperClass(MaxTemperatureMapper.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		//fs.deleteOnExit(outputPath);
		fs.delete(outputPath, true);
		FileOutputFormat.setOutputPath(job,outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
