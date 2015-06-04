package main.com.tao.mr.dedup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Dedup {
	 //map将输入中的value复制到输出数据的key上，并直接输出
    public static class Map extends Mapper<Object,Text,Text,Text>{
        private static Text line=new Text();//每行数据
       
        //实现map函数
        public void map(Object key,Text value,Context context)
                throws IOException,InterruptedException{
            line=value;
            context.write(line, new Text(""));
        }
       
    }
    
    //reduce将输入中的key复制到输出数据的key上，并直接输出
    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        //实现reduce函数
        public void reduce(Text key,Iterable<Text> values,Context context)
                throws IOException,InterruptedException{
        	
            context.write(key, new Text(""));
            
        }
       
    }
    
    public static void main(String[] args) throws Exception{
	    Configuration conf = new Configuration();
	    //这句话很关键
	    conf.set("mapred.task.timeout", "3600000");
	    conf.set("dfs.client.socket-timeout", "3600000");
	    conf.set("dfs.datanode.socket.write.timeout", "3600000"); 
	    conf.set("mapred.output.compress","false");
	    
	     String[] ioArgs=new String[]{"hdfs://182.48.116.58:8020/user/shaotao.zhang/input","hdfs://182.48.116.58:8020/user/shaotao.zhang/output"};
	     String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
	     if (otherArgs.length != 2) {
	     System.err.println("Usage: Data Deduplication <in> <out>");
	     	System.exit(2);
	     }
	     
	     //Job job = Job.getInstance(conf);
	     Job job = new Job(conf,"dedup");
	     job.setJarByClass(Dedup.class);
	     
	     //设置Map、Combine和Reduce处理类
	     job.setMapperClass(Map.class);
	     job.setCombinerClass(Reduce.class);
	     job.setReducerClass(Reduce.class);
	     
	     //设置输出类型
	     job.setOutputKeyClass(Text.class);
	     job.setOutputValueClass(Text.class);
	     
	     
	     job.setNumReduceTasks(1);
	     //job.setJobName("dedup");
	     
	     //设置输入和输出目录
	     
	     //System.out.println(otherArgs[0]+"----"+otherArgs[1]);
	     FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	     FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	   //  System.out.println(job.toString());
	   //  System.setProperty("hadoop.home.dir", "F:\\hadoop\\hadoop-2.5.2\\bin");
	     System.exit(job.waitForCompletion(true) ? 0 : 1);
     }
}
