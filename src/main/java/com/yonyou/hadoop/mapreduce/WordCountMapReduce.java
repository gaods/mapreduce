package com.yonyou.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.Text;

public class WordCountMapReduce {
	public static void main(String[] args) {
		Configuration conf=new Configuration();
		try {
			//创建 job对象
			Job job=Job.getInstance(conf, "wordcount");
			job.setJarByClass(WordCountMapReduce.class);
			
			//设置mapper类
			job.setMapperClass(WordCountMapper.class);
			//设置reducer类
			job.setReducerClass(WordCountReducer.class);
			
			//设置map输出的key。value
			job.setMapOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			//设置输入输出路径
			FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/words1"));
			//
			FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/out10"));
			
			boolean b=job.waitForCompletion(true);
			if(!b){
				System.out.print("111");
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
