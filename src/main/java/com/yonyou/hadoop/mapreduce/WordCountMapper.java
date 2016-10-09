package com.yonyou.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//得到输入的每一行数据
		String line=value.toString();
		//分割数据 ，通过空格来分割
		String[] words=line.split(" ");
		//循环遍历并输出
		for(String word:words){
			context.write(new Text(word),new IntWritable(1));	
		}
		
		
	}
 
 
}
