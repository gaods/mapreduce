package com.yonyou.hadoop.mapreduce.countip;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * @author gaods
 *
 */

public class CountIPApp {

	public static SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public static Date begin = null;
	public static Date end = null;
	private static int count=0;
	public static class MapClass extends Mapper<Object, Text, Text, IntWritable> {

		private static Text line = new Text();
		private final static IntWritable one = new IntWritable(1);
		private static int count = 0;
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// System.out.println("=======================map
			// process============================"+count++);
			InputSplit inputSplit = context.getInputSplit();
			String fileFullName = ((FileSplit) inputSplit).getPath().getName();
			String[] data = value.toString().split(",");
			Date date = null;
			try {
				date = dateFormater.parse(data[2]);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			if (date == null || date.getTime() < begin.getTime() || date.getTime() > end.getTime()) {
				return;
			}
			line.set(fileFullName + "_" + data[0]);  
			context.write(line, one);
		}
	}

	public static class CombineClass extends Reducer<Text, IntWritable, Text, IntWritable>  {

		private MultipleOutputs<Text, IntWritable> mos;
		private IntWritable result = new IntWritable();
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		 
			super.setup(context);
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//System.out.println("=======================CombineClass process============================"+count++);
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			String[] keys = key.toString().split("_");
 			String outFileName = getFileNameNoEx(keys[0]);
 			outFileName = outFileName + ".out";
 			//mos.write(new Text(keys[1]), result, outFileName);
			
			context.write(new Text(keys[1]), result);
		}

		@Override
        protected void cleanup(Context context) throws IOException,
                        InterruptedException {
                super.cleanup(context);
                mos.close();
        }

	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private MultipleOutputs<Text, IntWritable> mos;
		private IntWritable result = new IntWritable();
		private int count=0;
		
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			//System.out.println("=======================reduce process============================settup");
			super.setup(context);
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//System.out.println("=======================reduce process============================"+key+" ::"+count++);
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			String[] keys = key.toString().split("_");
			String outFileName = getFileNameNoEx(keys[0]);
			outFileName = outFileName + ".out";
		//	mos.write(new Text(keys[1]), result, outFileName);
			
		   context.write(key, result);
		}
		@Override
        protected void cleanup(Context context) throws IOException,
                        InterruptedException {
                super.cleanup(context);
                mos.close();
        }

		 
	}
	
	
	
	public static class SumMapClass extends Mapper<Object, Text, IntWritable, Text> {

		
		 
	  
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			  Text result = new Text();

            System.out.println("=======================countmap process============================");
            String[] values = value.toString().split(",");
            IntWritable line = new IntWritable(0);
            line.set(Integer.valueOf(values[1]).intValue());
            
            result.set(values[0]);
            context.write(line, result);
		 
		}
	}
	
	
	public static class SumReduce extends Reducer<IntWritable,Text, Text, IntWritable> {
	 
		private IntWritable result = new IntWritable();
		private int count=0; 
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("=======================SumReduce process============================settup");
			super.setup(context);
		 
		}
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("=======================SumReduce process============================"+key+" ::");
			if(count++>10)
				return;
			for (Text val : values) {
				 context.write(val,key );;
			}
		  
		}

        protected void cleanup(Context context) throws IOException,
                        InterruptedException {
                super.cleanup(context);
                
        } 
	}
	
	public static class IntWritableDecreasingComparator extends Comparator {
	    @SuppressWarnings("rawtypes")
	    public int compare( WritableComparable a,WritableComparable b){
	        return -super.compare(a, b);
	    }
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	        return -super.compare(b1, s1, l1, b2, s2, l2);
	    }
	}
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		try {
			begin = dateFormater.parse("2016-9-27 12:10:27");
			end = dateFormater.parse("2016-9-27 16:10:27");

			conf.set("mapred.job.tracker", "localhost:9000");
			conf.set("mapred.textoutputformat.ignoreseparator", "true");
			conf.set("mapred.textoutputformat.separator", ",");
			conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
			String[] ioArgs = new String[] { "hdfs://localhost:9000/ipcount", "hdfs://localhost:9000/count_ip", };

			// 创建 job对象
			// Job job = Job.getInstance(conf, "CountIPApp");
			Path in = new Path(ioArgs[0]);
			Path out = new Path(ioArgs[1]);
			FileSystem fileSystem = FileSystem.get(new URI(in.toString()), conf);
			if (fileSystem.exists(out)) {
				fileSystem.delete(out, true);
			}

			Job job = new Job(conf, "CountIPApp");
			
			job.setJarByClass(CountIPApp.class);

			// 设置mapper类
			job.setMapperClass(MapClass.class);
			job.setCombinerClass(CombineClass.class);
			// 设置reducer类
			job.setReducerClass(Reduce.class);

			// 设置map输出的key。value
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			FileInputFormat.addInputPath(job, new Path(ioArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(ioArgs[1]));
			job.waitForCompletion(true);

			 //Thread.sleep(2000);
		 
			//汇总的数据后 要 取出前10个
			
			String[] outArgs = new String[] { "hdfs://localhost:9000/count_ip", "hdfs://localhost:9000/sum_ip", };
			// 创建 job对象
			// Job job = Job.getInstance(conf, "CountIPApp");
		 
			Path out2 = new Path(outArgs[1]);
			FileSystem fileSystem2 = FileSystem.get(new URI(in.toString()), conf);
			if (fileSystem2.exists(out2)) {
				fileSystem.delete(out2, true);
			}
			

            Job job2 = new Job(conf, "sum_ip");
            job2.setJarByClass(CountIPApp.class);
            job2.setMapperClass(SumMapClass.class);
            job2.setReducerClass(SumReduce.class);
            
            
//            //设置map输出的key。value
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(Text.class);

	        job2.setSortComparatorClass(IntWritableDecreasingComparator.class);
//			job2.setOutputKeyClass(Text.class);
//			job2.setOutputValueClass(IntWritable.class);
            LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
            FileInputFormat.addInputPath(job2, new Path(outArgs[0]));
            FileOutputFormat.setOutputPath(job2, new Path(outArgs[1]));
            job2.waitForCompletion(true);
            System.out.println("count运算");
			
			
			

		} finally {

		}

	}

	// @Override
	// public int run(String[] arg0) throws Exception {
	//
	// return 0;
	// }

	/**
	 * jobconf 是老类 job是新类
	 */
	public static String getFileNameNoEx(String filename) {
		if ((filename != null) && (filename.length() > 0)) {
			int dot = filename.lastIndexOf('.');
			if ((dot > -1) && (dot < (filename.length()))) {
				return filename.substring(0, dot);
			}
		}
		return filename;
	}

}
