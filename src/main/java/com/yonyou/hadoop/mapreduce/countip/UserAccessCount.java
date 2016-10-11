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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 作者 梁臣
 * @author  
 *
 */

public class UserAccessCount {

        private static final Logger logger = LoggerFactory
                        .getLogger(UserAccessCount.class);

        public static Date begin = null;
        public static Date end = null;
        public static SimpleDateFormat dateFormater = new SimpleDateFormat(
                        "yyyy-MM-dd hh:mm:ss");

        public static class UserAccessMap extends
                        Mapper<Object, Text, Text, IntWritable> {
                private static Text line = new Text();
                private final static IntWritable one = new IntWritable(1);

                public void map(Object key, Text value, Context context)
                                throws IOException, InterruptedException {

                        System.out.println("=======================map process============================");
                        InputSplit inputSplit = context.getInputSplit();
                        String fileFullName = ((FileSplit) inputSplit).getPath().getName();
                        String[] data = value.toString().split(",");
                        Date date = null;
                        try {
                                date = dateFormater.parse(data[2]);
                        } catch (ParseException e) {
                                e.printStackTrace();
                        }
                        if (date == null || date.getTime() < begin.getTime()
                                        || date.getTime() > end.getTime()) {
                                return;
                        }
                        line.set(fileFullName + "_" + data[0]);
                        context.write(line, one);
                }
        }

        public static class Reduce extends
                        Reducer<Text, IntWritable, Text, IntWritable> {
                private MultipleOutputs<Text, IntWritable> mos;
                private IntWritable result = new IntWritable();

                protected void setup(Context context) throws IOException,
                                InterruptedException {
                        super.setup(context);
                        mos = new MultipleOutputs<Text, IntWritable>(context);
                }

                public void reduce(Text key, Iterable<IntWritable> values,
                                Context context) throws IOException, InterruptedException {
                        System.out.println("=======================reduce process============================");
                        int sum = 0;
                        for (IntWritable val : values) {
                                sum += val.get();
                        }
                        result.set(sum);
                        String[] keys = key.toString().split("_");
                        String outFileName = getFileNameNoEx(keys[0]);
                        outFileName = outFileName + ".out";
                        mos.write(new Text(keys[1]), result, outFileName);
                }

                protected void cleanup(Context context) throws IOException,
                                InterruptedException {
                        super.cleanup(context);
                       
                        mos.close();
                }
        }

        public static class CountMap extends
                        Mapper<Object, Text, Text, IntWritable> {
                private static Text line = new Text();
                private final static IntWritable result = new IntWritable(0);

                public void map(Object key, Text value, Context context)
                                throws IOException, InterruptedException {

                        System.out
                                        .println("=======================countmap process============================");
                        String[] values = value.toString().split(",");
                        line.set(values[0]);
                        result.set(Integer.valueOf(values[1]).intValue());
                        context.write(line, result);
                }
        }

        public static class CountReduce extends
                        Reducer<Text, IntWritable, Text, IntWritable> {
                private IntWritable result = new IntWritable();

                public void reduce(Text key, Iterable<IntWritable> values,
                                Context context) throws IOException, InterruptedException {
                        System.out.println("=======================countreduce process ============================");
                        int sum = 0;
                        for (IntWritable val : values) {
                                sum += val.get();
                        }
                        result.set(sum);
                        context.write(key, result);
                }

        }

        public static class FileNameOutputFormat extends
                        MultipleTextOutputFormat<Text, IntWritable> {

        }

        public static void main(String[] args) throws Exception {
                begin = dateFormater.parse(args[0]);
                end = dateFormater.parse(args[1]);
                Configuration conf = new Configuration();
                conf.set("mapred.job.tracker", "172.20.13.53:9000");
                conf.set("mapred.textoutputformat.ignoreseparator", "true");
                conf.set("mapred.textoutputformat.separator", ",");
                conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
                String[] ioArgs = new String[] {
                                "hdfs://172.20.13.53:9000/user/hadoop1/count_in",
                                "hdfs://172.20.13.53:9000/user/hadoop1/count_middle", };
                String[] otherArgs = new GenericOptionsParser(conf, ioArgs)
                                .getRemainingArgs();
                if (otherArgs.length != 2) {
                        System.err.println("Usage: Data Deduplication <in> <out>");
                        System.exit(2);
                }

                String[] ioArgs2 = new String[] {
                                "hdfs://172.20.13.53:9000/user/hadoop1/count_middle",
                                "hdfs://172.20.13.53:9000/user/hadoop1/count_out", };

                Path in = new Path(ioArgs[0]);
                Path out = new Path(ioArgs[1]);
                FileSystem fileSystem = FileSystem.get(new URI(in.toString()), conf);
                if (fileSystem.exists(out)) {
                        fileSystem.delete(out, true);
                }

                Path out2 = new Path(ioArgs2[1]);
                if (fileSystem.exists(out2)) {
                        fileSystem.delete(out2, true);
                }

                Job job = new Job(conf, "middle count");
                job.setJarByClass(UserAccessCount.class);
                job.setMapperClass(UserAccessMap.class);
                job.setCombinerClass(Reduce.class);
                job.setReducerClass(Reduce.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
                job.waitForCompletion(true);
                System.out.println("middle运算");
               
                Job job2 = new Job(conf, "count");
                job2.setJarByClass(UserAccessCount.class);
                job2.setMapperClass(CountMap.class);
                job2.setReducerClass(CountReduce.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(IntWritable.class);
                LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
                FileInputFormat.addInputPath(job2, new Path(ioArgs2[0]));
                FileOutputFormat.setOutputPath(job2, new Path(ioArgs2[1]));
                job2.waitForCompletion(true);
                System.out.println("count运算");

                System.exit(0);
        }

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