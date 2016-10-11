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
import org.apache.hadoop.mapred.JobConf;
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
 * 用一个并行计算任务显然是无法同时完成单词词频统计和排序的，这时我们可以利用 Hadoop 的任务管道能力，
 * 用上一个任务(词频统计)的输出做为下一个任务(排序)的输入，顺序执行两个并行计算任务。主要工作是修改代码清单3中的 run 函数，
 * 在其中定义一个排序任务并运行之。
	在 Hadoop 中要实现排序是很简单的，因为在 MapReduce 的过程中
	会把中间结果根据 key 排序并按 key 切成 R 份交给 R 个 Reduce 函数，而 Reduce 函数在处理中间结果之前也会有一个按 key 进行排序的过程，
	故 MapReduce 输出的最终结果实际上已经按 key 排好序。
	词频统计任务输出的 key 是单词，value 是词频，为了实现按词频排序，
	我们指定使用 InverseMapper 类作为排序任务的 Mapper 类( sortJob.setMapperClass(InverseMapper.class );)，
	这个类的 map 函数简单地将输入的 key 和 value 互换后作为中间结果输出，在本例中即是将词频作为 key,单词作为 value 输出, 
	这样自然就能得到按词频排好序的最终结果。我们无需指定 Reduce 类，Hadoop 会使用缺省的 IdentityReducer 类，将中间结果原样输出。
	还有一个问题需要解决: 排序任务中的 Key 的类型是 IntWritable, (sortJob.setOutputKeyClass(IntWritable.class)),
	 Hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排列。因此我们实现了一个 IntWritableDecreasingComparator 类,　
	 并指定使用这个自定义的 Comparator 类对输出结果中的 key (词频)进行排序：sortJob.setOutputKeyComparatorClass(IntWritableDecreasingComparator.class)
	详见代码清单 5 及其中的注释。

 * @author gaods
 *
 */

/**
 *  有10个文件，
 *  100w的ip数据记录，需要做第一个的输出是第二个的输入，不能同时加载10个文件
 *   
 * 
 * @author gaods
 *
 */


public class CountIPMapper {


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

                    System.out.println("=======================map process============================"+key);
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
                    System.out.println("=======================reduce process============================"+key);
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
                                    .println("=======================countmap process============================"+key);
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
                    System.out.println("=======================countreduce process ============================"+key);
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
            begin = dateFormater.parse("2016-9-27 12:10:27");
            end = dateFormater.parse("2016-9-27 16:10:27");
            
           
            
            Configuration conf = new Configuration();
            conf.set("mapred.job.tracker", "localhost:9000");
            conf.set("mapred.textoutputformat.ignoreseparator", "true");
            conf.set("mapred.textoutputformat.separator", ",");
            conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
            String[] ioArgs = new String[] {
                            "hdfs://localhost:9000/ipcount",
                            "hdfs://localhost:9000/count_middle", };
            String[] otherArgs = new GenericOptionsParser(conf, ioArgs)
                            .getRemainingArgs();
            if (otherArgs.length != 2) {
                    System.err.println("Usage: Data Deduplication <in> <out>");
                    System.exit(2);
            }

            String[] ioArgs2 = new String[] {
                            "hdfs://localhost:9000/count_middle",
                            "hdfs://localhost:9000/count_out", };

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
            //JobConf conf = new JobConf(getConf(), WordCount.class);
            
            Job job = new Job(conf, "middle count");
            job.setJarByClass(CountIPMapper.class);
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
            job2.setJarByClass(CountIPMapper.class);
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
