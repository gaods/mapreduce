package com.yonyou.hadoop.mapreduce.countip;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.LongWritable.Comparator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
 
  public static class MapClass extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String pattern="[^\\w]";
    public void map(LongWritable key, Text value, 
                    OutputCollector<Text, IntWritable> output, 
                    Reporter reporter) throws IOException {
      String line = value.toString().toLowerCase();
      line = line.replaceAll(pattern, " ");
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        output.collect(word, one);
      }
    }
  }
  
  public static class Reduce extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, IntWritable> {
    
    public void reduce(Text key, Iterator<IntWritable> values,
                       OutputCollector<Text, IntWritable> output, 
                       Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }
    
  public int run(String[] args) throws Exception {
    
    Path tempDir = new Path("wordcount-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        JobConf conf = new JobConf(getConf(), WordCount.class);
        try {
            conf.setJobName("wordcount");

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(IntWritable.class);

            conf.setMapperClass(MapClass.class);
            conf.setCombinerClass(Reduce.class);
            conf.setReducerClass(Reduce.class);
             
//            conf.setInputPath(new Path(args[0]));
//            conf.setOutputPath(tempDir);
            
            conf.setOutputFormat(SequenceFileOutputFormat.class);
            
            JobClient.runJob(conf);

            JobConf sortJob = new JobConf(getConf(), WordCount.class);
            sortJob.setJobName("sort");

          //  sortJob.setInputPath(tempDir);
            sortJob.setInputFormat(SequenceFileInputFormat.class);

            sortJob.setMapperClass(InverseMapper.class);
            
            sortJob.setNumReduceTasks(1); 
           // sortJob.setOutputPath(new Path(args[1]));
            sortJob.setOutputKeyClass(IntWritable.class);
            sortJob.setOutputValueClass(Text.class);
            
            sortJob.setOutputKeyComparatorClass(IntWritableDecreasingComparator.class);
            JobClient.runJob(sortJob);
        } finally {
            FileSystem.get(conf).delete(tempDir);
        }
    return 0;
  }
  
  private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
           
      public int compare(WritableComparable a, WritableComparable b) {
        return -super.compare(a, b);
      }
      
      public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
          return -super.compare(b1, s1, l1, b2, s2, l2);
      }
  }
  
  
  public static void main(String[] args) throws Exception {
    if(args.length != 2){
      System.err.println("Usage: WordCount <input path> <output path>");
      System.exit(-1);
    }
    int res = ToolRunner.run(new Configuration(), new WordCount(), args);
    System.exit(res);
  }

}
