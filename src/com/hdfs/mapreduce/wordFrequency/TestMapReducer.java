package com.hdfs.mapreduce.wordFrequency;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class TestMapReducer {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        //conf.set("fs.default.name", "hdfs://192.168.0.202:9000");
        // step1 : get a job
        Job job = Job.getInstance(conf);
        //step2: set jar main class
        job.setJarByClass(TestMapReducer.class);
        //step3: set map class and redcer class
        job.setMapperClass(WordFrequencyMapper.class);
        job.setReducerClass(WordFrequencyReducer.class);
        //step4: set map reduce output type
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //step5: set key/value output file format and input/output path
        FileInputFormat.setInputPaths(job, new Path("media/hdfs/wordFrequency/source.txt"));
        FileOutputFormat.setOutputPath(job, new Path("media/hdfs/wordFrequency/output"));
        //step6: commit job
        job.waitForCompletion(true);
    }
}
