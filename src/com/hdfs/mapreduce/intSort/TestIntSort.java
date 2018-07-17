package com.hdfs.mapreduce.intSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class TestIntSort {
    public static void main(String[] args) throws Exception{
        //获取作业对象
        Job job = Job.getInstance(new Configuration());
        //设置主类
        job.setJarByClass(TestIntSort.class);
        //设置job参数
        job.setMapperClass(IntSortMapper.class);
        job.setReducerClass(IntSortReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        //设置job输入输出
        //FileInputFormat.addInputPath(job, new Path("file:///simple/source.txt"));
        FileInputFormat.setInputPaths(job, new Path("media/hdfs/intSort/source.txt"));
        FileOutputFormat.setOutputPath(job, new Path("media/hdfs/intSort/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
