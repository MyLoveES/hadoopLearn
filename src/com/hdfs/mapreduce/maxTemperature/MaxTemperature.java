package com.hdfs.mapreduce.maxTemperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class MaxTemperature {
    public static void main(String[] args) throws Exception{
        //获取作业对象
        Job job = Job.getInstance(new Configuration());
        //设置主类
        job.setJarByClass(MaxTemperature.class);
        //设置job参数
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置job输入输出
        FileInputFormat.addInputPath(job, new Path("media/hdfs/maxTemperature/source.txt"));
        FileOutputFormat.setOutputPath(job, new Path("media/hdfs/maxTemperature/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
