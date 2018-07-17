package com.hdfs.mapreduce.uniquePocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class TestDeduplication {
    public static void main(String[] args) throws Exception{
        //获取作业对象
        Job job = Job.getInstance(new Configuration());
        //设置主类
        job.setJarByClass(TestDeduplication.class);
        //设置job参数
        job.setMapperClass(DeduplicationMapper.class);
        job.setReducerClass(DeduplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置job输入输出
        FileInputFormat.addInputPath(job, new Path("media/hdfs/uniqueProcess/source.txt"));
        FileOutputFormat.setOutputPath(job, new Path("media/hdfs/uniqueProcess/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
