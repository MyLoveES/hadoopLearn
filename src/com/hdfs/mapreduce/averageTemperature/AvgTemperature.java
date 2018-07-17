package com.hdfs.mapreduce.averageTemperature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class AvgTemperature {
    public static void main(String[] args) throws Exception{
        //获取作业对象
        Job job = Job.getInstance(new Configuration());
        //设置主类
        job.setJarByClass(AvgTemperature.class);
        //设置job参数
        job.setMapperClass(AvgTemperatureMapper.class);
        job.setReducerClass(AvgTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置job输入输出
        FileInputFormat.addInputPath(job, new Path("media/hdfs/averageTemperature/source.txt"));
        FileOutputFormat.setOutputPath(job, new Path("media/hdfs/averageTemperature/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
