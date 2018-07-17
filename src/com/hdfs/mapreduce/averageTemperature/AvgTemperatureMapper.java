package com.hdfs.mapreduce.averageTemperature;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class AvgTemperatureMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;
    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();//读取一条记录
        String year = line.substring(15, 19);//获取温度数
        System.out.println("year=="+year);
        int airTemperature;
        if (line.charAt(45) == '+') { //判断温度正负
            airTemperature = Integer.parseInt(line.substring(46, 50));
        } else {
            airTemperature = Integer.parseInt(line.substring(45, 50));
        }
        String quality = line.substring(50, 51);
        System.out.println("quality: " + quality);
        //判断温度是否异常
        if (airTemperature != MISSING && quality.matches("[01459]")) {
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }
}
