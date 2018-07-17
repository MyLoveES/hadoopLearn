package com.hdfs.mapreduce.maxTemperature;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class MaxTemperatureReducer extends Reducer
        <Text, IntWritable, Text, IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context)
            throws IOException, InterruptedException {
        //把Integer.MIN_VALUE = 2147483847作为maxValue的初始值
        int maxValue = Integer.MIN_VALUE;
        //循环取出最大值
        for(IntWritable value: values){
            maxValue = Math.max(maxValue, value.get());
        }
        context.write(key, new IntWritable(maxValue));
    }
}
