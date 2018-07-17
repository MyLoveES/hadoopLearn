package com.hdfs.mapreduce.minTemperature;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class MinTemperatureReducer extends Reducer
        <Text, IntWritable, Text, IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context)
            throws IOException, InterruptedException {
        //把Integer.Max_VALUE作为minValue的初始值
        int minValue = Integer.MAX_VALUE;
        //循环取出最大值
        for(IntWritable value: values){
            minValue = Math.min(minValue, value.get());
        }
        context.write(key, new IntWritable(minValue));
    }
}

