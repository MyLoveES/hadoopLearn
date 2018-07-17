package com.hdfs.mapreduce.averageTemperature;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class AvgTemperatureReducer extends Reducer
        <Text, IntWritable, Text, IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context)
            throws IOException, InterruptedException {
        //声明变量sumValue作为年温度和
        int sumValue = 0;
        //声明count作为统计同一年温度记录的次数
        int count = 0;
        //循环求同一年所有温度的和及温度记录次数
        for(IntWritable value: values){
            sumValue+=value.get();
            count++;
        }
        int avgValue = sumValue/count;
        context.write(key, new IntWritable(avgValue));
    }
}
