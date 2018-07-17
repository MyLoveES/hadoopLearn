package com.hdfs.mapreduce.intSort;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
public class IntSortReducer extends
        Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {
    //使用num作为控制打印序列的编号变量
    IntWritable num = new IntWritable(1);
    public void reduce(IntWritable key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        //Reducer阶段直接按键输出即可，键直接可以实现去重
        context.write(num, key);
        //为排序后的每个整数值按次序指定编号
        num = new IntWritable(num.get()+1);
    }
}
