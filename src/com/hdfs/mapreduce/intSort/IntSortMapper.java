package com.hdfs.mapreduce.intSort;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class IntSortMapper extends
        Mapper<LongWritable, Text, IntWritable, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, IntWritable, NullWritable>.Context context)
            throws IOException, InterruptedException {
        //按行读取整数信息并作为mapper的输出键，mapper的输出值置为空即可
        context.write(new IntWritable(Integer.valueOf(value.toString())),
                NullWritable.get());
    }
}
