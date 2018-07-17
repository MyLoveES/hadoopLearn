package com.hdfs.mapreduce.uniquePocess;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class DeduplicationMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //按行读取信息并作为mapper的输出键，mapper的输出值置为空文本即可
        Text line = value;
        context.write(line, new Text(""));
    }
}
