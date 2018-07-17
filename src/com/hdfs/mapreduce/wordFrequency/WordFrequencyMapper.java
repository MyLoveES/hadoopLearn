package com.hdfs.mapreduce.wordFrequency;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class WordFrequencyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        //get values string
        String valueString = value.toString();
        //spile string
        String wArr[] = valueString.split(" ");
        //for iterator
        for(int i=0;i<wArr.length;i++){
            //map out key/value
            context.write(new Text(wArr[i]), new LongWritable(1));
        }

    }
}
