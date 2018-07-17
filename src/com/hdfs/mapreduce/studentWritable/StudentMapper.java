package com.hdfs.mapreduce.studentWritable;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class StudentMapper extends Mapper<LongWritable, Text, NullWritable, StudentWritable> {
    @Override
    protected void map(
            LongWritable key,
            Text value,
            Mapper<LongWritable, Text, NullWritable, StudentWritable>.Context context)
            throws IOException, InterruptedException {
        String stuArr[] = value.toString().split(" ");
        context.write(NullWritable.get(), new StudentWritable(stuArr[0],Integer.parseInt(stuArr[1])));
    }
}
