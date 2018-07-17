package com.hdfs.mapreduce.studentWritable;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
public class StudentRedcer extends Reducer<NullWritable, StudentWritable, NullWritable, StudentWritable>{
    @Override
    protected void reduce(
            NullWritable key,
            Iterable<StudentWritable> iter,
            Reducer<NullWritable, StudentWritable, NullWritable, StudentWritable>.Context context)
            throws IOException, InterruptedException {
        //遍历数据
        Iterator<StudentWritable> it=  iter.iterator();
        while(it.hasNext()){
            context.write(NullWritable.get(), it.next());
        }
    }
}

