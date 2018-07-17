package com.hdfs.mapreduce.uniquePocess;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DeduplicationReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //Reducer阶段直接按键输出即可，键直接可以实现去重
        context.write(key, new Text(""));
    }
}
