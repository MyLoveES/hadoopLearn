package com.hdfs.mapreduce.wordFrequency;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class WordFrequencyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> v2s,
                          Reducer<Text, LongWritable, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        Iterator<LongWritable> it = v2s.iterator();
        //define var sum
        long sum = 0;
        // iterator count arr
        while(it.hasNext()){
            sum += it.next().get();
        }
        context.write(key, new LongWritable(sum));
    }
}
