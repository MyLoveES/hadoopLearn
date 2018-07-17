package com.hdfs.mapreduce.studentWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
public class StuPartitioner extends Partitioner<NullWritable, StudentWritable> {
    @Override
    public int getPartition(NullWritable key, StudentWritable value,
                            int numPartitions) {
        //按年龄进行分区，分区条件为大于18岁和小于18岁
        if(value.getAge()>=18){
            return 1;
        }else{
            return 0;
        }
    }
}
