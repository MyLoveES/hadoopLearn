package com.hdfs.mapreduce.studentWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class TestStuMapReducer {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        //conf.set("fs.default.name", "hdfs://192.168.0.xxx:9000");
        // step1 : get a job
        Job job = Job.getInstance(conf);
        //step2: set jar main class
        job.setJarByClass(TestStuMapReducer.class);
        //step3: set map class and redcer class
        job.setMapperClass(StudentMapper.class);
        job.setReducerClass(StudentRedcer.class);
        job.setPartitionerClass(StuPartitioner.class);
        job.setNumReduceTasks(2);

        //step4: set map reduce output type
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(StudentWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(StudentWritable.class);
        //step5: set key/value output file format and input/output path
        FileInputFormat.setInputPaths(job, new Path("media/hdfs/studentWritable/source.txt"));
        FileOutputFormat.setOutputPath(job, new Path("media/hdfs/studentWritable/output"));
        //CombineFileInputFormat.setInputPaths(job, inputPaths);
        //step6: commit job
        job.waitForCompletion(true);
    }
}
