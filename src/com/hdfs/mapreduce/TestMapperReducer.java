package com.hdfs.mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class TestMapperReducer {
    public static void main(String[] args) throws Exception{
        Configuration config = new Configuration();
        //实例化一个job对象
        Job job = Job.getInstance(config);
        //设置job相关方法参数
        job.setJarByClass(TestMapperReducer.class);
        job.setMapperClass(StudentSexCountMapper.class);
        job.setReducerClass(StudentSexCountReduer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //指定输入输出路径的格式化
        FileInputFormat.setInputPaths(job, new Path("hdfs://106.15.191.61:9000/student.log"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://106.15.191.61:9000/studentsexcount"));
        //提交作业
        System.exit(job.waitForCompletion(true)?1:0);
    }
}
