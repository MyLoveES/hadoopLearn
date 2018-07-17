package com.hdfs.mapreduce.studentSexCount;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class StudentSexCountMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
        //读取文件中的一行数据信息
        String lineMsg = ivalue.toString();
        //按照指定规则拆分读取的行数据并存放到数组中
        String[] arr = lineMsg.split(" ");
        //把学员编号和性别作为map阶段的输出
        context.write(new Text(arr[0]),new Text(arr[3]));
    }
}
