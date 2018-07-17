package com.hdfs.mapreduce.studentSexCount;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StudentSexCountReduer extends Reducer<Text, Text, Text, NullWritable> {
    static Integer malecount = 0;  //统计男性计数变量
    static Integer femalecount = 0; //统计女性计数变量
    public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //遍历统计男生个数和女生个数
        for (Text val : values) {
            if(val.toString().equals("male")){
                malecount+=1;
            }else{
                femalecount+=1;
            }
        }
    }
    @Override
    protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        //把男生个数统计结果和女生个数统计结果作为reduce的键值对输出

        context.write(new Text("male:"+malecount.toString()+",female:"+femalecount.toString()), NullWritable.get());
    }
}
