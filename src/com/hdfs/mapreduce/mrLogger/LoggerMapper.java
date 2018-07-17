package com.hdfs.mapreduce.mrLogger;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class LoggerMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable counter = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String result = handleLog(line);
        if(result!=null && result.length()>0){
            context.write(new Text(result),counter);
        }
    }

    /**
     * 日志处理
     */
    private String handleLog(String line){
        StringBuffer sBuffer = new StringBuffer();
        try{
            if(line.length()>0){
                if(line.indexOf("GET")>0){
                    String tmp = line.substring(line.indexOf("GET"),line.indexOf("HTTP/1.0"));
                    sBuffer.append(tmp.trim());
                }else if(line.indexOf("POST")>0 ){
                    String tmp = line.substring(line.indexOf("POST"),line.indexOf("HTTP/1.0"));
                    sBuffer.append(tmp.trim());
                }
            }else{
                return null;
            }

        }catch (Exception e) {
            e.printStackTrace();
            System.out.println(line);
        }
        return sBuffer.toString();
    }

}
