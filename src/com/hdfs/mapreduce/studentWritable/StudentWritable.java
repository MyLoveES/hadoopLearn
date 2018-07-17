package com.hdfs.mapreduce.studentWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
public class StudentWritable implements WritableComparable<StudentWritable> {
    private String name;
    private int age;
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public int getAge() {
        return age;
    }
    public void setAge(int age) {
        this.age = age;
    }
    public StudentWritable(){

    }
    public StudentWritable(String name,int age){
        this.name = name;
        this.age = age;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(age);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.age = in.readInt();
    }
    @Override
    public String toString() {
        return "StudentWritable [name=" + name + ", age=" + age + "]";
    }
    @Override
    public int compareTo(StudentWritable o) {
        return 1;
    }
}
