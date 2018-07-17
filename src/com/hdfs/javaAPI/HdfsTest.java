package com.hdfs.javaAPI;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Test;
import org.junit.Before;

public class HdfsTest {
    private FileSystem fs = null;
    private List<String> hdfsPathsLists;

    @Before
    public void init() throws Exception{
        Configuration con = new Configuration();
        con.setBoolean("dfs.support.append", true);
        fs = FileSystem.get(new URI("hdfs://106.15.191.61:9000"), con, "root");
    }

    @Test
    public void testexistsFile() throws Exception{
        String src = "hdfs://106.15.191.61:9000/windowsTest.txt";
        boolean flag = fs.exists(new Path(src));
        if(!flag){
            fs.createNewFile(new Path(src));
            System.out.println("Create succeed");
        }
        else{
            System.out.println("is exists");
        }
    }

    @Test
    public void testUploadFile() throws Exception{
        String src = "media/Shijiebei.png";
        String hdfsDst = "/";
        boolean flag = fs.exists(new Path(src));
        if(!flag){
            //fs.createNewFile(new Path(src));
            fs.copyFromLocalFile(new Path(src), new Path(hdfsDst));
            System.out.println("upload succeed");
        }
        else{
            System.out.println("is exists");
        }
    }

    @Test
    public void testDownloadFile() throws Exception{
        String src= "/duanyao/hadoop/studentsexcount/_SUCCESS";
        String hdfsDst="E:/IdeaProjects/hadoop/media";
        fs.copyToLocalFile(new Path(src), new Path(hdfsDst));
        System.out.println("downLoad success");
    }

    @Test
    public void testMkdir() throws Exception {
        boolean flag = fs.mkdirs(new Path("/duanyao/hadoop/mkdirTest"));
        System.out.println(flag ? "succeed" : "failed");
    }

    @Test
    public void testfilesFile() throws Exception {
        String src = "/duanyao/hadoop/In.txt";
        boolean b = fs.isDirectory(new Path(src));
        if(b){
            System.out.println("It's directory!");
        }else if(fs.isFile(new Path(src))){
            System.out.println("It's file!");
        }else{
            System.out.println("I don't know!");
        }
    }

    @Test
    public void testrenameFile() throws Exception{
        String src = "/duanyao/hadoop/Init.txt";
        String hdfsDst = "/duanyao/hadoop/In.txt";
        fs.rename(new Path(src), new Path(hdfsDst));
        System.out.println("rename sucess");
    }

    /**
     * 对hdfs上的文件进行移动到本地
     */
    @Test
    public void testmovetolocalFile() throws Exception {
        String src = "/duanyao/hadoop/In.txt";
        String desc = "media/";
        fs.moveToLocalFile(new Path(src), new Path(desc));
        System.out.println("move down succeed");
    }
    /**
     * 对本地的文件移动到hdfs上
     */
    @Test
    public void testmovetohdfsFile() throws Exception {
        String src = "media/Shijiebei.png";
        String desc = "/duanyao/hadoop/";
        fs.moveFromLocalFile(new Path(src), new Path(desc));
        System.out.println("move up succeed");
    }

    @Test
    public void getDirList() throws Exception{
        //初始化存放目录的List
        hdfsPathsLists = new ArrayList<String>();
        //调用获取目录方法
        getHdfsPaths(new Path("/"));
        //循环输入结果
        for(String p:hdfsPathsLists){
            System.out.println(p);
        }
    }
    /**
     * 递归方法便利获取目录及目录下文件
     * @param path 要获取的目录
     * @throws Exception 异常
     */
    private void getHdfsPaths(Path path) throws Exception{
        FileStatus[] dirs = fs.listStatus(path);
        for (FileStatus s : dirs){
            hdfsPathsLists.add(s.getPath().toString());
            if(s.isDirectory()){
                getHdfsPaths(s.getPath());
            }
        }
    }
    @After
    public void close()throws Exception{
        fs.close();
    }


}
