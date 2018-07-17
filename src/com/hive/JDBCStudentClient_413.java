package com.hive;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
public class JDBCStudentClient_413 {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://106.15.191.61:10000/duanyao";
    private static String user = "root";
    private static String password = "Lr715356";
    private static String sql = "";
    private static ResultSet res;
    public static void main(String[] args) {
        try {
            Class.forName(driverName);
            // Connection conn = DriverManager.getConnection(url, user,
            // password);
            // 默认使用端口10000, 使用默认数据库，用户名密码默认
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement stmt = conn.createStatement();
            // 创建的表名
            String tableName = "studentinfotable";
            /** 第一步:存在就先删除 **/
            sql = "drop table " + tableName;
            stmt.executeUpdate(sql);
            /** 第二步:不存在就创建 **/
            sql = "create table " + tableName
                    + " (stucode string,stuname string,stuage int,stusex string,stuaddr string, stuscore int)  row format delimited fields terminated by ' '";
            stmt.executeUpdate(sql);
            // 执行“load data into table”操作
            String filepath = "/hadooplearn/hiveLearn/duanyao/student.log";
            sql = "load data local inpath '" + filepath + "' into table " + tableName;
            stmt.executeUpdate(sql);
            // 执行“select * query”操作
            sql = "select * from " + tableName;
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行“select * query”运行结果:");
            while (res.next()) {
                System.out.println(res.getInt(1) + "\t" + res.getString(2));
            }
            conn.close();
            conn = null;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}