package com.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;
import java.util.Date;
/**
 * Handle data through hive on eclipse
 */
public class HiveJdbcClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://106.15.191.61:10000/duanyao";
    private static String user = "root";
    private static String password = "!Lr715356";
    private static String sql = "";
    private static ResultSet res;
    private static final Logger log = Logger.getLogger(HiveJdbcClient.class);
    public static void main(String[] args) {
        try {
            Date date = new Date();
            Class.forName(driverName);
            //Connection conn = DriverManager.getConnection(url, user, password);
            //默认使用端口10000, 使用默认数据库，用户名密码默认
            Connection conn = DriverManager.getConnection(url, user,password);
            Statement stmt = conn.createStatement();
            //System.out.println("conn success!");
            // 创建的表名
            String tableName = "testHiveDriverTable";
            sql = "drop TABLE "+tableName;
            stmt.execute(sql);
            //创建
            sql = "create table " + tableName + " (key int, value string)  row format delimited fields terminated by '\t'";
            stmt.executeUpdate(sql);
            // 执行“show tables”操作
            sql = "show tables '" + tableName + "'";
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行“show tables”运行结果:");
            if (res.next()) {
                System.out.println(res.getString(1));
            }
            // 执行“describe table”操作
            sql = "describe " + tableName;
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行“describe table”运行结果:");
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }
            // 执行“load data into table”操作
            String filepath = "/hadooplearn/hiveLearn/duanyao/userinfo.txt";
            sql = "load data local inpath '" + filepath + "' into table " + tableName;
            System.out.println("Running:" + sql);
            stmt.executeUpdate(sql);
            // 执行“select * query”操作
            sql = "select * from " + tableName;
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行“select * query”运行结果:");
            while (res.next()) {
                System.out.println(res.getInt(1) + "\t" + res.getString(2));
            }
            // 执行“regular hive query”操作
            sql = "select count(1) from " + tableName;
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行“regular hive query”运行结果:");
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            Date date1 = new Date();
            System.out.println(date1.getTime()-date.getTime());
            conn.close();
            conn = null;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.error(driverName + " not found!", e);
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("Connection error!", e);
            System.exit(1);
        }
    }
}
