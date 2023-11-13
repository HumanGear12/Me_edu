package com.atguigu.edu.realtime.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.atguigu.edu.realtime.common.EduConfig;
import org.junit.Test;

import java.sql.*;

public class DruidTest {


    @Test
    public void test1() throws SQLException {
        DruidDataSource druidDataSource = DruidDSUtil.getDruidDataSource();

        DruidPooledConnection connection = druidDataSource.getConnection();

        System.out.println(connection);
    }

    @Test
    public void test2(){
        try {
            // Register the Phoenix JDBC driver
            Class.forName(EduConfig.PHOENIX_DRIVER);

            // Create a connection to Phoenix
            Connection connection = DriverManager.getConnection(EduConfig.PHOENIX_SERVER);

            // Create a statement
            Statement statement = connection.createStatement();

            // Execute a query
            ResultSet resultSet = statement.executeQuery("SELECT * FROM SYSTEM.TASK");

            // Process the result set
            while (resultSet.next()) {
                // Handle each row
                String column1Value = resultSet.getString("column1");
                String column2Value = resultSet.getString("column2");
                // Add more columns as needed
                System.out.println("Column1: " + column1Value + ", Column2: " + column2Value);
            }

            // Close resources
            resultSet.close();
            statement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void test3() throws SQLException {
        // 1.添加链接
        String url = "jdbc:phoenix:192.168.12.122:2181";
        // 2.获取连接
        Connection connection = DriverManager.getConnection(url);
        // 3.编译SQL语句
        PreparedStatement preparedStatement = connection.prepareStatement("select * from student");
        // 4.执行语句
        ResultSet resultSet = preparedStatement.executeQuery();
        // 5.输出结果
        while (resultSet.next()){
            System.out.println(resultSet.getString(1) + ":" + resultSet.getString(2) + ":" + resultSet.getString(3));
        }
        // 6.关闭资源
        connection.close();
    }

    }

