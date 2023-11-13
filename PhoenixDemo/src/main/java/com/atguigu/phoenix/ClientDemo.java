package com.atguigu.phoenix;

import java.sql.*;


/**
 * Created by Smexy on 2023/11/7
 */
public class ClientDemo
{
    public static void main(String[] args) throws SQLException {
        // 1.添加链接
        String url = "jdbc:phoenix:hadoop122:2181";
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
