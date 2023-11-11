package com.atguigu.edu.realtime.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.junit.Test;

import java.sql.SQLException;

public class DruidTest {


    @Test
    public void test1(){
        DruidDataSource druidDataSource = DruidDSUtil.getDruidDataSource();
        try {
            DruidPooledConnection connection = druidDataSource.getConnection();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            System.out.println("连接池获取连接异常");
        }
    }
}
