package com.atguigu.edu.realtime.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.junit.Test;

import java.sql.SQLException;

public class DruidTest {


    @Test
    public void test1() throws SQLException {
        DruidDataSource druidDataSource = DruidDSUtil.getDruidDataSource();

        DruidPooledConnection connection = druidDataSource.getConnection();

        System.out.println(connection);
    }

    }

