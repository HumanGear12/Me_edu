package com.atguigu.edu.realtime.util;

import com.alibaba.druid.pool.DruidDataSource;
import org.junit.Test;

import java.sql.SQLException;

public class DruidTest {


    @Test
    public void test1(){
        DruidDataSource druidDataSource = DruidDSUtil.getDruidDataSource();
        System.out.println(druidDataSource);
    }
}
