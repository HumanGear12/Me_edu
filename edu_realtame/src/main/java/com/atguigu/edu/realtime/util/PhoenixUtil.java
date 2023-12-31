package com.atguigu.edu.realtime.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.EduConfig;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;


public class PhoenixUtil {
    private static DruidDataSource druidDataSource = DruidDSUtil.getDruidDataSource();

    public static void executeDDL(String sqlString){

        System.out.println("executeDDL........Start");
        DruidPooledConnection connection = null;
        //Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = druidDataSource.getConnection();
            //Class.forName(EduConfig.PHOENIX_DRIVER);
            //connection = DriverManager.getConnection(EduConfig.PHOENIX_SERVER);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            System.out.println("连接池获取连接异常");
        }


        try {
            preparedStatement = connection.prepareStatement(sqlString);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            System.out.println("编译sql异常");
        }

        try {
            preparedStatement.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            System.out.println("建表语句错误");
        }

        // 关闭资源
        try {
            preparedStatement.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try {
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }

    /**
     * DML 语句执行方法，通过预编译传参的方式避免 SQL 注入现象
     * conn 连接对象
     * sinkTable 目标表
     * jsonObj 数据对象
     */
    public static void executeDML(String sinkTable, JSONObject jsonObject) {
        System.out.println("executeDML........Start");

        //  拼接sql语言
        StringBuilder sql = new StringBuilder();
        Set<Map.Entry<String, Object>> entries = jsonObject.entrySet();
        ArrayList<String> columns = new ArrayList<>();
        ArrayList<Object> values = new ArrayList<>();
        StringBuilder symbols = new StringBuilder();
        for (Map.Entry<String, Object> entry : entries) {
            columns.add(entry.getKey());
            values.add(entry.getValue());
            symbols.append("?,");
        }

        sql.append("upsert into " + EduConfig.HBASE_SCHEMA + "." + sinkTable + "(");

        // 拼接列名
        String columnsStrings = StringUtils.join(columns, ",");
        String symbolStr = symbols.substring(0, symbols.length() - 1).toString();
        sql.append(columnsStrings)
                .append(")values(")
                .append(symbolStr)
                .append(")");
        System.out.println(sql);

        DruidPooledConnection connection = null;
        //Connection connection = null;
        try {
            connection = druidDataSource.getConnection();

            //connection = DriverManager.getConnection(EduConfig.PHOENIX_SERVER);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            System.out.println("连接池获取连接异常");
        }

        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql.toString());
            // 传入参数
            for (int i = 0; i < values.size(); i++) {
                preparedStatement.setObject(i + 1,values.get(i) + "" );
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            System.out.println("编译sql异常");
        }


        try {
            preparedStatement.executeUpdate();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            System.out.println("写入phoenix错误");
        }

        try {
            preparedStatement.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try {
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        System.out.println("executeDML........Done");
    }


}
