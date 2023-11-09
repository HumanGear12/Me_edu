package com.atguigu.edu.realtime.common;

public class EduConfig {

    // Phoenix库名
    public static final String HBASE_SCHEMA = "EDU_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop122,hadoop123,hadoop124:2181";

    // kafka连接地址
    public static final String KAFKA_BOOTSTRAPS = "hadoop122:9092,hadoop123:9092,hadoop124:9092";


}
