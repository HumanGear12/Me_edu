package com.atguigu.edu.realtime.common;

public class EduConfig {

    // Phoenix库名
    public static final String HBASE_SCHEMA = "EDU_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:192.168.12.122,192.168.12.123,192.168.12.124:2181";

    // kafka连接地址
    public static final String KAFKA_BOOTSTRAPS = "192.168.12.122:9092,192.168.12.123:9092,192.168.12.124:9092";


}
