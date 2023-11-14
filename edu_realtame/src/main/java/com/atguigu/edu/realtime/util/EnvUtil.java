package com.atguigu.edu.realtime.util;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class EnvUtil {

    //创建flink运行环境的方法
    public static StreamExecutionEnvironment getExecutionEnvironment(Integer parallelism){

        //创建flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(parallelism);

        //设置状态后端
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.12.122:8020/edu/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        return env;

    }


    /**
     * Flink SQL 动态表状态存活时间设置
     * @param tableEnv 表处理环境
     * @param ttl 状态存活时间
     */
    public static void setTableEnvStateTtl(StreamTableEnvironment tableEnv, String ttl) {
        // 获取配置对象
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // 为表关联时状态中存储的数据设置过期时间
        configuration.setString("table.exec.state.ttl", ttl);

        // 另一种设置方式
//        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));
    }



}
