package com.atguigu.edu.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimBroadcastProcessFunction;
import com.atguigu.edu.realtime.app.func.DimPhoenixSinkFunc;
import com.atguigu.edu.realtime.bean.DimTableProcess;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DimSinkApp {

    public static void main(String[] args) throws Exception {

        //1.创建flink运行环境
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        //2.读取kafka数据
        DataStreamSource<String> eduDS = env.fromSource(KafkaUtil.getKafkaConsumer("edu_db", "dim_sink_app"),
                WatermarkStrategy.noWatermarks(), "kafka_source");

        //3.对数据进行etl清洗
        SingleOutputStreamOperator<JSONObject> jsonDS = eduDS
                .flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String in, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(in);
                    String type = jsonObject.getString("type");
                    if (!(type.equals("bootstrap-complete") || type.equals("bootstrap-start"))) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("解析json出错");
                }
            }
        });
        //jsonDS.print();

//============================================================================================

        //4.使用flinkCDC读取配置表数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.12.122")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("edu_config")
                .tableList("edu_config.table_process")
                //定义读取数据的格式
                .deserializer(new JsonDebeziumDeserializationSchema())
                //设置读取数据的模式
                .startupOptions(StartupOptions.initial())
                .build();


        DataStreamSource<String> configDS = env.fromSource(mySqlSource,
                WatermarkStrategy.noWatermarks(),"mysql_source");

        configDS.print();

        //5.将配置表数据创建为广播流

        MapStateDescriptor<String, DimTableProcess> tableProcessState = new MapStateDescriptor<>(
                "table_process_state", String.class, DimTableProcess.class);
        BroadcastStream<String> broadcastStream = configDS.broadcast(tableProcessState);


        //6.合并
        BroadcastConnectedStream<JSONObject, String> connectCS = jsonDS.connect(broadcastStream);


        //对合并后的流进行分别处理
        SingleOutputStreamOperator<JSONObject> dimDS =
                connectCS.process(
                new DimBroadcastProcessFunction(tableProcessState));

        //将数据写入phoenix

        dimDS.addSink(new DimPhoenixSinkFunc());



        //执行程序
        env.execute();

    }
}
