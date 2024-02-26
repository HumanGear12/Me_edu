package com.atguigu.edu.realtime.util;

import com.atguigu.edu.realtime.common.EduConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;

public class KafkaUtil {

    public static KafkaSource<String> getKafkaConsumer(String topic, String groupId) {

        return KafkaSource.<String>builder()
                .setBootstrapServers(EduConfig.KAFKA_BOOTSTRAPS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
                        if (bytes != null && bytes.length != 0) {
                            return new String(bytes);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
    }

    public static KafkaSink<String> getKafkaProducer(String topic, String transId) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(EduConfig.KAFKA_BOOTSTRAPS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
                //指定生产的精准一次性
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //如果生产的精准一次性消费   那么每一个流数据在做向kakfa写入的时候  都会生成一个transId，默认名字生成规则相同，会冲突，我们这里指定前缀进行区分
                .setTransactionalIdPrefix(transId)
                .build();
    }


    /**
     * Kafka-Source DDL 语句
     *
     * @param topic   数据源主题
     * @param groupId 消费者组
     * @return 拼接好的 Kafka 数据源 DDL 语句
     */
    public static String getKafkaDDL(String topic, String groupId) {

        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + EduConfig.KAFKA_BOOTSTRAPS + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'group-offsets', " +
                " 'properties.auto.offset.reset' = 'earliest')";
    }

    /**
     * UpsertKafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 UpsertKafka-Sink DDL 语句
     */
    public static String getUpsertKafkaDDL(String topic) {

        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + EduConfig.KAFKA_BOOTSTRAPS + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }


    public static void createEduDb(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table edu_db(" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`type` string,\n" +
                "`data` map<string, string>,\n" +
                "`ts` string\n" +
                ")" + getKafkaDDL("edu_db", groupId));
    }


    /**
     * 自定义序列化器获取 FlinkKafkaProducer
     * @param kafkaRecordSerializationSchema 自义定 Kafka 序列化器
     * @param <T> 流中元素的数据类型
     * @return 返回的 FlinkKafkaProducer
     */
    public static <T> KafkaSink<T> getProducerBySchema(KafkaRecordSerializationSchema<T> kafkaRecordSerializationSchema, String transId) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(EduConfig.KAFKA_BOOTSTRAPS)
                .setRecordSerializer(kafkaRecordSerializationSchema)
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
                //指定生产的精准一次性
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //如果生产的精准一次性消费   那么每一个流数据在做向kakfa写入的时候  都会生成一个transId，默认名字生成规则相同，会冲突，我们这里指定前缀进行区分
                .setTransactionalIdPrefix(transId)
                .build();
    }



}



