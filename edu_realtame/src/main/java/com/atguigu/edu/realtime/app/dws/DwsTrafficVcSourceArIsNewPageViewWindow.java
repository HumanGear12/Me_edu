package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsTrafficForSourcePvBean;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsTrafficVcSourceArIsNewPageViewWindow {

    public static void main(String[] args) {

        // 1.创建环境
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        // 2. 读取pagelog主题数据
        String pageTopic = "dwd_traffic_page_log";
        String groupID = "dws_traffic_vc_source_ar_is_new_page_view_window";
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaConsumer(pageTopic, groupID);
        DataStreamSource<String> pageStream = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(),"page_log");


        // 3. 读取uv数据
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        KafkaSource<String> uvSource = KafkaUtil.getKafkaConsumer(uvTopic, groupID);
        DataStreamSource<String> uvStream = env.fromSource(
                uvSource, WatermarkStrategy.noWatermarks(),"uv_detail");


        // 4. 读取跳出用户数据
        String jumpTopic = "dwd_traffic_user_jump_detail";
        KafkaSource<String> jumpSource = KafkaUtil.getKafkaConsumer(jumpTopic, groupID);
        DataStreamSource<String> jumpStream = env.fromSource(
                jumpSource, WatermarkStrategy.noWatermarks(),"jump_detail");

        // 5. 转换数据结构
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> pageBeanStream = pageStream
                .map(new MapFunction<String, DwsTrafficForSourcePvBean>() {
            @Override
            public DwsTrafficForSourcePvBean map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                Long ts = jsonObject.getLong("ts");

                return DwsTrafficForSourcePvBean.builder()
                        .versionCode(common.getString("vc"))
                        .sourceId(common.getString("sc"))
                        .ar(common.getString("ar"))
                        .isNew(common.getString("is_new"))
                        .uvCount(0L)
                        .totalSessionCount(page.getString("last_page_id") == null ? 1L : 0L)
                        .pageViewCount(1L)
                        .jumpSessionCount(0L)
                        .totalDuringTime(page.getLong("during_time"))
                        .ts(ts)
                        .build();
            }
        });

        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> uvBeanStream = uvStream
                .map(new MapFunction<String, DwsTrafficForSourcePvBean>() {
            @Override
            public DwsTrafficForSourcePvBean map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = jsonObject.getLong("ts");

                return DwsTrafficForSourcePvBean.builder()
                        .versionCode(common.getString("vc"))
                        .sourceId(common.getString("sc"))
                        .ar(common.getString("ar"))
                        .isNew(common.getString("is_new"))
                        .uvCount(1L)
                        .totalSessionCount(0L)
                        .pageViewCount(0L)
                        .jumpSessionCount(0L)
                        .totalDuringTime(0L)
                        .ts(ts)
                        .build();
            }
        });

        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> jumpBeanStream = jumpStream.map(new MapFunction<String, DwsTrafficForSourcePvBean>() {
            @Override
            public DwsTrafficForSourcePvBean map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = jsonObject.getLong("ts");

                return DwsTrafficForSourcePvBean.builder()
                        .versionCode(common.getString("vc"))
                        .sourceId(common.getString("sc"))
                        .ar(common.getString("ar"))
                        .isNew(common.getString("is_new"))
                        .uvCount(0L)
                        .totalSessionCount(0L)
                        .pageViewCount(0L)
                        .jumpSessionCount(1L)
                        .totalDuringTime(0L)
                        .ts(ts)
                        .build();
            }
        });

        // 6. 合并三条数据流
        DataStream<DwsTrafficForSourcePvBean> unionStream = pageBeanStream
                .union(uvBeanStream)
                .union(jumpBeanStream);

        // 7. 添加水位线
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> withWaterMarkStream = unionStream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<DwsTrafficForSourcePvBean>forBoundedOutOfOrderness(Duration.ofSeconds(15L))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsTrafficForSourcePvBean>() {
                                    @Override
                                    public long extractTimestamp(DwsTrafficForSourcePvBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }));

        // 8. 分组开窗
        WindowedStream<DwsTrafficForSourcePvBean, String, TimeWindow> windowedStream = withWaterMarkStream.keyBy(new KeySelector<DwsTrafficForSourcePvBean, String>() {
            @Override
            public String getKey(DwsTrafficForSourcePvBean value) throws Exception {
                return value.getSourceId()
                        + value.getVersionCode()
                        + value.getAr()
                        + value.getIsNew();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10L)));


        // 9. 聚合统计
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> reduceStream = windowedStream
                .reduce(new ReduceFunction<DwsTrafficForSourcePvBean>() {
                         @Override
                         public DwsTrafficForSourcePvBean reduce(DwsTrafficForSourcePvBean value1, DwsTrafficForSourcePvBean value2) throws Exception {
                             value1.setTotalSessionCount(value1.getTotalSessionCount() + value2.getTotalSessionCount());
                             value1.setJumpSessionCount(value1.getJumpSessionCount() + value2.getJumpSessionCount());
                             value1.setPageViewCount(value1.getPageViewCount() + value2.getPageViewCount());
                             value1.setUvCount(value1.getUvCount() + value2.getUvCount());
                             value1.setTotalDuringTime(value1.getTotalDuringTime() + value2.getTotalDuringTime());
                             return value1;
                         }
                     }, new ProcessWindowFunction<DwsTrafficForSourcePvBean, DwsTrafficForSourcePvBean, String, TimeWindow>() {



                    @Override
                        public void process(String s, Context ctx, Iterable<DwsTrafficForSourcePvBean> iterable, Collector<DwsTrafficForSourcePvBean> out) throws Exception {
                            TimeWindow timeWindow = ctx.window();
                            String start = DateFormatUtil.toYmdHms(timeWindow.getStart());
                            String end = DateFormatUtil.toYmdHms(timeWindow.getEnd());
                            for (DwsTrafficForSourcePvBean dwsTrafficForSourcePvBean : iterable) {
                                dwsTrafficForSourcePvBean.setStt(start);
                                dwsTrafficForSourcePvBean.setEdt(end);
                                dwsTrafficForSourcePvBean.setTs(System.currentTimeMillis());
                                out.collect(dwsTrafficForSourcePvBean);
                            }

                        }

                     });
        reduceStream.print();

        // 10. 维度关联

//        reduceStream.map(new MapFunction<DwsTrafficForSourcePvBean, DwsTrafficForSourcePvBean>() {
//            @Override
//            public DwsTrafficForSourcePvBean map(DwsTrafficForSourcePvBean value) throws Exception {
//                // 关联来源名称
//                String sourceId = value.getSourceId();
//                String provinceId = value.getAr();
//                JSONObject dimBaseSource = DimUtil.getDimInfo("DIM_BASE_SOURCE", sourceId);
//                String sourceName = dimBaseSource.getString("SOURCE_SITE");
//                value.setSourceName(sourceName);
//                JSONObject dimBaseProvince = DimUtil.getDimInfo("DIM_BASE_PROVINCE",provinceId);
//                String provinceName = dimBaseProvince.getString("NAME");
//                value.setProvinceName(provinceName);
//                return value;
//            }
//        }).print();

        // 异步操作
        // 关联来源表
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> sourceBeanStream = AsyncDataStream
                .unorderedWait(reduceStream, new DimAsyncFunction<DwsTrafficForSourcePvBean>("DIM_BASE_SOURCE") {
            @Override
            public void join(DwsTrafficForSourcePvBean obj, JSONObject jsonObject) throws Exception {
                String sourceName = jsonObject.getString("SOURCE_SITE");
                obj.setSourceName(sourceName);
            }

            @Override
            public String getKey(DwsTrafficForSourcePvBean obj) {
                return obj.getSourceId();
            }
        }, 1, TimeUnit.MINUTES);

        // 关联省份
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> dimBeanStream = AsyncDataStream
                .unorderedWait(sourceBeanStream, new DimAsyncFunction<DwsTrafficForSourcePvBean>("DIM_BASE_PROVINCE") {
            @Override
            public void join(DwsTrafficForSourcePvBean obj, JSONObject jsonObject) throws Exception {
                String provinceName = jsonObject.getString("NAME");
                obj.setProvinceName(provinceName);
            }

            @Override
            public String getKey(DwsTrafficForSourcePvBean obj) {
                return obj.getAr();
            }
        }, 1, TimeUnit.MINUTES);




        // 11. 写出到clickhouse



    }
}
