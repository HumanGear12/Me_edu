package com.atguigu.edu.realtime.app.dws;

import com.atguigu.edu.realtime.app.func.KeywordUDTF;
import com.atguigu.edu.realtime.bean.KeywordBean;
import com.atguigu.edu.realtime.common.EduConstant;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow {

    public static void main(String[] args) {

        // 1. 环境准备
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        // 3. 从 Kafka dwd_traffic_page_log 主题中读取页面浏览日志数据
        String groupId = "dws_traffic_source_keyword_page_view_window";
        String topicName = "dwd_traffic_page_log";

        tableEnv.executeSql("create table page_log(\n" +
                "common map<String,String>,\n" +
                "page map<String,String>,\n" +
                "ts bigint\n" +
                "row_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ")" + KafkaUtil.getKafkaDDL(topicName, groupId));


        // 4. 从表中过滤搜索行为
        Table searchTable = tableEnv.sqlQuery("select\n" +
                "\tpage['item'] word,\n" +
                "\trow_time\n" +
                "from page_log\n" +
                "where page['item'] is not null\n" +
                "and page['item_type'] = 'keyword'\n" +
                //"and page['last_page_id'] = 'search'" +
                "");
        tableEnv.createTemporaryView("search_table",searchTable);


        // 5. 使用自定义的UDTF函数对搜索的内容进行分词
        Table splitTable = tableEnv.sqlQuery("select\n" +
                "keyword,\n" +
                "row_time \n" +
                "from search_table,\n" +
                "lateral table(ik_analyze(full_word))\n" +
                "as t(keyword)");
        tableEnv.createTemporaryView("split_table", splitTable);


        // 6. 分组、开窗、聚合计算
        Table KeywordBeanSearch = tableEnv.sqlQuery("select\n" +
                "DATE_FORMAT(TUMBLE_START(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "DATE_FORMAT(TUMBLE_END(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n'" +
                EduConstant.KEYWORD_SEARCH + "' source,\n" +
                "keyword,\n" +
                "count(*) keyword_count,\n" +
                "UNIX_TIMESTAMP()*1000 ts\n" +
                "from split_table\n" +
                "GROUP BY TUMBLE(row_time, INTERVAL '10' SECOND),keyword");


        // 7. 转化为流
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv
                .toDataStream(KeywordBeanSearch, KeywordBean.class);


        // 8. 将流中的数据写到ClickHouse中



    }
}



