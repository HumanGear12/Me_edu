package com.atguigu.edu.realtime.app.func;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.util.DimUtil;
import com.atguigu.edu.realtime.util.PhoenixUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.SQLException;

public class DimPhoenixSinkFunc implements SinkFunction<JSONObject> {
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        //  获取输出的表名
        System.out.println("获取输出的表名");
        // 获取目标表名
        String sinkTable = jsonObject.getString("sink_table");
        // 获取操作类型
        String type = jsonObject.getString("type");
        // 获取 id 字段的值
        String id = jsonObject.getString("id");
        // 清除 JSON 对象中的 sinkTable 字段和 type 字段
        // 以便可将该对象直接用于 HBase 表的数据写入
        jsonObject.remove("sink_table");
        jsonObject.remove("type");


//        // 获取连接对象
//        DruidPooledConnection conn = null;
//        try {
//            conn = druidDataSource.getConnection();
//        } catch (SQLException sqlException) {
//            sqlException.printStackTrace();
//            System.out.println("获取连接对象异常");
//        }

        //  使用工具类 写出数据
        System.out.println(sinkTable + "写出数据");
        PhoenixUtil.executeDML(sinkTable,jsonObject);







//            // 执行写入操作
//            PhoenixUtil.executeDML(conn, sinkTable, jsonObj);
//
//            // 如果操作类型为 update，则清除 redis 中的缓存信息
//            if ("update".equals(type)) {
//                DimUtil.deleteCached(sinkTable, id);
//            }
//

    }
}
