package com.atguigu.edu.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.util.PhoenixUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class DimPhoenixSinkFunc implements SinkFunction<JSONObject> {
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        //  获取输出的表名
        System.out.println("获取输出的表名");
        String sinkTable = jsonObject.getString("sink_table");
        String type = jsonObject.getString("type");
        String id = jsonObject.getString("id");
        jsonObject.remove("sink_table");
        jsonObject.remove("type");

        //  使用工具类 写出数据
        System.out.println(sinkTable + "写出数据");
        PhoenixUtil.executeDML(sinkTable,jsonObject);

    }
}
