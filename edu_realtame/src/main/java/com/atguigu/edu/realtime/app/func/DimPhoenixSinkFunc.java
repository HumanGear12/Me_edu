package com.atguigu.edu.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.util.PhoenixUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class DimPhoenixSinkFunc implements SinkFunction<JSONObject> {
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        // TODO 1 获取输出的表名
        String sinkTable = jsonObject.getString("sink_table");
        jsonObject.remove("sink_table");


        // TODO 3 使用工具类 写出数据
        PhoenixUtil.executeDML(sinkTable,jsonObject);
    }
}
