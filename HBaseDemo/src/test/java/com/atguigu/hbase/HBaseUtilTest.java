package com.atguigu.hbase;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Created by Smexy on 2023/11/6
 */
public class HBaseUtilTest
{


    /*
        put t1,1001,f1:name,jack
        put t1,1001,f1:age,20
        put t1,1001,f1:gender,M
     */
    @Test
    public void testPut() throws Exception{
        //获取表客户端
        Table t1 = HBaseUtil.getTable("t1");

        Put put1 = HBaseUtil.getPut("1001", "f2", "name", "jack2");
        Put put2 = HBaseUtil.getPut("1001", "f2", "age", "23");
        Put put3 = HBaseUtil.getPut("1001", "f2", "gender", "F");

        //写入
        t1.put(Arrays.asList(put1,put2,put3));

        //关闭客户端
        t1.close();

    }

    // get t1,1001
    @Test
    public void testGet() throws Exception{
        //获取表客户端
        Table t1 = HBaseUtil.getTable("t1");

        Get get = new Get(Bytes.toBytes("1001"));
        //代表单行的查询结果
        Result result = t1.get(get);
        HBaseUtil.printResult(result);
        //关闭客户端
        t1.close();
    }

    @Test
    public void testScan() throws Exception{
        //获取表客户端
        Table t1 = HBaseUtil.getTable("t1");

        // scan t1,{STARTROW => '1001',STOPROW => '1009',VERSIONS => 10, RAW => TRUE}
        Scan scan = new Scan()
            .withStartRow(Bytes.toBytes("1001"))
            .withStopRow(Bytes.toBytes("1009"))
            .readVersions(10)
            .setRaw(true);
        //代表多行的查询结果，就是 Result的集合
        ResultScanner scanner = t1.getScanner(scan);
        for (Result result : scanner) {

            HBaseUtil.printResult(result);
        }
        //关闭客户端
        t1.close();
    }

    @Test
    public void testDelete() throws Exception{
        //获取表客户端
        Table t1 = HBaseUtil.getTable("t1");
        //创建对单号的删除对象
        Delete delete = new Delete(Bytes.toBytes("1001"));

        //删除一列的最新版本
        //delete.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age"));

        //删除一列的所有版本
        //delete.addColumns(Bytes.toBytes("f1"),Bytes.toBytes("age"));

        //删除一个列族
        //delete.addFamily(Bytes.toBytes("f1"));

        //删除一行
        t1.delete(delete);

        t1.close();

    }


}