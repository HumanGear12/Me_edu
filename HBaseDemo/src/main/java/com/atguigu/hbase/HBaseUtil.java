package com.atguigu.hbase;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Smexy on 2023/11/6
    1.不支持JDBC
    2.创建客户端
        Table: 代表一张表，进行数据的读写操作。
        Admin: 代表数据库管理员，对表进行操作(建表，删表，描述表)

        Table 和 Admin都需要从Connection(程序和HBase服务的链接)中来获取。
            Table和Admin的创建是轻量级(省时省事)，但是不是线程安全的，因此只能作为方法中的局部变量，不能共享。

        Connection 可以通过 ConnectionFactory来创建。
        Connection的创建是重量级(费时间，费资源)，但是是线程安全的。因此可以在App中只创建一次，在不同的线程中共享。
        Connection可以作为静态属性(选择)或成员属性存在。

        客户端连接上HBase，需要HBase所使用的zookeeper集群的地址就行。

    3.使用客户端发送命令
        增|改 put
            Table.put(Put x);
            Put:  代表对单行数据的一次写入操作。
        删 delete,deleteall
            Table.delete(Delete x):
            Delete: 代表对单行数据的一次删除操作。
        查 get
            Table.get(Get g):
            Get: 代表对单行数据的一次查询操作。

        查 scan
            Table.getScanner(Scan x):
            Scan : 代表对多行数据的一次查询操作。

        ------------
        HBase是没有数据类型，所有的数据都是以byte[]存储。
            不管是读，还是写都需要把byte[]和常用的类型进行相互转换。

            HBase提供了Bytes工具类，可以帮你将常见的数据类型和byte[]进行相互转换。
                写： String -----> byte[]
                        Bytes.toBytes(xxx);
                读： byte[] -----> String
                        Bytes.toString(byte [] x)


    4.读操作，接收返回值
            Result: 单行的查询结果。
                一行中有很多列。Result是Cell的集合。

            HBase提供了工具类CellUtil，可以从一个Cell中取出想要的数据。

    5.使用完成后，关闭客户端

 */
public class HBaseUtil
{
    //随着类的加载而赋值，只会创建一次
    private static Connection conn ;

    static {
        try {
            //自动读取hadoop和hbase相关的配置文件
            conn = ConnectionFactory.createConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void close(Connection conn) throws IOException {
        if (conn != null){
            conn.close();
        }
    }

    //根据表名获取default库下的表对象
    public static Table getTable(String name) throws IOException {
        if (StringUtils.isBlank(name)){
            throw  new RuntimeException("表名不合法!");
        }else {
            return conn.getTable(TableName.valueOf(name));
        }
    }

    //put 表名,rowkey,列族名:列名,value
    public static Put getPut(String rk,String cf,String cq,String value){

        Put put = new Put(Bytes.toBytes(rk));
        //向这一行去写入一个cell
        put.addColumn(
            Bytes.toBytes(cf),
            Bytes.toBytes(cq),
            Bytes.toBytes(value)
        );
        return put;
    }


    public static void printResult(Result result) {

        //获取一行中的N个Cell
        Cell[] cells = result.rawCells();

        //CellUtil可以去操作Cell获取感兴趣的属性
        for (Cell cell : cells) {
            System.out.println(
                Bytes.toString(CellUtil.cloneRow(cell)) +"," +
                Bytes.toString(CellUtil.cloneFamily(cell)) +":" +
                Bytes.toString(CellUtil.cloneQualifier(cell)) +"," +
                Bytes.toString(CellUtil.cloneValue(cell)) +"," +
                    cell.getTimestamp() +",type:"+
                    cell.getType()
            );
        }

    }
}
