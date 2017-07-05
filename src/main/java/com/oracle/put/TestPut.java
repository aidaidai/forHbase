package com.oracle.put;

import com.oracle.test.BaseTest;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 代俊朴 on 2017/6/13.
 */
public class TestPut  extends BaseTest{

    @Test
    public void testPutOne() throws IOException {
        Table table=conn.getTable(TableName.valueOf("mytable"));
        Put put=new Put(Bytes.toBytes("mytable"));
//        put.addColumn(Bytes.toBytes("comF1"),Bytes.toBytes("qual365"),Bytes.toBytes("a"));
//        put.addColumn(Bytes.toBytes("comF1"),Bytes.toBytes("qual365"),Bytes.toBytes("b"));
        put.addColumn(Bytes.toBytes("comF1"),Bytes.toBytes("qual1"),Bytes.toBytes("a"));
        table.put(put);
    }
    @Test
    public void testPutList() throws IOException {
        Table table=conn.getTable(TableName.valueOf("mytable"));
        List<Put> list=new ArrayList<>();
        for(int i=2;i<10;i++){
            Put put=new Put(Bytes.toBytes("put"+i));
            put.addColumn(Bytes.toBytes("comF1"),Bytes.toBytes("qual365"),Bytes.toBytes("vv"+i));
            list.add(put);
        }
        table.put(list);
    }
}
