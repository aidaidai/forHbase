package com.oracle.test.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by 代俊朴 on 2017/6/9.
 */
public class TestCrud extends TestCreate{
    @Test
    public void testPut(){
        try {
            Table table=conn.getTable(TableName.valueOf("mytable"));
            System.out.println(table);
            Put put=new Put(Bytes.toBytes("row4"));

            put.addColumn(Bytes.toBytes("comF2"), Bytes.toBytes("qual2"),Bytes.toBytes("中文7"));
            put.addColumn(Bytes.toBytes("comF2"), Bytes.toBytes("qual2"),Bytes.toBytes("中文3"));
            put.addColumn(Bytes.toBytes("comF2"), Bytes.toBytes("qual2"),Bytes.toBytes("中文5"));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testGet(){
        try {
            Table table=conn.getTable(TableName.valueOf("mytable"));
            Get get  =new Get(Bytes.toBytes("row4"));
            Result result=table.get(get);
            byte[] cell = result.getValue(Bytes.toBytes("comF1"), Bytes.toBytes("qual2"));
            System.out.println(Bytes.toString(cell));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testGetColumn(){
        Get get=new Get(Bytes.toBytes("row4"));
        Table table= null;
        try {
            table = conn.getTable(TableName.valueOf("mytable"));
            Result result=table.get(get);
            List<Cell> list=result.getColumnCells(Bytes.toBytes("comF2"),Bytes.toBytes("qual2"));
            System.out.println(list.size());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    @Test
    public void testSCan(){
        Scan scan = new Scan(Bytes.toBytes("row1"), Bytes.toBytes("row900"));
        try {
            Table table=conn.getTable(TableName.valueOf("mytable"));
            ResultScanner results=table.getScanner(scan);
            for(Result r:results){
               byte rr []= r.getValue(Bytes.toBytes("comF1"),Bytes.toBytes("qual2"));
                System.out.println(Bytes.toString(rr));
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testDelete(){
        try {
            Table table=conn.getTable(TableName.valueOf("mytable"));
            Delete delete=new Delete(Bytes.toBytes("row2"));
            table.delete(delete);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDeleteComl(){
        try {
            Table table=conn.getTable(TableName.valueOf("mytable"));
            Delete delete=new Delete(Bytes.toBytes("row4"));
            delete.addFamily(Bytes.toBytes("comF1"));
            table.delete(delete);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
