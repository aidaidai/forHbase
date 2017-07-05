package com.oracle.delete;

import com.oracle.test.BaseTest;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 代俊朴 on 2017/6/13.
 */
public class TestDelete extends BaseTest {
    @Test
    public  void testDeleteOne() throws IOException {
        Table table=conn.getTable(TableName.valueOf("mytable"));
        Delete delete=new Delete(Bytes.toBytes("put1"));
        table.delete(delete);
    }
    @Test
    public void testDeleteVersion() throws IOException {
        Table table=conn.getTable(TableName.valueOf("mytable"));
        Delete delete=new Delete(Bytes.toBytes("put3"));
        delete.setTimestamp(1497336949166L);
        table.delete(delete);
    }
    @Test
    public void testDelColumn() throws IOException {
        Table table=conn.getTable(TableName.valueOf("mytable"));
        Delete delete=new Delete(Bytes.toBytes("mytable"));
        delete.addColumn(Bytes.toBytes("comF1"),Bytes.toBytes("qual2"));
        table.delete(delete);
    }

    @Test
    public void testDelList() throws IOException {
        List<Delete> list=new ArrayList<>();
        Table table=conn.getTable(TableName.valueOf("mytable"));
        for(int i=1;i<10;i++){
            Delete delete=new Delete(Bytes.toBytes("put"+i));
            list.add(delete);
        }
        table.delete(list);
    }
}
