package com.oracle.batch;

import com.oracle.test.BaseTest;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 代俊朴 on 2017/6/14.F
 */
public class TestBatch extends BaseTest {

    @Test
    public void testBatch() throws IOException, InterruptedException {
        Table table=conn.getTable(TableName.valueOf("mytable"));
         List<Row> rowList=new ArrayList<>();
         Get get=new Get(Bytes.toBytes("row4"));
         rowList.add(get);
         Put put=new Put(Bytes.toBytes("putx"));
         put.addColumn(Bytes.toBytes("comF1"),Bytes.toBytes("info"),Bytes.toBytes("haha"));
         rowList.add(put);
         Delete delete=new Delete(Bytes.toBytes("row3"));
         rowList.add(delete);
         Result [] results=new Result[rowList.size()];
         table.batch(rowList,results);
         for(Result r:results){
             System.out.println(Bytes.toString(r.value()));
         }
    }
}
