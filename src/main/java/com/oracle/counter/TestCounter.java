package com.oracle.counter;

import com.oracle.test.BaseTest;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by 代俊朴 on 2017/7/6.
 */
public class TestCounter extends BaseTest {
     @Test
     public void firstCounter() throws IOException {
          Table table=conn.getTable(TableName.valueOf("mytable"));
          table.incrementColumnValue(Bytes.toBytes("20110101"),Bytes.toBytes("comF1"),Bytes.toBytes("qual1"),1);
     }
}
