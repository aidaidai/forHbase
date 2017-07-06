package com.oracle.filter;

import com.oracle.test.BaseTest;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by 代俊朴 on 2017/7/3.
 */
public class TestFilter extends BaseTest{
     @Test
    public void testFilter() throws IOException {
         Table table=conn.getTable(TableName.valueOf("mytable"));
         Scan scan=new Scan();


         scan.addColumn(Bytes.toBytes("comF1"),Bytes.toBytes("info"));
         Filter filter=new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("putx")));
         scan.setFilter(filter);
         ResultScanner resultScanner=table.getScanner(scan);
         for (Result rs:resultScanner){
             System.out.println(rs);
         }
         resultScanner.close();
     }

}
