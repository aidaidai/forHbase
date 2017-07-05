package com.oracle.scan;

import com.oracle.test.BaseTest;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by 代俊朴 on 2017/6/14.
 */
public class TestScan extends BaseTest{

    @Test
    public void testScan() throws IOException {
        Table table=conn.getTable(TableName.valueOf("mytable"));
        Scan scan=new Scan();
        ResultScanner  rs=table.getScanner(scan);
        for(Result result:rs){
            System.out.println(Bytes.toString(result.value()));
        }
    }
    @Test
    public void testScanColumn() throws IOException {
        Scan scan=new Scan();
        scan.addColumn(Bytes.toBytes("comF1"),Bytes.toBytes("qual1"));
        Table table=conn.getTable(TableName.valueOf("mytable"));
        ResultScanner resultScanner=table.getScanner(scan);
        for(Result r:resultScanner){
            System.out.println(Bytes.toString(r.value()));
        }
    }
    @Test
    public void testStartStop() throws IOException {
        Scan scan=new Scan();
        scan.setStartRow(Bytes.toBytes("put1"));
        scan.setStopRow(Bytes.toBytes("put6"));
        Table table=conn.getTable(TableName.valueOf("mytable"));
        ResultScanner resultScanner=table.getScanner(scan);
        for(Result r:resultScanner){
            System.out.println(Bytes.toString(r.value()));
        }
    }
}
