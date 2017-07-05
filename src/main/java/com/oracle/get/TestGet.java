package com.oracle.get;

import com.oracle.test.BaseTest;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 代俊朴 on 2017/6/13.
 */
public class TestGet  extends BaseTest{
    @Test
    public void testGetOne() throws IOException {
         Get get =new Get(Bytes.toBytes("put1"));
         Table table=conn.getTable(TableName.valueOf("mytable"));
         Result result=table.get(get);
        System.out.println(Bytes.toString(result.value()));
    }
    @Test
    public void  testGetFamily() throws IOException {
        Get get=new Get(Bytes.toBytes("row4"));
        get.addFamily(Bytes.toBytes("comF2"));
        Table table=conn.getTable(TableName.valueOf("mytable"));
        Result result=table.get(get);
        System.out.println(Bytes.toString(result.value()));
    }
    @Test
    public void  testGetFamilyColumn() throws IOException {
        Get get=new Get(Bytes.toBytes("mytable"));
        get.addColumn(Bytes.toBytes("comF1"),Bytes.toBytes("qual1"));
        Table table=conn.getTable(TableName.valueOf("mytable"));
        Result result=table.get(get);
        System.out.println(Bytes.toString(result.value()));
    }
    @Test
    public void testGetVersion() throws IOException {
        Get get=new Get(Bytes.toBytes("put1"));
        get.addColumn(Bytes.toBytes("comF1"),Bytes.toBytes("qual365"));
        get.setMaxVersions(3);
        Table table=conn.getTable(TableName.valueOf("mytable"));
        Result result=table.get(get);
        List<Cell> cells=result.listCells();
        for(Cell cell:cells){
            //System.out.println(Bytes.toString(cell.getValue()));
            //                       System.out.println(Bytes.toString(cell.getValueArray()));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }
    @Test
    public void testGetList() throws IOException {
        List<Get> list=new ArrayList<>();
        for(int i=1;i<10;i++){
            Get get=new Get(Bytes.toBytes("put"+i));
            list.add(get);
        }
        Table table=conn.getTable(TableName.valueOf("mytable"));
        Result [] results=table.get(list);
        for(Result r:results){
            System.out.println(Bytes.toString(r.value()));
        }
    }
}
