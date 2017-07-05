package com.oracle.test.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by 代俊朴 on 2017/6/12.
 */
public class TableCRUD {
    public Admin admin;
    public Connection conn;
    @Test
     public void dropTable(){
         String name="tb_test1";
         Configuration conf;
         conf= HBaseConfiguration.create();
         //hbase.zookeeper.quorum
         conf.set("hbase.zookeeper.quorum","192.168.1.111");
         conf.set("hbase.zookeeper.property.clientPort", "2181");
         try {
             conn= ConnectionFactory.createConnection(conf);
             admin=conn.getAdmin();
             System.out.println(admin.tableExists(TableName.valueOf(name)));

             if(admin.tableExists(TableName.valueOf(name))){
                 if(admin.isTableEnabled(TableName.valueOf(name)))
                 admin.disableTable(TableName.valueOf(name));
                 admin.deleteTable(TableName.valueOf(name));
             }
         } catch (IOException e) {
             e.printStackTrace();
         }

     }
    public void createTable(String tableName,String... columnFamily){
        Configuration conf;
        conf= HBaseConfiguration.create();
        //hbase.zookeeper.quorum
        conf.set("hbase.zookeeper.quorum","192.168.1.111");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            conn= ConnectionFactory.createConnection(conf);
            admin=conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Admin admin = conn.getAdmin();
            HTableDescriptor hd = new HTableDescriptor(TableName.valueOf(tableName));
            for (String str : columnFamily){
                hd.addFamily(new HColumnDescriptor(str));
            }
            admin.createTable(hd);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testCreateTable(){
        TableCRUD tc=new TableCRUD();
       // tc.createTable("tb_test1","coml1","coml2","coml3");
       // tc.dropTable("tb_test1");
    }


}
