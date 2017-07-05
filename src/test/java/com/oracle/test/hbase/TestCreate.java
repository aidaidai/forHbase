package com.oracle.test.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
/**
 * Created by 代俊朴 on 2017/6/9.
 */
public class TestCreate {
   public Admin admin;
   public Connection conn;
    @Before
    public void init(){
        Configuration conf;
        conf= HBaseConfiguration.create();
        //hbase.zookeeper.quorum
        conf.set("hbase.zookeeper.quorum","192.168.1.111");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
             conn=ConnectionFactory.createConnection(conf);
            admin=conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @After
    public void destroy(){
        try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }











    public static void main(String[] args) {
        Configuration conf= HBaseConfiguration.create();
        //hbase.zookeeper.quorum
        conf.set("hbase.zookeeper.quorum","192.168.1.111");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            Connection connection= ConnectionFactory.createConnection(conf);
            System.err.println(connection);
            Admin admin=connection.getAdmin();
            HTableDescriptor td=new HTableDescriptor(TableName.valueOf("mytable"));
            //指定列族
            td.addFamily(new HColumnDescriptor("comF1"));
            td.addFamily(new HColumnDescriptor("comF2"));
            admin.createTable(td);
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
