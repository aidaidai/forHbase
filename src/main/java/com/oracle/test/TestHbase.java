package com.oracle.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by 代俊朴 on 2017/6/8.
 */
public class TestHbase {
    public static void main(String[] args) {
        Configuration  conf=HBaseConfiguration.create();
        //hbase.zookeeper.quorum
        conf.set("hbase.zookeeper.quorum","192.168.1.111");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
           Connection connection= ConnectionFactory.createConnection(conf);
           System.err.println(connection);
          Admin  admin=connection.getAdmin();
            System.err.println("========");
           boolean f=admin.tableExists(TableName.valueOf("testtables3"));
            System.err.println(f);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
