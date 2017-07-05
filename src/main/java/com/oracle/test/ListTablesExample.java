package com.oracle.test;

/**
 * 获取所有已建表的表结构
 * cc ListTablesExample Example listing the existing tables and their descriptors
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

//import util.HBaseHelper;//旧版本HBaseHelper

public class ListTablesExample {

	public static void main(String[] args) throws IOException,
			InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.1.111");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable11");
		helper.dropTable("testtable22");
		helper.dropTable("testtable33");
		helper.dropTable("testtable1");
		helper.dropTable("testtable2");
		helper.dropTable("testtable3");
		helper.createTable("testtable11", "colfam1", "colfam2", "colfam3");
		helper.createTable("testtable22", "colfam1", "colfam2", "colfam3");
		helper.createTable("testtable33", "colfam1", "colfam2", "colfam3");

		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();
		System.err.println(admin);
		// vv ListTablesExample
		// HBaseAdmin admin = new HBaseAdmin(conf);

		HTableDescriptor[] htds = admin.listTables();
		// ^^ ListTablesExample
		System.out.println("Printing all tables...");
		// vv ListTablesExample
		for (HTableDescriptor htd : htds) {
			System.out.println(htd);
		}
		System.err.println("==================================");

		HTableDescriptor htd1 = admin.getTableDescriptor(TableName
				.valueOf("testtable1"));
		// Bytes.toBytes("testtable1"));
		// ^^ ListTablesExample
		System.out.println("Printing testtable1...");
		// vv ListTablesExample
		System.out.println(htd1);

		HTableDescriptor htd2 = admin.getTableDescriptor(TableName
				.valueOf("testtable3"));
		// Bytes.toBytes("testtable10"));
		// ^^ ListTablesExample
		System.out.println("Printing testtable10...");
		// vv ListTablesExample
		System.out.println(htd2);
		// ^^ ListTablesExample
	}
}
