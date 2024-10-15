package com.haoran.hbase;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class HbaseDDL {
    // 单例连接
    public static Connection connection = HBaseConnect.connection;

    // 创建表空间 create_namespace 'bigdata'  默认表空间为default
    public static void createNamespace(String nameSpace) throws IOException {
        Admin admin = connection.getAdmin(); // 使用连接对象获取Admin对象
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build(); // 创建命名空间对象
        admin.createNamespace(namespaceDescriptor); // 创建命名空间
    }

    // 创建表 create 'bigdata:student', {NAME => 'info', VERSIONS => 5}, {NAME => 'msg'}
    public static void createTable(String table, String[] columnFamily) throws IOException {
        Admin admin = connection.getAdmin(); // 使用连接对象获取Admin对象
        TableName tableName = TableName.valueOf(table); // 定义表名
        TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);  // 定义表对象
        ColumnFamilyDescriptor family1 = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes()).setMaxVersions(5).build(); //构建列族对象
        ColumnFamilyDescriptor family2 = ColumnFamilyDescriptorBuilder.newBuilder("msg".getBytes()).build();
        tableDescriptor.setColumnFamilies(new ArrayList<>(){{add(family1); add(family2);}});  // 设置列族
        admin.createTable(tableDescriptor.build());  // 创建表
    }

    public static void main(String[] args) {
        try {
            createNamespace("bigdata");
            createTable("bigdata:student", new String[]{"data"});
            System.out.println(connection.getTable(TableName.valueOf("bigdata:student")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
