package com.haoran.hbase;

import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;


import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class HBaseConnect {
    // 使用类单例模式，确保使用一个连接，可以同时用于多个线程。
    public static Connection connection = null;

    static {
        try {
            // 创建hbase的连接，使用配置文件的方法
            connection = ConnectionFactory.createConnection();
            // 默认使用同步连接，可以使用异步连接
            // CompletableFuture<AsyncConnection> asyncConnection = ConnectionFactory.createAsyncConnection(conf);
        } catch (IOException e) {
            System.out.println("连接获取失败");
            e.printStackTrace();
        }
    }

    // 连接关闭方法,用于进程关闭时调用
    public static void closeConnection() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }
}
