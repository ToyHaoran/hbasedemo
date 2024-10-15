package com.haoran.phoenix;


import java.sql.*;
import java.util.Properties;

public class PhoenixClient {
    public static void main(String[] args) throws SQLException {
        // 标准的JDBC代码
        // 1.添加链接
        String url = "jdbc:phoenix:hadoop100,hadoop101,hadoop102:2181";

        // 2. 创建配置
        // 没有需要添加的必要配置  因为Phoenix没有账号密码
        Properties properties = new Properties();
        // 3. 获取连接
        Connection connection = DriverManager.getConnection(url, properties);
        // 4.编译SQL语句
        PreparedStatement preparedStatement = connection.prepareStatement("select * from student");
        // 5.执行语句
        ResultSet resultSet = preparedStatement.executeQuery();
        // 6.输出结果
        while (resultSet.next()){
            System.out.println(resultSet.getString(1) + ":" + resultSet.getString(2) + ":" + resultSet.getString(3));
        }

        // 7.关闭资源
        connection.close();

        // 由于Phoenix框架内部需要获取一个HBase连接,所以会延迟关闭。不影响后续的代码执行
        System.out.println("hello");
    }
}
