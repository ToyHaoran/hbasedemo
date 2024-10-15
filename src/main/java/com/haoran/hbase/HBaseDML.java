package com.haoran.hbase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseDML {
    // 单例连接
    public static Connection connection = HBaseConnect.connection;

    // 插入数据 put 'bigdata:student','1001','info:name','zhangsan'
    public static void putCell(String nameSpace, String tableName, String rowKey, String family, String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName)); // 1.获取table
        Put put = new Put(Bytes.toBytes(rowKey));  // 2.创建Put对象
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));  // 3.添加put属性
        table.put(put);  // 4.put数据
        table.close();  // 5.关闭资源
    }

    // 查询数据
    public static String getCell(String nameSpace, String tableName, String rowKey, String family, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Get get = new Get(Bytes.toBytes(rowKey));  // 2.获取Get对象
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));  // 3.添加get属性

        // 4.get数据
        // 简便用法
        // byte[] bytes = table.get(get).value();
        // String value1 = new String(bytes);
        // 复杂用法
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        StringBuilder value = new StringBuilder();
        for (Cell cell : cells) {
            value.append(Bytes.toString(CellUtil.cloneValue(cell))).append("-");
        }
        table.close();
        return value.toString();
    }

    // 扫描数据
    public static List<String> scanRows(String nameSpace, String tableName, String startRow, String stopRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));

        Scan scan = new Scan().withStartRow(Bytes.toBytes(startRow)).withStopRow(Bytes.toBytes(stopRow));  // 创建Scan对象
        ResultScanner scanner = table.getScanner(scan);   // 扫描数据
        ArrayList<String> arrayList = new ArrayList<>();
        for (Result result : scanner) {
            arrayList.add(Bytes.toString(result.value()));
        }

        scanner.close();
        table.close();
        return arrayList;
    }

    // 带过滤的数据扫描
    public static void filterScan(String namespace, String tableName, String startRow, String stopRow, String columnFamily, String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Scan scan = new Scan().withStartRow(Bytes.toBytes(startRow)).withStopRow(Bytes.toBytes(stopRow));
        // 创建过滤器列表，默认过滤所有，可以选择过滤出一个
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        // 列值过滤器  过滤出单列数据
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                Bytes.toBytes(columnFamily),  // 列族
                Bytes.toBytes(column),  // 列名
                CompareOperator.EQUAL,  // 匹配规则  一般为相等  也可以是大于等于 小于等于
                Bytes.toBytes(value)
        );

        // 单列值过滤器：过滤出符合添加的整行数据  结果包含其他列
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes(columnFamily),
                Bytes.toBytes(column),
                CompareOperator.EQUAL,
                Bytes.toBytes(value)
        );
        filterList.addFilter(singleColumnValueFilter);
        // 可以设置多个  需放入到过滤器列表中
        scan.setFilter(filterList);

        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.print(new String(CellUtil.cloneRow(cell)) + "-" + new String(CellUtil.cloneFamily(cell)) + "-" + new String(CellUtil.cloneQualifier(cell)) + "-" + new String(CellUtil.cloneValue(cell)) + '\t');
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();
    }

    // 删除column数据
    public static void deleteColumn(String nameSpace, String tableName, String rowKey, String family, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 3.添加删除信息
        // 3.1 删除单个版本
        // delete.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));
        // 3.2 删除所有版本
        delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(column));
        // 3.3 删除列族
        // delete.addFamily(Bytes.toBytes(family));

        table.delete(delete);
        table.close();
    }


    public static void main(String[] args) throws IOException {
        // 单行写入和读取
        // putCell("bigdata","student","1001","info","name","zhangsan");
        // putCell("bigdata","student","1002","info","name","lisi");
        // System.out.println(getCell("bigdata", "student", "1001", "info", "name"));
        // System.out.println(getCell("bigdata", "student", "1002", "info", "name"));

        // 扫描数据
        List<String> strings = scanRows("bigdata", "student", "1001", "2000");
        for (String string : strings) {
            System.out.println(string);
        }
    }
}
