package com.cheyipai.biglog.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseUtils {

    static final Logger LOG = LoggerFactory.getLogger(HbaseUtils.class);

    static Configuration configuration = HBaseConfiguration.create();

    static HBaseAdmin admin = null;

    static {
        configuration.addResource("hbase-site.xml");
        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static ResultScanner rs = null;

    /**
     * 删除一张表
     *
     * @param tableName
     */
    public static void dropTable(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("删除表" + tableName + "成功");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 根据rowKey删除一条记录
     *
     * @param rowKey
     */
    public static void deleteRow(String rowKey, String tableName) {
        try {
            HTable table = new HTable(configuration, tableName);
            List list = new ArrayList();
            Delete delete = new Delete(rowKey.getBytes());
            list.add(delete);
            table.delete(list);
            System.out.println("删除行" + rowKey + "成功!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询所有数据
     */
    public static void queryAll(String tableName) {
        try {
            HTable table = new HTable(configuration, tableName);
            rs = table.getScanner(new Scan());
            for (Result r : rs) {
                sysInfo(r);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rs.close();
        }
    }

    /**
     * 单条件查询,根据rowkey查询唯一一条记录
     */
    public static void queryByCondition(String rowKey, String tableName) {
        try {
            HTable table = new HTable(configuration, tableName);
            Get scan = new Get(rowKey.getBytes());// 根据rowKey查询
            Result r = table.get(scan);
            sysInfo(r);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 单条件按查询，查询多条记录
     */
    public static void queryByCondition(String column, String value, String tableName) {
        try {
            HTable table = new HTable(configuration, tableName);
            ;
            Filter filter = new SingleColumnValueFilter(Bytes
                    .toBytes(column), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes(value)); // 当列column的值为value时进行查询
            Scan s = new Scan();
            s.setFilter(filter);
            rs = table.getScanner(s);
            for (Result r : rs) {
                sysInfo(r);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rs.close();
        }

    }

    /**
     * 组合条件查询
     */
    public static void queryByCondition(List<String> columms, List<String> values, String tableName) {
        try {
            HTable table = new HTable(configuration, tableName);
            List<Filter> filters = new ArrayList<Filter>();
            for (int i = 0; i < columms.size(); i++) {
                Filter filter = new SingleColumnValueFilter(Bytes
                        .toBytes(columms.get(i)), null, CompareFilter.CompareOp.EQUAL, Bytes
                        .toBytes(values.get(i)));
                filters.add(filter);
            }
            FilterList filterList = new FilterList(filters);
            Scan scan = new Scan();
            scan.setFilter(filterList);
            rs = table.getScanner(scan);
            for (Result r : rs) {
                sysInfo(r);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rs.close();
        }
    }

    static void sysInfo(Result result) {
        StringBuffer sb = new StringBuffer();
        sb.append("rowKey:");
        sb.append(new String(result.getRow()));
        for (Cell cell : result.rawCells()) {
            sb.append(",column:");
            sb.append(new String(cell.getQualifier()));
            sb.append(",value:");
            sb.append(new String(cell.getValue()));
        }
        System.out.println(sb.toString());
    }

    public static void main(String[] args) {
        dropTable("big_log_201606");
    }
}
