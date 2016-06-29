package com.cheyipai.biglog.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseExample {

    static final Logger LOG = LoggerFactory.getLogger(HBaseExample.class);

    static Configuration configuration = HBaseConfiguration.create();

    static Admin admin = null;

    static Connection conn = null;

    static {
        configuration.addResource("hbase-site.xml");
        try {
            conn = ConnectionFactory.createConnection(configuration);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除一张表
     *
     * @param tableName
     */
    public static void dropTable(TableName tableName) {
        try {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            LOG.info("删除表" + tableName + "成功");
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
    public static void deleteRow(String rowKey, TableName tableName) {
        try {
            Table table = conn.getTable(tableName);
            List list = new ArrayList();
            Delete delete = new Delete(rowKey.getBytes());
            list.add(delete);
            table.delete(list);
            LOG.info("删除行" + rowKey + "成功!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String rowKey = "";
        TableName tableName = TableName.valueOf("big_log_201606");
//        deleteRow(rowKey, tableName);
        dropTable(tableName);
    }
}
