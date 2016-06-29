package com.cheyipai.biglog.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * HBase Connection
 */
public class HBaseConnection {

    private static Connection conn;

    private HBaseConnection() {
    }

    public static Connection getConnection() {
        if (conn == null) {
            Configuration configuration = HBaseConfiguration.create();
            try {
                conn = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }
}
