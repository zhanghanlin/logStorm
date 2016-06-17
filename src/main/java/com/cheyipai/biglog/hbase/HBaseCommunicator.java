package com.cheyipai.biglog.hbase;

import com.cheyipai.biglog.model.BigLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static com.cheyipai.biglog.utils.Global.lock;

public class HBaseCommunicator implements Serializable {

    private static final long serialVersionUID = -247440075453438034L;

    private static Configuration conf;
    private HTable table = null;
    private HBaseAdmin admin = null;

    public HBaseCommunicator() {
        this.conf = HBaseConfiguration.create();
        try {
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * check if the table exists
     */
    final boolean tableExists(final String tableName) {
        try {
            if (admin.tableExists(tableName)) {
                return true;
            }
        } catch (Exception e) {
            System.out.println("Exception occured while checking table's existence");
            e.printStackTrace();
        }
        return false;
    }


    /**
     * creates a table
     */
    public final void createTable(final String tableName, final ArrayList<String> colFamilies) {
        lock.lock();
        try {
            if (tableExists(tableName)) {
                return;
            }
            HTableDescriptor desc = new HTableDescriptor(tableName);
            for (int i = 0; i < colFamilies.size(); i++) {
                HColumnDescriptor meta = new HColumnDescriptor(colFamilies.get(i).getBytes());
                desc.addFamily(meta);
            }
            admin.createTable(desc);
        } catch (Exception e) {
            System.out.println("Exception occured creating table in hbase");
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public final void addRow(String tableName, String columnFamily, BigLog bigLog) {
        try {
            table = new HTable(conf, tableName);
            Put put = constructRow(columnFamily, bigLog);
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private final Put constructRow(String columnFamily, BigLog bigLog) {
        String rowKey = bigLog.getUserId() + bigLog.getDate() + bigLog.getLine() + bigLog.getType();
        Put put = new Put(Bytes.toBytes(rowKey));
        for (String f : bigLog.getFields()) {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(f), Bytes.toBytes(bigLog.get(f).toString()));
        }
        return put;
    }
}
