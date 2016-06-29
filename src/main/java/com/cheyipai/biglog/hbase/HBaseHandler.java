package com.cheyipai.biglog.hbase;

import com.cheyipai.biglog.model.DataModel;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.cheyipai.biglog.utils.Global.hbase_family;

/**
 * HBase工具
 */
public class HBaseHandler {

    static final Logger LOG = LoggerFactory.getLogger(HBaseHandler.class);

    public final static Lock createLock = new ReentrantLock();

    /**
     * 创建一个Table
     *
     * @param tableName
     * @param colFamilies
     */
    public static void createTable(Admin admin, final String tableName, final ArrayList<String> colFamilies) {
        try {
            createLock.lock();
            TableName tn = TableName.valueOf(tableName);
            if (admin.tableExists(tn)) {
                return;
            }
            HTableDescriptor desc = new HTableDescriptor(tn);
            for (String family : colFamilies) {
                HColumnDescriptor meta = new HColumnDescriptor(family.getBytes());
                desc.addFamily(meta);
            }
            admin.createTable(desc);
            LOG.info("create table : {} success!", tableName);
        } catch (Exception e) {
            LOG.error("Exception creating table in HBase! error : {} ", e.getMessage(), e);
            e.printStackTrace();
        } finally {
            createLock.unlock();
        }
    }

    /**
     * 新增数据
     *
     * @param model
     */
    public static void addRow(Connection conn, DataModel model) {
        Table table = null;
        try {
            table = conn.getTable(model.getTable());
            Put put = new Put(Bytes.toBytes(model.getRowKey()));
            for (Map.Entry<String, Object> entry : model.getData().entrySet()) {
                put.addColumn(Bytes.toBytes(hbase_family),
                        Bytes.toBytes(entry.getKey()),
                        Bytes.toBytes(entry.getValue().toString()));
            }
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
