package com.cheyipai.biglog.hbase;

import com.cheyipai.biglog.model.Entity;
import com.cheyipai.biglog.utils.DateUtils;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static com.cheyipai.biglog.utils.Global.*;

public class HBaseCommunicator implements Serializable {

    private static final long serialVersionUID = -247440075453438034L;

    static final Logger LOG = LoggerFactory.getLogger(HBaseCommunicator.class);

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
            LOG.error("exception while checking table's existence! error : {}", e.getMessage(), e);
            e.printStackTrace();
        }
        return false;
    }

    /**
     * creates a table
     */
    private final void createTable(final String tableName, final ArrayList<String> colFamilies) {
        try {
            if (tableExists(tableName)) {
                return;
            }
            HTableDescriptor desc = new HTableDescriptor(tableName);
            for (String family : colFamilies) {
                HColumnDescriptor meta = new HColumnDescriptor(family.getBytes());
                desc.addFamily(meta);
            }
            admin.createTable(desc);
            LOG.info("create table : {} success!", tableName);
        } catch (Exception e) {
            LOG.error("Exception creating table in HBase! error : {} ", e.getMessage(), e);
            e.printStackTrace();
        }
    }

    public final void addRow(Entity t) {
        try {
            table = new HTable(conf, getNowTName());
            Put put = constructRow(t);
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据Bean格式Put
     *
     * @param t
     * @return
     */
    private final Put constructRow(Entity t) {
        Put put = new Put(Bytes.toBytes(t.getRowKey()));
        for (Map.Entry<String, List<String>> entry : t.getFamily().entrySet()) {
            String columnFamily = entry.getKey();
            for (String f : entry.getValue()) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(f), Bytes.toBytes(t.get(f).toString()));
            }
        }
        return put;
    }

    /**
     * bolt初始化创建表
     */
    public final void boltCreateTable() {
        createLock.lock();
        try {
            ArrayList<String> colFamilies = Lists.newArrayList();
            colFamilies.add(row_family);
            colFamilies.add(col_family);
            createTable(getNowTName(), colFamilies);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            createLock.unlock();
            TableTimer timer = new TableTimer();
            timer.showTimer();
        }
    }

    /**
     * 获取当前月表名
     *
     * @return
     */
    String getNowTName() {
        return table_name_prefix + DateUtils.getMonthDate();
    }

    class TableTimer {
        public void showTimer() {
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    createTask();
                }
            };
            //设置执行时间
            Calendar calendar = Calendar.getInstance();
            int year = calendar.get(Calendar.YEAR);
            int month = calendar.get(Calendar.MONTH);
            int day = calendar.get(Calendar.DAY_OF_MONTH);//每天
            //定制每天的23:59:00执行，
            calendar.set(year, month, day, 23, 59, 00);
            Date date = calendar.getTime();
            Timer timer = new Timer();
            timer.schedule(task, date);
//            timer.schedule(task, 10000);
        }

        /**
         * 定时创建Table
         */
        void createTask() {
            taskLock.lock();
            try {
                String table_name = table_name_prefix + DateUtils.getMonthDate();
                String table_name_next = table_name_prefix + DateUtils.getMonthDate(1);
                ArrayList<String> colFamilies = Lists.newArrayList();
                colFamilies.add(row_family);
                colFamilies.add(col_family);
                createTable(table_name, colFamilies);
                createTable(table_name_next, colFamilies);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                taskLock.unlock();
            }
        }
    }
}
