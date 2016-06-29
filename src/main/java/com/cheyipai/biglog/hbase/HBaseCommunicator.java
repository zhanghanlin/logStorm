package com.cheyipai.biglog.hbase;

import com.cheyipai.biglog.model.DataModel;
import com.cheyipai.biglog.utils.DateUtils;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import static com.cheyipai.biglog.utils.Global.*;

/**
 * HBase
 */
public class HBaseCommunicator implements Serializable {

    private static final long serialVersionUID = -247440075453438034L;

    private Admin admin = null;
    private Connection conn;

    private static HBaseCommunicator communicator = null;

    public static HBaseCommunicator getInstance() {
        if (communicator == null) {
            communicator = new HBaseCommunicator();
        }
        return communicator;
    }

    /**
     * 初始化
     */
    private HBaseCommunicator() {
        try {
            this.conn = HBaseConnection.getConnection();
            this.admin = conn.getAdmin();
            initCreateTable();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化创建Table
     */
    public void initCreateTable() {
        ArrayList<String> colFamilies = Lists.newArrayList(hbase_family);
        String tableName = hbase_table_prefix + DateUtils.getMonthDate();
        HBaseHandler.createTable(admin, tableName, colFamilies);
    }

    /**
     * 新增列
     *
     * @param model
     */
    public final void addRow(DataModel model) {
        HBaseHandler.addRow(conn, model);
    }
}
