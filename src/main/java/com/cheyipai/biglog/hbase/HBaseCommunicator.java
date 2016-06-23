package com.cheyipai.biglog.hbase;

import com.cheyipai.biglog.model.Entity;
import com.cheyipai.biglog.utils.DateUtils;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import static com.cheyipai.biglog.utils.Global.col_family;
import static com.cheyipai.biglog.utils.Global.row_family;
import static com.cheyipai.biglog.utils.Global.table_name_prefix;

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
        ArrayList<String> colFamilies = Lists.newArrayList(row_family, col_family);
        String tableName = DateUtils.getNowTName(table_name_prefix);
        HBaseHandler.createTable(admin, tableName, colFamilies);
    }

    /**
     * 新增列
     *
     * @param t
     * @param tableName
     */
    public final void addRow(Entity t, String tableName) {
        HBaseHandler.addRow(conn, t, tableName);
    }
}
