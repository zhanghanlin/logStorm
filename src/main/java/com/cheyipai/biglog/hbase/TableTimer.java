package com.cheyipai.biglog.hbase;

import com.cheyipai.biglog.utils.DateUtils;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;

import static com.cheyipai.biglog.utils.Global.*;

/**
 * 定时器,用于定时创建HBaseTable
 */
@Component
public class TableTimer {

    static final Logger LOG = LoggerFactory.getLogger(TableTimer.class);

    static Admin admin = null;

    static Connection conn = null;

    static {
        conn = HBaseConnection.getConnection();
        try {
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @PostConstruct
    public void timer() {
        LOG.info("timer start ...");
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
        //定制每天的23:55:00执行，
        calendar.set(year, month, day, 23, 55, 00);
        Date date = calendar.getTime();
        Timer timer = new Timer();
        //间隔24H执行一次
        timer.schedule(task, date, 24 * 60 * 60 * 1000);
    }

    /**
     * 定时创建Table
     */
    void createTask() {
        try {
            LOG.info("timer - createTable begin ...");
            //当月Table
            String table_name = hbase_table_prefix + DateUtils.getMonthDate();
            //预创建下月Table
            String table_name_next = hbase_table_prefix + DateUtils.getMonthDate(1);
            ArrayList<String> colFamilies = Lists.newArrayList(hbase_family);
            HBaseHandler.createTable(admin, table_name, colFamilies);
            HBaseHandler.createTable(admin, table_name_next, colFamilies);
            LOG.info("timer - createTable end ...");
        } catch (Exception e) {
            LOG.error("createTask error : {}", e.getMessage(), e);
            e.printStackTrace();
        }
    }
}
