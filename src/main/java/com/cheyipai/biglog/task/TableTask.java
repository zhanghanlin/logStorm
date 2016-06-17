package com.cheyipai.biglog.task;

import com.cheyipai.biglog.example.HbaseUtils;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public class TableTask {

    public static void showTimer() {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                HbaseUtils.createTask();
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
    }

    public void afterPropertiesSet() throws Exception {
        HbaseUtils.createTask();
        showTimer();
    }
}
