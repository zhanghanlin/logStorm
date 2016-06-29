package com.cheyipai.biglog.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

    static final String formatPattern_month = "yyyyMM";

    public static Date getDate() {
        Calendar calendar = Calendar.getInstance();
        return calendar.getTime();
    }

    public static long getTimeStamp() {
        Calendar calendar = Calendar.getInstance();
        return calendar.getTimeInMillis();
    }

    public static String getMonthDate() {
        return getMonthDate(0);
    }


    public static String getMonthDate(int month) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MONTH, month);
        SimpleDateFormat format = new SimpleDateFormat(formatPattern_month);
        return format.format(calendar.getTime());
    }

    public static String getDate(String pattern) {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        return format.format(calendar.getTime());
    }
}
