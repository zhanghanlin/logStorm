package com.cheyipai.biglog.utils;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 全局配置类
 */
public class Global {

    /**
     * 同步锁
     */
    public final static Lock createLock = new ReentrantLock();
    public final static Lock taskLock = new ReentrantLock();
    public static final String spoutName = "spout";
    public static final String boltName = "hbaseBolt";
    public static final String table_name_prefix = "big_log_";
    public static final String row_family = "ROWD";
    public static final String col_family = "COLD";
    public static final String prop_file = "storm.properties";

}
