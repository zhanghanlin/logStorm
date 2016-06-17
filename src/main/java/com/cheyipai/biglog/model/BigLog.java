package com.cheyipai.biglog.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import static com.cheyipai.biglog.utils.Global.col_family;
import static com.cheyipai.biglog.utils.Global.row_family;

public class BigLog extends Entity {

    private static final long serialVersionUID = -7637218139941572984L;

    private String app; //应用标识
    private String line;    //产品线
    private String userId;    //用户ID
    private long date;  //日志时间
    private String content; //日志内容
    private int type;   //用户操作类型    加价等操作

    public BigLog() {
        super();
    }

    public BigLog(String app, String line, String userId, long date, String content, int type) {
        this.app = app;
        this.line = line;
        this.userId = userId;
        this.date = date;
        this.content = content;
        this.type = type;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getDate() {
        return date;
    }

    public void setDate(long date) {
        this.date = date;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    @Override
    public String getRowKey() {
        return userId + date + line + type;
    }

    @Override
    public Map<String, List<String>> getFamily() {
        Map<String, List<String>> map = Maps.newConcurrentMap();
        List<String> rowCols = Lists.newArrayList();
        List<String> colCols = Lists.newArrayList();
        rowCols.add("userId");
        rowCols.add("date");
        rowCols.add("line");
        rowCols.add("type");
        colCols.add("app");
        colCols.add("content");
        map.put(row_family, rowCols);
        map.put(col_family, colCols);
        return map;
    }
}