package com.cheyipai.biglog.model;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.util.Date;

public class BigLog implements Serializable {

    private static final long serialVersionUID = -7637218139941572984L;

    private String app; //应用标识
    private String line;    //产品线
    private String userId;    //用户ID
    private Date date;  //日志时间
    private byte[] content; //日志内容
    private int type;   //用户操作类型    加价等操作

    public BigLog() {
        super();
    }

    public BigLog(String app, String line, String userId, Date date, byte[] content, int type) {
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

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String toJson() {
        return JSONObject.toJSONString(this);
    }
}