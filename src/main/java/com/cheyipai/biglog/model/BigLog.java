package com.cheyipai.biglog.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import static com.cheyipai.biglog.utils.Global.col_family;
import static com.cheyipai.biglog.utils.Global.row_family;

public class BigLog extends Entity {

    private static final long serialVersionUID = -7637218139941572984L;

    private String logVersion;  //日志版本
    private String logTime; //日志时间时间戳
    private String traceId; //TraceId
    private String line;    //产品线
    private String app; //应用标识
    private String serverName;  //服务器名
    private String serverIp;    //服务器IP
    private String userId;    //用户ID
    private String clientId;    //记录设备ID
    private String clientType;  //无线客户端类型,pc ios 等
    private String serviceName; //调用服务接口名,地址等
    private String requestHeader;   //请求头
    private String requestBody;   //请求体
    private String responseCode;    //响应代码
    private String content; //日志内容
    private String exception;   //异常堆栈


    public BigLog() {
        super();
    }

    public String getLogVersion() {
        return logVersion;
    }

    public void setLogVersion(String logVersion) {
        this.logVersion = logVersion;
    }

    public String getLogTime() {
        return logTime;
    }

    public void setLogTime(String logTime) {
        this.logTime = logTime;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public String getServerIp() {
        return serverIp;
    }

    public void setServerIp(String serverIp) {
        this.serverIp = serverIp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientType() {
        return clientType;
    }

    public void setClientType(String clientType) {
        this.clientType = clientType;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getRequestHeader() {
        return requestHeader;
    }

    public void setRequestHeader(String requestHeader) {
        this.requestHeader = requestHeader;
    }

    public String getRequestBody() {
        return requestBody;
    }

    public void setRequestBody(String requestBody) {
        this.requestBody = requestBody;
    }

    public String getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(String responseCode) {
        this.responseCode = responseCode;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getException() {
        return exception;
    }

    public void setException(String exception) {
        this.exception = exception;
    }

    @Override
    public String getRowKey() {
        return userId + logTime;
    }

    @Override
    public Map<String, List<String>> familyArray() {
        Map<String, List<String>> map = Maps.newConcurrentMap();
        List<String> rowCols = Lists.newArrayList();
        List<String> colCols = Lists.newArrayList();
        rowCols.add("userId");
        rowCols.add("logTime");

        colCols.add("line");
        colCols.add("app");
        colCols.add("logVersion");
        colCols.add("traceId");
        colCols.add("serverName");
        colCols.add("serverIp");
        colCols.add("clientId");
        colCols.add("clientType");
        colCols.add("serviceName");
        colCols.add("requestHeader");
        colCols.add("requestBody");
        colCols.add("responseCode");
        colCols.add("content");
        colCols.add("exception");
        map.put(row_family, rowCols);
        map.put(col_family, colCols);
        return map;
    }
}