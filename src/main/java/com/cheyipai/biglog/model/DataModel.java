package com.cheyipai.biglog.model;

import com.alibaba.fastjson.JSONObject;
import com.cheyipai.biglog.utils.DateUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;

public class DataModel {

    private String[] rowKey;
    private JSONObject data;
    private String table;

    public String getRowKey() {
        String rk = "";
        for (String field : rowKey) {
            rk += data.getString(field);
        }
        return StringUtils.leftPad(rk, 20, "0");
    }

    public void setRowKey(String[] rowKey) {
        this.rowKey = rowKey;
    }

    public JSONObject getData() {
        return data;
    }

    public void setData(JSONObject data) {
        this.data = data;
    }

    public TableName getTable() {
        return TableName.valueOf(table + "_" + DateUtils.getMonthDate());
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String toJson() {
        return JSONObject.toJSONString(this);
    }
}
