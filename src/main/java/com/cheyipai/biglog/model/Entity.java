package com.cheyipai.biglog.model;

import com.alibaba.fastjson.JSONObject;
import com.cheyipai.biglog.utils.BeanUtils;

import java.io.Serializable;
import java.util.List;

public abstract class Entity implements Serializable {

    private static final long serialVersionUID = 6143131774453861945L;

    private String rowKey;

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public Object get(String field) {
        return BeanUtils.getBeanValue(field, this);
    }

    public List<String> getFields() {
        return BeanUtils.getBeanField(this.getClass());
    }

    public List<Object> getValues() {
        return BeanUtils.getBeanValue(this);
    }

    public String toJson() {
        return JSONObject.toJSONString(this);
    }
}
