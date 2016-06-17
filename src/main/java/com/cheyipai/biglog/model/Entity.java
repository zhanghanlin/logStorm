package com.cheyipai.biglog.model;

import com.alibaba.fastjson.JSONObject;
import com.cheyipai.biglog.utils.BeanUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

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

    /**
     * Bean的列族数据,包含列族中的列
     * @return
     */
    public abstract Map<String,List<String>> getFamily();

    public String toJson() {
        return JSONObject.toJSONString(this);
    }
}
