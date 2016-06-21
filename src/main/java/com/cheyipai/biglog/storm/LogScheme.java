package com.cheyipai.biglog.storm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cheyipai.biglog.model.BigLog;
import com.cheyipai.biglog.utils.BeanUtils;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class LogScheme implements Scheme {

    private static final Logger LOG = LoggerFactory.getLogger(LogScheme.class);

    @Override
    public List<Object> deserialize(ByteBuffer buffer) {
        byte[] bytes = Utils.toByteArray(buffer);
        try {
            BigLog log = JSONObject.toJavaObject(JSON.parseObject(new String(bytes)), BigLog.class);
            return new Values(log.beanValues().toArray());
        } catch (Exception e) {
            LOG.error("Object is not an instance of BigLog. {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(BeanUtils.getBeanField(BigLog.class));
    }
}
