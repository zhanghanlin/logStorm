package com.cheyipai.biglog.storm;

import com.cheyipai.biglog.model.BigLog;
import com.cheyipai.biglog.utils.BeanUtils;
import com.cheyipai.biglog.utils.Convert;
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
        Object obj = Convert.byte2Object(bytes);
        if (obj instanceof BigLog) {
            return new Values(BeanUtils.getFieldValues(obj).toArray());
        } else {
            LOG.error("Object is not an instance of BigLog.");
            return null;
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(BeanUtils.getFieldNames(BigLog.class));
    }
}
