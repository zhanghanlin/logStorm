package com.cheyipai.biglog.storm;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.nio.ByteBuffer;
import java.util.List;

import static com.cheyipai.biglog.utils.Global.fields;

public class LogScheme implements Scheme {

    @Override
    public List<Object> deserialize(ByteBuffer buffer) {
        byte[] bytes = Utils.toByteArray(buffer);
        return new Values(new String(bytes));
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(fields);
    }
}
