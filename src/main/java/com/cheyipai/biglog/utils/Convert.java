package com.cheyipai.biglog.utils;

import com.google.common.collect.Maps;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.storm.tuple.Tuple;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

public class Convert {

    public static Object byte2Object(byte[] bytes) {
        Object obj = null;
        ByteArrayInputStream bi = null;
        ObjectInputStream oi = null;
        try {
            bi = new ByteArrayInputStream(bytes);
            oi = new ObjectInputStream(bi);
            obj = oi.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bi != null) {
                try {
                    bi.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (oi != null) {
                try {
                    oi.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return obj;
    }

    public static <T> T tuple2Bean(Tuple tuple, T t) {
        Map<String, Object> map = tuple2Map(tuple);
        try {
            BeanUtils.populate(t, map);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return t;
    }

    private static Map<String, Object> tuple2Map(Tuple tuple) {
        Map<String, Object> map = Maps.newConcurrentMap();
        List<String> list = tuple.getFields().toList();
        for (String key : list) {
            map.put(key, tuple.getValueByField(key));
        }
        return map;
    }
}
