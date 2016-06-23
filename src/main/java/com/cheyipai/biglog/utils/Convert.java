package com.cheyipai.biglog.utils;

import com.google.common.collect.Maps;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.storm.tuple.Tuple;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

public class Convert {

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
