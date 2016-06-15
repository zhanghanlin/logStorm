package com.cheyipai.biglog.utils;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

public class BeanUtils {

    static final Logger LOG = LoggerFactory.getLogger(BeanUtils.class);

    public static List<String> getFields(Object t) {
        List<String> fields = Lists.newArrayList();
        List<Field> list = getFields(t.getClass());
        for (Field f : list) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            fields.add(f.getName());
        }
        return fields;
    }

    private static List<Field> getFields(Class clz) {
        List<Field> list = Lists.newArrayList();
        list.addAll(Arrays.asList(clz.getDeclaredFields()));
        if (clz.getGenericSuperclass() != null) {
            list.addAll(Arrays.asList(clz.getSuperclass().getDeclaredFields()));
        }
        return list;
    }


    public static List<Object> getFieldValues(Object t) {
        List<Object> values = Lists.newArrayList();
        List<Field> list = getFields(t.getClass());
        for (Field field : list) {
            String methodName = "get" + field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1);
            try {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                Method method = t.getClass().getMethod(methodName);
                Object o = method.invoke(t);
                if (null != o) {
                    values.add(o);
                }
            } catch (Exception e) {
                LOG.error("error:{}", e.getMessage(), e);
            }
        }
        return values;
    }
}
