package com.cheyipai.biglog.utils;

import com.google.common.collect.Lists;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

public class BeanUtils {

    public static List<String> getBeanField(Class clz) {
        List<String> fields = Lists.newArrayList();
        List<Field> list = getFields(clz);
        for (Field f : list) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            fields.add(f.getName());
        }
        return fields;
    }

    public static List<Field> getFields(Class clz) {
        List<Field> list = Lists.newArrayList();
        list.addAll(Arrays.asList(clz.getDeclaredFields()));
        if (clz.getGenericSuperclass() != null) {
            list.addAll(Arrays.asList(clz.getSuperclass().getDeclaredFields()));
        }
        return list;
    }

    public static <T> List<Object> getBeanValue(T t) {
        List<Object> values = Lists.newArrayList();
        List<Field> list = getFields(t.getClass());
        for (Field field : list) {
            try {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                PropertyDescriptor descriptor = new PropertyDescriptor(field.getName(), t.getClass());
                Method method = descriptor.getReadMethod();
                Object o = method.invoke(t);
                if (null != o) {
                    values.add(o);
                }
            } catch (IntrospectionException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return values;
    }

    public static <T> Object getBeanValue(String field, T t) {
        try {
            PropertyDescriptor descriptor = new PropertyDescriptor(field, t.getClass());
            Method method = descriptor.getReadMethod();
            return method.invoke(t);
        } catch (IntrospectionException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }
}
