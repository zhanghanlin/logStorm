package com.cheyipai.biglog.utils;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * 全局配置类
 */
public class Global {

    /**
     * 保存全局属性值
     */
    private static Map<String, String> map = Maps.newHashMap();

    /**
     * 属性文件加载对象
     */
    private static PropertiesLoader loader = new PropertiesLoader("storm.properties");

    /**
     * 获取配置
     */
    public static String getConfig(String key) {
        String value = map.get(key);
        if (value == null) {
            value = loader.getProperty(key);
            map.put(key, value != null ? value : StringUtils.EMPTY);
        }
        return value;
    }
}
