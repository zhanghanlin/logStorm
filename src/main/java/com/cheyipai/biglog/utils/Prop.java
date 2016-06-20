package com.cheyipai.biglog.utils;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

import static com.cheyipai.biglog.utils.Global.prop_file;

public class Prop {

    /**
     * 保存全局属性值
     */
    private static Map<String, String> map = Maps.newHashMap();

    /**
     * 属性文件加载对象
     */
    private static PropertiesLoader loader = new PropertiesLoader(prop_file);

    public static String zkHosts;
    public static String[] zkOffsetHosts;
    public static Integer zkOffsetPort;
    public static String kafkaBorkerHosts;
    public static String zkRoot;
    public static String topic;
    public static String spoutId;
    public static String topologyName;
    public static Integer spoutNum;
    public static Integer boltNum;
    public static boolean isDebug;
    public static boolean isLocalMode;
    public static Integer taskParallelism;
    public static Integer workerNum;    //集群中分配多少个进程来运行这个拓扑

    static {
        zkHosts = getConfig("zk.hosts");
        zkOffsetHosts = getConfig("zk.offset.hosts").split(",");
        zkOffsetPort = Integer.valueOf(getConfig("zk.offset.port"));
        zkRoot = getConfig("zk.root");
        topic = getConfig("kafka.topic");
        spoutId = getConfig("kafka.spout.id");
        kafkaBorkerHosts = getConfig("kafka.broker.host");
        topologyName = getConfig("storm.topology.name");
        spoutNum = Integer.valueOf(getConfig("storm.spout.num"));
        boltNum = Integer.valueOf(getConfig("storm.bolt.num"));
        isDebug = Boolean.valueOf(getConfig("storm.debug"));
        isLocalMode = Boolean.valueOf(getConfig("storm.localmode"));
        taskParallelism = Integer.valueOf(getConfig("storm.max.taskParallelism"));
        workerNum = Integer.valueOf(getConfig("storm.worker.num"));
    }

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
