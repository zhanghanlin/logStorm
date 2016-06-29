package com.cheyipai.biglog.utils;

public class Prop {

    /**
     * 属性文件加载对象
     */
    private static PropertiesLoader loader = new PropertiesLoader("storm.properties");

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
    public static boolean isLocalMode = false;  //默认非本地
    public static Integer taskParallelism;
    public static Integer workerNum;    //集群中分配多少个进程来运行这个拓扑

    static {
        zkHosts = loader.getProperty("zk.hosts");
        zkOffsetHosts = loader.getProperty("zk.offset.hosts").split(",");
        zkOffsetPort = loader.getInteger("zk.offset.port");
        zkRoot = loader.getProperty("zk.root");
        topic = loader.getProperty("kafka.topic");
        spoutId = loader.getProperty("kafka.spout.id");
        kafkaBorkerHosts = loader.getProperty("kafka.broker.host");
        topologyName = loader.getProperty("storm.topology.name");
        spoutNum = loader.getInteger("storm.spout.num");
        boltNum = loader.getInteger("storm.bolt.num");
        isDebug = loader.getBoolean("storm.debug");
        taskParallelism = loader.getInteger("storm.max.taskParallelism");
        workerNum = loader.getInteger("storm.worker.num");
    }
}
