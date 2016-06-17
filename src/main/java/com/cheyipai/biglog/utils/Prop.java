package com.cheyipai.biglog.utils;

public class Prop {

    public static final String spoutName = "spout";
    public static final String boltName = "hbaseBolt";
    public static final String table_name_prefix = "big_log_";
    public static final String family_name = "LD";

    public static String zkHosts;
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
        zkHosts = Global.getConfig("zk.hosts");
        zkRoot = Global.getConfig("zk.root");
        topic = Global.getConfig("kafka.topic");
        spoutId = Global.getConfig("kafka.spout.id");
        kafkaBorkerHosts = Global.getConfig("kafka.broker.host");
        topologyName = Global.getConfig("storm.topology.name");
        spoutNum = Integer.valueOf(Global.getConfig("storm.spout.num"));
        boltNum = Integer.valueOf(Global.getConfig("storm.bolt.num"));
        isDebug = Boolean.valueOf(Global.getConfig("storm.debug"));
        isLocalMode = Boolean.valueOf(Global.getConfig("storm.localmode"));
        taskParallelism = Integer.valueOf(Global.getConfig("storm.max.taskParallelism"));
        workerNum = Integer.valueOf(Global.getConfig("storm.worker.num"));
    }
}
