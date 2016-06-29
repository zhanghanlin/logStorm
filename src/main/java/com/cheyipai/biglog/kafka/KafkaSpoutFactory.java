package com.cheyipai.biglog.kafka;

import com.cheyipai.biglog.storm.LogScheme;
import com.google.common.collect.Lists;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;

import java.util.List;

import static com.cheyipai.biglog.utils.Prop.*;

/**
 * Kafka
 */
public class KafkaSpoutFactory {

    private SpoutConfig spoutConfig;
    private KafkaSpout kafkaSpout;

    /**
     * Kafka配置
     */
    private KafkaSpoutFactory() {
        BrokerHosts brokerHosts = new ZkHosts(zkHosts);
        spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new LogScheme());
        /**
         * true - 则每次拓扑重新启动时，都会从开头读取消息
         * false - 第一次启动，从开头读取，之后的重启均是从offset中读取
         */
        spoutConfig.ignoreZkOffsets = false;

        //使用zk记录storm的offset信息
        List<String> zkServers = Lists.newArrayList(zkOffsetHosts);
        spoutConfig.zkServers = zkServers;
        spoutConfig.zkPort = zkOffsetPort;
        /**
         * OffsetRequest.LatestTime() - 从最新的开始读
         * OffsetRequest.EarliestTime() - 从头开始读
         * 0L   - 从ZK记录的Offset读
         */
        spoutConfig.startOffsetTime = 0L;
        kafkaSpout = new KafkaSpout(spoutConfig);
    }

    /**
     * 获取Kafka对象实例
     *
     * @return
     */
    public static KafkaSpout getInstance() {
        return (new KafkaSpoutFactory()).kafkaSpout;
    }
}
