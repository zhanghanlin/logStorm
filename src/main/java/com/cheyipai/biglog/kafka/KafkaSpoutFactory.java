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

public class KafkaSpoutFactory {

    private SpoutConfig spoutConfig;
    private KafkaSpout kafkaSpout;

    private KafkaSpoutFactory() {
        BrokerHosts brokerHosts = new ZkHosts(zkHosts);
        spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new LogScheme());
        spoutConfig.ignoreZkOffsets = true;

        //将offset汇报到哪个zk集群,相应配置
        List<String> zkServers = Lists.newArrayList(zkOffsetHosts);
        spoutConfig.zkServers = zkServers;
        spoutConfig.zkPort = zkOffsetPort;

        spoutConfig.startOffsetTime = 0l;//zk OffsetRequest.LatestTime();
        kafkaSpout = new KafkaSpout(spoutConfig);
    }

    public static KafkaSpout getInstance() {
        return new KafkaSpoutFactory().kafkaSpout;
    }
}
