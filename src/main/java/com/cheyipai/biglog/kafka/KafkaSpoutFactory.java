package com.cheyipai.biglog.kafka;

import com.cheyipai.biglog.storm.LogScheme;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;

import static com.cheyipai.biglog.utils.Prop.*;

public class KafkaSpoutFactory {

    private SpoutConfig spoutConfig;
    private KafkaSpout kafkaSpout;

    private KafkaSpoutFactory() {
        BrokerHosts brokerHosts = new ZkHosts(zkHosts);
        spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new LogScheme());
        kafkaSpout = new KafkaSpout(spoutConfig);
    }

    public static KafkaSpout getInstance() {
        return new KafkaSpoutFactory().kafkaSpout;
    }
}
