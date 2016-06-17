package com.cheyipai.biglog.storm;

import com.cheyipai.biglog.bolt.LogHBaseBolt;
import com.cheyipai.biglog.kafka.KafkaSpoutFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import static com.cheyipai.biglog.utils.Global.*;
import static com.cheyipai.biglog.utils.Prop.*;

public class LogTopology {

    public static StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(spoutName, KafkaSpoutFactory.getInstance(), spoutNum);
        builder.setBolt(boltName, new LogHBaseBolt(), boltNum)
                .shuffleGrouping(spoutName);
        return builder.createTopology();
    }

    public static Config buildConfig() {
        Config conf = new Config();
        if (isLocalMode) {
            conf.setDebug(isDebug);
            conf.setMaxTaskParallelism(taskParallelism);
        }
        conf.setNumWorkers(workerNum);
        return conf;
    }

    public static void start(String[] args) throws Exception {
        Config config = buildConfig();
        if (args != null && args.length > 0) {
            StormSubmitter.submitTopology(args[0], config, buildTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config, buildTopology());
            Utils.sleep(10 * 60 * 1000); // 10 mins
            cluster.killTopology(topologyName);
            cluster.shutdown();
        }
    }
}
