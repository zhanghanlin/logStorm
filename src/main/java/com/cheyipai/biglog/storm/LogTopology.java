package com.cheyipai.biglog.storm;

import com.cheyipai.biglog.bolt.LogHBaseBolt;
import com.cheyipai.biglog.kafka.KafkaSpoutFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import static com.cheyipai.biglog.utils.Prop.*;

public class LogTopology {

    static final String spoutName = "spout";
    static final String boltName = "hbaseBolt";

    /**
     * 设置TopologyBuilder
     *
     * @return
     */
    public static StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(spoutName, KafkaSpoutFactory.getInstance(), spoutNum);
        builder.setBolt(boltName, new LogHBaseBolt(), boltNum).shuffleGrouping(spoutName);
        return builder.createTopology();
    }

    /**
     * 设置Config
     *
     * @return
     */
    public static Config buildConfig() {
        Config conf = new Config();
        if (isLocalMode) {
            conf.setDebug(isDebug);
            conf.setMaxTaskParallelism(taskParallelism);
        }
        conf.setNumWorkers(workerNum);
        return conf;
    }

    /**
     * 启动Topology
     *
     * @param topologyName
     * @throws Exception
     */
    public static void start(String topologyName) throws Exception {
        Config config = buildConfig();
        //是否本地模式
        if (isLocalMode) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config, buildTopology());
            Utils.sleep(10 * 60 * 1000); // 10 min
            cluster.killTopology(topologyName);
            cluster.shutdown();
        } else {
            StormSubmitter.submitTopology(topologyName, config, buildTopology());
        }
    }
}
