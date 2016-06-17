package com.cheyipai.biglog.bolt;

import com.cheyipai.biglog.hbase.HBaseCommunicator;
import com.cheyipai.biglog.model.BigLog;
import com.cheyipai.biglog.utils.BeanUtils;
import com.cheyipai.biglog.utils.Convert;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LogHBaseBolt implements IRichBolt {

    private static final long serialVersionUID = 8259058888219739163L;

    private static final Logger LOG = LoggerFactory.getLogger(LogHBaseBolt.class);

    private OutputCollector collector;
    private HBaseCommunicator communicator;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.communicator = new HBaseCommunicator();
        communicator.boltCreateTable();
    }

    @Override
    public void execute(Tuple tuple) {
        BigLog bigLog = Convert.tuple2Bean(tuple, new BigLog());
        if (LOG.isDebugEnabled()) {
            LOG.debug("LogHBaseBolt-execute : {}", bigLog.toJson());
        }
        try {
            communicator.addRow(bigLog);
        } catch (Exception e) {
            LOG.error("exception insert HBase table! error : {} ", e.getMessage(), e);
        }
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BeanUtils.getBeanField(BigLog.class)));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
