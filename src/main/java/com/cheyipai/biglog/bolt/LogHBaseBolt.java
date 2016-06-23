package com.cheyipai.biglog.bolt;

import com.cheyipai.biglog.hbase.HBaseCommunicator;
import com.cheyipai.biglog.model.BigLog;
import com.cheyipai.biglog.utils.BeanUtils;
import com.cheyipai.biglog.utils.Convert;
import com.cheyipai.biglog.utils.DateUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.cheyipai.biglog.utils.Global.table_name_prefix;

public class LogHBaseBolt implements IRichBolt {

    private static final long serialVersionUID = 8259058888219739163L;

    private static final Logger LOG = LoggerFactory.getLogger(LogHBaseBolt.class);

    private OutputCollector collector;
    private HBaseCommunicator communicator;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.communicator = HBaseCommunicator.getInstance();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            BigLog bigLog = Convert.tuple2Bean(tuple, new BigLog());
            communicator.addRow(bigLog, DateUtils.getNowTName(table_name_prefix));
        } catch (Exception e) {
            LOG.error("Exception insert HBase table! error : {} ", e.getMessage(), e);
            this.collector.fail(tuple);
            return;
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
