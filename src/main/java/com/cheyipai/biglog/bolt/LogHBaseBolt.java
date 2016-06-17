package com.cheyipai.biglog.bolt;

import com.cheyipai.biglog.hbase.HBaseCommunicator;
import com.cheyipai.biglog.model.BigLog;
import com.cheyipai.biglog.utils.BeanUtils;
import com.cheyipai.biglog.utils.Convert;
import com.cheyipai.biglog.utils.DateUtils;
import com.google.common.collect.Lists;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

import static com.cheyipai.biglog.utils.Prop.family_name;
import static com.cheyipai.biglog.utils.Prop.table_name_prefix;

public class LogHBaseBolt implements IRichBolt {

    private static final long serialVersionUID = 8259058888219739163L;

    private static final Logger LOG = LoggerFactory.getLogger(LogHBaseBolt.class);

    public static String table_name;

    private OutputCollector collector;
    private HBaseCommunicator communicator;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            this.communicator = new HBaseCommunicator();
            table_name = table_name_prefix + DateUtils.getMonthDate();
            if (!communicator.tableExists(table_name)) {
                ArrayList<String> fList = Lists.newArrayList(family_name);
                communicator.createTable(table_name, fList);
            }
        } catch (Exception e) {
            String errMsg = "Error retrieving connection and access to dangerousEventsTable";
            LOG.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        BigLog bigLog = Convert.tuple2Bean(tuple, new BigLog());
        if (LOG.isDebugEnabled()) {
            LOG.debug("LogHBaseBolt-execute : {}", bigLog.toJson());
        }
        try {
            communicator.addRow(table_name, family_name, bigLog);
            LOG.info("Success inserting event into HBase table[" + table_name + "]");
        } catch (Exception e) {
            LOG.error("	Error inserting event into HBase table[" + table_name + "]", e);
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
