package com.cheyipai.biglog.bolt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cheyipai.biglog.hbase.HBaseCommunicator;
import com.cheyipai.biglog.model.DataModel;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

import static com.cheyipai.biglog.utils.Global.fields;

public class LogHBaseBolt implements IRichBolt {

    private static final long serialVersionUID = 8259058888219739163L;

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
            JSONObject object = JSONObject.parseObject(tuple.getValueByField(fields).toString());
            communicator.addRow(JSON.toJavaObject(object, DataModel.class));
        } catch (Exception e) {
            e.printStackTrace();
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
        declarer.declare(new Fields(fields));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
