package com.cheyipai.biglog.bolt;

import com.cheyipai.biglog.hbase.HBaseCommunicator;
import com.cheyipai.biglog.model.BigLog;
import com.cheyipai.biglog.utils.BeanUtils;
import com.cheyipai.biglog.utils.Convert;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
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

public class LogHBaseBolt implements IRichBolt {

    private static final long serialVersionUID = 8259058888219739163L;

    private static final Logger LOG = LoggerFactory.getLogger(LogHBaseBolt.class);

    private static final String TABLE_NAME = "log_table";
    private static final String TABLE_COLUMN_FAMILY_NAME = "log_family";

    private OutputCollector collector;
    private Configuration configuration;
    private HBaseCommunicator communicator;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            this.configuration = constructConfiguration();
            this.communicator = new HBaseCommunicator(configuration);
            if (!communicator.tableExists(TABLE_NAME)) {
                ArrayList<String> fList = Lists.newArrayList(TABLE_COLUMN_FAMILY_NAME);
                communicator.createTable(TABLE_NAME, fList);
            }
        } catch (Exception e) {
            String errMsg = "Error retrievinging connection and access to dangerousEventsTable";
            LOG.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        BigLog bigLog = Convert.tuple2Bean(tuple, new BigLog());
        LOG.info("LogHBaseBolt-execute : {}", bigLog.toJson());
        if (LOG.isDebugEnabled()) {
            LOG.debug("LogHBaseBolt-execute : {}", bigLog.toJson());
        }
        //Store the event in HBase
        try {
            Put put = constructRow(TABLE_COLUMN_FAMILY_NAME, bigLog);
            communicator.addRow(TABLE_NAME, put);
            LOG.info("Success inserting event into HBase table[" + TABLE_NAME + "]");
        } catch (Exception e) {
            LOG.error("	Error inserting event into HBase table[" + TABLE_NAME + "]", e);
        }
        //collector.emit(input, new Values(driverId, truckId, eventTime, eventType, longitude, latitude, incidentTotalCount, driverName, routeId, routeName));
        //acknowledge even if there is an error
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        try {
//            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BeanUtils.getFieldNames(BigLog.class)));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public static Configuration constructConfiguration() {
        Configuration config = HBaseConfiguration.create();
        return config;
    }


    private Put constructRow(String columnFamily, BigLog bigLog) {
        String rowKey = bigLog.getUserId() + bigLog.getDate().getTime() + bigLog.getLine() + bigLog.getType();
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("app"), Bytes.toBytes(bigLog.getApp()));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("line"), Bytes.toBytes(bigLog.getLine()));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("userId"), Bytes.toBytes(bigLog.getUserId()));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("date"), Bytes.toBytes(bigLog.getDate().getTime()));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("content"), bigLog.getContent());
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("type"), Bytes.toBytes(bigLog.getType()));
        return put;
    }
}
