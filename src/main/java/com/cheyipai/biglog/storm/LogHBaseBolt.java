package com.cheyipai.biglog.storm;

import com.cheyipai.biglog.model.BigLog;
import com.cheyipai.biglog.utils.BeanUtils;
import com.cheyipai.biglog.utils.Convert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
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

import java.io.IOException;
import java.util.Map;

public class LogHBaseBolt implements IRichBolt {

    private static final long serialVersionUID = 8259058888219739163L;

    private static final Logger LOG = LoggerFactory.getLogger(LogHBaseBolt.class);

    private static final String TABLE_NAME = "log_table";
    private static final String TABLE_COLUMN_FAMILY_NAME = "log_family";

    private OutputCollector collector;
    private HConnection connection;
    private HTableInterface attachmentsTable;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
//        try {
//            this.connection = HConnectionManager.createConnection(constructConfiguration());
//            this.attachmentsTable = connection.getTable(TABLE_NAME);
//        } catch (Exception e) {
//            String errMsg = "Error retrievinging connection and access to dangerousEventsTable";
//            LOG.error(errMsg, e);
//            throw new RuntimeException(errMsg, e);
//        }
    }

    @Override
    public void execute(Tuple tuple) {
        BigLog bigLog = Convert.tuple2Bean(tuple, new BigLog());
        LOG.info("LogHBaseBolt-execute : {}", bigLog.toJson());
        if (LOG.isDebugEnabled()) {
            LOG.debug("LogHBaseBolt-execute : {}", bigLog.toJson());
        }
        //Store the event in HBase
//        try {
//            Put put = constructRow(TABLE_COLUMN_FAMILY_NAME, logId, attachmentName, model);
//            this.attachmentsTable.put(put);
//            LOG.info("Success inserting event into HBase table[" + TABLE_NAME + "]");
//        } catch (Exception e) {
//            LOG.error("	Error inserting event into HBase table[" + TABLE_NAME + "]", e);
//        }
        //collector.emit(input, new Values(driverId, truckId, eventTime, eventType, longitude, latitude, incidentTotalCount, driverName, routeId, routeName));
        //acknowledge even if there is an error
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        try {
//            attachmentsTable.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BeanUtils.getFields(new BigLog())));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public static Configuration constructConfiguration() {
        Configuration config = HBaseConfiguration.create();
        return config;
    }

    private Put constructRow(String columnFamily, int logId, String attachmentName, byte[] attachments) {
        LOG.info("Record with key[" + logId + "] going to be inserted...");
        Put put = new Put(Bytes.toBytes(logId));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(attachmentName), attachments);
        return put;
    }
}
