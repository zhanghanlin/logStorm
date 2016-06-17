package com.cheyipai.biglog.producer;

import com.cheyipai.biglog.model.BigLog;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.Properties;

import static com.cheyipai.biglog.utils.Prop.kafkaBorkerHosts;
import static com.cheyipai.biglog.utils.Prop.topic;

public class LogProducer {

    public static void main(String args[]) throws InterruptedException, IOException {
        Producer<String, byte[]> producer = getProducer();
        BigLog testData = new BigLog("app", "1", "456", (new Date()).getTime(), "加价200", 1);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        byte[] testData_bytes = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(testData);
            testData_bytes = bos.toByteArray();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        KeyedMessage<String, byte[]> data = new KeyedMessage(topic, testData_bytes);
        producer.send(data);
    }

    public static Producer<String, byte[]> getProducer() {
        Properties props = new Properties();
        props.put("metadata.broker.list", kafkaBorkerHosts);
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("request.required.acks", "-1");
        ProducerConfig config = new ProducerConfig(props);
        return new Producer(config);
    }
}
