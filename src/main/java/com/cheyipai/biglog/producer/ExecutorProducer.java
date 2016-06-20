package com.cheyipai.biglog.producer;

import com.cheyipai.biglog.model.BigLog;
import com.google.common.collect.Lists;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.cheyipai.biglog.utils.Prop.kafkaBorkerHosts;
import static com.cheyipai.biglog.utils.Prop.topic;

public class ExecutorProducer {

    /**
     * CPU核数
     */
    static final int preceCount = Runtime.getRuntime().availableProcessors();

    static ExecutorService exec = Executors.newFixedThreadPool(preceCount);

    static List<BigLog> list = Lists.newArrayList();

    static Producer<String, byte[]> producer = getProducer();

    /**
     * 集合数量
     */
    static int count = 10;

    public static void main(String[] args) {
        ExecutorProducer test = new ExecutorProducer();
        try {
            test.init();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        test.run();
    }

    /**
     * 初始化List
     */
    void init() throws UnknownHostException {
        InetAddress s = InetAddress.getLocalHost();
        for (int i = 0; i < count; i++) {
            BigLog log = new BigLog("app-" + (i + 1), "1", UUID.randomUUID().toString().substring(0, 8),
                    (new Date()).getTime(), "{\"key\":\"testkey " + i + "\",\"value\":\"testvalue " + i + "\"}", 1,
                    s.getHostName(), s.getHostAddress());
            list.add(log);
        }
    }

    public void run() {
        final CountDownLatch countDown = new CountDownLatch(count);
        for (BigLog log : list) {
            final BigLog l = log;
            exec.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutput out = null;
                        byte[] testData_bytes = null;
                        try {
                            out = new ObjectOutputStream(bos);
                            out.writeObject(l);
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
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        countDown.countDown();
                    }
                }
            });
        }
        exec.shutdown();
        try {
            countDown.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("all thread complete");
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