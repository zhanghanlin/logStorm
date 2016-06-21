package com.cheyipai.biglog.producer;

import com.cheyipai.biglog.model.BigLog;
import com.google.common.collect.Lists;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

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
    static int count = 1;

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
            BigLog log = new BigLog();
            log.setApp("app-" + i);
            log.setClientId("clientId-" + i);
            log.setClientType("PC");
            log.setContent("{\"key\":\"testkey " + i + "\",\"value\":\"testvalue " + i + "\"}");
            log.setException("");
            log.setLine("1000" + i);
            log.setLogTime((new Date()).getTime() + "");
            log.setLogVersion("1.2.7");
            log.setRequestBody("param=" + i);
            log.setRequestHeader("GET http://test.cheyipai.com/get \n" +
                    "Host: test.cheyipai.com \n" +
                    "Accept:*/* \n" +
                    "Pragma: no-cache \n" +
                    "Cache-Control: no-cache \n" +
                    "Referer: http://test.cheyipai.com/ \n" +
                    "User-Agent:Mozilla/4.04[en](Win95;I;Nav) \n" +
                    "Range:bytes=554554- ");
            log.setResponseCode("200");
            log.setServerIp(s.getHostAddress());
            log.setServerName(s.getHostName());
            log.setServiceName("get");
            log.setTraceId("traceId-" + i);
            log.setUserId(UUID.randomUUID().toString().substring(0, 8));
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
                        KeyedMessage<String, byte[]> data = new KeyedMessage(topic, l.toJson().getBytes());
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