package com.cheyipai.biglog.producer;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang.StringUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
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

    static List<JSONObject> list = Lists.newArrayList();

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

    static final String[] lins = {"B2B", "B2C", "C2C"};
    static final String[] logTypes = {"bigLog", "errorLog", "elseLog"};
    static final String[] apps = {"WEB", "MAPI", "API", "PROTAL"};
    static final String[] clientTypes = {"PC", "IOS", "MAC"};
    static final String[] requestCode = {"200", "500", "404", "302"};
    static final String[] services = {"/get", "/put", "/update", "/delete"};

    String userId() {
        Integer random = (new Random()).nextInt(100);
        return "U" + StringUtils.leftPad(random.toString(), 4, "0");
    }

    /**
     * 初始化List
     */
    void init() throws UnknownHostException {
        InetAddress s = InetAddress.getLocalHost();
        for (Integer i = 0; i < count; i++) {
            JSONObject log = new JSONObject();
            log.put("logTime", (new Date().getTime()));
            log.put("logType", logTypes[new Random().nextInt(3)]);
            log.put("traceId", new Random().nextInt(count));
            log.put("productLine", lins[new Random().nextInt(3)]);
            log.put("appName", apps[new Random().nextInt(4)]);
            log.put("serverName", s.getHostName());
            log.put("serverIp", s.getAddress());
            log.put("userId", userId());
            log.put("clientIp", s.getHostAddress());
            log.put("clientType", clientTypes[new Random().nextInt(3)]);
            log.put("serviceName", services[new Random().nextInt(4)]);
            log.put("requestUrl", "http://test.cheyipai.com");
            log.put("requestHeader", "requestHeader");
            log.put("requestBody", "requestBody");
            log.put("responseCode", requestCode[new Random().nextInt(4)]);
            log.put("responseContent", "{\"key\":\"key " + i + "\",\"value\":\"value " + i + "\"}");
            log.put("referUrl", "http://cheyipai.com");
            log.put("exceptionType", "");
            log.put("exceptionStack", "");
            list.add(log);
        }
    }

    public void run() {
        final CountDownLatch countDown = new CountDownLatch(count);
        final String table_prefix = "big_log";
        final String[] rowKey = new String[]{"userId", "logTime"};
        for (final JSONObject log : list) {
            exec.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject object = new JSONObject();
                        object.put("data", log);
                        object.put("rowKey", rowKey);
                        object.put("table", table_prefix);
                        KeyedMessage<String, byte[]> data = new KeyedMessage(topic, object.toJSONString().getBytes());
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
        System.out.println("All thread complete");
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