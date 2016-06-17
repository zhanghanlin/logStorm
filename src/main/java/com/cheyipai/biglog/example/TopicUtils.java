package com.cheyipai.biglog.example;

import kafka.admin.TopicCommand;

import static com.cheyipai.biglog.utils.Prop.zkHosts;

public class TopicUtils {

    public static void create(String topic, String partitions, String replication) {
        String[] options = new String[]{
                "--create",
                "--zookeeper",
                zkHosts,
                "--partitions",
                partitions,
                "--topic",
                topic,
                "--replication-factor",
                replication
        };
        command(options);
    }

    public static void create(String topic) {
        create(topic, "8", "1");
    }

    public static void list() {
        String[] options = new String[]{
                "--list",
                "--zookeeper",
                zkHosts
        };
        command(options);
    }

    public static void describe(String topic) {
        String[] options = new String[]{
                "--describe",
                "--zookeeper",
                zkHosts,
                "--topic",
                topic,
        };
        command(options);
    }

    public static void command(String[] options) {
        TopicCommand.main(options);
    }
}
