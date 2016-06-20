package com.cheyipai.biglog.example;

import kafka.admin.TopicCommand;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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

    public static void delete(String topic) {
        String[] options = new String[]{
                "--delete",
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

    public static void main(String[] args) {
        if (args != null && args.length > 0) {
            try {
                PropertyDescriptor descriptor = new PropertyDescriptor(args[0], TopicUtils.class);
                Method method = descriptor.getReadMethod();
                method.invoke(args[1]);
            } catch (IntrospectionException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }
}
