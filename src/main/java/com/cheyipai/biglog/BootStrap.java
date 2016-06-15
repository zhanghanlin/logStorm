package com.cheyipai.biglog;

import com.cheyipai.biglog.storm.LogTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootStrap {

    private static final Logger LOG = LoggerFactory.getLogger(BootStrap.class);

    public static void main(String[] args) {
        try {
            LOG.info("start begin...");
            LogTopology.start(args);
            LOG.info("start success!");
        } catch (Exception e) {
            LOG.error("start exception : {}", e.getMessage(), e);
            e.printStackTrace();
        }
    }
}
