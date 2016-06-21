package com.cheyipai.biglog;

import com.cheyipai.biglog.storm.LogTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.cheyipai.biglog.utils.Prop.isLocalMode;

public class BootStrap {

    private static final Logger LOG = LoggerFactory.getLogger(BootStrap.class);

    public static void main(String[] args) {
        try {
            LOG.info("start begin...");
            if (args != null && args.length > 0) {
                isLocalMode = Boolean.valueOf(args[0]);
            }
            LOG.info("isLocalMode : {}", isLocalMode);
            LogTopology.start();
            LOG.info("start success!");
        } catch (Exception e) {
            LOG.error("start exception : {}", e.getMessage(), e);
            e.printStackTrace();
        }
    }
}
