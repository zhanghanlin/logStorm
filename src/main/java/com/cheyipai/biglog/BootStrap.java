package com.cheyipai.biglog;

import com.cheyipai.biglog.storm.LogTopology;
import com.cheyipai.biglog.utils.Prop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static com.cheyipai.biglog.utils.Prop.isLocalMode;

public class BootStrap {

    private static final Logger LOG = LoggerFactory.getLogger(BootStrap.class);

    public static void main(String[] args) {
        try {
            LOG.info("start begin...");
            String topologyName = Prop.topologyName;
            if (args != null && args.length > 1) {
                if (args.length > 0) {
                    topologyName = args[0];
                }
                if (args.length > 1) {
                    isLocalMode = Boolean.valueOf(args[1]);
                }
            }
            LOG.info("isLocalMode : {}", isLocalMode);
            ApplicationContext instance =
                    new ClassPathXmlApplicationContext("applicationContext.xml");
            instance.getBean("tableTimer");
            LogTopology.start(topologyName);
            LOG.info("start success!");
        } catch (Exception e) {
            LOG.error("start exception : {}", e.getMessage(), e);
            e.printStackTrace();
        }
    }
}
