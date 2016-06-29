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
            LOG.info("BootStrap start...");
            String topologyName = Prop.topologyName;
            if (args != null && args.length > 1) {
                if (args.length > 0) {
                    topologyName = args[0];
                }
                if (args.length > 1) {
                    isLocalMode = Boolean.valueOf(args[1]);
                }
            }
            LOG.info("topologyName : {} , isLocalMode : {}", topologyName, isLocalMode);
            //获取Spring配置
            ApplicationContext instance = new ClassPathXmlApplicationContext("applicationContext.xml");
            //调用定时器
            instance.getBean("tableTimer");
            //启动Topo
            LogTopology.start(topologyName);
            LOG.info("BootStrap success!");
        } catch (Exception e) {
            LOG.error("BootStrap exception : {}", e.getMessage(), e);
            e.printStackTrace();
        }
    }
}
