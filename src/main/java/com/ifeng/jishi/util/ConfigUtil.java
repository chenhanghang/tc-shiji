package com.ifeng.jishi.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtil {

    private static final Logger logger = LoggerFactory.getLogger(ConfigUtil.class);

    private static Properties getConfig(String filePath) {
        InputStream inputStream = null;
        Properties prop = new Properties();
        try {
            inputStream = ConfigUtil.class.getResourceAsStream(filePath);
            prop.load(inputStream);
        } catch (Exception e) {
            logger.error("init properties error: " + filePath, e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    logger.error("can't close the input stream!", e);
                }
            }
        }
        return prop;
    }

    public static Properties getConfigProperties(){
        return ConfigUtil.getConfig("/config.properties");
    }
    public static void main(String[] args) {
        Properties prop = ConfigUtil.getConfig("/config.properties");
        logger.info(prop.getProperty("READ_NUM_DATA"));
    }
}
