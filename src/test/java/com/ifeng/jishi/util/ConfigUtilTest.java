package com.ifeng.jishi.util;

import org.junit.Test;

import static org.junit.Assert.*;
import java.util.Properties;

public class ConfigUtilTest {

    @Test
    public void testConfig() {
        Properties prop = ConfigUtil.getConfigProperties();
        assertEquals(prop.get("REDIS_PORT"),"6379");
    }

}

