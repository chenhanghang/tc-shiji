package com.ifeng.jishi.util;

import net.sf.json.JSONObject;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.*;

/**
 * @author chh
 */
public class RedisReaderUtil {

    private static final Logger logger = LoggerFactory.getLogger(RedisReaderUtil.class);
    private static  Properties pro = ConfigUtil.getConfigProperties();
    private static JedisPool jedisPool;
    static {
        try {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(100);
            config.setMaxIdle(20);
            config.setMaxWaitMillis(1800000);
            jedisPool = new JedisPool(config, pro.getProperty("REDIS_HOST"), Integer.parseInt(pro.getProperty("REDIS_PORT")), 5000, false);
        } catch (Exception e) {
            logger.error("First create JedisPool error : " + e);
        }
    }

    public static String getItem(String key) {
        Jedis jedis = jedisPool.getResource();
        jedis.select(Integer.parseInt(pro.getProperty("REDIS_DB_ID")));
        String result = jedis.get(key);
        jedis.close();
        return result;

    }

    public static Set<String> getClickKeys() {
        Jedis jedis = jedisPool.getResource();
        jedis.select(Integer.parseInt(pro.getProperty("REDIS_DB_ID")));
        Set<String> result = jedis.keys("click:*");
        jedis.close();
        return result;
    }

    public static Long delItem(String... key) {
        Jedis jedis = jedisPool.getResource();
        jedis.select(7);
        Long result = jedis.del(key);
        jedis.close();
        return result;
    }


    /**
     * 读取redis中的点击数据
     * @param count 控制取的数量 0 不限制
     */
    public static List<Row> getClicksFromRedis(int count) {
        List<Row> items = new ArrayList<>();
        Jedis jedis = jedisPool.getResource();
        jedis.select(Integer.parseInt(pro.getProperty("REDIS_DB_ID")));
        String cursor = "0";
        ScanParams scanParams = new ScanParams();
        scanParams.match("click:*");
        ScanResult<String> scanResult;
        do {
            scanResult = jedis.scan(String.valueOf(cursor), scanParams);
            cursor = scanResult.getStringCursor();
            scanResult.getResult().forEach(key -> {
                String value = jedis.get(key);
                JSONObject object = JSONObject.fromObject(value);
                Row tempRow = RowFactory.create(object.getString("userkey"), object.getString("gid"), 1.0);
                items.add(tempRow);
            });
            //todo delete
            if(items.size() > count) break;
        } while (!cursor.equals("0"));

        jedis.close();
        return items;
    }

    /**
     * 读取redis中的所有点击数据
     */
    public static List<Row> getClicksFromRedis() {
        List<Row> items = new ArrayList<>();
        Jedis jedis = jedisPool.getResource();
        jedis.select(Integer.parseInt(pro.getProperty("REDIS_DB_ID")));
        String cursor = "0";
        ScanParams scanParams = new ScanParams();
        scanParams.match("click:*");
        ScanResult<String> scanResult;
        do {
            scanResult = jedis.scan(String.valueOf(cursor), scanParams);
            cursor = scanResult.getStringCursor();
            scanResult.getResult().forEach(key -> {
                String value = jedis.get(key);
                JSONObject object = JSONObject.fromObject(value);
                Row tempRow = RowFactory.create(object.getString("userkey"), object.getString("gid"), 1.0);
                items.add(tempRow);
            });
        } while (!cursor.equals("0"));

        jedis.close();
        return items;
    }

    /**
     *  保存推荐的商品到redis中
     */
    public static void saveRecommendToRedis(Map<String, String> recommendGoods) {
        Jedis jedis = jedisPool.getResource();
        jedis.select(Integer.parseInt(pro.getProperty("REDIS_DB_ID")));
        jedis.hmset("recommend:goods", recommendGoods);
        jedis.close();
    }
}
