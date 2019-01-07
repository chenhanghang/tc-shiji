package com.ifeng.jishi;

import com.ifeng.jishi.recommend.CFModel;
import com.ifeng.jishi.recommend.ContentBasedJob;
import com.ifeng.jishi.recommend.ItemBasedJob;
import com.ifeng.jishi.util.ConfigUtil;
import com.ifeng.jishi.util.RedisReaderUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String args[]) {
        logger.info("############### Begin Spark job for jishi. ########################");
        Properties pro = ConfigUtil.getConfigProperties();
        if(Boolean.parseBoolean(pro.getProperty("DEGUB_MODE")))
            System.setProperty("hadoop.home.dir", "c:\\winutil\\");
        long startTime=System.currentTimeMillis();

        SparkSession spark = SparkSession.builder()
                .appName("ShiJiRecommendJob")
                .master(pro.getProperty("SPARK_MASTER"))
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // 从redis获取点击数据，大小可调整
        logger.info("#1 Begin get clicks from redis!\n");
        long startTimeRedis=System.currentTimeMillis();
        List<Row> list;
        if(Boolean.parseBoolean(pro.getProperty("DEGUB_MODE"))){
            String READ_NUM_DATA = (ConfigUtil.getConfigProperties()).getProperty("READ_NUM_DATA");
            list = RedisReaderUtil.getClicksFromRedis(Integer.parseInt(READ_NUM_DATA));
        } else {
            list = RedisReaderUtil.getClicksFromRedis();
        }
        logger.info("Get all items count" + list.size());
        JavaRDD<Row> data = jsc.parallelize(list);
        long endTimeRedis=System.currentTimeMillis();

        //基于用户的协同过滤
        logger.info("#2 Begin start item based recommend job!\n");
        long startTimeCf=System.currentTimeMillis();
        ItemBasedJob itemBasedJob = new ItemBasedJob();
        int numRecommendations = 10;//最多推荐20个商品
        int minVisitedPerItem = 1;  //物品的最小访问次数
        int maxPrefsPerUser = 1000; // 每个用户最大点击数量，超出为无效用户
        int minPrefsPerUser = 1;    // 每个用户最小点击数量，超出为无效用户
        int maxSimilaritiesPerItem = 500; //每个物品最多相似的个数
        int maxPrefsPerUserForRec = 100;  //采用前100（100个足够了）点击数据做推荐
        double minSimilar = 0.1; //两个物品最小相似度
        CFModel model = itemBasedJob.run(jsc,
                data,
                numRecommendations,
                minVisitedPerItem,
                maxPrefsPerUser,
                minPrefsPerUser,
                maxSimilaritiesPerItem,
                maxPrefsPerUserForRec,
                minSimilar);
        long endTimeCf=System.currentTimeMillis();

        //基于物品向量余弦相似度计算
        logger.info("#3 Begin start content recommend job!\n");
        long startTimeContent=System.currentTimeMillis();
        ContentBasedJob contentBasedJob = new ContentBasedJob();
        int recommendsPerUser = Integer.parseInt(pro.getProperty("RECOMMENDS_PER_USER")); //每个用户推荐的数量
        Map<String, String> user_similaries_map = contentBasedJob.run(spark, model, recommendsPerUser);
//            //物品 -> 物品 相似度
//            Map<String, List<Tuple2<String, Float>>> item_similaries_map = model.getItem_similaries().collectAsMap();
//            logger.info("=============== 商品相似度item_similaries_map ==============\n");
        //用户 -> 物品 相关度
        //Map<String, List<Tuple2<String, Float>>> user_similaries_map = model.getUser_similaries().collectAsMap();
        long endTimeContent=System.currentTimeMillis();

        //保存结果到redis
        logger.info("#4 Save recommend result to redis!\n");
        RedisReaderUtil.saveRecommendToRedis(user_similaries_map);

        long endTime=System.currentTimeMillis();
        logger.info("================== Finish Spark job for shiji. Total cost "+ (endTime-startTime) +"ms======================");
        logger.info("================== Read clicks from Redis. Cost "+ (endTimeRedis-startTimeRedis) +"ms======================");
        logger.info("================== CF recommend. Cost "+ (endTimeCf-startTimeCf) +"ms======================");
        logger.info("================== Content recommend. Cost "+ (endTimeContent-startTimeContent) +"ms======================");

//        Tuple2<Double, Double> recallPrecition = model.computeRecallPrecition(RedisReaderUtil.getClicksFromRedis(2000));
//        logger.info("*******************准确率："+ recallPrecition._2 + " 召回率： " + recallPrecition._1);

        System.exit(0);
    }

}
