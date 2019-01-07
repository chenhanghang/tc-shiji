package com.ifeng.jishi.recommend;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

//基于物品推荐模型
public class CFModel {
    // 相似的物品， 物品 -> [(物品, similar),...]
    private final JavaPairRDD<String, List<Tuple2<String, Double>>> itemSimilaries;
    // 相似的用户 用户 -> [(物品, score),...]
    private final JavaPairRDD<String, List<Tuple2<String, Double>>> userSimilaries;

    public CFModel(JavaPairRDD<String, List<Tuple2<String, Double>>> itemSimilaries,
                   JavaPairRDD<String, List<Tuple2<String, Double>>> userSimilaries) {
        this.itemSimilaries = itemSimilaries;
        this.userSimilaries = userSimilaries;
    }

    public JavaPairRDD<String, List<Tuple2<String, Double>>> getItemSimilaries() {
        return itemSimilaries;
    }

    public JavaPairRDD<String, List<Tuple2<String, Double>>> getUserSimilaries() {
        return userSimilaries;
    }

    /**
     * 根据用户id，获取推荐结果
     * @param uid
     * @return
     */
    public List<Tuple2<String, Double>> predict(String uid){
        Map<String, List<Tuple2<String,Double>>> user_similaries_map = userSimilaries.collectAsMap();
        return user_similaries_map.get(uid);
    }

    /**
     * 计算召回率和准确率
     */
    public Tuple2<Double, Double> computeRecallPrecition(List<Row> tests){
        //转化为Map
        Map<String,Set<String>> testMap = new HashMap<>();
        //nRecall 用户行为操作数
        //nPrecition 预测数量
        long nRecall=0, nPrecition=0, hit=0;

        for(Row row: tests) {
            if(testMap.get(row.getString(0))!=null){
                testMap.get(row.getString(0)).add(row.getString(1));
            } else {
                Set temp = new HashSet<String>();
                temp.add(row.getString(1));
                testMap.put(row.getString(0), temp);
            }
        }

        Iterator<Map.Entry<String, Set<String>>> entries = testMap.entrySet().iterator();
        while(entries.hasNext()) {
            Map.Entry<String, Set<String>> entry = entries.next();
            List<Tuple2<String, Double>> recommends = this.predict(entry.getKey());
            if(recommends == null) continue;
            Set recommendSet = recommends.stream().map(t->t._1).collect(Collectors.toSet());
            Set testSet = entry.getValue();
            //求交集
            recommendSet.retainAll(testSet);
            hit += recommendSet.size();
            nRecall += testSet.size();
            nPrecition += recommends.size();
        }

        return new Tuple2<Double, Double>(hit/(1.0*nRecall), hit/(1.0*nPrecition));
    }
}
