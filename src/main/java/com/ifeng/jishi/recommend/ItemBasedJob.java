package com.ifeng.jishi.recommend;

import com.google.common.collect.Lists;
import com.ifeng.jishi.util.MinHeap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author  chh
 * 推荐任务类，基于物品的推荐，采用余弦相似度
 */

public class ItemBasedJob  implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ItemBasedJob.class);

    /**
     * 存放(goodsId1,goodsId2)
     */
    class ItemPair implements Serializable {
        String a;
        String b;

        public ItemPair(String a, String b) {
            this.a = a.hashCode() >= b.hashCode()? a: b;
            this.b = a.hashCode() < b.hashCode()? a: b;
        }

        @Override
        public int hashCode() {
            return a.hashCode() + b.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            ItemPair other = (ItemPair) obj;
            if (other.a == a && other.b == b) {
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return String.format("[%s, %s]", a, b);
        }
    }

    /**
     * @param jsc
     * @param data                   格式[(userid,itemid,Double score),...]
     * @param numRecommendations     推荐的数量
     * @param maxPrefsPerUser        过滤点击次数过多的用户，最多点击次数
     * @param minPrefsPerUser        过滤新用户，这里最少点击次数
     * @param maxSimilaritiesPerItem 每个物品最多相似的个数
     * @param maxPrefsPerUserForRec  in final computer user's recommendations stage,
     *                               only use top maxPrefsPerUserForRec number of prefs for each user
     * @param minSimilar           两个商品最小相似度，小于minSimilar则规定商品之间没有关系
     * @return ItemBasedCFSparkJob.CFModel model
     */
    public CFModel run(JavaSparkContext jsc,
                       JavaRDD<Row> data,
                       final int numRecommendations,
                       final int minVisitedPerItem,
                       final int maxPrefsPerUser,
                       final int minPrefsPerUser,
                       final int maxSimilaritiesPerItem,
                       final int maxPrefsPerUserForRec,
                       final double minSimilar) {
        logger.info("Recommend is running!");
        //过滤合法的用户
        Map<String, Tuple2<Set<String>, Set<String>>> user_visited = data
                .filter(row -> row.getDouble(2) > 0)
                .mapToPair(row -> new Tuple2<>(row.getString(0), new Tuple2<>(row.getString(1), row.getDouble(2))))
                .aggregateByKey(new ArrayList<Tuple2<String, Double>>(),
                        (list, t) -> {
                            list.add(t);
                            return list;
                        },
                        (l1, l2) -> {
                            l1.addAll(l2);
                            return l1;
                        })//uid -> [(gid,score),...]
                //用户点击过少或者过多忽略
                .filter(t -> t._2.size() <= maxPrefsPerUser && t._2.size() >= minPrefsPerUser)
                .mapValues(list -> {
                    if (list.size() > maxPrefsPerUserForRec) {
                        list = (ArrayList<Tuple2<String, Double>>) list.stream()
                                .sorted((a, b) -> b._2.compareTo(a._2))
                                .collect(Collectors.toList());
                    }
                    //items use to computer user similary items
                    Set<String> visited_a = new HashSet<>();
                    //abandoned items which not use to computer user similary items
                    Set<String> visited_b = list.size() <= maxPrefsPerUserForRec ? null : new HashSet<>();
                    for (int i = 0; i < list.size(); i++) {
                        if (i < maxPrefsPerUserForRec) {
                            visited_a.add(list.get(i)._1);
                        }
                        if (i >= maxPrefsPerUserForRec) {
                            visited_b.add(list.get(i)._1);
                        }
                    }
                    return new Tuple2<Set<String>, Set<String>>(visited_a, visited_b);
                })
                .collectAsMap();//最终返回Map类型结果
        Broadcast<Map<String, Tuple2<Set<String>, Set<String>>>> user_visited_bd = jsc.broadcast(new HashMap<>(user_visited));

        //合法的items集合
        List<String> legal_items = data.mapToPair(row -> new Tuple2<String, Integer>(row.getString(1), 1))
                .reduceByKey((a,b) -> a+b)
                .filter(t -> t._2 >= minVisitedPerItem)
                .keys()
                .collect();
        Broadcast<Set<String>> legal_items_bd = jsc.broadcast(new HashSet<>(legal_items));

        //过滤后有效的数据
        JavaRDD<Row> filted_data = data
                .filter(row -> user_visited_bd.getValue().containsKey(row.getString(0))
                        && legal_items_bd.getValue().contains(row.getString(1)));
        filted_data.cache();

        // 物品 -> [(用户, score)] 倒排表
        JavaPairRDD<String, Iterable<Tuple2<String, Double>>> item_user_list = filted_data
                .mapToPair(row -> new Tuple2<>(row.getString(1),
                        new Tuple2<String, Double>(row.getString(0), row.getDouble(2))))
                .groupByKey();
        item_user_list.cache();

        // 物品的访问量，用于计算物品相似度
        Map<String, Double> item_norm = item_user_list
                .mapValues(iter -> {
                    Double score = 0.0;
                    for (Tuple2<String, Double> t : iter) {
                        score += t._2 * t._2;
                    }
                    return score;
                })
                .collectAsMap();
        Broadcast<Map<String, Double>> item_norm_bd = jsc.broadcast(new HashMap<>(item_norm));

        //计算物品物品的相似度，余弦相似度计算
        JavaPairRDD<String, List<Tuple2<String, Double>>> item_similaries = filted_data
                //group items visited by the same user
                .mapToPair(row -> new Tuple2<>(row.getString(0), new Tuple2<>(row.getString(1), row.getDouble(2))))
                .groupByKey() // 用户 -> [(物品, score),...]
                // computer item1 * item2
                .flatMapToPair(t -> { //计算商品两两一起出现的次数
                    List<Tuple2<String, Double>> items = Lists.newArrayList(t._2);
                    Set<Tuple2<ItemPair, Double>> list_set = new HashSet<>(items.size() * (items.size() - 1) / 2);
                    for (Tuple2<String, Double> i : items) {
                        for (Tuple2<String, Double> j : items) {
                            if (i._1 == j._1) continue;
                            list_set.add(new Tuple2<ItemPair, Double>(new ItemPair(i._1, j._1), i._2 * j._2));
                        }
                    }
                    return list_set.iterator();
                })
                .reduceByKey((a, b) -> a + b) //计算共同有过行为的次数
                //computer two item vector cosin: (item1 * item2) / (|item1| * |item2|)
                .mapToPair(t -> {//余弦相似度计算
                    ItemPair up = t._1;
                    Double norm_a = item_norm_bd.getValue().get(up.a);
                    Double norm_b = item_norm_bd.getValue().get(up.b);
                    return new Tuple2<>(up, t._2 / (Double) Math.sqrt(norm_a * norm_b));
                })
                .filter(t -> t._2 >= minSimilar)//过滤掉小于阙值的相似度
                .flatMapToPair(t -> {
                    List<Tuple2<String, Tuple2<String, Double>>> list = new ArrayList<>(2);
                    list.add(new Tuple2<>(t._1.a, new Tuple2<String, Double>(t._1.b, t._2)));
                    list.add(new Tuple2<>(t._1.b, new Tuple2<String, Double>(t._1.a, t._2)));
                    return list.iterator();
                })
                //聚合排序，保留前maxSimilaritiesPerItem个相似的
                .aggregateByKey(new MinHeap(maxSimilaritiesPerItem), MinHeap::add, MinHeap::addAll)
                .mapValues(heap -> { //转化为string -> list
                    List<Tuple2<String, Double>> tops = heap.getSortedItems();
                    //归一化
                    Double max = tops.get(0)._2;
                    return tops.stream()
                            .map(t -> new Tuple2<>(t._1, t._2/max))
                            .collect(Collectors.toList());
                });
        item_similaries.cache();

        //计算用户物品的相关度
        JavaPairRDD<String, List<Tuple2<String, Double>>> user_similaries = item_user_list
                .join(item_similaries)
                .flatMapToPair(t -> {
                    Set<Tuple2<String, Tuple2<String, Double>>> user_similary_items = new HashSet<>();
                    Iterable<Tuple2<String, Double>> users = t._2._1;//对于商品G对应的访问用户
                    List<Tuple2<String, Double>> items = t._2._2; //和商品G有关系的商品
                    for (Tuple2<String, Double> user : users) {
                        //获取当前用户已经访问过的物品
                        Tuple2<Set<String>, Set<String>> visited = user_visited_bd.getValue().get(user._1);
                        Set<String> visited_ids = visited._1;
                        Set<String> abandoned_ids = visited._2;
                        //filter items > maxPrefsPerUserForRec
                        if (abandoned_ids != null && abandoned_ids.contains(t._1)) continue;

                        for (Tuple2<String, Double> item : items) {
                            if (!visited_ids.contains(item._1)) {
                                Double score = item._2;
                                score *= user._2;
                                user_similary_items.add(new Tuple2<>(user._1,
                                        new Tuple2<String, Double>(item._1, score)));
                            }
                        }
                        //手动增加用户点击的数据！！！
                        user_similary_items.add(new Tuple2<>(user._1,
                                new Tuple2<String, Double>(t._1, 1.0)));
                    }
                    return user_similary_items.iterator();
                })
                //sum all scores
                .aggregateByKey(new HashMap<String, Double>(),
                        (m, item) -> {
                            m.merge(item._1, item._2, (a, b) -> b + a);
                            return m;
                        },
                        (m1, m2) -> {
                            HashMap<String, Double> m_big;
                            HashMap<String, Double> m_small;
                            if (m1.size() > m2.size()) {
                                m_big = m1;
                                m_small = m2;
                            } else {
                                m_big = m2;
                                m_small = m1;
                            }
                            for (Map.Entry<String, Double> e : m_small.entrySet()) {
                                Double v = m_big.get(e.getKey());
                                if (v != null) {
                                    m_big.put(e.getKey(), e.getValue() + v);
                                } else {
                                    m_big.put(e.getKey(), e.getValue());
                                }
                            }
                            return m_big;
                        })
                //sort and get topN similary items for user
                .mapValues(all_items -> {
                    List<Tuple2<String, Double>> limit_items = all_items.entrySet().stream()
                            .map(e -> new Tuple2<String, Double>(e.getKey(), e.getValue()))
                            .sorted((a, b) -> b._2.compareTo(a._2))
                            .limit(numRecommendations)
                            .collect(Collectors.toList());
                    return limit_items;
                });

        CFModel model = new CFModel(item_similaries, user_similaries);
        return model;
    }

}
