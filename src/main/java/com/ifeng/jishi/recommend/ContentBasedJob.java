package com.ifeng.jishi.recommend;

import com.ifeng.jishi.beans.RowVector;
import com.ifeng.jishi.util.ConfigUtil;
import com.ifeng.jishi.util.MinHeap;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.linalg.BLAS;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;
import org.apdplat.word.util.WordConfTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author  chh
 *  基于相似度计算推荐
 */
public class ContentBasedJob implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ContentBasedJob.class);
    private static Properties pro = ConfigUtil.getConfigProperties();
    /**
     * Get similar by 余弦相似度
     * @param firstFeatures
     * @param secondFeatures
     * @return
     */
    public double computeSimilarity(Vector firstFeatures, Vector secondFeatures) {
        double dot = BLAS.dot(firstFeatures, secondFeatures);
        double v1 = Vectors.norm(firstFeatures.toSparse(), 2.0);
        double v2 = Vectors.norm(secondFeatures.toSparse(), 2.0);
        double similarty = dot / (v1 * v2);
        return similarty;
    }

    /**
     *  Get goods from mysql
     * @param spark
     * @param sql
     * @return
     */
    public static Dataset<Row> getDataSetFromMysql(SparkSession spark, String sql ) {
        SQLContext sc = new SQLContext(spark);
        DataFrameReader reader = sc.read().format("jdbc");
        reader.option("url",pro.getProperty("MYSQL_URL"));//数据库路径
        reader.option("dbtable",sql);//数据表名
        reader.option("driver","com.mysql.jdbc.Driver");
        reader.option("user",pro.getProperty("MYSQL_USER"));
        reader.option("password",pro.getProperty("MYSQL_PASSWORD"));
        Dataset<Row> df = reader.load();
        return df;
    }

    /**
     * 对dataset row 中的商品标题进行分词
     * @param ds
     * @return
     */
    public Dataset<RowVector> segWord(Dataset<Row> ds) {
        Dataset<RowVector> dfs =  ds.map(t-> {
            RowVector gc = new RowVector();
            gc.setId(t.getString(0));
            gc.setTitle(t.getString(1));
            List<Word> words = WordSegmenter.seg(t.getString(1));
            List<String> tests = words.stream().map(word->word.getText()).collect(Collectors.toList());
            gc.setWords(tests);
            return gc;
        }, Encoders.bean(RowVector.class));
        return dfs;
    }

    /**
     * 基于文本相似度进行计算
     * @param spark
     * @param cfModel
     * @param recommendsPerUser 每个用户推荐的数量
     * @return
     */
    public Map<String, String> run(SparkSession spark, CFModel cfModel, int recommendsPerUser) {
        Dataset<Row> goodsDsChannel = this.getDataSetFromMysql(spark,"(SELECT goods_id,title FROM goods_channel) as goodsChannel");
        Dataset<Row> goodsDs = this.getDataSetFromMysql(spark,"(SELECT goodsId,title FROM goods_info) as goods");
        JavaRDD<Row> goodsRdd = goodsDs.toJavaRDD().persist(StorageLevel.MEMORY_AND_DISK());
//        Word2Vec word2Vec = new Word2Vec()
//                .setInputCol("words")
//                .setOutputCol("vector")
//                .setVectorSize(vectorSize)
//                .setMinCount(0);
//        Word2VecModel model = word2Vec.fit(goodsDataset);
        Word2VecModel model;
        if(Boolean.parseBoolean(pro.getProperty("DEGUB_MODE")))
            model = Word2VecModel.load(System.getProperty("user.dir")+"/vector.model");
        else
            model = Word2VecModel.load(pro.getProperty("WORD2VECTOR_MODEL_PATH"));
        /*1. 获取goodsChannelRdd */
        logger.info("Begin init the stop words table!");
        WordConfTools.set("stopwords.path", "classpath:stopwords");
        //分词
        logger.info("Begin seg word of title in goods channel!");
        Dataset<RowVector> dfsChannelDataset = this.segWord(goodsDsChannel);
        //获取词向量模型
        logger.info("Begin turn words of title in goods channel to vector!");
        Dataset<Row> goodsChannelDataset = model.transform(dfsChannelDataset);
        JavaRDD<Row> goodsChannelRdd = goodsChannelDataset.toJavaRDD().persist(StorageLevel.MEMORY_AND_DISK());

        /*2. 获取CF推荐结果的title similariesParsedRdd*/
        JavaPairRDD<String, List<Tuple2<String, Double>>> similariesRDD= cfModel.getUserSimilaries();
//        similariesRDD.foreach(t->  logger.info(t.toString()));
        // gid -> titile 用于做join
        JavaPairRDD<String, String> goodsPairRdd= goodsRdd
                .mapToPair(row-> new Tuple2<>(row.getString(0), row.getString(1)));
        // 获取goodinfo 表中商品titile
        logger.info("Get title of recommends by uid!");
        JavaRDD<Row> similariesParsedRdd = similariesRDD
                .flatMapToPair(t->{
                    List<Tuple2<String, String>> result = new ArrayList<>();
                    for(Tuple2<String, Double> tuple2 : t._2) {
                        String gid = tuple2._1;
                        result.add(new Tuple2<>(gid, t._1));
                    }
                    return result.iterator(); })
                .join(goodsPairRdd)
                .mapToPair(t-> t._2())
//                .reduceByKey((x, y)-> x+y)
                .map(t->RowFactory.create(t._1, t._2));

        /*3. 对similariesParsedRdd进行分词并转化为词向量*/
        logger.info("Turn recommend info to vector!");
        String schemaString = "uid words";
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> similariesDataFrame = spark.createDataFrame(similariesParsedRdd, schema);
        //把RDD转化为Dataset 用于分词
        Dataset<RowVector> vectorDataFrame = this.segWord(similariesDataFrame);
        //转化为uid -> vector 词向量
        Dataset<Row> similariesDataset = model.transform(vectorDataFrame);
        JavaRDD<Row> userRdd = similariesDataset.toJavaRDD();

        /*4. 计算相似的20（可修改）个商品*/
        logger.info("Get top similar goods!");
        JavaPairRDD<String, List<Tuple2<String, Double>>> resultSimilaries  = userRdd
                .cartesian(goodsChannelRdd).mapToPair(t-> {
                    String uid = t._1.getString(0);
                    Vector v1 = (Vector)t._1.get(3);
                    String gid = t._2.getString(0);
                    Vector v2 = (Vector)t._2.get(3);
                    Double score = this.computeSimilarity(v1,v2);
                    if(Double.isNaN(score)){
                        score = 0.0;
                    }
                    return new Tuple2<>(uid, new Tuple2<>(gid, score));
                })
                .aggregateByKey(new MinHeap(recommendsPerUser), MinHeap::add, MinHeap::addAll)
                .mapValues(heap -> heap.getSortedItems());

        /*5. 转化为map中的json字符串保存*/
        logger.info("Map data to json object!");
        Map<String, String> userSimilariesMap = resultSimilaries.mapValues(res -> {
            Collection c = res.stream()
                    .map(t -> {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("gid", t._1);
                        jsonObject.put("score", t._2);
                        return jsonObject;
                    }).collect(Collectors.toList());
            JSONArray jsonArray = JSONArray.fromObject(c);
            return jsonArray.toString();
        }).collectAsMap();
        return userSimilariesMap;
    }
}
