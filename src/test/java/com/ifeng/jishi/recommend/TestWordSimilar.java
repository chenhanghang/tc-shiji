package com.ifeng.jishi.recommend;

import com.ifeng.jishi.beans.RowVector;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.*;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;
import org.apdplat.word.util.WordConfTools;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class TestWordSimilar {

    public void testSimi() {
        ContentBasedJob contentBasedJob = new ContentBasedJob();
        SparkSession spark = SparkSession.builder()
                .appName("JiShiItemBasedJob")
                .master("local[3]")
                .getOrCreate();
        String strs[] = {
                "可爱卡通星座十二生肖项链",
                "绿萝盆栽花卉绿植",
                "天然红玛瑙手链",
                "2018年虎虎人专属转运手链",
                "【送32G卡】妙弦 儿童电话手表智能手表手机插卡通话触屏男女孩款定位电话手表学生 天空蓝（三代定位版+APP下载+拍照+16G卡）",
                "GESS 德国品牌颈椎按摩器 家居车载两用"
        };
        for (String str : strs){
            List<Word> words = WordSegmenter.seg(str);
            List<String> tests = words.stream().map(word -> word.getText()).collect(Collectors.toList());

            Dataset<Row> goodsDsChannel = contentBasedJob.getDataSetFromMysql(spark,"(SELECT goods_id,title FROM goods_channel) as goodsChannel");
            Dataset<Row> goodsDs = contentBasedJob.getDataSetFromMysql(spark,"(SELECT goodsId,title FROM goods_info) as goods");
            JavaRDD<Row> goodsRdd = goodsDs.toJavaRDD().cache();
            WordConfTools.set("stopwords.path", "classpath:stopwords");
            //1. 分词
            Dataset<RowVector> dfsChannelDataset = contentBasedJob.segWord(goodsDsChannel);

            //2. 转化为词向量
            Word2Vec word2Vec = new Word2Vec()
                    .setInputCol("words")
                    .setOutputCol("vector")
                    .setVectorSize(100)
                    .setMinCount(0);
            Word2VecModel model = word2Vec.fit(dfsChannelDataset);
            // 物品 gid -> vector
            Encoder<String> StringEncoder = Encoders.STRING();
            Dataset<String> primitiveDS = spark.createDataset(tests, StringEncoder);
            Dataset<Row> goodsChannelDataset = model.transform(dfsChannelDataset);
        }

    }



    public void testWordSeg() {
        String strs[] = {"碧欧泉女士肌底精华露奇迹水125ml滋养护肤 水润光泽 入门精华液",
                "悦野 新交规车牌架 多功能银色不锈钢通用牌照框车牌框",
                "百草味麻薯(抹茶味)210g 糕点小吃年货美食 休闲零食点心",
                "华为荣耀8手机壳青春版硅胶防摔保护套个性卡通带挂绳脖潮男女款",
                "德芙巧克力礼盒 年年得福大礼包 年货8袋装1172g",
                "加绒皮手套 加厚防寒保暖男士棉手套 冬季真皮手套骑车秋冬防风",
                "飞利浦（PHILIPS）电动剃须刀 多功能理容 全身水洗  刮胡刀 S5077/03",
                "老岩泥陶瓷茶叶罐"};

        for (String word : strs) {
            List<Word> words = WordSegmenter.seg(word);
            System.out.println(words);
        }
    }

    /**
     * 用来训练word2vector 模型
     * @throws IOException
     */
    public void trainModel() throws IOException{
        System.setProperty("hadoop.home.dir", "c:\\winutil\\");
        ContentBasedJob contentBasedJob = new ContentBasedJob();
        SparkSession spark = SparkSession.builder()
                .appName("JiShiItemBasedJob")
                .master("local[3]")
                .getOrCreate();
        Dataset<Row> goodsDs = contentBasedJob.getDataSetFromMysql(spark,"(SELECT goodsId,title FROM goods_info) as goods");
        WordConfTools.set("stopwords.path", "classpath:stopwords");
        //1. 分词
        Dataset<RowVector> goodsDataset = contentBasedJob.segWord(goodsDs);

        //2. 转化为词向量
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("words")
                .setOutputCol("vector")
                .setVectorSize(100)
                .setMinCount(0);
        Word2VecModel model = word2Vec.fit(goodsDataset);
        model.save("./vector.model");
        System.out.println("Success train the vector model!!!");

    }
}
