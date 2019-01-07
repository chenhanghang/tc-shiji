package com.ifeng.jishi.recommend;

import com.ifeng.jishi.util.RedisReaderUtil;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.scalatest.Ignore;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

public class RedisReaderUtilTest {

    @Test
    public void testGetClicksFromRedis() {
        List<Row> t = RedisReaderUtil.getClicksFromRedis(10);
        assert t.size() >=10;
    }

    @Test
    public void testSaveClickToFile() throws IOException{
        List<Row> t = RedisReaderUtil.getClicksFromRedis();
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("click.data"),"GBK"));
        for(Row row: t) {
             String str = row.getString(0)+" "+row.getString(1);
            //写入相关文件
            out.write(str);
            out.newLine();
        }
        //清楚缓存
        out.flush();
        out.close();
    }
}
