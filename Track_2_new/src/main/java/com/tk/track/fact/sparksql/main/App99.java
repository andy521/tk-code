package com.tk.track.fact.sparksql.main;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by itw_zhaisz on 2017/3/18.
 */
public class App99 {
    private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT) + "/appid-type.properties";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Taikang Activity WeChat  APP99");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM,
                //测试环境zookeeer集群，生产环境记得改回去
                TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        String staticsEventPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH);
        App.loadParquet(sqlContext, staticsEventPath, "F_STATISTICS_EVENT", false);


        String factUserInfoPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_USERINFO_INPUTPATH);
        App.loadParquet(sqlContext, factUserInfoPath, "FACT_USERINFO", false);

        App.loadWeChatActivityApp(sqlContext);
        sqlContext.clearCache();
        sc.stop();
    }

}
