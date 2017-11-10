package com.tk.track.fact.sparksql.main;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.etl.RecomendWithoutAlgo2Hbase;
import com.tk.track.fact.sparksql.etl.RecomengWithoutAlgo;
import com.tk.track.fact.sparksql.udf.RandomUDF;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by t-chenhao01 on 2016/11/19.
 */
public class App24 {
    private static final String bhSchema = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HIVE_USERBEHAVIOUR_SCHEMA);

    public static void main(String[] args) {
        /**
         * 此app用来更新人工强行推广的内容
         * 在app24推荐结果生成后，如果人为强推广的产品需要变动时，执行此app。
         */
        SparkConf conf = new SparkConf().setAppName("Taikang Track recommend without algorithm ETL APP24");
        conf.set(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        /*效率低下，不再使用UDF
//        RandomUDF.genrandom(sc, sqlContext);
//        RandomUDF.gendecode(sc, sqlContext);
//        RandomUDF.contactProductInfo(sc, sqlContext);//注册自定义UDF函数，用来从list2补充list1
        */

        //TODO 如果只更新了强推产品直接执行app24便可，如果自动推荐产品的信息有变动，需要放开执行以下代码
        //加载大健康表
//        RandomUDF.genrandom(sc, sqlContext);
//        String facthealthcreditscoreresultPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTHEALTHCREDITSCORERESULT_INPUTPATH);
//        facthealthcreditscoreresultPath=getDailyPath(facthealthcreditscoreresultPath,"_","yyyy-MM-dd",-1);
//        App.loadParquet(sqlContext, facthealthcreditscoreresultPath, "fact_healthcreditscoreresult", false);
//
//		RecomengWithoutAlgo recomengWithoutAlgo =new RecomengWithoutAlgo();
//		recomengWithoutAlgo.getFactHealthcreditscoreresultNotNullDF(sqlContext);
//
//        String recommendBasepath =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_OUTPUTPATH);
//        recommendBasepath=getDailyPath(recommendBasepath,"-","yyyyMMdd",0);
//		App.loadParquet(sqlContext, recommendBasepath, "RBASE_USERBROWSEANDPOLICYINFO", false);
////        //计算一段时间内产品浏览排名，用来做默认
//        App.loadDefaultRecommendWithoutAlgoResult(sqlContext, true, false);
//        String defaultRecommendWithoutAlgoResultpath =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGORESULTDEFAULT_OUTPUTPATH);
//        delete3daysAgoFile(defaultRecommendWithoutAlgoResultpath,"-","yyyyMMdd");
//        //计算得分和推荐结果
//        App.loadRecommendWithoutAlgoResult(sqlContext, true, false);
        //TODO 如果只更新了强推产品直接执行app24便可，如果自动推荐产品的信息有变动，需要放开执行以上代码

    
        //获得强推结果
        String sql="select force.forceproduct from "+bhSchema+"recommend_force force";
        DataFrame dfForce= DataFrameUtil.getDataFrame(sqlContext, sql, "recommend_force");
        final String orginalResult=dfForce.select("forceproduct").first().getString(0);
        
        //补充人工推荐结果
        App.loadRecomendWithoutAlgoForceResult(sqlContext, true, false,orginalResult);

        String forceResultPath =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_FORCERESULT_PATH);
        delete3daysAgoFile(forceResultPath,"-","yyyyMMdd");

        //推送结果到hbase
		RecomendWithoutAlgo2Hbase recomendWithoutAlgo2Hbase=new RecomendWithoutAlgo2Hbase();
		recomendWithoutAlgo2Hbase.loadToHbase(sqlContext);
		
       //吧WAP端的结果推送到hbase中
        recomendWithoutAlgo2Hbase.loadToHbaseWap(sqlContext);

        //吧pc端的结果推送到hase中
        recomendWithoutAlgo2Hbase.loadToHbasePc(sqlContext);
        
        //吧APP端的结果推送到hase中
        recomendWithoutAlgo2Hbase.loadToHbaseApp(sqlContext);
        
        //吧默认的结果导入到hbase中
        recomendWithoutAlgo2Hbase.insertDefaultProductRecommend(sqlContext,orginalResult);
        sqlContext.clearCache();
        sc.stop();
    }
    /**
     * 删除三天前的hdfs无用文件
     * @param orginalPath 原始路径
     * @param connectStr  与日期的连接符
     * @param datePattern   日期模式 yyyy MM dd
     */
    public static void delete3daysAgoFile(String orginalPath,String connectStr,String datePattern){
        String path=getDailyPath(orginalPath, connectStr, datePattern,-3);
        TK_DataFormatConvertUtil.deletePath(path);
    }
    /**
     * 拿到代日期的路径
     * @param path		原始路径
     * @param connector 连接符
     * @param parten	日期模式
     * @param dayDif	前推几天
     * @return
     */
    public static String getDailyPath(String path,String connector,String parten,int dayDif) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, dayDif);
        String yesterday = new SimpleDateFormat(parten).format(cal.getTime());
        return (path + connector + yesterday);
    }


}
