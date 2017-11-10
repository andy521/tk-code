package com.tk.track.fact.sparksql.etl;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.clue.UserBehaviorClueFactory;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorClue;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.fact.sparksql.util.DateTimeUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;

/**
 * Created by t-chenhao01 on 2017/1/12.
 */
public class WechatActivityClueCommon implements Serializable {
    private static final long serialVersionUID = -7435764133216270611L;
    private UserBehaviorClueFactory factory=null;
    public WechatActivityClueCommon() {
    }
    public WechatActivityClueCommon(UserBehaviorClueFactory factory) {
        this.factory=factory;
    }
    //总执行流程
    public  DataFrame getWechatActivityClue(HiveContext sqlContext, String appids) {

        if(appids==null || appids.equals("")){
            return null;
        }

        sqlContext.load(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH)).registerTempTable("FACT_STATISTICS_EVENT");
        String paramSql =factory.getParamSql();
         getDataFromStatisticsEventTable(sqlContext, appids, paramSql);
        DataFrame teleDf =RemoveTKEmployeeOpenId(sqlContext);
        JavaRDD<FactUserBehaviorClue> labelRdd = analysisLabelRDD(teleDf);
        sqlContext.createDataFrame(labelRdd, FactUserBehaviorClue.class).registerTempTable("TMP_ANALYSISLABEL");;
        getFACT_USERINFOQnique(sqlContext);
        DataFrame resultDataFram = fullFillUserNameByUserInfoTable(sqlContext);
        return resultDataFram;
    }


    public DataFrame getDataFromStatisticsEventTable(HiveContext sqlContext, String appids, String paramSql) {
        String hql = "SELECT ROWKEY,user_id,"
                +	"app_type,"
                +	"app_id,"
                +	"event,"
                +	"subtype,"
                +	"label,"
                +	"custom_val,"
                +	"visit_count,"
                +	"from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') as VISIT_TIME,"
                +	"visit_duration,"
                +	"from_id"
                + " FROM  FACT_STATISTICS_EVENT TB1 "
                + " WHERE LOWER(TB1.APP_ID) in (" + appids.toLowerCase() + ")"
                + " AND event <> 'page.load' AND event <> 'page.unload' ";
        if(paramSql != null && !paramSql.isEmpty()) {
            hql = hql + paramSql;
        }
        if (TK_DataFormatConvertUtil.isExistsPath(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORCLUE_OUTPUTPATH))) {
            String timeStamp = Long.toString(DateTimeUtil.getTodayTime(0) / 1000);
            String yesterdayTimeStamp = Long.toString(DateTimeUtil.getTodayTime(-1) / 1000);
            hql += " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast(" + yesterdayTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')"
                    +  " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast(" + timeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
        }
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILTER_COLLECTION");
    }

    public DataFrame RemoveTKEmployeeOpenId(HiveContext sqlContext){
        String hql="select oi.ROWKEY,oi.user_id,oi.app_type,oi.app_id,oi.event,oi.subtype,oi.label,oi.custom_val," +
                "oi.visit_count,oi.VISIT_TIME,oi.visit_duration,oi.from_id from TMP_FILTER_COLLECTION oi left join " +
                "tkubdb.tkemployeeopenid a on oi.user_id=a.open_id where a.emp_code is null";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "RemoveTKEmployeeOpenId");
    }


    public JavaRDD<FactUserBehaviorClue> getJavaRDD(DataFrame df) {
        JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID", "USER_TYPE", "APP_TYPE", "APP_ID",
                "EVENT_TYPE", "EVENT", "SUB_TYPE", "VISIT_DURATION", "FROM_ID", "PAGE_TYPE",
                "FIRST_LEVEL", "SECOND_LEVEL", "THIRD_LEVEL", "FOURTH_LEVEL", "VISIT_TIME","VISIT_COUNT","CLUE_TYPE","REMARK","USER_NAME").rdd().toJavaRDD();
        JavaRDD<FactUserBehaviorClue> pRDD = jRDD.map(new Function<Row, FactUserBehaviorClue>() {

            private static final long serialVersionUID = -1619903863589066084L;

            public FactUserBehaviorClue call(Row row) throws Exception {
                String ROWKEY = row.getString(0);
                String USER_ID = row.getString(1);
                String USER_TYPE = row.getString(2);
                String APP_TYPE = row.getString(3);
                String APP_ID = row.getString(4);
                String EVENT_TYPE = row.getString(5);
                String EVENT = row.getString(6);
                String SUB_TYPE = row.getString(7);
                String VISIT_DURATION = row.getString(8);
                String FROM_ID = row.getString(9);
                String PAGE_TYPE = row.getString(10);
                String FIRST_LEVEL = row.getString(11);
                String SECOND_LEVEL = row.getString(12);
                String THIRD_LEVEL = row.getString(13);
                String FOURTH_LEVEL = row.getString(14);
                String VISIT_TIME = row.getString(15);
                String VISIT_COUNT = row.getString(16);
                String CLUE_TYPE = row.getString(17);
                String REMARK = row.getString(18);
                String USER_NAME = row.getString(19);
                return new FactUserBehaviorClue(ROWKEY, USER_ID, USER_TYPE,
                        APP_TYPE, APP_ID, EVENT_TYPE, EVENT, SUB_TYPE,
                        VISIT_DURATION, FROM_ID, PAGE_TYPE, FIRST_LEVEL,
                        SECOND_LEVEL, THIRD_LEVEL, FOURTH_LEVEL, VISIT_TIME,
                        VISIT_COUNT, CLUE_TYPE, REMARK, USER_NAME);
            }
        });
        return pRDD;
    }

    public  JavaRDD<FactUserBehaviorClue> analysisLabelRDD(DataFrame df) {
        JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID", "APP_TYPE", "APP_ID",
                "EVENT", "SUBTYPE", "LABEL", "CUSTOM_VAL", "VISIT_COUNT", "VISIT_TIME", "VISIT_DURATION","FROM_ID").rdd().toJavaRDD();
        JavaRDD<FactUserBehaviorClue> pRDD = jRDD.map(new Function<Row, FactUserBehaviorClue>() {

            private static final long serialVersionUID = -8170582433458947914L;

            public FactUserBehaviorClue call(Row row) throws Exception {
                String ROWKEY = row.getString(0);
                String USER_ID = row.getString(1);
                String APP_TYPE = row.getString(2);
                String APP_ID = row.getString(3);
                String EVENT = row.getString(4);
                String SUBTYPE = row.getString(5);
                String LABEL = row.getString(6);
                String CUSTOM_VAL = row.getString(7);
                String VISIT_COUNT = row.getString(8);
                String VISIT_TIME = row.getString(9);
                String VISIT_DURATION = row.getString(10);
                String FROM_ID = row.getString(11);
                return factory.analysisLabelRDD(ROWKEY, USER_ID, APP_TYPE,
                        APP_ID, EVENT, SUBTYPE, LABEL, CUSTOM_VAL, VISIT_COUNT, VISIT_TIME,
                        VISIT_DURATION, FROM_ID);
            }
        });
        return pRDD;
    }
    /**
     * FACT_USERINFO 用户表中有客户有两条及以上记录，需要合并，，
     * 所以以open_id聚合，对于有多个会员号的人，按会员号降序排序，取最新的一条记录做为此人的记录。
     * @param sqlContext
     * @return
     */
    private DataFrame getFACT_USERINFOQnique(HiveContext sqlContext){
        String sql="select a.user_id_list[0] as user_id,member_id_list[0] as member_id,open_id,customer_id_list[0] as customer_id," +
                "name_list[0] as name from (" +
                "select collect_list(user_id)  user_id_list,collect_list(member_id) member_id_list, collect_list(customer_id) customer_id_list, " +
                "collect_list(name) name_list,open_id  from (" +
                "select user_id,member_id,customer_id,open_id,name from FACT_USERINFO where member_id is not null and member_id <>'' " +
                " and customer_id is not null and customer_id<>'' and open_id is not null and open_id <>'' order by member_id desc " +
                ") t group by open_id " +
                ") a";
        return DataFrameUtil.getDataFrame(sqlContext, sql, "USER_INFO_UNIQUE");
    }

    public DataFrame fullFillUserNameByUserInfoTable(HiveContext sqlContext) {
        String hql = factory.fullFillUserNameByUserInfoTable();
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_BEHAVIOR_CLUE");
    }




    public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactUserBehaviorClue> rdd, String path) {
        if (TK_DataFormatConvertUtil.isExistsPath(path)) {
            sqlContext.createDataFrame(rdd, FactUserBehaviorClue.class).save(path, "parquet", SaveMode.Append);
        } else {
            sqlContext.createDataFrame(rdd, FactUserBehaviorClue.class).saveAsParquetFile(path);
        }
    }
    public static void repatition(HiveContext sqlContext,String path){
        String tmpPath = path + "-temp";
        final Integer DEFAULT_PATION = 200;
        Integer pation = 0;
        try {
            pation = Integer.valueOf(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_REPARTITION_COUNT));
            if(pation <= 0)
                pation = DEFAULT_PATION;
        } catch (Exception e) {
            pation = DEFAULT_PATION;
        }
        //Load parquet[path], save as parquet[tmpPath]
        TK_DataFormatConvertUtil.deletePath(tmpPath);
        sqlContext.load(path).repartition(pation).saveAsParquetFile(tmpPath);
        //Delete parquet[path]
        TK_DataFormatConvertUtil.deletePath(path);
        //Rename parquet[tmpPath] as parquet[path]
        TK_DataFormatConvertUtil.renamePath(tmpPath, path);
    }
}
