package com.tk.track.fact.sparksql.etl;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorClue;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @author
 * @ClassName: 泰康在线综合意外险（含交通意外）
 * @Description: TODO
 * @date
 */
public class UserBehaviorActivitiesMonth6 {

    public DataFrame getUserBehaviorActivitiesCuleDF(HiveContext sqlContext, String appids) {
        if (appids == null || appids.equals("")) {
            return null;
        }
        sqlContext.load(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH)).registerTempTable("TMP_FS_MONTH6");
        getTempFs(sqlContext, appids);
        getTempFsWithUserInfo(sqlContext);
        return getDataWithClueLevel(sqlContext);

    }

    /**
     * 查出符合条件的用户行为
     *
     * @param sqlContext
     * @param appids
     * @return
     */
    public DataFrame getTempFs(HiveContext sqlContext, String appids) {
        long todayTime = getTodayTime(-1);
        String format = new SimpleDateFormat("yyyy-MM-dd").format(todayTime);
        String hql = "SELECT * " + "FROM TMP_FS_MONTH6 TB1 "
                + "WHERE LOWER(TB1.APP_ID) in (" + appids.toLowerCase() + ")"
                + " AND app_type = 'wechat'"
                + " AND ((EVENT='主会场' AND SUBTYPE in ('少儿专场','健康险专区','意外险专区','热销爆款'))"
                + " OR (EVENT='主会场' AND SUBTYPE='立即抢购') "
                + " OR EVENT='抽奖'"
                + " OR (EVENT='我的优惠券' AND SUBTYPE IS NOT NULL AND SUBTYPE <> ''))";

        String desPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTACTIVITIES_MONTH4_ACCI_OUTPUTPATH);
        if (TK_DataFormatConvertUtil.isExistsPath(desPath)) {
            hql += " AND FROM_UNIXTIME(CAST(CAST(VISIT_TIME AS BIGINT) / 1000 AS BIGINT), 'yyyy-MM-dd') ='" + format + "'";
        }

        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILTER_COLLECTION");
    }


    /**
     * 找打opendid 转话成memberid，未找到的存openid
     * 查出用户的基本信息
     *
     * @param sqlContext
     * @return
     */
    public DataFrame getTempFsWithUserInfo(HiveContext sqlContext) {
        String hql = "SELECT TMP_A.ROWKEY, "
                + "          TMP_A.FROM_ID, "
                + "          TMP_A.APP_TYPE, "
                + "          TMP_A.APP_ID, "
                + "           CASE WHEN UM.MEMBER_ID IS NOT NULL AND UM.MEMBER_ID <> '' THEN  UM.MEMBER_ID"
                + "				WHEN UM.CUSTOMER_ID IS NOT NULL AND UM.CUSTOMER_ID <>'' THEN UM.CUSTOMER_ID "
                + "				ELSE TMP_A.USER_ID END USER_ID ,"

                + "           CASE WHEN UM.MEMBER_ID IS NOT NULL AND UM.MEMBER_ID <> '' THEN  'MEM'"
                + "				WHEN UM.CUSTOMER_ID IS NOT NULL AND UM.CUSTOMER_ID <>'' THEN  'C' "
                + "				ELSE 'WE' END USER_TYPE ,"
                + "          TMP_A.EVENT, "
                + "          TMP_A.SUBTYPE,"
                + "          TMP_A.LABEL,"
                + "			 TMP_A.CUSTOM_VAL,"
                + "          TMP_A.VISIT_COUNT, "
                + "          TMP_A.VISIT_TIME, "
                + "          TMP_A.VISIT_DURATION,"
                + "			 UM.NAME AS USER_NAME,"
                + "			 CASE WHEN UM.GENDER = '0' THEN '男' WHEN  UM.GENDER ='1' THEN '女' ELSE '' END GENDER,"
                + "			 UM.BIRTHDAY"
                + "     FROM TMP_FILTER_COLLECTION TMP_A "
                + "     LEFT JOIN (SELECT NAME,GENDER,BIRTHDAY,CUSTOMER_ID,MEMBER_ID,OPEN_ID "
                + "					FROM FACT_USERINFO WHERE OPEN_ID IS NOT NULL AND OPEN_ID <> '') UM"
                + "       ON TMP_A.USER_ID = UM.OPEN_ID "
                + "    WHERE TMP_A.APP_TYPE = 'wechat'";

        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_BEHAVIOR_CLUE");
    }

    /**
     * 存入clue中
     *
     * @param sqlContext
     * @return
     */
    public DataFrame getDataWithClueLevel(HiveContext sqlContext) {
        String hql = "SELECT T5.ROWKEY,"
                + "       T5.USER_ID,   "
                + "       T5.USER_TYPE, "
                + "       T5.APP_TYPE,  "
                + "       T5.APP_ID,    "
                + "       'load' AS EVENT_TYPE,  "
                + "       T5.EVENT,     "
                + "       T5.SUBTYPE AS SUB_TYPE,"
                + "       '查询' AS PAGE_TYPE,   "
                + "       '微信经营活动' AS FIRST_LEVEL, "
                + "       '日常活动' AS SECOND_LEVEL, "
                + "       CASE WHEN EVENT='我的优惠券' THEN '6月大促-优惠券' "
                + "            WHEN EVENT='抽奖' THEN '6月大促-抽奖' "
                + "            WHEN EVENT='主会场' AND SUBTYPE='立即抢购' THEN '6月大促-页面浏览' "
                + "            WHEN EVENT='主会场' AND SUBTYPE<>'立即抢购' THEN '6月大促-列表页'"
                + "     END AS THIRD_LEVEL,    "
                + "       CASE WHEN EVENT='我的优惠券' AND SUBTYPE='成人重大疾病保险' THEN '成人重疾' "
                + "            WHEN EVENT='我的优惠券' AND SUBTYPE='老年恶性肿瘤医疗保险' THEN '老年恶性肿瘤保险' "
                + "            WHEN EVENT='我的优惠券' AND SUBTYPE='全年综合意外' THEN '全年综合意外' "
                + "            WHEN EVENT='我的优惠券' AND SUBTYPE='e顺轻症疾病保险' THEN 'e顺轻症疾病保险' "
                + "            WHEN EVENT='我的优惠券' AND SUBTYPE='泰康在线住院保' THEN '住院保' "
                + "            WHEN EVENT='我的优惠券' AND SUBTYPE='少儿重大疾病保险' THEN '少儿重疾' "
                + "            WHEN EVENT='抽奖' THEN '6月大促-抽奖' "
                + "            WHEN EVENT='主会场' AND SUBTYPE='立即抢购' AND regexp_extract(LABEL,'(?<=lrtName:\")[^\",]*',0)='悦享中华高端医疗费用报销' THEN '泰康悦享中华B款高端医疗保险' "
                + "            WHEN EVENT='主会场' AND SUBTYPE='立即抢购' AND regexp_extract(LABEL,'(?<=lrtName:\")[^\",]*',0)='E顺轻症疾病保险' THEN 'e顺轻症疾病保险' "
                + "            WHEN EVENT='主会场' AND SUBTYPE='立即抢购' AND regexp_extract(LABEL,'(?<=lrtName:\")[^\",]*',0)='成人重疾' THEN '成人重疾' "
                + "            WHEN EVENT='主会场' AND SUBTYPE='立即抢购' AND regexp_extract(LABEL,'(?<=lrtName:\")[^\",]*',0)='E享健康' THEN '泰康e享健康保障计划' "
                + "            WHEN EVENT='主会场' AND SUBTYPE='立即抢购' AND regexp_extract(LABEL,'(?<=lrtName:\")[^\",]*',0)='私家车综合保障计划' THEN '私家车综合保障计划' "
                + "            WHEN EVENT='主会场' AND SUBTYPE='立即抢购' AND regexp_extract(LABEL,'(?<=lrtName:\")[^\",]*',0)='少儿重疾' THEN '少儿重疾' "
                + "            WHEN EVENT='主会场' AND SUBTYPE='立即抢购' AND regexp_extract(LABEL,'(?<=lrtName:\")[^\",]*',0)='住院保' THEN '住院保' "
                + "            WHEN EVENT='主会场' AND SUBTYPE='立即抢购' AND regexp_extract(LABEL,'(?<=lrtName:\")[^\",]*',0)='全年综合意外' THEN '全年综合意外' "
                + "            WHEN EVENT='主会场' AND SUBTYPE='立即抢购' AND regexp_extract(LABEL,'(?<=lrtName:\")[^\",]*',0)='老年癌症医疗' THEN '老年恶性肿瘤保险' "
                + "            WHEN EVENT='主会场' AND SUBTYPE='立即抢购' AND regexp_extract(LABEL,'(?<=lrtName:\")[^\",]*',0)='铁路意外保障计划' THEN '铁路意外保障计划' "
                + "            WHEN EVENT='主会场' AND SUBTYPE='立即抢购' AND regexp_extract(LABEL,'(?<=lrtName:\")[^\",]*',0)='儿童门诊' THEN '儿童门诊' "
                + "            WHEN EVENT='主会场' AND SUBTYPE='立即抢购' AND regexp_extract(LABEL,'(?<=lrtName:\")[^\",]*',0)='航空意外保障计划' THEN '航空意外保障计划' "
                + "            WHEN EVENT='主会场' AND SUBTYPE='立即抢购' AND regexp_extract(LABEL,'(?<=lrtName:\")[^\",]*',0)='境外旅行至尊保障计划' THEN '境外旅行保障至尊款' "
                + "            WHEN EVENT='主会场' AND SUBTYPE<>'立即抢购'  THEN SUBTYPE"
                + "     END AS FOURTH_LEVEL,    "
                + "       T5.USER_NAME, "
                + "       T5.VISIT_COUNT,        "
                + "       FROM_UNIXTIME(INT(T5.VISIT_TIME / 1000)) AS VISIT_TIME,      "
                + "       T5.VISIT_DURATION,     "
                + "       T5.FROM_ID,   "
                + "       CONCAT('姓名：',       "
                + "              NVL(T5.USER_NAME, ' '),  "
                + "              '\073性别：',   "
                + "              NVL(T5.GENDER, ' '),     "
                + "              '\073出生日期：',        "
                + "              NVL(T5.BIRTHDAY, ' '),   "
                + "              '\073首次访问时间：',    "
                + "              NVL(FROM_UNIXTIME(INT(T5.VISIT_TIME / 1000)), ' '),"
                + "              '\073访问次数：',        "
                + "              NVL(T5.VISIT_COUNT, ' '),"
                + "              '\073访问时长：',        "
                + "              NVL(T5.VISIT_DURATION, ' ')) AS REMARK,"
                + "       '2' AS CLUE_TYPE       "
                + "  FROM TMP_USER_BEHAVIOR_CLUE T5";

        return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_USER_BEHAVIOR_ClUE").distinct();
    }

    public long getTodayTime(int day) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, day);
        String yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long timeStamp = 0;
        try {
            timeStamp = sdf.parse(yesterday).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return timeStamp;
    }


    public void saveAsParquet(DataFrame df, String path) {
        TK_DataFormatConvertUtil.deletePath(path);
        df.saveAsParquetFile(path);
    }
}
