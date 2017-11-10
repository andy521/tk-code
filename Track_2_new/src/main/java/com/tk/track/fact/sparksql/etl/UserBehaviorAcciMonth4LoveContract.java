package com.tk.track.fact.sparksql.etl;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @author
 * @ClassName: 4月长险-爱情契约
 * @Description: TODO
 * @date
 */
public class UserBehaviorAcciMonth4LoveContract {

    public DataFrame getUserBehaviorActivitiesAcciCuleDF(HiveContext sqlContext, String appids) {
        if (appids == null || appids.equals("")) {
            return null;
        }
        sqlContext.load(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH)).registerTempTable("TMP_FS");
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
        String hql = "SELECT * " + "FROM TMP_FS TB1 "
                + "WHERE LOWER(TB1.APP_ID) in (" + appids.toLowerCase() + ")"
                + " AND app_type = 'wechat'";
//                + " AND event = '爱情契约书'";

        String desPath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTACTIVITIES_MONTH4_LOVECONTRACT_OUTPUTPATH);
        if (TK_DataFormatConvertUtil.isExistsPath(desPath)) {
            hql += " AND SUBSTR(FROM_UNIXTIME(INT(VISIT_TIME/1000)),1,10) ='" + format + "'";
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
                + "       '大健康营销活动' AS FIRST_LEVEL, "
                + "       '2017三生三世姻缘测试' AS SECOND_LEVEL, "
                + "       '插码线索导入' THIRD_LEVEL,    "
                + "       '三生三世姻缘测试活动' AS FOURTH_LEVEL,"
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
