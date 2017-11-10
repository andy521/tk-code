package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.fact.sparksql.desttable.FactUserBehaviorTele;
import com.tk.track.fact.sparksql.desttable.TempUserBehaviorTeleWap;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.fact.sparksql.util.StringParse;
import com.tk.track.util.TK_DataFormatConvertUtil;

public class UserBehaviorFlowGoodsDetail implements Serializable {
	
	private static final long serialVersionUID = 5241589436279973504L;
	
    String sca_ol = TK_DataFormatConvertUtil.getSchema();

    public DataFrame getUserBehaviorFlowGoodsDetailDF(HiveContext sqlContext, String appids) {
    	
		if (appids == null || appids.equals("")) {
			return null;
		}

		DataFrame teleDf = getUserBehaviorTeleDF1(sqlContext, appids);

		JavaRDD<TempUserBehaviorTeleWap> labelRdd = analysisLabelRDD(teleDf);

		sqlContext.createDataFrame(labelRdd, TempUserBehaviorTeleWap.class).registerTempTable("TMP_ANALYSISLABEL");

		getUserBehaviorTeleLevelNotNull(sqlContext);

		getUserInfoMemberIdNotNull(sqlContext);
		
		getUserInfoMemberIdOpenIdNotNull(sqlContext);

		return getUserBehaviorTeleWithUserInfo(sqlContext);
    }
    
    /**
     * 获取商品详情数据
     * @Description: 
     * @param sqlContext
     * @param appids
     * @return
     * @author itw_qiuzq01
     * @date 2016年12月5日
     * @update [日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    public DataFrame getUserBehaviorTeleDF1(HiveContext sqlContext, String appids) {
    	String timeStamp = Long.toString(getTodayTime(0) / 1000);
		String yesterdayTimeStamp = Long.toString(getTodayTime(-1) / 1000);
		
        String hql = "SELECT * "
                + " FROM  FACT_STATISTICS_EVENT TB1 " 
                + " WHERE LOWER(TB1.APP_TYPE)='h5' and EVENT = '商品详情' and LOWER(TB1.APP_ID) in (" + appids.toLowerCase() + ")"
                + " AND   event <> 'page.load' AND event <> 'page.unload' "
                + " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast(" + yesterdayTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')"
    			+  " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast(" + timeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILTER_COLLECTION");
    }

    
    /**
     * df转rdd,用于解析label字段
     * @Description: 
     * @param df
     * @return
     * @author itw_qiuzq01
     * @date 2016年12月5日
     * @update [日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
	public JavaRDD<TempUserBehaviorTeleWap> analysisLabelRDD(DataFrame df) {
		JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID", "APP_TYPE", "APP_ID", "EVENT", "SUBTYPE", "LABEL", "CUSTOM_VAL",
						"VISIT_COUNT", "VISIT_TIME", "VISIT_DURATION", "FROM_ID").rdd().toJavaRDD();
		
		JavaRDD<TempUserBehaviorTeleWap> pRDD = jRDD.map(new Function<Row, TempUserBehaviorTeleWap>() {

			private static final long serialVersionUID = 2966182622609483359L;

			public TempUserBehaviorTeleWap call(Row v1) throws Exception {
				return analysisWapLable(v1.getString(0), v1.getString(1), v1.getString(2), v1.getString(3),
						v1.getString(4), v1.getString(5), v1.getString(6), v1.getString(7), v1.getString(8),
						v1.getString(9), v1.getString(10), v1.getString(11));
			}
		});
		return pRDD;
	}
	
	
	
	
	/**
	 * 解析label字段
	 * @Description: 
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年12月5日
	 * @update [日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private TempUserBehaviorTeleWap analysisWapLable(String rowKey, String userid, String apptype,
			String appid, String event, String subtype, String label, String customVal, String visitcount,
			String visittime, String visitduration, String fromId) {

		String classifyName = "";
		String lrtID = "";
		String lrtName = "";
		String classID = "";
		String thirdLevel = "";
		String fourthLevel = "";
		String userType = "";
		String openId = "";
		String finalUserId = userid;

		StringParse parse = new StringParse();
		Map<String, String> dataMap = parse.parse2Map(label);
		classifyName = dataMap.get("classifyName");
		lrtID = dataMap.get("lrt_id");
		fourthLevel = lrtID;
		lrtName = dataMap.get("lrtName");
		thirdLevel = dataMap.get("lrtType");
		openId = dataMap.get("openid");
		//晓波说label字段里openid不为空的为微信跳转过来的
		if (null != openId && !openId.trim().equals("")) {
			userType = "WE-MEM";
			finalUserId = openId;
		}
		
		return new TempUserBehaviorTeleWap(rowKey, finalUserId, apptype, appid, event, subtype, lrtID,
				classID, visitcount, visittime, visitduration, classifyName, lrtName, thirdLevel, fourthLevel,
				userType, fromId);
	}
	
	
	
	/**
	 * 取三、四级分类不为空的数据合并
	 * @Description: 
	 * @param sqlContext
	 * @return
	 * @author itw_qiuzq01
	 * @date 2016年12月5日
	 * @update [日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
    public DataFrame getUserBehaviorTeleLevelNotNull(HiveContext sqlContext) {
    	String hql = "SELECT TMP_BT.FROM_ID,"
        		+"       TMP_BT.APP_TYPE,"
        		+"       TMP_BT.APP_ID,"
        		+"       TMP_BT.USER_ID,"
        		+"       TMP_BT.USER_TYPE,"
        		+"       TMP_BT.USER_EVENT,"
        		+"       TMP_BT.THIRD_LEVEL,"
        		+"       TMP_BT.FOURTH_LEVEL,"
        		+"       TMP_BT.LRT_ID,"
        		+"       TMP_BT.CLASS_ID,"
        		+"       TMP_BT.APP_ID INFOFROM,"
        		+"       STRING(SUM(TMP_BT.VISIT_COUNT)) AS VISIT_COUNT,"
        		+"       STRING(MIN(FROM_UNIXTIME(CAST(CAST(TMP_BT.VISIT_TIME AS BIGINT) / 1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss'))) AS VISIT_TIME,"
        		+"       STRING(SUM(TMP_BT.VISIT_DURATION)) AS VISIT_DURATION,"
        		+"       STRING(MIN(TMP_BT.CLASSIFYNAME)) AS CLASSIFY_NAME,"
        		+"       STRING(MIN(TMP_BT.LRT_NAME)) AS PRODUCT_NAME"
        		+"  FROM TMP_ANALYSISLABEL TMP_BT"
        		+" WHERE TMP_BT.THIRD_LEVEL <> '' AND TMP_BT.THIRD_LEVEL IS NOT NULL"
        		+" AND TMP_BT.FOURTH_LEVEL <> '' AND TMP_BT.FOURTH_LEVEL IS NOT NULL"
        		+" GROUP BY TMP_BT.FROM_ID,"
        		+"          TMP_BT.APP_TYPE,"
        		+"          TMP_BT.APP_ID,"
        		+"          TMP_BT.USER_ID,"
        		+"          TMP_BT.USER_TYPE,"
        		+"          TMP_BT.USER_EVENT,"
        		+"          TMP_BT.THIRD_LEVEL,"
        		+"          TMP_BT.FOURTH_LEVEL,"
        		+"          TMP_BT.LRT_ID,"
        		+"          TMP_BT.CLASS_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USER_BEHAVIOR_TELE");
    }    
    
    
    /**
     * 获取会员号不为空的用户信息
     * @Description: 
     * @param sqlContext
     * @return
     * @author itw_qiuzq01
     * @date 2016年12月5日
     * @update [日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    public DataFrame getUserInfoMemberIdNotNull(HiveContext sqlContext) {
        String hql = "SELECT DISTINCT TFU.NAME, "
        		+ "           TFU.CUSTOMER_ID,"
        		+ "           TFU.MEMBER_ID, "
        		+ "           TFU.OPEN_ID "
        		+ "   FROM    FACT_USERINFO TFU "
        		+ "   WHERE   TFU.MEMBER_ID <>'' "
        		+ "   AND     TFU.MEMBER_ID IS NOT NULL";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_USERINFO_MEM_NOTNULL");
    }
    
    
    /**
     *  获取会员号和微信Id都不为空的数据
     * @Description: 
     * @param sqlContext
     * @return
     * @author itw_qiuzq01
     * @date 2016年12月5日
     * @update [日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    public DataFrame getUserInfoMemberIdOpenIdNotNull(HiveContext sqlContext) {
        String hql = "SELECT DISTINCT TFU.NAME, "
        		+ "           TFU.CUSTOMER_ID,"
        		+ "           TFU.MEMBER_ID, "
        		+ "           TFU.OPEN_ID "
        		+ "   FROM    FACT_USERINFO_MEM_NOTNULL TFU "
        		+ "   WHERE   TFU.OPEN_ID <>'' "
		        + "   AND     TFU.OPEN_ID IS NOT NULL";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_USERINFO_MEM_OPENID_NOTNULL");
    }

    /**
     * 行为数据关联用户信息
     * @Description: 
     * @param sqlContext
     * @return
     * @author itw_qiuzq01
     * @date 2016年12月5日
     * @update [日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    public DataFrame getUserBehaviorTeleWithUserInfo(HiveContext sqlContext) {
    	String hql = "SELECT DISTINCT '' ROWKEY,"
    			+"                CASE"
    			+ " 				WHEN CM.MEMBER_ID IS NOT NULL AND CM.MEMBER_ID <> '' THEN"
    			+"                   CM.MEMBER_ID"
    			+"                  WHEN CO.MEMBER_ID IS NOT NULL AND CO.MEMBER_ID <> '' THEN"
    			+"                   CO.MEMBER_ID"
    			+"                END AS USER_ID,"
    			+"                CASE"
    			+"                  WHEN T1.USER_TYPE IS NOT NULL AND T1.USER_TYPE <> '' THEN"
    			+ "						T1.USER_TYPE"
    			+ "					WHEN (CM.MEMBER_ID IS NOT NULL AND CM.MEMBER_ID <> '') "
    			+ "						OR (CO.MEMBER_ID IS NOT NULL AND CO.MEMBER_ID <> '') THEN"
    			+"                   'MEM'"
    			+"                END AS USER_TYPE,"
    			+"                T1.USER_EVENT,"
    			+"                T1.THIRD_LEVEL,"
    			+"                T1.FOURTH_LEVEL,"
    			+"                T1.LRT_ID,"
    			+"                T1.CLASS_ID,"
    			+"                CONCAT('GoodsDetail_', T1.INFOFROM) AS INFOFROM,"
    			+"                T1.VISIT_COUNT,"
    			+"                T1.VISIT_TIME,"
    			+"                T1.VISIT_DURATION,"
    			+"                T1.CLASSIFY_NAME,"
    			+"                T1.PRODUCT_NAME,"
    			+"                COALESCE(CM.NAME,CO.NAME,'') AS USER_NAME,"
    			+"                '1' IF_PAY,"
    			+"                T1.FROM_ID,"
    			+"                '' AS REMARK,"
    			+"                '' AS CLUE_TYPE"
    			+"  FROM TMP_USER_BEHAVIOR_TELE T1"
    			+"  LEFT JOIN FACT_USERINFO_MEM_NOTNULL CM ON T1.USER_ID = CM.MEMBER_ID"
    			+"  LEFT JOIN FACT_USERINFO_MEM_OPENID_NOTNULL CO ON T1.USER_ID = CO.OPEN_ID"
    			+ " left join tkoldb.wx_openid_nwq pc on pc.openid = co.open_id "
    			+"  WHERE (CM.MEMBER_ID IS NOT NULL AND CM.MEMBER_ID <> '') "
    			+ "	OR (CO.MEMBER_ID IS NOT NULL AND CO.MEMBER_ID <> '') and pc.openid is null";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "USER_BEHAVIOR_WITH_USERINFO");
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


	
	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactUserBehaviorTele> rdd, String path) {
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			sqlContext.createDataFrame(rdd, FactUserBehaviorTele.class).save(path, "parquet", SaveMode.Append);
		} else {
			sqlContext.createDataFrame(rdd, FactUserBehaviorTele.class).saveAsParquetFile(path);
		}
	}
    
	
    public void saveAsParquet(DataFrame df, String path) {
		TK_DataFormatConvertUtil.deletePath(path);
		df.saveAsParquetFile(path);
	}
}
