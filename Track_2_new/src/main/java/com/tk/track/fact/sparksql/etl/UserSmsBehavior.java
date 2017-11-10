package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.TempTeleAppUseSummary;
import com.tk.track.fact.sparksql.desttable.TempUserSmsBehavior;
import com.tk.track.fact.sparksql.desttable.UserSmsConfig;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.DBUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

public class UserSmsBehavior implements Serializable {
	
	private static final long serialVersionUID = 5241589537279973504L;
    private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT) + "/appid-type.properties";

	public DataFrame getUserSmsBehaviorDF(HiveContext sqlContext, UserSmsConfig userSmsConfig) {
		getOpenSmsPageLoadData(sqlContext, userSmsConfig);
		getOpenSmsPageUnloadData(sqlContext, userSmsConfig);
		combileLoadDataAndUnloadData(sqlContext);
		calculateDuration(sqlContext);
		DataFrame openUsers = getUserIdByScode(sqlContext, userSmsConfig);
		getClickButtonInfo(sqlContext, userSmsConfig);
		DataFrame clickUsers = fillTheUserIdByScode(sqlContext, userSmsConfig, "TMP_CLICKBUTTON_USERS", "2");
		getClickBuyButtonInfo(sqlContext, userSmsConfig);
		DataFrame clickBuyButtonUsers = fillTheUserIdByScode(sqlContext, userSmsConfig, "TMP_CLICK_BUY_BUTTON_USERS", "3");
		return openUsers.unionAll(clickUsers).unionAll(clickBuyButtonUsers);
	}

	private DataFrame fillTheUserIdByScode(HiveContext sqlContext,
			UserSmsConfig userSmsConfig, String tableName, String eventType) {
		String hql = "SELECT T2.CUSTOMER_DATA AS USER_ID, "
				   + "	CASE WHEN T2.CUSTOMER_DATA_TYPE = '0' THEN '手机号' "
				   + "     	 WHEN T2.CUSTOMER_DATA_TYPE = '1' THEN '会员号' "
				   + "       WHEN T2.CUSTOMER_DATA_TYPE = '2' THEN '客户号' "
	               + "END AS USER_TYPE,"
				   + "       FROM_UNIXTIME(CAST(CAST(T.VISIT_TIME AS BIGINT) / 1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss') AS LAST_VISITTIME,"
				   + "   '"+eventType+"' AS EVENT_TYPE,"
				   + "   '0' AS DURATION,"
				   + "'"+userSmsConfig.getSmsId()+"' AS SMS_ID"
				   + "  FROM "+tableName+" T"
				   + " INNER JOIN TKOLDB.SALES_SMS T2 "
			   + "    ON T.SCODE=T2.SALES_SMS_CODE";
		if(userSmsConfig.getActId() != null && !userSmsConfig.getActId().isEmpty()) {
			hql = hql + "   AND T2.ACT_ID='"+userSmsConfig.getActId()+"'"; 
		}
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "RESULT_"+tableName);
		return df;
	}

//	private DataFrame fillTheUserIdByScode(HiveContext sqlContext, UserSmsConfig userSmsConfig) {
//		String hql = "SELECT T2.CUSTOMER_DATA AS USER_ID, "
//				   + "	CASE WHEN T2.CUSTOMER_DATA_TYPE = '0' THEN '手机号' "
//				   + "     	 WHEN T2.CUSTOMER_DATA_TYPE = '1' THEN '会员号' "
//				   + "       WHEN T2.CUSTOMER_DATA_TYPE = '2' THEN '客户号' "
//	               + "END AS USER_TYPE,"
//				   + "       FROM_UNIXTIME(CAST(CAST(T.VISIT_TIME AS BIGINT) / 1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss') AS LAST_VISITTIME,"
//				   + "   '2' AS EVENT_TYPE,"
//				   + "   '0' AS DURATION,"
//				   + "'"+userSmsConfig.getSmsId()+"' AS SMS_ID"
//				   + "  FROM TMP_CLICKBUTTON_USERS T"
//				   + " INNER JOIN TKOLDB.SALES_SMS T2 "
// 			   + "    ON T.SCODE=T2.SALES_SMS_CODE";
//		if(userSmsConfig.getActId() != null && !userSmsConfig.getActId().isEmpty()) {
//			hql = hql + "   AND T2.ACT_ID='"+userSmsConfig.getActId()+"'"; 
//		}
//		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_CLICKBUTTON_RESULT");
//		return df;
//	}

	private DataFrame getClickButtonInfo(HiveContext sqlContext, UserSmsConfig userSmsConfig) {
		String hql = "SELECT T.SCODE, "
				   + "       MAX(T.VISIT_TIME) AS VISIT_TIME "
				   + "  FROM (SELECT PARSE_URL(CURRENTURL,'QUERY', 'scode') AS SCODE,"
				   + "               CAST(CLIENTTIME AS BIGINT) AS VISIT_TIME"
				   + "          FROM UBA_LOG_EVENT "
				   + "         WHERE SUBTYPE='立即投保' "
				   + "           AND CURRENTURL LIKE '"+userSmsConfig.getActURL()+"%' "
				   + "           AND CURRENTURL LIKE '%from="+userSmsConfig.getFromId()+"%' "
			   	   + "       ) T"
			   	   + " GROUP BY T.SCODE";
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_CLICKBUTTON_USERS");
		return df;
	}
	
	private DataFrame getClickBuyButtonInfo(HiveContext sqlContext, UserSmsConfig userSmsConfig) {
		String hql = "SELECT T.SCODE, "
				+ "       MAX(T.VISIT_TIME) AS VISIT_TIME "
				+ "  FROM (SELECT PARSE_URL(CURRENTURL,'QUERY', 'scode') AS SCODE,"
				+ "               CAST(CLIENTTIME AS BIGINT) AS VISIT_TIME"
				+ "          FROM UBA_LOG_EVENT "
				+ "         WHERE SUBTYPE='确认购买' "
				+ "           AND CURRENTURL LIKE '"+userSmsConfig.getDetailURL()+"%' "
				+ "           AND CURRENTURL LIKE '%from="+userSmsConfig.getFromId()+"%' "
				+ "       ) T"
				+ " GROUP BY T.SCODE";
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_CLICK_BUY_BUTTON_USERS");
		return df;
	}

	private DataFrame getOpenSmsPageLoadData(HiveContext sqlContext, UserSmsConfig userSmsConfig) {
		String hql = "SELECT PARSE_URL(CURRENTURL,'QUERY', 'scode') AS SCODE," 
	               + "       TERMINAL_ID," 
				   + "       SUBTYPE,"
				   + "       CAST(CLIENTTIME AS BIGINT) AS VISIT_TIME "
				   + "  FROM UBA_LOG_EVENT "
				   + " WHERE EVENT='page.load' "
				   + "   AND CURRENTURL LIKE '"+userSmsConfig.getActURL()+"%' "
				   + "   AND CURRENTURL LIKE '%from="+userSmsConfig.getFromId()+"%' ";
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_LOAD_DATA");
		return df;

	}

	private DataFrame getOpenSmsPageUnloadData(HiveContext sqlContext, UserSmsConfig userSmsConfig) {
		String hql = "SELECT PARSE_URL(CURRENTURL,'QUERY', 'scode') AS SCODE," 
	               + "       TERMINAL_ID," 
				   + "       SUBTYPE,"
				   + "       CAST(CLIENTTIME AS BIGINT) AS VISIT_TIME "
				   + "  FROM UBA_LOG_EVENT WHERE EVENT='page.unload' "
				   + "   AND CURRENTURL LIKE '"+userSmsConfig.getActURL()+"%' "
				   + "   AND CURRENTURL LIKE '%from="+userSmsConfig.getFromId()+"%' ";
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_UNLOAD_DATA");
		System.out.println("================getTeleAppUseSummaryShutdown================" + df.count());
		df.show(10);
		return df;
	}
/*
 * 拥有结束时间对应多个开始时间的情况（需要去掉）
 * 
 */
	private DataFrame combileLoadDataAndUnloadData(HiveContext sqlContext) {
		String hql = "SELECT TEMP.SCODE, TEMP.ENDTIME, MAX(TEMP.STARTTIME) AS STARTTIME "
					+ " FROM("
					+ "		SELECT D.SCODE AS SCODE,D.TERMINAL_ID AS TERMINAL_ID,D.VISIT_TIME AS STARTTIME,U.VISIT_TIME AS ENDTIME"
					+ " 	FROM TMP_LOAD_DATA AS D"
					+ "		LEFT JOIN TMP_UNLOAD_DATA AS U "
					+ "		ON( U.SCODE=D.SCODE "
					+ "		AND U.TERMINAL_ID=D.TERMINAL_ID"
					+ " 	AND U.VISIT_TIME<D.VISIT_TIME) "
					+ ") AS TEMP "
					+ " WHERE STARTTIME < ENDTIME or ENDTIME IS NULL"
					+ " GROUP BY TEMP.SCODE, TEMP.ENDTIME";
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_COMBILED_DATA");
		System.out.println("================getTeleSystemUseConditionResult================" + df.count());
		df.show(10);
		return df;
	}
	
	private DataFrame calculateDuration(HiveContext sqlContext) {
		String hql = "SELECT TEMP.SCODE,"
				+ "			 MAX(FROM_UNIXTIME(CAST(CAST(TEMP.STARTTIME AS BIGINT) / 1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss')) AS STARTTIME,"
				+ "			 CAST(MAX(TEMP.ENDTIME-TEMP.STARTTIME) AS STRING) DURATION"
				+ "	    FROM TMP_COMBILED_DATA AS TEMP"
				+ "	GROUP BY TEMP.SCODE";
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TELE_APPUSESUMMARY_RET");

		return df;
	}
	
	private DataFrame getUserIdByScode(HiveContext sqlContext, UserSmsConfig userSmsConfig) {
		String hql = "SELECT T2.CUSTOMER_DATA AS USER_ID, "
				   + "	CASE WHEN T2.CUSTOMER_DATA_TYPE = '0' THEN '手机号' "
				   + "     	 WHEN T2.CUSTOMER_DATA_TYPE = '1' THEN '会员号' "
				   + "       WHEN T2.CUSTOMER_DATA_TYPE = '2' THEN '客户号' "
	               + "END AS USER_TYPE,"
				   + "       MAX(T.STARTTIME) AS LAST_VISITTIME,"
				   + "   '1' AS EVENT_TYPE,"
				   + "       MAX(T.DURATION) AS DURATION,"
				   + "'"+userSmsConfig.getSmsId()+"' AS SMS_ID"
				   + "  FROM TMP_TELE_APPUSESUMMARY_RET T"
				   + " INNER JOIN TKOLDB.SALES_SMS T2 "
    			   + "    ON T.SCODE=T2.SALES_SMS_CODE";
		if(userSmsConfig.getActId() != null && !userSmsConfig.getActId().isEmpty()) {
			hql = hql + "   AND T2.ACT_ID='"+userSmsConfig.getActId()+"'"; 
		}
		hql = hql +" GROUP BY T2.CUSTOMER_DATA, T2.CUSTOMER_DATA_TYPE";
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_OPEN_PAGE_RESULT");
		return df;
	}
	
	public JavaRDD<TempUserSmsBehavior> getJavaRDD(DataFrame df) {
		JavaRDD<Row> jRDD = df.select("SMS_ID", "USER_ID", "USER_TYPE", "LAST_VISITTIME", "EVENT_TYPE", "DURATION").rdd().toJavaRDD();
		JavaRDD<TempUserSmsBehavior> pRDD = jRDD.map(new Function<Row, TempUserSmsBehavior>() {
			private static final long serialVersionUID = 2887518971635512003L;

			public TempUserSmsBehavior call(Row v1) throws Exception {
				return new TempUserSmsBehavior(v1.getString(0), v1.getString(1), v1.getString(2), v1.getString(3), v1.getString(4), v1.getString(5));
			}
		});
		return pRDD;
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return timeStamp;
	}

	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<TempUserSmsBehavior> rdd, String path) {
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			String tmpPath = path + "-temp";
			Integer repartition_count = 200;
			try {
				repartition_count = Integer
						.valueOf(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_REPARTITION_COUNT));
			} catch (Exception e) {
				repartition_count = 200;
			}

			// Append new data to parquet[path]
			sqlContext.createDataFrame(rdd, TempUserSmsBehavior.class).save(path, "parquet", SaveMode.Append);
			// Load parquet[path], save as parquet[tmpPath]
			TK_DataFormatConvertUtil.deletePath(tmpPath);
			sqlContext.load(path).repartition(repartition_count).saveAsParquetFile(tmpPath);
			// Delete parquet[path]
			TK_DataFormatConvertUtil.deletePath(path);
			// Rename parquet[tmpPath] as parquet[path]
			TK_DataFormatConvertUtil.renamePath(tmpPath, path);
		} else {
			sqlContext.createDataFrame(rdd, TempUserSmsBehavior.class).saveAsParquetFile(path);
		}
	}

	public List<UserSmsConfig> getUserSmsConfigs() {
		List<UserSmsConfig> userSmsConfigs = new ArrayList<UserSmsConfig>();
		String dbURL = TK_CommonConfig.getConfigValue(CONFIG_PATH, "db.url");
        String dbUsername = TK_CommonConfig.getConfigValue(CONFIG_PATH, "db.user");
        String dbPassword = TK_CommonConfig.getConfigValue(CONFIG_PATH, "db.password");
		Connection conn = DBUtil.getConnection(dbURL, dbUsername, dbPassword);
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "SELECT SMS_ID, FROM_ID, ACT_URL, ACT_ID, DETAIL_URL FROM SMS_CONFIG WHERE IS_EFFECT='1'";
        try {
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()) {
				UserSmsConfig userSmsConfig = new UserSmsConfig();
				String smsId = rs.getString("SMS_ID");
				String fromId = rs.getString("FROM_ID");
				String actURL = rs.getString("ACT_URL");
				String actId = rs.getString("ACT_ID");
				String detailURL = rs.getString("DETAIL_URL");
				userSmsConfig.setSmsId(Long.parseLong(smsId));
				userSmsConfig.setFromId(fromId);
				userSmsConfig.setActURL(actURL);
				userSmsConfig.setActId(actId);
				userSmsConfig.setDetailURL(detailURL);
				userSmsConfigs.add(userSmsConfig);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DBUtil.closeConnection(rs, ps, conn);
		}
		return userSmsConfigs;
	}

}
