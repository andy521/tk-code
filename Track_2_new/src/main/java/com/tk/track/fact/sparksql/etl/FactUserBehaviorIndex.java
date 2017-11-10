package com.tk.track.fact.sparksql.etl;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

public class FactUserBehaviorIndex {
	
	/**
	 * 
	 * @Description: 获取用户行为指标
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年8月2日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	public DataFrame getFactUserBehaviorIndex(HiveContext sqlContext){
		getWechatEvent(sqlContext);
		getAllUserId(sqlContext);
		getVisitTimes(sqlContext);
		getVisitDuration(sqlContext);
		getActive30(sqlContext);
		getActive7(sqlContext);
		getUserBehaviorIndexTmp(sqlContext);
		return getUserBehaviorIndex(sqlContext);
	}

	/**
	 * 
	 * @Description: 解析是否7天活跃和是否30天活跃用户
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年8月2日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserBehaviorIndex(HiveContext sqlContext) {
		String sql = "SELECT "
				+ "			 APP_TYPE,"
				+ "			 USER_ID,"
				+ "			 VISIT_TIMES,"
				+ "			 VISIT_DURATION,"
				+ "			 CASE WHEN USER_ID_7 <> '' AND USER_ID_7 IS NOT NULL THEN '1'"
				+ "				ELSE '0'"
				+ "			 END AS IS_ACTIVE7,"
				+ "			 CASE WHEN USER_ID_30 <> '' AND USER_ID_30 IS NOT NULL THEN '1'"
				+ "				ELSE '0'"
				+ "			 END AS IS_ACTIVE30"
				+ "	   FROM	 Tmp_UBI";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "FACT_USERBEHAVIOR_INDEX");
	}

	/**
	 * 
	 * @Description: 汇总各个用户行为指标
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年8月2日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserBehaviorIndexTmp(HiveContext sqlContext) {
		String sql = "SELECT "
				+ "			 TU.APP_TYPE,"
				+ "			 TU.USER_ID,"
				+ "			 VT.VISIT_TIMES,"
				+ "			 VD.VISIT_DURATION,"
				+ "			 TA30.USER_ID AS USER_ID_30,"
				+ "			 TA7.USER_ID AS USER_ID_7"
				+ "	   FROM  TMP_USERIDALL TU "
				+ " LEFT JOIN TMP_VT VT"
				+ "	  ON     TU.APP_TYPE = VT.APP_TYPE "
				+ "		 AND TU.USER_ID = VT.USER_ID "
				+ " LEFT JOIN TMP_VD VD"
				+ "	   ON    TU.APP_TYPE = VD.APP_TYPE "
				+ "		 AND TU.USER_ID = VD.USER_ID "
				+ " LEFT JOIN TMP_ACT30 TA30"
				+ "	  ON     TU.APP_TYPE = TA30.APP_TYPE "
				+ "		 AND TU.USER_ID = TA30.USER_ID "
				+ " LEFT JOIN TMP_ACT7 TA7"
				+ "	  ON     TU.APP_TYPE = TA7.APP_TYPE "
				+ "		 AND TU.USER_ID = TA7.USER_ID ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_UBI");
	}
	
	/**
	 * 
	 * @Description: 获取7天活跃用户
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年8月2日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getActive7(HiveContext sqlContext) {
		String sql = "SELECT APP_TYPE,"
				+ "			 USER_ID"
				+ "	   FROM  TMP_WE"
				+ "	  WHERE  FROM_UNIXTIME(CAST(CAST(TIME AS BIGINT) / 1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss') >= "
				+ "			 FROM_UNIXTIME((UNIX_TIMESTAMP() - 7 * 24 * 3600),'yyyy-MM-dd') "
				+ "		 AND FROM_UNIXTIME(CAST(CAST(TIME AS BIGINT) / 1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss') < "
                + "			 FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd') "
                + "	GROUP BY APP_TYPE,"
                + "			 USER_ID";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_Act7");
	}

	/**
	 * 
	 * @Description: 获取30天活跃用户
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年8月2日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getActive30(HiveContext sqlContext) {
		String sql = "SELECT APP_TYPE,"
				+ "			 USER_ID"
				+ "	   FROM  TMP_WE"
				+ "	  WHERE  FROM_UNIXTIME(CAST(CAST(TIME AS BIGINT) / 1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss') >= "
				+ "			 FROM_UNIXTIME((UNIX_TIMESTAMP() - 30 * 24 * 3600),'yyyy-MM-dd') "
				+ "		 AND FROM_UNIXTIME(CAST(CAST(TIME AS BIGINT) / 1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss') < "
                + "			 FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd') "
                + "	GROUP BY APP_TYPE,"
                + "			 USER_ID";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_Act30");
	}

	/**
	 * 
	 * @Description: 获取停留时常
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年8月2日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getVisitDuration(HiveContext sqlContext) {
		String sql = "SELECT "
				+ "			 APP_TYPE,"
				+ "			 USER_ID,"
				+ "			 CAST(SUM(CAST(COALESCE(DURATION, '0') AS BIGINT)) AS STRING) AS VISIT_DURATION"
				+ "	   FROM  TMP_WE "
				+ "   WHERE  LOWER(EVENT) = 'page.unload'"
				+ "		 AND DURATION <> ''"
				+ "		 AND DURATION IS NOT NULL"
				+ "		 AND DURATION > 0"
				+ "	GROUP BY APP_TYPE,"
				+ "			 USER_ID";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_VD");
	}

	/**
	 * 
	 * @Description: 获取所有的微信行为数据
	 * @param sqlContext
	 * @author moyunqing
	 * @date 2016年8月2日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getWechatEvent(HiveContext sqlContext) {
		String sql = "SELECT * "
				+ "	   FROM  TMP_UBA_LOG_EVENT ULE"
				+ "	   WHERE LOWER(APP_TYPE) = 'wechat'"
				+ "		 AND USER_ID <> ''"
				+ "		 AND USER_ID IS NOT NULL";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_WE", DataFrameUtil.CACHETABLE_EAGER);
	}

	/**
	 * 
	 * @Description: 获取浏览次数
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年8月2日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getVisitTimes(HiveContext sqlContext) {
		String sql = "SELECT "
				+ "			 APP_TYPE,"
				+ "			 USER_ID,"
				+ "			 CAST(COUNT(*) AS STRING) AS VISIT_TIMES"
				+ "	   FROM  TMP_WE "
				+ "	  WHERE  LOWER(EVENT) <> 'page.load'"
				+ "		 AND LOWER(EVENT) <> 'page.unload'"
				+ "		 AND EVENT IS NOT NULL"
				+ "		 AND EVENT <> ''"
				+ " GROUP BY APP_TYPE,"
				+ "			 USER_ID";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_VT");
	}

	/**
	 * 
	 * @Description: 获取所有用户ID
	 * @param sqlContext
	 * @author moyunqing
	 * @date 2016年8月2日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getAllUserId(HiveContext sqlContext) {
		String sql = "SELECT "
				+ "			 APP_TYPE,"
				+ "			 USER_ID"
				+ "	    FROM TMP_WE"
				+ "	GROUP BY APP_TYPE,USER_ID";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_UserIdAll",DataFrameUtil.CACHETABLE_EAGER);
	}
	
	/**
	 * 
	 * @Description: parquet文件保存
	 * @param df
	 * @param path
	 * @author moyunqing
	 * @date 2016年8月3日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	public void saveAsParquet(DataFrame df, String path) {
		TK_DataFormatConvertUtil.deletePath(path);
		df.saveAsParquetFile(path);
	}
}
