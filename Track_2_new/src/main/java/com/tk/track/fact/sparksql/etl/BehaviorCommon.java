package com.tk.track.fact.sparksql.etl;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

/**
 * 
* <p>Title: 泰康大数据部</p>
* <p>Description: 通用Etl处理程序入口</p>
* <p>Copyright: Copyright (c) 2016</p>
* <p>Company: 大连华信有限公司</p>
* <p>Department: 总公司\数据信息中心\IT外包公司\大连华信总公司\数据信息中心\IT外包公司\大连华信部</p>
* @author moyunqing
* @date   2016年9月20日
* @version 1.0
 */
public class BehaviorCommon {
	
	/**
	 * 
	 * @Description: 获取处理后的线索表
	 * @param sqlContext
	 * @param isAll
	 * @param target
	 * @return
	 * @author moyunqing
	 * @date 2016年9月20日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	public DataFrame getUBHClue(HiveContext sqlContext,String table,String isAll,String target){
		getAllClueByTarget(sqlContext,table,target);
		getNearstClueByDay(sqlContext,isAll);
		getEventOrClueByDay(sqlContext,isAll);
		updateUBHClueType(sqlContext);
		return getUBHClueAll(sqlContext);
	}
	

	/**
	 * 
	 * @Description: 合并线索类型数据
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月20日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUBHClueAll(HiveContext sqlContext) {
		String sql = "SELECT TEC.*"
				+ "	   FROM  TMP_EC TEC"
				+ "	   WHERE TEC.CLUE_TYPE = '2'"
				+ "	UNION ALL "
				+ "	  SELECT PC.*"
				+ "	   FROM  TMP_PRTCLUE PC";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "FACT_USER_BEHAVIOR_TELE");
	}


	/**
	 * 
	 * @Description: 区分出线索表中部分事件记录
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年9月20日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getEventOrClueByDay(HiveContext sqlContext,String isAll) {
		String sql = "SELECT DISTINCT TAC.*,"
				+ "			 CASE WHEN TMVT.USER_ID IS NOT NULL THEN ''"
				+ "				  WHEN TMVT.USER_ID IS NULL THEN '2'"
				+ "				  ELSE '0'"
				+ "			 END AS CLUE_TYPE";
		if(!(StringUtils.isNotBlank(isAll) && Boolean.parseBoolean(isAll))){
			sql += "   FROM (SELECT * "
					+ "		  FROM  TMP_ALLCLUE "
					+ "		 WHERE  TO_DATE(VISIT_TIME) = '" + getDate(-1) + "') TAC";
		}else{
			sql += "  FROM  TMP_ALLCLUE TAC";
		}
		sql = sql +  " LEFT JOIN TMP_MINVT TMVT"
				  + "	    ON   TAC.USER_ID = TMVT.USER_ID"
			      + "		 AND TAC.VISIT_TIME = TMVT.VISIT_TIME ";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_EC",DataFrameUtil.CACHETABLE_PARQUET);
	}


	/**
	 * 
	 * @Description: 根据条件获取所有线索
	 * @param sqlContext
	 * @param table
	 * @param target
	 * @return
	 * @author moyunqing
	 * @date 2016年9月20日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getAllClueByTarget(HiveContext sqlContext,String table,String target) {
		String sql = "SELECT DISTINCT"
				+ "			 ROWKEY,"
				+ "			 USER_ID,"
				+ "			 USER_TYPE,"
				+ "			 USER_EVENT,"
				+ "			 THIRD_LEVEL,"
				+ "			 FOURTH_LEVEL,"
				+ "			 LRT_ID,"
				+ "			 CLASS_ID,"
				+ "			 INFOFROM,"
				+ "			 VISIT_COUNT,"
				+ "			 VISIT_TIME,"
				+ "			 VISIT_DURATION,"
				+ "			 CLASSIFY_NAME,"
				+ "			 PRODUCT_NAME,"
				+ "			 USER_NAME,"
				+ "			 IF_PAY,"
				+ "			 FROM_ID,"
				+ "			 REMARK"
				+ "	   FROM  " + table;
		if(StringUtils.isNotBlank(target)){
			sql += " WHERE LOWER(APP_ID) IN (" + target.toLowerCase() + ")";
		}
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_AllClue",DataFrameUtil.CACHETABLE_PARQUET);
		
	}

	/**
	 * 
	 * @Description: 获取每天每个用户第一次的浏览记录
	 * @param sqlContext
	 * @param table
	 * @param isAll
	 * @param target
	 * @return
	 * @author moyunqing
	 * @date 2016年9月20日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getNearstClueByDay(HiveContext sqlContext,String isAll) {
		String sql = "SELECT DAY,"
				+ "			 USER_ID,"
				+ "			 MIN(VISIT_TIME) AS VISIT_TIME "
				+ "	   FROM("
				+ "			 SELECT "
				+ "					TO_DATE(VISIT_TIME) AS DAY,"
				+ "			 		USER_ID,"
				+ "			 		VISIT_TIME"
				+ "	   		  FROM  TMP_ALLCLUE) T";
		if(!(StringUtils.isNotBlank(isAll) && Boolean.parseBoolean(isAll))){
			sql += "   WHERE  DAY = '" + getDate(-1) + "'";
		}
		sql += "	GROUP BY DAY,USER_ID";
		return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_minVT");
	}

	/**
     * 
     * @Description: 添加线索类型列（1-线索；2-事件；0-预留）
     * @param sqlContext
     * @return
     * @author moyunqing
     * @date 2016年9月19日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    private DataFrame updateUBHClueType(HiveContext sqlContext) {
		String sql = "SELECT DISTINCT "
				+ "			 EC.ROWKEY,"
				+ "			 EC.USER_ID,"
				+ "			 EC.USER_TYPE,"
				+ "			 EC.USER_EVENT,"
				+ "			 EC.THIRD_LEVEL,"
				+ "			 EC.FOURTH_LEVEL,"
				+ "			 EC.LRT_ID,"
				+ "			 EC.CLASS_ID,"
				+ "			 EC.INFOFROM,"
				+ "			 EC.VISIT_COUNT,"
				+ "			 EC.VISIT_TIME,"
				+ "			 EC.VISIT_DURATION,"
				+ "			 EC.CLASSIFY_NAME,"
				+ "			 EC.PRODUCT_NAME,"
				+ "			 EC.USER_NAME,"
				+ "			 EC.IF_PAY,"
				+ "			 EC.FROM_ID,"
				+ "			 EC.REMARK,"
				+ "			 CASE WHEN TAC.USER_ID IS NULL THEN '1'"
				+ "				  WHEN TAC.USER_ID IS NOT NULL THEN '2'"
				+ "				  ELSE '0'"
				+ "			 END AS CLUE_TYPE"
				+ "    FROM  (SELECT * "
				+ "			   FROM  TMP_EC"
				+ "			  WHERE  CLUE_TYPE = '') EC "
				+ "	 LEFT JOIN Tmp_AllClue TAC"
				+ "		 ON  EC.USER_ID = TAC.USER_ID "
				+ "		 AND EC.VISIT_TIME > TAC.VISIT_TIME";
    	return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_prtClue");
	}
    
    /**
     * 
     * @Description: parquet文件保存
     * @param df
     * @param path
     * @author moyunqing
     * @date 2016年9月20日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    public void saveAsParquet(HiveContext sqlContext,DataFrame df,String tableName,String path,String isAll) {
    	if(!(StringUtils.isNotBlank(isAll) && Boolean.parseBoolean(isAll))){
    		long timeTag = System.currentTimeMillis();
			String preDataPath = path + timeTag;
			DataFrame preDf = getPreData(sqlContext,tableName);//过滤掉今日新生成数据
    		preDf.saveAsParquetFile(preDataPath);//保存之前数据
        	df.save(preDataPath, "parquet", SaveMode.Append);
        	TK_DataFormatConvertUtil.deletePath(path);
        	TK_DataFormatConvertUtil.renamePath(preDataPath, path);
    	}else{
    		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
    			long timeTag = System.currentTimeMillis();
    			String tmpPath = path + timeTag;
    			df.saveAsParquetFile(tmpPath);
    			TK_DataFormatConvertUtil.deletePath(path);
    			TK_DataFormatConvertUtil.renamePath(tmpPath, path);
    		} else {
    			df.saveAsParquetFile(path);
    		}
    	}
    	
	}
    
    /**
     * 
     * @Description: 取之前数据
     * @param sqlContext
     * @param tableName
     * @return
     * @author moyunqing
     * @date 2016年9月24日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    private DataFrame getPreData(HiveContext sqlContext, String tableName) {
		String sql = "SELECT * "
				+ "		  FROM  " + tableName
				+ "		 WHERE  TO_DATE(VISIT_TIME) < '" + getDate(-1) + "'";
    	return DataFrameUtil.getDataFrame(sqlContext, sql, "Tmp_preData");
	}


	/**
     * 
     * @Description: 获取日期字符串
     * @param day
     * @return
     * @author moyunqing
     * @date 2016年9月20日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    private static String getDate(int day) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, day);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(cal.getTime());
	}
}
