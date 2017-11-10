/**
 * @Title: TerminalMap.java
 * @Package: com.tk.track.fact.sparksql.etl
 * @Description: TODO
 * @author zhang.shy
 * @Data 2016年5月7日
 * @version 1.0
 */
package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.tk.track.base.table.BaseTable;
import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.FactTerminalMap;
import com.tk.track.fact.sparksql.desttable.TempFactSrcUserEvent;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.IDUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

/**
 * @ClassName: TerminalMap
 * @Description: TODO
 * @author zhang.shy
 * @date 2016年5月7日
 */
public class TerminalMap implements Serializable {

    private static final long serialVersionUID = -8855146171916772404L;
    private static final String HIVE_SCHEMA = TK_DataFormatConvertUtil.getSchema();
    private static Logger logger = LoggerFactory.getLogger(TerminalMap.class);

    public DataFrame getTerminalMapInfoResultDF(HiveContext sqlContext) {
    	//load csv file data
    	//从文件引入映射关系，变成了自动从sales_sms表中匹配，所以这个不需要了
//    	boolean existImportData = loadCSVFileData(sqlContext);
//    	logger.info("============existImportData=========="+existImportData);
    	//从sales_sms表中进行匹配
    	findScodeWithoutUserId(sqlContext);
    	getTerminalMapByScodeSMS(sqlContext);
    	
    	fillUserIdByOpenId(sqlContext);
    	boolean shoudImportData = true;
    	DataFrame TerMapDF = getTerminalMapDF(sqlContext, shoudImportData);
        JavaRDD<FactTerminalMap> TerMapRDD = getJavaRDDAll(TerMapDF);

        // write to parquet
        String TERMINAL_MAP_FILENAME_WRITE = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FTERMINALMAP_OUTPUTPATH);
        pairRDD2Parquet(sqlContext, TerMapRDD, TERMINAL_MAP_FILENAME_WRITE);

        // change file name
        String TempPath = TERMINAL_MAP_FILENAME_WRITE + "_all";
        TK_DataFormatConvertUtil.renamePath(TERMINAL_MAP_FILENAME_WRITE, TempPath);
        // reload data
        DataFrame df = sqlContext.load(TempPath);
        df.registerTempTable("TMP_TERMINAL_MAP_ALL");
        return getTerminalMapInfoResult(sqlContext);
    }
    
    private boolean loadCSVFileData(HiveContext sqlContext) {
    	Map<String, String> options = new HashMap<String, String>();
    	options.put("header", "true");
    	String csvFilePath = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FTERMINALMAP_CSVFILEPATH);
    	if(TK_DataFormatConvertUtil.isExistsPath(csvFilePath)) {
    		options.put("path", csvFilePath);
    		DataFrame df = sqlContext.load("com.databricks.spark.csv", options);
    		df.registerTempTable("IMPORT_TERMINAL_MAP_DATA");
    		logger.info("=================debug===============show():"+df.count());
    		
    		df.limit(10).show();
    		return true;
    	}
    	return false;
	}
    
    private void fillUserIdByOpenId(HiveContext sqlContext) {
    	String hql = "SELECT A.IP, "
    			   + " A.TIME, "
    			   + " A.APP_TYPE, "
    			   + " A.APP_ID, "
    			   + " CASE WHEN  A.LABEL LIKE '%openid%' AND A.LABEL NOT LIKE '%openid:\"\"%' THEN regexp_extract(A.LABEL,'(?<=openid:\")[^\",]*',0)" 
    			   + "      ELSE  A.USER_ID"
                   + " END    AS  USER_ID,"
    			   + " A.PAGE, "
    			   + " A.EVENT, "
    			   + " A.SUBTYPE, "
    			   + " A.LABEL, "
    			   + " A.CUSTOM_VAL, "
    			   + " A.REFER, "
    			   + " A.CURRENTURL, "
    			   + " A.BROWSER, "
    			   + " A.SYSINFO, "
    			   + " A.TERMINAL_ID, "
    			   + " A.DURATION, "
    			   + " A.CLIENTTIME, "
    			   + " A.FROM_ID "
                   + " FROM Temp_SrcUserEvent A ";
    	DataFrame resultDataFrame = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILTER_EVENT",DataFrameUtil.CACHETABLE_EAGER);
        JavaRDD<TempFactSrcUserEvent> labelRdd = analysisLabelRDD(resultDataFrame);
        sqlContext.createDataFrame(labelRdd, TempFactSrcUserEvent.class).registerTempTable("Temp_SrcUserEvent_WithOpenID");
//        DataFrameUtil.getDataFrame(sqlContext, hql, "Temp_SrcUserEvent_WithOpenID");
	}


    public JavaRDD<TempFactSrcUserEvent> analysisLabelRDD(DataFrame df) {
		JavaRDD<Row> jRDD = df.select("IP", "TIME", "APP_TYPE", "APP_ID", "USER_ID", "PAGE", "EVENT", "SUBTYPE",
						"LABEL", "CUSTOM_VAL", "REFER", "CURRENTURL", "BROWSER", "SYSINFO", "TERMINAL_ID", "DURATION", "CLIENTTIME", "FROM_ID").rdd().toJavaRDD();
		
		JavaRDD<TempFactSrcUserEvent> pRDD = jRDD.map(new Function<Row, TempFactSrcUserEvent>() {

			private static final long serialVersionUID = 2966182622609483359L;

			public TempFactSrcUserEvent call(Row v1) throws Exception {
				return analysisSrcUserEventLable(v1.getString(0), v1.getString(1), v1.getString(2), v1.getString(3),
						v1.getString(4), v1.getString(5), v1.getString(6), v1.getString(7), v1.getString(8),
						v1.getString(9), v1.getString(10), v1.getString(11), v1.getString(12), v1.getString(13), v1.getString(14), v1.getString(15), v1.getString(16), v1.getString(17));
			}
		});
		return pRDD;
	}
    
    protected TempFactSrcUserEvent analysisSrcUserEventLable(String IP,
			String TIME, String APP_TYPE, String APP_ID, String USER_ID,
			String PAGE, String EVENT, String SUBTYPE, String LABEL,
			String CUSTOM_VAL, String REFER, String CURRENTURL, String BROWSER,
			String SYSINFO, String TERMINAL_ID, String DURATION, String CLIENTTIME, String FROM_ID) {
    	Map<String, String> urlParamsMap = parseUrlParams(CURRENTURL);
    	String from = urlParamsMap.get("from");
    	String openId = urlParamsMap.get("openid");
    	String scode = urlParamsMap.get("scode");
    	if(from != null && !from.isEmpty() && openId!=null && !openId.isEmpty()) {
    		FROM_ID = from;
    	}
    	if(APP_ID.equals("clue_H5_mall_001") || APP_ID.equals("H5_insure_flow")) {
    		if(USER_ID == null || USER_ID.trim().isEmpty()) {
    			if( openId!=null && !openId.isEmpty()) {
    				if(LABEL == null || LABEL.isEmpty()) {
    					LABEL = "openid:\""+openId+"\"";
    				} else {
    					if(LABEL.indexOf("openid") == -1) {
    						LABEL = LABEL + ",openid:\""+openId+"\"";
    					} else {
    						LABEL = LABEL.replace("openid:\"\"", "openid:\""+openId+"\"");
    					}
    				}
    				USER_ID = openId;
    			}
    			if( scode!=null && !scode.isEmpty() && CURRENTURL.indexOf("http://m.tk.cn/product/wap/S20160001.html") != -1) {
    				if(LABEL == null || LABEL.isEmpty()) {
    					LABEL = "scode:\""+scode+"\"";
    				} else {
    					if(LABEL.indexOf("scode") == -1) {
    						LABEL = LABEL + ",scode:\""+scode+"\"";
    					}
    				}
    				USER_ID = scode;
    			}
    		}
    	}
		return new TempFactSrcUserEvent(IP, TIME, APP_TYPE, APP_ID, USER_ID, PAGE, EVENT,
				SUBTYPE, LABEL, CUSTOM_VAL, REFER, CURRENTURL, BROWSER, SYSINFO, TERMINAL_ID,
				DURATION, CLIENTTIME, FROM_ID);
	}
    /**
     * 去掉url中的路径，留下请求参数部分
     * @param strURL url地址
     * @return url请求参数部分
     */
    private static String TruncateUrlPage(String strURL) {
    	String strAllParam=null;
    	String[] arrSplit=null;
    	strURL=strURL.trim();
    	arrSplit=strURL.split("[?]");
    	if(strURL.length()>1) {
    		if(arrSplit.length>1) {
    			if(arrSplit[1]!=null) {
    				strAllParam=arrSplit[1];
    			}
    		}
    	}
    	return strAllParam;    
    }
    public static Map<String, String> parseUrlParams(String URL) {
    	Map<String, String> mapRequest = new HashMap<String, String>();
    	String[] arrSplit=null;
    	String strUrlParam = TruncateUrlPage(URL);
    	if(strUrlParam==null) {
    		return mapRequest;
    	}
    	//每个键值为一组
    	arrSplit=strUrlParam.split("[&]");
    	for(String strSplit : arrSplit) {
    		String[] arrSplitEqual=null;          
    		arrSplitEqual= strSplit.split("[=]"); 
    		//解析出键值
    		if(arrSplitEqual.length>1) {
    			//正确解析
    			mapRequest.put(arrSplitEqual[0], arrSplitEqual[1]);
    		} else {
    			if(arrSplitEqual[0]!="") {
    				//只有参数没有值，不加入
    				mapRequest.put(arrSplitEqual[0], "");        
    			}
    		}
    	}    
    	return mapRequest;    
    }
    private void findScodeWithoutUserId(HiveContext sqlContext) {
    	Calendar calendar = Calendar.getInstance();
    	calendar.add(Calendar.DAY_OF_MONTH, -1);
    	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    	String yesterdayDate = simpleDateFormat.format(calendar.getTime());
//    	String hql = "select regexp_extract(LABEL,'(?<=scode:\")[^\",]*',0) as SCODE,"
//    			+ "			 TERMINAL_ID,"
//    			+ "          CLIENTTIME "
//    			+ "     from Temp_SrcUserEvent "
//    			+ "    where from_unixtime(cast(cast(time as bigint) / 1000 as bigint),'yyyy-MM-dd')='"+yesterdayDate+"'"
//    			+ "      and event <> 'page.load' "
//    			+ "      and event <> 'page.unload' "
//    			+ "      and label like '%scode%' "
//    			+ "      and label not like '%scode:\"\"%' "
//    			+ "      and (user_id is null or user_id = '')";
    	String hql = "select parse_url(currenturl,'QUERY', 'scode') as SCODE, "
    		       + "       TERMINAL_ID,"
    			   + "       CLIENTTIME  "
    			   + " from Temp_SrcUserEvent "
    			   + " where from_unixtime(cast(cast(time as bigint) / 1000 as bigint),'yyyy-MM-dd')='"+yesterdayDate+"'"
//				   + " and event <> 'page.load' "
//				   + " and event <> 'page.unload' "
				   + " and currenturl like '%scode%' "
				   + " and (user_id is null or user_id = '')";
    	DataFrameUtil.getDataFrame(sqlContext, hql, "Temp_SrcUserEvent_ScodeWithoutUserId");
    }
    
    private void getTerminalMapByScodeSMS(HiveContext sqlContext) {
    	String hql = "select t1.TERMINAL_ID, "
    			+ "          t2.CUSTOMER_DATA,"
    			+ "          t1.CLIENTTIME,"
    			+ "          t2.CUSTOMER_DATA_TYPE "
    			+ "     from Temp_SrcUserEvent_ScodeWithoutUserId t1 "
    			+ "     join tkoldb.SALES_SMS t2 "
    			+ "       on t1.SCODE=t2.SALES_SMS_CODE";
    	DataFrameUtil.getDataFrame(sqlContext, hql, "Terminal_Map_Imp_Sms");
    }
    
	private DataFrame getTerminalMapDF(HiveContext sqlContext, boolean shoudImportData) {
		String hql = "";
		if(shoudImportData) {
			hql = "SELECT tt.APP_TYPE, tt.APP_ID, tt.TERMINAL_ID, tt.USER_ID, tt.CLIENTTIME FROM (" 
				+ "		SELECT A.APP_TYPE,"
    			+ "			 A.APP_ID,"
    			+ "			 A.TERMINAL_ID," 
    			+ "          A.USER_ID," 
    			+ "          CASE WHEN A.CLIENTTIME = '' OR A.CLIENTTIME IS NULL THEN A.TIME"
    			+ "				  ELSE A.CLIENTTIME"
    			+ "			 END AS CLIENTTIME"
    			+ "     FROM Temp_SrcUserEvent_WithOpenID A "
    			+ "    WHERE A.TERMINAL_ID <>'' "
    			+ "      AND A.TERMINAL_ID IS NOT NULL "
    			+ "      AND A.USER_ID <>''  "
    			+ "      AND A.USER_ID IS NOT NULL "
    			+ "		 AND LOWER(A.USER_ID) <> 'openid'"
    			+ "		 AND A.APP_TYPE <> ''"
    			+ "		 AND A.APP_TYPE IS NOT NULL"
    			+ "		 AND A.APP_ID <> ''"
    			+ "		 AND A.APP_ID IS NOT NULL"
//    			+ "      AND A.CURRENTURL NOT LIKE '%openid%'"	//导致wechat070等活动的terminalid不会进入f_terminal_app，导致f_statics_event没有数据
    			//从文件读取到的scode信息，有了后面的这个可以不用了
//    			+ "     UNION ALL "
//        		+ "   SELECT 'IMP' AS APP_TYPE,"
//    			+ "			 'IMP_001' AS APP_ID,"
//        		+ "			 T.TERMINAL_ID,"
//				+ "          CASE WHEN T.CUSTOMER_DATA_TYPE = '0' THEN CONCAT('PHO',T.CUSTOMER_DATA)"
//				+ "               WHEN T.CUSTOMER_DATA_TYPE = '1' THEN CONCAT('MEM',T.CUSTOMER_DATA)"
//				+ "               WHEN T.CUSTOMER_DATA_TYPE = '2' THEN CONCAT('CUS',T.CUSTOMER_DATA)"
//				+ "			 END AS USER_ID,"
//				+ "          T.CLIENTTIME"
//				+ "     FROM IMPORT_TERMINAL_MAP_DATA T "
//				+ "    WHERE T.TERMINAL_ID <>'' "
//				+ "      AND T.TERMINAL_ID IS NOT NULL "
//				+ "      AND T.CUSTOMER_DATA <>''  "
//				+ "      AND T.CUSTOMER_DATA IS NOT NULL "
//				+ "		 AND T.CUSTOMER_DATA_TYPE <> ''"
//				+ "		 AND T.CUSTOMER_DATA_TYPE IS NOT NULL"
				//自动同步过来的scode对应的客户号信息
				+ "     UNION ALL "
        		+ "   SELECT 'IMP' AS APP_TYPE,"
    			+ "			 'IMP_001' AS APP_ID,"
        		+ "			 TMIS.TERMINAL_ID,"
				+ "          CASE WHEN TMIS.CUSTOMER_DATA_TYPE = '0' THEN CONCAT('PHO',TMIS.CUSTOMER_DATA)"
				+ "               WHEN TMIS.CUSTOMER_DATA_TYPE = '1' THEN CONCAT('MEM',TMIS.CUSTOMER_DATA)"
				+ "               WHEN TMIS.CUSTOMER_DATA_TYPE = '2' THEN CONCAT('CUS',TMIS.CUSTOMER_DATA)"
				+ "			 END AS USER_ID,"
				+ "          TMIS.CLIENTTIME"
				+ "     FROM Terminal_Map_Imp_Sms TMIS "
				+ "    WHERE TMIS.TERMINAL_ID <>'' "
				+ "      AND TMIS.TERMINAL_ID IS NOT NULL "
				+ "      AND TMIS.CUSTOMER_DATA <>''  "
				+ "      AND TMIS.CUSTOMER_DATA IS NOT NULL "
				+ "		 AND TMIS.CUSTOMER_DATA_TYPE <> ''"
				+ "		 AND TMIS.CUSTOMER_DATA_TYPE IS NOT NULL) tt";
        } else {
        	hql = "SELECT A.APP_TYPE,"
        			+ "			 A.APP_ID,"
        			+ "			 A.TERMINAL_ID," 
        			+ "          A.USER_ID," 
        			+ "          CASE WHEN A.CLIENTTIME = '' OR A.CLIENTTIME IS NULL THEN A.TIME"
        			+ "				  ELSE A.CLIENTTIME"
        			+ "			 END AS CLIENTTIME"
        			+ "     FROM Temp_SrcUserEvent_WithOpenID A "
        			+ "    WHERE A.TERMINAL_ID <>'' "
        			+ "      AND A.TERMINAL_ID IS NOT NULL "
        			+ "      AND A.USER_ID <>''  "
        			+ "      AND A.USER_ID IS NOT NULL "
        			+ "		 AND LOWER(A.USER_ID) <> 'openid'"
        			+ "		 AND A.APP_TYPE <> ''"
        			+ "		 AND A.APP_TYPE IS NOT NULL"
        			+ "		 AND A.APP_ID <> ''"
        			+ "		 AND A.APP_ID IS NOT NULL";
        }
		logger.info("============getTerminalMapDF======hql===="+hql);
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_TERMINAL_MAP");
    }

    private DataFrame getTerminalMapInfoResult(HiveContext sqlContext) {
    	String hql = "SELECT TMA.APP_TYPE,"
        		+ "			 TMA.APP_ID,"
        		+ "			 TMA.TERMINAL_ID," 
                + "          TMA.USER_ID," 
                + "      CAST(MAX(CAST(TMA.CLIENTTIME AS BIGINT)) AS STRING) AS CLIENTTIME"
                + "     FROM TMP_TERMINAL_MAP_ALL TMA" 
                + "    GROUP BY TMA.APP_TYPE,TMA.APP_ID,TMA.TERMINAL_ID, TMA.USER_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FACT_TERMINAL_MAP");
    }

    public JavaRDD<FactTerminalMap> getJavaRDDAll(DataFrame df) {
        RDD<Row> rdd = df.select("TERMINAL_ID", "USER_ID", "CLIENTTIME","APP_TYPE","APP_ID").rdd();
        JavaRDD<Row> jRDD = rdd.toJavaRDD();
        JavaRDD<FactTerminalMap> pRDD = jRDD.map(new Function<Row, FactTerminalMap>() {

            private static final long serialVersionUID = 2921123701554148589L;

            public FactTerminalMap call(Row v1) throws Exception {
                String rowKey = "";
                rowKey = IDUtil.getUUID().toString();
                return new FactTerminalMap(rowKey, v1.getString(0), v1.getString(1), v1.getString(2),v1.getString(3),v1.getString(4));
            }
        });
        return pRDD;
    }
    
    public JavaRDD<FactTerminalMap> getJavaRDD(DataFrame df) {
        RDD<Row> rdd = df.select("TERMINAL_ID", "USER_ID", "CLIENTTIME","APP_TYPE","APP_ID").rdd();
        JavaRDD<Row> jRDD = rdd.toJavaRDD();
        JavaRDD<FactTerminalMap> pRDD = jRDD.filter(new Function<Row, Boolean>() {

			private static final long serialVersionUID = -7733160922141076082L;

			public Boolean call(Row v1) throws Exception {
				if (v1.getString(1).startsWith("scode:\"") || v1.getString(1).startsWith("wanzhangId:\"")) {
					return false;
				}
				return true;
			}
        	
        }).map(new Function<Row, FactTerminalMap>() {

            private static final long serialVersionUID = 2921123701554148589L;

            public FactTerminalMap call(Row v1) throws Exception {
                String rowKey = "";
                rowKey = IDUtil.getUUID().toString();
                return new FactTerminalMap(rowKey, v1.getString(0), v1.getString(1), v1.getString(2),v1.getString(3),v1.getString(4));
            }
        });
        return pRDD;
    }

    public JavaPairRDD<ImmutableBytesWritable, Put> getPut(JavaRDD<FactTerminalMap> rdd) {

        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = rdd
                .mapToPair(new PairFunction<FactTerminalMap, ImmutableBytesWritable, Put>() {

                    private static final long serialVersionUID = 6768982886655569361L;

                    public Tuple2<ImmutableBytesWritable, Put> call(FactTerminalMap t) throws ExecutionException {
                        FactTerminalMap p = (FactTerminalMap) t;
                        Put put = new Put(BaseTable.getBytes(p.getROWKEY()));
                        put.add(BaseTable.getBytes(BaseTable.FAMILY), BaseTable.getBytes("ROWKEY"),
                                BaseTable.getBytes(p.getROWKEY()));

                        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
                    }
                });
        return hbasePuts;
    }

    public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactTerminalMap> rdd, String path) {
        if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			String tmpPath = path + "-temp";
			Integer repartition_count = 200;
			try {
				repartition_count = Integer.valueOf(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_REPARTITION_COUNT));
			} catch (Exception e) {
				repartition_count = 200;
			}
			
			// Append new data to parquet[path]
			sqlContext.createDataFrame(rdd, FactTerminalMap.class).save(path, "parquet", SaveMode.Append);
			// Load parquet[path], save as parquet[tmpPath]
			TK_DataFormatConvertUtil.deletePath(tmpPath);
			sqlContext.load(path).repartition(repartition_count).saveAsParquetFile(tmpPath);
			// Delete parquet[path]
			TK_DataFormatConvertUtil.deletePath(path);
			// Rename parquet[tmpPath] as parquet[path]
			TK_DataFormatConvertUtil.renamePath(tmpPath, path);
        } else {
            sqlContext.createDataFrame(rdd, FactTerminalMap.class).saveAsParquetFile(path);
        }
    }
}
