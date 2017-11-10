package com.tk.track.fact.sparksql.etl;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.apache.commons.lang.StringUtils;
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
public class UserBehaviorTeleApp implements Serializable {
	
	private static final long serialVersionUID = 1498019506393921487L;
	String sca_nw = TK_DataFormatConvertUtil.getNetWorkSchema();
    String sca_ol = TK_DataFormatConvertUtil.getSchema();

    public DataFrame getUserBehaviorTeleDF(HiveContext sqlContext) {
    	
    	getPropertySplanRiskRel(sqlContext);
    	sqlContext.createDataFrame(analysisFields(getFStaticEventTmp(sqlContext)), TempUserBehaviorTeleWap.class).registerTempTable("Tmp_FS");
		getProInfoTmp(sqlContext);
        getVisitInfo(sqlContext);
        getFactUserinfo(sqlContext);
        getUserBehaviorTeleApp(sqlContext);
        return getUserBehaviorClue(sqlContext);
    }
    
    

    /**
     * 获取财产险销售方案对应的险种编码映射关系表
     * @Description: 
     * @return
     * @author itw_qiuzq01
     * @date 2016年11月11日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    private DataFrame getPropertySplanRiskRel(HiveContext sqlContext) {
    	String hql =  "SELECT T.SPLANCODE, T.RISKCODE, GR.RISKCNAME"
    			+"  FROM (SELECT T.SPLANCODE, MIN(T.RISKCODE) RISKCODE"
    			+"          FROM "+sca_ol+"GSCHANNELPLANCODE T"
    			+"         GROUP BY T.SPLANCODE) T"
    			+" INNER JOIN "+sca_ol+"GGRISK GR"
    			+"    ON T.RISKCODE = GR.RISKCODE";
    	return DataFrameUtil.getDataFrame(sqlContext, hql, "tmp_PropertySplanRiskRel");
	}
    

	private DataFrame getFStaticEventTmp(HiveContext sqlContext) {
    	String timeStamp = Long.toString(getTodayTime(0) / 1000);
		String yesterdayTimeStamp = Long.toString(getTodayTime(-1) / 1000);
    	String hql = "SELECT  gendecode(USER_ID,'UTF-8') AS USER_ID,"
    			+ "			  APP_TYPE,"
    			+ "			  APP_ID,"
    			+ "			  EVENT,"
    			+ "			  SUBTYPE,"
    			+ "			  LABEL,"
    			+ "			  VISIT_COUNT,"
    			+ "			  VISIT_TIME,"
    			+ "			  VISIT_DURATION,"
    			+ "			  FROM_ID"
                + " FROM  F_STATISTICS_EVENT TB1 " 
                + " WHERE LOWER(TB1.APP_TYPE)='app' and LOWER(TB1.APP_ID)='app001'"
                + "	AND  LOWER(EVENT) IN ('商品详情','detail','mall')"
    		    + " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast(" + yesterdayTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')"
    			+ " AND  from_unixtime(cast(cast(TB1.VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast(" + timeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_src");
    }
    
	

	/**
     * 
     * @Description: 字段解析
     * @param df
     * @return
     * @author moyunqing
     * @date 2016年10月17日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    private JavaRDD<TempUserBehaviorTeleWap> analysisFields(DataFrame df) {
    	JavaRDD<Row> jRDD = df.select("USER_ID", "APP_TYPE", "APP_ID", 
    			"EVENT", "SUBTYPE", "LABEL","VISIT_COUNT", "VISIT_TIME", "VISIT_DURATION","FROM_ID").rdd().toJavaRDD();
    	JavaRDD<TempUserBehaviorTeleWap> pRDD = jRDD.map(new Function<Row, TempUserBehaviorTeleWap>() {
			
    		private static final long serialVersionUID = -6173947550334216513L;

			public TempUserBehaviorTeleWap call(Row v1) throws Exception {
				TempUserBehaviorTeleWap tmp = new TempUserBehaviorTeleWap();
				tmp.setUSER_ID(v1.getString(0));
				tmp.setAPP_TYPE(v1.getString(1));
				tmp.setAPP_ID(v1.getString(2));
				String event = v1.getString(3);
				tmp.setUSER_EVENT(event);
				tmp.setUSER_SUBTYPE(v1.getString(4));
				String label = v1.getString(5);
				tmp.setVISIT_COUNT(v1.getString(6));
				tmp.setVISIT_TIME(v1.getString(7));
				tmp.setVISIT_DURATION(v1.getString(8));
				tmp.setFROM_ID(v1.getString(9));
				tmp.setUSER_TYPE("MEM");
				
				if("商品详情".equals(event)){
					StringParse parse = new StringParse();
					parse.appendSpecialValueRegex("x22|-22|X22");
					parse.parse2Map(label);//解析label
					tmp.setCLASSIFYNAME(parse.getIgnoreCase("classifyName"));
					tmp.setLRT_ID(parse.getIgnoreCase("lrtID"));
				    tmp.setLRT_NAME(parse.getIgnoreCase("lrtName"));
					tmp.setTHIRD_LEVEL(parse.getIgnoreCase("lrtType"));
					tmp.setFOURTH_LEVEL(parse.getIgnoreCase("lrtID"));
				}else if("detail".equalsIgnoreCase(event)){
					//app详情页：三级分类长短理财；四级分类：险种编码
					//在后续的SQL中填充
				}else if("mall".equals(event)){
					tmp.setTHIRD_LEVEL(tmp.getUSER_EVENT());
					tmp.setFOURTH_LEVEL(tmp.getUSER_SUBTYPE());
				}
				return tmp;
			}
    	});
    	return pRDD;
	}
    
   
    private DataFrame getUserBehaviorClue(HiveContext sqlContext) {
		String sql = "	SELECT  DISTINCT "
				+ "				'' AS ROWKEY,"
				+ "				TELE.FROM_ID, "
        		+ "             TELE.USER_ID, "
        		+ "             TELE.USER_TYPE, "
        		+ "             TELE.USER_EVENT, "
        		+ "             TELE.THIRD_LEVEL, "
        		+ "             TELE.FOURTH_LEVEL, "
        		+ "             TELE.LRT_ID, "
        		+ "             TELE.CLASS_ID, "
        		+ "             TELE.INFOFROM, "
        		+ "             TELE.VISIT_COUNT, "
        		+ "             TELE.VISIT_TIME, "
        		+ "             TELE.VISIT_DURATION, "
        		+ "             TELE.CLASSIFY_NAME, "
        		+ "             TELE.PRODUCT_NAME, "
        		+ "             TELE.USER_NAME,"
				+ "           	CASE "
				+ "               	WHEN PL.LRT_ID IS NOT NULL AND PL.LRT_ID <> '' THEN '0'"
				+ "               	WHEN TELE.LRT_ID <> '' AND TELE.LRT_ID IS NOT NULL THEN '1'"
				+ "				  	ELSE ''"
				+ "           	END AS IF_PAY,"
				+ "			  '' AS REMARK,"
				+ "			  '' AS CLUE_TYPE"
				+ "		FROM  TMP_TELE TELE"
				+ "	LEFT JOIN (SELECT POLICYHOLDER_ID,"
				+ "					  LRT_ID"
				+ "				FROM  FACT_PolicyInfo"
				+ "			   GROUP BY POLICYHOLDER_ID,"
				+ "						LRT_ID) PL"
				+ "	   ON     TELE.CUSTOMER_ID = PL.POLICYHOLDER_ID "
				+ "		  AND TELE.LRT_ID = PL.LRT_ID";
    	return DataFrameUtil.getDataFrame(sqlContext, sql, "FACT_USER_BEHAVIOR_TELE");
	}





    /**
     * 对app详情页财险产品补充三四级分类和险种信息
     * @Description: 
     * @param sqlContext
     * @return
     * @author itw_qiuzq01
     * @date 2016年11月11日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     * 三级分类长短理财；四级分类：险种编码
     * T.SPLANCODE, T.RISKCODE, GR.RISKCNAME,T.SPLANCODE, MIN(T.RISKCODE) RISKCODE,GGRISK GR
     */
    private DataFrame getProInfoTmp(HiveContext sqlContext) {
        String hql = "SELECT FS.FROM_ID,"
        		+"       FS.APP_TYPE,"
        		+"       FS.APP_ID,"
        		+"       FS.USER_ID,"
        		+"       FS.USER_TYPE,"
        		+"       FS.USER_EVENT,"
        		+"       COALESCE(CASE WHEN FS.USER_EVENT = 'detail' AND FS.USER_SUBTYPE LIKE 'S%' THEN '短险' ELSE  FS.THIRD_LEVEL END, '')  THIRD_LEVEL,"
        		+"       COALESCE(CASE WHEN FS.USER_EVENT = 'detail' AND FS.USER_SUBTYPE LIKE 'S%' THEN REL.RISKCODE  ELSE FS.FOURTH_LEVEL END, '') FOURTH_LEVEL,"
        		+"       COALESCE(CASE WHEN FS.USER_EVENT = 'detail' AND FS.USER_SUBTYPE LIKE 'S%' THEN REL.RISKCODE ELSE FS.LRT_ID END, '') LRT_ID,"
        		+"       FS.CLASS_ID,"
        		+"       FS.APP_ID AS INFOFROM,"
        		+"       FS.VISIT_COUNT,"
        		+"       FS.VISIT_TIME,"
        		+"       FS.VISIT_DURATION,"
        		+"       FS.CLASSIFYNAME AS CLASSIFY_NAME,"
        		+"       COALESCE(CASE WHEN FS.USER_EVENT = 'detail' AND FS.USER_SUBTYPE LIKE 'S%' THEN REL.RISKCNAME ELSE FS.LRT_NAME END, '') PRODUCT_NAME"
        		+"  FROM TMP_FS FS"
        		+"  LEFT JOIN TMP_PROPERTYSPLANRISKREL REL ON SUBSTR(FS.USER_SUBTYPE,1,9) = REL.SPLANCODE ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_ProInfo");
    }    
    
    
    private DataFrame getVisitInfo(HiveContext sqlContext) {
        String hql = "SELECT    "
        		+ "				FROM_ID,"
        		+ "			    APP_TYPE,"
        		+ "				APP_ID,"
        		+ "             USER_ID,"
        		+ "			    USER_TYPE,"
        		+ "				USER_EVENT,"
        		+ "				THIRD_LEVEL,"
        		+ "			    FOURTH_LEVEL,"
        		+ "			    LRT_ID, "
                + "             CLASS_ID, "
                + "			    INFOFROM,"
                + "             sum(VISIT_COUNT)    as VISIT_COUNT, "
                + "             from_unixtime(cast(cast(min(VISIT_TIME) as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') AS VISIT_TIME, "
                + "             sum(VISIT_DURATION) as VISIT_DURATION, "
                + "             min(CLASSIFY_NAME)  as CLASSIFY_NAME,"
                + "             min(PRODUCT_NAME)   as PRODUCT_NAME "
                + "   FROM      Tmp_ProInfo"
                + "   GROUP BY  FROM_ID,"
                + "				APP_TYPE,"
                + "				APP_ID,"
                + "             USER_ID,"
                + "				USER_TYPE,"
                + "				USER_EVENT,"
                + "				THIRD_LEVEL,"
                + "			    FOURTH_LEVEL, "
                + "			    LRT_ID, "
                + "             CLASS_ID,"
                + "			    INFOFROM ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_VisitInfo");
    }
    
    private DataFrame getFactUserinfo(HiveContext sqlContext) {
        String hql = "SELECT DISTINCT NAME, "
        		+ "           CUSTOMER_ID,"
        		+ "           MEMBER_ID "
        		+ "   FROM    FACT_USERINFO"
        		+ "   WHERE   CUSTOMER_ID <> '' "
        		+ "   AND     CUSTOMER_ID IS NOT NULL "
        		+ "   AND     MEMBER_ID <>'' "
        		+ "   AND     MEMBER_ID IS NOT NULL"; 
        return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_UserInfo");
    }

    
    private DataFrame getUserBehaviorTeleApp(HiveContext sqlContext) {
        String hql = "SELECT    DISTINCT "
        		+ "				T1.FROM_ID, "
        		+ "             T1.USER_ID, "
        		+ "             T1.USER_TYPE, "
        		+ "             T1.USER_EVENT, "
        		+ "             T1.THIRD_LEVEL, "
        		+ "             T1.FOURTH_LEVEL, "
        		+ "             T1.LRT_ID, "
        		+ "             T1.CLASS_ID, "
        		+ "             T1.INFOFROM, "
        		+ "             CAST(T1.VISIT_COUNT as STRING) as VISIT_COUNT, "
        		+ "             CAST(T1.VISIT_TIME as STRING) as VISIT_TIME, "
        		+ "             CAST(T1.VISIT_DURATION as STRING) as VISIT_DURATION, "
        		+ "             T1.CLASSIFY_NAME, "
        		+ "             T1.PRODUCT_NAME, "
        		+ "             T2.NAME AS USER_NAME,"
        		+ "				T2.CUSTOMER_ID"
        		+ "   FROM      Tmp_VisitInfo T1 "
        		+ "   LEFT JOIN Tmp_UserInfo T2 "
        		+ "   ON        T1.USER_TYPE = 'MEM' "
        		+ "   AND       T1.USER_ID = T2.MEMBER_ID ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_tele");
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


    public JavaRDD<FactUserBehaviorTele> getJavaRDD(DataFrame df) {
    	JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID", "USER_TYPE", "USER_EVENT", "THIRD_LEVEL", 
    			"FOURTH_LEVEL", "LRT_ID", "CLASS_ID", "INFOFROM", "VISIT_COUNT", "VISIT_TIME", 
    			"VISIT_DURATION", "CLASSIFY_NAME", "PRODUCT_NAME", "USER_NAME", "IF_PAY","FROM_ID","REMARK","CLUE_TYPE").rdd().toJavaRDD();
        JavaRDD<FactUserBehaviorTele> pRDD = jRDD.filter(new Function<Row, Boolean>() {

			private static final long serialVersionUID = 4414424743032730478L;

			public Boolean call(Row v1) throws Exception {
				if(StringUtils.isBlank(v1.getString(4))  || StringUtils.isBlank(v1.getString(5))){
					return false;
				}
				return true;
			}
        	
        }).map(new Function<Row, FactUserBehaviorTele>() {

			private static final long serialVersionUID = 5574755113903641L;

			public FactUserBehaviorTele call(Row v1) throws Exception {
                return new FactUserBehaviorTele(v1.getString(0), v1.getString(1), v1.getString(2), 
                        v1.getString(3), v1.getString(4), v1.getString(5), 
                        v1.getString(6), v1.getString(7), v1.getString(8), 
                        v1.getString(9), v1.getString(10), v1.getString(11), 
                        v1.getString(12), v1.getString(13), v1.getString(14), 
                        v1.getString(15),v1.getString(16),v1.getString(17),v1.getString(18));
            }
        });
        return pRDD;
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
