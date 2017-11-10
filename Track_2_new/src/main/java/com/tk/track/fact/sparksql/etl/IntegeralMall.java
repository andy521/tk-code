package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.fact.sparksql.desttable.FactIntegeralMall;
import com.tk.track.fact.sparksql.util.DataFrameUtil;


public class IntegeralMall implements Serializable {

	
	private static final long serialVersionUID = -8437963687669525344L;
	
	String productid;
	String productname;
	String classify;
	String tradenum;
	String result;
	
	
	public DataFrame getIntegeralMallDF(HiveContext sqlContext,String appids) {
	    	
		
		sqlContext.createDataFrame(analysisFields(getFStaticEventTmp(sqlContext, appids)), FactIntegeralMall.class).registerTempTable("TMP_FS");
		getUserInfo(sqlContext); 
		getTmpIntegeralMallClue(sqlContext);
		return getIntegeralMallClue(sqlContext);
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
	
	
	private DataFrame getFStaticEventTmp(HiveContext sqlContext, String appids) {
    	String timeStamp = Long.toString(getTodayTime(0) / 1000);
		String yesterdayTimeStamp = Long.toString(getTodayTime(-1) / 1000);
    	String hql = "SELECT  USER_ID AS OPEN_ID,"
    			+ "			  APP_TYPE,"
    			+ "			  APP_ID,"
    			+ "			  EVENT,"
    			+ "			  SUBTYPE,"
    			+ "			  LABEL,"
    			+ "			  VISIT_COUNT,"
    			+ "			  VISIT_TIME,"
    			+ "			  VISIT_DURATION,"
    			+ "			  FROM_ID"
                + " FROM  F_STATISTICS_EVENT " 
                + " WHERE APP_TYPE IN ('webSite', 'app','H5') AND APP_ID IN"
                + " (" + appids +")"
                + " AND USER_ID IS NOT NULL AND USER_ID <> ''"
                + " AND EVENT IS NOT NULL AND EVENT <> '' AND EVENT NOT LIKE 'page%'"
    		    + " AND  from_unixtime(cast(cast(VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') >= from_unixtime(cast(" + yesterdayTimeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')"
    			+ " AND  from_unixtime(cast(cast(VISIT_TIME as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss') < from_unixtime(cast(" + timeStamp + " as bigint),'yyyy-MM-dd HH:mm:ss')";
        
    	return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_SRC");
    }
	
	
	private DataFrame getUserInfo(HiveContext sqlContext) {	
			
			String hql = "SELECT HUSER.OPEN_ID, HUSER.MEMBER_ID, HUSER.NAME, SUBSTR(HUSER.BIRTHDAY,0,10) AS BIRTHDAY, "
					+"	 CASE"
					+"         WHEN HUSER.GENDER = '0' THEN"
					+"          '男'"
					+"         WHEN HUSER.GENDER = '1' THEN"
					+"          '女'"
					+"         ELSE"
					+"          '其他'"
					+"       END AS GENDER"
					+"  FROM FACT_USERINFO HUSER"
					+" INNER JOIN (SELECT OPEN_ID, MAX(USER_ID) USER_ID"
					+"               FROM FACT_USERINFO"
					+"              WHERE OPEN_ID IS NOT NULL AND OPEN_ID <> ''"
					+"              GROUP BY OPEN_ID) MAX_USER ON HUSER.USER_ID = MAX_USER.USER_ID"
					+" WHERE HUSER.OPEN_ID IS NOT NULL AND HUSER.OPEN_ID <> ''";
			
			return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_USERINFO");
	}
	
	
	public DataFrame getTmpIntegeralMallClue(HiveContext sqlContext) {
		
		String hql = "SELECT FS.OPEN_ID,"
				+ "	HUSER.MEMBER_ID AS USER_ID,"
				+ "	'MEM' AS USER_TYPE,"
				+ "	FS.APP_TYPE,"
				+ "	FS.APP_ID,"
				+ "	FS.EVENT,"
				+ "	FS.SUBTYPE AS SUB_TYPE,"
				+ "	FS.VISIT_DURATION,"
				+ "	FS.FROM_ID,"
				+ " FS.PRODUCT_ID,"
				+ " FS.PRODUCT_NAME,"
				+ " FS.CLASSIFY,"
				+ " FS.TRADE_NUM,"
				+ " FS.RESULT,"
				+ " HUSER.NAME AS USER_NAME,"
				+ " HUSER.GENDER,"
				+ " HUSER.BIRTHDAY,"
				+ "	FROM_UNIXTIME(INT(FS.VISIT_TIME / 1000)) AS VISIT_TIME,"
				+ " FS.VISIT_COUNT,"
				+ "	CONCAT('姓名：',"
				+ " 	NVL(NAME,' '),"
				+ " 	'\073性别：',"
				+ " 	NVL(GENDER,' '),"
				+ " 	'\073出生日期：',"
				+ " 	NVL(BIRTHDAY,' '),"
				+ " 	'\073首次访问时间：',"
				+ " 	NVL(FROM_UNIXTIME(INT(FS.VISIT_TIME / 1000)),' '),"
				+ " 	'\073访问次数：',"
				+ " 	NVL(VISIT_COUNT,' '),"
				+ " 	'\073访问时长：',"
				+ " 	NVL(VISIT_DURATION,' '),"
				+ " 	'\073产品ID：',"
				+ " 	NVL(PRODUCT_ID,' '),"
				+ "		'\073产品名称：',"
				+ " 	NVL(PRODUCT_NAME,' '),"
				+ "		'\073产品所属类别：',"
				+ " 	NVL(CLASSIFY,' '),"
				+ "		'\073订单编号：',"
				+ "		NVL(TRADE_NUM,' '),"
				+ "		'\073订单提交结果：',"
				+ "		NVL(RESULT,' ')) AS REMARK"
				+ " FROM TMP_FS FS"
				+ " INNER JOIN TMP_USERINFO HUSER ON FS.OPEN_ID = HUSER.MEMBER_ID";
				
		return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_INTEGERALMALL");
		
	}
	
	public DataFrame getIntegeralMallClue(HiveContext sqlContext) {
		String hql="SELECT "
				+"    '' AS ROWKEY,"
				+"    TEMP_TABLE.USER_ID,"
				+"    MAX(TEMP_TABLE.USER_TYPE) AS USER_TYPE,"
				+"    MAX(TEMP_TABLE.APP_TYPE) AS APP_TYPE,"
				+"    MAX(TEMP_TABLE.APP_ID) AS APP_ID,	"
				+"    '积分商城' AS EVENT_TYPE,"
				+"    MAX(TEMP_TABLE.EVENT) AS EVENT,"
				+"    MAX(TEMP_TABLE.SUB_TYPE) AS SUB_TYPE,"
				+"    MAX(TEMP_TABLE.VISIT_DURATION) AS VISIT_DURATION,"
				+"    MAX(TEMP_TABLE.FROM_ID) AS FROM_ID,"
				+"	'积分商城' AS PAGE_TYPE,"
				+"    '官网会员行为' AS FIRST_LEVEL,"
				+"    '积分商城页面' AS SECOND_LEVEL,"
				+"    MAX(TEMP_TABLE.THIRD_LEVEL) AS THIRD_LEVEL,"
				+"    '积分商城' AS FOURTH_LEVEL,"
				+"    MAX(TEMP_TABLE.VISIT_TIME) AS VISIT_TIME,"
				+"    MAX(TEMP_TABLE.VISIT_COUNT) AS VISIT_COUNT,"
				+"	'2' AS CLUE_TYPE,"
				+"    MAX(TEMP_TABLE.REMARK) AS REMARK,"
				+"    MAX(TEMP_TABLE.USER_NAME) AS USER_NAME "
				+"FROM (SELECT"
				+"		USER_ID,"
				+"		USER_TYPE,"
				+"		APP_TYPE,"
				+"		APP_ID,"
				+"		EVENT,"
				+"		SUB_TYPE,"
				+"		VISIT_DURATION,"
				+"		FROM_ID,"
				+"		CASE "
				+"			WHEN APP_TYPE = 'webSite' THEN"
				+"				'pc积分商城页'"
				+"			WHEN APP_TYPE = 'H5' THEN"
				+"				'WAP积分商城页'"
				+"			WHEN APP_TYPE = 'app' THEN"
				+"				'APP积分商城页'"
				+"			ELSE"
				+"				'' END AS THIRD_LEVEL,"
				+"		VISIT_TIME,"
				+"		VISIT_COUNT,"
				+"		REMARK,"
				+"		USER_NAME"
				+"		FROM TMP_INTEGERALMALL) AS TEMP_TABLE GROUP BY TEMP_TABLE.USER_ID";	
		return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_INTEGERALMALL");
		
	}

	
	public JavaRDD<FactIntegeralMall> analysisFields(DataFrame df) {
		
		JavaRDD<Row> jRdd = df.select("OPEN_ID", "APP_TYPE","APP_ID","EVENT","SUBTYPE","LABEL",
				"VISIT_COUNT","VISIT_TIME","VISIT_DURATION","FROM_ID").rdd().toJavaRDD();
		
		 JavaRDD<FactIntegeralMall> pRDD = jRdd.map(new Function<Row, FactIntegeralMall>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 995138781485940351L;

				public FactIntegeralMall call(Row v1) throws Exception {
					String open_id = v1.getString(0);
					String app_type = v1.getString(1);
					String app_id = v1.getString(2);
					String event = v1.getString(3);
					String subtype = v1.getString(4);
					String label = v1.getString(5);
					String visit_count = v1.getString(6);
					String visit_time = v1.getString(7);
					String visit_duration = v1.getString(8);
					String from_id = v1.getString(9);
					String product_id = "";
					String product_name = "";
					String classify = "";
					String trade_num = "";
					String result = "";
					
					
					if("兑换流程".equals(event) || "团购详情".equals(event) || "商品详情".equals(event) || "产品详情".equals(event) || "秒杀详情".equals(event)){
						if(label.contains("0:{productid")) {
							String[] groupproductid = label.split("\\{");
							for(int i=1;i<=groupproductid.length-1;i++) {
								String x = groupproductid[i].toString();
								
								Pattern p1 = Pattern.compile("(productid:.)(\\d+)(.+productname)(.+)");
								Pattern p2 = Pattern.compile("(productname:.)(.+)(..productclassic)(.+)");
								Pattern p3 = Pattern.compile("(productclassic:\")(.+[\u0391-\uFFE5][\u0391-\uFFE5][\u0391-\uFFE5][\u0391-\uFFE5])(\")");
								Pattern p4 = Pattern.compile("(tradeNum:\")(.+)(\",result)");
								Pattern p5 = Pattern.compile("(result:\")(.+)(\"})");
		
								Matcher m1 = p1.matcher(x);
								Matcher m2 = p2.matcher(x);
								Matcher m3 = p3.matcher(x);
								Matcher m4 = p4.matcher(x);
								Matcher m5 = p5.matcher(x);	
								
								if(m1.find()) {
									productid += m1.group(2) + "   ";
								}
								if(m2.find()) {
									productname += m2.group(2) + "   ";
								}
								if(m3.find()) {
									classify += m3.group(2) + "   ";
								}
								if(m4.find()) {
									tradenum += m4.group(2) + "   ";
								}
								if(m5.find()) {
									result += m5.group(2) + "   ";
								}

							}
							
						} else {
								
								Pattern p1 = Pattern.compile("(productid:.)(\\d+)(.+productname)(.+)");
								Pattern p2 = Pattern.compile("(productname:.)(.+)(..+productclassic)(.+)");
								Pattern p3 = Pattern.compile("(productclassic:\")(.+[\u0391-\uFFE5][\u0391-\uFFE5][\u0391-\uFFE5][\u0391-\uFFE5])(\")");
								Pattern p4 = Pattern.compile("(tradeNum:\")(.+)(\",result)");
								Pattern p5 = Pattern.compile("(result:\")(.+)(\")");
		
								Matcher m1 = p1.matcher(label);
								Matcher m2 = p2.matcher(label);
								Matcher m3 = p3.matcher(label);
								Matcher m4 = p4.matcher(label);
								Matcher m5 = p5.matcher(label);
								
								
								if(m1.find()) {
									productid = m1.group(2);
								}
								if(m2.find()) {
									productname = m2.group(2);
								}
								if(m3.find()) {
									classify = m3.group(2);
								}
								if(m4.find()) {
									tradenum = m4.group(2);
								}
								if(m5.find()) {
									result = m5.group(2);
								}
							}

					
					return new FactIntegeralMall(open_id,app_type,app_id,event,subtype,visit_count,visit_time,visit_duration,
							from_id,productid,productname,classify,tradenum,result);
					}
					return new FactIntegeralMall(open_id,app_type,app_id,event,subtype,visit_count,visit_time,visit_duration,
							from_id,product_id,product_name,classify,trade_num,result);
					
				
			}
			
		});

		return pRDD;

	}

}
