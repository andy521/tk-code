package com.tk.track.fact.sparksql.etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.DBUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;
import com.tk.track.vo.UserBehaviorConfigVo;
import com.tk.track.vo.UserBehaviorDetailConfig;
import com.tk.track.vo.UserBehaviorSpecialDataConfigVo;

public class UserBehaviorClueCommon {
	
    private static final String CONFIG_PATH = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TRACK_CONFIG_PATH_ROOT) + "/appid-type.properties";
    private static final String SCHEMA_UB = TK_DataFormatConvertUtil.getUserBehaviorSchema();
	public DataFrame getUserBehaviorClueDF(HiveContext sqlContext,
			UserBehaviorConfigVo userBehaviorConfigVo) {
		String appId = userBehaviorConfigVo.getAppId();
		if(appId == null || appId.isEmpty()) {
			return null;
		}
        getSrcTempData(sqlContext, userBehaviorConfigVo);
		return getDataWithClueLevel(sqlContext, userBehaviorConfigVo);
	}

	/**
     * 查出符合条件的用户行为
     *
     * @param sqlContext
     * @param appids
     * @return
     */
    public DataFrame getSrcTempData(HiveContext sqlContext, UserBehaviorConfigVo userBehaviorConfigVo) {
    	long todayTime = getTodayTime(-1);
    	String format = new SimpleDateFormat("yyyy-MM-dd").format(todayTime);
    	String appId = userBehaviorConfigVo.getAppId();
    	String appType = userBehaviorConfigVo.getAppType();
    	List<UserBehaviorDetailConfig> userBehaviorDetailConfigList = userBehaviorConfigVo.getUserBehaviorDetailConfigList();
    	String params = "";
    	List<String> paramList = new ArrayList<String>();
    	for(UserBehaviorDetailConfig userBehaviorDetailConfig : userBehaviorDetailConfigList) {
    		String event = userBehaviorDetailConfig.getEvent().trim();
    		String subType = userBehaviorDetailConfig.getSubtype();
    		boolean subTypeNotEmpty = checkNotEmpty(subType);
    		String param = "EVENT='"+event+"'";
    		if(subTypeNotEmpty) {
    			subType = subType.trim();
    			if(subType.indexOf("#{") != -1) {
    				param = param + " AND SUBTYPE IS NOT NULL AND SUBTYPE <> ''";
    			} else {
    				param = param + " AND SUBTYPE='"+subType+"'";
    			}
//    			if(userType == null || userType.isEmpty()) {
//    				userType = "MEM";
//    			}
//    			param = param + " AND USER_TYPE='"+userType+"'";
    		}
    		paramList.add(param);
    	}
    	for(String param : paramList) {
    		if(params.isEmpty()) {
    			params = " ("+param+") ";
    		} else {
    			params = params + " OR " + " ("+param+") ";
    		}
    	}
    	String hql = "SELECT * " + "FROM F_STATISTIC_EVENT TB1 "
				   + " WHERE TRIM(TB1.APP_ID)='" + appId + "'"
				   + " AND TB1.APP_TYPE = '"+appType+"'"
				   + " AND ("+params+")";
    	
    	hql += " AND FROM_UNIXTIME(CAST(CAST(VISIT_TIME AS BIGINT) / 1000 AS BIGINT), 'yyyy-MM-dd') ='" + format + "'";
    	DataFrame TMP_FILTER_COLLECTION = DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_FILTER_COLLECTION");
    	System.out.println("==================TMP_FILTER_COLLECTION=============="+TMP_FILTER_COLLECTION.count());
    	return TMP_FILTER_COLLECTION;
    }

    private boolean checkNotEmpty(String str) {
		return str != null && !str.isEmpty();
	}

    /**
     * 存入clue中
     *
     * @param sqlContext
     * @param userBehaviorConfigVo 
     * @return
     */
    public DataFrame getDataWithClueLevel(HiveContext sqlContext, UserBehaviorConfigVo userBehaviorConfigVo) {
        List<UserBehaviorDetailConfig> userBehaviorDetailConfigs = userBehaviorConfigVo.getUserBehaviorDetailConfigList();
        List<String> firstLevelSqls = new ArrayList<String>();
        List<String> secondLevelSqls = new ArrayList<String>();
        List<String> thirdLevelSqls = new ArrayList<String>();
        List<String> fourthLevelSqls = new ArrayList<String>();
        List<String> userIdSqls = new ArrayList<String>();
        List<String> userTypeSqls = new ArrayList<String>();
        for(UserBehaviorDetailConfig userBehaviorDetailConfig : userBehaviorDetailConfigs) {
        	List<UserBehaviorSpecialDataConfigVo> specialConfigList = userBehaviorDetailConfig.getSpecialConfigList();
        	String subtype = userBehaviorDetailConfig.getSubtype();
        	String sql = " WHEN EVENT ='"+userBehaviorDetailConfig.getEvent()+"'";
        	if(checkNotEmpty(subtype)) {
        		if(subtype.indexOf("#{") != -1) {
        			sql = sql + " AND SUBTYPE IS NOT NULL AND SUBTYPE <> ''";
    			} else {
    				sql = sql + " AND SUBTYPE='"+subtype+"'";
    			}
        	}
        	List<UserBehaviorSpecialDataConfigVo> firstLevelSpecialDataConfigVos = new ArrayList<UserBehaviorSpecialDataConfigVo>();
        	List<UserBehaviorSpecialDataConfigVo> secondLevelSpecialDataConfigVos = new ArrayList<UserBehaviorSpecialDataConfigVo>();
        	List<UserBehaviorSpecialDataConfigVo> thirdLevelSpecialDataConfigVos = new ArrayList<UserBehaviorSpecialDataConfigVo>();
        	List<UserBehaviorSpecialDataConfigVo> fourthLevelSpecialDataConfigVos = new ArrayList<UserBehaviorSpecialDataConfigVo>();
        	for(UserBehaviorSpecialDataConfigVo userBehaviorSpecialDataConfigVo : specialConfigList) {
    			if(userBehaviorSpecialDataConfigVo.getClueLevel().equals("1")) {
    				firstLevelSpecialDataConfigVos.add(userBehaviorSpecialDataConfigVo);
    			}
    			if(userBehaviorSpecialDataConfigVo.getClueLevel().equals("2")) {
    				secondLevelSpecialDataConfigVos.add(userBehaviorSpecialDataConfigVo);
    			}
    			if(userBehaviorSpecialDataConfigVo.getClueLevel().equals("3")) {
    				thirdLevelSpecialDataConfigVos.add(userBehaviorSpecialDataConfigVo);
    			}
    			if(userBehaviorSpecialDataConfigVo.getClueLevel().equals("4")) {
    				fourthLevelSpecialDataConfigVos.add(userBehaviorSpecialDataConfigVo);
    			}
        	}
        	String firstLevel = userBehaviorDetailConfig.getFirstLevel();
        	String secondLevel = userBehaviorDetailConfig.getSecondLevel();
        	String thirdLevel = userBehaviorDetailConfig.getThirdLevel();
        	String fourthLevel = userBehaviorDetailConfig.getFourthLevel();
        	String userType = userBehaviorDetailConfig.getUserType();
        	String firstLevelsql = this.getHqlParamsByConfig(sql, firstLevel, userBehaviorDetailConfig, firstLevelSpecialDataConfigVos);
        	String secondLevelsql = this.getHqlParamsByConfig(sql, secondLevel, userBehaviorDetailConfig, secondLevelSpecialDataConfigVos);
        	String thirdLevelsql = this.getHqlParamsByConfig(sql, thirdLevel, userBehaviorDetailConfig, thirdLevelSpecialDataConfigVos);
        	String fourthLevelsql = this.getHqlParamsByConfig(sql, fourthLevel, userBehaviorDetailConfig, fourthLevelSpecialDataConfigVos);
        	String userIdSql = "";
        	String userTypeSql = "";
        	if(userType.equals("MEM")) {
        		userIdSql = sql + " THEN T5.USER_ID";
        		userTypeSql = sql + " THEN 'MEM'";
        	} else if(userType.equals("WE")){
        		userIdSql = sql + " AND UM.MEMBER_ID IS NOT NULL AND TRIM(UM.MEMBER_ID) <> '' THEN UM.MEMBER_ID ";
        		userIdSql = userIdSql + sql + " AND (UM.MEMBER_ID IS NULL OR TRIM(UM.MEMBER_ID)='') AND UM.CUSTOMER_ID IS NOT NULL AND TRIM(UM.CUSTOMER_ID) <> '' THEN UM.CUSTOMER_ID ";
        		userIdSql = userIdSql + sql + " AND (UM.MEMBER_ID IS NULL OR TRIM(UM.MEMBER_ID)='') AND (UM.CUSTOMER_ID IS NULL OR TRIM(UM.CUSTOMER_ID)='') THEN T5.USER_ID ";
        		userTypeSql = sql + " AND UM.MEMBER_ID IS NOT NULL AND UM.MEMBER_ID <> '' THEN 'WE-MEM'";
        		userTypeSql = userTypeSql + sql + " AND (UM.MEMBER_ID IS NULL OR TRIM(UM.MEMBER_ID)='') AND UM.CUSTOMER_ID IS NOT NULL AND TRIM(UM.CUSTOMER_ID) <> '' THEN 'WE-C'";
        		userTypeSql = userTypeSql + sql + " AND (UM.MEMBER_ID IS NULL OR TRIM(UM.MEMBER_ID)='') AND (UM.CUSTOMER_ID IS NULL OR TRIM(UM.CUSTOMER_ID)='') THEN 'WE'";
        	}
        	firstLevelSqls.add(firstLevelsql);
        	secondLevelSqls.add(secondLevelsql);
        	thirdLevelSqls.add(thirdLevelsql);
        	fourthLevelSqls.add(fourthLevelsql);
        	userIdSqls.add(userIdSql);
        	userTypeSqls.add(userTypeSql);
        }
        String firstLevelParams = this.getLevelSqlParamsByConditions(firstLevelSqls)+" END AS FIRST_LEVEL,";
        String secondLevelParams = this.getLevelSqlParamsByConditions(secondLevelSqls)+" END AS SECOND_LEVEL,";
        String thirdLevelParams = this.getLevelSqlParamsByConditions(thirdLevelSqls)+" END AS THIRD_LEVEL,";
        String fourthLevelParams = this.getLevelSqlParamsByConditions(fourthLevelSqls)+" END AS FOURTH_LEVEL,";
        String userIdParams = this.getLevelSqlParamsByConditions(userIdSqls)+" END AS USER_ID,";
        String userTypeParams = this.getLevelSqlParamsByConditions(userTypeSqls)+" END AS USER_TYPE,";
    	String filterUserInfoHql = "SELECT NAME,GENDER,BIRTHDAY,MEMBER_ID,OPEN_ID,CUSTOMER_ID "
                + "					FROM FACT_USERINFO WHERE MEMBER_ID IS NOT NULL AND MEMBER_ID <> '' AND OPEN_ID IS NOT NULL AND TRIM(OPEN_ID) <> ''";
    	String filterUserInfoHql2 = "SELECT NAME,GENDER,BIRTHDAY,MEMBER_ID,OPEN_ID,CUSTOMER_ID "
    			+ "					FROM FACT_USERINFO WHERE CUSTOMER_ID IS NOT NULL AND CUSTOMER_ID <> '' AND OPEN_ID IS NOT NULL AND TRIM(OPEN_ID) <> ''";
//    	String filterUserInfoHql = "SELECT NAME,GENDER,BIRTHDAY,MEMBER_ID,OPEN_ID,CUSTOMER_ID "
//    			+ "					FROM FACT_USERINFO WHERE ((MEMBER_ID IS NOT NULL AND MEMBER_ID <> '') OR (CUSTOMER_ID IS NOT NULL AND CUSTOMER_ID <> '')) AND ((OPEN_ID IS NOT NULL AND TRIM(OPEN_ID) <> '') OR (MEMBER_ID IS NOT NULL AND MEMBER_ID <> ''))";
    	DataFrameUtil.getDataFrame(sqlContext, filterUserInfoHql, "FILTER_FACT_USER_INFO");
    	DataFrameUtil.getDataFrame(sqlContext, filterUserInfoHql2, "FILTER_FACT_USER_INFO2");
    	String hql = "SELECT /*+MAPJOIN(T5)*/"
    			+ "       T5.ROWKEY,"
                + userIdParams
                + userTypeParams
                + "       T5.APP_TYPE,  "
                + "       T5.APP_ID,    "
                + "       'load' AS EVENT_TYPE,  "
                + "       T5.EVENT,     "
                + "       T5.SUBTYPE AS SUB_TYPE,"
                + "       '查询' AS PAGE_TYPE,   "
                + firstLevelParams
                + secondLevelParams
                + thirdLevelParams
                + fourthLevelParams
                + "       UM.NAME, "
                + "       T5.VISIT_COUNT,        "
                + "       FROM_UNIXTIME(INT(T5.VISIT_TIME / 1000)) AS VISIT_TIME,      "
                + "       T5.VISIT_DURATION,     "
                + "       T5.FROM_ID,   "
                + "       CONCAT('姓名：',       "
                + "              NVL(UM.NAME, ' '),  "
                + "              '\073性别：',   "
                + "              NVL(UM.GENDER, ' '),     "
                + "              '\073出生日期：',        "
                + "              NVL(UM.BIRTHDAY, ' '),   "
                + "              '\073首次访问时间：',    "
                + "              NVL(FROM_UNIXTIME(INT(T5.VISIT_TIME / 1000)), ' '),"
                + "              '\073访问次数：',        "
                + "              NVL(T5.VISIT_COUNT, ' '),"
                + "              '\073访问时长：',        "
                + "              NVL(T5.VISIT_DURATION, ' ')) AS REMARK,"
                + "       '2' AS CLUE_TYPE       "
                + "  FROM TMP_FILTER_COLLECTION T5"
                + "  LEFT JOIN (SELECT NAME,GENDER,BIRTHDAY,MEMBER_ID,OPEN_ID,CUSTOMER_ID "
                + "					FROM FILTER_FACT_USER_INFO) UM"
                + "       ON (T5.USER_ID = UM.OPEN_ID OR T5.USER_ID=UM.MEMBER_ID)";
    	DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_USER_BEHAVIOR_ClUE_TEMP");
    	String hql2 = "SELECT "
    			+ "       A.ROWKEY,"
    			+ "       A.USER_ID,"
    			+ "       A.USER_TYPE,"
    			+ "       A.APP_TYPE,  "
    			+ "       A.APP_ID,    "
    			+ "       A.EVENT_TYPE,  "
    			+ "       A.EVENT,     "
    			+ "       A.SUB_TYPE,"
    			+ "       A.PAGE_TYPE,   "
    			+ "       A.FIRST_LEVEL,"
    			+ "       A.SECOND_LEVEL,"
    			+ "       A.THIRD_LEVEL,"
    			+ "       A.FOURTH_LEVEL,"
    			+ "       A.NAME, "
    			+ "       A.VISIT_COUNT,        "
    			+ "       A.VISIT_TIME,      "
    			+ "       A.VISIT_DURATION,     "
    			+ "       A.FROM_ID,   "
    			+ "       A.REMARK,"
    			+ "       A.CLUE_TYPE       "
    			+ "  FROM FACT_USER_BEHAVIOR_ClUE_TEMP A WHERE A.USER_TYPE <> 'WE'"
    			+ "       UNION ALL"
    			+ "  SELECT "
    			+ "       A.ROWKEY,"
    			+ "       CASE WHEN UM.CUSTOMER_ID IS NOT NULL AND UM.CUSTOMER_ID <> '' THEN UM.CUSTOMER_ID"
    			+ "       ELSE A.USER_ID END AS USER_ID,"
    			+ "       CASE WHEN UM.CUSTOMER_ID IS NOT NULL AND UM.CUSTOMER_ID <> '' THEN 'WE-C'"
    			+ "       ELSE 'WE' END AS USER_TYPE,"
    			+ "       A.APP_TYPE,  "
    			+ "       A.APP_ID,    "
    			+ "       A.EVENT_TYPE,  "
    			+ "       A.EVENT,     "
    			+ "       A.SUB_TYPE,"
    			+ "       A.PAGE_TYPE,   "
    			+ "       A.FIRST_LEVEL,"
    			+ "       A.SECOND_LEVEL,"
    			+ "       A.THIRD_LEVEL,"
    			+ "       A.FOURTH_LEVEL,"
    			+ "       UM.NAME, "
    			+ "       A.VISIT_COUNT,        "
    			+ "       A.VISIT_TIME,      "
    			+ "       A.VISIT_DURATION,     "
    			+ "       A.FROM_ID,   "
    			+ "       CONCAT('姓名：',       "
                + "              NVL(UM.NAME, ' '),  "
                + "              '\073性别：',   "
                + "              NVL(UM.GENDER, ' '),     "
                + "              '\073出生日期：',        "
                + "              NVL(UM.BIRTHDAY, ' '),   "
                + "              '\073首次访问时间：',    "
                + "              NVL(FROM_UNIXTIME(INT(A.VISIT_TIME / 1000)), ' '),"
                + "              '\073访问次数：',        "
                + "              NVL(A.VISIT_COUNT, ' '),"
                + "              '\073访问时长：',        "
                + "              NVL(A.VISIT_DURATION, ' ')) AS REMARK,"
    			+ "       A.CLUE_TYPE       "
    			+ "  FROM FACT_USER_BEHAVIOR_ClUE_TEMP A"
    			+ "  LEFT JOIN (SELECT NAME,GENDER,BIRTHDAY,MEMBER_ID,OPEN_ID,CUSTOMER_ID "
    			+ "					FROM FILTER_FACT_USER_INFO2) UM"
    			+ "       ON A.USER_ID = UM.OPEN_ID"
    			+ "  WHERE A.USER_TYPE = 'WE'";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_USER_BEHAVIOR_ClUE").distinct();
    }

	private String getLevelSqlParamsByConditions(List<String> levelSqls) {
		String sql = " CASE ";
		for(String levelSql : levelSqls) {
			sql = sql + levelSql +" ";
		}
		return sql;
	}

	private String getHqlParamsByConfig(String sql, String clueLevel, UserBehaviorDetailConfig userBehaviorDetailConfig, List<UserBehaviorSpecialDataConfigVo> specialDataConfigVos) {
    	String result = "";
    	if(clueLevel.indexOf("#{") != -1) {
    		if(specialDataConfigVos.size() > 0) {
    			String notInParams = "";
    			if(clueLevel.indexOf("#{subtype}") != -1) {
    				for(int i=0; i<specialDataConfigVos.size(); i++) {
    					UserBehaviorSpecialDataConfigVo specialDataConfigVo =specialDataConfigVos.get(i);
    					String originalValue = specialDataConfigVo.getOriginalValue();
    					originalValue = originalValue.replaceAll("#", "'");
    					if(i==0) {
    						if(originalValue.indexOf("','") != -1) {
    							notInParams = originalValue;
    							result = sql + " AND SUBTYPE in ("+originalValue+") THEN '"+specialDataConfigVo.getConvertValue()+"' ";
    						} else {
    							notInParams = "'"+originalValue+"'";
    							result = sql + " AND SUBTYPE='"+originalValue+"' THEN '"+specialDataConfigVo.getConvertValue()+"' ";
    						}
    					} else {
    						if(originalValue.indexOf("','") != -1) {
    							notInParams = notInParams + ","+originalValue;
    							result = result + sql + " AND SUBTYPE in ("+originalValue+") THEN '"+specialDataConfigVo.getConvertValue()+"' ";
    						} else {
    							notInParams = notInParams + ",'"+originalValue+"'";
    							result = result + sql + " AND SUBTYPE='"+originalValue+"' THEN '"+specialDataConfigVo.getConvertValue()+"' ";
    						}
    					}
    				}
    				if(clueLevel.startsWith("#") && clueLevel.endsWith("}")) {
    					result = result + sql + " AND SUBTYPE NOT IN ("+notInParams+") THEN SUBTYPE";
    				} else if(clueLevel.startsWith("#") && !clueLevel.endsWith("}")) {
    					String str = clueLevel.substring(clueLevel.indexOf("}")+1);
    					result = result + sql + " AND SUBTYPE NOT IN ("+notInParams+") THEN CONCAT(SUBTYPE, '"+str+"')";
    				} else if(!clueLevel.startsWith("#")&& clueLevel.endsWith("}")) {
    					String str = clueLevel.substring(0, clueLevel.indexOf("#"));
    					result = result + sql + " AND SUBTYPE NOT IN ("+notInParams+") THEN CONCAT('"+str+"', SUBTYPE)";
    				} else if(!clueLevel.startsWith("#")&& !clueLevel.endsWith("}")) {
    					String str1 = clueLevel.substring(0, clueLevel.indexOf("#"));
    					String str2 = clueLevel.substring(clueLevel.indexOf("}")+1);
    					result = result + sql + " AND SUBTYPE NOT IN ("+notInParams+") THEN CONCAT('"+str1+"', SUBTYPE, '"+str2+"')";
    				}
    			} else if(clueLevel.indexOf("#{event}") != -1) {
    				for(int i=0; i<specialDataConfigVos.size(); i++) {
    					UserBehaviorSpecialDataConfigVo specialDataConfigVo =specialDataConfigVos.get(i);
    					String originalValue = specialDataConfigVo.getOriginalValue();
    					originalValue = originalValue.replaceAll("#", "'");
    					if(i==0) {
    						if(originalValue.indexOf("','") != -1) {
    							notInParams = originalValue;
    							result = sql + " AND EVENT in ("+originalValue+") THEN '"+specialDataConfigVo.getConvertValue()+"' ";
    						} else {
    							notInParams = "'"+originalValue+"'";
    							result = sql + " AND EVENT='"+originalValue+"' THEN '"+specialDataConfigVo.getConvertValue()+"' ";
    						}
    					} else {
    						if(originalValue.indexOf("','") != -1) {
    							notInParams = notInParams + ","+originalValue;
    							result = result + sql + " AND EVENT in ("+originalValue+") THEN '"+specialDataConfigVo.getConvertValue()+"' ";
    						} else {
    							notInParams = notInParams + ",'"+originalValue+"'";
    							result = result + sql + " AND EVENT='"+originalValue+"' THEN '"+specialDataConfigVo.getConvertValue()+"' ";
    						}
    					}
    				}
    				if(clueLevel.startsWith("#") && clueLevel.endsWith("}")) {
        				result = result + sql + " AND EVENT NOT IN ("+notInParams+") THEN EVENT";
    				} else if(clueLevel.startsWith("#") && !clueLevel.endsWith("}")) {
    					String str = clueLevel.substring(clueLevel.indexOf("}")+1);
    					result = result + sql + " AND EVENT NOT IN ("+notInParams+") THEN CONCAT(EVENT, '"+str+"')";
    				} else if(!clueLevel.startsWith("#")&& clueLevel.endsWith("}")) {
    					String str = clueLevel.substring(0, clueLevel.indexOf("#"));
    					result = result + sql + " AND EVENT NOT IN ("+notInParams+") THEN CONCAT('"+str+"', EVENT)";
    				} else if(!clueLevel.startsWith("#")&& !clueLevel.endsWith("}")) {
    					String str1 = clueLevel.substring(0, clueLevel.indexOf("#"));
    					String str2 = clueLevel.substring(clueLevel.indexOf("}")+1);
    					result = result + sql + " AND EVENT NOT IN ("+notInParams+") THEN CONCAT('"+str1+"', EVENT, '"+str2+"')";
    				}
    			} else if(clueLevel.indexOf("#{label") != -1) {
    				String flag = clueLevel.substring(clueLevel.indexOf("#")+8, clueLevel.length()-1);
    				for(int i=0; i<specialDataConfigVos.size(); i++) {
    					UserBehaviorSpecialDataConfigVo specialDataConfigVo =specialDataConfigVos.get(i);
    					String originalValue = specialDataConfigVo.getOriginalValue();
    					originalValue = originalValue.replaceAll("#", "'");
    					if(i==0) {
    						if(originalValue.indexOf("','") != -1) {
    							notInParams = originalValue;
    							result = sql + " AND regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0) in ("+originalValue+") THEN '"+specialDataConfigVo.getConvertValue()+"' ";
    						} else {
    							notInParams = "'"+originalValue+"'";
    							result = sql + " AND regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0)='"+originalValue+"' THEN '"+specialDataConfigVo.getConvertValue()+"' ";
    						}
    					} else {
    						if(originalValue.indexOf("','") != -1) {
    							notInParams = notInParams + ","+originalValue;
    							result = result + sql + " AND regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0) in ("+originalValue+") THEN '"+specialDataConfigVo.getConvertValue()+"' ";
    						} else {
    							notInParams = notInParams + ",'"+originalValue+"'";
    							result = result + sql + " AND regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0)='"+originalValue+"' THEN '"+specialDataConfigVo.getConvertValue()+"' ";
    						}
    					}
    				}
    				if(clueLevel.startsWith("#") && clueLevel.endsWith("}")) {
        				result = result + sql + " AND regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0) NOT IN ("+notInParams+") THEN regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0)";
    				} else if(clueLevel.startsWith("#") && !clueLevel.endsWith("}")) {
    					String str = clueLevel.substring(clueLevel.indexOf("}")+1);
    					result = result + sql + " AND regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0) NOT IN ("+notInParams+") THEN CONCAT(regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0), '"+str+"')";
    				} else if(!clueLevel.startsWith("#")&& clueLevel.endsWith("}")) {
    					String str = clueLevel.substring(0, clueLevel.indexOf("#"));
    					result = result + sql + " AND regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0) NOT IN ("+notInParams+") THEN CONCAT('"+str+"', regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0))";
    				} else if(!clueLevel.startsWith("#")&& !clueLevel.endsWith("}")) {
    					String str1 = clueLevel.substring(0, clueLevel.indexOf("#"));
    					String str2 = clueLevel.substring(clueLevel.indexOf("}")+1);
    					result = result + sql + " AND regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0) NOT IN ("+notInParams+") THEN CONCAT('"+str1+"', regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0), '"+str2+"')";
    				}
    			}
    		} else {
    			if(clueLevel.indexOf("#{subtype}") != -1) {
    				if(clueLevel.startsWith("#") && clueLevel.endsWith("}")) {
    					result = sql + " THEN SUBTYPE";
    				} else if(clueLevel.startsWith("#") && !clueLevel.endsWith("}")) {
    					String str = clueLevel.substring(clueLevel.indexOf("}")+1);
    					result = sql + " THEN CONCAT(SUBTYPE, '"+str+"')";
    				} else if(!clueLevel.startsWith("#")&& clueLevel.endsWith("}")) {
    					String str = clueLevel.substring(0, clueLevel.indexOf("#"));
    					result = sql + " THEN CONCAT('"+str+"', SUBTYPE)";
    				} else if(!clueLevel.startsWith("#")&& !clueLevel.endsWith("}")) {
    					String str1 = clueLevel.substring(0, clueLevel.indexOf("#"));
    					String str2 = clueLevel.substring(clueLevel.indexOf("}")+1);
    					result = sql + " THEN CONCAT('"+str1+"', SUBTYPE, '"+str2+"')";
    				}
    			} else if(clueLevel.indexOf("#{event}") != -1) {
    				if(clueLevel.startsWith("#") && clueLevel.endsWith("}")) {
        				result = sql + " THEN EVENT";
    				} else if(clueLevel.startsWith("#") && !clueLevel.endsWith("}")) {
    					String str = clueLevel.substring(clueLevel.indexOf("}")+1);
    					result = sql + " THEN CONCAT(EVENT, '"+str+"')";
    				} else if(!clueLevel.startsWith("#")&& clueLevel.endsWith("}")) {
    					String str = clueLevel.substring(0, clueLevel.indexOf("#"));
    					result = sql + " THEN CONCAT('"+str+"', EVENT)";
    				} else if(!clueLevel.startsWith("#")&& !clueLevel.endsWith("}")) {
    					String str1 = clueLevel.substring(0, clueLevel.indexOf("#"));
    					String str2 = clueLevel.substring(clueLevel.indexOf("}")+1);
    					result = sql + " THEN CONCAT('"+str1+"', EVENT, '"+str2+"')";
    				}
    			} else if(clueLevel.indexOf("#{label") != -1) {
    				String flag = clueLevel.substring(clueLevel.indexOf("#")+8, clueLevel.length()-1);
    				result = sql + " THEN regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0)";
    				if(clueLevel.startsWith("#") && clueLevel.endsWith("}")) {
        				result = sql + " THEN regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0)";
    				} else if(clueLevel.startsWith("#") && !clueLevel.endsWith("}")) {
    					String str = clueLevel.substring(clueLevel.indexOf("}")+1);
    					result = sql + " THEN CONCAT(regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0), '"+str+"')";
    				} else if(!clueLevel.startsWith("#")&& clueLevel.endsWith("}")) {
    					String str = clueLevel.substring(0, clueLevel.indexOf("#"));
    					result = sql + " THEN CONCAT('"+str+"', regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0))";
    				} else if(!clueLevel.startsWith("#")&& !clueLevel.endsWith("}")) {
    					String str1 = clueLevel.substring(0, clueLevel.indexOf("#"));
    					String str2 = clueLevel.substring(clueLevel.indexOf("}")+1);
    					result = sql + " THEN CONCAT('"+str1+"', regexp_extract(LABEL,'(?<="+flag+":\")[^\",]*',0), '"+str2+"')";
    				}
    			}
    		}
    	} else {
    		result = sql + " THEN '"+clueLevel+"'";
    	}
    	return result;
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

	public List<UserBehaviorConfigVo> getUserBehaviorConfigVos() {
		//从数据库读取配置表
//        String dbURL = "jdbc:oracle:thin:@10.137.46.5:1521:tkorasol";
//        String dbUsername = "tkinsure";
//        String dbPassword = "110B8D102C55DF60DE87DE38BE44AF91B4C70A58DE128DE118D90D88";
        String dbURL = TK_CommonConfig.getConfigValue(CONFIG_PATH, "db.url");
        String dbUsername = TK_CommonConfig.getConfigValue(CONFIG_PATH, "db.user");
        String dbPassword = TK_CommonConfig.getConfigValue(CONFIG_PATH, "db.password");
		Connection conn = DBUtil.getConnection(dbURL, dbUsername, dbPassword);
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<UserBehaviorConfigVo> userBehaviorConfigVos = new ArrayList<UserBehaviorConfigVo>();
		String sql = "SELECT APP_TYPE, APP_ID, FUNC_DESC FROM USERBEHAVIOR_TRACK_CODE WHERE IF_ALALYSE='1'";
        try {
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()) {
				UserBehaviorConfigVo userBehaviorConfigVo = new UserBehaviorConfigVo();
				String appType = rs.getString("APP_TYPE");
				String appId = rs.getString("APP_ID");
				String functionDesc = rs.getString("FUNC_DESC");
				userBehaviorConfigVo.setAppId(appId);
				userBehaviorConfigVo.setAppType(appType);
				userBehaviorConfigVo.setFunctionDesc(functionDesc);
				userBehaviorConfigVos.add(userBehaviorConfigVo);
			}
			List<UserBehaviorDetailConfig> userBehaviorDetailConfigs = new ArrayList<UserBehaviorDetailConfig>();
			for(UserBehaviorConfigVo userBehaviorConfigVo : userBehaviorConfigVos) {
				String appId = userBehaviorConfigVo.getAppId();
				sql = "SELECT ID, EVENT, SUB_TYPE,USER_TYPE, LABEL, FIRST_LEVEL, SECOND_LEVEL, THIRD_LEVEL, FOURTH_LEVEL FROM USERBEHAVIOR_TRACK_CODE_DETAIL WHERE APP_ID='"+appId+"' AND IF_ANALYSE='1'";
				ps = conn.prepareStatement(sql);
				rs = ps.executeQuery();
				while(rs.next()) {
					UserBehaviorDetailConfig userBehaviorDetailConfig = new UserBehaviorDetailConfig();
					userBehaviorDetailConfig.setId(rs.getLong("ID"));
					userBehaviorDetailConfig.setEvent(rs.getString("EVENT"));
					userBehaviorDetailConfig.setSubtype(rs.getString("SUB_TYPE"));
					userBehaviorDetailConfig.setUserType(rs.getString("USER_TYPE"));
					userBehaviorDetailConfig.setLabel(rs.getString("LABEL"));
					userBehaviorDetailConfig.setFirstLevel(rs.getString("FIRST_LEVEL"));
					userBehaviorDetailConfig.setSecondLevel(rs.getString("SECOND_LEVEL"));
					userBehaviorDetailConfig.setThirdLevel(rs.getString("THIRD_LEVEL"));
					userBehaviorDetailConfig.setFourthLevel(rs.getString("FOURTH_LEVEL"));
					userBehaviorDetailConfigs.add(userBehaviorDetailConfig);
				}
				userBehaviorConfigVo.setUserBehaviorDetailConfigList(userBehaviorDetailConfigs);
				for(UserBehaviorDetailConfig userBehaviorDetailConfig : userBehaviorDetailConfigs) {
					List<UserBehaviorSpecialDataConfigVo> specialDataConfigVos = new ArrayList<UserBehaviorSpecialDataConfigVo>();
					long id = userBehaviorDetailConfig.getId();
					sql = "SELECT ID, APP_ID, CONFIG_DETAIL_ID, CLUE_LEVEL, ORIGINAL_VALUE, CONVERT_VALUE FROM USERBEHAVIOR_SPL_DATA WHERE CONFIG_DETAIL_ID="+id;
					ps = conn.prepareStatement(sql);
					rs = ps.executeQuery();
					while(rs.next()) {
						UserBehaviorSpecialDataConfigVo userBehaviorSpecialDataConfigVo = new UserBehaviorSpecialDataConfigVo();
						userBehaviorSpecialDataConfigVo.setId(rs.getString("ID"));
						userBehaviorSpecialDataConfigVo.setAppId(rs.getString("APP_ID"));
						userBehaviorSpecialDataConfigVo.setConfigDetailId(rs.getString("CONFIG_DETAIL_ID"));
						userBehaviorSpecialDataConfigVo.setClueLevel(rs.getString("CLUE_LEVEL"));
						userBehaviorSpecialDataConfigVo.setOriginalValue(rs.getString("ORIGINAL_VALUE"));
						userBehaviorSpecialDataConfigVo.setConvertValue(rs.getString("CONVERT_VALUE"));
						specialDataConfigVos.add(userBehaviorSpecialDataConfigVo);
					}
					userBehaviorDetailConfig.setSpecialConfigList(specialDataConfigVos);
				}
				userBehaviorDetailConfigs = new ArrayList<UserBehaviorDetailConfig>();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DBUtil.closeConnection(rs, ps, conn);
		}
		return userBehaviorConfigVos;
	}

}
