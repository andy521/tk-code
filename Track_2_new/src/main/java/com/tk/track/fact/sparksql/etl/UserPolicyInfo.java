package com.tk.track.fact.sparksql.etl;
import java.io.Serializable;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.main.App;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;


public class UserPolicyInfo implements Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = -4734822495033680768L;
	String sca_nw = TK_DataFormatConvertUtil.getNetWorkSchema();
    String sca_ol = TK_DataFormatConvertUtil.getSchema();

    /**
     * 
     * @Description: 获取用户保单详细
     * @param sqlContext
     * @return
     * @author moyunqing
     * @date 2016年7月12日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    public DataFrame getUserPolicyInfoDF(HiveContext sqlContext) {
    	getUserInfoTmp(sqlContext);
    	loadPolicyInfoResultTable(sqlContext);
    	loadBehavior(sqlContext);
    	getCustomerID(sqlContext);
    	getPolicyByBehaviorTele(sqlContext);
        loadPolicyAll(sqlContext);
        return getUserPolicyInfoResult(sqlContext);
    }
    
    /**
     * 
     * @Description: 根据线索表和有效保单表关联出保单号
     * @param sqlContext
     * @return
     * @author moyunqing
     * @date 2016年7月12日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    private DataFrame getPolicyByBehaviorTele(HiveContext sqlContext) {
    	String hql = "SELECT * FROM ("
    			+ "		SELECT  DISTINCT"
        		+ "           CSTM.USER_ID, "
        		+ "           CSTM.LRT_ID, "
        		+ "           CSTM.CUSTOMER_ID, "
        		+ "           PRT.LIA_POLICYNO "
        		+ "   FROM    TMP_CSTM CSTM  "
        		+ "   LEFT JOIN TMP_PINFORST PRT "
        		+ "   ON      PRT.POLICYHOLDER_ID = CSTM.CUSTOMER_ID "
        		+ "   AND     PRT.LRT_ID = CSTM.LRT_ID ) TMP_POLICY_ALL"
        		+ "	 WHERE   LIA_POLICYNO IS NOT NULL"
        		+ "		 AND LIA_POLICYNO <> '' ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_Policy"); 
	}


    /**
     * 
     * @Description: load全部线索表
     * @param sqlContext
     * @return
     * @author moyunqing
     * @date 2016年7月12日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
	private DataFrame loadBehavior(HiveContext sqlContext) {
    	String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTUSERBEHAVIORTELE_OUTPUTPATH);
    	DataFrame df = sqlContext.load(path);
    	df.registerTempTable("Tmp_UserBehaviorTele");
    	return df;
	}


	/**
	 * 
	 * @Description: 获取userinfo表信息
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年7月12日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private DataFrame getUserInfoTmp(HiveContext sqlContext) {
        String hql = "SELECT  TFU.NAME, "
        		+ "           TFU.CUSTOMER_ID,"
        		+ "           TFU.MEMBER_ID "
        		+ "   FROM    FACT_USERINFO TFU "
        		+ "   WHERE   TFU.CUSTOMER_ID <> '' "
        		+ "   AND     TFU.CUSTOMER_ID IS NOT NULL "
        		+ "   AND     TFU.MEMBER_ID <>'' "
        		+ "   AND     TFU.MEMBER_ID IS NOT NULL";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "Tmp_UserInfo");
    }
    
	/**
	 * 
	 * @Description: 通过线索表和userinfo表关联出customer_id
	 * @param sqlContext
	 * @return
	 * @author moyunqing
	 * @date 2016年7月12日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
    private DataFrame getCustomerID(HiveContext sqlContext) {
        String hql = "SELECT  "
                + "           FUBTC.USER_ID, "
                + "           FUBTC.LRT_ID,"
                + "           FUCN.CUSTOMER_ID"
                + "     FROM  TMP_USERBEHAVIORTELE FUBTC, (SELECT DISTINCT MEMBER_ID,CUSTOMER_ID FROM TMP_USERINFO) FUCN " 
                + "    WHERE  FUBTC.IF_PAY = '0' "
                + "      AND  FUBTC.USER_ID = FUCN.MEMBER_ID";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_Cstm");
    }
    
    /**
     * 
     * @Description: load保单汇总表
     * @param sqlContext
     * @return
     * @author moyunqing
     * @date 2016年7月12日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    private DataFrame loadPolicyAll(HiveContext sqlContext) {
    	String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FNETPOLICYSUMMARY_OUTPUTPATH);
    	DataFrame df = sqlContext.load(path);
    	df.registerTempTable("FNET_POLICYSUMMARY");
        return df;
    }
    
    /**
     * 
     * @Description: 生成最终购买详情表
     * @param sqlContext
     * @return
     * @author moyunqing
     * @date 2016年7月12日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    private DataFrame getUserPolicyInfoResult(HiveContext sqlContext) {
        String hql = "SELECT  "
                + "           PL.USER_ID, "
                + "           PL.LRT_ID, "
                + "           PL.LIA_POLICYNO,"
                + "           FPLS.LIA_ACCEPTTIME "
                + "     FROM  TMP_POLICY PL "
                + " LEFT JOIN (SELECT DISTINCT "
                + "					  LIA_ACCEPTTIME,"
                + "					  POLICYHOLDER_ID,"
                + "					  LRT_ID,"
                + "					  LIA_POLICYNO"
                + "				FROM  FNET_POLICYSUMMARY) FPLS"
                + "    ON  PL.CUSTOMER_ID = FPLS.POLICYHOLDER_ID "
                + "      AND  PL.LRT_ID = FPLS.LRT_ID "
                + "      AND  PL.LIA_POLICYNO = FPLS.LIA_POLICYNO ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "FACT_USER_POLICYINFO");
    }
    
    /**
     * 
     * @Description: load有效保单表
     * @param sqlContext
     * @return
     * @author moyunqing
     * @date 2016年7月12日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    private Boolean loadPolicyInfoResultTable(HiveContext sqlContext) {
    	boolean succ = false;
    	String sysdt = App.GetSysDate(-1);
    	String pathTemp = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FACTNETPOLICYINFORESULT_OUTPUTPATH);
    	String path = pathTemp + "-" + sysdt;
    	if (TK_DataFormatConvertUtil.isExistsPath(path)) {
    		succ = true;
	    	sqlContext.load(path).registerTempTable("Tmp_PInfoRst"); //有效保单
    	}
    	return succ;
    } 
    
/*    public JavaRDD<FactUserPolicyInfo> getJavaRDD(DataFrame df) {
        RDD<Row> rdd = df.select("ROWKEY", "USER_ID", "LRT_ID", "LIA_POLICYNO", "LIA_ACCEPTTIME").rdd();
        JavaRDD<Row> jRDD = rdd.toJavaRDD();

        JavaRDD<FactUserPolicyInfo> pRDD = jRDD.map(new Function<Row, FactUserPolicyInfo>() {
            private static final long serialVersionUID = -6741916281211281524L;

            public FactUserPolicyInfo call(Row v1) throws Exception {
                return new FactUserPolicyInfo(v1.getString(0), v1.getString(1), v1.getString(2),
                        v1.getString(3), v1.getString(4));
            }
        });
        return pRDD;
    }
*/    
	/*public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactUserPolicyInfo> rdd, String path) {
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			String tmpPath = path + "-temp";
			Integer repartition_count = 200;
			try {
				repartition_count = Integer.valueOf(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_REPARTITION_COUNT));
			} catch (Exception e) {
				repartition_count = 200;
			}
			
			// Append new data to parquet[path]
			sqlContext.createDataFrame(rdd, FactUserPolicyInfo.class).save(path, "parquet", SaveMode.Append);
			// Load parquet[path], save as parquet[tmpPath]
			TK_DataFormatConvertUtil.deletePath(tmpPath);
			sqlContext.load(path).repartition(repartition_count).saveAsParquetFile(tmpPath);
			// Delete parquet[path]
			TK_DataFormatConvertUtil.deletePath(path);
			// Rename parquet[tmpPath] as parquet[path]
			TK_DataFormatConvertUtil.renamePath(tmpPath, path);
		} else {
			sqlContext.createDataFrame(rdd, FactUserPolicyInfo.class).saveAsParquetFile(path);
		}
	}*/
    
    /**
     * 
     * @Description: parquet文件保存
     * @param df
     * @param path
     * @author moyunqing
     * @date 2016年7月12日
     * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
     */
    public void saveAsParquet(DataFrame df, String path) {
		TK_DataFormatConvertUtil.deletePath(path);
		df.saveAsParquetFile(path);
	}
    
}
