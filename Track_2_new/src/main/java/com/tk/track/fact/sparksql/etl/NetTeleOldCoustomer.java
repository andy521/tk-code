package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorClue;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.fact.sparksql.util.DateTimeUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;
/**
 * 网电  京鄂鲁老客户app
 * @author itw_shanll
 *
 */
public class NetTeleOldCoustomer implements Serializable {

	private static final long serialVersionUID = 539606288031574480L;

	public DataFrame getOldCoustomer(HiveContext sqlContext){
		
//		sqlContext.load(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_FSTATISTICSEVENT_OUTPUTPATH)).registerTempTable("FACT_STATISTICS_EVENT");
//		DataFrame teleDf = getUserBehaviorTele(sqlContext); 
//		JavaRDD<FactUserBehaviorClue> labelRdd = analysisLabelRDD(teleDf);
//		sqlContext.createDataFrame(labelRdd, FactUserBehaviorClue.class).registerTempTable("TMP_ANALYSISLABEL");;
		
		DataFrame df = getUserPolicyno(sqlContext);
//		saveAsParquet(df,"/user/tkonline/taikangscore/data/sll/getUserPolicyno");
		
		DataFrame df1 = getLongRisksName(sqlContext);
//		saveAsParquet(df1,"/user/tkonline/taikangscore/data/sll/getLongRisksName");
		
		DataFrame df2 = getUserCluesInfo(sqlContext);
//		saveAsParquet(df2, "/user/tkonline/taikangscore/data/sll/getUserCluesInfo");
		
		DataFrame df3 = getPlifeinsure(sqlContext);
//		saveAsParquet(df3, "/user/tkonline/taikangscore/data/sll/getPlifeinsure");
		
		DataFrame df4 = getUserPolicyholderId(sqlContext);
//		saveAsParquet(df4,"/user/tkonline/taikangscore/data/sll/getUserPolicyholderId");
		
		
		DataFrame df5 = getTakeMemberRegion(sqlContext);
//		saveAsParquet(df5,"/user/tkonline/taikangscore/data/sll/getTakeMemberRegion");
		
		DataFrame df6 = getUserPlifeMemberId(sqlContext);
//		saveAsParquet(df6,"/user/tkonline/taikangscore/data/sll/getUserPlifeMemberId");
		
//		DataFrame df6 = getOldUserBehaviorTele(sqlContext);
		
		return df6;
	}
	
	public void saveAsParquet(DataFrame df, String path) {
		TK_DataFormatConvertUtil.deletePath(path);
		df.saveAsParquetFile(path);
	}
	
	/**
	 * 查询长险，
	 * @param sqlconContext//accepttime
	 * @return//添加时间
	 */
	private DataFrame getUserPolicyno(HiveContext sqlContext){
		String timeStamp = getTodayTime(-2);
		String hql = "select st.lia_policyno,"
				+ " st.lrt_id,"
				+ " st.name,"
				+ " st.policyfee,"
				+ " st.risktypecode"
				+ " from tknwdb.sales_trade st "
				+ " where st.risktypecode='2' and creat_time ="+"'"+timeStamp+"'"+"";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "USER_POLICYNO");
	}
	
	//取长险名称
	private DataFrame getLongRisksName(HiveContext sqlContext){
		String hql = "select lrt_id,"
				+ " lrt_name"
				+ " from tkoldb.d_liferisktype"
				+ " where lrt_id in ('260','262','32','244','31','38','251','297','234','238','290','267','298','150','222','246','254')";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "LONGRISKSNAME");
	}
	
	
	private DataFrame getUserCluesInfo(HiveContext sqlContext){
		String hql = "select /*+MAPJOIN(lo)*/ lo.lrt_name,"
				+ " us.lia_policyno,"
				+ " us.lrt_id,"
				+ " us.name,"
				+ " us.policyfee"
				+ " from LONGRISKSNAME lo "
				+ " right join USER_POLICYNO us "
				+ " on us.lrt_id = lo.lrt_id";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "USERCLUSE_INFO");
	}
	
	private DataFrame getPlifeinsure(HiveContext sqlContext){
		String hql = "select t.policyholder_id,"
				+ " t.lia_policyno,"
				+ " t.lia_accepttime"
				+ " from(select policyholder_id, min(lia_accepttime) as accepttime "
				+ " from P_LIFEINSURE"
				+ " where policyholder_id is not null and policyholder_id <> '' "
				+ " group by policyholder_id) a"
				+ " inner join P_LIFEINSURE t"
				+ " on a.policyholder_id = t.policyholder_id"
				+ " and a.accepttime = t.lia_accepttime";
//				+ ""
//				+ ""
//				+ " from(select policyholder_id,lia_policyno,lia_accepttime, row_number() over(partition by policyholder_id order by lia_accepttime asc) rk"
//				+ " from P_LIFEINSURE "
//				+ " where policyholder_id is not null and policyholder_id <> '') t"
//				+ " where t.rk = 1";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "PLIFEINSURE");
	}
	
	private DataFrame getUserPolicyholderId(HiveContext sqlContext){
		String hql = "select distinct pl.policyholder_id,"
				+ " pl.lia_accepttime,"
				+ " up.lia_policyno,"
				+ " up.name, "
				+ " up.policyfee,"
				+ " up.lrt_name"
				+ " from PLIFEINSURE pl"
				+ " inner join USERCLUSE_INFO up"
				+ " on trim(up.lia_policyno) = trim(pl.lia_policyno)";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "USER_PLIFEINSURE_COUSTOMER");
	}
	
	
	private DataFrame getTakeMemberRegion(HiveContext sqlContext){
		String hql = "select member_id,"
				+ " customer_id "
				+ " from P_MEMBER "
				+ " where member_id is not null  and member_id <> '' "
				+ " and customer_id is not null and customer_id<>'' "
				+ " and company_no is not null and company_no<>'' "
				+ " and company_no in ('1','2','C')";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "MEMBER_REGION");
	}
	
	//寿险关联P_member取memberId
	private DataFrame getUserPlifeMemberId(HiveContext sqlContext){
		String timeStamp = getTodayTime(-1);
		String hql = "select pm.member_id as user_id,"
				+ " 'userBehavior_001' as app_id,"
				+ " 'C' as user_type,"
				+ " '' as ROWKEY,"
				+ " '车险' first_level,"
				+ " '车险线索客户' second_level,"
				+ " '三地网电已购客户' third_level,"
				+ " '长险' fourth_level,"
				+ " "+"'"+timeStamp+"'"+" as VISIT_TIME,"
				+ " CONCAT(NVL(pc.lia_policyno,'---'),';', NVL(pc.name,'---'),';', NVL(pc.lrt_name,'---'),';',NVL(pc.policyfee,'---'),';',NVL(pc.lia_accepttime,'---'))  REMARK"
				+ " from USER_PLIFEINSURE_COUSTOMER pc"
				+ " left join MEMBER_REGION pm"
				+ " on pc.policyholder_id=pm.customer_id"
				+ " where pm.member_id is not null and pm.member_id <> ''";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "USER_PLIFE_MEMBERID");
	}
	
	public static String getTodayTime(int day){
		Date date = new Date();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		calendar.add(Calendar.DATE, day);
		date=calendar.getTime();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		String dateString = formatter.format(date);
		return dateString;
	}
	
	
	public JavaRDD<FactUserBehaviorClue> getJavaRDD(DataFrame df){
		JavaRDD<Row> jRDD = df.select("ROWKEY", "USER_ID", 
    			"FIRST_LEVEL", "SECOND_LEVEL", "THIRD_LEVEL", "FOURTH_LEVEL","REMARK").rdd().toJavaRDD();
		JavaRDD<FactUserBehaviorClue> pRDD = jRDD.map(new Function<Row, FactUserBehaviorClue>(){

			private static final long serialVersionUID = -1619903863589066084L;
			
			@Override
			public FactUserBehaviorClue call(Row row) throws Exception {
				String ROWKEY = row.getString(0);         
			    String USER_ID = row.getString(1);        
			    String FIRST_LEVEL = row.getString(2);    
			    String SECOND_LEVEL = row.getString(3);   
			    String THIRD_LEVEL = row.getString(4);    
			    String FOURTH_LEVEL = row.getString(5);   
			    String REMARK = row.getString(6);	
				return new FactUserBehaviorClue(ROWKEY, USER_ID, FIRST_LEVEL,SECOND_LEVEL, THIRD_LEVEL,FOURTH_LEVEL,REMARK, null, null, null, null, null, null, null, null, null, null, null, null, null);
			}
		});
		return pRDD;
	}	
	
	public void pairRDD2Parquet(HiveContext sqlContext, JavaRDD<FactUserBehaviorClue> rdd, String path) {
		if (TK_DataFormatConvertUtil.isExistsPath(path)) {
			sqlContext.createDataFrame(rdd, FactUserBehaviorClue.class).save(path, "parquet", SaveMode.Append);
		} else {
			sqlContext.createDataFrame(rdd, FactUserBehaviorClue.class).saveAsParquetFile(path);
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
