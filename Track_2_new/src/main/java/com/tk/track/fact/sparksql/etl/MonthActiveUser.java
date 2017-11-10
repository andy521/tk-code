package com.tk.track.fact.sparksql.etl;

import java.io.Serializable;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;
/**
 * 月活跃用户
 * @author itw_shanll
 *
 */
public class MonthActiveUser implements Serializable {
	private static final long serialVersionUID = -1440874889137218202L;
	
	/**
	 * 加载月活跃用户
	 * @param sqlContext
	 */
	public static void loadMonthActiveUser(HiveContext sqlContext){
		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_MONTHACTIVEUSER_OUTPUTPATH);
		
		DataFrame df1= getMonthUser(sqlContext);
//		saveAsParquet(df1, "/user/tkonline/taikangtrack/data/sll/getMonthUser");
		
		DataFrame df2 = getMemberId(sqlContext);
//		saveAsParquet(df2, "/user/tkonline/taikangtrack/data/sll/getMemberId");
		
		MonthActiveUser activeUser = new MonthActiveUser();
		activeUser.saveAsParquet(df2, path);
		
	}
	
//	//访问路径
//	public DataFrame getSharedVisitRecord(HiveContext sqlContext){
//		DataFrame df1= getMonthUser(sqlContext);
//		
//		DataFrame df2 = getMemberId(sqlContext);
//		
//		return df2;
//	}
	
	//取30天内登录过的用户
	public static DataFrame getMonthUser(HiveContext sqlContext){
		String hql = "select distinct uba.user_id"
				+ " from TMP_UBA_LOG_EVENT uba "
				+ " where datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),from_unixtime(cast(cast(uba.clienttime as bigint) / 1000 as bigint),'yyyy-MM-dd')) <= 30"
				+ " and uba.app_type='app' and uba.user_id is not null and uba.user_id <>''";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "MONTH_ACTIVE_USER");
	}
	
	//取member_id
	public static DataFrame getMemberId(HiveContext sqlContext){
		String hql = "select /*+MAPJOIN(m)*/ m.user_id as member_id"
//				+ " tp.member_id "
				+ " from MONTH_ACTIVE_USER m "
				+ " inner join TMP_P_MEMBER tp "
				+ " on trim(m.user_id)=trim(tp.member_id)";
		return DataFrameUtil.getDataFrame(sqlContext, hql, "MONTH_ACTIVE_USER");
	}
	
	
	
	public static void saveAsParquet(DataFrame df, String path) {
		TK_DataFormatConvertUtil.deletePath(path);
		df.saveAsParquetFile(path);
	}

}
