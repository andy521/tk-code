package com.tk.track.fact.sparksql.udf;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;

import com.tk.track.util.IDUtil;

public class RandomUDF {
    public static final String ALLCHAR = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";  
    public static final String LETTERCHAR = "abcdefghijkllmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";  
    public static final String NUMBERCHAR = "0123456789";
	public static final String defaultReNum= TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_DEFAULT_RECOMMENDNUM);
	/*
	 * generate random number
	 */
	public static void genrandom(SparkContext sc, HiveContext sqlContext) {
		sqlContext.udf().register("genrandom", new UDF1<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = -6709911270475566751L;

			public String call(String str) { 
				return str + generateMixString(100);
			}
		}, DataTypes.StringType);
//		DataFrame dd = sqlContext.sql("select genrandom(name) from aaa");
//		dd.show();
	}
	
	public static String generateMixString(int length) {  
        StringBuffer sb = new StringBuffer();  
        Random random = new Random();  
        for (int i = 0; i < length; i++) {  
            sb.append(ALLCHAR.charAt(random.nextInt(LETTERCHAR.length())));  
        }  
        return sb.toString();  
    }

	public static void getUUID(SparkContext sc, HiveContext sqlContext) {
		sqlContext.udf().register("getUUID", new UDF1<String, String>() {

			private static final long serialVersionUID = 421437729041414070L;

			public String call(String str) { 
				return str + IDUtil.getUUID().toString();
			}
		}, DataTypes.StringType);
		
	}
	
	
	public static void gendecode(SparkContext sc, HiveContext sqlContext) {
		sqlContext.udf().register("gendecode", new UDF2<String,String,String>() {
			
			private static final long serialVersionUID = 7188936672557150586L;
			
			public String call(String str,String charSet) throws Exception {
				return URLDecoder.decode(str,charSet);
			}
		
		}, DataTypes.StringType);
	}
	
	/**
	 * 
	 * @Description: 比较两个字符串是否含有相同的部分（通过split切割后），存在则返回1，否则返回0
	 * @param src
	 * @param compare
	 * @param split
	 * @return
	 * @author moyunqing
	 * @date 2016年9月28日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	public static void spequals(SparkContext sc, HiveContext sqlContext) {
		sqlContext.udf().register("spequals", new UDF3<String,String,String,String>() {
			
			private static final long serialVersionUID = 8254616588117420276L;

			public String call(String src,String compare,String split) throws Exception {
				return specialEquals(src,compare,split);
			}

		}, DataTypes.StringType);
	}
	
	/**
	 * 
	 * @Description: 比较两个字符串是否含有相同的部分（通过split切割后），存在则返回1，否则返回0
	 * @param src
	 * @param compare
	 * @param split
	 * @return
	 * @author moyunqing
	 * @date 2016年9月28日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	private static String specialEquals(String src, String compare, String split) {
		String result = "0";
		if(StringUtils.isNotBlank(src) && StringUtils.isNotBlank(compare)){
			String [] srcArr = src.split(split);
			List<String>  srclist = new ArrayList<String>();
			for(String value : srcArr){
				if(StringUtils.isNotBlank(value) && !srclist.contains(value))
					srclist.add(value);
			}
			String [] compareArr = compare.split(split);
			for(String value : compareArr){
				if(StringUtils.isNotBlank(value) && srclist.contains(value)){
					result = "1";
					break;
				}
			}
		}
		return result;
	}
	
	
	public static void genUniq(SparkContext sc, HiveContext sqlContext) {
		sqlContext.udf().register("genuniq", new UDF3<String,String,String,String>() {
			
			private static final long serialVersionUID = -5389410033494100937L;

			public String call(String src,String spilt,String filter) throws Exception {
				return uniq(src,spilt,filter);
			}
		
		}, DataTypes.StringType);
	}
	
	public static String uniq(String src, String spilt, String filter) {
		if(StringUtils.isBlank(src)){
			return src;
		}
		String [] srcArr = src.split(spilt);
		List<String> values = new ArrayList<String>();
		
		for(String value : srcArr){
			if(!values.contains(value) && StringUtils.isNotBlank(value)){
				value = value.replaceAll("\"|\'|\\||\\t|\\r|\\n|\\{|\\}|/|\\\\", "");
				if(StringUtils.isNotBlank(filter) && value.matches(filter))
					value = "";
				if(StringUtils.isNotBlank(value))
					values.add(value);
			}
		}
		String distStr = "";
		for(String value : values){
			distStr += value + spilt;
		}
		if(StringUtils.isNotBlank(distStr) && distStr.lastIndexOf(spilt) != -1)
			return distStr.substring(0, distStr.lastIndexOf(spilt));
		return distStr;
	}
	
	/**
	 * 毫秒转成时分秒格式
	 * 
	 * @Description:
	 * @param sc
	 * @param sqlContext
	 * @author itw_qiuzq01
	 * @date 2016年9月14日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
	public static void mseconds2hms(SparkContext sc, HiveContext sqlContext) {
		sqlContext.udf().register("mseconds2hms", new UDF1<Double, String>() {

			private static final long serialVersionUID = 7188936672557150586L;

			public String call(Double ms) throws Exception {
				int ss = 1000;
				int mi = ss * 60;
				int hh = mi * 60;

				int hour = (int) (ms / hh);
				int minute = (int) (ms - hour * hh) / mi;
				int second = (int) (ms - hour * hh - minute * mi) / ss;
				System.out.println("hour" + hour + "minute" + minute + "secod" + second);

				String strHour = hour < 10 ? "0" + hour : "" + hour;
				String strMinute = minute < 10 ? "0" + minute : "" + minute;
				String strSecond = second < 10 ? "0" + second : "" + second;

				String rs = String.format("%s:%s:%s", strHour, strMinute, strSecond);

				return rs;
			}

		}, DataTypes.StringType);
	}

	/**
	 *  1 jsonArray 形式的数据 2 jsonArray
	 *  判断1的数量到达10个没有，如果没有，用2补充，返回补充后的列
	 * @param sc
	 * @param sqlContext
	 */
	public static void contactProductInfo(SparkContext sc, HiveContext sqlContext) {

		sqlContext.udf().register("contactproductinfo", new UDF2<String,String, String>() {

			private static final long serialVersionUID = -1095826986296381742L;

			public String call(String str1,String str2) throws Exception {

				String result ="";
				JSONArray jarray= JSONArray.fromObject(str1);
				JSONArray jarray2= JSONArray.fromObject(str2);
				int defaultRecomendNum=Integer.valueOf(defaultReNum);
				if(jarray.size()>defaultRecomendNum){
					int discardNum=jarray.size()-defaultRecomendNum;
					for(int i=0;i<discardNum;i++){
						jarray.discard(jarray.size()-1);
					}
					result=jarray.toString();
				}else if(jarray.size()==defaultRecomendNum){
					result=str1;
				}else{
					//要补充的数量
					int suppleNum=defaultRecomendNum-jarray.size();
					//记录现在已经有的产品，补充时排除
					List<String> list = new ArrayList<String>();
					for(int k=0;k<jarray.size();k++){
						JSONObject oneJson1=jarray.getJSONObject(k);
						String orginalId=oneJson1.getString("productID");
						list.add(orginalId);
					}
					for(int i=0;i<suppleNum && i<jarray2.size();i++){
						JSONObject obj2Add=jarray2.getJSONObject(i);
						String suppleProductId=obj2Add.getString("productID");
						if(list.contains(suppleProductId)){
							//跳过当前重复的，要补充的量往后推一
							suppleNum=suppleNum+1;
						}else{
							jarray.add(obj2Add);
						}
					}
					result=jarray.toString();
				}
				return result;
			}

		}, DataTypes.StringType);
	}

}
