package com.tk.track.fact.sparksql.etl;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.RenewalMessageBean;
import com.tk.track.fact.sparksql.util.DMUtility;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;

import scala.Tuple2;
public class RenewalMessage implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2457665299032809035L;
	public static final String HIVE_SCHEMA = TK_DataFormatConvertUtil.getSchema();
	public static final String TKPIDB = "tkpidb.";
	public static final String TKNWDB = "tknwdb.";
	public DataFrame getDataFrame(HiveContext sqlContext) {
		sqlContext.load("common-parquet/GUPOLICYMAIN").registerTempTable("GUPOLICYMAIN");
		sqlContext.load("common-parquet/GUPOLICYRELATEDPARTY").registerTempTable("GUPOLICYRELATEDPARTY");
		getAllpolicy(sqlContext);
		getPersonInfo(sqlContext);
		getIsgoingPolicy(sqlContext);
		return getRenewalPolicy(sqlContext);
	}


	


	/*
	 * 获取发送续保消息的所有保单
	 */
	private DataFrame getAllpolicy(HiveContext sqlContext) {
		String withwaitdaysproduct = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_WITHWAITDAYS_PRODUCT);
		String allpruduct = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_ALL_PRODUCT);
		String sql = "SELECT  /*+mapjoin(G)*/                                             "
				+ "		 G.RISKNAME,                                                      "
				+"       G.RISKCODE,                                                      "
				+"       T.POLICYNO,                                                      "
				+ "      G.SPLANNAME,                                                     "
				+ "     case when g.splancode in  ("+withwaitdaysproduct+") then"
						+ "   T.WAITDAYS else '' end as WAITDAYS,                                                     "
				+ "      case when g.splancode in  ("+withwaitdaysproduct+") then"
					    + " '1' "
					    + "else "
					    + " '0'"
					    + "end as iswithwaitdays,             "
				+"       H.IDENTIFYNUMBER,                                                "
				+"       H.EMAIL,  "
				+ "      H.mobilephone,                                                   "
				+ "      G.SPLANCODE, "
				+ "      T.SURRENDERIND,"
				+ "      T.CANCELIND,                                                    "
				+"       H.IDENTIFYTYPE,                                                  "
				+"       T.SURVEYIND,"
				+ "      T.sumgrosspremium,                                                    "
				+"       H.INSUREDNAME                                                    "
				+" FROM (select POLICYNO,WAITDAYS,SURRENDERIND,CANCELIND,SURVEYIND,FLOWID,sumgrosspremium  "
				+ "  from GUPOLICYMAIN  where flowid is not null and flowid <> '' ) T                                     "
				+"  INNER JOIN  "+HIVE_SCHEMA+"GSCHANNELPLANCODE G                        "
				+"    ON G.DPLANCODE = T.FLOWID                                           "
				+" INNER JOIN GUPOLICYRELATEDPARTY H                       "
				+"    ON T.POLICYNO = H.POLICYNO                                          "
				+" WHERE "
				+ "   G.SPLANCODE IN  ("+allpruduct+"     )    ";
//						+ "AND T.groupind <> '2'        ";

		
		System.out.println("==============AllPolicysql=================");
		System.out.println(sql);
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, sql, "AllPolicy");
		return df;
		
	}
	
	
	
	private DataFrame getPersonInfo(HiveContext sqlContext) {
		String tencheappruduct= TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_TENCHEAP_PRODUCT);
		String eightcheappruduct= TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_EIGHTCHEAP_PRODUCT);
		String sql = "SELECT "
				+ "		 G.RISKNAME,                                                      "
				+"       G.RISKCODE,                                                      "
				+"       G.POLICYNO,                                                      "
				+ "      G.SPLANNAME,                                                     "
				+"       G.WAITDAYS,"
				+"       G.IDENTIFYNUMBER,                                                "
				+"       G.EMAIL,  "
				+ "      G.mobilephone,                                                   "
				+ "      G.SPLANCODE,                                                     "
				+"       G.IDENTIFYTYPE, "
				+"       G.SURVEYIND,                                                     "
				+"       G.iswithwaitdays,"
				+"          G.INSUREDNAME as INSURED_NAME, "
				+ "      G.sumgrosspremium,                                                "
				+"       CASE                                                             "
				+"         WHEN M.POLICYNO IS NULL AND N.POLICYNO IS NOT NULL THEN        "
				+"          N.INSUREDNAME                                                 "
				+"         WHEN N.POLICYNO IS  NULL AND M.POLICYNO IS NOT NULL THEN        "
				+"          M.CLIENTCNAME                                                 "
				+"         ELSE                                                           "
				+"          G.INSUREDNAME                                                 "
				+"       END INSUREDNAME,                                                 "
				+ "       CASE WHEN G.SPLANCODE IN ("+eightcheappruduct+")   THEN"
				+"       '8'    "
				+ "       ELSE  "
				+ "       '' END CHEAP,"
				+ "       CASE WHEN G.SURRENDERIND = '0'  AND G.CANCELIND = '0' "
				+ "       AND GC.POLICYNO IS   NULL  "
				+ "       THEN   '0'                                                      "
				+ "       ELSE '1'                                                        "
				+ "       END   ISRENEWAL   "
				+ "      FROM ALLPOLICY G"
				+"  LEFT JOIN  "
				+ " "+TKPIDB+"GUPOLICYITEMACCILIST  M  "
				+"    ON G.POLICYNO = M.POLICYNO                                          "
				+"  LEFT JOIN (SELECT POLICYNO,insuredname from "
				+ " "+TKPIDB+"gupolicyriskrelatedparty    "
				+ "  group by     POLICYNO,insuredname)  N             "
				+"    ON G.POLICYNO = N.POLICYNO                "
				+ "LEFT JOIN "+HIVE_SCHEMA+" GCCLAIMMAIN GC ON G.POLICYNO = GC.POLICYNO        ";
		System.out.println("=================personinfosql================");
		System.out.println(sql);
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, sql, "PERSONINFO");
		return df;
		
		
//		return DataFrameUtil.getDataFrame(sqlContext, sql, "PERSONINFO");
	}
	
	
	
	
	
	 /**
     * 所有将要过期的保单
     * @param sqlContext
     * @return
     */
	private DataFrame getIsgoingPolicy(HiveContext sqlContext) {

		String sql = "SELECT  POLICYNO,  substr(G.ENDDATE,1,19) AS ENDDATE ,substr(G.STARTDATE,1,19) AS STARTDATE,    "
				+" 30 AS  POLICYTIME,                                                                    "
				+"CASE WHEN GRM.RENEWALNO IS NULL THEN '0'                                             "
				+"ELSE '2' END ISRENEWAL                                                               "
				+"FROM (select POLICYNO,ENDDATE,STARTDATE from "+TKPIDB +"gupolicyrisk "
                + "GROUP BY POLICYNO,ENDDATE,STARTDATE) G                                                  "
				+"LEFT JOIN "+TKPIDB+"GURENEWALMAIN GRM                                                   "
				+"ON G.POLICYNO = GRM.RENEWALNO                                                        "
				+"  GROUP BY G.POLICYNO,G.ENDDATE, GRM.RENEWALNO ,G.STARTDATE                                       ";
		System.out.println("====================ISGOINGPOLICY=========================");
		System.out.println(sql);
		DataFrame df = DataFrameUtil.getDataFrame(sqlContext, sql, "ISGOINGPOLICY");
		return df;
//		return DataFrameUtil.getDataFrame(sqlContext, sql, "ISGOINGPOLICY");
		
	}
	
	 /**
     * 要通知的 续保用户
     * @param sqlContext
     * @return
     */
	private DataFrame getRenewalPolicy(HiveContext sqlContext) {
		String  sql = "SELECT /*+mapjoin(HC)*/ distinct AP.RISKNAME,                   "
				 +"      AP.RISKCODE,                     "
				 +"      AP.POLICYNO,                     "
				 +"      AP.IDENTIFYNUMBER,               "
				 +"      AP.EMAIL,                        "
				 +"      AP.MOBILEPHONE,                  "
				 +"      AP.SURVEYIND AS FROMID,          "
				 +"      AP.INSUREDNAME,                  "
				 +"      HC.PLATFORM,                     "
				 +"      AP. CHEAP AS CHEAPOFF,           "
				 +"      IGP.ENDDATE,                     "
				 +"      CASE                             "
				 +"        WHEN IGP.ISRENEWAL = '0' THEN  "
				 +"          AP.ISRENEWAL                 "
				 +"        ELSE                           "
				 +"         IGP.ISRENEWAL                 "
				 +"      END ISRENEWAL,                   "
				 +"      AP.WAITDAYS,                     "
				 +"      AP.IDENTIFYTYPE,                 "
				 +"      IGP.POLICYTIME,                 "
				 +"      FU.OPEN_ID,                      "
				 +"      '60' AS KXDAYS,                  "
				 +"      IGP.STARTDATE,                   "
				 +"      AP.SPLANCODE,                    "
				 +"      AP.SPLANNAME,"
				 + "     AP.INSURED_NAME,"
				 + "     AP.sumgrosspremium,              "
				 + "     ap.iswithwaitdays"
				 +" FROM PERSONINFO AP                     "
				 +"INNER JOIN ISGOINGPOLICY IGP            "
				 +"   ON AP.POLICYNO = IGP.POLICYNO       "
				 +" LEFT JOIN tknwdb.HANDLE_CLIENT HC     "
				 +"   ON AP.SURVEYIND = HC.FROM_ID        "
				 +" LEFT JOIN (select CID_NUMBER ,max(OPEN_ID) as "
				 + "            OPEN_ID from tkoldb.FACT_USERINFO group by CID_NUMBER) FU     "
				 +"   ON AP.IDENTIFYNUMBER = FU.CID_NUMBER";
		System.out.println("======================RENEWALPOLICY==================");
		System.out.println(sql);
		return DataFrameUtil.getDataFrame(sqlContext, sql, "RENEWALPOLICY");
	}
	
	/**
	 * 存入hbase
	 * @param sqlContext
	 */
	public static void loadToHbase(HiveContext sqlContext,String date){
	   DataFrame df = getHbaseFrame(sqlContext);
	   String hfilePath=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RENEWAL_TOHBASE);
	   DMUtility.deletePath(hfilePath);
	   df.select("riskname", "riskcode", "policyno", "identifynumber", "email", "mobilephone", "fromid",
			   "insuredname","platform",
			   "cheapoff","enddate","policytime","isrenewal","waitdays",
			   "identifytype","open_id","kxdays","startdate","splancode","splanname","iswithwaitdays").
       toJavaRDD().flatMapToPair(new PairFlatMapFunction<Row, String, RenewalMessageBean>() {

           private static final long serialVersionUID = -7992483268543464592L;

           public Iterable<Tuple2<String, RenewalMessageBean>> call(Row t) throws Exception {
               List<Tuple2<String, RenewalMessageBean>> list = new ArrayList<>();

               String riskname=t.getString(0);
               String riskcode=t.getString(1);
               String policyno=t.getString(2);
               String identifynumber=t.getString(3);
               String email =t.getString(4);
               String mobilephone =t.getString(5);
               String fromid =t.getString(6);
               String insuredname =t.getString(7);
               String platform=t.getString(8);
               String cheapoff=t.getString(9);
               String enddate =t.getString(10);
               String policytime=t.get(11)+"";
               String isrenewal =t.getString(12);
//               String waitdays=t.getString(13);
               String waitdays = t.get(13)+"";
               String identifytype=t.getString(14);
               String open_id=t.getString(15);
               String kxdays=t.getString(16);
               String startdate = t.getString(17);
               String splancode = t.getString(18);
               String splanname = t.getString(19);
               String iswithwaitdays = t.getString(20);
               String rowkey = "";
              
               Random r = new Random();
               int nextInt = r.nextInt(100000000);
               String chars = "abcdefghijklmnopqrstuvwsyz";
               rowkey =chars.charAt((int)(Math.random()*26))+"_"+System.currentTimeMillis()+"_"+nextInt+"";

               System.out.println("===================================");
               System.out.println("wcy========== "+rowkey);
               
               RenewalMessageBean bean = new RenewalMessageBean(riskname,riskcode,policyno,identifynumber,
            		   email,mobilephone,fromid,insuredname,platform,cheapoff,enddate,policytime,
            		   isrenewal,waitdays,identifytype,open_id,kxdays,startdate,splancode,splanname,iswithwaitdays);
               list.add(new Tuple2<String, RenewalMessageBean>(rowkey, bean));
               return list;
           }

       }).groupByKey(32).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<RenewalMessageBean>>, ImmutableBytesWritable, KeyValue>() {

           private static final long serialVersionUID = -2190849107088990449L;

           @Override
           public Iterable<Tuple2<ImmutableBytesWritable, KeyValue>> call(Tuple2<String, Iterable<RenewalMessageBean>> t1) throws Exception {
               List<Tuple2<ImmutableBytesWritable, KeyValue>> R_list = new ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>>();
               String rowkey = t1._1();
               Iterator<RenewalMessageBean> it = t1._2().iterator();

               while (it.hasNext()) {
            	   RenewalMessageBean onebean = it.next();
            	   String riskname = onebean.getRiskname()==null?"":onebean.getRiskname();
            	   String riskcode = onebean.getRiskcode()==null?"":onebean.getRiskcode();
            	   String policyno = onebean.getPolicyno()==null?"":onebean.getPolicyno();
            	   String identifynumber = onebean.getIdentifynumber()==null?"":onebean.getIdentifynumber();
            	   String email = onebean.getEmail()==null?"":onebean.getEmail();
            	   String mobilephone = onebean.getMobilephone()==null?"":onebean.getMobilephone();
            	   String fromid = onebean.getFromid()==null?"":onebean.getFromid();
            	   String insuredname = onebean.getInsuredname()==null?"":onebean.getInsuredname();
            	   String platform = onebean.getPlatform()==null?"":onebean.getPlatform();
            	   String cheapoff = onebean.getCheapoff()==null?"":onebean.getCheapoff();
            	   String enddate = onebean.getEnddate()==null?"":onebean.getEnddate();
            	   String policytime = onebean.getPolicytime()==null?"":onebean.getPolicytime();
            	   String isrenewal = onebean.getIsrenewal()==null?"":onebean.getIsrenewal();
            	   String waitdays = onebean.getWaitdays()==null?"":onebean.getWaitdays();
            	   String identifytype = onebean.getIdentifytype()==null?"":onebean.getIdentifytype();
            	   String open_id = onebean.getOpen_id()==null?"":onebean.getOpen_id();
            	   String kxdays = onebean.getKxdays()==null?"":onebean.getKxdays();
            	   String startdate = onebean.getStartdate()==null?"":onebean.getStartdate();
            	   String splancode = onebean.getSplancode()==null?"":onebean.getSplancode();
            	   String splanname = onebean.getSplanname()==null?"":onebean.getSplanname();
            	  String iswithwaitdays = onebean.getIswithwaitdays() ==null?"":onebean.getIswithwaitdays();
            	  
            	   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("CHEAPOFF").getBytes(), cheapoff.getBytes())));
            	   
            	   
            	   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("EMAIL").getBytes(), email.getBytes())));

                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("ENDDATE").getBytes(), enddate.getBytes())));
                  R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("FROMID").getBytes(), fromid.getBytes())));
                   
                   
                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("IDENTIFYNUMBER").getBytes(), identifynumber.getBytes())));

                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("IDENTIFYTYPE").getBytes(), identifytype.getBytes())));

                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("INSUREDNAME").getBytes(), insuredname.getBytes())));

                   
                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("ISRENEWAL").getBytes(), isrenewal.getBytes())));
                  
                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("ISWITHWAITDAYS").getBytes(), iswithwaitdays.getBytes())));
                   
                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("KXDAYS").getBytes(), kxdays.getBytes())));
                   
                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("MOBILEPHONE").getBytes(), mobilephone.getBytes())));
                   
                   
                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("OPEN_ID").getBytes(), open_id.getBytes())));
                   
                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("PLATFORM").getBytes(), platform.getBytes())));
                 
                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("POLICYNO").getBytes(), policyno.getBytes())));
                   
                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("POLICYTIME").getBytes(), policytime.getBytes())));
                   
                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("RISKCODE").getBytes(), riskcode.getBytes())));

                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("RISKNAME").getBytes(), riskname.getBytes())));
                  
                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("SPLANCODE").getBytes(), splancode.getBytes())));
                   
                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("SPLANNAME").getBytes(), splanname.getBytes())));
                   
                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("STARTDATE").getBytes(), startdate.getBytes())));
                   
                   
                   R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                           new ImmutableBytesWritable(rowkey.getBytes()),
                           new KeyValue(rowkey.getBytes(),
                                   TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                   ("WAITDAYS").getBytes(), waitdays.getBytes())));


               }
               return R_list;
           }
       })
       .repartitionAndSortWithinPartitions(new RennewalFuctionPartitoner(17))
       .saveAsNewAPIHadoopFile(hfilePath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, HBaseConfiguration.create()); 
	   String tableName_Pix = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RENEWAL_TOHBASE_Name)+date;
	   try {
           //新增数据
//       	DMUtility.bulkloadAppend(hfilePath, tableName_Pix,getSplitKeys());
		   DMUtility.bulkloadWithoutDate(hfilePath, tableName_Pix, getSplitKeys());
		   String signTableName=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_SIGN_TABLENAME);
	       DMUtility.insert2TableWithoutDate(signTableName, "3", "info", "RENEWAL_RESULT_TABLE", tableName_Pix);
       } catch (IOException e) {
           e.printStackTrace();
       };
		
	}


	private static DataFrame getHbaseFrame(HiveContext sqlContext) {
		String sql =" SELECT         "
				+ "     RISKNAME,                  "
				+"		RISKCODE,                  "
				+"		POLICYNO,                  "
				+"		IDENTIFYNUMBER,            "
				+"		EMAIL,                     "
				+"		MOBILEPHONE,               "
				+"		FROMID,                    "
				+"		INSUREDNAME,               "
				+"		PLATFORM,                  "
				+"		CHEAPOFF,                  "
				+"		ENDDATE,                   "
				+"		POLICYTIME,                "
				+"		ISRENEWAL ,                "
				+"		WAITDAYS,                  "
				+"      IDENTIFYTYPE,              "
				+"      KXDAYS,                    "
				+ "     OPEN_ID,                   "
				+ "     STARTDATE,                 "
				+ "     SPLANCODE,                 "
				+ "     SPLANNAME,                 "
				+ "     iswithwaitdays             "
				+" FROM RENEWALPOLICY2             ";
		DataFrame df = sqlContext.sql(sql); 
		return df;
	}
	
    public static byte[][] getSplitKeys(){
        String[] keys = new String[]{"0","1","2","3","4","5","6","7","8","9","a","b","c","e","f"};
        byte[][] splitkeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//asc
        for (int i = 0;i < keys.length;i++){
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowkeyIter = rows.iterator();
        int count = 0;
        while (rowkeyIter.hasNext()){
            byte[] tempRow = rowkeyIter.next();
            rowkeyIter.remove();
            splitkeys[count] = tempRow;
            count++;
        }
        return splitkeys;
    }
	
    
    public static class RennewalFuctionPartitoner extends Partitioner implements Serializable{

        private static final long serialVersionUID = -5697564408603445108L;
        private int partitions;

        public RennewalFuctionPartitoner(int OPPartitioners){
            partitions = OPPartitioners;
        }

        /* (non-Javadoc)
         * @see org.apache.spark.Partitioner#getPartition(java.lang.Object)
         */

        @Override
        public int getPartition(Object keyOb) {
            ImmutableBytesWritable key = (ImmutableBytesWritable)keyOb;
            String keys = new String(key.get());
            String[] SplitKey = new String[]{"0","1","2","3","4","5","6","7","8","9","a","b","c","e","f"};
            int index = 0;
            for (String split : SplitKey) {
                if (keys.compareTo(split)>=0) {
                    ++index;
                } else {
                    break;
                }
            }
            return index;
        }

        /* (non-Javadoc)
         * @see org.apache.spark.Partitioner#numPartitions()
         */
        @Override
        public int numPartitions() {
            // TODO Auto-generated method stub
            return partitions;
        }

    }
    
}
