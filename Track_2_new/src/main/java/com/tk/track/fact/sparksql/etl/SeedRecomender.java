package com.tk.track.fact.sparksql.etl;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.FactSrcUserEvent;
import com.tk.track.fact.sparksql.desttable.SeedRecomenderBean;
import com.tk.track.fact.sparksql.main.App;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.fact.sparksql.util.DateTimeUtil;
import com.tk.track.util.TK_DataFormatConvertUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by itw_chenhao01 on 2017/8/15.
 */
public class SeedRecomender implements Serializable {
    private static final long serialVersionUID = -2162456612968993235L;
    private static final String DEST_TABLE_NAME="SeedRecomender";

    /**
     * 程序入口
     * @param sqlContext
     * @param isCreate
     * @param isCache
     */
    public static void loadSeedProduct(HiveContext sqlContext,boolean isCreate,boolean isCache) {
        DataFrame df=null;
        //write to parquet
        String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMENDERSHARE_OUTPUTPATH);
//        String sysdt = DateTimeUtil.GetSysDate(0);
//        path = path + "-" + sysdt;
        if(isCreate){
            SeedRecomender sipa=new SeedRecomender();
            df=sipa.getExtPageLoadDF(sqlContext);
            if(df!=null){
                //解析日志并生成所需要parquet
                DataFrame seedDf=sipa.getSharedVisitRecord(sqlContext, df);
                sipa.saveAsParquet(seedDf,path);

            }else{
                System.out.println("df = null");
            }
        }else{
            df = sqlContext.load(path);
            df.registerTempTable(DEST_TABLE_NAME);
            if(isCache)
                sqlContext.cacheTable(DEST_TABLE_NAME);
        }
    }

    /**
     * 加载用户行为表
     * @param sqlContext
     * @return
     */
    public DataFrame getExtPageLoadDF(HiveContext sqlContext) {
        String hql = "select * from TMP_UBA_LOG_EVENT where " +
                "event <>'page.load' AND event <> 'page.unload' ";

        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_PAGE_LOAD");
    }

    /**
     * 程序总体流程入口
     * @param hiveContext
     * @param df
     * @return
     */
    public DataFrame getSharedVisitRecord(HiveContext hiveContext,DataFrame df){
        JavaRDD<Row> rowJavaRDD= fliterUsefulData(df);
       DataFrame seedDf= getSeedRecomender(hiveContext, rowJavaRDD);
        //TODO
//        saveAsParquet(seedDf,"/user/tkonline/taikangtrack_test/data/recomendershare_orgi_record");
        DataFrame dfff= UserEachProductVisit(hiveContext);
       // saveAsParquet(dfff,"/user/tkonline/taikangtrack_test/data/recomendershare_ueachpv");
       DataFrame dfconi= contactInfo(hiveContext);
//        saveAsParquet(dfconi,"/user/tkonline/taikangtrack_test/data/recomendershare_dfconi");
//
//
//        String path = "/user/tkonline/taikangtrack_test/data/recomendershare_dfconi";
//        App.loadParquet(hiveContext,path, "UserEachProductVisit", false);

        DataFrame dfres=shareInfo(hiveContext);
        return dfres;
    }

    /**
     * 过滤得出需要的数据
     * @param df
     * @return
     */
    private  JavaRDD<Row> fliterUsefulData(DataFrame df){
        /**
         *  ROWKEY = rOWKEY;
         IP = iP;
         TIME = tIME;
         APP_TYPE = aPP_TYPE;
         APP_ID = aPP_ID;
         USER_ID = uSER_ID;
         PAGE = pAGE;
         EVENT = eVENT;
         SUBTYPE = sUBTYPE;
         LABEL = lABEL;
         CUSTOM_VAL = cUSTOM_VAL;
         REFER = rEFER;
         CURRENTURL = cURRENTURL;
         BROWSER = bROWSER;
         SYSINFO = sYSINFO;
         TERMINAL_ID = tERMINAL_ID;
         DURATION = dURATION;
         CLIENTTIME = cLIENTTIME;
         this.FROM_ID = fROM_ID;
         */
        JavaRDD<Row> jRDD= df.select("ROWKEY", "IP", "TIME", "APP_TYPE","APP_ID","USER_ID","PAGE",
                "EVENT", "SUBTYPE", "LABEL", "CUSTOM_VAL", "CURRENTURL", "TERMINAL_ID","FROM_ID").rdd().toJavaRDD();
        JavaRDD<Row> rowRdd=jRDD.filter(new Function<Row, Boolean>() {
            //过滤掉 currentUrl 中不包含seedId和seedType的 及 label中不包含产品信息的
            public Boolean call(Row v1) throws Exception {
                String appType=v1.getString(3);
                String label = v1.getString(9);
                String currentUrl = v1.getString(11);
//                System.out.println("currentUrl:"+currentUrl);

                if ((v1.getString(7) == null) || v1.getString(7).equals("") || v1.getString(7).isEmpty() ||
                        (label == null) || label.equals("") || label.isEmpty() ||
                        (currentUrl == null) || currentUrl.equals("") || currentUrl.isEmpty()) {
                    return false;
                } else {
                    label = label.replaceAll("\"", "");
//                    System.out.println("label:"+label);
                    //取得label中的参数
                    Map<String, String> labelMap = getLabelParameter(label);
                    //过滤本次需要的数据
                    String project_name = labelMap.get("project_name");

                    if (!("seed_share".equals(project_name)&&"wechat".equals(appType))) {
                        return false;
                    }
                }
                return true;
            }
        });
        return rowRdd;
    }

    /**
     * 将用户行为日志表转化为需要用的表
     * @param hiveContext
     * @param rdd
     * @return
     */
    private DataFrame  getSeedRecomender(HiveContext hiveContext,JavaRDD<Row> rdd){
        JavaRDD<SeedRecomenderBean> seedRdd = rdd.map(new Function<Row, SeedRecomenderBean>() {

            @Override
            public SeedRecomenderBean call(Row row) throws Exception {
                FactSrcUserEvent userEvent=   new FactSrcUserEvent(row.getString(0),row.getString(1),row.getString(2),row.getString(3),
                        row.getString(4),row.getString(5),row.getString(6),row.getString(7),row.getString(8),row.getString(9),
                        row.getString(10),"",row.getString(11),"","",row.getString(12),
                        "","",row.getString(13));

                return userEvent2SeedRecomender(userEvent);
            }
        });
       DataFrame seedDf= hiveContext.createDataFrame(seedRdd, SeedRecomenderBean.class);
        seedDf.registerTempTable("seedRecomender");
        return seedDf;
    }

    /**
     * 将用户行为对象转化为SeedRecomenderBean对象
     * @param eventlog
     * @return
     */
    private SeedRecomenderBean userEvent2SeedRecomender(FactSrcUserEvent eventlog){
        String openId=eventlog.getUSER_ID();
        String time=eventlog.getTIME();
        String labelStr=eventlog.getLABEL();
        String label=labelStr.replaceAll("\"","");

//        System.out.println("label:"+label);
        //取得label中的参数
        Map<String,String> labelMap=getLabelParameter(label);
//        String productName = labelMap.get("lrtName");
//        String productId = labelMap.get("lrtID");
        String unionid = labelMap.get("unionid");
        String productId = labelMap.get("productId");
        String productName=labelMap.get("productName");
//        String lrtName = labelMap.get("lrtName");
//        String referId = labelMap.get("referId");

        SeedRecomenderBean seedBean=new SeedRecomenderBean();
        seedBean.setOpenId(openId);
        seedBean.setProductId(productId);
        seedBean.setProductName(productName);
        seedBean.setUnionid(unionid);
        seedBean.setVisitTime(time);
        seedBean.setVisitPage("true");
        return seedBean;
    }

    /**
     * 按用户产品group by  ，相同用户产品浏览只记一次
     * @param hiveContext
     * @return
     */
    private DataFrame UserEachProductVisit(HiveContext hiveContext){
        String hql="select unionid,productId,productName,openId,visitPage,max(visitTime) as  visitTime from seedRecomender " +
                "group by unionid,productId,productName,openId,visitPage";
        return DataFrameUtil.getDataFrame(hiveContext, hql, "UserEachProductVisit");
    }

    /**
     * 将除unionid ，产品id 外的字段连接到一起。后面聚合。
     * @param hiveContext
     * @return
     */
    private DataFrame contactInfo(HiveContext hiveContext){
        String hql="select unionid,"+
        "concat('{',concat_ws(',',concat_ws(':','\"productId\"',concat('\"',productId,'\"'))," +
                "concat_ws(':','\"openId\"',concat('\"',openId,'\"'))," +
                "concat_ws(':','\"productName\"',concat('\"',productName,'\"'))," +
                "concat_ws(':','\"visitPage\"',concat('\"',visitPage,'\"'))," +
                "concat_ws(':','\"visitTime\"',concat('\"',visitTime,'\"'))" +
                "),'}') as shareinfo from UserEachProductVisit";
        return DataFrameUtil.getDataFrame(hiveContext, hql, "contactInfo");
    }

    /**
     * 聚合得到 推荐人 分享浏览的记录列表
     * @param hiveContext
     * @return
     */
    private DataFrame shareInfo(HiveContext hiveContext){
        String hql="select t.unionid,concat('[',concat_ws(',',collect_set(t.shareinfo)),']') as shareinfolist " +
                "from contactInfo t group by t.unionid";
        return DataFrameUtil.getDataFrame(hiveContext, hql, "RecomenderShareInfo");
    }




    /**
     * 取得label中的参数，放入map
     * @param label
     * @return
     */
    public static Map<String,String> getLabelParameter(String label){
        Map<String, String> map = new HashMap<String, String>();
        String[] temLabel= label.split(",");
        if(temLabel!=null&&temLabel.length>0){
            for (String temk2v : temLabel) {
                String [] kv=temk2v.split(":");
                if(kv!=null&&kv.length>1){
                    String key = kv[0];
                    String value = kv[1];
                    map.put(key, value);
                }
            }
        }
        return map;
    }

    public void saveAsParquet(DataFrame df, String path) {
        TK_DataFormatConvertUtil.deletePath(path);
        df.saveAsParquetFile(path);
    }
}
