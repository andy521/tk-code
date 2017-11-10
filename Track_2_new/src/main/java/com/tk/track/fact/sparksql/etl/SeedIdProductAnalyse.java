package com.tk.track.fact.sparksql.etl;


import com.tk.track.fact.sparksql.desttable.FactSrcUserEvent;
import com.tk.track.fact.sparksql.desttable.PtradeReferId;
import com.tk.track.fact.sparksql.desttable.SeedIdProduct;
import com.tk.track.fact.sparksql.main.App;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.fact.sparksql.util.XMLStrParse;
import com.tk.track.util.TK_DataFormatConvertUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by t-chenhao01 on 2016/9/13.
 * 分析坐席种子链接点击
 */
public class SeedIdProductAnalyse implements Serializable {
    private static final long serialVersionUID = 2344332376733456689L;
    public static final int CACHETABLE_PARQUET = 3;

    /**
     * 得到页面加载的事件子集，来找到包含了通过点种子链接进入的事件。
     * @param sqlContext
     * @return
     */
    public DataFrame getExtPageLoadDF(HiveContext sqlContext) {
        String hql = "select * from TMP_UBA_LOG_EVENT where " +
                "event <>'page.load' AND event <> 'page.unload' ";

        return DataFrameUtil.getDataFrame(sqlContext, hql, "TMP_PAGE_LOAD");
    }

    public DataFrame analyseSeedIdProduct(DataFrame df,HiveContext hiveContext) {
        JavaRDD<Row> jRDD= df.select("ROWKEY", "IP", "TIME", "APP_TYPE","APP_ID","USER_ID","PAGE",
                "EVENT", "SUBTYPE", "LABEL", "CUSTOM_VAL", "CURRENTURL", "TERMINAL_ID","FROM_ID","JPUSHREGISTRATIONID","LOCATION").rdd().toJavaRDD();
//        long sum=jRDD.count();
//        System.out.println("sum:"+sum);
        JavaRDD<SeedIdProduct> resultRdd=jRDD.filter(new Function<Row, Boolean>() {
            //过滤掉 currentUrl 中不包含seedId和seedType的 及 label中不包含产品信息的
            public Boolean call(Row v1) throws Exception {
                String label = v1.getString(9);
                String currentUrl = v1.getString(11);
//                System.out.println("currentUrl:"+currentUrl);

                if ((v1.getString(7) == null) || v1.getString(7).equals("") || v1.getString(7).isEmpty() ||
                        (label == null) || label.equals("") || label.isEmpty() ||
                        (currentUrl == null) || currentUrl.equals("") || currentUrl.isEmpty()) {
                    return false;
                } else {
                    label=label.replaceAll("\"", "");
//                    System.out.println("label:"+label);
                    //取得label中的参数
                    Map<String,String> labelMap=getLabelParameter(label);
                    String productName = labelMap.get("productName");
                    String productId = labelMap.get("productId");
                    String seedType = labelMap.get("seedType");
                    String seedId = labelMap.get("seedId");

                    boolean containProductInfo =(productName!=null) &&(productId!=null) ;
                    boolean containSeedId=(seedType!=null) &&(seedId!=null);

                    if (!containProductInfo||!containSeedId) {
                        return false;
                    }
                }
                return true;
            }
        }).map(new Function<Row, SeedIdProduct>() {
            //将原始日志对象转换为种子链接统计对象
            public SeedIdProduct call(Row v1) throws Exception {

                FactSrcUserEvent userEvent=   new FactSrcUserEvent(v1.getString(0),v1.getString(1),v1.getString(2),v1.getString(3),
                        v1.getString(4),v1.getString(5),v1.getString(6),v1.getString(7),v1.getString(8),v1.getString(9),
                        v1.getString(10),"",v1.getString(11),"","",v1.getString(12),
                       "","",v1.getString(13));
//                System.out.println("user event object:"+userEvent);
                //转换
                return  userEvent2SeedURL(userEvent);
            }
        });

//        System.out.println("reslutRdd size:"+ resultRdd.count());
        //注册临时表
        DataFrame seedDf=hiveContext.createDataFrame(resultRdd, SeedIdProduct.class);
        seedDf.registerTempTable("temp_detail_seedUrl");

        return getSeedUrlFinal(hiveContext);
    }

    /**
     * 生成最终表格
     * @param sqlContext
     * @return
     * SEED_ID	SEED_TYPE	SEED_URL	PRODUCT_ID	PRODUCT_NAME
     * CLICK_DAY	按日期生成统计数据，推数表后
     * CLICK_NUM
     * PRO_SEED_ID	PRO_PRODUCT_ID	ORDER_DAY	ORDER_NUM	DEAL_SEED_ID
     * DEAL_PRODUCT_ID	DEAL_DAY	DEAL_NUM
     */
    private DataFrame getSeedUrlFinal(HiveContext sqlContext){
        getSeedStatisticsInfo(sqlContext);
        getOrderStatisticsInfo(sqlContext);
        getDealStatisticsInfo(sqlContext);

        String hql="select seed_statistics_info.seed_id,seed_statistics_info.seed_type,seed_statistics_info.product_id,"
        		+ "seed_statistics_info.product_name,seed_statistics_info.seed_url,seed_statistics_info.click_day,seed_statistics_info.click_num, "
        		+ "seed_statistics_info.click_day as order_day,order_statistics_info.order_num,seed_statistics_info.click_day as deal_day,deal_statistics_info.deal_num "
        		+ " from seed_statistics_info left join order_statistics_info " +
                "on seed_statistics_info.seed_id =order_statistics_info.pro_seed_id " +
                "and seed_statistics_info.product_id=order_statistics_info.pro_product_id " +
                "and seed_statistics_info.click_day=order_statistics_info.order_day " +
                "left join deal_statistics_info " +
                "on seed_statistics_info.seed_id =deal_statistics_info.deal_seed_id " +
                "and seed_statistics_info.product_id=deal_statistics_info.deal_product_id " +
                "and seed_statistics_info.click_day=deal_statistics_info.deal_day ";

         return DataFrameUtil.getDataFrame(sqlContext, hql, "SeedUrlFinal");
    }
    private void getSeedStatisticsInfo(HiveContext sqlContext){
//        String hql = "select seed_id,seed_type, product_id,product_name, seed_url,cast(from_unixtime(cast((click_time)/1000 as bigint),'yyyy-MM-dd') as string) click_day,cast(count(*) as string) click_num " +
//                "from temp_detail_seedUrl F " +
//                "group by f.seed_id, f.product_id, from_unixtime(cast((click_time)/1000 as bigint),'yyyy-MM-dd'),f.seed_type,f.product_name,f.seed_url ";
//       
        String hql="select seed_id," + 
        		"       seed_type," + 
        		"       product_id," + 
        		"       product_name," + 
        		"       seed_url," + 
        		"       click_day," + 
        		"       cast(count(*) as string) click_num " + 
        		"from ( " + 
        		"select seed_id," + 
        		"       seed_type," + 
        		"       product_id," + 
        		"       product_name," + 
        		"       seed_url," + 
        		"       cast(from_unixtime(cast((click_time) / 1000 as bigint), 'yyyy-MM-dd') as" + 
        		"            string) click_day " + 
        		"  from temp_detail_seedUrl) t1 " + 
        		"  group by seed_id," + 
        		"  seed_type," + 
        		"       product_id," + 
        		"       product_name," + 
        		"       seed_url," + 
        		"       click_day";
        DataFrameUtil.getDataFrame(sqlContext, hql, "seed_statistics_info");
    }
    private void getOrderStatisticsInfo(HiveContext sqlContext){
//        String sql=" select seed_id pro_seed_id,product_id pro_product_id,cast(to_date(created_time) as string) order_day," +
//                "cast(count(*) as string) order_num  " +
//                "from testhive.proposal pro "
//                + "where trim(seed_id) <> '' and trim(seed_id) <> 'null' and trim(product_id) <> '' and trim(product_id) <> 'null' " +
//                "group by pro.seed_id," +
//                "pro.product_id, " +
//                "to_date(created_time)";
    	String sql="select pro_seed_id," + 
    			"       pro_product_id," + 
    			"       order_day," + 
    			"       cast(count(*) as string) order_num " + 
    			"from ( " + 
    			"select seed_id pro_seed_id," + 
    			"       product_id pro_product_id," + 
    			"       cast(to_date(created_time) as string) order_day" + 
    			"  from tkubdb.proposal pro" +
    			" where trim(seed_id) <> ''" + 
    			"   and trim(seed_id) <> 'null'" + 
    			"   and trim(product_id) <> ''" + 
    			"   and trim(product_id) <> 'null'" + 
    			"   ) tt" + 
    			" group by pro_seed_id," + 
    			"          pro_product_id," + 
    			"          order_day";
        DataFrameUtil.getDataFrame(sqlContext, sql, "order_statistics_info");
    }
    private void getDealStatisticsInfo(HiveContext sqlContext){
//        String sql=" select seed_id deal_seed_id,product_id deal_product_id," +
//                "cast(to_date(accepted_time) as string) deal_day," +
//                "cast(count(*) as string) deal_num " +
//                "from tkubdb.policy_qingyun p where trim(seed_id) <> 'null' and trim(product_id) <> '' and trim(product_id) <> 'null' " +
//                "group by p.seed_id," +
//                "p.product_id," +
//                "to_date(accepted_time)";
    	String sql="select deal_seed_id," + 
    			"       deal_product_id," + 
    			"       deal_day," + 
    			"       cast(count(*) as string) deal_num from " + 
    			"(" + 
    			"select seed_id deal_seed_id," + 
    			"       product_id deal_product_id," + 
    			"       cast(to_date(accepted_time) as string) deal_day    " + 
    			"  from tkubdb.policy_qingyun p" +
    			" where trim(seed_id) <> 'null'" + 
    			"   and trim(product_id) <> ''" + 
    			"   and trim(product_id) <> 'null'" + 
    			"  ) tp " + 
    			" group by deal_seed_id,deal_product_id, deal_day";

        DataFrameUtil.getDataFrame(sqlContext, sql, "deal_statistics_info");
    }


    private SeedIdProduct userEvent2SeedURL(FactSrcUserEvent eventlog){
        String currentUrl=eventlog.getCURRENTURL();
        if(currentUrl.contains("?")){
            currentUrl=currentUrl.split("\\?")[0];
        }

        //取得url中的 seedType 和 seedId  a.html?A=xx&B=xx
//        Map<String,String> urlMap= getURLParameter(currentUrl);
//        String seedType= urlMap.get("seedType");
//        String seedId= urlMap.get("seedId");
        
        if(currentUrl.contains("?")){
            currentUrl=currentUrl.split("\\?")[0];
        }

        String time=eventlog.getTIME();
        String labelStr=eventlog.getLABEL();
        String label=labelStr.replaceAll("\"","");

//        System.out.println("label:"+label);
        //取得label中的参数
        Map<String,String> labelMap=getLabelParameter(label);
        String productName = labelMap.get("productName");
        String productId = labelMap.get("productId");
        String seedType = labelMap.get("seedType");
        String seedId = labelMap.get("seedId");

        SeedIdProduct seedProduct = new SeedIdProduct(productId, productName, seedId, seedType, currentUrl, time,"");

        //TODO 如果传来参数不同，需要特殊处理,此处插码参数相同，统一方式取数据
        //商城
//        if("mall".equals(eventlog.getAPP_TYPE())){
//
//
//        }
        return seedProduct;
    }


    /**
     * 新版寿险种子链接，插码中不传seedId seedType.只有referId,
     * 需要 通过reference表查询出来seedId
     * @return
     */
    public DataFrame analyseReferId(HiveContext hiveContext,DataFrame df){
//        String sql="select * from TMP_PAGE_LOAD where  app_id ='clue_H5_mall_001' and event='商品详情' and " +
//                //将无referId的日志记录过滤
//                " instr(currenturl,'referId')<>0 ";

        JavaRDD<Row> jRDD= df.select("ROWKEY", "IP", "TIME", "APP_TYPE","APP_ID","USER_ID","PAGE",
                "EVENT", "SUBTYPE", "LABEL", "CUSTOM_VAL", "CURRENTURL", "TERMINAL_ID","FROM_ID","JPUSHREGISTRATIONID","LOCATION").rdd().toJavaRDD();

        JavaRDD<SeedIdProduct> resultRdd=jRDD.filter(new Function<Row, Boolean>() {
            //过滤掉 currentUrl 中不包含seedId和seedType的 及 label中不包含产品信息的
            public Boolean call(Row v1) throws Exception {
                String label = v1.getString(9);
                String currentUrl = v1.getString(11);
//                System.out.println("currentUrl:"+currentUrl);

                if ((v1.getString(7) == null) || v1.getString(7).equals("") || v1.getString(7).isEmpty() ||
                        (label == null) || label.equals("") || label.isEmpty() ||
                        (currentUrl == null) || currentUrl.equals("") || currentUrl.isEmpty()||(currentUrl.contains("referId")==false)) {
                    return false;
                } else {
                    label=label.replaceAll("\"", "");
//                    System.out.println("label:"+label);
                    //取得label中的参数
                    Map<String,String> labelMap=getLabelParameter(label);
//                    String productName = labelMap.get("productName");
//                    String productId = labelMap.get("productId");
//                    String seedType = labelMap.get("seedType");
//                    String seedId = labelMap.get("seedId");
                    String lrtId = labelMap.get("lrtID");
                    String lrtName = labelMap.get("lrtName");
                    String referId = labelMap.get("referId");

                    boolean containlrtId =(lrtName!=null) &&(lrtId!=null) ;
                    boolean containreferId=(referId!=null);

                    if (!containlrtId||!containreferId) {
                        return false;
                    }
                }
                return true;
            }
        }).map(new Function<Row, SeedIdProduct>() {
            //将原始日志对象转换为种子链接统计对象
            public SeedIdProduct call(Row v1) throws Exception {

                FactSrcUserEvent userEvent = new FactSrcUserEvent(v1.getString(0), v1.getString(1), v1.getString(2), v1.getString(3),
                        v1.getString(4), v1.getString(5), v1.getString(6), v1.getString(7), v1.getString(8), v1.getString(9),
                        v1.getString(10), "", v1.getString(11), "", "", v1.getString(12),
                        "", "", v1.getString(13));
//                System.out.println("user event object:" + userEvent);
                //转换
                return userEvent2ReferIdSeedBean(userEvent);
            }
        });

//        System.out.println("reslutRdd size:"+ resultRdd.count());
        //注册临时表
        DataFrame seedDf=hiveContext.createDataFrame(resultRdd, SeedIdProduct.class);
        seedDf.registerTempTable("temp_detail_seedUrl_referid");
        //TODO
//        saveAsParquet(seedDf, "/user/tkonline/taikangtrack_test/data/seed_referId_log");
//        saveAsParquet(seedDf, "/user/tkonline/taikangtrack/data/seed_referId_log");

        return getReferIdSeedMain(hiveContext);
    }

    /**
     * 新版referid 种子链接统计主流程
     * @param hiveContext
     * @return
     */
    private DataFrame getReferIdSeedMain(HiveContext hiveContext) {
        getSeedIdByReferId(hiveContext);
        getReferIdStatisticsInfo(hiveContext);
        getTradeReferIdSeedId(hiveContext);
        getReferIdOrderInfo(hiveContext);
        getReferIdDelalInfo(hiveContext);
       return  getReferIdFinal(hiveContext);
    }

    /**
     * 新版
     * 日志记录关联reference表找出referId对应的seedId 和seedType
     * @param sqlContext
     * @return
     */
    private DataFrame getSeedIdByReferId(HiveContext sqlContext){
        String sql="select " +
                "     rf.seed_id as seed_id," +
                "       rf.seed_type as seed_type," +
                "dr.product_id," +
                "dr.product_name," +
                "dr.seed_url," +
                "cast(from_unixtime(cast((dr.click_time) / 1000 as bigint), 'yyyy-MM-dd') as string) click_day " +
                "from temp_detail_seedUrl_referid dr inner join tkoldb.reference rf " +
                "on dr.refer_id=rf.refer_id";

        return  DataFrameUtil.getDataFrame(sqlContext, sql, "detail_seedUrl_referid");
    }
    /**
     * 新版
     * 按day聚合日志记录
     */
    private DataFrame getReferIdStatisticsInfo(HiveContext sqlContext){

        String hql="select seed_id," +
                "       seed_type," +
                "       product_id," +
                "       product_name," +
                "       seed_url," +
                "       click_day," +
                "       cast(count(1) as string) click_num " +
                "from detail_seedUrl_referid t1 " +
                "  group by seed_id," +
                "  seed_type," +
                "       product_id," +
                "       product_name," +
                "       seed_url," +
                "       click_day";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "ReferIdStatisticsInfo");
    }

    /**
     * 新版
     * 为统计订单与保单数量，找出seedId
     * 需要用p_trade表中 referid得到seedId，
     * trade_record字段 (xml格式)中种子编号为referid,可以关联reference表找出seedId。
     * 保单表与订单表用tradeid关联。
     * @param hiveContext
     * @return
     */
    private DataFrame getTradeReferIdSeedId(HiveContext hiveContext){
        //TODO 测试用
//        String hql="select pt_id,trade_date,trade_pono,trade_record,trade_id,trade_billno from testhive.p_trade_seedtest " +
        //TODO
        String hql="select pt_id,trade_date,trade_pono,trade_record,trade_id,trade_billno from tkoldb.p_trade " +
                "where unix_timestamp(trade_date)>unix_timestamp('2017-02-23 00:00:00') and trade_record <>'' and trade_record is not null";
//        String hql="select trade_id,paymethod_id,pt_id,agency_id,trade_billno,trade_pono," +
//                "trade_cost,trade_date,trade_succeed,trade_record,trade_signmethod,trade_signature," +
//                "trade_msg,payurl_remark,trade_remark,old_billno,send_status,prdoduct_code,payway_id," +
//                "form_id,operator_id,error_type,error_code,error_desc,notified,merchant_id,ext1,ext2 " +
//                "from tkoldb.p_trade";
        DataFrame pTradeDf= DataFrameUtil.getDataFrame(hiveContext, hql, "p_tradeDf");
        JavaRDD<PtradeReferId> tradereferIdDf=pTradeDf.select("pt_id", "trade_date", "trade_pono", "trade_record","trade_id","trade_billno").
                rdd().toJavaRDD().
                map(new Function<Row, PtradeReferId>() {
                    @Override
                    public PtradeReferId call(Row v1) throws Exception {
                        String lrt_id = v1.getString(0);
                        String trade_date = v1.getString(1);
                        String trade_pono = v1.getString(2);
                        String trade_record = v1.getString(3);
                        String trade_id = v1.getString(4);
                        String trade_billno = v1.getString(5);
                        //解析trace_record （xml格式）得到refer_id
                        String refer_id = XMLStrParse.getSingleByTagName(trade_record, "种子编号");
//                        System.out.println("refer_id:" + refer_id);

                        PtradeReferId ptradeReferid = new PtradeReferId();
                        ptradeReferid.setPt_id(lrt_id);
                        ptradeReferid.setTrade_date(trade_date);
                        ptradeReferid.setTrade_pono(trade_pono);
                        ptradeReferid.setRefer_id(refer_id);
                        ptradeReferid.setTrade_id(trade_id);
                        ptradeReferid.setTrade_billno(trade_billno);
                        return ptradeReferid;
                    }

                    private static final long serialVersionUID = -1278402198168893999L;
                });

        //注册临时表
        DataFrame seedDf=hiveContext.createDataFrame(tradereferIdDf, PtradeReferId.class);
        seedDf.registerTempTable("TradereferId");

        String sql="select rf.seed_id," +
                "rf.seed_type, " +
                "tid.pt_id as product_id, " +
                "cast(to_date(trade_date) as string) order_day," +
                "tid.trade_pono," +
                "tid.trade_id, " +
                "tid.trade_billno " +
                "from " +
                "TradereferId tid inner join tkoldb.reference rf " +
                "on tid.refer_id=rf.refer_id";
        return DataFrameUtil.getDataFrame(hiveContext, sql, "TradeReferIdSeedId",CACHETABLE_PARQUET);
    }

    /**
     * 新版
     * 根据上面生成的TradeReferIdSeedId表格统计 种子订单数量
     * @param hiveContext
     * @return
     */
    private DataFrame getReferIdOrderInfo(HiveContext hiveContext){
        String sql="select seed_id,product_id,order_day," +
                "cast(count(1) as string) order_num  from TradeReferIdSeedId " +
                "where seed_id <>'' and seed_id is not null and " +
                "product_id <>'' and product_id is not null and " +
                "order_day <>'' and order_day is not null " +
                "group by seed_id," +
//                "seed_type," +
                "product_id," +
                "order_day";
        return DataFrameUtil.getDataFrame(hiveContext, sql, "ReferIdOrderInfo");
    }

    /**
     * 新版
     *  根据上面生成的TradeReferIdSeedId表格统计 种子成交数量
     * @param hiveContext
     * @return
     */
    private DataFrame getReferIdDelalInfo(HiveContext hiveContext){
        String sql="select seed_id,product_id,deal_day," +
                "cast(count(trade_pono) as string) deal_num " +
                " from(" +
//                "select tri.seed_id,tri.product_id," +
//                "cast(to_date(plf.lia_accepttime) as string) deal_day  " +
//                //
//
//                " from TradeReferIdSeedId tri inner join tkoldb.p_lifeinsure plf" +
//                " on tri.trade_billno=plf.trade_billno " +
//                "where plf.lia_policyno <>'' and plf.lia_policyno is not null " +
                  "select tri.seed_id,tri.product_id,tri.order_day as deal_day,trade_pono " +
                  " from TradeReferIdSeedId tri where trade_pono is not null and  " +
                  "  order_day is not null and " +
                  "   product_id is not null and " +
                  "  seed_id is not null "+
                ") t " +
                "group by seed_id,product_id,deal_day" ;
        return DataFrameUtil.getDataFrame(hiveContext, sql, "ReferIdDelalInfo");
    }
    private DataFrame getReferIdFinal(HiveContext hiveContext){
        String hql="select sif.seed_id,sif.seed_type,sif.product_id,"
                + "sif.product_name,sif.seed_url,sif.click_day,sif.click_num, "
                + "sif.click_day as order_day,oif.order_num,sif.click_day as deal_day,dif.deal_num "
                + " from ReferIdStatisticsInfo sif left join ReferIdOrderInfo oif " +
                "on sif.seed_id =oif.seed_id " +
                "and sif.product_id=oif.product_id " +
                "and sif.click_day=oif.order_day " +
                "left join ReferIdDelalInfo dif " +
                "on sif.seed_id =dif.seed_id " +
                "and sif.product_id=dif.product_id " +
                "and sif.click_day=dif.deal_day ";

        return DataFrameUtil.getDataFrame(hiveContext, hql, "ReferIdFinal");
    }

    private DataFrame getTotalSeedResult(HiveContext hiveContext) {
        String hql = "select  seed_id,seed_type,product_id,product_name,seed_url,click_day,order_day,deal_day," +
                "cast(sum(cast(click_num as bigint)) as string)  click_num,cast(sum(cast(order_num as bigint)) as string)  order_num," +
                "cast(sum(cast(deal_num as bigint)) as string)  deal_num from " +
                "(select seed_id,seed_type,product_id,product_name,seed_url,click_day,click_num,order_day," +
                "order_num,deal_day,deal_num from SeedUrlFinal " +
                "union all " +
                "select seed_id,seed_type,product_id,product_name,seed_url,click_day,click_num,order_day," +
                "order_num,deal_day,deal_num from ReferIdFinal) t " +
                "group by seed_id,seed_type,product_id,product_name,seed_url,click_day,order_day,deal_day" ;
        return DataFrameUtil.getDataFrame(hiveContext, hql, "TotalSeedResult");
    }

    public DataFrame getSeedIdProductAnalyse(HiveContext hiveContext,DataFrame df){
        analyseSeedIdProduct(df, hiveContext);
        DataFrame fdfi=analyseReferId(hiveContext, df);
        //TODO 测试用，省去analyseReferId步骤
//        saveAsParquet(fdfi, "/user/tkonline/taikangtrack_test/data/seed_referId_final");
//        App.loadParquet(hiveContext, "/user/tkonline/taikangtrack_test/data/seed_referId_final", "ReferIdFinal", false);
        //TODO
        return getTotalSeedResult(hiveContext);
    }


    /**
     * 新版 referId 将日志bean 转化成 SeedIdProduct
     * @param eventlog
     * @return
     */
    private SeedIdProduct userEvent2ReferIdSeedBean(FactSrcUserEvent eventlog){
        String currentUrl=eventlog.getCURRENTURL();
        /*
        currentUrl？referId=22223  referId与每个人有关，做统计时如果链接上有这个参数，
        无法以url 作聚合。
         */
        if(currentUrl.contains("?")){
            currentUrl=currentUrl.split("\\?")[0];
        }

        String time=eventlog.getTIME();
        String labelStr=eventlog.getLABEL();
        String label=labelStr.replaceAll("\"","");

//        System.out.println("label:"+label);
        //取得label中的参数
        Map<String,String> labelMap=getLabelParameter(label);
//        String productName = labelMap.get("lrtName");
//        String productId = labelMap.get("lrtID");
        String seedType = labelMap.get("seedType");//null
        String seedId = labelMap.get("seedId");//null
        String lrtId = labelMap.get("lrtID");
        String lrtName = labelMap.get("lrtName");
        String referId = labelMap.get("referId");

        SeedIdProduct seedProduct = new SeedIdProduct(lrtId, lrtName, seedId, seedType, currentUrl, time,referId);

        //TODO 如果传来参数不同，需要特殊处理,此处插码参数相同，统一方式取数据
        //商城
//        if("mall".equals(eventlog.getAPP_TYPE())){
//
//
//        }
        return seedProduct;
    }



    public void saveAsParquet(DataFrame df, String path) {
        TK_DataFormatConvertUtil.deletePath(path);
        df.saveAsParquetFile(path);
    }

    /**
     * 取得url中的key value，放入map中
     * @param url
     * @return
     */
    public static Map<String,String> getURLParameter(String url){
        Map<String, String> map = new HashMap<String, String>();
        String[] temURL= url.split("?");
        if(temURL!=null&&temURL.length>0){
            String[] temk2v= temURL[1].split("&");
            if(temk2v!=null&&temk2v.length>0) {
                for (String s : temk2v) {
                    String key = s.split("=")[0];
                    String value = s.split("=")[1];
                    map.put(key, value);
                }
            }
        }
        return map;
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

    /**
     * 得到当前时间偏移n天的日期
     * @param day
     * @return
     */
    public static String GetSysDate(int day) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, day);
        String dateStr = new SimpleDateFormat("yyyyMMdd").format(cal.getTime());
        return dateStr;
    }


}
