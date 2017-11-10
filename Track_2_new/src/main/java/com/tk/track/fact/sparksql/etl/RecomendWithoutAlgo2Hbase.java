package com.tk.track.fact.sparksql.etl;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.RecommendWithoutAlgoToHbaseBean;
import com.tk.track.fact.sparksql.main.App;
import com.tk.track.fact.sparksql.util.DMUtility;
import com.tk.track.fact.sparksql.util.DataFrameUtil;
import com.tk.track.util.MD5Util;
import com.tk.track.util.TimeUtil;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
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
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * Created by t-chenhao01 on 2016/11/1.
 */
public class RecomendWithoutAlgo2Hbase implements Serializable {

    private static final long serialVersionUID = 8419589046630614949L;
    private static final String bhSchema = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HIVE_USERBEHAVIOUR_SCHEMA);
    public static final String defaultReNum= TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_DEFAULT_RECOMMENDNUM);


    public void loadToHbase(HiveContext context) {
        DataFrame df = getDataFrame(context);
        String hfilePath=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_TOHBASEPATH);
        DMUtility.deletePath(hfilePath);

        df.select("user_id", "open_id", "member_id", "terminal_id", "wxpinfo", "pcpinfo", "wappinfo", "apppinfo").
                toJavaRDD().flatMapToPair(new PairFlatMapFunction<Row, String, RecommendWithoutAlgoToHbaseBean>() {

            private static final long serialVersionUID = -7992483268543464592L;

            public Iterable<Tuple2<String, RecommendWithoutAlgoToHbaseBean>> call(Row t) throws Exception {
                List<Tuple2<String, RecommendWithoutAlgoToHbaseBean>> list = new ArrayList<>();

                String user_id = t.getString(0);
                String open_id = t.getString(1);
                String member_id = t.getString(2);
                String terminal_id = t.getString(3);
                String wxpinfo = t.getString(4);
                String pcpinfo = t.getString(5);
                String wappinfo = t.getString(6);
                String apppinfo = t.getString(7);

                String rowkey = "";
                if (StringUtils.isNotBlank(user_id)) {
                    rowkey = user_id;
                } else {
                    rowkey = MD5Util.getMd5(terminal_id);
                }

                RecommendWithoutAlgoToHbaseBean bean = new RecommendWithoutAlgoToHbaseBean(user_id, terminal_id, open_id,
                        member_id, wxpinfo, pcpinfo, wappinfo, apppinfo);
                list.add(new Tuple2<String, RecommendWithoutAlgoToHbaseBean>(rowkey, bean));
                return list;
            }

        }).groupByKey(32).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<RecommendWithoutAlgoToHbaseBean>>, ImmutableBytesWritable, KeyValue>() {

            private static final long serialVersionUID = -2190849107088990449L;

            @Override
            public Iterable<Tuple2<ImmutableBytesWritable, KeyValue>> call(Tuple2<String, Iterable<RecommendWithoutAlgoToHbaseBean>> t1) throws Exception {
                List<Tuple2<ImmutableBytesWritable, KeyValue>> R_list = new ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>>();
                String rowkey = t1._1();
                Iterator<RecommendWithoutAlgoToHbaseBean> it = t1._2().iterator();

                while (it.hasNext()) {
                    RecommendWithoutAlgoToHbaseBean onebean = it.next();
                    String user_id = onebean.getUser_id()==null?"":onebean.getUser_id();
                    String open_id = onebean.getOpen_id()==null?"":onebean.getOpen_id();
                    String member_id = onebean.getMember_id()==null?"":onebean.getMember_id();
                    String terminal_id = onebean.getTerminal_id()==null?"":onebean.getTerminal_id();
                    String wxpinfo = onebean.getWechat_product_list()==null?"":onebean.getWechat_product_list();
                    String pcpinfo = onebean.getPc_product_list()==null?"":onebean.getPc_product_list();
                    String wappinfo = onebean.getM_product_list()==null?"":onebean.getM_product_list();
                    String apppinfo = onebean.getApp_product_list()==null?"":onebean.getApp_product_list();


                    R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                            new ImmutableBytesWritable(rowkey.getBytes()),
                            new KeyValue(rowkey.getBytes(),
                                    TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                    ("APP_PRODUCT_LIST").getBytes(), apppinfo.getBytes())));

                    R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                            new ImmutableBytesWritable(rowkey.getBytes()),
                            new KeyValue(rowkey.getBytes(),
                                    TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                    ("MEMBER_ID").getBytes(), member_id.getBytes())));

                    R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                            new ImmutableBytesWritable(rowkey.getBytes()),
                            new KeyValue(rowkey.getBytes(),
                                    TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                    ("M_PRODUCT_LIST").getBytes(), wappinfo.getBytes())));

                    R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                            new ImmutableBytesWritable(rowkey.getBytes()),
                            new KeyValue(rowkey.getBytes(),
                                    TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                    ("OPEN_ID").getBytes(), open_id.getBytes())));

                    R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                            new ImmutableBytesWritable(rowkey.getBytes()),
                            new KeyValue(rowkey.getBytes(),
                                    TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                    ("PC_PRODUCT_LIST").getBytes(), pcpinfo.getBytes())));

                    R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                            new ImmutableBytesWritable(rowkey.getBytes()),
                            new KeyValue(rowkey.getBytes(),
                                    TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                    ("TERMINAL_ID").getBytes(), terminal_id.getBytes())));

                    R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                            new ImmutableBytesWritable(rowkey.getBytes()),
                            new KeyValue(rowkey.getBytes(),
                                    TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                    ("WECHAT_PRODUCT_LIST").getBytes(), wxpinfo.getBytes())));






                }
                return R_list;
            }
        })
        .repartitionAndSortWithinPartitions(new RWithoutAFuctionPartitoner(17))
//                .sortByKey()
        .saveAsNewAPIHadoopFile(hfilePath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, HBaseConfiguration.create()); ;
        String tableName_Pix = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_TOHBASE_Name);
        try {
            DMUtility.bulkload(hfilePath, tableName_Pix,getSplitKeys());
//            DMUtility.bulkloadWithoutDate(hfilePath, tableName_Pix, getSplitKeys());
        } catch (IOException e) {
            e.printStackTrace();
        };
    }
    public void insertDefaultProductRecommend(HiveContext context,String forceResult){
        String defaultStr=getDefaultProductString(context,"total_wx_info");
        String wechatValue=supplementForceInfo(forceResult,defaultStr);
        String wapValue=getDefaultProductString(context,"total_wap_info");
        String PcValue=getDefaultProductString(context,"total_pc_info");
        String AppValue=getDefaultProductString(context,"total_app_info");
        
        //recommondwithoutalgo_result
        String tableName_Pix = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_TOHBASE_Name);
        try {
            
            DMUtility.insert2Table(tableName_Pix, MD5Util.getMd5("111111"),"info","APP_PRODUCT_LIST",AppValue);
            DMUtility.insert2Table(tableName_Pix,MD5Util.getMd5("111111"),"info","MEMBER_ID","111111");
            DMUtility.insert2Table(tableName_Pix,MD5Util.getMd5("111111"),"info","M_PRODUCT_LIST",wapValue);
            DMUtility.insert2Table(tableName_Pix,MD5Util.getMd5("111111"),"info","OPEN_ID","111111");
            DMUtility.insert2Table(tableName_Pix,MD5Util.getMd5("111111"),"info","PC_PRODUCT_LIST",PcValue);
            DMUtility.insert2Table(tableName_Pix,MD5Util.getMd5("111111"),"info","TERMINAL_ID","111111");
            DMUtility.insert2Table(tableName_Pix,MD5Util.getMd5("111111"),"info","WECHAT_PRODUCT_LIST",wechatValue);

            
            String tableNameTime= tableName_Pix+ "_" + TimeUtil.getNowStr("yyyy-MM-dd");
            //HEALTH_SCORE_INFORMATION
            String signTableName=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_SIGN_TABLENAME);
            DMUtility.insert2TableWithoutDate(signTableName, "2", "info", "RECOMEND_RESULT_TABLE", tableNameTime);

//            DMUtility.insert2TableWithoutDate(tableName_Pix,"111111", "info", "APP_PRODUCT_LIST", "1");
//            DMUtility.insert2TableWithoutDate(tableName_Pix, "111111","info", "MEMBER_ID", "1");
//            DMUtility.insert2TableWithoutDate(tableName_Pix, "111111","info", "M_PRODUCT_LIST", "1");
//            DMUtility.insert2TableWithoutDate(tableName_Pix, "111111","info", "OPEN_ID", "1");
//            DMUtility.insert2TableWithoutDate(tableName_Pix,"111111", "info", "PC_PRODUCT_LIST", "1");
//            DMUtility.insert2TableWithoutDate(tableName_Pix, "111111","info", "TERMINAL_ID", "1");
//            DMUtility.insert2TableWithoutDate(tableName_Pix,"111111","info","WECHAT_PRODUCT_LIST",wechatValue);

//            DMUtility.insert2TableWithoutDate(tableName_Pix,"2","info","RECOMEND_RESULT_TABLE",tableName_Pix);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public DataFrame getDataFrame(HiveContext context){
        String path =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_FORCERESULT_PATH);
        String sysdt = App.GetSysDate(0);
        path = path + "-" + sysdt;
        DataFrame df = context.load(path);
        return df;
    }
    public String getDefaultProductString(HiveContext context,String info){
        String path =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGORESULTDEFAULT_OUTPUTPATH);
        String sysdt = App.GetSysDate(0);
        path = path + "-" + sysdt;
        DataFrame df = context.load(path);
        df.registerTempTable("defaultProductDF");
        String result=df.select(info).first().getString(0);
        return result;
    }
    /**
     * 补充人工干涉结果到默认结果里，放在前面
     * @param sqlContext
     * @return
     */
    public String  supplementForceInfo(String str1,String str2){
//        String sql="select " +
//                "contactproductinfo(force.forceproduct,res.total_wx_info) as total_wx_info " +
//                "from defaultProductDF res ,"+bhSchema+"recommend_force force";
//        return DataFrameUtil.getDataFrame(sqlContext, sql, "RecomendSupplementDfaultInfo");

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

    public void loadToHbasePc(HiveContext context) {
        String path=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_SCORE_PC);
//        String path =  "/user/tkonline/taikangtrack/data/recommengwioutalgo_dfwapResult";

        DataFrame df = context.load(path);
        String hfilePath=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_TOHBASEPATH_PC);
        DMUtility.deletePath(hfilePath);

        df.select("user_id", "open_id", "member_id", "terminal_id", "pcpinfo").
                toJavaRDD().flatMapToPair(new PairFlatMapFunction<Row, String, RecommendWithoutAlgoToHbaseBean>() {

            private static final long serialVersionUID = -7992473268543464592L;

            public Iterable<Tuple2<String, RecommendWithoutAlgoToHbaseBean>> call(Row t) throws Exception {
                List<Tuple2<String, RecommendWithoutAlgoToHbaseBean>> list = new ArrayList<>();

                String user_id = t.getString(0);
                String open_id = t.getString(1);
                String member_id = t.getString(2);
                String terminal_id = t.getString(3);
                String pcpinfo = t.getString(4);


                String rowkey = "";
                if (StringUtils.isNotBlank(user_id)) {
                    rowkey = user_id;
                } else {
                    rowkey = MD5Util.getMd5(terminal_id);
                }

                RecommendWithoutAlgoToHbaseBean bean = new RecommendWithoutAlgoToHbaseBean();
                bean.setUser_id(user_id);
                bean.setOpen_id(open_id);
                bean.setMember_id(member_id);
                bean.setTerminal_id(terminal_id);
                bean.setPc_product_list(pcpinfo);
                list.add(new Tuple2<String, RecommendWithoutAlgoToHbaseBean>(rowkey, bean));
                return list;
            }

        }).groupByKey(32).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<RecommendWithoutAlgoToHbaseBean>>, ImmutableBytesWritable, KeyValue>() {

            private static final long serialVersionUID = -2190849107088990449L;

            @Override
            public Iterable<Tuple2<ImmutableBytesWritable, KeyValue>> call(Tuple2<String, Iterable<RecommendWithoutAlgoToHbaseBean>> t1) throws Exception {
                List<Tuple2<ImmutableBytesWritable, KeyValue>> R_list = new ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>>();
                String rowkey = t1._1();
                Iterator<RecommendWithoutAlgoToHbaseBean> it = t1._2().iterator();

                while (it.hasNext()) {
                    RecommendWithoutAlgoToHbaseBean onebean = it.next();
                    String user_id = onebean.getUser_id();
                    String open_id = onebean.getOpen_id();
                    String member_id = onebean.getMember_id();
                    String terminal_id = onebean.getTerminal_id();
                    String pcpinfo = onebean.getPc_product_list();

                    if(member_id!=null && !"".equals(member_id.trim())){
                        R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                                new ImmutableBytesWritable(rowkey.getBytes()),
                                new KeyValue(rowkey.getBytes(),
                                        TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                        ("MEMBER_ID").getBytes(), member_id.getBytes())));
                    }

                    if(open_id!=null && !"".equals(open_id.trim())){
                        R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                                new ImmutableBytesWritable(rowkey.getBytes()),
                                new KeyValue(rowkey.getBytes(),
                                        TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                        ("OPEN_ID").getBytes(), open_id.getBytes())));
                    }

                        R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                                new ImmutableBytesWritable(rowkey.getBytes()),
                                new KeyValue(rowkey.getBytes(),
                                        TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                        ("PC_PRODUCT_LIST").getBytes(), pcpinfo.getBytes())));

                   

                        R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                                new ImmutableBytesWritable(rowkey.getBytes()),
                                new KeyValue(rowkey.getBytes(),
                                        TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                        ("TERMINAL_ID_PC").getBytes(), terminal_id.getBytes())));
                }
                return R_list;
            }
        })
                .repartitionAndSortWithinPartitions(new RWithoutAFuctionPartitoner(17))
//                .sortByKey()
                .saveAsNewAPIHadoopFile(hfilePath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, HBaseConfiguration.create()); ;
        String tableName_Pix = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_TOHBASE_Name);
        try {
        	//创建hbase表 ，上线的时候这个地方一定要改
//        	DMUtility.bulkload(hfilePath, tableName_Pix,getSplitKeys());
            //新增数据
        	DMUtility.bulkloadAppend(hfilePath, tableName_Pix,getSplitKeys());
//            DMUtility.bulkloadWithoutDate(hfilePath, tableName_Pix, getSplitKeys());
        } catch (IOException e) {
            e.printStackTrace();
        };
    }

    
    public void loadToHbaseWap(HiveContext context) {
        String path=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_SCORE_WAP);
//        String path =  "/user/tkonline/taikangtrack/data/recommengwioutalgo_dfwapResult";

        DataFrame df = context.load(path);
        String hfilePath=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_TOHBASEPATH_WAP);
        DMUtility.deletePath(hfilePath);

        df.select("user_id", "open_id", "member_id", "terminal_id", "wappinfo").
                toJavaRDD().flatMapToPair(new PairFlatMapFunction<Row, String, RecommendWithoutAlgoToHbaseBean>() {

            private static final long serialVersionUID = -7992483268543464592L;

            public Iterable<Tuple2<String, RecommendWithoutAlgoToHbaseBean>> call(Row t) throws Exception {
                List<Tuple2<String, RecommendWithoutAlgoToHbaseBean>> list = new ArrayList<>();

                String user_id = t.getString(0);
                String open_id = t.getString(1);
                String member_id = t.getString(2);
                String terminal_id = t.getString(3);

                String wappinfo = t.getString(4);


                String rowkey = "";
                if (StringUtils.isNotBlank(user_id)) {
                    rowkey = user_id;
                } else {
                    rowkey = MD5Util.getMd5(terminal_id);
                }

                RecommendWithoutAlgoToHbaseBean bean = new RecommendWithoutAlgoToHbaseBean();
                bean.setUser_id(user_id);
                bean.setOpen_id(open_id);
                bean.setMember_id(member_id);
                bean.setTerminal_id(terminal_id);
                bean.setM_product_list(wappinfo);
                list.add(new Tuple2<String, RecommendWithoutAlgoToHbaseBean>(rowkey, bean));
                return list;
            }

        }).groupByKey(32).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<RecommendWithoutAlgoToHbaseBean>>, ImmutableBytesWritable, KeyValue>() {

            private static final long serialVersionUID = -2190849107088990449L;

            @Override
            public Iterable<Tuple2<ImmutableBytesWritable, KeyValue>> call(Tuple2<String, Iterable<RecommendWithoutAlgoToHbaseBean>> t1) throws Exception {
                List<Tuple2<ImmutableBytesWritable, KeyValue>> R_list = new ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>>();
                String rowkey = t1._1();
                Iterator<RecommendWithoutAlgoToHbaseBean> it = t1._2().iterator();

                while (it.hasNext()) {
                    RecommendWithoutAlgoToHbaseBean onebean = it.next();
                    String user_id = onebean.getUser_id();
                    String open_id = onebean.getOpen_id();
                    String member_id = onebean.getMember_id();
                    String terminal_id = onebean.getTerminal_id();
                    String wappinfo = onebean.getM_product_list();

                    if(member_id!=null && !"".equals(member_id.trim())){
                        R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                                new ImmutableBytesWritable(rowkey.getBytes()),
                                new KeyValue(rowkey.getBytes(),
                                        TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                        ("MEMBER_ID").getBytes(), member_id.getBytes())));
                    }


                        R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                                new ImmutableBytesWritable(rowkey.getBytes()),
                                new KeyValue(rowkey.getBytes(),
                                        TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                        ("M_PRODUCT_LIST").getBytes(), wappinfo.getBytes())));

                    if(open_id!=null && !"".equals(open_id.trim())){
                        R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                                new ImmutableBytesWritable(rowkey.getBytes()),
                                new KeyValue(rowkey.getBytes(),
                                        TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                        ("OPEN_ID").getBytes(), open_id.getBytes())));
                    }


                        R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                                new ImmutableBytesWritable(rowkey.getBytes()),
                                new KeyValue(rowkey.getBytes(),
                                        TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                        ("TERMINAL_ID_WAP").getBytes(), terminal_id.getBytes())));



                }
                return R_list;
            }
        })
                .repartitionAndSortWithinPartitions(new RWithoutAFuctionPartitoner(17))
//                .sortByKey()
                .saveAsNewAPIHadoopFile(hfilePath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, HBaseConfiguration.create()); ;
        String tableName_Pix = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_TOHBASE_Name);
        try {
            DMUtility.bulkloadAppend(hfilePath, tableName_Pix,getSplitKeys());
//            DMUtility.bulkloadWithoutDate(hfilePath, tableName_Pix, getSplitKeys());
        } catch (IOException e) {
            e.printStackTrace();
        };
    }

    
    public void loadToHbaseApp(HiveContext context) {
        String path=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_SCORE_APP);
//        String path =  "/user/tkonline/taikangtrack/data/recommengwioutalgo_dfAppResult";

        DataFrame df = context.load(path);
        String hfilePath=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_TOHBASEPATH_APP);
        // /user/tkonline/taikangtrack_test/data/recommondwithoutalgo_tohbase_app
        DMUtility.deletePath(hfilePath);

        df.select("user_id", "open_id", "member_id", "terminal_id", "appinfo").
                toJavaRDD().flatMapToPair(new PairFlatMapFunction<Row, String, RecommendWithoutAlgoToHbaseBean>() {

            private static final long serialVersionUID = -7992473268543464592L;

            public Iterable<Tuple2<String, RecommendWithoutAlgoToHbaseBean>> call(Row t) throws Exception {
                List<Tuple2<String, RecommendWithoutAlgoToHbaseBean>> list = new ArrayList<>();

                String user_id = t.getString(0);
                String open_id = t.getString(1);
                String member_id = t.getString(2);
                String terminal_id = t.getString(3);
                String appinfo = t.getString(4);


                String rowkey = "";
                if (StringUtils.isNotBlank(user_id)) {
                    rowkey = user_id;
                } else {
                    rowkey = MD5Util.getMd5(terminal_id);
                }

                RecommendWithoutAlgoToHbaseBean bean = new RecommendWithoutAlgoToHbaseBean();
                bean.setUser_id(user_id);
                bean.setOpen_id(open_id);
                bean.setMember_id(member_id);
                bean.setTerminal_id(terminal_id);
                bean.setApp_product_list(appinfo);
                list.add(new Tuple2<String, RecommendWithoutAlgoToHbaseBean>(rowkey, bean));
                return list;
            }

        }).groupByKey(32).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<RecommendWithoutAlgoToHbaseBean>>, ImmutableBytesWritable, KeyValue>() {

            private static final long serialVersionUID = -2190849107088990449L;

            @Override
            public Iterable<Tuple2<ImmutableBytesWritable, KeyValue>> call(Tuple2<String, Iterable<RecommendWithoutAlgoToHbaseBean>> t1) throws Exception {
                List<Tuple2<ImmutableBytesWritable, KeyValue>> R_list = new ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>>();
                String rowkey = t1._1();
                Iterator<RecommendWithoutAlgoToHbaseBean> it = t1._2().iterator();

                while (it.hasNext()) {
                    RecommendWithoutAlgoToHbaseBean onebean = it.next();
                    String user_id = onebean.getUser_id();
                    String open_id = onebean.getOpen_id();
                    String member_id = onebean.getMember_id();
                    String terminal_id = onebean.getTerminal_id();
                    String appinfo = onebean.getApp_product_list();

                    R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                            new ImmutableBytesWritable(rowkey.getBytes()),
                            new KeyValue(rowkey.getBytes(),
                                    TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                    ("APP_PRODUCT_LIST").getBytes(), appinfo.getBytes())));

                    
                    
                    if(member_id!=null && !"".equals(member_id.trim())){
                        R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                                new ImmutableBytesWritable(rowkey.getBytes()),
                                new KeyValue(rowkey.getBytes(),
                                        TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                        ("MEMBER_ID").getBytes(), member_id.getBytes())));
                    }

                    if(open_id!=null && !"".equals(open_id.trim())){
                        R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                                new ImmutableBytesWritable(rowkey.getBytes()),
                                new KeyValue(rowkey.getBytes(),
                                        TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                        ("OPEN_ID").getBytes(), open_id.getBytes())));
                    }

                      
                   

                        R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                                new ImmutableBytesWritable(rowkey.getBytes()),
                                new KeyValue(rowkey.getBytes(),
                                        TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                        ("TERMINAL_ID_APP").getBytes(), terminal_id.getBytes())));
                }
                return R_list;
            }
        })
                .repartitionAndSortWithinPartitions(new RWithoutAFuctionPartitoner(17))
//                .sortByKey()
                .saveAsNewAPIHadoopFile(hfilePath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, HBaseConfiguration.create()); ;
        String tableName_Pix = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_TOHBASE_Name);
        try {
        	//创建hbase表 ，上线的时候这个地方一定要改
//        	DMUtility.bulkload(hfilePath, tableName_Pix,getSplitKeys());
            //新增数据
        	DMUtility.bulkloadAppend(hfilePath, tableName_Pix,getSplitKeys());
//            DMUtility.bulkloadWithoutDate(hfilePath, tableName_Pix, getSplitKeys());
        } catch (IOException e) {
            e.printStackTrace();
        };
    }


    public byte[][] getSplitKeys(){
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

    public static class RWithoutAFuctionPartitoner extends Partitioner implements Serializable{

        private static final long serialVersionUID = -5697564408603445108L;
        private int partitions;

        public RWithoutAFuctionPartitoner(int OPPartitioners){
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
