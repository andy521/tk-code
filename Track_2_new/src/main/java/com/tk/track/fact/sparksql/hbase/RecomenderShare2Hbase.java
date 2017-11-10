package com.tk.track.fact.sparksql.hbase;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.SeedRecommenderToHbase;
import com.tk.track.fact.sparksql.desttable.SeedRecommenderToHbase;
import com.tk.track.fact.sparksql.main.App;
import com.tk.track.fact.sparksql.util.DMUtility;
import com.tk.track.util.MD5Util;
import com.tk.track.util.TimeUtil;
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
import java.util.*;

/**
 * Created by itw_chenhao01 on 2017/8/16.
 */
public class RecomenderShare2Hbase implements Serializable {
    private static final long serialVersionUID = 174661169873281608L;


    public DataFrame getDataFrame(HiveContext context){
        String path =  TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMENDERSHARE_OUTPUTPATH);
        DataFrame df = context.load(path);
        return df;
    }
    // unionid,productId,openId,visitPage,visitTime
    public void loadToHbase(HiveContext context) {
        DataFrame df = getDataFrame(context);
        String hfilePath= TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMENDERSHARE_TOHBASEPATH);
        DMUtility.deletePath(hfilePath);

        df.select("unionid", "shareinfolist").
                toJavaRDD().flatMapToPair(new PairFlatMapFunction<Row, String, SeedRecommenderToHbase>() {


            private static final long serialVersionUID = -866101599388467884L;

            public Iterable<Tuple2<String, SeedRecommenderToHbase>> call(Row t) throws Exception {
                List<Tuple2<String, SeedRecommenderToHbase>> list = new ArrayList<>();

                String unionid = t.getString(0);
                String shareinfo = t.getString(1);


                String rowkey = "";

                if (StringUtils.isNotBlank(unionid)) {
                    rowkey =  MD5Util.getMd5(unionid);
                }else{
                    rowkey= MD5Util.getMd5("");
                }

                SeedRecommenderToHbase bean = new SeedRecommenderToHbase();
                bean.setUnionid(unionid);
                bean.setShareinfo(shareinfo);
                list.add(new Tuple2<>(rowkey, bean));
                return list;
            }

        }).groupByKey(32).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<SeedRecommenderToHbase>>, ImmutableBytesWritable, KeyValue>() {

            private static final long serialVersionUID = -2190849107088990449L;

            @Override
            public Iterable<Tuple2<ImmutableBytesWritable, KeyValue>> call(Tuple2<String, Iterable<SeedRecommenderToHbase>> t1) throws Exception {
                List<Tuple2<ImmutableBytesWritable, KeyValue>> R_list = new ArrayList<>();
                String rowkey = t1._1();
                Iterator<SeedRecommenderToHbase> it = t1._2().iterator();

                while (it.hasNext()) {
                    SeedRecommenderToHbase onebean = it.next();
                    String unionid = onebean.getUnionid()==null?"":onebean.getUnionid();
                    String shareinfo = onebean.getShareinfo()==null?"":onebean.getShareinfo();



                    R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                            new ImmutableBytesWritable(rowkey.getBytes()),
                            new KeyValue(rowkey.getBytes(),
                                    TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                    ("SHAREINFO").getBytes(), shareinfo.getBytes())));

                    R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
                            new ImmutableBytesWritable(rowkey.getBytes()),
                            new KeyValue(rowkey.getBytes(),
                                    TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
                                    ("UNIONID").getBytes(), unionid.getBytes())));
                }
                return R_list;
            }
        })
                .repartitionAndSortWithinPartitions(new SeedShareFuctionPartitoner(16))
//                .sortByKey()
                .saveAsNewAPIHadoopFile(hfilePath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, HBaseConfiguration.create()); ;
        String tableName_Pix = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMENDERSHARE_TOHBASE_NAME);
        try {
//            DMUtility.bulkload(hfilePath, tableName_Pix,getSplitKeys());
            DMUtility.bulkloadWithoutDate(hfilePath, tableName_Pix, getSplitKeys());
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

    /**
     * 得到一个指定长度的随机数字字符串
     * @param strLength
     * @return
     */
    private String getRandomStr(int strLength){
        StringBuffer buffer=new StringBuffer();
        Random random=new Random();
        for(int i=0;i<strLength;i++){
            Integer a=random.nextInt(10);
            buffer.append(a.toString());
        }
        return buffer.toString();
    }


    public void inserConfigTable(){
        String tableName_Pix = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMENDERSHARE_TOHBASE_NAME);
        String tableNameTime= tableName_Pix+ "_" + TimeUtil.getNowStr("yyyy-MM-dd");
        //HEALTH_SCORE_INFORMATION
        String signTableName=TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_RECOMMONDWITHOUTALGO_SIGN_TABLENAME);
        try {
            DMUtility.insert2TableWithoutDate(signTableName, "4", "info", "SEEDSSHARING_RESULT_TABLE", tableNameTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class SeedShareFuctionPartitoner extends Partitioner implements Serializable{

        private static final long serialVersionUID = -7561324778474166673L;
        private int partitions;

        public SeedShareFuctionPartitoner(int OPPartitioners){
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
