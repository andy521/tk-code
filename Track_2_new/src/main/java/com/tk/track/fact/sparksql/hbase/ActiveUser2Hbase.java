package com.tk.track.fact.sparksql.hbase;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

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

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.desttable.MonthActiveUserToHbase;
import com.tk.track.fact.sparksql.util.DMUtility;
import com.tk.track.util.MD5Util;
/**
 * 月活跃用户parquet转hbase
 * @author itw_shanll
 *
 */
public class ActiveUser2Hbase implements Serializable {

	//加载parquet文件
	public DataFrame getDataFrame(HiveContext context){
		String path = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_MONTHACTIVEUSER_OUTPUTPATH);
		DataFrame df = context.load(path);
		return df;
	}
	
	
	 public void loadToHbase(HiveContext context) {
	        DataFrame df = getDataFrame(context);
	        String hfilePath= TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_MONTHACTIVEUSER_TOHBASEPATH);
	        DMUtility.deletePath(hfilePath);

	        df.select("member_id").
	                toJavaRDD().flatMapToPair(new PairFlatMapFunction<Row, String, MonthActiveUserToHbase>() {


	            private static final long serialVersionUID = -866101599388467884L;

	            public Iterable<Tuple2<String, MonthActiveUserToHbase>> call(Row t) throws Exception {
	                List<Tuple2<String, MonthActiveUserToHbase>> list = new ArrayList<>();

	                String memberId = t.getString(0);

	                String rowkey = "";

	                if (StringUtils.isNotBlank(memberId)) {
	                    rowkey =  MD5Util.getMd5(memberId);
	                }else{
	                    rowkey= MD5Util.getMd5("");
	                }

	                MonthActiveUserToHbase bean = new MonthActiveUserToHbase();
	                bean.setMemberId(memberId);
	                list.add(new Tuple2<>(rowkey, bean));
	                return list;
	            }

	        }).groupByKey(32).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<MonthActiveUserToHbase>>, ImmutableBytesWritable, KeyValue>() {

	            private static final long serialVersionUID = -2190849107088990449L;

	            @Override
	            public Iterable<Tuple2<ImmutableBytesWritable, KeyValue>> call(Tuple2<String, Iterable<MonthActiveUserToHbase>> t1) throws Exception {
	                List<Tuple2<ImmutableBytesWritable, KeyValue>> R_list = new ArrayList<>();
	                String rowkey = t1._1();
	                Iterator<MonthActiveUserToHbase> it = t1._2().iterator();
	                

	                while (it.hasNext()) {
	                	MonthActiveUserToHbase onebean = it.next();
	                	String memberid = onebean.getMemberId()==null?"":onebean.getMemberId();
	                	
	                	
	                    R_list.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
	                            new ImmutableBytesWritable(rowkey.getBytes()),
	                            new KeyValue(rowkey.getBytes(),
	                            		TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY).getBytes(),
	                            		("MEMBERID").getBytes(),memberid.getBytes())));
	                    
	                }
	                return R_list;
	            }
	        })
	                .repartitionAndSortWithinPartitions(new MonthFuctionPartitoner(16))
	                .saveAsNewAPIHadoopFile(hfilePath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, HBaseConfiguration.create()); ;
	        String tableName_Pix = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_MONTHACTIVEUSER_TOHBASE_NAME);
	        String timeStamp = getTodayTime(-1);
	        try {
	            DMUtility.bulkloadWithoutDate(hfilePath, tableName_Pix+"_"+timeStamp, getSplitKeys());
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

//	 public void inserConfigTable(){
//		 String tableName_Pix = TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HDFS_MONTHACTIVEUSER_TOHBASE_NAME);
//		 String tableNameTime = tableName_Pix+"_"+TimeUtil.getNowStr("yyyy-MM-dd");
//	 }
	 
	 public static class MonthFuctionPartitoner extends Partitioner implements Serializable{

	        private static final long serialVersionUID = -7561324778474166673L;
	        private int partitions;

	        public MonthFuctionPartitoner(int OPPartitioners){
	            partitions = OPPartitioners;
	        }

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

	        @Override
	        public int numPartitions() {
	            // TODO Auto-generated method stub
	            return partitions;
	        }

	    }
	 /**
	  * 获取日期
	  * @param day
	  * @return
	  */
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
	
}
