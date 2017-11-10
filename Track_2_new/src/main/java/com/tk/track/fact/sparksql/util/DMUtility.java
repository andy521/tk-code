package com.tk.track.fact.sparksql.util;


import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_Constants;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DMUtility {	
	
	public static void deletePath(String output) {
		Configuration conf = new Configuration();
		FileSystem filesystem;
		try {
			filesystem = FileSystem.get(conf);
			if (filesystem.exists(new Path(output))) {
				filesystem.delete(new Path(output), true);
			}		
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static boolean isExistsPath(String output) {
		boolean ret = false;
		Configuration conf = new Configuration();
		FileSystem filesystem;
		try {
			filesystem =  FileSystem.get(conf);
			if (filesystem.exists(new Path(output))) {
				ret = true;
			}	
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ret;
	}

	public static double getDiffFromToday(String date) {
		Date current = new Date();
		double delta = Math.floor((double)(current.getTime() - string2SimDate(date).getTime())/(24*60*60*1000));
		if (delta < 1.0) {
			delta = 1.0;
		}
		return delta;
	}
	static private final String TAIKANG_SIMPLE_DATEFORMAT = "yyyy-MM-dd";
	static public Date string2SimDate(String str) {
		if (str != null && str.length() > 0) {
			if (str.length() > TAIKANG_SIMPLE_DATEFORMAT.length()) {
				str = str.substring(0, TAIKANG_SIMPLE_DATEFORMAT.length());
			}
			SimpleDateFormat df = getSimpleDateFormat(TAIKANG_SIMPLE_DATEFORMAT);
			try {
				return df.parse(str);
			} catch (ParseException e) {
			}
		}
		return null;
	}
	
	static public SimpleDateFormat getSimpleDateFormat(String format) {
		SimpleDateFormat df = new SimpleDateFormat(format);
		TimeZone zone = TimeZone.getTimeZone("GMT+8:00");
		df.setTimeZone(zone);
		df.setLenient(false);
		return df;
	}
	
	public static String getDatePathName(String path, int afterOfDays) {
		Calendar cal = Calendar.getInstance();
		if (afterOfDays != 0) {
			cal.add(Calendar.DAY_OF_YEAR, afterOfDays);
		}
		String strdt = formatDate(cal.getTime());
		return path + "_" + strdt;
	}
	
	
	public static void bulkload(String hfilePath, String tableName, byte[][] splits) throws IOException {
		chmod(hfilePath, "777");
		deleteHBaseTable(tableName + "_" + TimeUtil.getBYStr());
		deleteHBaseTable(tableName + "_" + TimeUtil.getNowStr("yyyy-MM-dd"));
		createHBaseTable(tableName + "_" + TimeUtil.getNowStr("yyyy-MM-dd"), TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY), splits);
		Configuration conf = getHbaseConfiguration();
        HTable table = new HTable(conf, tableName + "_" + TimeUtil.getNowStr("yyyy-MM-dd"));
        LoadIncrementalHFiles loader;
		try {
			loader = new LoadIncrementalHFiles(conf);
			loader.doBulkLoad(new Path(hfilePath), table);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void bulkloadAppend(String hfilePath, String tableName, byte[][] splits) throws IOException {
		chmod(hfilePath, "777");
		Configuration conf = getHbaseConfiguration();
		HTable table = new HTable(conf, tableName + "_" + TimeUtil.getNowStr("yyyy-MM-dd"));
		LoadIncrementalHFiles loader;
		try {
			loader = new LoadIncrementalHFiles(conf);
			loader.doBulkLoad(new Path(hfilePath), table);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void bulkloadWithoutDate(String hfilePath, String tableName, byte[][] splits) throws IOException {
		chmod(hfilePath, "777");
		deleteHBaseTable(tableName);
		createHBaseTable(tableName, TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY), splits);
		Configuration conf = getHbaseConfiguration();
		HTable table = new HTable(conf, tableName);
		LoadIncrementalHFiles loader;
		try {
			loader = new LoadIncrementalHFiles(conf);
			loader.doBulkLoad(new Path(hfilePath), table);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static Configuration getHbaseConfiguration() {
		Configuration conf = HBaseConfiguration.create();
    	conf.set(TK_Constants.TAIKANG_HBASE_ZOOKEEPER_QUORUM, TK_CommonConfig.getValue(TK_Constants.TAIKANG_HBASE_ZOOKEEPER_QUORUM));
		return conf;
	}
	
	public static void chmod(String path, String mod) {
		Configuration conf = new Configuration();
		FsShell shell = new FsShell(conf);
		try {
			shell.run(new String[] { "-chmod", "-R", mod, path });
		} catch (Exception e) {
		}
	}
	public static void deleteHBaseTable(String tablename) {
		Configuration conf = getHbaseConfiguration();
		try {
        	Connection connection = ConnectionFactory.createConnection(conf);
        	Admin admin = connection.getAdmin();
        	TableName table = TableName.valueOf(tablename);
        	if (admin.isTableAvailable(table)) {
        		System.out.println("delete table:" + tablename);
        	    admin.disableTable(table);
        	    admin.deleteTable(table);
        	}
        	admin.close();
        	connection.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }
	}
	public static void createHBaseTable(String tablename, String familyname, byte[][] splits) {
		Configuration conf = getHbaseConfiguration();
		try {
        	Connection connection = ConnectionFactory.createConnection(conf);
        	Admin admin = connection.getAdmin();
        	if (!admin.isTableAvailable(TableName.valueOf(tablename))) {
        		System.out.println("create table:" + tablename);
        	    HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(tablename));
        	    hbaseTable.addFamily(new HColumnDescriptor(familyname));
        	    if (splits != null) {
        	    	admin.createTable(hbaseTable, splits);
        	    } else {
        	    	admin.createTable(hbaseTable);
        	    }
        	}
        	admin.close();
        	connection.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }
	}
	public static void insert2Table(String tableName,String rowkey,String family,String qualifier,String value) throws IOException{
		Configuration conf = getHbaseConfiguration();
		HTable table = new HTable(conf, tableName + "_" + TimeUtil.getNowStr("yyyy-MM-dd"));
		Put put =new Put(rowkey.getBytes());
		put.add(family.getBytes(),qualifier.getBytes(), value.getBytes());
		table.put(put);
	}
	public static void insert2TableWithoutDate(String tableName,String rowkey,String family,String qualifier,String value) throws IOException{
		Configuration conf = getHbaseConfiguration();
		HTable table = new HTable(conf, tableName);
		Put put =new Put(rowkey.getBytes());
		put.add(family.getBytes(),qualifier.getBytes(), value.getBytes());
		table.put(put);
	}
	
	public static String formatDate(Date dt) {
		SimpleDateFormat sdf = getSimpleDateFormat(TAIKANG_SIMPLE_DATEFORMAT);
		return sdf.format(dt);
	}
	
}