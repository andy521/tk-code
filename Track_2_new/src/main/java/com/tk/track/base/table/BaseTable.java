package com.tk.track.base.table;

import java.io.Serializable;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;
import com.tk.track.fact.sparksql.util.HBaseUtil;

public class BaseTable implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6701567949233433108L;

	private String rowKey;
	public static final String FAMILY = "info";

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public static String getString(Result result, String qualifier) {
		return getString(result, FAMILY, qualifier);
	}

	public static String getString(Result result, String family, String qualifier) {
		byte[] b = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		String i = null;
		try {
			i = Bytes.toString(b);
		} catch (Exception e) {

		} finally {
			return i;
		}
	}

	public static Integer getInt(Result result, String qualifier) {
		return getInt(result, FAMILY, qualifier);
	}

	public static Integer getInt(Result result, String family, String qualifier) {
		String str = getString(result, family, qualifier);
		Integer i = null;
		try {
			i = Integer.parseInt(str);
		} catch (Exception e) {

		} finally {
			return i;
		}
	}

	public static byte[] getBytes(String str) {
		if (str != null) {
			return Bytes.toBytes(str);
		}
		return null;
	}
	
	public static void updateHbase(String rowkeyStr,String date){
		//HBASE UPDATE
		String tableName= TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FACTNETTELERESULTINFO);
		byte[] rowkey=Bytes.toBytes(rowkeyStr);
		byte[] family=Bytes.toBytes(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_FAMILY));
		byte[] qualifier=Bytes.toBytes(TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HBASE_QUALIFIER));
		byte[] value=Bytes.toBytes(date);
		HBaseUtil.put(tableName,rowkey,family,qualifier,value);
	}

}
