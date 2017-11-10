package com.tk.track.fact.sparksql.cfg;

public class DataFrameConfig {
	/*
	 * Map From Hbase To Hive config
	 */

	public final static String DT_DEST_TAIL = "";

//  just for example
//	/*
//	 * Table TAI_USERINFO
//	 */
//	public final static String DT_DEST_TABLE_NAME_TAI_USERINFO = "TAI_USERINFO" + DT_DEST_TAIL;
//	public final static String DT_SRC_TABLE_NAME_TAI_USERINFO = "TAI_USERINFO";
//	public final static String DT_TABLE_FIELD_TAI_USERINFO = " OPEN_ID,USER_NAME,USER_GENDER,USER_CIDNO,USER_CIDTYPE,USER_PHONE,USER_EMAIL,USER_ADDRESS,CREATED_TIME,UPDATED_TIME";
//
//	public final static String DT_DEST_FIELD_STRING_TAI_USERINFO = getDestField(DT_TABLE_FIELD_TAI_USERINFO);
//	public final static String DT_SRC_FIELD_STRING_TAI_USERINFO = getSrcField(DT_TABLE_FIELD_TAI_USERINFO);

	private static String getDestField(String tableField) {
		tableField = tableField.replaceAll(" ", "");
		tableField = tableField.replaceAll(",", " string,");
		tableField += " string";
		return tableField;
	}

	private static String getSrcField(String tableField) {
		tableField = tableField.replaceAll(" ", "");
		tableField = tableField.replaceAll(",", ",info:");
		tableField = "info:" + tableField;
		return tableField;
	}
}
