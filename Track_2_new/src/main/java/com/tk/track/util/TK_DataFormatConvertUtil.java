package com.tk.track.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.tk.track.common.TK_CommonConfig;
import com.tk.track.common.TK_DatabaseValues;

public class TK_DataFormatConvertUtil {	
    
    private static final boolean DEBUG_FLAG = true;
    
    /************ get data from hbase **************/
    static public Date getDateFromResult(Result result, byte[] family, byte[] qualifier) {
        if (DEBUG_FLAG) {
            String str = getStringFromResult(result, family, qualifier);
            if (str != null) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    return sdf.parse(str);
                } catch (ParseException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }
        byte[] bytes = result.getValue(family, qualifier);
        if (bytes != null) {
            long times = Bytes.toLong(bytes);
            if (times != 0) {
                return new Date(times);
            }
        }
        return null;
    }
	static public String getStringFromResult(Result result, byte[] family, byte[] qualifier) {
		byte[] bytes = result.getValue(family, qualifier);
		if (bytes != null) {
			String str = Bytes.toString(bytes);
			if (str.length() > 0) {
				return str;
			}
		}
		return null;
	}
	static public long getLongFromResult(Result result, byte[] family, byte[] qualifier) {
		if (DEBUG_FLAG) {
			String str = getStringFromResult(result, family, qualifier);
			if (str != null) {
				return Long.valueOf(str);
			} else {
				return 0;
			}
		}
		byte[] bytes = result.getValue(family, qualifier);
		if (bytes != null) {
			return Bytes.toLong(bytes);
		}
		return 0;
	}
	static public int getIntFromResult(Result result, byte[] family, byte[] qualifier) {
		if (DEBUG_FLAG) {
			String str = getStringFromResult(result, family, qualifier);
			if (str != null) {
				return Integer.valueOf(str);
			} else {
				return 0;
			}
		}
		byte[] bytes = result.getValue(family, qualifier);
		if (bytes != null) {
			return Bytes.toInt(bytes);
		}
		return 0;
	}
	static public double getDoubleFromResult(Result result, byte[] family, byte[] qualifier) {
		if (DEBUG_FLAG) {
			String str = getStringFromResult(result, family, qualifier);
			if (str != null) {
				return Double.valueOf(str);
			} else {
				return 0;
			}
		}
		byte[] bytes = result.getValue(family, qualifier);
		if (bytes != null) {
			return Bytes.toDouble(bytes);
		}
		return 0;
	}
	
	/************ add data to hbase **************/
	static public void addColumn(Put put, byte[] family, byte[] qualifier, String value) {
		if (value != null && value.length() > 0) {
			put.addColumn(family, qualifier, value.getBytes());
		}
	}
	static public void addColumn(Put put, byte[] family, byte[] qualifier, double value) {
		if (value != 0) {
			if (DEBUG_FLAG) {
				addColumn(put, family, qualifier, String.format("%.1f", value));
			} else {
				put.addColumn(family, qualifier, Bytes.toBytes(value));
			}
		}
	}
	static public void addColumn(Put put, byte[] family, byte[] qualifier, int value) {
		if (value != 0) {
			//put.addColumn(family, qualifier, Bytes.toBytes(value));
			addColumn(put, family, qualifier, Integer.toString(value));
		}
	}
	static public void addColumn(Put put, byte[] family, byte[] qualifier, Date value) {
		if (value != null) {
			// put.addColumn(family, qualifier, Bytes.toBytes(value.getTime()));
			put.addColumn(family, qualifier, date2String(value).getBytes());
		}
	}

	/************ add data to hdfs **************/
	static public void writeData(DataOutput output, Date dt) throws IOException {
		if (dt == null) {
			output.writeLong(0);
		} else {
			output.writeLong(dt.getTime());
		}
	}
	static public void writeData(DataOutput output, String value) throws IOException {
		if (value == null) {
			output.writeUTF("");
		} else {
			output.writeUTF(value);
		}
	}
	static public void writeData(DataOutput output, double value) throws IOException {
		output.writeDouble(value);
	}
	static public void writeData(DataOutput output, int value) throws IOException {
		output.writeInt(value);
	}
	static public void writeData(DataOutput output, long value) throws IOException {
		output.writeLong(value);
	}
	
	/************ read data from hdfs **************/
	static public Date readDate(DataInput input) throws IOException {
		long v = input.readLong();
		if (v == 0) {
			return null;
		} else {
			return new Date(v);
		}
	}
	
	/************ date & string **************/
	static private final String TAIKANG_DATAFORMAT = "yyyy-MM-dd hh:mm:ss";
	static public String date2String(Date date) {
		if (date != null) {
			SimpleDateFormat df = new SimpleDateFormat(TAIKANG_DATAFORMAT);
			return df.format(date);
		} else {
			return null;
		}
	}
	static public Date string2Date(String str) {
		if (str != null) {
			SimpleDateFormat df = new SimpleDateFormat(TAIKANG_DATAFORMAT);
			df.setLenient(false);
			try {
				return df.parse(str);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	static public int string2int(String str) {
		if (str != null && str.length() > 0) {
			try {
				return Integer.valueOf(str);
			} catch (Exception e) {
			}
		}
		return 0;
	}
	
	/*********** output for debug ***********/
	static public void log(String str) {
		System.out.println(str);
	}
	
	public static KeyValue createKeyValue(byte[] rowkey, byte[] familyname, byte[] qualifier, String value) {
		if (value != null && value.length() > 0) {
			return new KeyValue(rowkey, familyname, qualifier, value.getBytes());
		}
		return null;
	}
	
	public static KeyValue createKeyValue(byte[] rowkey, byte[] familyname, byte[] qualifier, int value) {
		if (value != 0) {
			return new KeyValue(rowkey, familyname, qualifier, String.valueOf(value).getBytes());
		}
		return null;
	}
	
	public static KeyValue createKeyValue(byte[] rowkey, byte[] familyname, byte[] qualifier, Date value) {
		if (value != null && value.getTime() != 0) {
			return new KeyValue(rowkey, familyname, qualifier, date2String(value).getBytes());
		}
		return null;
	}
	
	public static KeyValue createKeyValue(byte[] rowkey, byte[] familyname, byte[] qualifier, double value) {
		if (value != 0) {
			return new KeyValue(rowkey, familyname, qualifier, String.valueOf(value).getBytes());
		}
		return null;
	}
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
	
	public static boolean isExistsPath(String path){
		Configuration conf = null;
		FileSystem filesystem = null;
		try {
			conf = new Configuration();
			filesystem = FileSystem.get(conf);
			if (filesystem.exists(new Path(path))) 
				return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
    public static void renamePath(String srcPath, String dstPath){
        Configuration conf = null;
        FileSystem filesystem = null;
        try {
            conf = new Configuration();
            filesystem = FileSystem.get(conf);
            if (filesystem.exists(new Path(srcPath))) {
                filesystem.rename(new Path(srcPath), new Path(dstPath));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
	public static String getSchema() {
		return TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HIVE_HEALTHSCORE_SCHEMA);
	}
	
	public static String getUserBehaviorSchema() {
		return TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HIVE_USERBEHAVIOUR_SCHEMA);
	}
	
	public static String getNetWorkSchema() {
		return TK_CommonConfig.getValue(TK_DatabaseValues.TAIKANG_HIVE_NETWORKTELECOM_SCHEMA);
	}
	
}