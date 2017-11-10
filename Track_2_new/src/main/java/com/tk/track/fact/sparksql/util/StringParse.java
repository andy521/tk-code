package com.tk.track.fact.sparksql.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class StringParse {
	public static final String SPILT_ONE = ",";
	public static final String SPILT_TWO = ":";
	public static final String SPILT_THREE = "=";
	public static final String SPILT_FOUR = "&";
	public static final String SPILT_FIVE = "\\|";
	public static final String DEFAULT_COL_SPILT = SPILT_ONE;
	public static final String DEFAULT_KV_SPILT = SPILT_TWO;
	public static final String DEFAULT_SPECIAL_REGEX = "\"|\'|\\||\\t|\\r|\\n|\\{|\\}|/|\\\\";
	public static final boolean DEFAULT_VALUE_DECODE = false;
	public static final boolean DEFAULT_KEY_DECODE = false;
	public static final String DEFAULT_CHARSET = "UTF-8";
	public static final boolean DEFAULT_KEY_REPLACE = true;
	public static final boolean DEFAULT_VALUE_REPLACE = true;
	private String colSpilt;
	private String kvSpilt;
	private String secialKeyRegex;
	private String specialValueRegex;
	private boolean keyDecode;
	private boolean valueDecode;
	private String charSet;
	private boolean keyReplace;
	private boolean valueReplace;
	private Map<String,String> result = null;
	private Map<String,String> ignoreResult = null;
	
	public String get(String key){
		if(result != null)
			return result.get(key);
		return null;
	}
	
	public String getIgnoreCase(String key){
		if(result != null){
			if(ignoreResult == null)
				ignoreResult = converToLowerCaseMap(result);
			if(key != null)
				key = key.toLowerCase();
			return ignoreResult.get(key);
		}
		return null;
	}
	
	private Map<String, String> converToLowerCaseMap(Map<String, String> result) {
		Map<String,String> dataMap = new HashMap<String, String>(result.size());
		String value = null;
		for(String key : result.keySet()){
			value = result.get(key);
			if(key !=  null)
				key = key.toLowerCase();
			dataMap.put(key, value);
		}
		return dataMap;
	}

	public Map<String,String> parse2Map(String src,String colSpilt,String kvSpilt,boolean keyDecode,boolean valueDecode,boolean keyReplace,boolean valueReplace,String secialKeyRegex,String specialValueRegex,String charSet){
		Map<String,String> dataMap = new HashMap<String, String>(20); 
		if(StringUtils.isNotBlank(src)){
			if(StringUtils.isBlank(colSpilt))
				colSpilt = DEFAULT_COL_SPILT;
			if(StringUtils.isBlank(kvSpilt))
				kvSpilt = DEFAULT_KV_SPILT;
			if(StringUtils.isBlank(charSet))
				charSet = DEFAULT_CHARSET;
			String [] keyValues = src.split(colSpilt);
			String [] keyValue = null;
			String key = "";
			String value = "";
			if(keyValues != null && keyValues.length > 0){
				for(String kv : keyValues){
					keyValue = kv.split(kvSpilt);
					if(keyValue != null){
						key = keyValue[0];
						if(keyDecode){
							try {
								key = URLDecoder.decode(key, charSet);
							} catch (UnsupportedEncodingException e) {
								e.printStackTrace();
								key = keyValue[0];
							}
						}
						if(keyValue.length >= 2){
							value = keyValue[1];
							if(valueDecode){
								try {
									value = URLDecoder.decode(value, charSet);
								} catch (UnsupportedEncodingException e) {
									e.printStackTrace();
									value = keyValue[1];
								}
							}
						}
						if(keyReplace){
							if(StringUtils.isBlank(secialKeyRegex))
								secialKeyRegex = DEFAULT_SPECIAL_REGEX;
							key = key.replaceAll(secialKeyRegex, "");
						}
						if(valueReplace){
							if(StringUtils.isBlank(specialValueRegex))
								specialValueRegex = DEFAULT_SPECIAL_REGEX;
							value = value.replaceAll(specialValueRegex, "");
						}
						if(StringUtils.isNotBlank(key)){
							if(StringUtils.isNotBlank(value))
								dataMap.put(key, value);
							else
								dataMap.put(key,"");
						}
						key = "";
						value = "";
					}
				}
			}
		}
		this.setResult(dataMap);
		this.ignoreResult = null;
		return dataMap;
	}
	
	
	
	public StringParse(String colSpilt, String kvSpilt, String secialKeyRegex, String specialValueRegex,
			boolean keyDecode, boolean valueDecode,String charSet,boolean keyReplace, boolean valueReplace) {
		super();
		this.colSpilt = colSpilt;
		this.kvSpilt = kvSpilt;
		this.secialKeyRegex = secialKeyRegex;
		this.specialValueRegex = specialValueRegex;
		this.keyDecode = keyDecode;
		this.valueDecode = valueDecode;
		this.keyReplace = keyReplace;
		this.valueReplace = valueReplace;
		this.charSet = charSet;
	}



	public StringParse(){
		this(DEFAULT_COL_SPILT,DEFAULT_KV_SPILT,DEFAULT_SPECIAL_REGEX,DEFAULT_SPECIAL_REGEX,
				DEFAULT_KEY_DECODE, DEFAULT_VALUE_DECODE,DEFAULT_CHARSET,DEFAULT_KEY_REPLACE,DEFAULT_VALUE_REPLACE);
	}
	
	public Map<String,String> parse2Map(String src){
		return parse2Map(src,colSpilt,kvSpilt,keyDecode,valueDecode,keyReplace,valueReplace,secialKeyRegex,specialValueRegex,charSet);
	}

	/**
	 * @return Returns the colSpilt.
	 */
	public String getColSpilt() {
		return colSpilt;
	}

	/**
	 * @param colSpilt The colSpilt to set.
	 */
	public void setColSpilt(String colSpilt) {
		this.colSpilt = colSpilt;
	}

	/**
	 * @return Returns the kvSpilt.
	 */
	public String getKvSpilt() {
		return kvSpilt;
	}

	/**
	 * @param kvSpilt The kvSpilt to set.
	 */
	public void setKvSpilt(String kvSpilt) {
		this.kvSpilt = kvSpilt;
	}

	/**
	 * @return Returns the secialKeyRegex.
	 */
	public String getSecialKeyRegex() {
		return secialKeyRegex;
	}

	/**
	 * @param secialKeyRegex The secialKeyRegex to set.
	 */
	public void setSecialKeyRegex(String secialKeyRegex) {
		this.secialKeyRegex = secialKeyRegex;
	}

	/**
	 * @return Returns the specialValueRegex.
	 */
	public String getSpecialValueRegex() {
		return specialValueRegex;
	}

	/**
	 * @param specialValueRegex The specialValueRegex to set.
	 */
	public void setSpecialValueRegex(String specialValueRegex) {
		this.specialValueRegex = specialValueRegex;
	}

	/**
	 * @return Returns the keyDecode.
	 */
	public boolean isKeyDecode() {
		return keyDecode;
	}

	/**
	 * @param keyDecode The keyDecode to set.
	 */
	public void setKeyDecode(boolean keyDecode) {
		this.keyDecode = keyDecode;
	}

	/**
	 * @return Returns the valueDecode.
	 */
	public boolean isValueDecode() {
		return valueDecode;
	}

	/**
	 * @param valueDecode The valueDecode to set.
	 */
	public void setValueDecode(boolean valueDecode) {
		this.valueDecode = valueDecode;
	}

	/**
	 * @return Returns the keyReplace.
	 */
	public boolean isKeyReplace() {
		return keyReplace;
	}

	/**
	 * @param keyReplace The keyReplace to set.
	 */
	public void setKeyReplace(boolean keyReplace) {
		this.keyReplace = keyReplace;
	}

	/**
	 * @return Returns the valueReplace.
	 */
	public boolean isValueReplace() {
		return valueReplace;
	}

	/**
	 * @param valueReplace The valueReplace to set.
	 */
	public void setValueReplace(boolean valueReplace) {
		this.valueReplace = valueReplace;
	}



	public String getCharSet() {
		return charSet;
	}



	public void setCharSet(String charSet) {
		this.charSet = charSet;
	}
	
	public void appendSecialKeyRegex(String regex){
		if(StringUtils.isNotBlank(regex)){
			if(StringUtils.isNotBlank(secialKeyRegex)){
				for(String value : regex.split("\\|")){
					if(!secialKeyRegex.contains(value) && StringUtils.isNotBlank(value)){
						secialKeyRegex = secialKeyRegex + "|" + value;
					}
				}
			}else{
				secialKeyRegex = regex;
			}
		}
	}
	
	
	public void appendSpecialValueRegex(String regex){
		if(StringUtils.isNotBlank(regex)){
			if(StringUtils.isNotBlank(specialValueRegex)){
				for(String value : regex.split("\\|")){
					if(!specialValueRegex.contains(value) && StringUtils.isNotBlank(value)){
						specialValueRegex = specialValueRegex + "|" + value;
					}
				}
			}else{
				specialValueRegex = regex;
			}
		}
	}
	
	public void removeSecialKeyRegex(String regex){
		if(StringUtils.isNotBlank(regex) && StringUtils.isNotBlank(secialKeyRegex)){
			for(String value : regex.split("\\|")){
				if(secialKeyRegex.contains(value) && StringUtils.isNotBlank(value)){
					if(secialKeyRegex.endsWith(value) )
						secialKeyRegex = secialKeyRegex.replaceAll("\\|" + value, "");
					else if(secialKeyRegex.startsWith(value))
						secialKeyRegex = secialKeyRegex.replaceAll(value + "\\|", "");
					else
						secialKeyRegex = secialKeyRegex.replaceAll("\\|" + value + "\\|", "|");
				}
			}
		}
	}
	
	public void removeSpecialValueRegex(String regex){
		if(StringUtils.isNotBlank(regex) && StringUtils.isNotBlank(specialValueRegex)){
			for(String value : regex.split("\\|")){
				if(specialValueRegex.contains(value) && StringUtils.isNotBlank(value)){
					if(specialValueRegex.endsWith(value) )
						specialValueRegex = specialValueRegex.replaceAll("\\|" + value, "");
					else if(specialValueRegex.startsWith(value))
						specialValueRegex = specialValueRegex.replaceAll(value + "\\|", "");
					else
						specialValueRegex = specialValueRegex.replaceAll("\\|" + value + "\\|", "|");
				}
			}
		}
	}



	public Map<String,String> getResult() {
		return result;
	}



	public void setResult(Map<String,String> result) {
		this.result = result;
		this.ignoreResult = null;
	}
	
}
