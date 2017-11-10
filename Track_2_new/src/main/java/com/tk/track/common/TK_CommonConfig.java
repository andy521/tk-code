/**
 * 
 */
package com.tk.track.common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.ResourceBundle;
/**
 * 
 */
public class TK_CommonConfig {
	private static final String BUNDLE_NAME = "common-config";
	private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME);

	public static String getValue(String key) {
		try {
			return RESOURCE_BUNDLE.getString(key).trim();
		} catch (MissingResourceException e) {
			return "";
		}
	}
	
	public static boolean hasKey(String key) {
	    return RESOURCE_BUNDLE.containsKey(key);
	}
	
	public static Enumeration<String> getKeys() {
	    return RESOURCE_BUNDLE.getKeys();
	}
	
	public static String getConfigValue(String configFileName, String key) {
		Properties prop = new Properties();// 属性集合对象 
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(configFileName);
			prop.load(fis);// 将属性文件流装载到Properties对象中 
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fis.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return prop.getProperty(key);
	}
	
	public static Properties getConfig(String filename) {
		Properties prop = new Properties();// 属性集合对象 
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(filename);
			prop.load(fis);// 将属性文件流装载到Properties对象中 
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fis.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return prop;
	}
	
}
