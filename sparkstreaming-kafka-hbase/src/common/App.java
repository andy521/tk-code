package common;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import eu.bitwalker.useragentutils.UserAgent;




/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		//String str = "10.135.88.199 - - [25/Jun/2017:21:49:04 +0800] \"GET /track/dummy.png?terminalId=352105067489394%2C357714186790243%2Cd1d050042ae5024b%2Cf4%3A09%3Ad8%3A8c%3A7c%3Aa9%2CAC%3A36%3A13%3AAE%3A4E%3ADC%2C&fromId=56705&dataType=order&appId=app001&location=%7B%22timestamp%22%3A1498398534052%2C%22area%22%3A%22%E4%BA%8C%E4%B8%83%E5%8C%BA%22%2C%22street%22%3A%22%E6%A3%89%E7%BA%BA%E4%B8%9C%E8%B7%AF%22%2C%22province%22%3A%22%E6%B2%B3%E5%8D%97%E7%9C%81%22%2C%22longitude%22%3A113.641301%2C%22latitude%22%3A34.762476%2C%22city%22%3A%22%E9%83%91%E5%B7%9E%E5%B8%82%22%7D&appType=app&sysinfo=samsung%2CSM-G9008W&pageCode=10 HTTP/1.1\" 200 68 \"-\" \"okhttp/3.1.2\" \"10.130.11.68\" 04%2C%22area%22%3A%22%E7%A7%80%E5%B3%B0%E5%8C%BA%22%2C%22street%22%3A%22%E4%B8%9C%E5%AE%89%E8%B7%AF%22%2C%22province%22%3A%22%E5%B9%BF%E8%A5%BF%E5%A3%AE%E6%97%8F%E8%87%AA%E6%B2%BB%E5%8C%BA%22%2C%22longitude%22%3A110.270349%2C%22latitude%22%3A25.274131%2C%22city%22%3A%22%E6%A1%82%E6%9E%97%E5%B8%82%22%7D&userId=16053112&incr=1&appType=app&sysinfo=OPPO%2COPPO+A59s&pageCode=7 HTTP/1.1\" 200 68 \"-\" \"okhttp/3.1.2\" \"10.130.11.68\" 0.000 ";
		String str="10.135.88.199 - - [09/Nov/2017:15:38:05 +0800] \"GET /track/dummy.png?appType=wechat&appId=wechat067&terminalId=TKj61w6a6il&fromId=&page=%E5%85%8D%E8%B4%B9%E9%A2%86%E5%8F%96&refer=&time=1510213085150&duration=2&event=%E9%A2%86%E5%8F%96%E9%A1%B5&subType=&userId=13802263&v=1.1.160507&j9s5vmcfn HTTP/1.1\" 200 68 \"https://m.tk.cn/app/remakeftb/Ftb_FcbFreeTofcbReceive.html\" \"Mozilla/5.0 (Linux; Android 7.0; MHA-AL00 Build/HUAWEIMHA-AL00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/59.0.3071.125 Mobile Safari/537.36\" \"223.104.12.176\" 0.000 ";
		List<UserLogInfo> UserLogInfos = analysisContext(str);
		for (UserLogInfo factSrcUserEvent : UserLogInfos) {
			System.out.println("原始日志："+str);
			System.out.println("解析数据如下：");
			System.out.println("APP_ID:"+factSrcUserEvent.getAPP_ID());
			System.out.println("APP_TYPE:"+factSrcUserEvent.getAPP_TYPE());
			System.out.println("BROWSER:"+factSrcUserEvent.getBROWSER());
			System.out.println("CLIENTTIME:"+factSrcUserEvent.getCLIENTTIME());
			System.out.println("CURRENTURL:"+factSrcUserEvent.getCURRENTURL());
			System.out.println("CUSTOM_VAL:"+factSrcUserEvent.getCUSTOM_VAL());
			System.out.println("DURATION:"+factSrcUserEvent.getDURATION());
			System.out.println("EVENT:"+factSrcUserEvent.getEVENT());
			System.out.println("FROM_ID:"+factSrcUserEvent.getFROM_ID());
			System.out.println("IP:"+factSrcUserEvent.getIP());
			System.out.println("LABEL:"+factSrcUserEvent.getLABEL());
			System.out.println("PAGE:"+factSrcUserEvent.getPAGE());
			System.out.println("REFER:"+factSrcUserEvent.getREFER());
			System.out.println("ROWKEY:"+factSrcUserEvent.getROWKEY());
			System.out.println("SUBTYPE:"+factSrcUserEvent.getSUBTYPE());
			System.out.println("SYSINFO:"+factSrcUserEvent.getSYSINFO());
			System.out.println("TERMINAL_ID:"+factSrcUserEvent.getTERMINAL_ID());
			System.out.println("TIME:"+factSrcUserEvent.getTIME());
			System.out.println("USER_ID:"+factSrcUserEvent.getUSER_ID());
		}
	}

	public static List<UserLogInfo> analysisContext(String context) {
		List<UserLogInfo> factSrcUserEventList = new ArrayList<UserLogInfo>();
		if (context == null || context.equals("")) {
			return null;
		} else {
			String rowKey = "";
			String ip = "";
			String time = "";
			String clientTime = "";
			String app_type = "";
			String app_id = "";
			String user_id = "";
			String page = "";
			String event = "";
			String subtype = "";
			String label = "";
			String custom_val = "";
			String refer = "";
			String currentURL = "";
			String browser = "";
			String sysInfo = "";
			String terminal_id = "";
			String duration = "";
			String fromId = "";
			String appFromId = "";// app用devicetype来覆盖fromid
			String customize = "";// 定制信息
			String regex = "\"";
			String substr = "|";
			context = context.replaceAll(regex, substr);
			context = context.replaceAll("\\+", "%2b");
			String[] data = context.split("\\|");
			ip = data[7];
			UserAgent userAgent = UserAgent.parseUserAgentString(data[5]);
			browser = userAgent.getBrowser().toString();
			sysInfo = userAgent.getOperatingSystem().toString();
			time = data[0].substring(data[0].indexOf("[") + 1, data[0].indexOf("]"));
			time = convertDateFormat(time); // server time
			currentURL = getDecode(data[3]);
			String regex1 = "[ ?&]";
			String substr1 = "|";
			String srcStrArg = data[1].replaceAll(regex1, substr1);
			String[] srcStrArgs = srcStrArg.split("\\|");
			srcStrArgs = preHandle(srcStrArgs);
			String tempVal = "";
			boolean AppFlag = false;
			boolean isProtogenesis = true;
			for (int i = 0; i < srcStrArgs.length; i++) {
				tempVal = srcStrArgs[i];
				if (valueIscontains(tempVal, "appType=")) {
					tempVal = getDecode(tempVal);
					app_type = tempVal.isEmpty() ? tempVal
							: tempVal.substring((tempVal.indexOf("appType") + "appType=".length()));
					if (app_type.equalsIgnoreCase("app")) {// 判断为APP类
						AppFlag = true;
					}
				} else if (valueIsExist(tempVal, "appId=")) {
					tempVal = getDecode(tempVal);
					app_id = tempVal.isEmpty() ? tempVal : tempVal.substring("appId=".length());
				} else if (valueIsExist(tempVal, "userId=")) {
					tempVal = getDecode(tempVal);
					tempVal = delObj(tempVal);
					user_id = tempVal.isEmpty() ? tempVal : tempVal.substring("userId=".length()).trim();
					user_id = user_id.replaceAll("(\\+|\\%20|\\%2b)", "");
				} else if (valueIsExist(tempVal, "page=")) {
					tempVal = getDecode(tempVal);
					page = tempVal.isEmpty() ? tempVal : tempVal.substring("page=".length());
				} else if (valueIsExist(tempVal, "event=")) {
					isProtogenesis = false;// 存在event则不为原生App
					// 原生的APP类是用安卓代码开发的界面，否则为H5开发的安卓的界面
					tempVal = getDecode(tempVal);
					event = tempVal.isEmpty() ? tempVal : tempVal.substring("event=".length()).trim();
				} else if (valueIsExist(tempVal, "subType=")) {
					tempVal = getDecode(tempVal);
					subtype = tempVal.isEmpty() ? tempVal : tempVal.substring("subType=".length());
				} else if (valueIsExist(tempVal, "label=")) {
					tempVal = getDecode(tempVal);
					label = tempVal.isEmpty() ? tempVal : tempVal.substring("label=".length());
				} else if (valueIsExist(tempVal, "customVal=")) {
					tempVal = getDecode(tempVal);
					custom_val = tempVal.isEmpty() ? tempVal : tempVal.substring("customVal=".length());
				} else if (valueIsExist(tempVal, "refer=")) {
					tempVal = getDecode(tempVal);
					refer = tempVal.isEmpty() ? tempVal : tempVal.substring("refer=".length()).trim();
				} else if (valueIscontains(tempVal, "terminalId=")) {
					tempVal = getDecode(tempVal);
					terminal_id = tempVal.isEmpty() ? tempVal
							: tempVal.substring((tempVal.indexOf("terminalId") + "terminalId=".length()));
				} else if (valueIsExist(tempVal, "time=")) {
					tempVal = getDecode(tempVal);
					clientTime = tempVal.isEmpty() ? tempVal : tempVal.substring("time=".length());
				} else if (valueIsExist(tempVal, "duration=")) {
					tempVal = getDecode(tempVal);
					duration = tempVal.isEmpty() ? tempVal : tempVal.substring("duration=".length());
//					System.out.println("duration:" + duration);
				} else if (valueIsExist(tempVal, "fromId=")) {
					tempVal = getDecode(tempVal);
					fromId = tempVal.isEmpty() ? tempVal : tempVal.substring("fromId=".length());
				} else if (valueIsExist(tempVal, "devicetype=")) {
					tempVal = getDecode(tempVal);
					appFromId = tempVal.isEmpty() ? tempVal : tempVal.substring("devicetype=".length());
					// devicetype: 1 android;2 ios
					// p_clientorg: 54325 泰康在线APP(IOS);53685 泰康在线APP(Android)
					if (appFromId.equals("1")) {
						appFromId = "53685";
					} else if (appFromId.equals("2")) {
						appFromId = "54325";
					} else {
						appFromId = "";// 防止devicetype异常数据导致问题,虽然目前只有1和2
					}
				} else if (valueIsExist(tempVal, "customize=")) {
					tempVal = getDecode(tempVal);
					customize = tempVal.isEmpty() ? tempVal : tempVal.substring("customize=".length());
				} else if (valueIscontains(tempVal, "JPushRegistrationID=")) {
					if (custom_val != "" && custom_val != null) {
						custom_val = "JPushRegistrationID:"
								+ (tempVal.isEmpty() ? tempVal : tempVal.substring("JPushRegistrationID=".length()))
								+ ",";
					} else {
						custom_val = "JPushRegistrationID:"
								+ (tempVal.isEmpty() ? tempVal : tempVal.substring("JPushRegistrationID=".length()))
								+ ",";
					}
				} else if (valueIscontains(tempVal, "location=")) {
					if (custom_val != "" && custom_val != null) {
						custom_val += ",location:"
								+ (tempVal.isEmpty() ? tempVal : tempVal.substring("location=".length())) + ",";
					} else {
						custom_val = "location:"
								+ (tempVal.isEmpty() ? tempVal : tempVal.substring("location=".length())) + ",";
					}
				}
			}
			if (AppFlag) {// 为APP
				if (isProtogenesis) {// 原生App
					for (int i = 0; i < srcStrArgs.length; i++) {
						tempVal = srcStrArgs[i];
						if (valueIsExist(tempVal, "dataType=")) {
							tempVal = getDecode(tempVal);
							event = tempVal.isEmpty() ? tempVal : tempVal.substring("dataType=".length()).trim();
						} else if (valueIsExist(tempVal, "pageCode=")) {
							tempVal = getDecode(tempVal);
							subtype = tempVal.isEmpty() ? tempVal : tempVal.substring("pageCode=".length()).trim();
						} else if (valueIsExist(tempVal, "incr=")) {
							tempVal = getDecode(tempVal);
							custom_val = tempVal.isEmpty() ? tempVal : tempVal.substring("incr=".length());
							// 设备号
						}
						// else if (valueIscontains(tempVal,
						// "JPushRegistrationID=")) {
						// tempVal = getDecode(tempVal);
						// jpushregistrationid = tempVal.isEmpty() ? tempVal :
						// tempVal.substring((tempVal.indexOf("JPushRegistrationID")
						// +"JPushRegistrationID=".length()));
						// //GPS
						// } else if (valueIscontains(tempVal, "location=")) {
						// tempVal = getDecode(tempVal);
						// location = tempVal.isEmpty() ? tempVal :
						// tempVal.substring((tempVal.indexOf("location")
						// +"location=".length()));
						// }
						else if (valueIsExist(tempVal, "time=")) {
							tempVal = getDecode(tempVal);
							time = tempVal.isEmpty() ? tempVal : tempVal.substring("time=".length());
							clientTime = tempVal.isEmpty() ? tempVal : tempVal.substring("time=".length());
						}
					}
				} else {// 非原生App，如果label里面含有设备和系统信息需要取出来覆盖通用解析中的设备和系统信息
					if (label != null && !"".equals(label)) {
						String labelTmp = label.replaceAll("(\\+|\\%20|\\%2b)", "");
						String sysinfoLabel = "sysinfo:\"";
						String terminalLabel = "terminalid:\"";
						int indexOfLabel = 0;
						if ((indexOfLabel = labelTmp.toLowerCase().indexOf(sysinfoLabel)) != -1) {
							sysInfo = labelTmp.substring(indexOfLabel + sysinfoLabel.length(),
									labelTmp.indexOf("\"", indexOfLabel + sysinfoLabel.length()));
						}
						if ((indexOfLabel = labelTmp.toLowerCase().indexOf(terminalLabel)) != -1)
							terminal_id = labelTmp.substring(indexOfLabel + terminalLabel.length(),
									labelTmp.indexOf("\"", indexOfLabel + terminalLabel.length()));
					}
					custom_val = "-2";
				}
			}

			// 覆盖app的fromId
			if (null != app_id
					&& (app_id.equalsIgnoreCase("app001") || app_id.equalsIgnoreCase("clue_H5_website_002"))) {
				if (null != appFromId && !appFromId.equals("")) {
					fromId = appFromId;
				}
			}

			rowKey = IDUtil.getUUID().toString();

			UserLogInfo srcUsr = new UserLogInfo(rowKey, ip, time, app_type, app_id, user_id, page, event,
					subtype, label, custom_val, refer, currentURL, browser, sysInfo, terminal_id, duration, clientTime,
					fromId);
			defaultHandleFromId(srcUsr, context);
			// 通用投保流程，App_Type=javaWeb;App_ID=javaWeb001
			insuranceProcessHandle(srcUsr, customize);

			factSrcUserEventList.add(srcUsr);

			// 用原始对象解析scode，新增一条线索
			String scode_apptype = "h5";
			String scode_str = "scode";
			factSrcUserEventList = addSignUserIdClue(factSrcUserEventList, srcUsr, scode_apptype, scode_str);

			// 用原始对象解析wanzhangId，新增一条线索
			String wzId_apptype = "h5";
			String wzId_str = "wanzhangId";
			factSrcUserEventList = addSignUserIdClue(factSrcUserEventList, srcUsr, wzId_apptype, wzId_str);
			return factSrcUserEventList;
		}
	}
	
	private static List<UserLogInfo> addSignUserIdClue(List<UserLogInfo> UserLogInfoList,
			UserLogInfo srcUsr, String apptype_sign, String str_sign) {
		// String scode_apptype = "h5";
		// String scode_str = "scode";
		String app_type = srcUsr.getAPP_TYPE();
		String label = srcUsr.getLABEL();

		if (app_type.equalsIgnoreCase(apptype_sign)) {
			Pattern p = Pattern.compile("(.*):\"(.*)\"");
			String reg = "\",";
			String sub = "\";";
			String label2 = label.replaceAll(reg, sub);// ,->;
			String[] datas = label2.split(";");
			for (int i = 0; i < datas.length; i++) {
				Matcher m = p.matcher(datas[i]);
				if (m.find() && m.group(1).equalsIgnoreCase(str_sign) && null != m.group(2)
						&& !m.group(2).trim().equals("")) {
					String signCode = str_sign + ":\"" + m.group(2) + "\"";
					UserLogInfo srcUsr2 = new UserLogInfo();
					try {
						srcUsr2 = (UserLogInfo) srcUsr.clone();
					} catch (CloneNotSupportedException e) {
						e.printStackTrace();
					}
					srcUsr2.setUSER_ID(signCode);
					srcUsr2.setROWKEY(IDUtil.getUUID().toString());
					srcUsr2.setTERMINAL_ID(srcUsr.getTERMINAL_ID() + "_" + str_sign);// 防止MEM的默认user_id被补
					UserLogInfoList.add(srcUsr2);
				}
			}
		}
		return UserLogInfoList;
	}
	
	public static boolean valueIsExist(String data, String keyContext) {
		boolean ret = false;
		if (data.startsWith(keyContext) && (data.length() > keyContext.length())) {
			ret = true;
		}
		return ret;
	}

	public static boolean valueIscontains(String data, String keyContext) {
		boolean ret = false;
		if (data.contains(keyContext)) {
			ret = true;
		}
		return ret;
	}

	public static String convertDateFormat(String str) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
		try {
			Date date = simpleDateFormat.parse(str);
			return Long.toString(date.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return "";
	}
	
	public static boolean paramIsWrong(String param) {
		// url or other param is wrong because of length limit
		// System.out.println("getDecoder");
		String reg = "%(?![0-9a-fA-F]{2})";
		Pattern p = Pattern.compile(reg);
		return p.matcher(param).find();
	}

	public static String getDecode(String src) {
		if (paramIsWrong(src)) {
			return "";
		}
		try {
			src = URLDecoder.decode(src, "utf-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return src;
	}

	public static String delObj(String src) {
		if (src.toLowerCase().contains("object") || src.toLowerCase().equals("userid=0")) {
			src = "";
		}
		return src;
	}

	
	private static String[] preHandle(String[] data) {
		List<String> result = new ArrayList<String>();
		String[] tmpValues;
		for (String value : data) {
			if (value != null && !"".equals(value)) {
				// \\是空格 \\t制表符，向后跳8个字符
				tmpValues = value.split("(\\t|\\s)");
				for (String tmpVal : tmpValues) {
					if (tmpVal != null && !"".equals(tmpVal.trim()))
						result.add(tmpVal);
				}
			}
		}
		return result.toArray(new String[] {});
	}
	
	private static void defaultHandleFromId(UserLogInfo srcUsr, String srclog) {
		String fromId = srcUsr.getFROM_ID();
		String appType = srcUsr.getAPP_TYPE();
		String label = srcUsr.getLABEL();
		String sysInfo = srcUsr.getSYSINFO().toLowerCase();
		if (label != null && !"".equals(label) && label.toLowerCase().indexOf("scode") != -1) {
			fromId = fromMap.get("sms");
		}

		// add wechat fromId default handle by author moyunqing 2016-11-03
		if ("wechat".equalsIgnoreCase(appType)) {
			StringParse sp = new StringParse();
			sp.setColSpilt("&|\\||\\?");
			sp.setKvSpilt("=");
			sp.parse2Map(srclog);
			Object from = sp.getIgnoreCase("from");
			if (from == null)
				from = sp.getIgnoreCase("fromId");
			if (from == null)
				from = sp.getIgnoreCase("from_Id");
			if (from != null && StringUtils.isNotBlank(from.toString()))
				fromId = from.toString();
		}

		if (fromId == null || "".equals(fromId)) {
			if ("h5".equalsIgnoreCase(appType)) {
				fromId = fromMap.get("wap");
			} else if ("wechat".equalsIgnoreCase(appType)) {
				fromId = fromMap.get("wechat");
			} else if ("app".equalsIgnoreCase(appType)) {
				if (sysInfo.indexOf("mac_os") != -1 || sysInfo.indexOf("ios") != -1) {
					fromId = fromMap.get("appApple");
				} else {
					fromId = fromMap.get("appAndroid");
				}
			} else if ("website".equalsIgnoreCase(appType)) {
				fromId = fromMap.get("website");
			} else {
				fromId = fromMap.get("pc");
			}
		}
		srcUsr.setFROM_ID(fromId);
	}
	
	private static void insuranceProcessHandle(UserLogInfo srcUsr, String customize) {
		if ("javaweb".equals(srcUsr.getAPP_TYPE().toLowerCase())
				&& "javaweb001".equals(srcUsr.getAPP_ID().toLowerCase())) {
			srcUsr.setCUSTOM_VAL(customize);
		}
	}
	
	private static final Map<String, String> fromMap = new HashMap<String, String>();

	static {
		fromMap.put("pc", "3");
		fromMap.put("wap", "53686");
		fromMap.put("appAndroid", "53685");
		fromMap.put("appApple", "54325");
		fromMap.put("wechat", "60185");
		fromMap.put("sms", "53686");
		fromMap.put("website", "3");
	}

}
