package com.tk.track.fact.sparksql.etl;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.catalyst.expressions.Substring;

import com.tk.track.fact.sparksql.desttable.FactSrcUserEvent;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorClue;
import com.tk.track.fact.sparksql.desttable.TempNetTeleCallSkill;
import com.tk.track.fact.sparksql.desttable.TempTeleUseCondition;
import com.tk.track.fact.sparksql.desttable.TempUserBehaviorTele;
import com.tk.track.fact.sparksql.desttable.TempUserBehaviorTeleH5Act;
import com.tk.track.fact.sparksql.desttable.TempUserBehaviorTeleWap;
import com.tk.track.fact.sparksql.util.StringParse;
import com.tk.track.util.IDUtil;

import eu.bitwalker.useragentutils.UserAgent;

public class SrcLogParse {

	public static void main(String[] args) {
		String  str = "10.135.88.199 - - [31/Oct/2017:15:15:37 +0800] \"GET /track/dummy.png?terminalId=125888606754820%2C426908313008760%2C0bjvtn9ysy3tixij%2CW9%3A3E%3A2B%3A2Z%3ASP%3A2V%2C%2C&fromId=60405&time=1509434137862&appId=app001&userId=16958796++&JPushRegistrationID=13065ffa4e3f23ce858&incr=1&appType=app&sysinfo= HTTP/1.1\" 200 68 \"-\" \"okhttp/3.1.2\" \"10.130.11.68\" 0.000";
		List<FactSrcUserEvent> factSrcUserEvents = SrcLogParse.analysisContext(str);
		for (FactSrcUserEvent factSrcUserEvent : factSrcUserEvents) {
			System.out.println(factSrcUserEvent.getAPP_ID());
			System.out.println(factSrcUserEvent.getAPP_TYPE());
			System.out.println(factSrcUserEvent.getBROWSER());
			System.out.println(factSrcUserEvent.getCLIENTTIME());
			System.out.println(factSrcUserEvent.getCURRENTURL());
			System.out.println("-------------------");
			System.out.println(factSrcUserEvent.getCUSTOM_VAL());
			System.out.println("-------------------");
			System.out.println(factSrcUserEvent.getDURATION());
			System.out.println(factSrcUserEvent.getEVENT());
			System.out.println(factSrcUserEvent.getFROM_ID());
			System.out.println(factSrcUserEvent.getIP());
			System.out.println(factSrcUserEvent.getLABEL());
			System.out.println(factSrcUserEvent.getPAGE());
			System.out.println(factSrcUserEvent.getREFER());
			System.out.println(factSrcUserEvent.getROWKEY());
			System.out.println(factSrcUserEvent.getSUBTYPE());
			System.out.println(factSrcUserEvent.getSYSINFO());
			System.out.println(factSrcUserEvent.getTERMINAL_ID());
			System.out.println(factSrcUserEvent.getTIME());
			System.out.println(factSrcUserEvent.getUSER_ID());
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

	/**
	 * @param context
	 * @return
	 */
	public static List<FactSrcUserEvent> analysisContext(String context) {
		List<FactSrcUserEvent> factSrcUserEventList = new ArrayList<FactSrcUserEvent>();
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
					if(valueIscontains(label, "fromId:")){
						String[] arr=label.split(",");
						for(int j=0;j<arr.length;j++){
							String str=arr[j];
							if(null!=str && ""!=str && valueIsExist(str, "fromId")){
								fromId=str.isEmpty() ? str : str.substring("fromId:".length());
								fromId=fromId.isEmpty()?fromId:fromId.replace("\"", "");
							}
						}
					}
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
					System.out.println("duration:" + duration);
				} else if (valueIsExist(tempVal, "fromId=")) {
					tempVal = getDecode(tempVal);
					String str = tempVal.isEmpty() ? fromId : tempVal.substring("fromId=".length());
					fromId=str.isEmpty() ? fromId : str;
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
							custom_val += tempVal.isEmpty() ? tempVal : tempVal.substring("incr=".length());
							// 设备号
						}else if (valueIsExist(tempVal, "JPushRegistrationID=")) {
							if (custom_val != "" && custom_val != null) {
								custom_val = "JPushRegistrationID:"
										+ (tempVal.isEmpty() ? tempVal : tempVal.substring("JPushRegistrationID=".length()))
										+ ",";
							} else {
								custom_val = "JPushRegistrationID:"
										+ (tempVal.isEmpty() ? tempVal : tempVal.substring("JPushRegistrationID=".length()))
										+ ",";
							}
						} else if (valueIsExist(tempVal, "location=")) {
							if (custom_val != "" && custom_val != null) {
								custom_val += ",location:"
										+ (tempVal.isEmpty() ? tempVal : tempVal.substring("location=".length())) + ",";
							} else {
								custom_val = "location:"
										+ (tempVal.isEmpty() ? tempVal : tempVal.substring("location=".length())) + ",";
							}
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

			FactSrcUserEvent srcUsr = new FactSrcUserEvent(rowKey, ip, time, app_type, app_id, user_id, page, event,
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

	private static void insuranceProcessHandle(FactSrcUserEvent srcUsr, String customize) {
		if ("javaweb".equals(srcUsr.getAPP_TYPE().toLowerCase())
				&& "javaweb001".equals(srcUsr.getAPP_ID().toLowerCase())) {
			srcUsr.setCUSTOM_VAL(customize);
		}
	}

	/**
	 * 解析tele_system_use_condition
	 */
	public static TempTeleUseCondition analysisTeleUseLabel(String userId, String visitTime, String appType,
			String appId, String mainMenu, String subMenu, String label) {
		String functionDesc = "";
		// desc:""
		// desc:"我的关注 客户服务 本周"
		Pattern p = Pattern.compile("(.*):\"(.*)\"");
		if (appType.equalsIgnoreCase("system")) {
			Matcher m = p.matcher(label);
			if (m.find()) {
				if (m.group(1).equalsIgnoreCase("desc") && m.group(2) != null) {
					functionDesc = m.group(2).toString();
				}
			}
		}

		return new TempTeleUseCondition(userId, visitTime, appType, appId, mainMenu, subMenu, functionDesc);

	}

	/**
	 * 
	 * @Description: 对原数据进行预处理，去除空格、tab以及日志自动生成部分对参数解析的影响
	 * @param data
	 * @return
	 * @author moyunqing
	 * @date 2016年9月2日
	 * @update:[日期YYYY-MM-DD] [更改人姓名][变更描述]
	 */
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

	private static void defaultHandleFromId(FactSrcUserEvent srcUsr, String srclog) {
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

	private static List<FactSrcUserEvent> addSignUserIdClue(List<FactSrcUserEvent> factSrcUserEventList,
			FactSrcUserEvent srcUsr, String apptype_sign, String str_sign) {
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
					FactSrcUserEvent srcUsr2 = new FactSrcUserEvent();
					try {
						srcUsr2 = (FactSrcUserEvent) srcUsr.clone();
					} catch (CloneNotSupportedException e) {
						e.printStackTrace();
					}
					srcUsr2.setUSER_ID(signCode);
					srcUsr2.setROWKEY(IDUtil.getUUID().toString());
					srcUsr2.setTERMINAL_ID(srcUsr.getTERMINAL_ID() + "_" + str_sign);// 防止MEM的默认user_id被补
					factSrcUserEventList.add(srcUsr2);
				}
			}
		}
		return factSrcUserEventList;
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

	/**
	 * 解析label
	 */
	public static TempUserBehaviorTele analysislShopLable(String rowKey, String userid, String apptype, String appid,
			String event, String subtype, String label, String customVal, String visitcount, String visittime,
			String visitduration, String fromId) {
		// public static void analysislShopLable(String apptype, String appid,
		// String event, String label) {
		String classifyName = "";
		String productID = "";
		String productName = "";
		String classID = "";
		String className = "";
		String code = "";
		Pattern p = Pattern.compile("(.*):\"(.*)\"");
		if (appid.equals("mall001") && apptype.equals("mall")) { // 商城
			String regex = "\",";
			String substr = "\";";
			label = label.replaceAll(regex, substr);// ,->;
			String[] data = label.split(";");
			if (event.equals("商品详情")) { // 商品详情页面
				for (int i = 0; i < data.length; i++) {
					Matcher m = p.matcher(data[i]);
					if (m.find()) {
						if (m.group(1).equals("classifyName")) {
							classifyName = m.group(2);
						} else if (m.group(1).equals("productID")) {
							productID = m.group(2);
						} else if (m.group(1).equals("productName")) {
							productName = m.group(2);
						} else if (m.group(1).equals("code")) {
							code = m.group(2);
						}
					}
				}
			} else if (event.equals("商品列表")) {
				for (int i = 0; i < data.length; i++) {
					Matcher m = p.matcher(data[i]);
					if (m.find()) {
						if (m.group(1).equals("classID")) {
							classID = m.group(2);
						} else if (m.group(1).equals("className")) {
							className = m.group(2);
						}
						if (className.equals("养老保险")) {
							className = "养老险";
						}
					}
				}
			}
		}
		return new TempUserBehaviorTele(rowKey, userid, apptype, appid, event, subtype, productID, classID, className,
				visitcount, visittime, visitduration, classifyName, productName, fromId, code);
	}

	/**
	 * 解析WapLabel
	 */
	public static TempUserBehaviorTeleWap analysisWapLable(String rowKey, String userid, String apptype, String appid,
			String event, String subtype, String label, String customVal, String visitcount, String visittime,
			String visitduration, String fromId) {

		String classifyName = "";
		String lrtID = "";
		String lrtName = "";
		String classID = "";
		// String className = "";
		String thirdLevel = "";
		String fourthLevel = "";
		String userType = "";
		String finalUserId = "";

		Pattern p = Pattern.compile("(.*):\"(.*)\"");

		if (apptype.equalsIgnoreCase("h5")) { // WAP
			String regex = "\",";
			String substr = "\";";
			label = label.replaceAll(regex, substr);// ,->;
			String[] data = label.split(";");

			if (event.equals("商品详情")) { // Wap商品详情
				String openId = "";
				for (int i = 0; i < data.length; i++) {
					Matcher m = p.matcher(data[i]);
					if (m.find()) {
						if (m.group(1).equalsIgnoreCase("classifyName")) {
							classifyName = m.group(2);
						} else if (m.group(1).equalsIgnoreCase("lrtID")) {
							lrtID = m.group(2);
							fourthLevel = m.group(2);// 四级分类直接取
						} else if (m.group(1).equalsIgnoreCase("lrtName")) {
							lrtName = m.group(2);
						} else if (m.group(1).equalsIgnoreCase("lrtType") && openId.isEmpty()) {
							thirdLevel = m.group(2);// 三级分类直接取
						} else if (m.group(1).equalsIgnoreCase("openid")) {
							openId = m.group(2); // 获取openId的值
							if (!openId.isEmpty()) {
								finalUserId = openId;
								userType = "WE";
								// thirdLevel = "产品详情页";
							}
						}
					}
				}
			} else if (event.equals("商品列表")) { // Wap商品列表
				for (int i = 0; i < data.length; i++) {
					Matcher m = p.matcher(data[i]);
					if (m.find()) {
						if (m.group(1).equalsIgnoreCase("classID")) {
							classID = m.group(2);// 暂未传，固为空
						} else if (m.group(1).equalsIgnoreCase("className")) {
							// className = m.group(2);
							thirdLevel = m.group(2);// 三级分类直接取
							String[] classNameInfo = thirdLevel.split("-");
							if (classNameInfo.length > 1) {
								fourthLevel = "Wap" + classNameInfo[1];// 拼接四级分类
							} else {
								fourthLevel = "Wap" + classNameInfo[0];
							}
						}
					}
				}
			} else if (event.equals("wap商品详情")) {
				String openId = "";
				thirdLevel = "短险";
				for (int i = 0; i < data.length; i++) {
					Matcher m = p.matcher(data[i]);
					if (m.find()) {
						if (m.group(1).equalsIgnoreCase("classifyName")) {
							classifyName = m.group(2);
						} else if (m.group(1).equalsIgnoreCase("productId")) {
							lrtID = m.group(2);
							fourthLevel = m.group(2);// 四级分类直接取
						} else if (m.group(1).equalsIgnoreCase("lrtName")) {
							lrtName = m.group(2);
						} else if (m.group(1).equalsIgnoreCase("lrtType") && openId.isEmpty()) {
							// thirdLevel = m.group(2);//三级分类直接取
						} else if (m.group(1).equalsIgnoreCase("openid")) {
							openId = m.group(2); // 获取openId的值
							if (!openId.isEmpty()) {
								finalUserId = openId;
								userType = "WE";
								// thirdLevel = "产品详情页";
							}
						}
					}
				}
			}
			Matcher id = p.matcher(userid);
			if (id.find() && id.group(1).equalsIgnoreCase("wanzhangId") && null != id.group(2)
					&& !id.group(2).trim().equals("")) {
				String wanzhangId = id.group(2);
				finalUserId = wanzhangId.equals("0") ? "" : wanzhangId;
				userType = "WZ";
			} else {
				finalUserId = userid;
				// 判断user_id是否为数字，不是的则认为是openid
				if (!isNumeric(finalUserId.trim())) {
					userType = "WE";
				}
				if (!userType.equals("WE")) {
					userType = "MEM";
				}
			}
		}
		return new TempUserBehaviorTeleWap(rowKey, finalUserId, apptype, appid, event, subtype, lrtID, classID,
				visitcount, visittime, visitduration, classifyName, lrtName, thirdLevel, fourthLevel, userType, fromId);
	}

	public static boolean isNumeric(String str) {
		Pattern pattern = Pattern.compile("[0-9]*");
		Matcher isNum = pattern.matcher(str);
		if (!isNum.matches()) {
			return false;
		}
		return true;
	}

	/**
	 * 解析H5ActLable
	 */
	public static TempUserBehaviorTeleH5Act analysisH5ActLable(String rowKey, String userid, String apptype,
			String appid, String event, String subtype, String label, String customVal, String visitcount,
			String visittime, String visitduration, String fromId) {
		String classifyName = "";
		String productID = "";
		String productName = "";
		String classID = "";
		String className = "";
		String userType = "";
		String lrtId = "";
		String finalUserId = "";
		Pattern p = Pattern.compile("(.*):\"(.*)\"");

		if (apptype.equalsIgnoreCase("h5")) { // Wap
			String regex = "\",";
			String substr = "\";";
			label = label.replaceAll(regex, substr);// ,->;
			String[] data = label.split(";");

			Matcher id = p.matcher(userid);
			if (id.find() && id.group(1).equalsIgnoreCase("scode") && null != id.group(2)
					&& !id.group(2).trim().equals("")) {
				finalUserId = id.group(2);
				userType = "SMS";
			} else {
				finalUserId = userid;
				userType = "MEM";
			}

			if (null != subtype && !subtype.trim().equals("")) { // 商品详情页面
				for (int i = 0; i < data.length; i++) {
					Matcher m = p.matcher(data[i]);
					if (m.find()) {
						if (m.group(1).equalsIgnoreCase("lrt_id")) {
							lrtId = m.group(2);
						} else if (m.group(1).equalsIgnoreCase("lrt_name")) {
							productName = m.group(2);
						}
					}
				}
			}
		}
		return new TempUserBehaviorTeleH5Act(rowKey, finalUserId, apptype, appid, event, subtype, productID, classID,
				className, visitcount, visittime, visitduration, classifyName, productName, userType, lrtId, fromId);
	}

	/**
	 * 解析WechatHotLable
	 */
	public static TempUserBehaviorTeleWap analysisWechatHotLable(String rowKey, String userid, String apptype,
			String appid, String event, String subtype, String label, String customVal, String visitcount,
			String visittime, String visitduration, String fromId) {

		String classifyName = "";
		String lrtID = "";
		String lrtName = "";
		String classID = "";
		// String className = "";
		String thirdLevel = "";
		String fourthLevel = "";
		String userType = "";
		String finalUserId = "";
		Set<String> productListSet = new HashSet<String>();
		productListSet.add("小康推荐");
		productListSet.add("健康险");
		productListSet.add("意外险");
		productListSet.add("理财");
		productListSet.add("财产险");
		productListSet.add("长期险");
		Pattern p = Pattern.compile("(.*):(.*)");
		String regex = "\"";
		String substr = "";
		label = label.replaceAll(regex, substr);
		if (apptype.trim().equalsIgnoreCase("wechat")) {
			String[] data = label.split(",");

			if (!productListSet.contains(event) && (subtype == null || subtype.trim().isEmpty())
					&& !event.equals("信息填写")) {
				// thirdLevel = "产品详情页";
				thirdLevel = "短险";
				lrtName = subtype;
				if (event.equals("短期意外险首页")) {
					lrtID = "1103";
					fourthLevel = lrtID;
					classifyName = "短险-意外险";
				} else {
					for (int i = 0; i < data.length; i++) {
						Matcher m = p.matcher(data[i]);
						if (m.find()) {
							if (m.group(1).equalsIgnoreCase("lrt_id")) {
								lrtID = m.group(2);
								fourthLevel = lrtID;
							} else if (m.group(1).equalsIgnoreCase("className")) {
								classifyName = m.group(2);
							}
						}
					}
				}

			} else if (productListSet.contains(event) && productListSet.contains(subtype) && event.equals(subtype)) {
				// thirdLevel = "产品列表页";
				if (event.equals("小康推荐")) {
					fourthLevel = "推荐页";// 推荐->推荐页
					thirdLevel = "推荐";// 推荐->推荐页
				} else {
					thirdLevel = event;
					fourthLevel = event;
				}
			}
			finalUserId = userid;
			userType = "WECHAT";
		}
		return new TempUserBehaviorTeleWap(rowKey, finalUserId, apptype, appid, event, subtype, lrtID, classID,
				visitcount, visittime, visitduration, classifyName, lrtName, thirdLevel, fourthLevel, userType, fromId);
	}

	/**
	 * 解析wechat_insure_flow
	 */
	public static TempUserBehaviorTeleWap analysisWechatInsureFlowLable(String rowKey, String userid, String apptype,
			String appid, String event, String subtype, String label, String customVal, String visitcount,
			String visittime, String visitduration, String fromId) {

		String classifyName = "";
		String lrtID = "";
		String lrtName = "";
		String classID = "";
		// String className = "";
		String thirdLevel = "";
		String fourthLevel = "";
		String userType = "";
		String finalUserId = "";
		Pattern p = Pattern.compile("(.*):(.*)");
		String regex = "\"";
		String substr = "";
		label = label.replaceAll(regex, substr);
		if (apptype.trim().equalsIgnoreCase("wechat")) {
			String[] data = label.split(",");
			// thirdLevel = "产品详情页";
			thirdLevel = "短险";
			for (int i = 0; i < data.length; i++) {
				Matcher m = p.matcher(data[i]);
				if (m.find()) {
					if (m.group(1).equalsIgnoreCase("productId")) {
						lrtID = m.group(2);
						fourthLevel = lrtID;
					} else if (m.group(1).equalsIgnoreCase("productName")) {
						lrtName = m.group(2);
					}
				}
			}
			finalUserId = userid;
			userType = "WECHAT";
		}
		return new TempUserBehaviorTeleWap(rowKey, finalUserId, apptype, appid, event, subtype, lrtID, classID,
				visitcount, visittime, visitduration, classifyName, lrtName, thirdLevel, fourthLevel, userType, fromId);
	}

	/**
	 * 新app7插码规则
	 * 
	 * @return
	 */
	public static TempUserBehaviorTeleWap newAnalysisWechatHotLable(String rowKey, String userid, String apptype,
			String appid, String event, String subtype, String label, String customVal, String visitcount,
			String visittime, String visitduration, String fromId) {
		String classifyName = "";
		String lrtID = "";
		String lrtName = "";
		String classID = "";
		// String className = "";
		String thirdLevel = "";
		String fourthLevel = "";
		String userType = "";
		String finalUserId = "";
		Pattern p = Pattern.compile("(.*):(.*)");
		String regex = "\"";
		String substr = "";
		label = label.replaceAll(regex, substr);
		if (apptype.trim().equalsIgnoreCase("wechat")) {
			String[] data = label.split(",");
			// thirdLevel = "产品详情页";
			thirdLevel = "短险";
			lrtName = subtype;
			for (int i = 0; i < data.length; i++) {
				Matcher m = p.matcher(data[i]);
				if (m.find()) {
					if (m.group(1).equalsIgnoreCase("lrt_id") || m.group(1).equalsIgnoreCase("riskCode")) {
						lrtID = m.group(2);
						fourthLevel = lrtID;
					} else if (m.group(1).equalsIgnoreCase("classifyName")) {
						classifyName = m.group(2);
					}
					String[] classifyNames = classifyName.split("-");
					if (classifyNames.length == 2) {
						thirdLevel = classifyNames[0];
					}
				}
			}
			finalUserId = userid;
			userType = "WECHAT";
		}
		return new TempUserBehaviorTeleWap(rowKey, finalUserId, apptype, appid, event, subtype, lrtID, classID,
				visitcount, visittime, visitduration, classifyName, lrtName, thirdLevel, fourthLevel, userType, fromId);
	}

	/**
	 * 解析ACCOUNT WORTH SEARCH Lable
	 */
	public static FactUserBehaviorClue analysisAccountWorthSearchLable(String ROWKEY, String USER_ID, String APP_TYPE,
			String APP_ID, String EVENT, String SUB_TYPE, String LABEL, String CUSTOM_VAL, String VISIT_COUNT,
			String VISIT_TIME, String VISIT_DURATION, String FROM_ID) {

		String policyNos = "";
		// String className = "";
		String USER_TYPE = "MEM";
		String PAGE_TYPE = "查询";
		String EVENT_TYPE = "load";
		// to do 以后根据具体需求设定 start
		String FIRST_LEVEL = "";
		String SECOND_LEVEL = "";
		String THIRD_LEVEL = "";
		String FOURTH_LEVEL = "";
		// to do 以后根据具体需求设定 end
		String REMARK = "";
		String CLUE_TYPE = "";
		String USER_NAME = "";
		Pattern p = Pattern.compile("(.*):\"(.*)\"");
		String regex = "\",";
		String substr = "\";";
		LABEL = LABEL.replaceAll(regex, substr);// ,->;
		String[] data = LABEL.split(";");
		for (int i = 0; i < data.length; i++) {
			Matcher m = p.matcher(data[i]);
			if (m.find()) {
				if (m.group(1).equalsIgnoreCase("policyNo")) {
					policyNos = m.group(2).trim();
					REMARK = "查询到的保单号：" + policyNos;
				}
			}
		}
		return new FactUserBehaviorClue(ROWKEY, USER_ID, USER_TYPE, APP_TYPE, APP_ID, EVENT_TYPE, EVENT, SUB_TYPE,
				VISIT_DURATION, FROM_ID, PAGE_TYPE, FIRST_LEVEL, SECOND_LEVEL, THIRD_LEVEL, FOURTH_LEVEL, VISIT_TIME,
				VISIT_COUNT, CLUE_TYPE, REMARK, USER_NAME);
	}

	/**
	 * 解析网电月活跃度用户
	 */
	public static FactUserBehaviorClue analysisNetMonthActive(String USER_ID, String NAME, String ACTIVEMONTH, String LASTACTIVETIME,String VISITDAYCOUNT){
		String USER_TYPE = "MEM";
		String PAGE_TYPE = "statistic";
		String EVENT_TYPE = "2";
		String SUB_TYPE = "3次以上";
		String EVENT ="app月度活跃";
		String APP_ID="app_logincount";
		String APP_TYPE="statisticclue";
		// to do 以后根据具体需求设定 start
		String FIRST_LEVEL = "制式化营销";
		String SECOND_LEVEL = "2017销售流程";
		String THIRD_LEVEL = "App月活客户";
		String FOURTH_LEVEL = "App月活3次客户";
		// to do 以后根据具体需求设定 end
//		concat(NVL(AU.MEMBER_ID,'--'),';', NVL(AU.NAME,'--'),';', NVL(AU.ACTIVEMONTH,'--'),';', NVL(AU.LASTACTIVETIME,'--')
		String REMARK = USER_ID+";"+NAME==null?"--":NAME+";"+LASTACTIVETIME+";"+ACTIVEMONTH;
		String CLUE_TYPE = "";
		String USER_NAME = "";
		String VISIT_COUNT = VISITDAYCOUNT;
		String VISIT_TIME = LASTACTIVETIME;
		
		return new FactUserBehaviorClue("", USER_ID, USER_TYPE, APP_TYPE, APP_ID, EVENT_TYPE, EVENT, SUB_TYPE,
				"", "", PAGE_TYPE, FIRST_LEVEL, SECOND_LEVEL, THIRD_LEVEL, FOURTH_LEVEL, VISIT_TIME,
				VISIT_COUNT, CLUE_TYPE, REMARK, USER_NAME);
	}
	
	
	/**
	 * 解析Health Service Lable
	 */
	public static FactUserBehaviorClue analysisHealthServiceLable(String ROWKEY, String USER_ID, String APP_TYPE,
			String APP_ID, String EVENT, String SUB_TYPE, String LABEL, String CUSTOM_VAL, String VISIT_COUNT,
			String VISIT_TIME, String VISIT_DURATION, String FROM_ID) {

		// String className = "";
		String USER_TYPE = "MEM";
		String PAGE_TYPE = "查询";
		String EVENT_TYPE = "load";
		// to do 以后根据具体需求设定 start
		String FIRST_LEVEL = "官网会员行为";
		String SECOND_LEVEL = "客服保全类";
		String THIRD_LEVEL = "";
		String FOURTH_LEVEL = "";
		// to do 以后根据具体需求设定 end
		String REMARK = "";
		String CLUE_TYPE = "2";
		String USER_NAME = "";
		if (APP_ID.equals("app001")) {
			Map<String, String> codeValueMap = initAppHealthServiceMap();
			String pageCode = EVENT.trim() + SUB_TYPE.trim();
			String thirdAndFourthLevel = codeValueMap.get(pageCode);
			if (thirdAndFourthLevel != null) {
				THIRD_LEVEL = thirdAndFourthLevel.split(":")[0];
				FOURTH_LEVEL = thirdAndFourthLevel.split(":")[1];
			}
		} else if (APP_ID.equals("clue_H5_website_001")) {
			if (EVENT.equals("健康页面")) {
				THIRD_LEVEL = "健康百科";
				FOURTH_LEVEL = SUB_TYPE;
			} else if (EVENT.equals("门诊挂号首页")) {
				THIRD_LEVEL = "健康服务";
				FOURTH_LEVEL = "门诊挂号";
			} else if (EVENT.equals("重疾绿通道首页")) {
				THIRD_LEVEL = "健康服务";
				FOURTH_LEVEL = "重疾绿通";
			} else if (EVENT.equals("电话医生首页")) {
				THIRD_LEVEL = "健康服务";
				FOURTH_LEVEL = "电话医生";
			} else if (EVENT.equals("齿科券首页")) {
				THIRD_LEVEL = "健康服务";
				FOURTH_LEVEL = "齿科券";
			} else if (EVENT.equals("体检券首页")) {
				THIRD_LEVEL = "健康服务";
				FOURTH_LEVEL = "体检券";
			}
		}
		APP_ID = "healthService_" + APP_ID;
		return new FactUserBehaviorClue(ROWKEY, USER_ID, USER_TYPE, APP_TYPE, APP_ID, EVENT_TYPE, EVENT, SUB_TYPE,
				VISIT_DURATION, FROM_ID, PAGE_TYPE, FIRST_LEVEL, SECOND_LEVEL, THIRD_LEVEL, FOURTH_LEVEL, VISIT_TIME,
				VISIT_COUNT, CLUE_TYPE, REMARK, USER_NAME);
	}

	/**
	 * 解析CallSkillLabel
	 */
	public static TempNetTeleCallSkill analysisCallSkillLabel(String rowKey, String userid, String apptype,
			String appid, String event, String subtype, String label, String customVal, String visitcount,
			String visittime, String visitduration) {

		String fromPage = "";
		String callSkillId = "";
		String callSkillType = "";
		String createdUserId = "";
		String firstLevel = "";
		String secondLevel = "";
		String thirdLevel = "";
		String fourthLevel = "";

		Pattern p = Pattern.compile("(.*):\"(.*)\"");
		String regex = "\",";
		String substr = "\";";
		label = label.replaceAll(regex, substr);// ,->;
		String[] data = label.split(";");

		if (apptype.equalsIgnoreCase("system")) {
			for (int i = 0; i < data.length; i++) {
				Matcher m = p.matcher(data[i]);
				if (m.find()) {
					if (m.group(1).equalsIgnoreCase("fromPage") && m.group(2) != null) {
						fromPage = m.group(2).toString();
					} else if (m.group(1).equalsIgnoreCase("callSkillId") && m.group(2) != null) {
						callSkillId = m.group(2).toString();
					} else if (m.group(1).equalsIgnoreCase("callSkillType") && m.group(2) != null) {
						callSkillType = m.group(2).toString();
					} else if (m.group(1).equalsIgnoreCase("createdUserId") && m.group(2) != null) {
						createdUserId = m.group(2).toString();
					} else if (m.group(1).equalsIgnoreCase("firstLevel") && m.group(2) != null) {
						firstLevel = m.group(2).toString();
					} else if (m.group(1).equalsIgnoreCase("secondLevel") && m.group(2) != null) {
						secondLevel = m.group(2).toString();
					} else if (m.group(1).equalsIgnoreCase("thirdLevel") && m.group(2) != null) {
						thirdLevel = m.group(2).toString();
					} else if (m.group(1).equalsIgnoreCase("fourthLevel") && m.group(2) != null) {
						fourthLevel = m.group(2).toString();
					}
				}
			}
		}
		return new TempNetTeleCallSkill(rowKey, userid, appid, event, fromPage, callSkillId, callSkillType,
				createdUserId, visitcount, visittime, visitduration, firstLevel, secondLevel, thirdLevel, fourthLevel);
	}

	/**
	 * 解析Mobile ACCOUNT WORTH SEARCH Lable
	 */
	public static FactUserBehaviorClue analysisMobileAccountWorthSearchLable(String ROWKEY, String USER_ID,
			String APP_TYPE, String APP_ID, String EVENT, String SUB_TYPE, String LABEL, String CUSTOM_VAL,
			String VISIT_COUNT, String VISIT_TIME, String VISIT_DURATION, String FROM_ID) {
		// String className = "";
		String USER_TYPE = "MEM";
		String PAGE_TYPE = "查询";
		String EVENT_TYPE = "load";
		// to do 以后根据具体需求设定 start
		String FIRST_LEVEL = "官网会员行为";
		String SECOND_LEVEL = "客服保全类";
		String THIRD_LEVEL = "";
		String FOURTH_LEVEL = "";
		// to do 以后根据具体需求设定 end
		String REMARK = "";
		String CLUE_TYPE = "2";
		String USER_NAME = "";
		if (APP_ID.equals("app001")) {
			Map<String, String> codeValueMap = initAppPageCodeAndClueTypeMap();
			String pageCode = EVENT.trim() + SUB_TYPE.trim();
			String thirdAndFourthLevel = codeValueMap.get(pageCode);
			if (thirdAndFourthLevel != null) {
				THIRD_LEVEL = thirdAndFourthLevel.split(":")[0];
				FOURTH_LEVEL = thirdAndFourthLevel.split(":")[1];
			}
		} else if (APP_ID.equals("clue_H5_website_001")) {
			if (EVENT.equals("账户价格查询页面首页")) {
				switch (SUB_TYPE) {
				case "泰康e理财A款投资连结保险":
					THIRD_LEVEL = "投连产品价格查询";
					FOURTH_LEVEL = "e理财A价格查询";
					break;
				case "泰康e理财B款投资连结保险":
					THIRD_LEVEL = "投连产品价格查询";
					FOURTH_LEVEL = "e理财B价格查询";
					break;
				case "泰康e理财C款投资连结保险":
					THIRD_LEVEL = "投连产品价格查询";
					FOURTH_LEVEL = "e理财C价格查询";
					break;
				case "泰康e理财D款投资连结保险":
					THIRD_LEVEL = "投连产品价格查询";
					FOURTH_LEVEL = "e理财D价格查询";
					break;
				case "泰康开泰稳利账户":
					THIRD_LEVEL = "投连产品价格查询";
					FOURTH_LEVEL = "泰康开泰价格查询";
					break;
				case "放心理财投资连结保险":
					THIRD_LEVEL = "投连产品价格查询";
					FOURTH_LEVEL = "放心理财价格查询";
					break;
				case "赢家理财B款终身寿险":
					THIRD_LEVEL = "投连产品价格查询";
					FOURTH_LEVEL = "赢家理财B价格查询";
					break;
				}
			} else if (EVENT.equals("净值公布") && SUB_TYPE.equals("账户查看")) {
				Pattern p = Pattern.compile("(.*):\"(.*)\"");
				String regex = "\",";
				String substr = "\";";
				LABEL = LABEL.replaceAll(regex, substr);// ,->;
				String[] data = LABEL.split(";");
				String productName = "";
				for (int i = 0; i < data.length; i++) {
					Matcher m = p.matcher(data[i]);
					if (m.find()) {
						if (m.group(1).equalsIgnoreCase("productName")) {
							productName = m.group(2).trim();
						}
					}
				}
				switch (productName) {
				case "泰康e理财终身寿险(万能型)":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "泰康e理财终身寿险";
					break;
				case "泰康附加智慧之选两全保险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "泰康附加智慧之选";
					break;
				case "泰康附加财富赢家定期寿险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "泰康附加财富赢家定期寿";
					break;
				case "爱家赢家终身寿险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "爱家赢家终身寿";
					break;
				case "卓越人生终身寿险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "卓越人生终身寿";
					break;
				case "泰康稳健理财C款终身寿险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "泰康稳健理财C款";
					break;
				case "泰康稳健理财B款终身寿险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "泰康稳健理财B款";
					break;
				case "泰康致富理财终身寿险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "泰康致富理财终身寿";
					break;
				case "放心理财财富版终身寿险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "放心理财财富版";
					break;
				case "放心理财经典版终身寿险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "放心理财经典版";
					break;
				case "附加赢家定期寿险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "附加赢家定期寿";
					break;
				case "稳健理财两全保险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "稳健理财两全";
					break;
				case "附加赢家(2007)定期寿险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "附加赢家2007定期寿";
					break;
				case "卓越财富(2007)终身寿险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "卓越财富2007终身寿";
					break;
				case "卓越财富B款终身寿险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "卓越财富B款终身寿";
					break;
				case "泰康人寿卓越财富终身寿险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "卓越财富终身寿";
					break;
				case "放心理财终身寿险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "放心理财终身寿";
					break;
				case "团体年金保险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "团体年金";
					break;
				case "稳健理财终身寿险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "稳健理财终身寿";
					break;
				case "金账户年金保险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "金账户年金";
					break;
				case "积极成长年金保险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "积极成长年金";
					break;
				case "日日鑫年金保险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "日日鑫年金";
					break;
				case "幸福成长年金保险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "幸福成长年金";
					break;
				case "泰康附加黑钻账户两全保险（万能型）":
					THIRD_LEVEL = "万能产品价格查询";
					FOURTH_LEVEL = "泰康附加黑钻账户两全";
					break;
				}
			}
		}
		return new FactUserBehaviorClue(ROWKEY, USER_ID, USER_TYPE, APP_TYPE, APP_ID, EVENT_TYPE, EVENT, SUB_TYPE,
				VISIT_DURATION, FROM_ID, PAGE_TYPE, FIRST_LEVEL, SECOND_LEVEL, THIRD_LEVEL, FOURTH_LEVEL, VISIT_TIME,
				VISIT_COUNT, CLUE_TYPE, REMARK, USER_NAME);
	}

	/**
	 * 解析Wap Service Page Lable
	 */
	public static FactUserBehaviorClue analysisWapServicePageLable(String ROWKEY, String USER_ID, String APP_TYPE,
			String APP_ID, String EVENT, String SUB_TYPE, String LABEL, String CUSTOM_VAL, String VISIT_COUNT,
			String VISIT_TIME, String VISIT_DURATION, String FROM_ID) {
		// String className = "";
		String USER_TYPE = "MEM";
		String PAGE_TYPE = "查询";
		String EVENT_TYPE = "load";
		// to do 以后根据具体需求设定 start
		String FIRST_LEVEL = "";
		String SECOND_LEVEL = "";
		String THIRD_LEVEL = "";
		String FOURTH_LEVEL = "";
		// to do 以后根据具体需求设定 end
		String REMARK = "";
		String CLUE_TYPE = "2";
		String USER_NAME = "";
		if (SUB_TYPE != null && !SUB_TYPE.isEmpty()) {
			EVENT_TYPE = "click";
		}
		return new FactUserBehaviorClue(ROWKEY, USER_ID, USER_TYPE, APP_TYPE, APP_ID, EVENT_TYPE, EVENT, SUB_TYPE,
				VISIT_DURATION, FROM_ID, PAGE_TYPE, FIRST_LEVEL, SECOND_LEVEL, THIRD_LEVEL, FOURTH_LEVEL, VISIT_TIME,
				VISIT_COUNT, CLUE_TYPE, REMARK, USER_NAME);
	}

	private static Map<String, String> initAppPageCodeAndClueTypeMap() {
		Map<String, String> appPageCodeAndClueTypeMap = new HashMap<String, String>();
		appPageCodeAndClueTypeMap.put("money3", "投连盈亏查询:积极成长型账户");
		appPageCodeAndClueTypeMap.put("money4", "投连盈亏查询:稳健收益型账户");
		appPageCodeAndClueTypeMap.put("money5", "投连盈亏查询:进取型账户");
		appPageCodeAndClueTypeMap.put("money6", "投连盈亏查询:货币避险型账户");
		appPageCodeAndClueTypeMap.put("money7", "投连盈亏查询:优选成长型账户");
		appPageCodeAndClueTypeMap.put("money8", "投连盈亏查询:开泰稳利型账户");
		appPageCodeAndClueTypeMap.put("money9", "投连盈亏查询:五年定期保证收益账户");
		appPageCodeAndClueTypeMap.put("money10", "投连盈亏查询:创新动力型投资账户");
		appPageCodeAndClueTypeMap.put("money11", "投连盈亏查询:平衡配置型投资账户");
		appPageCodeAndClueTypeMap.put("money14", "投连产品价格查询:e理财A价格查询");
		appPageCodeAndClueTypeMap.put("money15", "投连产品价格查询:e理财B价格查询");
		appPageCodeAndClueTypeMap.put("money16", "投连产品价格查询:e理财C价格查询");
		appPageCodeAndClueTypeMap.put("money17", "投连产品价格查询:e理财D价格查询");
		appPageCodeAndClueTypeMap.put("money18", "投连产品价格查询:泰康开泰价格查询");
		appPageCodeAndClueTypeMap.put("money19", "投连产品价格查询:放心理财价格查询");
		appPageCodeAndClueTypeMap.put("money20", "投连产品价格查询:赢家理财B价格查询");
		appPageCodeAndClueTypeMap.put("money22", "万能产品价格查询:泰康e理财终身寿险");
		appPageCodeAndClueTypeMap.put("money23", "万能产品价格查询:泰康附加智慧之选");
		appPageCodeAndClueTypeMap.put("money24", "万能产品价格查询:泰康附加财富赢家定期寿");
		appPageCodeAndClueTypeMap.put("money25", "万能产品价格查询:爱家赢家终身寿");
		appPageCodeAndClueTypeMap.put("money26", "万能产品价格查询:卓越人生终身寿");
		appPageCodeAndClueTypeMap.put("money27", "万能产品价格查询:泰康稳健理财C款");
		appPageCodeAndClueTypeMap.put("money28", "万能产品价格查询:泰康稳健理财B款");
		appPageCodeAndClueTypeMap.put("money29", "万能产品价格查询:泰康致富理财终身寿");
		appPageCodeAndClueTypeMap.put("money30", "万能产品价格查询:放心理财财富版");
		appPageCodeAndClueTypeMap.put("money31", "万能产品价格查询:放心理财经典版");
		appPageCodeAndClueTypeMap.put("money32", "万能产品价格查询:附加赢家定期寿");
		appPageCodeAndClueTypeMap.put("money33", "万能产品价格查询:稳健理财两全");
		appPageCodeAndClueTypeMap.put("money34", "万能产品价格查询:附加赢家2007定期寿");
		appPageCodeAndClueTypeMap.put("money35", "万能产品价格查询:卓越财富2007终身寿");
		appPageCodeAndClueTypeMap.put("money36", "万能产品价格查询:卓越财富B款终身寿");
		appPageCodeAndClueTypeMap.put("money37", "万能产品价格查询:卓越财富终身寿");
		appPageCodeAndClueTypeMap.put("money38", "万能产品价格查询:放心理财终身寿");
		appPageCodeAndClueTypeMap.put("money39", "万能产品价格查询:团体年金");
		appPageCodeAndClueTypeMap.put("money40", "万能产品价格查询:稳健理财终身寿");
		appPageCodeAndClueTypeMap.put("money41", "万能产品价格查询:金账户年金");
		appPageCodeAndClueTypeMap.put("money42", "万能产品价格查询:积极成长年金");
		appPageCodeAndClueTypeMap.put("money43", "万能产品价格查询:日日鑫年金");
		appPageCodeAndClueTypeMap.put("money44", "万能产品价格查询:幸福成长年金");
		appPageCodeAndClueTypeMap.put("money45", "万能产品价格查询:泰康附加钻石账户年金");
		appPageCodeAndClueTypeMap.put("money46", "万能产品价格查询:泰康附加黑钻账户两全");
		return appPageCodeAndClueTypeMap;
	}

	private static Map<String, String> initAppHealthServiceMap() {
		Map<String, String> appPageCodeAndClueTypeMap = new HashMap<String, String>();
		appPageCodeAndClueTypeMap.put("health3", "健康服务:门诊挂号");
		appPageCodeAndClueTypeMap.put("health4", "健康服务:重疾绿通");
		appPageCodeAndClueTypeMap.put("health5", "健康服务:电话医生");
		appPageCodeAndClueTypeMap.put("health6", "健康服务:齿科券");
		appPageCodeAndClueTypeMap.put("health7", "健康服务:体检券");
		appPageCodeAndClueTypeMap.put("health9", "健康百科:常见疾病");
		appPageCodeAndClueTypeMap.put("health10", "健康百科:常用药");
		appPageCodeAndClueTypeMap.put("health11", "健康百科:急救手册");
		appPageCodeAndClueTypeMap.put("health12", "健康百科:定点医院");
		return appPageCodeAndClueTypeMap;
	}

	public static TempUserBehaviorTeleWap analysisCarLable(String rowKey, String userid, String apptype, String appid,
			String event, String subtype, String label, String customVal, String visitcount, String visittime,
			String visitduration, String fromId) {

		String classifyName = "";
		String lrtID = "";
		String lrtName = "";
		String classID = "";
		// String className = "";
		String thirdLevel = "";
		String fourthLevel = "";
		String userType = "";
		String finalUserId = "";
		Pattern p = Pattern.compile("(.*):(.*)");
		String regex = "\"";
		String substr = "";
		label = label.replaceAll(regex, substr);
		String[] data = label.split(",");
		thirdLevel = "产品详情页";
		for (int i = 0; i < data.length; i++) {
			Matcher m = p.matcher(data[i]);
			if (m.find()) {
				if (m.group(1).equalsIgnoreCase("riskCode") || m.group(1).equalsIgnoreCase("productCode")) {
					lrtID = m.group(2);
					fourthLevel = lrtID;
				} else if (m.group(1).equalsIgnoreCase("classifyName")) {
					classifyName = m.group(2);
				} else if (m.group(1).equalsIgnoreCase("riskName")) {
					lrtName = m.group(2);
				}
			}
		}

		if (apptype.equalsIgnoreCase("app")) {
			userType = "app";
		} else if (apptype.equalsIgnoreCase("wap")) {
			userType = "wap";
		} else if (apptype.equalsIgnoreCase("pc")) {
			userType = "pc";
		} else {
			userType = "wechat";
		}

		finalUserId = userid;
		return new TempUserBehaviorTeleWap(rowKey, finalUserId, apptype, appid, event, subtype, lrtID, classID,
				visitcount, visittime, visitduration, classifyName, lrtName, thirdLevel, fourthLevel, userType, fromId);

	}
}
