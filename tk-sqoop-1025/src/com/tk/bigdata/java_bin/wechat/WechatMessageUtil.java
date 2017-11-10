package com.tk.bigdata.java_bin.wechat;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.jsoup.Connection;
import org.jsoup.Jsoup;

import com.alibaba.fastjson.JSONObject;

public class WechatMessageUtil {
//	taikang.wechat.appId=wxcd7143c00e5bb6f7
//	taikang.wechat.templateId=KqlKNUsHmpUXTnYczriiDwFos7olDKAhwT22V6uBG6g
//	taikang.wechat.interfaceurl=http://ecsw.taikang.com/tkmap/wechat/templateMsg/send.do?
//	taikang.wechat.openIds=omCIGj-MY74-DfVGAhqU8OwRvPlY,omCIGj_e79zyRsNP_ve7-7DNNoLc
	private final String COLOR = "#173177";
	private final String MSG_FROM = "泰康在线大数据部-->新环境";

	private final String appId;
	private final String templateId;
	private final String wechatInterfactUrl;
//	private final String failedMsgPath;
//	private final String succeedMsgPath;
	
	public WechatMessageUtil() {
		this.appId = "wxcd7143c00e5bb6f7";
		this.templateId = "KqlKNUsHmpUXTnYczriiDwFos7olDKAhwT22V6uBG6g";
		this.wechatInterfactUrl = "http://ecsw.taikang.com/tkmap/wechat/templateMsg/send.do?";
//		this.failedMsgPath = "../failedMsgPath/";
//		this.succeedMsgPath = "../succeedMsgPath/";
	}
	
	private static WechatMessageUtil bean = null;
	public static String adminopenids = null;
	public static String openids = null;
	static{
		bean = new WechatMessageUtil();
		
		System.out.println("微信配置文件路径 --> " + new File(System.getProperty("wechat_config")).getAbsolutePath());
		
		try (InputStream in = new FileInputStream(System.getProperty("wechat_config"))){
			Properties prop = new Properties();
			prop.load(in);
			
			openids = prop.getProperty("openids");
			adminopenids = prop.getProperty("adminopenids");
			
		} catch (Exception e) {
			e.printStackTrace();
			openids = "omCIGj_3ddQm2xC2LRoGi1bxwx7I";
		}
		System.out.println("微信openids: " + openids);
		System.out.println("adminopenids: " + adminopenids);
		
//		TimeoutDetection.openids = openids;
	}
	
	public static void send(String openids, String content){
		bean.sendMsg(openids, content, null, true);
	}
	
	public static void send(WechatTableBean tableBean){
		bean.sendMsg(openids, tableBean.toString(), null, true);
	}
	
	private Boolean sendMsg(String openId, String content, String detailPageUrl, boolean f) {
		if(openId == null) {
			System.out.println("微信ID为空, 暂未发送");
			return false;
		}
		if(openId.contains(",")){
			String[] ids = openId.split(",");
			for (String id : ids) {
				if(id == null || id.length() != 28) continue;
				sendMsgByOne(id, content, detailPageUrl, f);
			}
		}else{
			if(openId.length() != 28) return false;
			sendMsgByOne(openId, content, detailPageUrl, f);
		}
		return true;
	}
	
	private Boolean sendMsgByOne(String openId, String content, String detailPageUrl, boolean f) {
		String url = String.format("%sappId=%s&openId=%s", wechatInterfactUrl, appId, openId);
		System.out.println(url);
		String result = null;
		try {
			WechatMsgEntity msg = getMsgData(content, detailPageUrl);

			Connection conn = Jsoup.connect(url)
								   .ignoreContentType(true)
								   .timeout(30 * 1000)
								   .header("Content-Type", "application/json")
								   .postDataCharset("UTF-8");
			
			conn.requestBody(JSONObject.toJSONString(msg));
			conn.post();
			
			result = conn.response().body();
			
			if (result != null) {
				System.out.println(result);
				
				if(f){
					try {//{"rspCode":0,"rspDesc":"请求成功"}
						if(JSONObject.parseObject(result).getInteger("rspCode") != 0){//微信通知失败
							sendMsg(adminopenids, "导出监控微信发送失败\n 请求openids:"+openId+"\n反馈结果:"+result, null, false);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				
				return true;
			} else {
				System.out.println(String.format("--wechat--error--,接口调用无返回==%s,%s,%s", openId, content, detailPageUrl));
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	private WechatMsgEntity getMsgData(String content, String detailPageUrl) {
		// post的数据
		Map<String, Map<String, String>> data = new HashMap<>();

		// post数据中的data
		Map<String, String> dk = new HashMap<String, String>();
		dk.put("color", COLOR);
		dk.put("value", MSG_FROM);
		data.put("keyword1", dk);

		dk = new HashMap<String, String>();
		dk.put("color", COLOR);
		dk.put("value", content);
		data.put("keyword2", dk);

		WechatMsgEntity msg = new WechatMsgEntity();
		msg.setTemplateId(templateId);
		msg.setUrl(detailPageUrl);
		msg.setData(data);

		return msg;
	}
	
	public static String show(List<WechatTableBean> list) throws Exception{
		StringBuffer sb = new StringBuffer("");
		String se = "";
		int max = 0;
		for (WechatTableBean tbean : list) {
			Field[] fields = WechatTableBean.class.getDeclaredFields();
			for (Field field : fields) {
				Method method = WechatTableBean.class.getMethod("get" + toUpperCase4Index(field.getName()));
				String val = (String) method.invoke(tbean, new Object[]{});
				int len = val.length();
				
				if(len > max){
					max = len;
				}
			}
		}
		
		{
			String val = "";
			while(val.length() < max){
				val += "*";
			}
			
			se = "*";
			for (int j = 0; j < WechatTableBean.class.getDeclaredFields().length; j++) {
				se += "*" + val + "***";
			}
			se += "\n";
			sb.append(se);
		}
		
		for (WechatTableBean tbean : list) {
			Field[] fields = WechatTableBean.class.getDeclaredFields();
			
			sb.append("*");
			
			for (Field field : fields) {
				Method method = WechatTableBean.class.getMethod("get" + toUpperCase4Index(field.getName()));
				String val = (String) method.invoke(tbean, new Object[]{});
				
				boolean fb = false;
				while(val.length() < max){
					fb = !fb;
					val = fb ? val + " " : " " + val;
				}
				sb.append(" ").append(val).append("  *");
			}
			
			sb.append("\n");
		}

		sb.append(se);
		return "\n" + sb.toString();
	}
	
	private static String toUpperCase4Index(String string) {  
	    char[] methodName = string.toCharArray();  
	    methodName[0] = toUpperCase(methodName[0]);  
	    return String.valueOf(methodName);  
	} 
	
	private static char toUpperCase(char chars) {  
	    if (97 <= chars && chars <= 122) {  
	        chars ^= 32;  
	    }  
	    return chars;  
	} 
	
	public static void main(String[] args) throws Exception {
//		List<WechatTableBean> list = new ArrayList<>();
//		list.add(new WechatTableBean("QxsdfsdfdHx", "2", "1", "2"));
//		System.out.println(new WechatMessageUtil().sendMsg("omCIGj_3ddQm2xC2LRoGi1bxwx7I", show(list), null));
//		send(new WechatTableBean("name", "12", "23", "time", Status.success));
//		System.out.println(123);
//		send(openids, "这是一条测试消息");
		System.out.println(13);
		
	}
	
}
