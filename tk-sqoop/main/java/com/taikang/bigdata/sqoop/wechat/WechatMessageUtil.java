package com.taikang.bigdata.sqoop.wechat;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.jsoup.Connection;
import org.jsoup.Jsoup;

public class WechatMessageUtil {
	private final String COLOR = "#173177";
	private final String MSG_FROM = "泰康在线大数据部-->新环境";

	private final String appId;
	private final String templateId;
	private final String wechatInterfactUrl;
	
	public WechatMessageUtil() {
		this.appId = "wxcd7143c00e5bb6f7";
		this.templateId = "KqlKNUsHmpUXTnYczriiDwFos7olDKAhwT22V6uBG6g";
		this.wechatInterfactUrl = "http://ecsw.taikang.com/tkmap/wechat/templateMsg/send.do?";
	}
	
	private static WechatMessageUtil bean = null;
	public static String openids = null;
	static{
		bean = new WechatMessageUtil();
		
		try (InputStream in = new FileInputStream("wechat.config")){
			Properties prop = new Properties();
			prop.load(in);
			
			openids = prop.getProperty("openids");
			
		} catch (Exception e) {
			e.printStackTrace();
			openids = "omCIGj-MY74-DfVGAhqU8OwRvPlY";
		}
		System.out.println("微信openids: " + openids);
		
	}
	
	public static void send(String openids, String content){
		bean.sendMsg(openids, content, null);
	}
	
	public static void send(WechatTableBean tableBean){
		bean.sendMsg(openids, tableBean.toString(), null);
	}
	
	private Boolean sendMsg(String openId, String content, String detailPageUrl) {
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
			
			conn.requestBody(msg.toString());
			conn.post();
			
			result = conn.response().body();
			
			if (result != null) {
				System.out.println(result);
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
	
	public static void main(String[] args) throws Exception {
		System.out.println(123);
	}
	
}
