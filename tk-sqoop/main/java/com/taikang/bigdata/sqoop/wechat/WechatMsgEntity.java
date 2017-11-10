package com.taikang.bigdata.sqoop.wechat;

import java.util.Map;


public class WechatMsgEntity {

	/**templateId*/
	private String templateId;
	/**url*/
	private String url;
	/**data*/
	private Map<String, Map<String, String>> data;
	
	public String getTemplateId() {
		return templateId;
	}
	public void setTemplateId(String templateId) {
		this.templateId = templateId;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public Map<String, Map<String, String>> getData() {
		return data;
	}
	public void setData(Map<String, Map<String, String>> data) {
		this.data = data;
	}
	@Override
	public String toString()
	{
		return "ClaimSendBean [" + (templateId != null ? "templateId=" + templateId + ", " : "")
				+ (url != null ? "url=" + url + ", " : "")
				+ (data != null ? "data=" + data : "") + "]";
	}
	

}
