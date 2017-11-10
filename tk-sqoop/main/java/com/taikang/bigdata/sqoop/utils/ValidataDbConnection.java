package com.taikang.bigdata.sqoop.utils;

import com.taikang.bigdata.sqoop.domain.LinkBean;
import com.tkonline.common.db.util.StringUtil;

public class ValidataDbConnection {
	
	public static boolean validata(LinkBean bean){
		//TODO 密码在此需要进行解密处理
		bean.setPassword(StringUtil.decrpt(bean.getPassword()));
		return true;
	}
	
}
