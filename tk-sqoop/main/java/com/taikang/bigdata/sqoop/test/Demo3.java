package com.taikang.bigdata.sqoop.test;

import java.io.FileInputStream;
import java.util.List;

import org.yaml.snakeyaml.Yaml;

import com.taikang.bigdata.sqoop.domain.DbBean;
import com.taikang.bigdata.sqoop.domain.SqoopBean;
import com.taikang.bigdata.sqoop.utils.ConfigUtils;
import com.taikang.bigdata.sqoop.utils.ProcessUtils;
import com.taikang.bigdata.sqoop.utils.SqoopProcessUtils;

public class Demo3 {
	
	public static void main(String[] args) throws Exception {
		if(args.length >= 2){
			System.setProperty("basic_path", args[0]);
			System.setProperty("config_path", args[1]);
		}else if(args.length >= 1){
			System.setProperty("config_path", args[0]);
		}else{
			System.out.println("没有获取配置文件, 程序已终止!");
			return ;
		}
		
		Yaml yaml = new Yaml();
		DbBean bean = yaml.loadAs(new FileInputStream(System.getProperty("config_path")), DbBean.class);
		List<SqoopBean> ls = ConfigUtils.loadingConfig(bean);
		System.out.println("加载表个数: " + ls.size());
		for (SqoopBean sqoopBean : ls) {
			String commond = SqoopProcessUtils.renderCommond(sqoopBean);
			ProcessUtils.execProcess(sqoopBean.getName(), commond);
		}
		System.out.println("启动任务结束!");
	}
	
}
