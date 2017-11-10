package com.tk.bigdata.java_bin.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.yaml.snakeyaml.Yaml;

import com.tk.bigdata.java_bin.domain.DbBean;
import com.tk.bigdata.java_bin.utils.ConfigUtils;
import com.tk.bigdata.java_bin.wechat.WechatMessageUtil;

public class Demo15_ConfigValidate {
	
	public static void main(String[] args) {
//		//测试配置文件
//		if(null == args || args.length <=0){
//			args = new String[1];
//			args[0] = "C:\\Users\\itw_meisf\\Desktop\\tables_count_20171020.yml";
//		}
//	
		
		
		
		if(args.length == 0){
			System.out.println("参数错误, 未识别配置文件路径!");
			return ;
		}
		File f = new File(args[0]);
		if(!f.exists()){
			System.out.println("配置文件路径有误, 文件不存在!");
			return ;
		}
		try {
			if(f.getName().endsWith(".yml")){
				DbBean dbBean =new Yaml().loadAs(new FileInputStream(f), DbBean.class);
		        System.out.println("加载表个数: " + ConfigUtils.loadingConfig(dbBean).size());
				String yamlStr = new String(readYaml(f));
				if(yamlStr.contains("，")){
					throw new Exception("配置文件中包含中文\"，\"");
				}
			}else if(f.getName().endsWith(".config")){
				System.out.println("微信文件校验结果: " + WechatMessageUtil.openids);
			}else{
				throw new Exception(String.format("未知文件格式![%s]", f.getAbsolutePath()));
			}
			System.out.println(String.format("文件校验通过![%s]", f.getAbsolutePath()));
		} catch (Exception e) {
			System.out.println("配置文件格式错误!!!");
			System.out.println(e.getMessage());
		}
	}

	private static byte[] readYaml(File f) throws FileNotFoundException,
			IOException {
		InputStream input =new FileInputStream(f);
		byte[] b = new byte[(int) f.length()];
		input.read(b);
		input.close();
		return b;
	}
	
}
