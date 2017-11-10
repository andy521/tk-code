package com.tk.bigdata.java_bin.test;

import com.tkonline.common.db.util.StringUtil;

public class Demo4 {
	public static void main(String[] args) {
		//123DE60C66CE70AE71A45B77BE27D8DF101BE53AF42CF80D49C45D73
		String password = "123DE60C66CE70AE71A45B77BE27D8DF101BE53AF42CF80D49C45D73";
		String decrpt = StringUtil.decrpt(password);
		System.out.println(decrpt);
	}
}
