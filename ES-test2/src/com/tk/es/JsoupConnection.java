package com.tk.es;

import java.io.IOException;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.Connection.Method;
import org.jsoup.Connection.Response;

public class JsoupConnection {
	
	public static String get(String url) throws Exception {
		return execute(url, null, Connection.Method.GET);
	}
	
	public static String delete(String url) throws Exception {
		return execute(url, null, Connection.Method.DELETE);
	}
	
	public static boolean head(String url) throws Exception {
		try {
			Connection.Response resp = connection(url, null, Method.HEAD);
			if(resp != null && resp.statusCode() == 200){
				return true;
			}
		} catch (Exception e) {
		}
		return false;
	}
	
	public static String post(String url, String body) throws Exception {
		return execute(url, body, Connection.Method.POST);
	}
	
	public static String put(String url, String body) throws Exception {
		return execute(url, body, Connection.Method.PUT);
	}

	private static String execute(String url, String object, Method method) {
		Connection.Response resp = null;
		try {
			resp = connection(url, object, method);
			return resp.body();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private static Response connection(String url, String body, Method method) throws IOException {
		return Jsoup.connect(url)
					.method(method)
					.ignoreContentType(true)
					.requestBody(body)
					.execute();
	}
	
}
