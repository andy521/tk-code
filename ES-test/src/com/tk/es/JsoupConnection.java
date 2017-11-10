package com.tk.es;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.Connection.Method;

import java.io.IOException;

public class JsoupConnection {
    
	public  static String get(String url) throws Exception {
        return execute(url, null, Connection.Method.GET);
    }
    
    public static String delete(String url) {
    	return execute(url, null, Connection.Method.DELETE);
    }


    public static boolean head(String url) {
		try {
			Connection.Response response = connection(url, null, Method.HEAD);
			if(null != response && response.statusCode() == 200){
	    		return true;
	    	}
		} catch (IOException e) {
		}
        return false;
    }

    public static String post(String url, String body) throws Exception {
        return execute(url, body, Connection.Method.POST);
    }

    public static String put(String url, String body) throws Exception {
        return execute(url, body, Connection.Method.PUT);
    }

    public static String execute(String url, String body, Connection.Method method) {
        Connection.Response response = null;
		try {
			response = connection(url, body, method);
	            return response.body();
		} catch (IOException e) {
			e.printStackTrace();
		}
        return null;
    }

    private static Connection.Response connection(String url, String body, Connection.Method method) throws IOException {
        return Jsoup.connect(url)
                .method(method)
                .ignoreContentType(true)
                .requestBody(body)
                .execute();
    }
}
