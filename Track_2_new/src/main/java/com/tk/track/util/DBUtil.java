package com.tk.track.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.tkonline.common.db.util.StringUtil;

public class DBUtil {
	
	public static Connection getConnection(String URL, String USER, String PASSWORD) {
		Connection conn = null;
		PASSWORD = StringUtil.decrpt(PASSWORD);
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection(URL, USER, PASSWORD);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	public static void closeConnection(ResultSet rs, PreparedStatement st, Connection conn) {
		if(rs != null) {
    		try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
    	}
		if (st != null) {
			try {
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
