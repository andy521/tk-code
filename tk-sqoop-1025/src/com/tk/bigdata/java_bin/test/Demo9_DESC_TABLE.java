package com.tk.bigdata.java_bin.test;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tk.bigdata.java_bin.domain.DescTableaBean;
import com.tkonline.common.db.util.StringUtil;

public class Demo9_DESC_TABLE {
	
	private static QueryRunner runner = new QueryRunner();
	
	public static Connection connect(String url, String username, String password) throws Exception{
		password = StringUtil.decrpt(password);
//		Connection conn = Demo7_JDBC.getConnectionOracle(url, username, password);
		Connection conn = Demo7_JDBC.getConnectionMysql(url, username, password);
		return conn;
	}
	
	public static List<Map<String, Object>> getTableDesc(Connection conn, String owner, String tablename) throws SQLException{
		/*String sql1 = "SELECT  * FROM all_tab_columns WHERE owner=? and table_name=?";
		String sql2 = "SELECT  * FROM all_tab_columns WHERE table_name=?";
		if(owner == null){
			return runner.query(conn, sql2, new MapListHandler(), tablename);
		}else{
			return runner.query(conn, sql1, new MapListHandler(), owner, tablename);
		}*/
		return runner.query(conn, "DESC " + tablename, new MapListHandler());
	}
	
	public static void main(String[] args) throws Exception {
		PrintWriter pw = new PrintWriter("tables_structure.properties");
		
//		String json = "[{\"owner\":{\"password\":\"E22D106D117B32BE26B24BE57A51DF10B127B32DE51B43C90DF15DE70\",\"schema\":\"ICD\",\"url\":\"jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.135.8.15)(PORT=1521))(LOAD_BALANCE=yes)(FAILOVER=on))(CONNECT_DATA=(SERVICE_NAME=ipcc)))\",\"username\":\"hadoopuser\"},\"tables\":[\"T_DAYLOG_AGENTATTENDANCE\",\"T_DAYLOG_AGENTCALL\",\"T_DAYLOG_RPT_AGENTOPRINFO\",\"T_DAYLOG_RPT_CALLANALYSIS\"]},{\"owner\":{\"password\":\"E22D106D117B32BE26B24BE57A51DF10B127B32DE51B43C90DF15DE70\",\"schema\":\"WEBMANAGE\",\"url\":\"jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=db.shop.online.taikang.com)(PORT=1521))(LOAD_BALANCE=yes)(FAILOVER=on))(CONNECT_DATA=(SERVICE_NAME=tkwebdb)))\",\"username\":\"hadoopuser\"},\"tables\":[\"PRODUCT\"]},{\"owner\":{\"password\":\"95B83BE5D84D95C90A121BE92C86BF72CE83CF96CE23DF77CE88A79\",\"schema\":\"TKINSURE\",\"url\":\"jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=db.fcb.online.taikang.com)(PORT=1521))(LOAD_BALANCE=yes)(FAILOVER=on))(CONNECT_DATA=(SERVICE_NAME=tkorahy)))\",\"username\":\"ORAHYREADER\"},\"tables\":[\"AL_INSURANT\",\"AL_INSURE\",\"AL_POLICYHOLDER\",\"AL_T_INSURE\",\"COOP_APPLYINSURE\",\"COOP_CUSTOMER\"]},{\"owner\":{\"password\":\"E57AE70A55BE75DF79CF12BF128D48C52BF35AF28D83A1A44CE59B91\",\"schema\":\"UPICCORE\",\"url\":\"jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=db1.pis.online.taikang.com)(PORT=1521))(LOAD_BALANCE=yes)(FAILOVER=on))(CONNECT_DATA=(SERVICE_NAME=PISCORE)))\",\"username\":\"upiccore\"},\"tables\":[\"GCADJUSTMENTCHARGE\",\"GCADJUSTMENTITEM\",\"GCADJUSTMENTMAIN\",\"GCADJUSTMENTPERSONFEE\",\"GCESTIMATELOSS\",\"GCPREPAYMAIN\",\"GCREGISTMAIN\",\"GCREGISTPOLICY\",\"GGCODE\",\"GGCOMPANY\",\"GGDYNAMICITEMCONFIG\",\"GGKIND\",\"GGRISKCLASS\",\"GSINTERMEDIARYMAIN\",\"GUENDORENDORHEAD\",\"GUPOLICYCOINSURANCE\",\"GUPOLICYCOPYCOINSURANCE\",\"GUPOLICYCOPYCOMMISSION\",\"GUPOLICYCOPYITEMDYNAMIC\",\"GUPOLICYCOPYITEMKIND\",\"GUPOLICYCOPYMAIN\",\"GUPOLICYCOPYRISK\",\"GUPOLICYITEMACCI\",\"GUPOLICYITEMMOTOR\",\"GURENEWALMAIN\",\"PROPERTY_CUSTOMER\",\"GUPOLICYRELATEDPARTY\",\"GUPOLICYMAIN\",\"GGRISK\",\"GCCLAIMMAIN\",\"GUPOLICYRISKRELATEDPARTY\",\"GUPOLICYCOPYITEMACCILIST\",\"COOP_QIHOOCUSTOMER\",\"T_IMPORT_RELATED_DIDI\",\"T_IMPORT_POLICY_RELATION_DIDI\",\"GSCHANNELPLANCODE\",\"GUPOLICYITEMACCILIST\"]},{\"owner\":{\"password\":\"E22D106D117B32BE26B24BE57A51DF10B127B32DE51B43C90DF15DE70\",\"schema\":\"TELE\",\"url\":\"jdbc:oracle:thin:@//db1.tele.online.taikang.com:1521/tkntl\",\"username\":\"hadoopuser\"},\"tables\":[\"ASSIGN_MODE\",\"ASSIGN_RECORD\",\"ASSIGN_RECORD_DETAIL\",\"ASSIGN_RULE\",\"ASSIGN_RULE_DETAIL\",\"CALL_RECORDS\",\"CUSTOMER_GROUP\",\"CUSTOMER_GROUP_DETAIL\",\"CUSTOMER_INFO\",\"DYNAMIC_USER_GROUP\",\"MODE_CUSTOMER_GROUP\",\"MODE_RULE\",\"MODE_USER_GROUP\",\"SALES_EVENT\",\"SALES_LEADS\",\"SALES_TRADE\",\"SEMI_AUTO_ASSIGN\",\"STATIC_USER_GROUP\",\"USER_GROUP\"]},{\"owner\":{\"password\":\"F124CE5DF27C71AE128DF2D111DF106A76D1DF37C83CF112AE47CF100DF8\",\"schema\":\"LIFEUSER\",\"url\":\"jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=oracle1inner.bi.online.taikang.com)(PORT=1521)))(CONNECT_DATA=(SID=tkpi)))\",\"username\":\"hadoopuser\"},\"tables\":[\"TOL_BPM_DTL_SRC\",\"TOL_CLAIM_DTL_SRC\",\"TOL_BZT_DTL_SRC\",\"D_CLIENTORG\",\"BI_MULU_CLUE\"]},{\"owner\":{\"password\":\"E22D106D117B32BE26B24BE57A51DF10B127B32DE51B43C90DF15DE70\",\"schema\":\"TKCTRIP\",\"url\":\"jdbc:oracle:thin:@//db.ctrip.online.taikang.com:1521/tkct\",\"username\":\"hadoopuser\"},\"tables\":[\"COOP_APPLYINSURE\",\"COOP_CUSTOMER\",\"P_CUSTOMER\",\"P_INSURANT\",\"P_LIFEINSURE\"]},{\"owner\":{\"password\":\"69CF120DF67BF121DE62B126DF87DF37DF69AF120AE69CE34A73B53DE2AE54\",\"schema\":\"UPICCORE\",\"url\":\"jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.129.119.152)(PORT=1521))(LOAD_BALANCE=yes)(FAILOVER=on))(CONNECT_DATA=(SERVICE_NAME=PISCOREF)))\",\"username\":\"pisreader\"},\"tables\":[\"GUPOLICYRISK\",\"GUPOLICYITEMMOTOR\"]},{\"owner\":{\"password\":\"4CF16DF7D107AF50BF26BE55A35CF96BF123DF97D16AE4B111B37D64\",\"schema\":\"TKINSURE\",\"url\":\"jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=lifedb.tkol.taikang.com)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=lifedb2.tkol.taikang.com)(PORT=1521))(LOAD_BALANCE=yes)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=tkorasol)))\",\"username\":\"hadoopuser\"},\"tables\":[\"APP_PRODUCT\",\"CLAIM_COMMONINFO\",\"CLAIM_INFO\",\"CLM_CASE\",\"CLM_INQUIRE\",\"COOP_CHILD_INSURE\",\"CS_INFOCHANGE\",\"CS_LOG\",\"CS_MEMBER_COOP\",\"D_ATTRIBUTION\",\"D_BRANCH\",\"D_CIDTYPE\",\"D_COUPON\",\"D_INFOCHANGEITEM\",\"D_LIFERISKTYPE\",\"D_LIFERISKTYPECOMP\",\"D_PAYWAY\",\"D_RELATION\",\"D_SEAT\",\"D_WORKTYPE\",\"D_WORKTYPENEW\",\"HEALTH_ITEM\",\"POLICY_SPL_DATA\",\"PROVINCE_CITY_AREA\",\"P_APPLYFORM\",\"P_BENEFICIARY\",\"P_BI_DATAEXCHANGE\",\"P_BONUS\",\"P_BONUSHISTORY\",\"P_CLIENTORG\",\"P_COUPONDETAIL\",\"P_COUPONLRTCONFIG\",\"P_CSCPOLICYNO\",\"P_CUSTOMER\",\"P_GOODSEXCHANGE\",\"P_GOODSEXCHANGEDETAIL\",\"P_INFOCHANGE\",\"P_INFOCHANGEPROPERTY\",\"P_INPUTPAY\",\"P_INSURANT\",\"P_INVESTINSUREADD\",\"P_LIFEINSURE\",\"P_LIFEINSURECOMP\",\"P_MEMBER\",\"P_MEMBERPOLICY\",\"P_MEMBER_RIGHTS\",\"P_MEMBER_UPDATE\",\"P_MESSAGEINFONEW_ACT\",\"P_PAYRECORD\",\"P_POTENTIALCUSTOMER\",\"P_RECOMMENDAPPLIST\",\"P_RECOMMENDPOLICYLIST\",\"P_RENEWAL_PROMPT\",\"P_SEATINVITE\",\"P_TRADE\",\"P_TRAVELINSURESPECIALDATA\",\"SALES_SMS\",\"SALES_SMS_MODEL\",\"TB_GROUNDCHECK\",\"TB_HEALTHINFORM_DETAIL\",\"TB_MANUAL_CHECK\",\"T_INSURELIST\",\"T_INSURELIST_EC\",\"VOTE_VOTERRESULT\",\"WECHAT_POLICY\",\"WECHAT_SUBSCRIPTION\"]}]";
		String json = "[{\"owner\":{\"password\":\"112C20DE87DF116BE23AF17DE27D61CF113C49AE38AF79C54C2C114BE1\",\"schema\":\"\",\"url\":\"jdbc:mysql://10.147.32.206:3306/tkpi?useUnicode=true&amp;characterEncoding=utf8\",\"username\":\"qryuser\"},\"tables\":[\"S_TRADE\",\"S_VERIFYLOG\"]}]";
		JSONArray arr = JSONArray.parseArray(json);
		for (int i = 0; i < arr.size(); i++) {
			JSONObject jsonObject = arr.getJSONObject(i);
			JSONObject owner = jsonObject.getJSONObject("owner");
			try {
				Connection conn = connect(owner.getString("url"), owner.getString("username"), owner.getString("password"));
				JSONArray tables = jsonObject.getJSONArray("tables");
				
				String username = owner.getString("username");
				String schema = owner.getString("schema");
				
				for (int j = 0; j < tables.size(); j++) {
					String tablename = tables.getString(j);
					String structure = JSONObject.toJSONString(getTableDesc(conn, schema, tablename));
					if("[]".equals(structure)){
						structure = JSONObject.toJSONString(getTableDesc(conn, null, tablename));
					}
					String res = username+"."+schema+"."+tablename+"="+structure;
					pw.println(res);
				}
			} catch (Exception e) {
				System.out.println("数据库链接失败: " + owner);
			}
			
		}
		
		pw.flush();
		pw.close();
		
		/*
		String url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=db.fcb.online.taikang.com)(PORT=1521))(LOAD_BALANCE=yes)(FAILOVER=on))(CONNECT_DATA=(SERVICE_NAME=tkorahy)))";
		String username = "ORAHYREADER";
		String password = "95B83BE5D84D95C90A121BE92C86BF72CE83CF96CE23DF77CE88A79";
		Map<String, List<String>> tableMap = new HashMap<>();
		List<String> tables = new ArrayList<>();
		tables.add("COOP_APPLYINSURE");
		tables.add("COOP_CUSTOMER");
		tableMap.put("TKCOOP", tables);
		
		Connection conn = connect(url, username, password);
		
		for (Entry<String, List<String>> entry : tableMap.entrySet()) {
			String owner = entry.getKey();
			List<String> val = entry.getValue();
			for (String tablename : val) {
				System.out.println(owner+"."+tablename+"="+JSONObject.toJSONString(getTableDesc(conn, owner, tablename)));
			}
		}
		*/
	}
	
	public static void getTables() throws Exception {
		Connection conn = Demo7_JDBC.getConnectionMysql("jdbc:mysql:///bigdata?serverTimezone=UTC", "root", "root");
		List<Map<String, Object>> list = runner.query(conn, "SELECT url, username, `password`, UPPER(`schema`) `schema`, tableName FROM tables0923 WHERE url LIKE '%mysql%';", new MapListHandler());
		Map<DescTableaBean, List<String>> tableMap = new HashMap<>();
		
		for (Map<String, Object> map : list) {
			String url = (String)map.get("url");
//			url = URLEncoder.encode(url, "UTF-8");
			
			String username = (String)map.get("username");
			String password = (String)map.get("password");
			String schema = (String)map.get("schema");
			String tableName = (String)map.get("tableName");
			
			DescTableaBean bean = new DescTableaBean(url, username, password, schema);
			
			List<String> val = null;
			if(tableMap.containsKey(bean)){
				val = tableMap.get(bean);
			}else{
				val = new ArrayList<>();
				tableMap.put(bean, val);
			}
			
			val.add(tableName);
		}
		
		List<Object> ls = new ArrayList<>();
		for (Entry<DescTableaBean, List<String>> entry : tableMap.entrySet()) {
			Map<String, Object> map = new HashMap<>();
			map.put("owner", entry.getKey());
			map.put("tables", entry.getValue());
			ls.add(map);
		}
		
		System.out.println(JSONObject.toJSONString(ls));
	}
	
}
