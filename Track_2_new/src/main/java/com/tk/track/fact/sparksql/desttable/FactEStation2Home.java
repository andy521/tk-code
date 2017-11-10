package com.tk.track.fact.sparksql.desttable;

/**
 * @author itw_zhaocq
 * @Date:2016年9月22日
 */
public class FactEStation2Home {

	//    private String USER_ID;        //用于用户身份的标识 memberid
	//    private String APP_TYPE;       //标记日志log的来源
	//    private String APP_ID;          //javaweb002 e站到家
	//    private String EVENT;     //用户行为,对应浏览页面
	//    private String SUBTYPE;   //用户具体行为
	//    private String VISIT_COUNT;    //页面点击次数
	//    private String VISIT_TIME;     //浏览时间 clienttime
	//    private String VISIT_DURATION; //浏览页面的时长
	//    private String CUSTOM_VAL;//随机参数
	//    private String FROM_ID;//来源
	//    private String POLICYNO;//保单号
	//    private String INFO;//页面访问结果
	
	private String CUSTOMERID;   //用户CUSTOMERID
	private String USERID;    //用户USERID
	private String NAME;        //用于用户姓名
	private String GENDER;       //用于用户性别
	private String USER_AGE;     //用于用户年龄
	private String BIRTHDAY;     //用户生日
	private String APP_ID;     //javaweb002 e站到家
	private String TRACK_INFO; //浏览轨迹
	private String VISIT_COUNT;//浏览次数
	private String VISIT_TIME;//最后一次浏览时间
	private String VISIT_DURATION;//浏览时长
	private String LIFE_POLICYINFO;//线索参数 关联寿险保单信息
	private String RELATION_INFO;//家庭信息
	private String LABEL;//Label 浏览结果
	
	private String CLUE_TYPE;//线索类型
	private String FIRST_INDEX_LEVEL;//推送一级分类
	private String SECOND_INDEX_LEVEL;//推送二级分类
	private String THIRD_INDEX_LEVEL;//推送三级分类
	private String FOURTH_INDEX_LEVEL;//推送四级分类
	private String NOTE_KEY;//备注信息
	
	private String TRACK_TIME;//线索生成时间
	
	public FactEStation2Home(String cUSTOMERID, String uSERID, String nAME, String gENDER, String uSER_AGE,
			String bIRTHDAY, String aPP_ID, String tRACK_INFO, String vISIT_COUNT, String vISIT_TIME,
			String vISIT_DURATION, String lIFE_POLICYINFO, String rELATION_INFO, String lABEL,
			String cLUE_TYPE,String fIRST_INDEX_LEVEL, String sECOND_INDEX_LEVEL, String tHIRD_INDEX_LEVEL, String fOURTH_INDEX_LEVEL,
			String tRACK_TIME) {
		super();
		CUSTOMERID = cUSTOMERID;
		USERID = uSERID;
		NAME = nAME;
		GENDER = gENDER;
		USER_AGE = uSER_AGE;
		BIRTHDAY = bIRTHDAY;
		APP_ID = aPP_ID;
		TRACK_INFO = tRACK_INFO;
		VISIT_COUNT = vISIT_COUNT;
		VISIT_TIME = vISIT_TIME;
		VISIT_DURATION = vISIT_DURATION;
		LIFE_POLICYINFO = lIFE_POLICYINFO;
		RELATION_INFO = rELATION_INFO;
		LABEL = lABEL;

		CLUE_TYPE = cLUE_TYPE;
		FIRST_INDEX_LEVEL = fIRST_INDEX_LEVEL;
		SECOND_INDEX_LEVEL = sECOND_INDEX_LEVEL;
		THIRD_INDEX_LEVEL = tHIRD_INDEX_LEVEL;
		FOURTH_INDEX_LEVEL = fOURTH_INDEX_LEVEL;
		TRACK_TIME = tRACK_TIME;

	}
	public String getNAME() {
		return NAME;
	}
	public void setNAME(String nAME) {
		NAME = nAME;
	}
	public String getGENDER() {
		return GENDER;
	}
	public void setGENDER(String gENDER) {
		GENDER = gENDER;
	}
	public String getUSER_AGE() {
		return USER_AGE;
	}
	public void setUSER_AGE(String uSER_AGE) {
		USER_AGE = uSER_AGE;
	}
	public String getBIRTHDAY() {
		return BIRTHDAY;
	}
	public void setBIRTHDAY(String bIRTHDAY) {
		BIRTHDAY = bIRTHDAY;
	}
	public String getCUSTOMERID() {
		return CUSTOMERID;
	}
	public void setCUSTOMERID(String cUSTOMERID) {
		CUSTOMERID = cUSTOMERID;
	}
	public String getUSERID() {
		return USERID;
	}
	public void setUSERID(String uSERID) {
		USERID = uSERID;
	}
	public String getAPP_ID() {
		return APP_ID;
	}
	public void setAPP_ID(String aPP_ID) {
		APP_ID = aPP_ID;
	}
	public String getTRACK_INFO() {
		return TRACK_INFO;
	}
	public void setTRACK_INFO(String tRACK_INFO) {
		TRACK_INFO = tRACK_INFO;
	}
	public String getVISIT_COUNT() {
		return VISIT_COUNT;
	}
	public void setVISIT_COUNT(String vISIT_COUNT) {
		VISIT_COUNT = vISIT_COUNT;
	}
	public String getVISIT_TIME() {
		return VISIT_TIME;
	}
	public void setVISIT_TIME(String vISIT_TIME) {
		VISIT_TIME = vISIT_TIME;
	}
	public String getVISIT_DURATION() {
		return VISIT_DURATION;
	}
	public void setVISIT_DURATION(String vISIT_DURATION) {
		VISIT_DURATION = vISIT_DURATION;
	}
	public String getLIFE_POLICYINFO() {
		return LIFE_POLICYINFO;
	}
	public void setLIFE_POLICYINFO(String lIFE_POLICYINFO) {
		LIFE_POLICYINFO = lIFE_POLICYINFO;
	}
	public String getRELATION_INFO() {
		return RELATION_INFO;
	}
	public void setRELATION_INFO(String rELATION_INFO) {
		RELATION_INFO = rELATION_INFO;
	}
	public String getCLUE_TYPE() {
		return CLUE_TYPE;
	}
	public void setCLUE_TYPE(String cLUE_TYPE) {
		CLUE_TYPE = cLUE_TYPE;
	}
	public String getFIRST_INDEX_LEVEL() {
		return FIRST_INDEX_LEVEL;
	}
	public void setFIRST_INDEX_LEVEL(String fIRST_INDEX_LEVEL) {
		FIRST_INDEX_LEVEL = fIRST_INDEX_LEVEL;
	}
	public String getSECOND_INDEX_LEVEL() {
		return SECOND_INDEX_LEVEL;
	}
	public void setSECOND_INDEX_LEVEL(String sECOND_INDEX_LEVEL) {
		SECOND_INDEX_LEVEL = sECOND_INDEX_LEVEL;
	}
	public String getTHIRD_INDEX_LEVEL() {
		return THIRD_INDEX_LEVEL;
	}
	public void setTHIRD_INDEX_LEVEL(String tHIRD_INDEX_LEVEL) {
		THIRD_INDEX_LEVEL = tHIRD_INDEX_LEVEL;
	}
	public String getFOURTH_INDEX_LEVEL() {
		return FOURTH_INDEX_LEVEL;
	}
	public void setFOURTH_INDEX_LEVEL(String fOURTH_INDEX_LEVEL) {
		FOURTH_INDEX_LEVEL = fOURTH_INDEX_LEVEL;
	}
	public String getNOTE_KEY() {
		return NOTE_KEY;
	}
	public void setNOTE_KEY(String nOTE_KEY) {
		NOTE_KEY = nOTE_KEY;
	}
	/**
	 * @return the lABEL
	 */
	public String getLABEL() {
		return LABEL;
	}
	/**
	 * @param lABEL the lABEL to set
	 */
	public void setLABEL(String lABEL) {
		LABEL = lABEL;
	}
	/**
	 * @return the tRACK_TIME
	 */
	public String getTRACK_TIME() {
		return TRACK_TIME;
	}
	/**
	 * @param tRACK_TIME the tRACK_TIME to set
	 */
	public void setTRACK_TIME(String tRACK_TIME) {
		TRACK_TIME = tRACK_TIME;
	}

}
