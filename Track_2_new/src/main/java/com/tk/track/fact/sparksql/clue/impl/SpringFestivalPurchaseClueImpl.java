package com.tk.track.fact.sparksql.clue.impl;

import com.tk.track.fact.sparksql.clue.UserBehaviorClueFactory;
import com.tk.track.fact.sparksql.desttable.FactUserBehaviorClue;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by t-chenhao01 on 2017/1/17.
 * 年货节活动线索
 */
public class SpringFestivalPurchaseClueImpl extends UserBehaviorClueFactory {
    private static final long serialVersionUID = 7847121924644613225L;

    @Override
    public FactUserBehaviorClue analysisLabelRDD(String ROWKEY, String USER_ID, String APP_TYPE, String APP_ID, String EVENT, String SUB_TYPE, String LABEL, String CUSTOM_VAL, String VISIT_COUNT, String VISIT_TIME, String VISIT_DURATION, String FROM_ID) {
        String USER_TYPE = "MEM";
        String PAGE_TYPE = "活动"; //
        String EVENT_TYPE = "load";
        //to do 以后根据具体需求设定 start
        String FIRST_LEVEL = "渠道客户转化";
        String SECOND_LEVEL = "泰康在线公众号";
        String THIRD_LEVEL = "微信活动";
        String FOURTH_LEVEL = "2017年货节";
        //to do 以后根据具体需求设定 end
        String REMARK = "";
        String CLUE_TYPE = "2";
        String USER_NAME = "";

        Map<String,String> labelMap=analyKVStr2Map(LABEL);
        String fromId=RMNull(labelMap.get("fromId"));//fromId 有更新，以label中传来为准
        String telephone=RMNull(labelMap.get("telephone"));//fromId 有更新，以label中传来为准
        String address=RMNull(labelMap.get("address"));//fromId 有更新，以label中传来为准
        if(!"".equals(telephone)){
            REMARK=REMARK+"电话:"+telephone+";";
        }
        if(!"".equals(address)){
            REMARK=REMARK+"地址:"+address+";";
        }

/*
dataList.add("访问页面:"+handleNullData(USER_EVENT) +
						";访问次数:"+VISIT_COUNT +
						";访问时间:"+handleNullData(VISIT_TIME) +
						";停留时间:"+handleNullData(VISIT_DURATION) +
						";来源:"+handleNullData(DESCRIPTION));
 */



        return new FactUserBehaviorClue(ROWKEY, USER_ID, USER_TYPE, APP_TYPE,
                APP_ID, EVENT_TYPE, EVENT, SUB_TYPE, VISIT_DURATION, fromId,
                PAGE_TYPE, FIRST_LEVEL, SECOND_LEVEL, THIRD_LEVEL, FOURTH_LEVEL,
                VISIT_TIME, VISIT_COUNT, CLUE_TYPE, REMARK, USER_NAME);
    }

    @Override
    public String getParamSql() {
        return " AND subtype<>'活动首页' and subtype<>'年货节首页'  ";
    }

    @Override
    public String fullFillUserNameByUserInfoTable() {
        String hql = "SELECT TMP_A.ROWKEY,"
                +"       TMP_A.FROM_ID,"
                +"       TMP_A.APP_TYPE,"
                +"       TMP_A.APP_ID,"
                +"       TMP_A.USER_ID AS OPEN_ID,"
                +"       TMP_A.USER_TYPE,"
                +"       TMP_A.EVENT_TYPE,"
                +"       TMP_A.EVENT,"
                +"       TMP_A.SUB_TYPE,"
                +"       TMP_A.VISIT_DURATION,"
                +"       UM.NAME AS USER_NAME,"
                +"       TMP_A.PAGE_TYPE,"
                +"       TMP_A.FIRST_LEVEL,"
                +"       TMP_A.SECOND_LEVEL,"
                +"       TMP_A.THIRD_LEVEL,"
                +"       TMP_A.FOURTH_LEVEL,"
                +"       TMP_A.VISIT_TIME,"
                +"       TMP_A.VISIT_COUNT,"
                +"       TMP_A.CLUE_TYPE,"
                +"       UM.MEMBER_ID AS USER_ID,"
                +"       CONCAT(TMP_A.REMARK,'姓名：',"
                +"              NVL(UM.NAME, ''),"
                +"              '\073访问页面：',"
                +"              NVL(TMP_A.SUB_TYPE, ''),"
                +"              '\073首次访问时间：',"
                +"              NVL(TMP_A.VISIT_TIME, ''),"
                +"              '\073访问次数：',"
                +"              NVL(TMP_A.VISIT_COUNT, ''),"
                +"              '\073访问时长：',"
                +"              NVL(TMP_A.VISIT_DURATION, '')) AS REMARK"
                +"  FROM TMP_ANALYSISLABEL TMP_A"
                +"  JOIN USER_INFO_UNIQUE UM ON TMP_A.USER_ID = UM.OPEN_ID ";
        return hql;
    }

    private Map<String,String> analyKVStr2Map(String label){
        String[] strs=label.split(",");
        Map<String,String> labelMap=new HashMap<>();
        if(strs!=null && strs.length>0){
            for (String s : strs) {
                String[] kvStrArr=s.split(":");
                if(kvStrArr.length>1){
                    labelMap.put(kvStrArr[0].replace("\"",""),kvStrArr[1].replace("\"",""));
                }
            }
        }
        return labelMap;
    }

    private String RMNull(String str){
        if(StringUtils.isBlank(str))
            return "";
        else
            return str;
    }


}
