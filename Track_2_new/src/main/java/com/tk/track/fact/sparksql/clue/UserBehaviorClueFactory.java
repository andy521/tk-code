package com.tk.track.fact.sparksql.clue;

import com.tk.track.fact.sparksql.desttable.FactUserBehaviorClue;

import java.io.Serializable;

/**
 * Created by t-chenhao01 on 2017/1/13.
 */
public abstract class  UserBehaviorClueFactory implements Serializable {
    private static final long serialVersionUID = 6495945876829993746L;

    public  abstract FactUserBehaviorClue analysisLabelRDD(String ROWKEY, String USER_ID, String APP_TYPE,
                                                String APP_ID, String EVENT, String SUB_TYPE,
                                                String LABEL, String CUSTOM_VAL, String VISIT_COUNT,
                                                String VISIT_TIME, String VISIT_DURATION, String FROM_ID);


    public abstract String getParamSql();
    public abstract String fullFillUserNameByUserInfoTable();
}
