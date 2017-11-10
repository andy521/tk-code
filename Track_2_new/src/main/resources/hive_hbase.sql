use tkoldb;
CREATE external TABLE if not exists P_MEMBER (
 key string,
 MEMBER_ID string,
 MEMTYPE_ID string,
 MEMBER_NAME string,
 MEMBER_PASSWORD string,
 MEMBER_EMAIL string,
 MEMBER_NICKNAME string,
 MEMBER_REGDATE string,
 MEMBER_MODDATE string,
 MEMBER_CA string,
 MEMBER_HIT string,
 MEMBER_ANSWER string,
 CUSTOMER_ID string,
 LIA_POLICYNO string,
 MEMBER_TYPE string,
 CIF_NO string,
 MEMBER_MOBILE string,
 MEMBER_BIRTH string,
 MEMBER_CIDNUMBER string,
 MEMBER_IFEMAIL string,
 MEMBER_IP string,
 MEMBER_LOGONTIME string,
 MEMBER_GENDER string,
 MEMBER_REGISTERMODE string,
 MEMBER_VERIFYCODE string,
 MEMBER_VERIFYDATE string,
 PARTYID string,
 COMPUTEFLAG string,
 COMPANY_NO string,
 BRANCH_NO string,
 MEMBER_POLICY string,
 MEMBER_FLAG string,
 MEMBER_FROMID string,
 MEMBER_ACTID string,
 MEMBER_TELEPHONE string,
 MEMBER_CONTACTFLAG string,
 EMAIL_SUFFIX string,
 MEMBER_REALNAME string,
 CIDTYPE_ID string,
 POTENTIAL_PARTYID string,
 MEMBER_SID string,
 LOGIN_EMAILVERIFY string,
 LOGIN_MOBILE string,
 LOGIN_MOBILEVERIFY string,
 SYN_CID string,
 UPDATEDATE string,
 CID_PROVINCE string,
 CID_CITY string, 
 MOBILE_PROVINCE string,
 MOBILE_CITY string,
 FLAG string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:MEMBER_ID,info:MEMTYPE_ID,info:MEMBER_NAME,info:MEMBER_PASSWORD,info:MEMBER_EMAIL,info:MEMBER_NICKNAME,info:MEMBER_REGDATE,info:MEMBER_MODDATE,info:MEMBER_CA,info:MEMBER_HIT,info:MEMBER_ANSWER,info:CUSTOMER_ID,info:LIA_POLICYNO,info:MEMBER_TYPE,info:CIF_NO,info:MEMBER_MOBILE,info:MEMBER_BIRTH,info:MEMBER_CIDNUMBER,info:MEMBER_IFEMAIL,info:MEMBER_IP,info:MEMBER_LOGONTIME,info:MEMBER_GENDER,info:MEMBER_REGISTERMODE,info:MEMBER_VERIFYCODE,info:MEMBER_VERIFYDATE,info:PARTYID,info:COMPUTEFLAG,info:COMPANY_NO,info:BRANCH_NO,info:MEMBER_POLICY,info:MEMBER_FLAG,info:MEMBER_FROMID,info:MEMBER_ACTID,info:MEMBER_TELEPHONE,info:MEMBER_CONTACTFLAG,info:EMAIL_SUFFIX,info:MEMBER_REALNAME,info:CIDTYPE_ID,info:POTENTIAL_PARTYID,info:MEMBER_SID,info:LOGIN_EMAILVERIFY,info:LOGIN_MOBILE,info:LOGIN_MOBILEVERIFY,info:SYN_CID,info:UPDATEDATE,info:CID_PROVINCE,info:CID_CITY,info:MOBILE_PROVINCE,info:MOBILE_CITY,info:FLAG") TBLPROPERTIES("hbase.table.name"="P_MEMBER");


CREATE external TABLE if not exists P_CUSTOMER (
 key string,
 NAME string,
 CUSTOMER_ID string,
 ISLIVE string,
 WORKTYPE_ID string,
 CIDTYPE_ID string,
 CID_NUMBER string,
 BIRTHDAY string,
 GENDER string,
 MOBILE string,
 BP string,
 EMAIL string,
 EMAIL2 string,
 COMMONTELEPHONE string,
 COMMONADDRESS string,
 COMMONADDRESSPOSTALCODE string,
 HOMETELEPHONE string,
 HOMEADDRESS string,
 HOMEADDRESSPOSTALCODE string,
 UNITNAME string,
 UNITTELEPHONE string,
 UNITADDRESS string,
 UNITADDRESSPOSTALCODE string,
 ORGANIZATION_ID string,
 HIGH string,
 HEAVY string,
 CITY_ID string,
 HOMEPAGE string,
 ISPERSON string,
 NATIONAL string,
 LINKMAN_NAME string,
 FAX string,
 ENGLISHNAME string,
 AGE string,
 CSC_CLNTNUM string,
 CREATEDATE string,
 CREATEOPTYPE string,
 LASTUPDATE string,
 UPDATEOPTYPE string,
 STATUS string,
 LOCK_STATUS string,
 WORKTYPE_ID_BAS string,
 CID_PROVINCE string,
 CID_CITY string, 
 MOBILE_PROVINCE string,
 MOBILE_CITY string,
 FLAG string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:NAME,info:CUSTOMER_ID,info:ISLIVE,info:WORKTYPE_ID,info:CIDTYPE_ID,info:CID_NUMBER,info:BIRTHDAY,info:GENDER,info:MOBILE,info:BP,info:EMAIL,info:EMAIL2,info:COMMONTELEPHONE,info:COMMONADDRESS,info:COMMONADDRESSPOSTALCODE,info:HOMETELEPHONE,info:HOMEADDRESS,info:HOMEADDRESSPOSTALCODE,info:UNITNAME,info:UNITTELEPHONE,info:UNITADDRESS,info:UNITADDRESSPOSTALCODE,info:ORGANIZATION_ID,info:HIGH,info:HEAVY,info:CITY_ID,info:HOMEPAGE,info:ISPERSON,info:NATIONAL,info:LINKMAN_NAME,info:FAX,info:ENGLISHNAME,info:AGE,info:CSC_CLNTNUM,info:CREATEDATE,info:CREATEOPTYPE,info:LASTUPDATE,info:UPDATEOPTYPE,info:STATUS,info:LOCK_STATUS,info:WORKTYPE_ID_BAS,info:CID_PROVINCE,info:CID_CITY,info:MOBILE_PROVINCE,info:MOBILE_CITY,info:FLAG") TBLPROPERTIES("hbase.table.name"="P_CUSTOMER");


CREATE external TABLE if not exists CS_MEMBER_COOP(
 key string,
 MEMBER_ID string,
 MEMBER_NAME string,
 COOP_ID string,
 COOP_NAME string,
 COOP_CIDNUMBER string,
 CREATE_TIME string,
 CHANNEL string,
 STATUS string,
 COOP_CIDTYPE string,
 PLATFORM_FLAG string,
 SUBCHANNEL string,
 COOP_DESC string,
 EXT1 string,
 EXT2 string,
 CUSTOMER_CIDNUMBER string,
 COOP_MOBILE string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:MEMBER_ID,info:MEMBER_NAME,info:COOP_ID,info:COOP_NAME,info:COOP_CIDNUMBER,info:CREATE_TIME,info:CHANNEL,info:STATUS,info:COOP_CIDTYPE,info:PLATFORM_FLAG,info:SUBCHANNEL,info:COOP_DESC,info:EXT1,info:EXT2,info:CUSTOMER_CIDNUMBER,info:COOP_MOBILE") TBLPROPERTIES("hbase.table.name"="CS_MEMBER_COOP");


CREATE external TABLE if not exists TB_MANUAL_CHECK(
  key string,
  FORM_ID            string,
  MEMBER_ID          string,
  COMPANY_NO         string,
  BRANCH_NO          string,
  RULE_TYPE          string,
  FE_TIME            string,
  FE_STATUS          string,
  FE_BACK_TIME       string,
  FE_RESULT          string,
  FE_DESC            string,
  FE_RESPONSE        string,
  FE_POLICY_TIME     string,
  FE_POLICY_STATUS   string,
  PARTY_ID           string,
  CSC_CLNTNUM        string,
  SENDMDM_FLAG       string,
  SENDCSC_FLAG       string,
  SENDMDM_DATE       string,
  SENDCSC_DATE       string,
  SENDMDM_STATUS     string,
  SENDCSC_STATUS     string,
  MANUALCHECK_RESULT string,
  TASK_STATUS        string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:FORM_ID,info:MEMBER_ID,info:COMPANY_NO,info:BRANCH_NO,info:RULE_TYPE,info:FE_TIME,info:FE_STATUS,info:FE_BACK_TIME,info:FE_RESULT,info:FE_DESC,info:FE_RESPONSE,info:FE_POLICY_TIME,info:FE_POLICY_STATUS,info:PARTY_ID,info:CSC_CLNTNUM,info:SENDMDM_FLAG,info:SENDCSC_FLAG,info:SENDMDM_DATE,info:SENDCSC_DATE,info:SENDMDM_STATUS,info:SENDCSC_STATUS,info:MANUALCHECK_RESULT,info:TASK_STATUS") TBLPROPERTIES("hbase.table.name"="TB_MANUAL_CHECK");


CREATE external TABLE if not exists TB_GROUNDCHECK(
  key string,
  GROUNDCHECK_ID     int,
  FORM_ID            string,
  MEMBER_ID          string,
  STATUS             string,
  CHECK_RESULT       string,
  CHECK_DESC         string,
  SEND_DATE          string,
  RECEIVE_DATE       string,
  EXAMDATE           string,
  EXAMADDCODE        string,
  EXAMADDSTR         string,
  EXAMREMARK         string,
  VISITDATE          string,
  VISITADDCODE       string,
  VISITADDSTR        string,
  VISITREMARK        string,
  GROUNDCHECKTYPE    string,
  SENDMESSAGE_STATUS string,
  SENDMESSAGE_DATE   string,
  RADOMNUMBER        string,
  MOBILE             string,
  REFUSE_REASON      string,
  CHECK_ADDINFO      string,
  TELECHECK_TYPE     string,
  TEL_RESULT         string,
  TEL_DESC           string,
  TEL_DATE           string,
  REPEAT_PAY         int,
  ILOG_DESC          string,
  FE_RESULT          string,
  TASK_STATUS        string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:GROUNDCHECK_ID,info:FORM_ID,info:MEMBER_ID,info:STATUS,info:CHECK_RESULT,info:CHECK_DESC,info:SEND_DATE,info:RECEIVE_DATE,info:EXAMDATE,info:EXAMADDCODE,info:EXAMADDSTR,info:EXAMREMARK,info:VISITDATE,info:VISITADDCODE,info:VISITADDSTR,info:VISITREMARK,info:GROUNDCHECKTYPE,info:SENDMESSAGE_STATUS,info:SENDMESSAGE_DATE,info:RADOMNUMBER,info:MOBILE,info:REFUSE_REASON,info:CHECK_ADDINFO,info:TELECHECK_TYPE,info:TEL_RESULT,info:TEL_DESC,info:TEL_DATE,info:REPEAT_PAY,info:ILOG_DESC,info:FE_RESULT,info:TASK_STATUS") TBLPROPERTIES("hbase.table.name"="TB_GROUNDCHECK");


CREATE external TABLE if not exists HAOLA_TEST(
key string,
NULLSTRING string,
ID string,
WEIXIN_ID string,
HAOLA_USER_NAME string,
HAOLA_TELPHONE string,
HAOLA_SEX string,
HAOLA_AGE string,
HAOLA_TEST_VERSION string,
HAOLA_TEST_CODE string,
HAOLA_TEST_NAME string,
HAOLA_SUBMIT_DATE string,
HAOLA_SCORE string,
HAOLA_ANSWER string,
CREATE_DATE string,
STATUE string,
HAOLA_IS_AGREE string,
HAOLA_TEST_SORT string,
HAOLA_VERSION_NUM string,
FREEUSE1 string,
FREEUSE2 string,
FREEUSE3 string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:NULLSTRING,info:ID,info:WEIXIN_ID,info:HAOLA_USER_NAME,info:HAOLA_TELPHONE,info:HAOLA_SEX,info:HAOLA_AGE,info:HAOLA_TEST_VERSION,info:HAOLA_TEST_CODE,info:HAOLA_TEST_NAME,info:HAOLA_SUBMIT_DATE,info:HAOLA_SCORE,info:HAOLA_ANSWER,info:CREATE_DATE,info:STATUE,info:HAOLA_IS_AGREE,info:HAOLA_TEST_SORT,info:HAOLA_VERSION_NUM,info:FREEUSE1,info:FREEUSE2,info:FREEUSE3") TBLPROPERTIES("hbase.table.name"="HAOLA_TEST");


CREATE external TABLE if not exists WECHAT_TEST(
key string,
NULLSTRING string,
WEIXIN_ID string,
TEST_CODE string,
TEST_NAME string,
TEST_DESC string,
CREATE_DATE string,
TEST_SUM string,
FREEUSE1 string,
FREEUSE2 string,
FREEUSE3 string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:NULLSTRING,info:WEIXIN_ID,info:TEST_CODE,info:TEST_NAME,info:TEST_DESC,info:CREATE_DATE,info:TEST_SUM,info:FREEUSE1,info:FREEUSE2,info:FREEUSE3") TBLPROPERTIES("hbase.table.name"="WECHAT_TEST");


CREATE external table if not exists GM_FRIENDSHIP(
key string,
NULLSTRING string,
ID string,
WEIXIN_ID string,
FROMWEIXIN_ID string,
CREATE_TIME string,
CHANNEL_WORD string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:NULLSTRING,info:ID,info:WEIXIN_ID,info:FROMWEIXIN_ID,info:CREATE_TIME,info:CHANNEL_WORD") TBLPROPERTIES("hbase.table.name"="GM_FRIENDSHIP");


CREATE external TABLE if not exists POLICY(
key string,
NULLSTRING string,
LRT_ID string,
POLICY_ID string,
PREMIUM string,
APPLY_TIME string,
ACCEPT_TIME string,
START_DATE string,
END_DATE string,
PROPOSAL_ID string,
WECHAT_OPENID string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:NULLSTRING,info:LRT_ID,info:POLICY_ID,info:PREMIUM,info:APPLY_TIME,info:ACCEPT_TIME,info:START_DATE,info:END_DATE,info:PROPOSAL_ID,info:WECHAT_OPENID") TBLPROPERTIES("hbase.table.name"="POLICY");


CREATE external table if not exists COOP_INQUIRE_ANSWER(
key string,
NULLSTRING string,
ANSWER_SEQ string,
POLICY_ID string,
VERSION string,
ANSWER string,
RISK_LEVEL string,
CUST_SERVICE string,
SUBMIT_TIME string,
ANSWER_TIME string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:NULLSTRING,info:ANSWER_SEQ,info:POLICY_ID,info:VERSION,info:ANSWER,info:RISK_LEVEL,info:CUST_SERVICE,info:SUBMIT_TIME,info:ANSWER_TIME") TBLPROPERTIES("hbase.table.name"="COOP_INQUIRE_ANSWER");


CREATE external TABLE if not exists MPEVENT (
 key string,
 APPID string,
 OPENID string,
 EVENTTIME string,
 TYPE string,
 EVENT string,
 EVENTKEY string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:APPID,info:OPENID,info:EVENTTIME,info:TYPE,info:EVENT,info:EVENTKEY") TBLPROPERTIES("hbase.table.name"="MPEVENT");


CREATE external TABLE if not exists RECV (
 key string,
 OPENID string,
 C string,
 FO string,
 DT string,
 D string,
 H string,
 P string,
 N string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:OPENID,info:C,info:FO,info:DT,info:D,info:H,info:P,info:N") TBLPROPERTIES("hbase.table.name"="RECV");


CREATE external TABLE if not exists WECHAT_POLICY(
  key string,
  WEIXIN_ID       string,
  LIA_POLICYNO    string,
  TRADE_BILLNO    string,
  CREATE_TIME     string,
  WECHATRENEWFLAG string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:WEIXIN_ID,info:LIA_POLICYNO,info:TRADE_BILLNO,info:CREATE_TIME,info:WECHATRENEWFLAG") TBLPROPERTIES("hbase.table.name"="WECHAT_POLICY");


CREATE external TABLE if not exists P_LIFEINSURE(
  key string,
  LRT_ID                 string,
  LIA_ID                 string,
  POLICYHOLDER_ID        string,
  TYPE_POLICYHOLDER      string,
  POLICY_EMAIL           string,
  LIA_POLICYNO           string,
  ALTER_PASSWORD         string,
  AGENT_ID               string,
  ORGANIZATION_ID        string,
  LIA_PREMIUM            string,
  LIA_REMAINPREMIUM      string,
  LIA_APPLYTIME          string,
  LIA_ACCEPTTIME         string,
  LIA_VALIDPERIODBEGIN   string,
  LIA_VALIDPERIODWILLEND string,
  LIA_VALIDPERIODEND     string,
  LIA_STATUS             string,
  LIA_STATUSINFO         string,
  LIA_NEEDPAYDATE        string,
  LIA_FROMORDERID        string,
  POLICY_EMAILID         string,
  HANDLE_ID              string,
  LIA_LEGACYNO           string,
  OLD_POLICYNO           string,
  OLD_POLICYNOFROM       string,
  BCHANGE                string,
  IOLDID                 string,
  LIA_POLICYTYPE         string,
  LIA_INSURANTTYPE       string,
  LIA_DELIVERFEE         string,
  LIA_ALTER              string,
  LIA_CANCLE             string,
  LIA_WITHDRAW           string,
  HANDLE_CLIENTMEMBERID  string,
  HANDLE_CLIENTID        string,
  LIA_HELPINFO           string,
  LIA_NEEDINVOICE        string,
  LIA_PRINTINVOICE       string,
  LIA_REALPREMIUM        string,
  PAYWAY_ID              string,
  LIA_BONUSSTATUS        string,
  BONUS_ID               string,
  LIA_BONUSPOINT         string,
  LIA_PAYFLAG            string,
  TRADE_BILLNO           string,
  EBA_SENTFLAG           string,
  PAY_MODE               string,
  PAY_FREQ               string,
  LIA_BONUSGETMETHOD     string,
  LIA_PREMIUMPAYLATER    string,
  LIA_SLIP               string,
  MDM_FLAG               string,
  LIA_SERIALNO           string,
  LIA_SERIALSUBNO        string,
  LIA_ORGID              string,
  BATCH_NO               string,
  CARD_MEMO              string,
  SECOND_SENT_FLAG       string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:LRT_ID,info:LIA_ID,info:POLICYHOLDER_ID,info:TYPE_POLICYHOLDER,info:POLICY_EMAIL,info:LIA_POLICYNO,info:ALTER_PASSWORD,info:AGENT_ID,info:ORGANIZATION_ID,info:LIA_PREMIUM,info:LIA_REMAINPREMIUM,info:LIA_APPLYTIME,info:LIA_ACCEPTTIME,info:LIA_VALIDPERIODBEGIN,info:LIA_VALIDPERIODWILLEND,info:LIA_VALIDPERIODEND,info:LIA_STATUS,info:LIA_STATUSINFO,info:LIA_NEEDPAYDATE,info:LIA_FROMORDERID,info:POLICY_EMAILID,info:HANDLE_ID,info:LIA_LEGACYNO,info:OLD_POLICYNO,info:OLD_POLICYNOFROM,info:BCHANGE,info:IOLDID,info:LIA_POLICYTYPE,info:LIA_INSURANTTYPE,info:LIA_DELIVERFEE,info:LIA_ALTER,info:LIA_CANCLE,info:LIA_WITHDRAW,info:HANDLE_CLIENTMEMBERID,info:HANDLE_CLIENTID,info:LIA_HELPINFO,info:LIA_NEEDINVOICE,info:LIA_PRINTINVOICE,info:LIA_REALPREMIUM,info:PAYWAY_ID,info:LIA_BONUSSTATUS,info:BONUS_ID,info:LIA_BONUSPOINT,info:LIA_PAYFLAG,info:TRADE_BILLNO,info:EBA_SENTFLAG,info:PAY_MODE,info:PAY_FREQ,info:LIA_BONUSGETMETHOD,info:LIA_PREMIUMPAYLATER,info:LIA_SLIP,info:MDM_FLAG,info:LIA_SERIALNO,info:LIA_SERIALSUBNO,info:LIA_ORGID,info:BATCH_NO,info:CARD_MEMO,info:SECOND_SENT_FLAG") TBLPROPERTIES("hbase.table.name"="P_LIFEINSURE");


CREATE external TABLE if not exists P_BENEFICIARY(
key string,
LIAC_ID	string,
LIA_ID	string,
INSURANT_CUSTOMID string,
BENEFICIARY_CUSTOMID string,
CONTACT_INFO string,
RELATIONID_INSURANT	string,
BENE_TYPE string,
BENE_PERCENTAGE	string,
BENE_ID	string,
INSURANT_ID	string,
BENE_MONEY	string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:LIAC_ID,info:LIA_ID,info:INSURANT_CUSTOMID,info:BENEFICIARY_CUSTOMID,info:CONTACT_INFO,info:RELATIONID_INSURANT,info:BENE_TYPE,info:BENE_PERCENTAGE,info:BENE_ID,info:INSURANT_ID,info:BENE_MONEY") TBLPROPERTIES("hbase.table.name"="P_BENEFICIARY");


CREATE external TABLE if not exists P_INSURANT(
key string,
LIAC_ID	string,
PERSON_ID string,
LIAC_COVERAGE string,
CUSTOMER_ID	string,
CONTACT_INFO string,
RELATIONID_POLICYHOLDER	string,
INSURANT_SPECIALTABLE string,
LIA_ID	string,
LRTC_ID	string,
INSURANT_VISA string,
LIAC_PREMIUM string,
INSURANT_FAMILY	string,
EMERGENCYRELATIONPERSON	string,
INSURANT_TAG string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:liac_id,info:PERSON_ID,info:LIAC_COVERAGE,info:CUSTOMER_ID,info:CONTACT_INFO,info:RELATIONID_POLICYHOLDER,info:INSURANT_SPECIALTABLE,info:LIA_ID,info:LRTC_ID,info:INSURANT_VISA,info:LIAC_PREMIUM,info:INSURANT_FAMILY,info:EMERGENCYRELATIONPERSON,info:INSURANT_TAG") TBLPROPERTIES("hbase.table.name"="P_INSURANT");


CREATE external TABLE if not exists P_CLIENTORG(
key string,
ORGANIZATION_ID string,
ORGANIZATION_TYPE string,
ORGANIZATION_NAME string,
ORGANIZATION_SHORTNAME string,
ORGANIZATION_DESC string,
ORGANIZATION_POSTCODE string,
ORGANIZATION_ADDRESS string,
ORGANIZATION_PHONE string,
ORGANIZATION_PHONE2 string,
ORGANIZATION_FAX string,
ORGANIZATION_EMAIL string,
ORGANIZATION_EMAIL2 string,
LINKMAN_ID string,
ACTIVATE_PASSWORD string,
ACTIVEPERIOD_BEGIN string,
ACTIVEPERIOD_END string,
ACTIVE_STATUS string,
PARENT_ID string,
ORGANIZATION_CODE string,
COMPANYORG_ID string,
ORGANIZATION_RESERVE string,
ORG_FLAG string,
PAYWAY string,
COMPANY_NO string,
CSC_ORGNO string,
SALESNAME string,
SALESNUM string,
EBA_ORGNO string,
PAY_SOURCE string,
CERTIFICATION string,
SALES_CHANNEL string,
EBA_ORGNAME string,
BILL99_FLAG string,
MOBILE_CHECK string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:ORGANIZATION_ID,info:ORGANIZATION_TYPE,info:ORGANIZATION_NAME,info:ORGANIZATION_SHORTNAME,info:ORGANIZATION_DESC,info:ORGANIZATION_POSTCODE,info:ORGANIZATION_ADDRESS,info:ORGANIZATION_PHONE,info:ORGANIZATION_PHONE2,info:ORGANIZATION_FAX,info:ORGANIZATION_EMAIL,info:ORGANIZATION_EMAIL2,info:LINKMAN_ID,info:ACTIVATE_PASSWORD,info:ACTIVEPERIOD_BEGIN,info:ACTIVEPERIOD_END,info:ACTIVE_STATUS,info:PARENT_ID,info:ORGANIZATION_CODE,info:COMPANYORG_ID,info:ORGANIZATION_RESERVE,info:ORG_FLAG,info:PAYWAY,info:COMPANY_NO,info:CSC_ORGNO,info:SALESNAME,info:SALESNUM,info:EBA_ORGNO,info:PAY_SOURCE,info:CERTIFICATION,info:SALES_CHANNEL,info:EBA_ORGNAME,info:BILL99_FLAG,info:MOBILE_CHECK") TBLPROPERTIES("hbase.table.name"="P_CLIENTORG");


CREATE external TABLE if not exists P_MEMBERPOLICY(
key string,
MEMBER_ID string,
LIA_POLICYNO string,
TYPE string,
CREATEDATE string,
COMPANYNO string,
PRODUCTNAME string,
PRODUCTCODE string,
LIA_PREMIUM string,
VALIDPERIODBEGIN string,
POLICYSTATE string,
PRODUCTCODESOURCE string,
APPLYFORMTYPE string,
POLICYTYPE string,
BIND_STATUS string,
LASTUPDATE string,
MEMBERPOLICY_ID string,
RENEWAL_PREMIUM string,
NEXT_PAYDATE string,
CHANGE_DATE string,
SYN_FLAG string,
SYN_DATE string,
VALIDPERIOD_END string,
PAY_PERIOD string,
ACCOUNT_VALUE string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:MEMBER_ID,info:LIA_POLICYNO,info:TYPE,info:CREATEDATE,info:COMPANYNO,info:PRODUCTNAME,info:PRODUCTCODE,info:LIA_PREMIUM,info:VALIDPERIODBEGIN,info:POLICYSTATE,info:PRODUCTCODESOURCE,info:APPLYFORMTYPE,info:POLICYTYPE,info:BIND_STATUS,info:LASTUPDATE,info:MEMBERPOLICY_ID,info:RENEWAL_PREMIUM,info:NEXT_PAYDATE,info:CHANGE_DATE,info:SYN_FLAG,info:SYN_DATE,info:VALIDPERIOD_END,info:PAY_PERIOD,info:ACCOUNT_VALUE") TBLPROPERTIES("hbase.table.name"="P_MEMBERPOLICY");


CREATE external TABLE if not exists P_RECOMMENDPOLICYLIST(
key string,
LIA_POLICYNO string,
RECOMMEND_SOURCE string,
LRT_ID string,
LIA_PREMIUM	string,
LIA_ACCEPTTIME string,
POLICYHOLDER_ID	string,
OWNER_ID string,
MEMBER_ID1 string,
MEMBER_ID2 string,
COMPUTEFLAG string,
ISCANCEL string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:LIA_POLICYNO,info:RECOMMEND_SOURCE,info:LRT_ID,info:LIA_PREMIUM,info:LIA_ACCEPTTIME,info:POLICYHOLDER_ID,info:OWNER_ID,info:MEMBER_ID1,info:MEMBER_ID2,info:COMPUTEFLAG,info:ISCANCEL") TBLPROPERTIES("hbase.table.name"="P_RECOMMENDPOLICYLIST");


create external table if not exists P_TRADE(
key string,
TRADE_ID string,
PAYMETHOD_ID string,
PT_ID string,
AGENCY_ID string,
TRADE_BILLNO string,
TRADE_PONO string,
TRADE_COST string,
TRADE_DATE string,
TRADE_SUCCEED string,
TRADE_RECORD string,
TRADE_SIGNMETHOD string,
TRADE_SIGNATURE string,
TRADE_MSG string,
PAYURL_REMARK string,
TRADE_REMARK string,
OLD_BILLNO string,
SEND_STATUS string,
PRDODUCT_CODE string,
PAYWAY_ID string,
FORM_ID string,
OPERATOR_ID string,
ERROR_TYPE string,
ERROR_CODE string,
ERROR_DESC string,
NOTIFIED string,
MERCHANT_ID string,
EXT1 string,
EXT2 string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:TRADE_ID,info:PAYMETHOD_ID,info:PT_ID,info:AGENCY_ID,info:TRADE_BILLNO,info:TRADE_PONO,info:TRADE_COST,info:TRADE_DATE,info:TRADE_SUCCEED,info:TRADE_RECORD,info:TRADE_SIGNMETHOD,info:TRADE_SIGNATURE,info:TRADE_MSG,info:PAYURL_REMARK,info:TRADE_REMARK,info:OLD_BILLNO,info:SEND_STATUS,info:PRDODUCT_CODE,info:PAYWAY_ID,info:FORM_ID,info:OPERATOR_ID,info:ERROR_TYPE,info:ERROR_CODE,info:ERROR_DESC,info:NOTIFIED,info:MERCHANT_ID,info:EXT1,info:EXT2") TBLPROPERTIES("hbase.table.name"="P_TRADE");


CREATE external TABLE if not exists P_RENEWAL_PROMPT(
key string,
PROMPT_ID string,
LIA_POLICYNO string,
PROMPT_TYPE string,
SEND_DATE string,
SEND_FLAG string,
EMAIL string,
BEGINDATE string,
ENDDATE string,
CUSTOMER_ID string,
EMAIL_ID string,
CSC_POLICYNO string,
SUBCOMPANY_NO string,
SUBCOMPANY_NAME string,
PLCSTATE string,
PLCSTATEDESC string,
PAYMETHOD string,
NEXTPAYAMT string,
NEXTPAYDATE string,
RENEWALSTATE string,
RENEWALSTATEDESC string,
RECEIPT_EMAILID string,
PAYDATE string,
BANKNAME string,
PAY_CHANNEL string,
SMS_STATUS string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:PROMPT_ID,info:LIA_POLICYNO,info:PROMPT_TYPE,info:SEND_DATE,info:SEND_FLAG,info:EMAIL,info:BEGINDATE,info:ENDDATE,info:CUSTOMER_ID,info:EMAIL_ID,info:CSC_POLICYNO,info:SUBCOMPANY_NO,info:SUBCOMPANY_NAME,info:PLCSTATE,info:PLCSTATEDESC,info:PAYMETHOD,info:NEXTPAYAMT,info:NEXTPAYDATE,info:RENEWALSTATE,info:RENEWALSTATEDESC,info:RECEIPT_EMAILID,info:PAYDATE,info:BANKNAME,info:PAY_CHANNEL,info:SMS_STATUS") TBLPROPERTIES("hbase.table.name"="P_RENEWAL_PROMPT");


create external table if not exists P_APPLYFORM(
key string,
FORM_ID string,
FORM_TYPE string,
FORM_NAME string,
FORM_GENDER string,
FORM_IDNUMBER string,
FORM_IPADDRESS string,
FORM_AREA string,
FORM_STATUS string,
FORM_RECORD string,
FORM_DATE string,
FORM_TOTAL string,
MEMBER_ID string,
FORM_PAYTIME string,
FORM_PAYTOTAL string,
FORM_PAYBALANCE string,
FORM_MSG string,
FORM_MSGTYPE string,
LRT_ID string,
LIA_POLICYNO string,
TRADE_BILLNO string,
BI_DATAEX_ID string,
BI_STATUS string,
INFORMS_ID string,
PAYWAY_ID string,
BANK_ACCOUNT string,
BANK_ACCOUNTNAME string,
BANK_ACCOUNTCIDNUM string,
BANK_ACCOUNTCIDTYPE string,
FORM_ACTION string,
AGENCY_ID string,
CLNT_DATA_SID string,
CLNT_DATA string,
ORDER_URL string,
COME_FORM_ID string,
SENDEMAILSMS string,
ERRORMSG string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:FORM_ID,info:FORM_TYPE,info:FORM_NAME,info:FORM_GENDER,info:FORM_IDNUMBER,info:FORM_IPADDRESS,info:FORM_AREA,info:FORM_STATUS,info:FORM_RECORD,info:FORM_DATE,info:FORM_TOTAL,info:MEMBER_ID,info:FORM_PAYTIME,info:FORM_PAYTOTAL,info:FORM_PAYBALANCE,info:FORM_MSG,info:FORM_MSGTYPE,info:LRT_ID,info:LIA_POLICYNO,info:TRADE_BILLNO,info:BI_DATAEX_ID,info:BI_STATUS,info:INFORMS_ID,info:PAYWAY_ID,info:BANK_ACCOUNT,info:BANK_ACCOUNTNAME,info:BANK_ACCOUNTCIDNUM,info:BANK_ACCOUNTCIDTYPE,info:FORM_ACTION,info:AGENCY_ID,info:CLNT_DATA_SID,info:CLNT_DATA,info:ORDER_URL,info:COME_FORM_ID,info:SENDEMAILSMS,info:ERRORMSG") TBLPROPERTIES("hbase.table.name"="P_APPLYFORM");


CREATE external TABLE if not exists CS_LOG(
key string,
LOG_ID string,
MEMBER_ID string,
MEMBER_NAME string,
MEMBER_TYPE string,
LOG_DATE string,
LOG_ITEM string,
LOG_IP string,
LOG_POLICYNO string,
SUCCESS_FLAG string,
MEMO string,
COMPUTEFLAG string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:LOG_ID,info:MEMBER_ID,info:MEMBER_NAME,info:MEMBER_TYPE,info:LOG_DATE,info:LOG_ITEM,info:LOG_IP,info:LOG_POLICYNO,info:SUCCESS_FLAG,info:MEMO,info:COMPUTEFLAG") TBLPROPERTIES("hbase.table.name"="CS_LOG");


CREATE external TABLE if not exists HEALTH_ITEM(
key string,
ITEM_ID	string,
ITEM_DETAIL string,
PRIORITY_NUM string,
ENABLED string,
ITEM_TYPE string,
CREATE_TIME	string,
MODIFY_TIME	string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:ITEM_ID,info:ITEM_DETAIL,info:PRIORITY_NUM,info:ENABLED,info:ITEM_TYPE,info:CREATE_TIME,info:MODIFY_TIME") TBLPROPERTIES("hbase.table.name"="HEALTH_ITEM");


CREATE external TABLE if not exists TB_HEALTHINFORM_DETAIL(
key string,
INFO_ID string,
INFO_CONTENT string,
INFO_TYPE string,
CHOICE_SHOW string,
INFO_CHECK string,
INFO_ANSWER string,
APPLY_TIME string,
PARENT_ID string,
CONTROL string,
NAMES string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:INFO_ID,info:INFO_CONTENT,info:INFO_TYPE,info:CHOICE_SHOW,info:INFO_CHECK,info:INFO_ANSWER,info:APPLY_TIME,info:PARENT_ID,info:CONTROL,info:NAMES") TBLPROPERTIES("hbase.table.name"="TB_HEALTHINFORM_DETAIL");


CREATE external TABLE if not exists WECHAT_CONCERN_DTAIL(
key string,
WEIXIN_ID string,
FANS_ID string,
CREATE_TIME string,
TRADE_BILLNO string,
NICKNAMES string,
FANS_NOTE string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:WEIXIN_ID,info:FANS_ID,info:CREATE_TIME,info:TRADE_BILLNO,info:NICKNAMES,info:FANS_NOTE") TBLPROPERTIES("hbase.table.name"="WECHAT_CONCERN_DTAIL");


CREATE external TABLE if not exists TAI_USERINFO (
 key string,
OPEN_ID string,
USER_NAME string,
USER_GENDER string,
USER_CIDNO string,
USER_CIDTYPE string,
USER_PHONE string,
USER_EMAIL string,
USER_ADDRESS string,
CREATED_TIME string,
UPDATED_TIME string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:OPEN_ID,info:USER_NAME,info:USER_GENDER,info:USER_CIDNO,info:USER_CIDTYPE,info:USER_PHONE,info:USER_EMAIL,info:USER_ADDRESS,info:CREATED_TIME,info:UPDATED_TIME") TBLPROPERTIES("hbase.table.name"="TAI_USERINFO");


CREATE external TABLE if not exists TAI_WECHATSHARE (
key string,
OPEN_ID string,
CREATED_TIME string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,info:OPEN_ID,info:CREATED_TIME") TBLPROPERTIES("hbase.table.name"="TAI_WECHATSHARE");


CREATE EXTERNAL TABLE FACT_USERINFO(
USER_ID string,
CUSTOMER_ID string,
MEMBER_ID string,
OPEN_ID string,
MDM_ID string,
NAME string,
CIDTYPE_ID string,
CID_NUMBER string,
BIRTHDAY string,
GENDER string,
MOBILE string,
EMAIL string,
COMMONADDRESS string,
COMMONADDRESSPOSTCODE string,
CIDNUMBER_PROVINCE string,
CIDNUMBER_CITY string,
MOBILE_PROVINCE string,
MOBILE_CITY string,
INCOME string,
LIVEFLAG string,
DAJIANKANGFLAG string,
LOADDATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe' 
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat" 
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat" 
LOCATION '/user/tkonline/taikangscore/data/userinfo-parquet';


CREATE EXTERNAL TABLE FACT_HEALTHINDEX(
USER_ID string,
OPEN_ID string,
LAST_TESTSORT string,
LAST_SUBMITDATE string,
LAST_TESTRESULT string,
SUM_TEST_SORT string,
CHECK_RESULT string,
CHECK_REASON string,
TRADERECORD_APPNTID string,
CHECKRESULT_APPNTID string,
HEALTHTESTACCURACY string,
LOADDATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe' 
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat" 
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat" 
LOCATION '/user/tkonline/taikangscore/data/healthindex-parquet';


CREATE EXTERNAL TABLE P_CUSTOMER_DETAIL (
CUSTOMER_ID string,
TKONLINE_POLICYHOLDER string,
TK_POLICYHOLDER string,
TKONLINE_INSURANT string,
TK_INSURANT string,
TKONLINE_BENEFICIARY string,
TK_BENEFICIARY string,
TK_RENEWAL string,
UNKNOWN string,
CREATEDATE string,
UPDATEDATE string,
FIRST_CST_CHANNELS string,
FIRST_POLICY string,
FIRST_PAYWAY string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe' 
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat" 
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat" 
LOCATION '/user/tkonline/taikangscore/data/customerdetail-parquet';
