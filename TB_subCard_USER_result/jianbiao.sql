CREATE  TABLE ${db_name}.tb_subCard_zfk_pri
     (
      CAL_MONTH INTEGER,
      SUBS_ID NUMBER(16,0),
      IF_ZHUKA VARCHAR2(1),
      PROB_0 NUMBER(6,4),
      PROB_1 NUMBER(6,4))
PARTITION BY LIST (CAL_MONTH)
(
  partition P_201810 values (201810)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),
  partition P_201811 values (201811)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    )  
);





CREATE  TABLE ${db_name}.DRT_ZFK_MODEL_RES 
     (
      CAL_MONTH INTEGER,
      SUBS_ID NUMBER(16,0),
      MSISDN VARCHAR2(40),
      PROB_1 NUMBER(6,4))
PARTITION BY LIST (CAL_MONTH)
(
  partition P_201810 values (201810)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),
  partition P_201811 values (201811)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    )  
);








CREATE  TABLE ${db_name}.TB_360_USERMID_M 
     (
      CAL_MONTH	INTEGER,
SUBS_ID	NUMBER(16,0),
MSISDN	VARCHAR2(40),
BA_CERT_MOBILE_NUM	INTEGER,
BA_CERT_FIXNET_NUM	INTEGER,
BA_CUST_MOBILE_NUM	INTEGER,
BA_CUST_FIXNET_NUM	INTEGER,
BA_FLOW_PACK_FLAG	VARCHAR2(10),
BA_SF_ROAM_DAYS	INTEGER,
BA_ROAM_DAYS	INTEGER,
VC_WD_CALL_TIMES	INTEGER,
VC_RD_CALL_TIME	INTEGER,
VC_WD_CALL_DUR	NUMBER(18,2),
VC_RD_CALL_DUR	NUMBER(18,2),
VC_WD_CALLING_TIMES	INTEGER,
VC_RD_CALLING_TIME	INTEGER,
VC_WD_CALLING_DUR	NUMBER(18,2),
VC_RD_CALLING_DUR	NUMBER(18,2),
VC_MOBILE_CALL_TIMES	INTEGER,
VC_MOBILE_CALL_DUR	NUMBER(18,2),
VC_MOBILE_CALLING_TIMES	INTEGER,
VC_MOBILE_CALLING_DUR	NUMBER(18,2),
VC_UNICOM_CALL_TIMES	NUMBER(18,2),
VC_UNICOM_CALL_DUR	NUMBER(18,2),
VC_UNICOM_CALLING_TIMES	NUMBER(18,2),
VC_UNICOM_CALLING_DUR	NUMBER(18,2),
VC_10086_CALL_TIMES	NUMBER(18,2),
VC_10086_CALLING_TIMES	NUMBER(18,2),
VC_10010_CALL_TIMES	NUMBER(18,2),
VC_10010_CALLING_TIMES	NUMBER(18,2),
VC_DAYS	NUMBER(18,2),
VC_MAX_INTERVAL_DAYS	NUMBER(18,2),
VC_MIN_INTERVAL_DAYS	NUMBER(18,2),
VC_ROAM_DAYS	NUMBER(18,2),
VC_BS_NUM	NUMBER(18,2),
VC_BN_BT5_DAYS	NUMBER(18,2),
VC_BN_BT10_DAYS	NUMBER(18,2),
VC_BN_BT15_DAYS	NUMBER(18,2),
VC_BN_BT20_DAYS	NUMBER(18,2),
VC_BN_BT30_DAYS	NUMBER(18,2),
FC_FLOW_SATURATION	NUMBER(18,2),
FC_PROD_IN_FLOW	NUMBER(18,2),
FC_PROD_EXT_FLOW	NUMBER(18,2),
FC_TRANS_FLOW	NUMBER(18,2),
FC_DAYS	NUMBER(18,2),
FC_MAX_INTERVAL_DAYS	NUMBER(18,2),
FC_MIN_INTERVAL_DAYS	NUMBER(18,2),
FC_ROAM_DAYS	NUMBER(18,2),
FC_BS_NUM	NUMBER(18,2),
FC_BN_BT10_DAYS	NUMBER(18,2),
FC_BN_BT20_DAYS	NUMBER(18,2),
FC_BN_BT30_DAYS	NUMBER(18,2),
FC_BN_BT50_DAYS	NUMBER(18,2),
FC_MAX_D_FLOW	NUMBER(18,2),
FC_DIFF_D_FLOW	NUMBER(18,2),
FC_NIGHT_FLOW	NUMBER(18,2),
FC_MAX_D_NIGHT_FLOW	NUMBER(18,2),
FC_MAX_H_FLOW	NUMBER(18,2),
FC_H_FLOW_BT50_H_NUM	NUMBER(18,2),
FC_H_FLOW_BT100_H_NUM	NUMBER(18,2),
FC_H_FLOW_BT50_D_NUM	NUMBER(18,2),
FC_H_FLOW_BT100_D_NUM	NUMBER(18,2),
JWQ_NUM	NUMBER(18,2),
JWQ_VALID_NUM	NUMBER(18,2),
JWQ_FIX_NUM	NUMBER(18,2),
JWQ_EXTNET_NUM	NUMBER(18,2),
JWQ_EXTNET_VALID_NUM	NUMBER(18,2),
JWQ_DX_LONG_NUM	NUMBER(18,2),
JWQ_CLOSE_NUM	NUMBER(18,2),
JWQ_EXTNET_CLOSE_NUM	NUMBER(18,2),
DPI_APP_DAYS	NUMBER(18,2),
DPI_APP_NUM	NUMBER(18,2),
DPI_APP_WEIXIN_DAYS	NUMBER(18,2),
DPI_APP_WEIXIN_PV	NUMBER(18,2),
DPI_APP_WEIXIN_FLOW	NUMBER(18,2))
PARTITION BY LIST (CAL_MONTH)
(
  partition P_201810 values (201810)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),
  partition P_201811 values (201811)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    )  
)
;
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.CAL_MONTH	is	'统计月份';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.SUBS_ID	is	'用户标识';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.MSISDN	is	'手机号码';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.BA_CERT_MOBILE_NUM	is	'证件下移动号码数量';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.BA_CERT_FIXNET_NUM	is	'证件下宽带号码数量';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.BA_CUST_MOBILE_NUM	is	'客户下移动号码数量';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.BA_CUST_FIXNET_NUM	is	'客户下宽带号码数量';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.BA_FLOW_PACK_FLAG	is	'是否订购流量包';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.BA_SF_ROAM_DAYS	is	'春节漫游天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.BA_ROAM_DAYS	is	'漫游天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_WD_CALL_TIMES	is	'工作日通话次数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_RD_CALL_TIME	is	'休息日通话次数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_WD_CALL_DUR	is	'工作日通话时长';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_RD_CALL_DUR	is	'休息日通话时长';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_WD_CALLING_TIMES	is	'工作日主叫次数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_RD_CALLING_TIME	is	'休息日主叫次数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_WD_CALLING_DUR	is	'工作日主叫时长';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_RD_CALLING_DUR	is	'休息日主叫时长';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_MOBILE_CALL_TIMES	is	'移动号码通话次数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_MOBILE_CALL_DUR	is	'移动号码通话时长';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_MOBILE_CALLING_TIMES	is	'移动号码主叫次数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_MOBILE_CALLING_DUR	is	'移动号码主叫时长';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_UNICOM_CALL_TIMES	is	'联通号码通话次数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_UNICOM_CALL_DUR	is	'联通号码通话时长';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_UNICOM_CALLING_TIMES	is	'联通号码主叫次数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_UNICOM_CALLING_DUR	is	'联通号码主叫时长';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_10086_CALL_TIMES	is	'10086通话次数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_10086_CALLING_TIMES	is	'10086主叫次数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_10010_CALL_TIMES	is	'10010通话次数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_10010_CALLING_TIMES	is	'10010主叫次数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_DAYS	is	'语音通信天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_MAX_INTERVAL_DAYS	is	'语音通信最大间隔天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_MIN_INTERVAL_DAYS	is	'语音通信最小间隔天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_ROAM_DAYS	is	'语音漫游天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_BS_NUM	is	'语音基站个数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_BN_BT5_DAYS	is	'语音基站超过5个天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_BN_BT10_DAYS	is	'语音基站超过10个天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_BN_BT15_DAYS	is	'语音基站超过15个天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_BN_BT20_DAYS	is	'语音基站超过20个天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.VC_BN_BT30_DAYS	is	'语音基站超过30个天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_FLOW_SATURATION	is	'流量饱和度';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_PROD_IN_FLOW	is	'主套餐内使用流量';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_PROD_EXT_FLOW	is	'主套餐外使用流量';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_TRANS_FLOW	is	'结转流量';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_DAYS	is	'流量通信天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_MAX_INTERVAL_DAYS	is	'流量通信最大间隔天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_MIN_INTERVAL_DAYS	is	'流量通信最小间隔天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_ROAM_DAYS	is	'流量漫游天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_BS_NUM	is	'流量基站个数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_BN_BT10_DAYS	is	'流量基站超过10个天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_BN_BT20_DAYS	is	'流量基站超过20个天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_BN_BT30_DAYS	is	'流量基站超过30个天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_BN_BT50_DAYS	is	'流量基站超过50个天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_MAX_D_FLOW	is	'最高日流量';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_DIFF_D_FLOW	is	'最高最低日流量差';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_NIGHT_FLOW	is	'晚间流量';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_MAX_D_NIGHT_FLOW	is	'日最高晚间流量';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_MAX_H_FLOW	is	'时段最高流量';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_H_FLOW_BT50_H_NUM	is	'流量大于50M时段数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_H_FLOW_BT100_H_NUM	is	'流量大于100M时段数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_H_FLOW_BT50_D_NUM	is	'时段流量大于50M天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.FC_H_FLOW_BT100_D_NUM	is	'时段流量大于100M天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.JWQ_NUM	is	'交往圈个数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.JWQ_VALID_NUM	is	'有效交往圈个数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.JWQ_FIX_NUM	is	'固网交往圈个数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.JWQ_EXTNET_NUM	is	'异网交往圈个数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.JWQ_EXTNET_VALID_NUM	is	'异网有效交往圈个数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.JWQ_DX_LONG_NUM	is	'定向长途个数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.JWQ_CLOSE_NUM	is	'紧密联系人个数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.JWQ_EXTNET_CLOSE_NUM	is	'异网紧密联系人个数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.DPI_APP_DAYS	is	'APP活跃天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.DPI_APP_NUM	is	'APP个数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.DPI_APP_WEIXIN_DAYS	is	'微信活跃天数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.DPI_APP_WEIXIN_PV	is	'微信访问次数';
COMMENT ON COLUMN ${db_name}.TB_360_USERMID_M.DPI_APP_WEIXIN_FLOW	is	'微信使用流量';






CREATE TABLE ${db_name}.tb_subCard_PBK$TX_DATE(
SUBS_ID	NUMBER(16,0)
,IF_ZHUKA	VARCHAR(1)
,PROB_0	NUMBER(16,4)
,PROB_1	NUMBER(16,4));

CREATE TABLE ${db_name}.tb_subCard_LX4G$TX_DATE(
SUBS_ID	NUMBER(16,0)
,IF_ZHUKA	VARCHAR(1)
,PROB_0	NUMBER(16,4)
,PROB_1	NUMBER(16,4));

CREATE TABLE ${db_name}.tb_subCard_bxl$TX_DATE(
SUBS_ID	NUMBER(16,0)
,IF_ZHUKA	VARCHAR(1)
,PROB_0	NUMBER(16,4)
,PROB_1	NUMBER(16,4));

CREATE TABLE ${db_name}.tb_subCard_lxj$TX_DATE(
SUBS_ID	NUMBER(16,0)
,IF_ZHUKA	VARCHAR(1)
,PROB_0	NUMBER(16,4)
,PROB_1	NUMBER(16,4));




   
CREATE TABLE ${db_name}.tb_subCard_pbk_PRI(
	  CAL_MONTH INTEGER,
      SUBS_ID NUMBER(16,0),
      IF_ZHUKA VARCHAR2(1),
      PROB_0 NUMBER(6,4),
      PROB_1 NUMBER(6,4));
  
CREATE TABLE ${db_name}.tb_subCard_pbk(
	  CAL_MONTH INTEGER,
      SUBS_ID NUMBER(16,0),
      MSISDN VARCHAR2(40),
      PROB_1 NUMBER(6,4));
	  

  
CREATE TABLE ${db_name}.tb_subCard_lx4g_PRI(
	  CAL_MONTH INTEGER,
      SUBS_ID NUMBER(16,0),
      IF_ZHUKA VARCHAR2(1),
      PROB_0 NUMBER(6,4),
      PROB_1 NUMBER(6,4));
  
CREATE TABLE ${db_name}.tb_subCard_lx4g(
	  CAL_MONTH INTEGER,
      SUBS_ID NUMBER(16,0),
      MSISDN VARCHAR2(40),
      PROB_1 NUMBER(6,4));
	  

	  
CREATE TABLE ${db_name}.tb_subCard_bxl_PRI (
	  CAL_MONTH INTEGER,
      SUBS_ID NUMBER(16,0),
      IF_ZHUKA VARCHAR2(1),
      PROB_0 NUMBER(6,4),
      PROB_1 NUMBER(6,4));
	  
CREATE TABLE ${db_name}.tb_subCard_bxl(
	  CAL_MONTH INTEGER,
      SUBS_ID NUMBER(16,0),
      MSISDN VARCHAR2(40),
      PROB_1 NUMBER(6,4));

	  
	  
CREATE TABLE ${db_name}.tb_subCard_lxj_PRI(
	  CAL_MONTH INTEGER,
      SUBS_ID NUMBER(16,0),
      IF_ZHUKA VARCHAR2(1),
      PROB_0 NUMBER(6,4),
      PROB_1 NUMBER(6,4));
	  
CREATE TABLE ${db_name}.tb_subCard_lxj(
	  CAL_MONTH INTEGER,
      SUBS_ID NUMBER(16,0),
      MSISDN VARCHAR2(40),
      PROB_1 NUMBER(6,4));









---导入表
CREATE  TABLE ${db_name}.TB_PHONE_NO_HOME 
     (
      ID_NO VARCHAR2(20),
      front_seven_no VARCHAR2(20),
      provice VARCHAR2(30),
      city VARCHAR2(30),
      operators VARCHAR2(30),
      area_code VARCHAR2(30),
      postcode VARCHAR2(30)) compress ;
COMMENT ON COLUMN ${db_name}.TB_PHONE_NO_HOME.ID_NO is '序号';
COMMENT ON COLUMN ${db_name}.TB_PHONE_NO_HOME.front_seven_no is  '手机号前7位';
COMMENT ON COLUMN ${db_name}.TB_PHONE_NO_HOME.provice is '所在省';
COMMENT ON COLUMN ${db_name}.TB_PHONE_NO_HOME.city is '所在市';
COMMENT ON COLUMN ${db_name}.TB_PHONE_NO_HOME.operators is '所属运营商';
COMMENT ON COLUMN ${db_name}.TB_PHONE_NO_HOME.area_code is '地区编号';
COMMENT ON COLUMN ${db_name}.TB_PHONE_NO_HOME.postcode is '邮编';










CREATE  TABLE ${db_name}.zude_tcdangwei
     (
      PRICE_PLAN_CD NUMBER(12,0),
      NAME VARCHAR2(200),
      pge_stall INTEGER,
      pge_flow NUMBER(15,1),
      pge_vc INTEGER) compress;




	  

----DPI表建表语句
CREATE TABLE  ${db_name}.TB_EVT_C_USER_DPI_M			
(				
CAL_MONTH	INTEGER,		
USER_ID	NUMBER(20,0),
PHONE_ID	VARCHAR2(20),		
VISIT_TYPE	VARCHAR2(5),		
TYPE_NAME1	VARCHAR2(50),		
TYPE_NAME2	VARCHAR2(50),		
TYPE_NAME3	VARCHAR2(50),		
TYPE_NAME4	VARCHAR2(50),		
TYPE_NAME5	VARCHAR2(50),		
TYPE_NAME6	VARCHAR2(50),		
APP_TYPE_NAME1	VARCHAR2(50),		
APP_TYPE_NAME2	VARCHAR2(500),		
APP_TYPE_NAME3	VARCHAR2(50),		
KEYWORD	VARCHAR2(4000),		
PV	NUMBER(18,4),		
USERFLOW	NUMBER(18,4),		
MTD_PV	NUMBER(18,4),		
MTD_USERFLOW	NUMBER(18,4),		
G4_FLAG	VARCHAR2(5),		
WW_PHOTE_FLAG	INTEGER)
PARTITION BY LIST (CAL_MONTH)
(
  partition P_201810 values (201810)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),
  partition P_201811 values (201811)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    )  
);
				
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.CAL_MONTH	is	'统计月份';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.USER_ID	is	'用户标识';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.PHONE_ID	is	'手机号码';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.VISIT_TYPE	is	'访问类型';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.TYPE_NAME1	is	'访问类型1级';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.TYPE_NAME2	is	'访问类型2级';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.TYPE_NAME3	is	'访问类型3级';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.TYPE_NAME4	is	'访问类型4级';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.TYPE_NAME5	is	'访问类型5级';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.TYPE_NAME6	is	'访问类型6级';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.APP_TYPE_NAME1	is	'APP访问类型1级';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.APP_TYPE_NAME2	is	'APP访问类型2级';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.APP_TYPE_NAME3	is	'APP访问类型3级';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.KEYWORD	is	'搜索关键字';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.PV	is	'PV';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.USERFLOW	is	'使用流量';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.MTD_PV	is	'MTV_PV';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.MTD_USERFLOW	is	'MTD使用流量';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.G4_FLAG	is	'4G标识';		
comment on column ${db_name}.TB_EVT_C_USER_DPI_M.WW_PHOTE_FLAG	is	'维挽拍照标识';		





CREATE TABLE ${db_name}.xyx_mubiao_zfk_main_m 
     (
CAL_MONTH                 INTEGER,
SUBS_ID               	NUMBER(16,0),
ZK_MONTH              	VARCHAR2(8) ,
FK_MONTH              	VARCHAR2(8) ,
if_fuka_10M           	INTEGER,
P_AGE                   NUMBER(16,0),
P_GENDER_ID              CHAR(2) ,
LOCAL_FLAG            	CHAR(2) ,
chunjie_roam_days     	NUMBER(16,0),
STAY_DUR              	NUMBER(16,0),
MAIN_PROD_ID2            INTEGER,
USER_STS_ID            	INTEGER,
ronghe_flag           	INTEGER,
PROD_COMP_RELA_ROLE_CD   NUMBER(16,0),
cert_type_name        	VARCHAR2(64) ,
cnet_user_nums1       	NUMBER(16,0),
kd_user_nums1         	NUMBER(16,0),
cnet_user_nums2       	NUMBER(16,0),
kd_user_nums2         	NUMBER(16,0),
if_jt_user            	INTEGER,
month_contract_over   	NUMBER(16,0),
tc_name               	VARCHAR2(200) ,
tc_type               	VARCHAR2(6) ,
pge_stall             	NUMBER(16,0),
if_change_pge         	CHAR(2) ,
if_change_trmn        	CHAR(2) ,
market_price          	NUMBER(16,2),
standby_type          	VARCHAR2(200) ,
BILL_FLAG             	INTEGER,
chuz_nums             	NUMBER(16,0),
USER_CREDIT_VALUE     	NUMBER(16,0),
MEMBERSHIP_LEVEL      	NUMBER(16,0),
YJDK_FLAG_nums        	NUMBER(16,0),
YKDJ_FLAG_nums        	NUMBER(16,0),
CONSUME_AMT           	NUMBER(16,2),
consume_amt_avg       	NUMBER(16,2),
income_dangwei_pp     	NUMBER(16,4),
income_dangwei_pp_avg 	NUMBER(16,4),
AGG_OWE_FEE          	NUMBER(16,2),
owe_nums              	NUMBER(16,0),
CHARGE_TIMES          	NUMBER(16,0),
pay_nums             NUMBER(16,0),   
roam_days            NUMBER(16,0),
roam_days_avg        NUMBER(16,2),   
CALL_DURATION             NUMBER(16,2),
dur_avg              NUMBER(16,2),
CALLING_DURATION     NUMBER(16,2),
calling_dur_avg      NUMBER(16,2),
calling_pp           NUMBER(16,4),
calling_pp_avg       NUMBER(16,4),
roam_pp              NUMBER(16,4),
roam_pp_avg          NUMBER(16,4),
long_pp              NUMBER(16,4),
long_pp_avg          NUMBER(16,4),
flows                NUMBER(16,2),
flow_avg             NUMBER(16,2),
flow_satur_pp        NUMBER(16,4),
flow_satur_pp_avg    NUMBER(16,4),
flow_roam_pp         NUMBER(16,4),
flow_roam_pp_avg     NUMBER(16,4),
FOR_TIMES              NUMBER(16,0),
FOR_TIMES_avg          NUMBER(16,2),
FOR_DURATION              NUMBER(16,2),
FOR_DURATION_avg          NUMBER(16,2),
nums_bs_flow         NUMBER(16,0),
nums_bs_flow_avg     NUMBER(16,2),
flow_bs_20_pp        NUMBER(16,4),
flow_bs_20_pp_avg    NUMBER(16,4),
nums_bs_vc           NUMBER(16,0),
nums_bs_vc_avg       NUMBER(16,2),
vc_bs_20_pp         NUMBER(16,4),
vc_bs_20_pp_avg       NUMBER(16,4),
flow_days            NUMBER(16,0), 
flow_days_avg        NUMBER(16,2), 
flows_diff_pp        NUMBER(16,4), 
flows_diff_pp_avg    NUMBER(16,4), 
vc_days              NUMBER(16,0),
vc_days_avg          NUMBER(16,2),
nums_APP             NUMBER(16,0),       
nums_app_avg         NUMBER(16,2),
if_yzf_user          INTEGER,
if_yzf_user_avg      INTEGER,
flow_huango_user     NUMBER(16,2),
flow_huango_user_avg NUMBER(16,2),
flow_edu_user        NUMBER(16,2),
flow_edu_user_avg    NUMBER(16,2),
flow_house_user      NUMBER(16,2),
flow_house_user_avg  NUMBER(16,2),
flow_mom_user        NUMBER(16,2),
flow_mom_user_avg    NUMBER(16,2),
if_search_user       INTEGER,
if_search_user_avg   INTEGER,
nums_jwq             NUMBER(16,0),
nums_jwq_avg         NUMBER(16,2),
jwq_fixed_pp         NUMBER(16,4),
jwq_fixed_pp_avg     NUMBER(16,4),
jwq_valid_pp         NUMBER(16,4),
jwq_valid_pp_avg     NUMBER(16,4),
nums_dx_long         NUMBER(16,0),
nums_close_contact   NUMBER(16,0),
call_times           NUMBER(16,0),
call_times_avg       NUMBER(16,2),
nums_tousu           NUMBER(16,0),       
nums_tousu_avg       NUMBER(16,2),
if_undealed_list     INTEGER
)PARTITION BY LIST (CAL_MONTH)
(
  partition P_201810 values (201810)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),
  partition P_201811 values (201811)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    )  
); 


	  
	  

-----360日表的建表语句
CREATE TABLE	${db_name}.s_td_user_360_d	
(			
CAL_day	INTEGER,	
SUBS_ID	NUMBER,	
MSISDN	VARCHAR2(40),	
ACCT_ID	NUMBER,	
CUST_ID	NUMBER,	
SUBS_STATE	NUMBER(20),	
SUBS_STATE_DATE	VARCHAR2(19),	
GENDER	VARCHAR2(2),	
AGE	NUMBER,	
SUB_BRAND_ID	VARCHAR2(10),	
CREATE_DATE	VARCHAR2(19),	
DESTROY_DATE	VARCHAR2(19),	
FIRST_ACTIVE_DATE	VARCHAR2(19),	
OPEN_DATE	VARCHAR2(19),	
REMOVE_MODE	NUMBER(20),	
REMOVE_DATE	VARCHAR2(19),	
PRE_DESTROY_DATE	VARCHAR2(19),	
LAST_STOP_DATE	VARCHAR2(19),	
IN_FLAG	INTEGER,	
COUNTRY_FLAG	NUMBER(10),	
MONTH_NEW_FLAG	NUMBER(10),	
MONTH_OFF_FLAG	NUMBER(10),	
FREE_FLAG	VARCHAR2(1),	
PREPAY_FLAG	NUMBER(10),	
SCHOOL_FLAG	NUMBER(10),	
IF_REALNAME	NUMBER(10),	
IF_UPLOAD	NUMBER(10),	
REALNAME_DATE	VARCHAR2(19),	
STOP_STS_TYPE	NUMBER(20),	
USER_DEVELOP_STAFF_ID	VARCHAR2(50),	
AGE_LEVEL_CODE	INTEGER,	
LOCAL_FLAG	VARCHAR2(2),	
STAY_DUR	NUMBER,	
USER_TYPE_CODE	NUMBER,	
CUST_LEVEL	NUMBER,	
MAIN_PROD_ID	NUMBER,	
MAIN_PP_ID	NUMBER,	
OFFER_ID	NUMBER,	
C_PROD_ID2	NUMBER(20),	
NEXT_PP_4G_ID	NUMBER,	
SINGLE_PROD_FLAG	NUMBER(10),	
COM_PROD_INST_ID	NUMBER,	
COM_PROD_ID	NUMBER,	
COM_PROD_OFFER_ID	NUMBER,	
G3_FUNC_FLAG	VARCHAR2(2),	
COMM_NUM	NUMBER,	
COMM_DUR	NUMBER,	
LOCAL_COMM_NUM	NUMBER,	
LOCAL_COMM_DUR	NUMBER,	
ROAM_COMM_NUM	NUMBER,	
ROAM_COMM_DUR	NUMBER,	
LONG_COMM_NUM	NUMBER,	
LONG_COMM_DUR	NUMBER,	
CALLING_COMM_NUM	NUMBER,	
CALLING_COMM_DUR	NUMBER,	
CALLED_COMM_NUM	NUMBER,	
CALLED_COMM_DUR	NUMBER,	
FWD_NUM	NUMBER,	
CALL_NOTOLL_TIMES	NUMBER,	
FWD_DUR	NUMBER,	
CALLING_CMC_DURATION	NUMBER,	
CALLING_CUC_DURATION	NUMBER,	
CALLING_CUC_TIMES	NUMBER,	
CALLING_CMC_TIMES	NUMBER,	
CALLING_CTC_TIMES	NUMBER,	
KEFU_CMC_TIMES	NUMBER,	
KEFU_CUC_TIMES	NUMBER,	
UNICOM_H_TIMES	NUMBER,	
MOBILE_H_TIMES	NUMBER,	
PSTN_CALLING_DUR	NUMBER,	
PSTN_FWD_NUM	NUMBER,	
COMM_10000_NUM	NUMBER,	
BASE_DURATION	NUMBER,	
TOLL_DURATION	NUMBER,	
LOACL_DURATION	NUMBER,	
LOACL_NOTOLL_DURATION	NUMBER,	
LOACL_CALLING_DURATION	NUMBER,	
GN_BILL_DURA	NUMBER,	
GJ_BILL_DURA	NUMBER,	
SMS_TIMES	NUMBER,	
MMS_TIMES	NUMBER,	
IX_DURATION	NUMBER,	
IX_BASE_DURATION	NUMBER,	
IX_ROAM_DURATION	NUMBER,	
IX_KBYTES	NUMBER,	
IX_MO_KBYTES	NUMBER,	
IX_MO_BASE_KBYTES	NUMBER,	
IX_MO_ROAM_KBYTES	NUMBER,	
IX_MT_KBYTES	NUMBER,	
IX_MT_BASE_KBYTES	NUMBER,	
IX_MT_ROAM_KBYTES	NUMBER,	
IX_LOC_KBYTES	NUMBER,	
IX_ROAM_KBYTES	NUMBER,	
WLAN_DURA	NUMBER,	
G2_DURA	NUMBER,	
G3_DURA	NUMBER,	
G4_DURA	NUMBER,	
WLAN_FLUX	NUMBER,	
G2_FLUX	NUMBER,	
G3_FLUX	NUMBER,	
GAT_ROAM_BILL_DURA	NUMBER,	
GAT_ROAM_IX_FLUX	NUMBER,	
WIFI_3G_MON_CNT	NUMBER,	
WIFI_ACTIVE_MON_CNT	NUMBER,	
GJ_ROAM_CALLING_BILL_DUR	NUMBER,	
GJ_ROAM_CALLED_BILL_DUR	NUMBER,	
GJ_ROAM_IX_FLUX	NUMBER,	
GJ_ROAM_SMS_TIMS	NUMBER,	
CONSUME_AMT	NUMBER,	
FEE	NUMBER,	
CURR_OWE_FEE	NUMBER,	
FIRST_OWE_MONTH	VARCHAR2(19),	
USER_BALANCE	NUMBER,	
VPN_FLAG	VARCHAR2(2),	
LEASE_FLAG	VARCHAR2(2),	
LOCAL_INDUSTRY_TYPE_ID1	VARCHAR2(20),	
LOCAL_INDUSTRY_TYPE_ID	VARCHAR2(20),	
LEASE_PLAN_ID	INTEGER,	
LEASE_PP_EFF_DATE	VARCHAR2(19),	
LEASE_PP_EXP_DATE	VARCHAR2(19),	
LEASE_PP_ORDER_DATE	VARCHAR2(19),	
PLAN_ITEM_NAME	VARCHAR2(100),	
DEVICE_BRAND_ID	VARCHAR2(100),	
DEVICE_MODEL_ID	VARCHAR2(100),	
PLAN_ITEM_NAME_BILL	VARCHAR2(200),	
INTEL_PHONE	VARCHAR2(10),	
INTEL_PHONE_SYSTEM	VARCHAR2(50),	
INTEL_PHONE_VERSION	VARCHAR2(50),	
DOUBLE_NET_PHONE	VARCHAR2(10),	
PHONE_ORDER_SEQ	INTEGER,	
FIRST_LEASE_PLAN_ID	NUMBER,	
FIRST_MAIN_PP_ID	NUMBER,	
BILL_FLAG	NUMBER(20),	
ACTIVE_FLAG	NUMBER(20),	
USER_3G_ACTIVE_FLAG	NUMBER,	
NO_URGE_FLAG	NUMBER,	
ENT_LEASE_FLAG	VARCHAR2(2),	
FLUX_PKG_TYPE	NUMBER,	
PHOTO_FLAG	INTEGER,	
CHARGE_AMT	NUMBER,	
CHARGE_TIMES	INTEGER,	
USER_DEVELOP_DEPART_ID	VARCHAR2(50),	
USER_DEVELOP_DEPART_ID2	VARCHAR2(50),	
USER_DEVELOP_DEPART_ID3	VARCHAR2(50),	
D_ECHANNEL_USER_TYPE	INTEGER,	
ENT_INDUSTRY_TYPE_ID	VARCHAR2(64),	
USER_CREDIT_VALUE	NUMBER,	
USER_CLASS_ID	NUMBER,	
ORG_CHANNEL_ID1	VARCHAR2(20),	
ORG_CHANNEL_ID2	VARCHAR2(20),	
ORG_CHANNEL_ID	VARCHAR2(20),	
G4_FLUX	NUMBER,	
FLAG_4G	INTEGER,	
FLAG_4G_MAIN_PP	INTEGER,	
FLAG_4G_FLUX_PKG	INTEGER,	
FLAG_4G_NONE_PP	INTEGER,	
FLAG_4G_IMEI	INTEGER,	
FLAG_4G_LEASE_PP	INTEGER,	
FLAG_4G_SIM	INTEGER,	
FLAG_4G_FUNC	INTEGER,	
FLUX_PKG_G4_ORDER_DATE	VARCHAR2(19),	
AGG_SCORE_VALUE	NUMBER,	
VALID_SCORE_VALUE	NUMBER,	
INVALID_SCORE_VALUE	NUMBER,	
MONTH_NEW_SCORE_VALUE	NUMBER,	
MON_CONSUME_SCORE_VALUE	NUMBER,	
MON_ADJUST_SCORE_VALUE	NUMBER,	
KEY_PP_ID	NUMBER,	
FREE_FLUX_REAL	NUMBER,	
FREE_FLUX	NUMBER,	
MAIN_FLUX_REAL	NUMBER,	
MAIN_FLUX	NUMBER,	
NOFREE_FLUX_REAL	NUMBER,	
NOFREE_FLUX	NUMBER,	
ADD_FLUX_REAL	NUMBER,	
ADD_FLUX	NUMBER,	
OUT_FLUX_REAL	NUMBER,	
ACTIVE_DAY_NUM	INTEGER,	
VIP_CARD_TYPE	VARCHAR2(20),	
BUILDING_BURA_ID	VARCHAR2(128),	
BUILDING_ID	VARCHAR2(128),	
BUILDING_KIND_ID	VARCHAR2(20),	
BUILDING_NAME	VARCHAR2(256),	
BUILDING_TYPE_ID	VARCHAR2(20),	
BRD_LINE_RATE	VARCHAR2(250),	
COOP_CHANNEL	VARCHAR2(50),	
FIX_BRD_DURA	NUMBER,	
FIX_BRD_FLUX	NUMBER,	
C_PSTN_FLAG	NUMBER(10),	
IN_MODE_ID	INTEGER,	
YPAY_MAIN_PP_ID	NUMBER,	
BI_U_GROUP_ID	INTEGER,	
EDUCATION_SCHOOL	VARCHAR2(2),	
nbr_cnt	INTEGER,	
CUST_NAMELEN	INTEGER,	
FLAG_G4_DEVICE_SIM	VARCHAR2(1),	
real_balance	NUMBER)
PARTITION BY LIST (CAL_day)
(
  partition P_20181101 values (20181101)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),
  partition P_20181102 values (20181102)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ), 
  partition P_20181103 values (20181103)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181104 values (20181104)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181105 values (20181105)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181106 values (20181106)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181107 values (20181107)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181108 values (20181108)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181109 values (20181109)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181110 values (20181110)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181111 values (20181111)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  	
  partition P_20181112 values (20181112)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),
  partition P_20181113 values (20181113)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ), 
  partition P_20181114 values (20181114)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181115 values (20181115)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181116 values (20181116)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181117 values (20181117)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181118 values (20181118)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181119 values (20181119)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181120 values (20181120)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181121 values (20181121)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181122 values (20181122)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  	
  partition P_20181123 values (20181123)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),
  partition P_20181124 values (20181124)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ), 
  partition P_20181125 values (20181125)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181126 values (20181126)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181127 values (20181127)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181128 values (20181128)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181129 values (20181129)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    ),  
  partition P_20181130 values (20181130)
    tablespace TBS_DATA24
    pctfree 0
    initrans 1
    maxtrans 255
    storage
    (
      initial 80K
      next 1M
      minextents 1
      maxextents unlimited
    )
);	
			
						
							
			
comment on column ${db_name}.s_td_user_360_d.CAL_day	is	'统计日期';
comment on column ${db_name}.s_td_user_360_d.SUBS_ID	is	'用户标识';
comment on column ${db_name}.s_td_user_360_d.MSISDN	is	'手机号码';
comment on column ${db_name}.s_td_user_360_d.ACCT_ID	is	'账户编号';
comment on column ${db_name}.s_td_user_360_d.CUST_ID	is	'客户标识';
comment on column ${db_name}.s_td_user_360_d.SUBS_STATE	is	'用户状态';
comment on column ${db_name}.s_td_user_360_d.SUBS_STATE_DATE	is	'用户状态变更日期';
comment on column ${db_name}.s_td_user_360_d.GENDER	is	'性别';
comment on column ${db_name}.s_td_user_360_d.AGE	is	'年龄';
comment on column ${db_name}.s_td_user_360_d.SUB_BRAND_ID	is	'子品牌ID';
comment on column ${db_name}.s_td_user_360_d.CREATE_DATE	is	'建档时间';
comment on column ${db_name}.s_td_user_360_d.DESTROY_DATE	is	'离网时间';
comment on column ${db_name}.s_td_user_360_d.FIRST_ACTIVE_DATE	is	'首次激活时间';
comment on column ${db_name}.s_td_user_360_d.OPEN_DATE	is	'入网时间';
comment on column ${db_name}.s_td_user_360_d.REMOVE_MODE	is	'离网类型';
comment on column ${db_name}.s_td_user_360_d.REMOVE_DATE	is	'拆机时间';
comment on column ${db_name}.s_td_user_360_d.PRE_DESTROY_DATE	is	'预拆机时间';
comment on column ${db_name}.s_td_user_360_d.LAST_STOP_DATE	is	'最近停机时间';
comment on column ${db_name}.s_td_user_360_d.IN_FLAG	is	'在网标志';
comment on column ${db_name}.s_td_user_360_d.COUNTRY_FLAG	is	'标志_城市农村';
comment on column ${db_name}.s_td_user_360_d.MONTH_NEW_FLAG	is	'标志_当月新增用户';
comment on column ${db_name}.s_td_user_360_d.MONTH_OFF_FLAG	is	'标志_当月流失用户';
comment on column ${db_name}.s_td_user_360_d.FREE_FLAG	is	'公免测试类型';
comment on column ${db_name}.s_td_user_360_d.PREPAY_FLAG	is	'标志_预付后付';
comment on column ${db_name}.s_td_user_360_d.SCHOOL_FLAG	is	'是否校园';
comment on column ${db_name}.s_td_user_360_d.IF_REALNAME	is	'实名制标志1';
comment on column ${db_name}.s_td_user_360_d.IF_UPLOAD	is	'实名制标志2';
comment on column ${db_name}.s_td_user_360_d.REALNAME_DATE	is	'实名制时间';
comment on column ${db_name}.s_td_user_360_d.STOP_STS_TYPE	is	'停机类型';
comment on column ${db_name}.s_td_user_360_d.USER_DEVELOP_STAFF_ID	is	'用户发展员工';
comment on column ${db_name}.s_td_user_360_d.AGE_LEVEL_CODE	is	'年龄分层';
comment on column ${db_name}.s_td_user_360_d.LOCAL_FLAG	is	'是否本地人';
comment on column ${db_name}.s_td_user_360_d.STAY_DUR	is	'入网时长';
comment on column ${db_name}.s_td_user_360_d.USER_TYPE_CODE	is	'客户类型';
comment on column ${db_name}.s_td_user_360_d.CUST_LEVEL	is	'客户等级';
comment on column ${db_name}.s_td_user_360_d.MAIN_PROD_ID	is	'主产品细类标识';
comment on column ${db_name}.s_td_user_360_d.MAIN_PP_ID	is	'资费标识';
comment on column ${db_name}.s_td_user_360_d.OFFER_ID	is	'套餐标识(销售品)';
comment on column ${db_name}.s_td_user_360_d.C_PROD_ID2	is	'移动产品二级分类码';
comment on column ${db_name}.s_td_user_360_d.NEXT_PP_4G_ID	is	'下月主资费';
comment on column ${db_name}.s_td_user_360_d.SINGLE_PROD_FLAG	is	'标志_单产品融合产品';
comment on column ${db_name}.s_td_user_360_d.COM_PROD_INST_ID	is	'组合产品实例标识';
comment on column ${db_name}.s_td_user_360_d.COM_PROD_ID	is	'组合产品标识';
comment on column ${db_name}.s_td_user_360_d.COM_PROD_OFFER_ID	is	'组合产品销售品细类标识';
comment on column ${db_name}.s_td_user_360_d.G3_FUNC_FLAG	is	'是否开通3G功能';
comment on column ${db_name}.s_td_user_360_d.COMM_NUM	is	'通话次数';
comment on column ${db_name}.s_td_user_360_d.COMM_DUR	is	'通话时长';
comment on column ${db_name}.s_td_user_360_d.LOCAL_COMM_NUM	is	'本地通话次数';
comment on column ${db_name}.s_td_user_360_d.LOCAL_COMM_DUR	is	'本地通话时长';
comment on column ${db_name}.s_td_user_360_d.ROAM_COMM_NUM	is	'漫游通话次数';
comment on column ${db_name}.s_td_user_360_d.ROAM_COMM_DUR	is	'漫游通话时长';
comment on column ${db_name}.s_td_user_360_d.LONG_COMM_NUM	is	'长途通话次数';
comment on column ${db_name}.s_td_user_360_d.LONG_COMM_DUR	is	'长途通话时长';
comment on column ${db_name}.s_td_user_360_d.CALLING_COMM_NUM	is	'主叫通话次数';
comment on column ${db_name}.s_td_user_360_d.CALLING_COMM_DUR	is	'主叫通话时长';
comment on column ${db_name}.s_td_user_360_d.CALLED_COMM_NUM	is	'被叫通话次数';
comment on column ${db_name}.s_td_user_360_d.CALLED_COMM_DUR	is	'被叫通话时长';
comment on column ${db_name}.s_td_user_360_d.FWD_NUM	is	'呼转次数';
comment on column ${db_name}.s_td_user_360_d.CALL_NOTOLL_TIMES	is	'本月非长途通话次数';
comment on column ${db_name}.s_td_user_360_d.FWD_DUR	is	'呼转时长';
comment on column ${db_name}.s_td_user_360_d.CALLING_CMC_DURATION	is	'本月主叫移动通话时长(秒)';
comment on column ${db_name}.s_td_user_360_d.CALLING_CUC_DURATION	is	'本月主叫联通通话时长(秒)';
comment on column ${db_name}.s_td_user_360_d.CALLING_CUC_TIMES	is	'本月主叫联通通话次数';
comment on column ${db_name}.s_td_user_360_d.CALLING_CMC_TIMES	is	'本月主叫移动通话次数';
comment on column ${db_name}.s_td_user_360_d.CALLING_CTC_TIMES	is	'本月主叫电信通话次数';
comment on column ${db_name}.s_td_user_360_d.KEFU_CMC_TIMES	is	'本月移动客服通话次数';
comment on column ${db_name}.s_td_user_360_d.KEFU_CUC_TIMES	is	'本月联通客服通话次数';
comment on column ${db_name}.s_td_user_360_d.UNICOM_H_TIMES	is	'本月呼转联通次数';
comment on column ${db_name}.s_td_user_360_d.MOBILE_H_TIMES	is	'本月呼转移动次数';
comment on column ${db_name}.s_td_user_360_d.PSTN_CALLING_DUR	is	'网内主叫时长';
comment on column ${db_name}.s_td_user_360_d.PSTN_FWD_NUM	is	'网内呼转次数';
comment on column ${db_name}.s_td_user_360_d.COMM_10000_NUM	is	'通话本网客服次数';
comment on column ${db_name}.s_td_user_360_d.BASE_DURATION	is	'本月累计基本计费时长';
comment on column ${db_name}.s_td_user_360_d.TOLL_DURATION	is	'本月累计长途计费时长';
comment on column ${db_name}.s_td_user_360_d.LOACL_DURATION	is	'累积本地计费时长';
comment on column ${db_name}.s_td_user_360_d.LOACL_NOTOLL_DURATION	is	'累积本地拨本地计费时长';
comment on column ${db_name}.s_td_user_360_d.LOACL_CALLING_DURATION	is	'累积本地主叫计费时长';
comment on column ${db_name}.s_td_user_360_d.GN_BILL_DURA	is	'本月国内长途计费时长';
comment on column ${db_name}.s_td_user_360_d.GJ_BILL_DURA	is	'本月国际长途计费时长';
comment on column ${db_name}.s_td_user_360_d.SMS_TIMES	is	'本月累计短信条数(包括行业短信等)';
comment on column ${db_name}.s_td_user_360_d.MMS_TIMES	is	'本月累计彩信条数';
comment on column ${db_name}.s_td_user_360_d.IX_DURATION	is	'本月无线宽带总时长';
comment on column ${db_name}.s_td_user_360_d.IX_BASE_DURATION	is	'本月无线宽带时长_本地时长';
comment on column ${db_name}.s_td_user_360_d.IX_ROAM_DURATION	is	'本月无线宽带时长_漫游';
comment on column ${db_name}.s_td_user_360_d.IX_KBYTES	is	'本月无线宽带总流量';
comment on column ${db_name}.s_td_user_360_d.IX_MO_KBYTES	is	'本月无线宽带总上行流量';
comment on column ${db_name}.s_td_user_360_d.IX_MO_BASE_KBYTES	is	'本月无线宽带总上行流量_本地';
comment on column ${db_name}.s_td_user_360_d.IX_MO_ROAM_KBYTES	is	'本月无线宽带总上行流量_漫游';
comment on column ${db_name}.s_td_user_360_d.IX_MT_KBYTES	is	'本月无线宽带总下行流量';
comment on column ${db_name}.s_td_user_360_d.IX_MT_BASE_KBYTES	is	'本月无线宽带总下行流量_本地';
comment on column ${db_name}.s_td_user_360_d.IX_MT_ROAM_KBYTES	is	'本月无线宽带总下行流量_漫游';
comment on column ${db_name}.s_td_user_360_d.IX_LOC_KBYTES	is	'本月无线宽带流量_本地';
comment on column ${db_name}.s_td_user_360_d.IX_ROAM_KBYTES	is	'本月无线宽带流量_漫游';
comment on column ${db_name}.s_td_user_360_d.WLAN_DURA	is	'本月WLAN上网时长';
comment on column ${db_name}.s_td_user_360_d.G2_DURA	is	'本月2G上网时长';
comment on column ${db_name}.s_td_user_360_d.G3_DURA	is	'本月3G上网时长';
comment on column ${db_name}.s_td_user_360_d.G4_DURA	is	'本月累计4G上网时长';
comment on column ${db_name}.s_td_user_360_d.WLAN_FLUX	is	'本月WLAN上网流量';
comment on column ${db_name}.s_td_user_360_d.G2_FLUX	is	'本月2G上网流量';
comment on column ${db_name}.s_td_user_360_d.G3_FLUX	is	'本月3G上网流量';
comment on column ${db_name}.s_td_user_360_d.GAT_ROAM_BILL_DURA	is	'本月港澳台漫游计费时长';
comment on column ${db_name}.s_td_user_360_d.GAT_ROAM_IX_FLUX	is	'本月港澳台漫游数据流量';
comment on column ${db_name}.s_td_user_360_d.WIFI_3G_MON_CNT	is	'累计3G活跃月份';
comment on column ${db_name}.s_td_user_360_d.WIFI_ACTIVE_MON_CNT	is	'累计wifi活跃月份';
comment on column ${db_name}.s_td_user_360_d.GJ_ROAM_CALLING_BILL_DUR	is	'国际漫游主叫计费时长';
comment on column ${db_name}.s_td_user_360_d.GJ_ROAM_CALLED_BILL_DUR	is	'国际漫游被叫计费时长';
comment on column ${db_name}.s_td_user_360_d.GJ_ROAM_IX_FLUX	is	'国际漫游数据流量';
comment on column ${db_name}.s_td_user_360_d.GJ_ROAM_SMS_TIMS	is	'国际漫游短信条数';
comment on column ${db_name}.s_td_user_360_d.CONSUME_AMT	is	'出账金额';
comment on column ${db_name}.s_td_user_360_d.FEE	is	'总账费用';
comment on column ${db_name}.s_td_user_360_d.CURR_OWE_FEE	is	'当前欠费累计金额';
comment on column ${db_name}.s_td_user_360_d.FIRST_OWE_MONTH	is	'最早欠费月份';
comment on column ${db_name}.s_td_user_360_d.USER_BALANCE	is	'总余额';
comment on column ${db_name}.s_td_user_360_d.VPN_FLAG	is	'是否虚拟网用户';
comment on column ${db_name}.s_td_user_360_d.LEASE_FLAG	is	'是否营销计划用户';
comment on column ${db_name}.s_td_user_360_d.LOCAL_INDUSTRY_TYPE_ID1	is	'本地行业大类';
comment on column ${db_name}.s_td_user_360_d.LOCAL_INDUSTRY_TYPE_ID	is	'本地行业小类（细类）';
comment on column ${db_name}.s_td_user_360_d.LEASE_PLAN_ID	is	'租机计划';
comment on column ${db_name}.s_td_user_360_d.LEASE_PP_EFF_DATE	is	'租机计划生效时间';
comment on column ${db_name}.s_td_user_360_d.LEASE_PP_EXP_DATE	is	'租机计划失效时间';
comment on column ${db_name}.s_td_user_360_d.LEASE_PP_ORDER_DATE	is	'租机计划订购时间';
comment on column ${db_name}.s_td_user_360_d.PLAN_ITEM_NAME	is	'捆绑机型';
comment on column ${db_name}.s_td_user_360_d.DEVICE_BRAND_ID	is	'手机品牌';
comment on column ${db_name}.s_td_user_360_d.DEVICE_MODEL_ID	is	'手机型号';
comment on column ${db_name}.s_td_user_360_d.PLAN_ITEM_NAME_BILL	is	'所属高校';
comment on column ${db_name}.s_td_user_360_d.INTEL_PHONE	is	'是否智能机';
comment on column ${db_name}.s_td_user_360_d.INTEL_PHONE_SYSTEM	is	'操作系统版本';
comment on column ${db_name}.s_td_user_360_d.INTEL_PHONE_VERSION	is	'智能机操作系统版本';
comment on column ${db_name}.s_td_user_360_d.DOUBLE_NET_PHONE	is	'是否双待机器';
comment on column ${db_name}.s_td_user_360_d.PHONE_ORDER_SEQ	is	'手机序号';
comment on column ${db_name}.s_td_user_360_d.FIRST_LEASE_PLAN_ID	is	'入网时租机计划';
comment on column ${db_name}.s_td_user_360_d.FIRST_MAIN_PP_ID	is	'入网时主资费';
comment on column ${db_name}.s_td_user_360_d.BILL_FLAG	is	'累积出账标志(A口径)';
comment on column ${db_name}.s_td_user_360_d.ACTIVE_FLAG	is	'本月是否活跃';
comment on column ${db_name}.s_td_user_360_d.USER_3G_ACTIVE_FLAG	is	'3g用户活跃标志';
comment on column ${db_name}.s_td_user_360_d.NO_URGE_FLAG	is	'免催停标志';
comment on column ${db_name}.s_td_user_360_d.ENT_LEASE_FLAG	is	'是否单位担保租机';
comment on column ${db_name}.s_td_user_360_d.FLUX_PKG_TYPE	is	'订购流量包类型';
comment on column ${db_name}.s_td_user_360_d.PHOTO_FLAG	is	'拍照用户标识';
comment on column ${db_name}.s_td_user_360_d.CHARGE_AMT	is	'缴费金额';
comment on column ${db_name}.s_td_user_360_d.CHARGE_TIMES	is	'缴费次数';
comment on column ${db_name}.s_td_user_360_d.USER_DEVELOP_DEPART_ID	is	'实体入网渠道ID';
comment on column ${db_name}.s_td_user_360_d.USER_DEVELOP_DEPART_ID2	is	'实体入网渠道ID2';
comment on column ${db_name}.s_td_user_360_d.USER_DEVELOP_DEPART_ID3	is	'实体入网渠道ID2';
comment on column ${db_name}.s_td_user_360_d.D_ECHANNEL_USER_TYPE	is	'电子入网渠道ID';
comment on column ${db_name}.s_td_user_360_d.ENT_INDUSTRY_TYPE_ID	is	'客户战略分群ID(政企行业类型)';
comment on column ${db_name}.s_td_user_360_d.USER_CREDIT_VALUE	is	'信用度';
comment on column ${db_name}.s_td_user_360_d.USER_CLASS_ID	is	'用户等级';
comment on column ${db_name}.s_td_user_360_d.ORG_CHANNEL_ID1	is	'集团渠道1级';
comment on column ${db_name}.s_td_user_360_d.ORG_CHANNEL_ID2	is	'集团渠道2级';
comment on column ${db_name}.s_td_user_360_d.ORG_CHANNEL_ID	is	'集团渠道';
comment on column ${db_name}.s_td_user_360_d.G4_FLUX	is	'本月4G流量';
comment on column ${db_name}.s_td_user_360_d.FLAG_4G	is	'是否4G用户：订购4G主套餐或流量包或有4G服务且4G卡且有4G流量';
comment on column ${db_name}.s_td_user_360_d.FLAG_4G_MAIN_PP	is	'是否订购4G主套餐';
comment on column ${db_name}.s_td_user_360_d.FLAG_4G_FLUX_PKG	is	'是否订购4G流量包';
comment on column ${db_name}.s_td_user_360_d.FLAG_4G_NONE_PP	is	'没有主套餐或流量但是有流量';
comment on column ${db_name}.s_td_user_360_d.FLAG_4G_IMEI	is	'4G手机用户，自注册平台';
comment on column ${db_name}.s_td_user_360_d.FLAG_4G_LEASE_PP	is	'4G终端用户，有合约的';
comment on column ${db_name}.s_td_user_360_d.FLAG_4G_SIM	is	'4G卡用户';
comment on column ${db_name}.s_td_user_360_d.FLAG_4G_FUNC	is	'4G功能用户';
comment on column ${db_name}.s_td_user_360_d.FLUX_PKG_G4_ORDER_DATE	is	'4G流量包订购时间';
comment on column ${db_name}.s_td_user_360_d.AGG_SCORE_VALUE	is	'累计积分';
comment on column ${db_name}.s_td_user_360_d.VALID_SCORE_VALUE	is	'有效积分';
comment on column ${db_name}.s_td_user_360_d.INVALID_SCORE_VALUE	is	'无效积分';
comment on column ${db_name}.s_td_user_360_d.MONTH_NEW_SCORE_VALUE	is	'当月新增积分';
comment on column ${db_name}.s_td_user_360_d.MON_CONSUME_SCORE_VALUE	is	'当月消费积分';
comment on column ${db_name}.s_td_user_360_d.MON_ADJUST_SCORE_VALUE	is	'当月调整积分';
comment on column ${db_name}.s_td_user_360_d.KEY_PP_ID	is	'原始主资费（区别于main_pp_id）';
comment on column ${db_name}.s_td_user_360_d.FREE_FLUX_REAL	is	'免费包实际流量';
comment on column ${db_name}.s_td_user_360_d.FREE_FLUX	is	'免费包流量';
comment on column ${db_name}.s_td_user_360_d.MAIN_FLUX_REAL	is	'主套餐内实际流量';
comment on column ${db_name}.s_td_user_360_d.MAIN_FLUX	is	'主套餐内流量';
comment on column ${db_name}.s_td_user_360_d.NOFREE_FLUX_REAL	is	'收费包实际流量';
comment on column ${db_name}.s_td_user_360_d.NOFREE_FLUX	is	'收费包流量';
comment on column ${db_name}.s_td_user_360_d.ADD_FLUX_REAL	is	'加餐包实际流量';
comment on column ${db_name}.s_td_user_360_d.ADD_FLUX	is	'加餐包流量';
comment on column ${db_name}.s_td_user_360_d.OUT_FLUX_REAL	is	'溢出流量';
comment on column ${db_name}.s_td_user_360_d.ACTIVE_DAY_NUM	is	'本月累计活跃天数';
comment on column ${db_name}.s_td_user_360_d.VIP_CARD_TYPE	is	'客户VIP卡类型';
comment on column ${db_name}.s_td_user_360_d.BUILDING_BURA_ID	is	'楼宇区局';
comment on column ${db_name}.s_td_user_360_d.BUILDING_ID	is	'楼宇ID';
comment on column ${db_name}.s_td_user_360_d.BUILDING_KIND_ID	is	'楼宇性质';
comment on column ${db_name}.s_td_user_360_d.BUILDING_NAME	is	'楼宇名称';
comment on column ${db_name}.s_td_user_360_d.BUILDING_TYPE_ID	is	'楼宇类型';
comment on column ${db_name}.s_td_user_360_d.BRD_LINE_RATE	is	'宽带速率';
comment on column ${db_name}.s_td_user_360_d.COOP_CHANNEL	is	'合作渠道(发展代理商)';
comment on column ${db_name}.s_td_user_360_d.FIX_BRD_DURA	is	'本月累计固网宽带时长';
comment on column ${db_name}.s_td_user_360_d.FIX_BRD_FLUX	is	'本月累计固网宽带流量';
comment on column ${db_name}.s_td_user_360_d.C_PSTN_FLAG	is	'标志_移动固网';
comment on column ${db_name}.s_td_user_360_d.IN_MODE_ID	is	'接入方式ID';
comment on column ${db_name}.s_td_user_360_d.YPAY_MAIN_PP_ID	is	'年付费标识';
comment on column ${db_name}.s_td_user_360_d.BI_U_GROUP_ID	is	'用户类型';
comment on column ${db_name}.s_td_user_360_d.EDUCATION_SCHOOL	is	'校园用户标识';
comment on column ${db_name}.s_td_user_360_d.nbr_cnt	is	'对应证件开卡数';
comment on column ${db_name}.s_td_user_360_d.CUST_NAMELEN	is	'用户名长度';
comment on column ${db_name}.s_td_user_360_d.FLAG_G4_DEVICE_SIM	is	'4G终端且换卡标识';
comment on column ${db_name}.s_td_user_360_d.real_balance	is	'实时话费余额';














