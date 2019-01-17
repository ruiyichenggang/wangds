#!/usr/bin/perl
######################################################################
# BTEQ script in Perl, generate by Script Wizard
# Date Time        : 2018/12/28 11:17:27
# Target Table     : PDATA.TB_CUSTOMER_COE_M
# Script File      : TB_CUSTOMER_COE_M.pl
# Interface Name   : 客户价值，粘性系数
# Refresh Frequency: DAILY
# Refresh Mode     : 增量
# Authors          :
# Version Info     : 1.0
# Source Table     : PDATA.TB_CUSTOMER_COE_M
######################################################################
use strict; # Declare using Perl strict syntax
use DBI; #Declare using Perl DBI syntax
######################################################################
# Variable Section

my $AUTO_HOME = "$ENV{AUTO_HOME}";
my $AUTO_DSN = "$ENV{AUTO_DSN}";

my $LOGON_STR;
my $LOGON_FILE = "${AUTO_HOME}/etc/LOGON_ETL";
my $CONTROL_FILE = "";
my $db_name = "$ENV{db_name}";

my $dbh="";
my $PROV="";
my $TX_DATE = "";      #交易日 格式YYYYMMDD

###当前日期是否是星期日 1:是
my $ISSUNDAY_FLAG = "";
my $USER;
my $PASSWD;

###当年天数   当月天数
my $YEARDAYNUM="";
my $MONDAYNUM="";

my $TX_DATE = "";      #交易日 格式YYYYMMDD

##当天，本月业务日期,本月第1天,本月最后1天  格式'YYYYMMDD'
my $DATE_TODAY="";
my $MONTH_TODAY="";
my $MONTH_FIRSTDAY="";
my $MONTH_LASTDAY="";

###本月标示  格式'YYYYMM'
my $TX_MONTH = "";


###上月第1天,上月最后1天,上月当天 格式'YYYYMMDD'
my $LAST1MONTH_FIRSTDAY="";
my $LAST1MONTH_LASTDAY="";
my $LAST1MONTH_TODAY="";

#本年第一天 格式'YYYYMMDD'
my $THIS_YEAR_FIRSTDAY="";

#上年第一天,上年当天,上年最后一天 格式'YYYYMMDD'
my $LASTYEAR_TODAY="";
my $LASTYEAR_FIRSTDAY="";
my $LASTYEAR_LASTDAY="";

###下月第1天,下月最后1天,下月当天 格式'YYYYMMDD'
my $NEXT1MONTH_FIRSTDAY="";
my $NEXT1MONTH_LASTTDAY="";
my $NEXT1MONTH_TODAY="";


###下1月到下3月 格式'YYYYMM'
my $NEXT1MONTH_CHAR="";
my $NEXT2MONTH_CHAR="";
my $NEXT3MONTH_CHAR="";

###上1月到上5月 格式'YYYYMM'
my $LAST1MONTH_CHAR="";
my $LAST2MONTH_CHAR="";
my $LAST3MONTH_CHAR="";
my $LAST4MONTH_CHAR="";
my $LAST5MONTH_CHAR="";

###本年1月到上12月 格式'YYYYMM'
my $THIS_YEAR_MON1 = "";


###上年1月到上12月 格式'YYYYMM'
my $LASTYEAR_1MONTH="";
my $LASTYEAR_12MONTH="";

###上1天到上6天 格式'YYYYMMDD'
my $DATE_TODAY_L1="";
my $DATE_TODAY_L2="";
my $DATE_TODAY_L3="";
my $DATE_TODAY_L4="";
my $DATE_TODAY_L5="";
my $DATE_TODAY_L6="";


###当前日期是否是星期日 1:是
my $ISSUNDAY_FLAG = "";

######################################################################
# BTEQ function
sub run_bteq_command
{
	 open(LOGONFILE_H, "${LOGON_FILE}");
   $LOGON_STR = <LOGONFILE_H>;
   close(LOGONFILE_H);
   $LOGON_STR = `${AUTO_HOME}/bin/IceCode.exe "$LOGON_STR"`;
   my ($logoncmd, $userpw) = split(' ',$LOGON_STR);
   chop($userpw);
   my ($USER, $PASSWD) = split(',' , $userpw);

	 my $connect_string = "$USER\/$PASSWD\@$AUTO_DSN";	 
 #print "Right here11111!\n";
	 my $sqlplus_settings = 'set linesize 300;';
	 my $result = qx { sqlplus $connect_string<<eof
	 $sqlplus_settings
		select 1,sysdate from dual;
	 -------SQL 开始-----------------------------------
	/*
	替换规则：
	20180228  替换成  20180331
	201802    替换成  201803
	*/

-------按月返费范流量政策所有资费——20180224
/*
993028391 	后付费预存分月返话费活动（存100元得200元）
993034829 	2016预存50元分月赠送通话活动-201702
993028393 	后付费预存分月返话费活动（存400元得720元）
993028392 	后付费预存分月返话费活动（存200元得360元）
993033806 	预付费用户预存30元返30元翼支付201610
993028468 	短厅预存分月返话费活动（存200元得360元）
993034826 	后付费预存分月返话费活动（存50元得100元）-201702
993033797 	后付费无合约老用户预存300元返120元翼支付201610
993032443 	2016预存200元分月赠送流量活动
993033807 	预付费用户预存60元返60元翼支付201610
993033809 	预付费用户预存180元返180元翼支付201610
993033798 	后付费无合约老用户预存600元返300元翼支付201610
993033808 	预付费用户预存120元返120元翼支付201610
993033799 	后付费无合约老用户预存1200元返720元翼支付201610
993032441 	2016预存50元分月赠送流量活动
993032440 	2016预存400元分月赠送通话活动
993032438 	2016预存100元分月赠送通话活动
993032444 	2016预存400元分月赠送流量活动
993028467 	短厅预存分月返话费活动（存100元得200元）
993032439 	2016预存200元分月赠送通话活动
993028390 	后付费预存分月返话费活动（存50元得100元）
993028466 	短厅预存分月返话费活动（存50元得100元）
993034828 	2016预存50元分月赠送流量活动-201702
993032437 	2016预存50元分月赠送通话活动
993028469 	短厅预存分月返话费活动（存400元得720元）
993034827 	短厅预存分月返话费活动（存50元得100元）-201702
993032442 	2016预存100元分月赠送流量活动
*/

---不限量资费
/*
天翼不限量129元套餐201802
天翼不限量129元套餐201802（共享）
天翼不限量99元套餐201802
天翼不限量99元套餐201802（共享）
天翼不限量199元套餐201802
天翼不限量199元套餐201802（共享）
天翼不限量299元套餐201802
天翼不限量299元套餐201802（共享）
十全十美不限量299元套餐201802
十全十美不限量299元套餐201802（共享）
天翼不限量399元套餐201802
天翼不限量399元套餐201802（共享）
天翼不限量499元套餐201802
天翼不限量499元套餐201802（共享）
天翼不限量599元套餐201802
天翼不限量599元套餐201802（共享）
天翼不限量999元套餐201802
天翼不限量999元套餐201802（共享）
不限量199元套餐
不限量299元套餐
不限量299元套餐（共享）
不限量399元套餐
不限量399元套餐（共享）
不限量499元套餐
不限量499元套餐（共享）
十全十美不限量299元套餐
十全十美不限量299元套餐（共享）
不限量129元套餐-北京
不限量129元套餐
不限量129元套餐（共享）
不限量169元套餐
不限量169元套餐（共享）
不限量升级包100元201802
不限量升级包100元-5折201802
不限量升级包100元-4折-高档201802
201707不限量视频流量包19元
201707不限量省内流量包100元
201707不限量省内流量包100元-7折
201707不限量省内流量包100元-4折-高档套餐
*/


--------从360中提取目标用户(公众，C网，预后付费，在网)
drop table $db_name.ndxs_temp_gongzhong_user;

CREATE TABLE $db_name.ndxs_temp_gongzhong_user AS(	
   select
    t1.OP_TIME
   ,t1.USER_ID
   ,t1.ACC_NBR
   ,case when t1.MAIN_PROD_ID2='10' then '后付费' else '预付费' end    user_type
   ,case when t1.BILL_FLAG=1 then '1' else '0' end  BILL_FLAG
   ,t1.USER_ONLINE_DURA       ----在网时长
   ,t1.LEASE_FLAG     ----合约标识
   ,t1.AGG_OWE_FEE/100 AGG_OWE_FEE
   ,t1.BILL_FEE/100 ARPU
   ,t1.FEE/100 FEE
   ,t1.CALL_DURATION/60 MOU
   ,(COALESCE(t1.G4_FLUX,0)+COALESCE(t1.G3_FLUX,0)+COALESCE(t1.G2_FLUX,0))/1024    DOU
   ,t2.name
   ,t3.DEPT_NAME1
   ,t3.DEPT_NAME2
   ,t3.DEPT_NAME3
   ,t3.DEPT_NAME4
   ,t3.DEPT_NAME5
   from edw_share.s_td_user_360_$TX_MONTH  t1
 --  inner join ods_share.s_td_user_360_d_20181024 t0  --360用户日表--
 --  on t1.USER_ID=t0.USER_ID
   left join dim.d_price_plan   t2
   on t1.main_pp_id=t2.PRICE_PLAN_CD
   left join DIM.D_DEPT  t3
   on t1.USER_DEVELOP_DEPART_ID=t3.DEPT_ID
   where t1.INNET_FLAG = 1          --在网
   and t1.C_PSTN_FLAG = 1        --C网
   and t1.MAIN_PROD_ID2  in (10,11)   ---后付费，预付费
 --  and t0.op_time = 20181024
 --  and t0.INNET_FLAG = 1          --在网
 --  and t0.CUST_NAMELEN<5
   and t1.CUST_NAMELEN <5
   and t1.OP_TIME = $TX_MONTH
);

--------参与按月返费范流量政策用户
drop table $db_name.ndxs_temp_fanfei_zc_user;

CREATE  TABLE $db_name.ndxs_temp_fanfei_zc_user AS(	
select 
    USER_ID 
   from integ.i_u_user_price_m  ---- sdata.TB_SUBS_PRD_ORDER_M 替换成  integ.i_u_user_price_m
   where USER_ID in (select user_id from $db_name.ndxs_temp_gongzhong_user group by user_id)
   and op_time=$TX_MONTH
   and (TRIM(EXTRACT(YEAR FROM start_dt))
       ||SUBSTR(TO_CHAR(start_dt,'yyyy-mm'),6,7))<=$TX_MONTH
   and (TRIM(EXTRACT(YEAR FROM end_dt))
       ||SUBSTR(TO_CHAR(start_dt,'yyyy-mm'),6,7))>=$TX_MONTH
   and price_plan_cd in ( 993028391, 
                          993034829,
                          993028393,
                          993028392,
                          993033806,
                          993028468,
                          993034826,
                          993033797,
                          993032443,
                          993033807,
                          993033809,
                          993033798,
                          993033808,
                          993033799,
                          993032441,
                          993032440,
                          993032438,
                          993032444,
                          993028467,
                          993032439,
                          993028390,
                          993028466,
                          993034828,
                          993032437,
                          993028469,
                          993034827,
                          993032442)
    group by USER_ID
);

------翼支付年度消费用户
drop table $db_name.ndxs_temp_yzf_active_year_user;

CREATE  TABLE $db_name.ndxs_temp_yzf_active_year_user AS(	
   select
   msisdn ACC_NBR
   from $db_name.TB_EVT_YZF_DEAL_M                       
   where substr(Cen_Plat_Account_Date,1,4)=substr($TX_MONTH,1,4)
   and substr(Cen_Plat_Account_Date,1,6)<=$TX_MONTH
   and (EXCHG_Before_AMT-EXCHG_After_AMT)>0
   group by msisdn
);

-------主卡用户
drop table $db_name.ndxs_temp_zhuka_user;

 CREATE  TABLE $db_name.ndxs_temp_zhuka_user AS(
   select 
    t1.OP_TIME
   ,t1.user_id   USER_ID
   ,t1.START_DT
   from INTEG.I_U_COM_PROD_M  t1
   inner join INTEG.I_U_COM_PROD_M t2
   on t1.COM_PROD_INST_ID=t2.COM_PROD_INST_ID
   and t1.op_time=t2.op_time
   where (TRIM(EXTRACT(YEAR FROM t1.start_dt))
       ||SUBSTR(TO_CHAR(t1.start_dt,'yyyy-mm'),6,7))<=t1.op_time
   and (TRIM(EXTRACT(YEAR FROM t1.END_DT))
       ||SUBSTR(TO_CHAR(t1.END_DT,'yyyy-mm'),6,7))>=t1.op_time
  -- and substr(t1.END_DT,1,6)>=t1.OP_TIME
   and (TRIM(EXTRACT(YEAR FROM t2.start_dt))
       ||SUBSTR(TO_CHAR(t2.start_dt,'yyyy-mm'),6,7))<=t2.op_time
   --and substr(t2.START_DT,1,6)<=t2.OP_TIME
   and (TRIM(EXTRACT(YEAR FROM t2.END_DT))
       ||SUBSTR(TO_CHAR(t2.END_DT,'yyyy-mm'),6,7))>=t2.op_time
   --and substr(t2.END_DT,1,6)>=t2.OP_TIME
   and t1.STATUS_CD<>'22'
   and t1.PROD_COMP_RELA_ROLE_CD='243'
   and t2.PROD_COMP_RELA_ROLE_CD='244'
   and t1.op_time=$TX_MONTH
   group by     t1.OP_TIME
   ,t1.user_id 
   ,t1.START_DT
   --QUALIFY ROW_NUMBER() OVER (PARTITION BY t1.OP_TIME,USER_ID ORDER BY t1.START_DT desc)=1
);


------汇总黏度系数结果表
delete   from $db_name.TB_PHONE_EVALUATE_SCORE_MON
WHERE OP_TIME=$TX_MONTH;

INSERT INTO $db_name.TB_PHONE_EVALUATE_SCORE_MON
select
 $TX_MONTH op_time
,t1.USER_ID
,t1.ACC_NBR
,t1.user_type
,t1.BILL_FLAG
,case when t1.USER_ONLINE_DURA>=24 then 1 else 0 end            ONLINE_SCORE 
,case when t2.USER_ID is not null then 1 else 0 end             RE_FEE_SCORE  
,case when t1.LEASE_FLAG='1' then 1 else 0 end                  CONTRACT_SCORE 
,case when t5.ACC_NBR is not null then 1 else 0 end              HUAN_GO_SCORE  
,case when t3.ACC_NBR is not null then 1 else 0 end              YZF_SCORE 
,case when t4.USER_ID is not null then 1 else 0 end             VICE_CARD_SCORE
,(case when t1.USER_ONLINE_DURA>=24 then 1 else 0 end )+(case when t2.USER_ID is not null then 1 else 0 end )
+(case when t1.LEASE_FLAG='1' then 1 else 0 end)+coalesce(case when t5.ACC_NBR is not null then 1 else 0 end ,0)
+(case when t3.ACC_NBR is not null then 1 else 0 end)+(case when t4.USER_ID is not null then 1 else 0 end )   TOT_SCORE  
from $db_name.ndxs_temp_gongzhong_user  t1
left join $db_name.ndxs_temp_fanfei_zc_user  t2
on t1.USER_ID=t2.USER_ID
left join $db_name.ndxs_temp_yzf_active_year_user  t3
on t1.ACC_NBR=t3.ACC_NBR
left join $db_name.ndxs_temp_zhuka_user t4
on t1.USER_ID=t4.USER_ID
left join $db_name.tb_huan_go  t5       -----------目前有7-10月
on t1.ACC_NBR=t5.ACC_NBR
group by  
 $TX_MONTH
,t1.USER_ID
,t1.ACC_NBR
,t1.user_type
,t1.BILL_FLAG
,case when t1.USER_ONLINE_DURA>=24 then 1 else 0 end
,case when t2.USER_ID is not null then 1 else 0 end
,case when t1.LEASE_FLAG='1' then 1 else 0 end
,case when t5.ACC_NBR is not null then 1 else 0 end  
,case when t3.ACC_NBR is not null then 1 else 0 end
,case when t4.USER_ID is not null then 1 else 0 end 
,(case when t1.USER_ONLINE_DURA>=24 then 1 else 0 end )+(case when t2.USER_ID is not null then 1 else 0 end )+
(case when t1.LEASE_FLAG='1' then 1 else 0 end)+coalesce(case when t5.ACC_NBR is not null then 1 else 0 end ,0)+
(case when t3.ACC_NBR is not null then 1 else 0 end)+(case when t4.USER_ID is not null then 1 else 0 end )
;


----------------------------------------01-02价值指数-01移动公众用户 开始------

------沉默用户
drop table $db_name.ndxs_temp_pg_chenmo_user;
CREATE  TABLE $db_name.ndxs_temp_pg_chenmo_user AS(	
  select
   OP_TIME 
  ,USER_ID
  ,sum(COALESCE(CALL_DURATION,0)+COALESCE(G4_FLUX,0)+COALESCE(G3_FLUX,0)+COALESCE(G2_FLUX,0)+COALESCE(SMS_TIMES,0))   tot_num
   from edw_share.s_td_user_360_$TX_MONTH t1
   where OP_TIME = $TX_MONTH
   and INNET_FLAG  = 1
   and C_PSTN_FLAG = 1  --C网
   and t1.MAIN_PROD_ID2  in (10,11)   ---后付费，预付费 
   group by OP_TIME ,USER_ID
   having sum(COALESCE(CALL_DURATION,0)+COALESCE(G4_FLUX,0)+COALESCE(G3_FLUX,0)+COALESCE(G2_FLUX,0)+COALESCE(SMS_TIMES,0))=0
 ) ;

select 1,sysdate from dual;

------翼支付月活跃用户
drop table $db_name.ndxs_temp_yzf_active_mon_user;
CREATE  TABLE $db_name.ndxs_temp_yzf_active_mon_user AS(	
    select
    MSISDN ACC_NBR 
   from $db_name.TB_EVT_YZF_DEAL_M
   where substr(Cen_Plat_Account_Date,1,6)=substr($TX_MONTH,1,4)||'02'
   group by MSISDN
);

select 2,sysdate from dual;

------翼支付消费用户
drop table $db_name.ndxs_temp_yzf_cmp_mon_user;
CREATE  TABLE $db_name.ndxs_temp_yzf_cmp_mon_user AS(	
   select
    MSISDN ACC_NBR
   from $db_name.TB_EVT_YZF_DEAL_M
   where substr(Cen_Plat_Account_Date,1,6)=substr($TX_MONTH,1,4)||'02'
   and (EXCHG_Before_AMT-EXCHG_After_AMT)>0
   group by MSISDN
);

select 3,sysdate from dual;

------翼支付数据手工数据：①msisdn转换成ACC_NBR ；②substr(Cen_Plat_Account_Date,1,6)=$TX_MONTH 无数据，故改为 substr(Cen_Plat_Account_Date,1,6)=201802
-----天翼视讯活跃
drop table $db_name.ndxs_temp_TYSX_active_user;
CREATE  TABLE $db_name.ndxs_temp_TYSX_active_user AS(	
SELECT
 cal_month OP_TIME
,USER_ID
from $db_name.TB_EVT_C_USER_DPI_M  
where cal_month=$TX_MONTH
and APP_TYPE_NAME2='天翼视讯'
group by cal_month,USER_ID
);

select 4,sysdate from dual;



-----189邮箱活跃
drop table $db_name.ndxs_temp_189_active_user;
CREATE  TABLE $db_name.ndxs_temp_189_active_user AS(	
SELECT 
 cal_month OP_TIME
,USER_ID
from $db_name.TB_EVT_C_USER_DPI_M  
where cal_month=$TX_MONTH
and APP_TYPE_NAME2='189邮箱'
group by cal_month,USER_ID
);

select 5,sysdate from dual;



-----使用APP个数
drop table $db_name.ndxs_temp_app_num;

alter session force parallel dml

CREATE  TABLE $db_name.ndxs_temp_app_num AS(	
select /*+append parallel(t,4) nologging*/
cal_month OP_TIME
,USER_ID
,count(distinct case when VISIT_TYPE='app' then APP_TYPE_NAME2 else null end)  APP_NUM
,count(distinct case when VISIT_TYPE='app' and (MTD_USERFLOW/1024/1024)>=5 then APP_TYPE_NAME2 else null end)    APP_5M_NUM
,count(distinct case when VISIT_TYPE='app' and MTD_PV>=10 then APP_TYPE_NAME2 else null end)    APP_10PV_NUM
from $db_name.TB_EVT_C_USER_DPI_M  
where cal_month=$TX_MONTH --and rownum < 101
group by cal_month,USER_ID
);

alter session disable parallel dml

select 6,sysdate from dual;

-----APP活跃天数
drop table $db_name.ndxs_temp_app_active_days;

alter session force parallel dml

CREATE  TABLE $db_name.ndxs_temp_app_active_days AS(
select
/*+append parallel(t,4) nologging*/
 USER_ID
,APP_TYPE_NAME2
,count(distinct cal_month)    active_days
from $db_name.TB_EVT_C_USER_DPI_M                              
where 
--op_time/100=201802
cal_month=$TX_MONTH
and VISIT_TYPE='app'
group by USER_ID,APP_TYPE_NAME2
having count(distinct cal_month)>=5
);

alter session disable parallel dml

select 7,sysdate from dual;

drop table $db_name.ndxs_temp_app_active_days1;
CREATE  TABLE $db_name.ndxs_temp_app_active_days1 AS(
select
 USER_ID
,count(distinct APP_TYPE_NAME2)   APP_ACT5_NUMS
from $db_name.ndxs_temp_app_active_days                              
group by USER_ID
);

select 8,sysdate from dual;

--20181217修改：该表依赖APP活跃天数数据--
-----ARPU,MOU,DOU及增幅
drop table $db_name.ndxs_temp_arpu_mou_dou;
CREATE  TABLE $出.ndxs_temp_arpu_mou_dou AS(	
 select
    t1.OP_TIME
   ,t1.USER_ID
   ,t1.FLAG_4G_IMEI
   ,t1.flag_4g_sim
   ,t1.FEE/100 ARPU    
   ,count(distinct t2.OP_TIME)  month_num
   ,sum(t2.FEE)/(100*(count(distinct t2.OP_TIME)))   ARPU_LAST3
   ,case when sum(t2.FEE)/(100*(count(distinct t2.OP_TIME)))=0 then null else ((t1.FEE/100)*1.00/sum(t2.FEE)/(100*(count(distinct t2.OP_TIME)))) - 1 end as ARPU_INCREASE
  -- ,(ARPU*1.00/nullifzero(ARPU_LAST3))-1    ARPU_INCREASE    
   ,t1.CALL_DURATION/60         MOU
   ,sum(t2.CALL_DURATION)/(60*(count(distinct t2.OP_TIME)))     MOU_LAST3
   ,case when sum(t2.CALL_DURATION)/(60*(count(distinct t2.OP_TIME))) =0 then null else ((t1.CALL_DURATION/60)*1.00/sum(t2.CALL_DURATION)/(60*(count(distinct t2.OP_TIME)))) - 1 end as MOU_INCREASE
  -- ,(MOU*1.00/nullifzero(MOU_LAST3))-1    MOU_INCREASE    
   ,(COALESCE(t1.G4_FLUX,0)+COALESCE(t1.G3_FLUX,0)+COALESCE(t1.G2_FLUX,0))/1024    DOU
   ,(sum(t2.G4_FLUX)+sum(t2.G3_FLUX)+sum(t2.G2_FLUX))/(1024*(count(distinct t2.OP_TIME)))      DOU_LAST3   
   ,case when (sum(t2.G4_FLUX)+sum(t2.G3_FLUX)+sum(t2.G2_FLUX))/(1024*(count(distinct t2.OP_TIME)))=0 then null else (((COALESCE(t1.G4_FLUX,0)+COALESCE(t1.G3_FLUX,0)+COALESCE(t1.G2_FLUX,0))/1024 )*1.00/(sum(t2.G4_FLUX)+sum(t2.G3_FLUX)+sum(t2.G2_FLUX))/(1024*(count(distinct t2.OP_TIME)))) - 1 end as DOU_INCREASE
  -- ,(DOU*1.00/nullifzero(DOU_LAST3))-1    DOU_INCREASE
   from edw_share.s_td_user_360_$TX_MONTH  t1
   left join edw_share.s_td_user_360_$TX_MONTH  t2
   on t1.USER_ID=t2.USER_ID
   and trunc(t2.OP_TIME/100)=trunc(t1.OP_TIME/100)-1
   and t2.INNET_FLAG = 1 
   where t1.INNET_FLAG = 1          --在网
   and t1.C_PSTN_FLAG = 1        --C网
   and t1.MAIN_PROD_ID2  in (10,11)   ---后付费，预付费
   and t1.OP_TIME = $TX_MONTH
   group by  t1.OP_TIME
   ,t1.USER_ID
   ,t1.FLAG_4G_IMEI
   ,t1.flag_4g_sim
   ,t1.FEE/100
   ,t1.CALL_DURATION/60
   ,(COALESCE(t1.G4_FLUX,0)+COALESCE(t1.G3_FLUX,0)+COALESCE(t1.G2_FLUX,0))/1024
);

select 9,sysdate from dual;

-------自然年内双停次数
drop table $db_name.ndxs_temp_dou_stop_num;
CREATE  TABLE $db_name.ndxs_temp_dou_stop_num AS(	
select
    t1.OP_TIME
   ,t1.USER_ID 
   ,count(distinct t2.OP_TIME)  month_num 
   ,count(case when t1.OP_TIME=t2.OP_TIME then 1 else null end)   dou_stop_flag
   from edw_share.s_td_user_360_$TX_MONTH  t1
   left join edw_share.s_td_user_360_$TX_MONTH  t2
   on t1.USER_ID=t2.USER_ID
   and t2.OP_TIME>=trim(trunc(t1.OP_TIME/100)||'01')
   and t2.OP_TIME<=t1.OP_TIME
   and t2.USER_STS_ID='5'
   where t1.INNET_FLAG = 1          --在网
   and t1.C_PSTN_FLAG = 1        --C网
   and t1.MAIN_PROD_ID2  in (10,11)   ---后付费，预付费
   and t1.OP_TIME = $TX_MONTH
   group by t1.OP_TIME,t1.USER_ID
);

select 10,sysdate from dual;
-------交往圈个数及电信占比
drop table $db_name.ndxs_temp_jwq_dx_num;
CREATE  TABLE $db_name.ndxs_temp_jwq_dx_num AS(	
select
 op_time
,USER_ID
,count(distinct OPP_NBR)     jwq_num
,count(distinct case when OPP_PARTNER_ID='1' then OPP_NBR else null end)  jwq_dx_num
,(count(distinct case when OPP_PARTNER_ID='1' then OPP_NBR else null end))*1.00/(count(distinct OPP_NBR))     jwq_dx_pp
from ods_sum.s_b_cdma_jwzs_m
where op_time=$TX_MONTH
--and user_id='102018570409'
group by  op_time,USER_ID
);

select 11,sysdate from dual;
--------流量包套餐
drop table $db_name.ndxs_temp_flow_packP_AGE;
CREATE  TABLE $db_name.ndxs_temp_flow_packP_AGE AS(	
select
 PRICE_PLAN_CD
,main_flag
,name
from dim.d_price_plan 
where (main_flag<>1 or main_flag is null)
and (name like '%流量包%' or name like '%加餐包%' or name like '%闲时流量%' or name like '%后向流量%' or name like '%定向流量%' or name like '%流量季包%' or name like '%流量半年包%' )
group by  PRICE_PLAN_CD,main_flag,name
);

select 12,sysdate from dual;
-------订购的流量包数量
drop table $db_name.ndxs_temp_flow_packP_AGE_num;
CREATE  TABLE $db_name.ndxs_temp_flow_packP_AGE_num AS(	
select
 user_id
,count(distinct PRICE_PLAN_CD)  num
from integ.i_u_user_price_m
where OP_TIME=$TX_MONTH
and PRICE_PLAN_CD in (select PRICE_PLAN_CD from $db_name.ndxs_temp_flow_packP_AGE group by PRICE_PLAN_CD)
--and (TRIM(EXTRACT(YEAR FROM start_dt))
    --||(CASE WHEN CHAR(TRIM(EXTRACT(MONTH FROM start_dt)))=1 THEN TRIM(0||TRIM(EXTRACT(MONTH FROM start_dt))) ELSE TRIM(EXTRACT(MONTH FROM start_dt)) END))<=201802
--and (TRIM(EXTRACT(YEAR FROM end_dt))
   -- ||(CASE WHEN CHAR(TRIM(EXTRACT(MONTH FROM end_dt)))=1 THEN TRIM(0||TRIM(EXTRACT(MONTH FROM end_dt))) ELSE TRIM(EXTRACT(MONTH FROM end_dt)) END))>=201802
   and (TRIM(EXTRACT(YEAR FROM start_dt))
       ||SUBSTR(TO_CHAR(start_dt,'yyyy-mm'),6,7))<=$TX_MONTH
   and (TRIM(EXTRACT(YEAR FROM end_dt))
       ||SUBSTR(TO_CHAR(start_dt,'yyyy-mm'),6,7))>=$TX_MONTH
group by user_id
);

select 13,sysdate from dual;
-------营销活动参与数量（销售品）
drop table $db_name.ndxs_temp_market_act_num;
CREATE  TABLE $db_name.ndxs_temp_market_act_num AS(	
select
  USER_ID
 ,count(distinct price_plan_cd)  num
from integ.i_u_user_price_m t1
where USER_ID in (select USER_ID user_id from $db_name.ndxs_temp_gongzhong_user where OP_TIME=$TX_MONTH group by USER_ID)
and OP_TIME=$TX_MONTH
--and (TRIM(EXTRACT(YEAR FROM start_dt))
--    ||(CASE WHEN CHAR(TRIM(EXTRACT(MONTH FROM start_dt)))=1 THEN TRIM(0||TRIM(EXTRACT(MONTH FROM start_dt))) ELSE TRIM(EXTRACT(MONTH FROM start_dt)) END))<=201802
--and (TRIM(EXTRACT(YEAR FROM end_dt))
--    ||(CASE WHEN CHAR(TRIM(EXTRACT(MONTH FROM end_dt)))=1 THEN TRIM(0||TRIM(EXTRACT(MONTH FROM end_dt))) ELSE TRIM(EXTRACT(MONTH FROM end_dt)) END))>=201802
   and (TRIM(EXTRACT(YEAR FROM start_dt))
       ||SUBSTR(TO_CHAR(start_dt,'yyyy-mm'),6,7))<=$TX_MONTH
   and (TRIM(EXTRACT(YEAR FROM end_dt))
       ||SUBSTR(TO_CHAR(start_dt,'yyyy-mm'),6,7))>=$TX_MONTH
and price_plan_cd in ( 993028391, 
                          993034829,
                          993028393,
                          993028392,
                          993033806,
                          993028468,
                          993034826,
                          993033797,
                          993032443,
                          993033807,
                          993033809,
                          993033798,
                          993033808,
                          993033799,
                          993032441,
                          993032440,
                          993032438,
                          993032444,
                          993028467,
                          993032439,
                          993028390,
                          993028466,
                          993034828,
                          993032437,
                          993028469,
                          993034827,
                          993032442)
    group by USER_ID
);

select 14,sysdate from dual;
------终端价格、上市时间
drop table $db_name.ndxs_temp_termn_price;
CREATE  TABLE $db_name.ndxs_temp_termn_price AS(	
select
    t1.OP_TIME
   ,t1.USER_ID 
   ,case when t2.market_price is not null then cast(t2.market_price as integer) else 300 end     TERMN_PRICE
   ,case when t2.market_time is not null then 
    substr('$TX_MONTH',1,4)*12+substr('$TX_MONTH',5,2)+1-(substr(t2.market_time,1,4)*12+substr(t2.market_time,5,2)) else 60 end    OTC_TIME
   from edw_share.s_td_user_360_$TX_MONTH  t1
   left join (select
              prod_MODEL
             ,market_price
             ,market_time
              from ods_sum.s_ldapd_trmnl_code_20181227
             -- where day_id=20181210
              group by prod_MODEL,market_price,market_time
              --QUALIFY ROW_NUMBER() OVER (PARTITION BY prod_MODEL ORDER BY market_price desc)=1
              )t2
   on t1.DEVICE_MODEL_ID=t2.prod_MODEL
   where t1.INNET_FLAG = 1          --在网
   and t1.C_PSTN_FLAG = 1        --C网
   and t1.MAIN_PROD_ID2  in (10,11)   ---后付费，预付费
   and t1.OP_TIME = $TX_MONTH
   group by t1.OP_TIME ,t1.USER_ID,case when t2.market_price is not null then cast(t2.market_price as integer) else 300 end ,
   case when t2.market_time is not null then substr('$TX_MONTH',1,4)*12+substr('$TX_MONTH',5,2)+1-(substr(t2.market_time,1,4)*12+substr(t2.market_time,5,2)) else 60 end 
);

select 15,sysdate from dual;
------是否收到过银行/支付宝/微信行短
drop table $db_name.ndxs_temp_sms_user;
CREATE  TABLE $db_name.ndxs_temp_sms_user AS(	
select
 user_id 
from integ.i_u_sms_mo_d t1
where trunc(op_time/100)=$TX_MONTH
and opp_number in (  '8695017'                       
                    ,'8695188'   
                    ,'8695500'   
                    ,'8695501'   
                    ,'8695508'   
                    ,'8695511'   
                    ,'8695518'   
                    ,'8695519'   
                    ,'8695522'   
                    ,'8695528'   
                    ,'8695533'   
                    ,'8695535'   
                    ,'8695555'   
                    ,'8695558'   
                    ,'8695559'   
                    ,'8695561'   
                    ,'8695566'   
                    ,'8695567'   
                    ,'8695568'   
                    ,'8695577'   
                    ,'8695580'   
                    ,'8695588'   
                    ,'8695595'   
                    ,'8695599'   
                    ,'8695105588'
                   )
group by user_id
union
select
 user_id 
from integ.i_u_sms_mt_d t1
where trunc(op_time/100)=$TX_MONTH
and opp_number in (  '8695017'                       
                    ,'8695188'   
                    ,'8695500'   
                    ,'8695501'   
                    ,'8695508'   
                    ,'8695511'   
                    ,'8695518'   
                    ,'8695519'   
                    ,'8695522'   
                    ,'8695528'   
                    ,'8695533'   
                    ,'8695535'   
                    ,'8695555'   
                    ,'8695558'   
                    ,'8695559'   
                    ,'8695561'   
                    ,'8695566'   
                    ,'8695567'   
                    ,'8695568'   
                    ,'8695577'   
                    ,'8695580'   
                    ,'8695588'   
                    ,'8695595'   
                    ,'8695599'   
                    ,'8695105588'
                   )
group by user_id);

select 16,sysdate from dual;

select 17,sysdate from dual;

---主套餐为这个：'标准资费-4G主副卡主卡套餐'或'标准资费-4G主副卡副卡套餐'，然后 I_U表 的壳子ID为不限量套餐的用户
drop table $db_name.ndxs_buxianliang_4g_zhufuka;
CREATE  TABLE $db_name.ndxs_buxianliang_4g_zhufuka AS(	
select
 t1.USER_ID
from edw_share.s_td_user_360_$TX_MONTH  t1
inner join dim.d_price_plan   t2
on t1.main_pp_id=t2.PRICE_PLAN_CD
inner join (
            select
            USER_ID
            FROM INTEG.I_U_COM_PROD_M
            WHERE OP_TIME=$TX_MONTH
            AND COM_KEY_PP_ID IN (select PRICE_PLAN_CD from dim.d_price_plan where 
						NAME LIKE '%不限量%' OR NAME LIKE '%畅享%' AND NAME NOT LIKE '%抖音畅享%' group by PRICE_PLAN_CD)
            group by USER_ID
            )t3
on t1.USER_ID=t3.USER_ID
where t1.INNET_FLAG = 1          --在网
and t1.C_PSTN_FLAG = 1        --C网
and t1.MAIN_PROD_ID2  in (10,11)   ---后付费，预付费
and t1.OP_TIME = $TX_MONTH
and t2.name in ('标准资费-4G主副卡主卡套餐','标准资费-4G主副卡副卡套餐')
group by  t1.USER_ID
);

select 18,sysdate from dual;

drop table $db_name.ndxs_temp_buxianliang_flag;
CREATE  TABLE $db_name.ndxs_temp_buxianliang_flag AS(	
select
  user_id  
from integ.i_u_user_price_m t1
where OP_TIME=$TX_MONTH
and PRICE_PLAN_CD in (select PRICE_PLAN_CD from dim.d_price_plan where 
NAME LIKE '%不限量%' OR NAME LIKE '%畅享%' AND NAME NOT LIKE '%抖音畅享%' group by PRICE_PLAN_CD)
group by user_id
union
select
 USER_ID
from $db_name.ndxs_buxianliang_4g_zhufuka
group by USER_ID
);

select 19,sysdate from dual;

------汇总移动公众客户价值评估基础表
DELETE FROM $db_name.TB_MCUST_VALUE_BASE_MON
where OP_TIME=$TX_MONTH;

alter session force parallel dml

INSERT /*+append parallel(t,4) nologging*/ INTO $db_name.TB_MCUST_VALUE_BASE_MON 
select
 t1.OP_TIME
,t1.USER_ID
,t1.ACC_NBR
,t1.user_type
,t1.BILL_FLAG
,nvl(t10.ARPU,0)
,nvl(t10.ARPU_INCREASE,0)
,nvl(t10.MOU,0)
,nvl(t10.MOU_INCREASE,0)
,nvl(t10.DOU,0)
,nvl(t10.DOU_INCREASE,0)
,case when t21.USER_ID is not null then 1 else 0 end  ----是否订购不限量套餐
,t19.TERMN_PRICE                                     -----终端价格
,case when t19.OTC_TIME=0 then 1 else t19.OTC_TIME end -----终端机龄
,case when t6.USER_ID is not null then 1 else 0 end    ----沉默标识
,case when t10.FLAG_4G_IMEI='1' and t10.flag_4g_sim='1' then 1 else 0 end 
,case when t1.AGG_OWE_FEE>0 then 1 else 0 end         ----是否欠费
,case when t15.dou_stop_flag>0 then 1 else 0 end       ----当月是否双停
,t15.month_num                                         ----自然年度内双停次数
,case when t1.USER_ONLINE_DURA=-1 then 1 else t1.USER_ONLINE_DURA end   USER_ONLINE_DURA
,case when t4.USER_ID is not null then 1 else 0 end     ----是否有副卡
,nvl(t17.num,0)                                    ----订购流量包数量（月包，假日包，加餐包）
,case when t3.ACC_NBR  is not null then 1 else 0 end    ----翼支付是否活跃
,case when t16.ACC_NBR is not null then 1 else 0 end    ----翼支付是否消费
,case when t8.USER_ID is not null then 1 else 0 end    ----天翼视讯活跃
,case when t5.ACC_NBR is not null then 1 else 0 end     ----欢GO活跃
,nvl(t18.num,0)
,nvl(t13.jwq_num,0)
,nvl(t13.jwq_dx_pp,0)
,case when t20.USER_ID is not null then 1 else 0 end    ----是否收到过银行/支付宝/微信行短
,nvl(t12.APP_NUM,0)
,nvl(t12.APP_10PV_NUM,0)
,nvl(t12.APP_5M_NUM,0)
,nvl(t14.APP_ACT5_NUMS,0)
,case when t7.user_id is not null then 1 else 0 end    ----是否机卡异常
from $db_name.ndxs_temp_gongzhong_user t1
left join $db_name.ndxs_temp_fanfei_zc_user  t2
on t1.USER_ID=t2.USER_ID
left join $db_name.ndxs_temp_yzf_active_mon_user  t3
on t1.ACC_NBR=t3.ACC_NBR
left join $db_name.ndxs_temp_zhuka_user t4
on t1.USER_ID=t4.USER_ID
left join (select
             ACC_NBR
           from $db_name.tb_huan_go  
           group by ACC_NBR
           )t5
on t1.ACC_NBR=t5.ACC_NBR
left join $db_name.ndxs_temp_pg_chenmo_user t6
on t1.USER_ID=t6.USER_ID
left join 
(select * from edw_share.s_td_user_360_$TX_MONTH where G4_YICHANG_ESN_FLAG=1 or G4_YICHANG_SERV_FLAG=1) t7
on t1.USER_ID=t7.user_id
/*
$db_name.tb_yichang_1ton_m t7
on t1.USER_ID=t7.user_id
and t7.OP_TIME=$TX_MONTH
and (t7.G4_YICHANG_ESN_FLAG=1 or t7.G4_YICHANG_SERV_FLAG=1)
*/
left join $db_name.ndxs_temp_TYSX_active_user  t8
on t1.USER_ID=t8.USER_ID
left join $db_name.ndxs_temp_189_active_user  t9
on t1.USER_ID=t9.USER_ID
left join $db_name.ndxs_temp_arpu_mou_dou  t10
on t1.USER_ID=t10.USER_ID
--left join $db_name.TB_USER_LIFE_CYCLE_INFO_MODIFY t11
--on t1.USER_ID=t11.USER_ID
and t11.OP_TIME=$TX_MONTH
left join $db_name.ndxs_temp_app_num t12
on t1.USER_ID=t12.USER_ID
left join $db_name.ndxs_temp_jwq_dx_num t13
on t1.USER_ID=t13.USER_ID
left join $db_name.ndxs_temp_app_active_days1 t14
on t1.USER_ID=t14.USER_ID
left join $db_name.ndxs_temp_dou_stop_num t15
on t1.USER_ID=t15.USER_ID
left join $db_name.ndxs_temp_yzf_cmp_mon_user t16
on t1.ACC_NBR=t16.ACC_NBR
left join $db_name.ndxs_temp_flow_packP_AGE_num t17
on t1.USER_ID=t17.user_id
left join $db_name.ndxs_temp_market_act_num  t18
on t1.USER_ID=t18.user_id
left join $db_name.ndxs_temp_termn_price  t19
on t1.USER_ID=t19.USER_ID
left join $db_name.ndxs_temp_sms_user  t20
on t1.USER_ID=t20.USER_ID
left join $db_name.ndxs_temp_buxianliang_flag t21
on t1.USER_ID=t21.USER_ID
where t1.OP_TIME=$TX_MONTH;

alter session disable parallel dml

select 20,sysdate from dual;

------汇总移动公众客户价值评估基础表----预付费
delete   from $db_name.TB_MCUST_VALUE_BASE_MON_BF
where OP_TIME=$TX_MONTH;

alter session force parallel dml

INSERT /*+append parallel(t,4) nologging*/ INTO $db_name.TB_MCUST_VALUE_BASE_MON_BF
select
 t1.OP_TIME
,t1.USER_ID
,t1.ACC_NBR
,t1.user_type
,t1.BILL_FLAG
,nvl(t10.ARPU,0)
,nvl(t10.ARPU_INCREASE,0)
,nvl(t10.MOU,0)
,nvl(t10.MOU_INCREASE,0)
,nvl(t10.DOU,0)
,nvl(t10.DOU_INCREASE,0)
,case when t21.USER_ID is not null then 1 else 0 end  ----是否订购不限量套餐
,t19.TERMN_PRICE                                     -----终端价格
,case when t19.OTC_TIME=0 then 1 else t19.OTC_TIME end -----终端机龄
,case when t6.USER_ID is not null then 1 else 0 end    ----沉默标识
,case when t10.FLAG_4G_IMEI='1' and t10.flag_4g_sim='1' then 1 else 0 end 
,case when t1.AGG_OWE_FEE>0 then 1 else 0 end         ----是否欠费
,case when t15.dou_stop_flag>0 then 1 else 0 end       ----当月是否双停
,t15.month_num                                         ----自然年度内双停次数
,case when t1.USER_ONLINE_DURA=-1 then 1 else t1.USER_ONLINE_DURA end   USER_ONLINE_DURA
,case when t4.USER_ID is not null then 1 else 0 end     ----是否有副卡
,nvl(t17.num,0)                                                ----订购流量包数量（月包，假日包，加餐包）
,case when t3.ACC_NBR is not null then 1 else 0 end    ----翼支付是否活跃
,case when t16.ACC_NBR is not null then 1 else 0 end    ----翼支付是否消费
,case when t8.USER_ID is not null then 1 else 0 end    ----天翼视讯活跃
,case when t5.ACC_NBR is not null then 1 else 0 end     ----欢GO活跃
,nvl(t18.num,0)
,nvl(t13.jwq_num,0)
,nvl(t13.jwq_dx_pp,0)
,case when t20.USER_ID is not null then 1 else 0 end    ----是否收到过银行/支付宝/微信行短
,nvl(t12.APP_NUM,0)
,nvl(t12.APP_10PV_NUM,0)
,nvl(t12.APP_5M_NUM,0)
,nvl(t14.APP_ACT5_NUMS,0)
,case when t7.user_id is not null then 1 else 0 end    ----是否机卡异常
from $db_name.ndxs_temp_gongzhong_user t1
left join $db_name.ndxs_temp_fanfei_zc_user  t2
on t1.USER_ID=t2.USER_ID
left join $db_name.ndxs_temp_yzf_active_mon_user  t3
on t1.ACC_NBR=t3.ACC_NBR
left join $db_name.ndxs_temp_zhuka_user t4
on t1.USER_ID=t4.USER_ID
left join (select
           ACC_NBR
           from $db_name.tb_huan_go  
           group by ACC_NBR
           )t5
on t1.ACC_NBR=t5.ACC_NBR
left join $db_name.ndxs_temp_pg_chenmo_user t6
on t1.USER_ID=t6.USER_ID
left join 
(select * from edw_share.s_td_user_360_$TX_MONTH where G4_YICHANG_ESN_FLAG=1 or G4_YICHANG_SERV_FLAG=1) t7
on t1.USER_ID=t7.user_id
left join $db_name.ndxs_temp_TYSX_active_user  t8
on t1.USER_ID=t8.USER_ID
left join $db_name.ndxs_temp_189_active_user  t9
on t1.USER_ID=t9.USER_ID
left join $db_name.ndxs_temp_arpu_mou_dou  t10
on t1.USER_ID=t10.USER_ID
--left join $db_name.TB_USER_LIFE_CYCLE_INFO_MODIFY t11
--on t1.USER_ID=t11.USER_ID
and t11.OP_TIME=$TX_MONTH
left join $db_name.ndxs_temp_app_num t12
on t1.USER_ID=t12.USER_ID
left join $db_name.ndxs_temp_jwq_dx_num t13
on t1.USER_ID=t13.USER_ID
left join $db_name.ndxs_temp_app_active_days1 t14
on t1.USER_ID=t14.USER_ID
left join $db_name.ndxs_temp_dou_stop_num t15
on t1.USER_ID=t15.USER_ID
left join $db_name.ndxs_temp_yzf_cmp_mon_user t16
on t1.ACC_NBR=t16.ACC_NBR
left join $db_name.ndxs_temp_flow_packP_AGE_num t17
on t1.USER_ID=t17.user_id
left join $db_name.ndxs_temp_market_act_num  t18
on t1.USER_ID=t18.user_id
left join $db_name.ndxs_temp_termn_price  t19
on t1.USER_ID=t19.USER_ID
left join $db_name.ndxs_temp_sms_user  t20
on t1.USER_ID=t20.USER_ID
left join $db_name.ndxs_temp_buxianliang_flag t21
on t1.USER_ID=t21.USER_ID
where t1.OP_TIME=$TX_MONTH
and t1.user_type='预付费'
;

alter session disable parallel dml

select 21,sysdate from dual;

------汇总移动公众客户价值评估基础表----后付费
delete   from $db_name.TB_MCUST_VALUE_BASE_MON_AF
where OP_TIME=$TX_MONTH;

alter session force parallel dml

INSERT /*+append parallel(t,4) nologging*/ INTO $db_name.TB_MCUST_VALUE_BASE_MON_AF
select
 t1.OP_TIME
,t1.USER_ID
,t1.ACC_NBR
,t1.user_type
,t1.BILL_FLAG
,nvl(t10.ARPU,0)
,nvl(t10.ARPU_INCREASE,0)
,nvl(t10.MOU,0)
,nvl(t10.MOU_INCREASE,0)
,nvl(t10.DOU,0)
,nvl(t10.DOU_INCREASE,0)
,case when t21.USER_ID is not null then 1 else 0 end  ----是否订购不限量套餐
,t19.TERMN_PRICE                                     -----终端价格
,case when t19.OTC_TIME=0 then 1 else t19.OTC_TIME end -----终端机龄
,case when t6.USER_ID is not null then 1 else 0 end    ----沉默标识
,case when t10.FLAG_4G_IMEI='1' and t10.flag_4g_sim='1' then 1 else 0 end 
,case when t1.AGG_OWE_FEE>0 then 1 else 0 end         ----是否欠费
,case when t15.dou_stop_flag>0 then 1 else 0 end       ----当月是否双停
,t15.month_num                                         ----自然年度内双停次数
,case when t1.USER_ONLINE_DURA=-1 then 1 else t1.USER_ONLINE_DURA end   USER_ONLINE_DURA
,case when t4.USER_ID is not null then 1 else 0 end     ----是否有副卡
,nvl(t17.num,0)                                                ----订购流量包数量（月包，假日包，加餐包）
,case when t3.ACC_NBR is not null then 1 else 0 end    ----翼支付是否活跃
,case when t16.ACC_NBR is not null then 1 else 0 end    ----翼支付是否消费
,case when t8.USER_ID is not null then 1 else 0 end    ----天翼视讯活跃
,case when t5.ACC_NBR is not null then 1 else 0 end     ----欢GO活跃
,nvl(t18.num,0)
,nvl(t13.jwq_num,0)
,nvl(t13.jwq_dx_pp,0)
,case when t20.USER_ID is not null then 1 else 0 end    ----是否收到过银行/支付宝/微信行短
,nvl(t12.APP_NUM,0)
,nvl(t12.APP_10PV_NUM,0)
,nvl(t12.APP_5M_NUM,0)
,nvl(t14.APP_ACT5_NUMS,0)
,case when t7.user_id is not null then 1 else 0 end    ----是否机卡异常
from $db_name.ndxs_temp_gongzhong_user t1
left join $db_name.ndxs_temp_fanfei_zc_user  t2
on t1.USER_ID=t2.USER_ID
left join $db_name.ndxs_temp_yzf_active_mon_user  t3
on t1.ACC_NBR=t3.ACC_NBR
left join $db_name.ndxs_temp_zhuka_user t4
on t1.USER_ID=t4.USER_ID
left join (select 
           ACC_NBR
           from $db_name.tb_huan_go 
           group by ACC_NBR
           )t5
on t1.ACC_NBR=t5.ACC_NBR
left join $db_name.ndxs_temp_pg_chenmo_user t6
on t1.USER_ID=t6.USER_ID
left join 
(select * from edw_share.s_td_user_360_$TX_MONTH where G4_YICHANG_ESN_FLAG=1 or G4_YICHANG_SERV_FLAG=1) t7
on t1.USER_ID=t7.user_id
left join $db_name.ndxs_temp_TYSX_active_user  t8
on t1.USER_ID=t8.USER_ID
left join $db_name.ndxs_temp_189_active_user  t9
on t1.USER_ID=t9.USER_ID
left join $db_name.ndxs_temp_arpu_mou_dou  t10
on t1.USER_ID=t10.USER_ID
--left join $db_name.TB_USER_LIFE_CYCLE_INFO_MODIFY t11
--on t1.USER_ID=t11.USER_ID
and t11.OP_TIME=$TX_MONTH
left join $db_name.ndxs_temp_app_num t12
on t1.USER_ID=t12.USER_ID
left join $db_name.ndxs_temp_jwq_dx_num t13
on t1.USER_ID=t13.USER_ID
left join $db_name.ndxs_temp_app_active_days1 t14
on t1.USER_ID=t14.USER_ID
left join $db_name.ndxs_temp_dou_stop_num t15
on t1.USER_ID=t15.USER_ID
left join $db_name.ndxs_temp_yzf_cmp_mon_user t16
on t1.ACC_NBR=t16.ACC_NBR
left join $db_name.ndxs_temp_flow_packP_AGE_num t17
on t1.USER_ID=t17.user_id
left join $db_name.ndxs_temp_market_act_num  t18
on t1.USER_ID=t18.user_id
left join $db_name.ndxs_temp_termn_price  t19
on t1.USER_ID=t19.USER_ID
left join $db_name.ndxs_temp_sms_user  t20
on t1.USER_ID=t20.USER_ID
left join $db_name.ndxs_temp_buxianliang_flag t21
on t1.USER_ID=t21.USER_ID
where t1.OP_TIME=$TX_MONTH
and t1.user_type='后付费'
;

alter session disable parallel dml

select 22,sysdate from dual;

------汇总移动公众客户价值评估基础表----预付费----极值表
delete   from $db_name.TB_MCUST_VALUE_CAL_MON_BF;

alter session force parallel dml

INSERT /*+append parallel(t,4) nologging*/ INTO $db_name.TB_MCUST_VALUE_CAL_MON_BF 
select
 t1.OP_TIME
,t1.USER_ID
,t1.ACC_NBR
,t1.user_type
,t1.BILL_FLAG
,case when ARPU<0 then 0                      ------ARPU
      when ARPU>60 then 60
      else ARPU end
,case when ARPU_INCREASE_PP<-1 then -1           ------当月ARPU 比上年平均ARPU增幅
      when ARPU_INCREASE_PP>1  then  1
      else ARPU_INCREASE_PP end
,case when MOU<0 then 0                       ------MOU
      when MOU>350 then 350
      else MOU end
,case when MOU_INCREASE_PP<-1 then -1            ------当月MOU比上年平均MOU增幅
      when MOU_INCREASE_PP>1  then  1
      else MOU_INCREASE_PP end
,case when DOU<0 then 0                       ------DOU
      when DOU>1024 then 1024
      else DOU end
,case when DOU_INCREASE_PP<-1 then -1            ------当月DOU比上年平均DOU增幅
      when DOU_INCREASE_PP>2  then  2
      else DOU_INCREASE_PP end
,TERMN_PRICE                                    
,OTC_TIME                                       
,CHENMO_FLAG                                    
,G4_FLAG 
,QIANFEI_FLAG                                   
,DOU_STOP_FLAG                                  
,DOU_STOP_NUM                                   
,case when USER_ONLINE_DURA<1 then 1                  ----在网时长     
      when USER_ONLINE_DURA>120 then 120
      else USER_ONLINE_DURA end                          
,ZHUFUKA_FLAG                                  
,case when FLOW_PACKET_NUM>4 then 4 else FLOW_PACKET_NUM end   ----订购流量包数量（月包，假日包，加餐包）
,YZF_ACT_FLAG                          
,YZF_CSM_FLAG   
,TYSX_ACT_FLAG  
,HUANGO_ACT_FLAG   
,MARKET_NUM
,case when jwq_num>100 then 100 else jwq_num end      -------交往圈个数
,JWQ_TELE_PP
,MESS_FLAG
,case when APP_NUMS>25 then 25 else APP_NUMS end                          ------APP使用个数
,case when APP_VISIT_10_NUMS>15 then 15 else APP_VISIT_10_NUMS end        ------访问次数大于10次的APP个数
,case when APP_5M_NUMS>3 then 3 else APP_5M_NUMS end                      ------使用流量大于5M的APP个数
,case when APP_ACT_5D_NUMS>25 then 25 else APP_ACT_5D_NUMS end            ------活跃天数大于5的APP个数
,YJDK_FLAG
from $db_name.TB_MCUST_VALUE_BASE_MON_BF t1;

alter session disable parallel dml

select 23,sysdate from dual;

------汇总移动公众客户价值评估基础表----后付费----极值表
delete   from $db_name.TB_MCUST_VALUE_CAL_MON_AF;

alter session force parallel dml

INSERT /*+append parallel(t,4) nologging*/ INTO $db_name.TB_MCUST_VALUE_CAL_MON_AF 
select 
 t1.OP_TIME
,t1.USER_ID
,t1.ACC_NBR
,t1.user_type
,t1.BILL_FLAG
,case when ARPU<0 then 0                      ------ARPU
      when ARPU>60 then 60
      else ARPU end
,case when ARPU_INCREASE_PP<-1 then -1           ------当月ARPU 比上年平均ARPU增幅
      when ARPU_INCREASE_PP>1  then  1
      else ARPU_INCREASE_PP end
,case when MOU<0 then 0                       ------MOU
      when MOU>350 then 350
      else MOU end
,case when MOU_INCREASE_PP<-1 then -1            ------当月MOU比上年平均MOU增幅
      when MOU_INCREASE_PP>1  then  1
      else MOU_INCREASE_PP end
,case when DOU<0 then 0                       ------DOU
      when DOU>1024 then 1024
      else DOU end
,case when DOU_INCREASE_PP<-1 then -1            ------当月DOU比上年平均DOU增幅
      when DOU_INCREASE_PP>2  then  2
      else DOU_INCREASE_PP end
,TERMN_PRICE                                    
,OTC_TIME                                       
,CHENMO_FLAG                                    
,G4_FLAG 
,QIANFEI_FLAG                                   
,DOU_STOP_FLAG                                  
,DOU_STOP_NUM                                   
,case when USER_ONLINE_DURA<1 then 1                  ----在网时长     
      when USER_ONLINE_DURA>120 then 120
      else USER_ONLINE_DURA end                          
,ZHUFUKA_FLAG                                  
,case when FLOW_PACKET_NUM>4 then 4 else FLOW_PACKET_NUM end   ----订购流量包数量（月包，假日包，加餐包）
,YZF_ACT_FLAG                          
,YZF_CSM_FLAG   
,TYSX_ACT_FLAG  
,HUANGO_ACT_FLAG   
,MARKET_NUM
,case when jwq_num>100 then 100 else jwq_num end      -------交往圈个数
,JWQ_TELE_PP
,MESS_FLAG
,case when APP_NUMS>25 then 25 else APP_NUMS end                          ------APP使用个数
,case when APP_VISIT_10_NUMS>15 then 15 else APP_VISIT_10_NUMS end        ------访问次数大于10次的APP个数
,case when APP_5M_NUMS>3 then 3 else APP_5M_NUMS end                      ------使用流量大于5M的APP个数
,case when APP_ACT_5D_NUMS>25 then 25 else APP_ACT_5D_NUMS end            ------活跃天数大于5的APP个数
,YJDK_FLAG
from $db_name.TB_MCUST_VALUE_BASE_MON_AF t1
;

alter session disable parallel dml

select 24,sysdate from dual;

----------执行标准化SQL—新版
----2、标准化
----2.1、0-1标准化【0.5~1】，ARPU、DOU、终端价格、机龄、网龄分层标准化，预付费
DROP TABLE 	$db_name.TB_MCUST_VALUE_CAL_MON_BF_STD;
CREATE TABLE $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD
(
 op_time 	INTEGER 
,user_id 	NUMBER(16,0) 
,acc_nbr 	VARCHAR(20) 
,USER_TYPE 	VARCHAR(20) 
,ARPU 	FLOAT
,ARPU_INCREASE_PP 	FLOAT
,DOU 	FLOAT
,DOU_INCREASE_PP 	FLOAT
,TERMN_PRICE 	FLOAT
,OTC_TIME 	FLOAT
,CHENMO_FLAG  	FLOAT
,G4_FLAG 	FLOAT
,BILL_FLAG 	FLOAT
,USER_ONLINE_DURA  	FLOAT
,ZHUFUKA_FLAG 	FLOAT
,FLOW_PACKET_NUM 	FLOAT
,YZF_CSM_FLAG 	FLOAT
,TYSX_ACT_FLAG 	FLOAT
,HUANGO_ACT_FLAG 	FLOAT
,MARKET_NUM 	FLOAT
,JWQ_NUM 	FLOAT
,JWQ_TELE_PP 	FLOAT
,MESS_FLAG 	FLOAT
,APP_NUMS 	FLOAT
,APP_VISIT_10_NUMS 	FLOAT
,APP_5M_NUMS   	FLOAT
,APP_ACT_5D_NUMS 	FLOAT
,YJDK_FLAG 	NUMBER(4,0) 
);

comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.op_time  is  '处理日期'                           ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.user_id  is  '用户id'                               ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.acc_nbr  is  '手机号码'                              ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.USER_TYPE  is  '用户类型'                           ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.ARPU  is  '总账收入'                                ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.ARPU_INCREASE_PP  is  '总账收入增幅'                ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.DOU  is  '流量'                                     ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.DOU_INCREASE_PP  is  '流量增幅'                     ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.TERMN_PRICE  is  '终端价格'                         ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.OTC_TIME  is  '终端机龄'                            ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.CHENMO_FLAG   is  '是否沉默用户'                    ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.G4_FLAG  is  '是否4G用户'                           ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.BILL_FLAG  is  '是否出账'                           ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.USER_ONLINE_DURA   is  '在网时长'                           ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.ZHUFUKA_FLAG  is  '是否有副卡'                      ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.FLOW_PACKET_NUM  is  '订购流量包数量'               ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.YZF_CSM_FLAG  is  '翼支付是否消费'                  ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.TYSX_ACT_FLAG  is  '天翼视讯活跃'                   ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.HUANGO_ACT_FLAG  is  '欢GO活跃'                     ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.MARKET_NUM  is  '营销活动参与数量'                  ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.JWQ_NUM  is  '交往圈个数'                           ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.JWQ_TELE_PP  is  '交往圈中电信号码占比'             ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.MESS_FLAG  is  '是否收到过银行/支付宝/微信行短'     ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.APP_NUMS  is  'APP使用个数'                         ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.APP_VISIT_10_NUMS  is  '访问次数大于10次的APP个数'  ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.APP_5M_NUMS    is  '使用流量大于5M的APP个数'        ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.APP_ACT_5D_NUMS  is  '活跃天数大于5的APP个数'       ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD.YJDK_FLAG  is  '是否机卡异常'                       ;

select 25,sysdate from dual;

DELETE FROM $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD WHERE op_time=$TX_MONTH;

alter session force parallel dml

INSERT /*+append parallel(t,4) nologging*/ INTO	$db_name.TB_MCUST_VALUE_CAL_MON_BF_STD
SELECT	op_time 	
	,user_id 	
	,acc_nbr 	
	,USER_TYPE 	
	,CASE WHEN ARPU>=3  AND ARPU<10 	THEN 0.5     + 0.1400 * (CAST(ARPU	AS FLOAT) - 3)  / (10 - 3)
	      WHEN ARPU>=10 AND ARPU<20	THEN 0.64    + 0.1333 * (CAST(ARPU	AS FLOAT) - 10) / (20 - 10)
	      WHEN ARPU>=20 AND ARPU<=40	THEN 0.7733  + 0.2267 * (CAST(ARPU	AS FLOAT) - 20) / (40 - 20)
	 END AS ARPU
	,decode((MAX_ARPU_INCREASE_PP	- MIN_ARPU_INCREASE_PP),0,0.5,0.5+0.5 * ((CAST(ARPU_INCREASE_PP AS FLOAT)	- MIN_ARPU_INCREASE_PP  ) / (MAX_ARPU_INCREASE_PP	- MIN_ARPU_INCREASE_PP  ))) AS ARPU_INCREASE_PP    
	,CASE WHEN DOU>=0   AND DOU<100 	THEN 0.5 + 0.1 * (CAST(DOU	AS FLOAT) - 0)   / (100 - 0)
	      WHEN DOU>=100 AND DOU<500	THEN 0.6 + 0.2 * (CAST(DOU	AS FLOAT) - 100) / (500 - 100)
	      WHEN DOU>=500 AND DOU<=1000	THEN 0.8 + 0.2 * (CAST(DOU	AS FLOAT) - 500) / (1000 - 500)
	 END AS DOU
	,decode((MAX_DOU_INCREASE_PP 	- MIN_DOU_INCREASE_PP),0,0.5,0.5 + 0.5 * ((CAST(DOU_INCREASE_PP 	AS FLOAT)	- MIN_DOU_INCREASE_PP   ) / (MAX_DOU_INCREASE_PP 	- MIN_DOU_INCREASE_PP))) AS DOU_INCREASE_PP
	,CASE WHEN TERMN_PRICE < 1000	THEN 0.5
	      WHEN TERMN_PRICE < 2500	THEN 0.75
	      ELSE			     1.0
	END AS TERMN_PRICE
	,CASE WHEN OTC_TIME < 12	THEN 1.0
	      WHEN OTC_TIME < 24	THEN 0.8333
	      WHEN OTC_TIME < 36	THEN 0.6667
	      ELSE			     0.5
	END AS OTC_TIME
	,decode((MIN_CHENMO_FLAG  	- MAX_CHENMO_FLAG  	),0,0.5,0.5 + 0.5 * ((CAST(CHENMO_FLAG  	AS FLOAT)	- MAX_CHENMO_FLAG  	) / (MIN_CHENMO_FLAG  	- MAX_CHENMO_FLAG  	))) AS CHENMO_FLAG  		--负向指标
	,decode((MAX_G4_FLAG 	- MIN_G4_FLAG 	),0,0.5,0.5 + 0.5 * ((CAST(G4_FLAG 	AS FLOAT)	- MIN_G4_FLAG 	) / (MAX_G4_FLAG 	- MIN_G4_FLAG 	))) AS G4_FLAG
	,decode((MAX_BILL_FLAG 	- MIN_BILL_FLAG 	),0,0.5,0.5 + 0.5 * ((CAST(BILL_FLAG 	AS FLOAT)	- MIN_BILL_FLAG 	) / (MAX_BILL_FLAG 	- MIN_BILL_FLAG 	))	) AS BILL_FLAG 	            
	,CASE WHEN USER_ONLINE_DURA < 12	THEN 0.5
	      WHEN USER_ONLINE_DURA < 24	THEN 0.6667
	      WHEN USER_ONLINE_DURA < 36	THEN 0.8333
	      ELSE			     1.0
	END AS USER_ONLINE_DURA
	,decode((MAX_ZHUFUKA_FLAG 	- MIN_ZHUFUKA_FLAG 	),0,0,0.0 + 1.0 * ((CAST(ZHUFUKA_FLAG 	AS FLOAT)	- MIN_ZHUFUKA_FLAG 	) / (MAX_ZHUFUKA_FLAG 	- MIN_ZHUFUKA_FLAG 	))) AS ZHUFUKA_FLAG 
  ,decode( (MAX_FLOW_PACKET_NUM 	- MIN_FLOW_PACKET_NUM   ),0,0.5,0.5 + 0.5 * ((CAST(FLOW_PACKET_NUM 	AS FLOAT)	- MIN_FLOW_PACKET_NUM   ) / (MAX_FLOW_PACKET_NUM 	- MIN_FLOW_PACKET_NUM   ))) AS FLOW_PACKET_NUM 
	,decode((MAX_YZF_CSM_FLAG 	- MIN_YZF_CSM_FLAG 	),0,0.5,0.5 + 0.5 * ((CAST(YZF_CSM_FLAG 	AS FLOAT)	- MIN_YZF_CSM_FLAG 	) / (MAX_YZF_CSM_FLAG 	- MIN_YZF_CSM_FLAG 	))	 	) AS YZF_CSM_FLAG 		
	,decode((MAX_TYSX_ACT_FLAG 	- MIN_TYSX_ACT_FLAG 	),0,0.5,0.5 + 0.5 * ((CAST(TYSX_ACT_FLAG 	AS FLOAT)	- MIN_TYSX_ACT_FLAG 	) / (MAX_TYSX_ACT_FLAG 	- MIN_TYSX_ACT_FLAG 	))	) AS TYSX_ACT_FLAG
	,decode((MAX_HUANGO_ACT_FLAG 	- MIN_HUANGO_ACT_FLAG   ),0,0.5,0.5 + 0.5 * ((CAST(HUANGO_ACT_FLAG 	AS FLOAT)	- MIN_HUANGO_ACT_FLAG   ) / (MAX_HUANGO_ACT_FLAG 	- MIN_HUANGO_ACT_FLAG   ))) AS HUANGO_ACT_FLAG
	,decode((MAX_MARKET_NUM 	- MIN_MARKET_NUM 	),0,0.5,0.5 + 0.5 * ((CAST(MARKET_NUM 	AS FLOAT)	- MIN_MARKET_NUM 	) / (MAX_MARKET_NUM 	- MIN_MARKET_NUM 	))) AS MARKET_NUM 	 	
	,decode((MAX_JWQ_NUM 	- MIN_JWQ_NUM 	),0,0.5,0.5 + 0.5 * ((CAST(JWQ_NUM 	AS FLOAT)	- MIN_JWQ_NUM 	) / (MAX_JWQ_NUM 	- MIN_JWQ_NUM 	))) AS JWQ_NUM 	 		
	,decode((MAX_JWQ_TELE_PP 	- MIN_JWQ_TELE_PP 	),0,0.5,0.5 + 0.5 * ((CAST(JWQ_TELE_PP 	AS FLOAT)	- MIN_JWQ_TELE_PP 	) / (MAX_JWQ_TELE_PP 	- MIN_JWQ_TELE_PP 	))) AS JWQ_TELE_PP 	 	            
	,decode((MAX_MESS_FLAG 	- MIN_MESS_FLAG 	),0,0.5,0.5 + 0.5 * ((CAST(MESS_FLAG 	AS FLOAT)	- MIN_MESS_FLAG 	) / (MAX_MESS_FLAG 	- MIN_MESS_FLAG 	))) AS MESS_FLAG
	,decode((MAX_APP_NUMS 	- MIN_APP_NUMS 	),0,0.5,0.5 + 0.5 * ((CAST(APP_NUMS 	AS FLOAT)	- MIN_APP_NUMS 	) / (MAX_APP_NUMS 	- MIN_APP_NUMS 	))) AS APP_NUMS
	,decode((MAX_APP_VISIT_10_NUM	- MIN_APP_VISIT_10_NUM  ),0,0.5,0.5 + 0.5 * ((CAST(APP_VISIT_10_NUMS AS FLOAT)	- MIN_APP_VISIT_10_NUM  ) / (MAX_APP_VISIT_10_NUM	- MIN_APP_VISIT_10_NUM  ))) AS APP_VISIT_10_NUMS 	
	,decode((MAX_APP_5M_NUMS   		  - MIN_APP_5M_NUMS   	),0,0.5,0.5 + 0.5 * ((CAST(APP_5M_NUMS   	AS FLOAT)	- MIN_APP_5M_NUMS   	) / (MAX_APP_5M_NUMS   		  - MIN_APP_5M_NUMS   	))) AS APP_5M_NUMS
	,decode((MAX_APP_ACT_5D_NUMS 	- MIN_APP_ACT_5D_NUMS   ),0,0.5,0.5 + 0.5 * ((CAST(APP_ACT_5D_NUMS 	AS FLOAT)	- MIN_APP_ACT_5D_NUMS   ) /(MAX_APP_ACT_5D_NUMS 	- MIN_APP_ACT_5D_NUMS   ) )	) AS APP_ACT_5D_NUMS     	
	,YJDK_FLAG	 	
FROM 	$db_name.TB_MCUST_VALUE_CAL_MON_BF	A
,(
SELECT	 MAX(ARPU 		) AS MAX_ARPU 	 
	,MAX(ARPU_INCREASE_PP   ) AS MAX_ARPU_INCREASE_PP
	,MAX(DOU 		) AS MAX_DOU 		
	,MAX(DOU_INCREASE_PP 	) AS MAX_DOU_INCREASE_PP 
	,MAX(TERMN_PRICE 	) AS MAX_TERMN_PRICE 	
	,MAX(OTC_TIME 	) AS MAX_OTC_TIME 	 
	,MAX(CHENMO_FLAG  	) AS MAX_CHENMO_FLAG  	
	,MAX(G4_FLAG 	) AS MAX_G4_FLAG 	 
	,MAX(BILL_FLAG 	) AS MAX_BILL_FLAG 	 
	,MAX(USER_ONLINE_DURA  	) AS MAX_USER_ONLINE_DURA  	 
	,MAX(ZHUFUKA_FLAG 	) AS MAX_ZHUFUKA_FLAG 	
	,MAX(FLOW_PACKET_NUM 	) AS MAX_FLOW_PACKET_NUM 
	,MAX(YZF_CSM_FLAG 	) AS MAX_YZF_CSM_FLAG 	
	,MAX(TYSX_ACT_FLAG 	) AS MAX_TYSX_ACT_FLAG 	
	,MAX(HUANGO_ACT_FLAG 	) AS MAX_HUANGO_ACT_FLAG 
	,MAX(MARKET_NUM 	) AS MAX_MARKET_NUM 	
	,MAX(JWQ_NUM 	) AS MAX_JWQ_NUM 	 
	,MAX(JWQ_TELE_PP 	) AS MAX_JWQ_TELE_PP 	
	,MAX(MESS_FLAG 	) AS MAX_MESS_FLAG 	 
	,MAX(APP_NUMS 	) AS MAX_APP_NUMS 	 
	,MAX(APP_VISIT_10_NUMS  ) AS MAX_APP_VISIT_10_NUM
	,MAX(APP_5M_NUMS   	) AS MAX_APP_5M_NUMS   	
	,MAX(APP_ACT_5D_NUMS 	) AS MAX_APP_ACT_5D_NUMS 
	,MAX(YJDK_FLAG 	) AS MAX_YJDK_FLAG 	 
	,MIN(ARPU 		) AS MIN_ARPU 	 
	,MIN(ARPU_INCREASE_PP   ) AS MIN_ARPU_INCREASE_PP
	,MIN(DOU 		) AS MIN_DOU 	
	,MIN(DOU_INCREASE_PP 	) AS MIN_DOU_INCREASE_PP 
	,MIN(TERMN_PRICE 	) AS MIN_TERMN_PRICE 	
	,MIN(OTC_TIME 	) AS MIN_OTC_TIME 	 
	,MIN(CHENMO_FLAG  	) AS MIN_CHENMO_FLAG  	
	,MIN(G4_FLAG 	) AS MIN_G4_FLAG 	 
	,MIN(BILL_FLAG 	) AS MIN_BILL_FLAG 	 
	,MIN(USER_ONLINE_DURA  	) AS MIN_USER_ONLINE_DURA  	 
	,MIN(ZHUFUKA_FLAG 	) AS MIN_ZHUFUKA_FLAG 	
	,MIN(FLOW_PACKET_NUM 	) AS MIN_FLOW_PACKET_NUM 
	,MIN(YZF_CSM_FLAG 	) AS MIN_YZF_CSM_FLAG 	
	,MIN(TYSX_ACT_FLAG 	) AS MIN_TYSX_ACT_FLAG 	
	,MIN(HUANGO_ACT_FLAG 	) AS MIN_HUANGO_ACT_FLAG 
	,MIN(MARKET_NUM 	) AS MIN_MARKET_NUM 	
	,MIN(JWQ_NUM 	) AS MIN_JWQ_NUM 	 
	,MIN(JWQ_TELE_PP 	) AS MIN_JWQ_TELE_PP 	
	,MIN(MESS_FLAG 	) AS MIN_MESS_FLAG 	 
	,MIN(APP_NUMS 	) AS MIN_APP_NUMS 	 
	,MIN(APP_VISIT_10_NUMS  ) AS MIN_APP_VISIT_10_NUM
	,MIN(APP_5M_NUMS   	) AS MIN_APP_5M_NUMS   	
	,MIN(APP_ACT_5D_NUMS 	) AS MIN_APP_ACT_5D_NUMS 
	,MIN(YJDK_FLAG 	) AS MIN_YJDK_FLAG 	 	
FROM	$db_name.TB_MCUST_VALUE_CAL_MON_BF
WHERE	op_time=$TX_MONTH
)				B
WHERE	op_time=$TX_MONTH
;

alter session disable parallel dml

select 26,sysdate from dual;

----2.2、0-1标准化【0.5~1】，ARPU、DOU、终端价格、机龄、网龄分层标准化，后付费
DROP TABLE 	$db_name.TB_MCUST_VALUE_CAL_MON_AF_STD;
CREATE TABLE $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD
(
 op_time 	INTEGER 
,user_id 	NUMBER(16,0) 
,acc_nbr 	VARCHAR(20) 
,USER_TYPE 	VARCHAR(20)  
,ARPU 	FLOAT
,ARPU_INCREASE_PP 	FLOAT
,DOU 	FLOAT
,DOU_INCREASE_PP 	FLOAT
,TERMN_PRICE 	FLOAT
,OTC_TIME 	FLOAT
,CHENMO_FLAG  	FLOAT
,G4_FLAG 	FLOAT
,BILL_FLAG 	FLOAT
,USER_ONLINE_DURA  	FLOAT
,ZHUFUKA_FLAG 	FLOAT
,FLOW_PACKET_NUM 	FLOAT
,YZF_CSM_FLAG 	FLOAT
,TYSX_ACT_FLAG 	FLOAT
,HUANGO_ACT_FLAG 	FLOAT
,MARKET_NUM 	FLOAT
,JWQ_NUM 	FLOAT
,JWQ_TELE_PP 	FLOAT
,MESS_FLAG 	FLOAT
,APP_NUMS 	FLOAT
,APP_VISIT_10_NUMS 	FLOAT
,APP_5M_NUMS   	FLOAT
,APP_ACT_5D_NUMS 	FLOAT
,YJDK_FLAG 	NUMBER(4,0) 
);
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.op_time  is  '处理日期'                            ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.user_id  is  '用户id'                                ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.acc_nbr  is  '手机号码'                               ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.USER_TYPE  is  '用户类型'                            ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.ARPU  is  '总账收入'                                 ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.ARPU_INCREASE_PP  is  '总账收入增幅'                 ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.DOU  is  '流量'                                      ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.DOU_INCREASE_PP  is  '流量增幅'                      ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.TERMN_PRICE  is  '终端价格'                          ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.OTC_TIME  is  '终端机龄'                             ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.CHENMO_FLAG   is  '是否沉默用户'                     ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.G4_FLAG  is  '是否4G用户'                            ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.BILL_FLAG  is  '是否出账'                            ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.USER_ONLINE_DURA   is  '在网时长'                            ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.ZHUFUKA_FLAG  is  '是否有副卡'                       ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.FLOW_PACKET_NUM  is  '订购流量包数量'                ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.YZF_CSM_FLAG  is  '翼支付是否消费'                   ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.TYSX_ACT_FLAG  is  '天翼视讯活跃'                    ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.HUANGO_ACT_FLAG  is  '欢GO活跃'                      ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.MARKET_NUM  is  '营销活动参与数量'                   ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.JWQ_NUM  is  '交往圈个数'                            ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.JWQ_TELE_PP  is  '交往圈中电信号码占比'              ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.MESS_FLAG  is  '是否收到过银行/支付宝/微信行短'      ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.APP_NUMS  is  'APP使用个数'                          ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.APP_VISIT_10_NUMS  is  '访问次数大于10次的APP个数'   ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.APP_5M_NUMS    is  '使用流量大于5M的APP个数'         ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.APP_ACT_5D_NUMS  is  '活跃天数大于5的APP个数'        ;
comment on column $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD.YJDK_FLAG  is  '是否机卡异常'                        ;


DELETE FROM	$db_name.TB_MCUST_VALUE_CAL_MON_AF_STD WHERE op_time=$TX_MONTH;

alter session force parallel dml

INSERT /*+append parallel(t,4) nologging*/ INTO	$db_name.TB_MCUST_VALUE_CAL_MON_AF_STD
SELECT	op_time 	
	,user_id 	
	,acc_nbr 	
	,USER_TYPE 	
	,CASE WHEN ARPU>=30  AND ARPU<50 	THEN 0.5000  + 0.1333 * (CAST(ARPU	AS FLOAT) - 30)  / (50 - 30)
	      WHEN ARPU>=50  AND ARPU<100	THEN 0.6333  + 0.2000  * (CAST(ARPU	AS FLOAT) - 50)  / (100 - 50)
	      WHEN ARPU>=100 AND ARPU<=150	THEN 0.8333  + 0.1667 * (CAST(ARPU	AS FLOAT) - 100) / (150 - 100)
	 END AS ARPU
  ,decode((MAX_ARPU_INCREASE_PP	- MIN_ARPU_INCREASE_PP),0,0.5,0.5+0.5 * ((CAST(ARPU_INCREASE_PP AS FLOAT)	- MIN_ARPU_INCREASE_PP  ) / (MAX_ARPU_INCREASE_PP	- MIN_ARPU_INCREASE_PP  ))) AS ARPU_INCREASE_PP      
	,CASE WHEN DOU>=0    AND DOU<500 	THEN 0.5000 + 0.1333 * (CAST(DOU	AS FLOAT) - 0)    / (500 - 0)
	      WHEN DOU>=500  AND DOU<1000	THEN 0.6333 + 0.1000 * (CAST(DOU	AS FLOAT) - 500)  / (1000 - 500)
	      WHEN DOU>=1000 AND DOU<=3000	THEN 0.7333 + 0.2667 * (CAST(DOU	AS FLOAT) - 1000) / (3000 - 1000)
	 END AS DOU
	,decode((MAX_DOU_INCREASE_PP 	- MIN_DOU_INCREASE_PP),0,0.5,0.5 + 0.5 * ((CAST(DOU_INCREASE_PP 	AS FLOAT)	- MIN_DOU_INCREASE_PP   ) / (MAX_DOU_INCREASE_PP 	- MIN_DOU_INCREASE_PP))) AS DOU_INCREASE_PP
	,CASE WHEN TERMN_PRICE < 1000	THEN 0.5
	      WHEN TERMN_PRICE < 2500	THEN 0.75
	      ELSE			     1.0
	END AS TERMN_PRICE
	,CASE WHEN OTC_TIME < 12	THEN 1.0
	      WHEN OTC_TIME < 24	THEN 0.8333
	      WHEN OTC_TIME < 36	THEN 0.6667
	      ELSE			     0.5
	END AS OTC_TIME
	,decode((MIN_CHENMO_FLAG  	- MAX_CHENMO_FLAG  	),0,0.5,0.5 + 0.5 * ((CAST(CHENMO_FLAG  	AS FLOAT)	- MAX_CHENMO_FLAG  	) / (MIN_CHENMO_FLAG  	- MAX_CHENMO_FLAG  	))) AS CHENMO_FLAG  		--负向指标
	,decode((MAX_G4_FLAG 	- MIN_G4_FLAG 	),0,0.5,0.5 + 0.5 * ((CAST(G4_FLAG 	AS FLOAT)	- MIN_G4_FLAG 	) / (MAX_G4_FLAG 	- MIN_G4_FLAG 	))) AS G4_FLAG
	,decode((MAX_BILL_FLAG 	- MIN_BILL_FLAG 	),0,0.5,0.5 + 0.5 * ((CAST(BILL_FLAG 	AS FLOAT)	- MIN_BILL_FLAG 	) / (MAX_BILL_FLAG 	- MIN_BILL_FLAG 	))	) AS BILL_FLAG 	 
	,CASE WHEN USER_ONLINE_DURA < 12	THEN 0.5
	      WHEN USER_ONLINE_DURA < 24	THEN 0.6667
	      WHEN USER_ONLINE_DURA < 36	THEN 0.8333
	      ELSE			     1.0
	END AS USER_ONLINE_DURA
	,decode((MAX_ZHUFUKA_FLAG 	- MIN_ZHUFUKA_FLAG 	),0,0,0.0 + 1.0 * ((CAST(ZHUFUKA_FLAG 	AS FLOAT)	- MIN_ZHUFUKA_FLAG 	) / (MAX_ZHUFUKA_FLAG 	- MIN_ZHUFUKA_FLAG 	))) AS ZHUFUKA_FLAG 
  ,decode( (MAX_FLOW_PACKET_NUM 	- MIN_FLOW_PACKET_NUM   ),0,0.5,0.5 + 0.5 * ((CAST(FLOW_PACKET_NUM 	AS FLOAT)	- MIN_FLOW_PACKET_NUM   ) / (MAX_FLOW_PACKET_NUM 	- MIN_FLOW_PACKET_NUM   ))) AS FLOW_PACKET_NUM 
	,decode((MAX_YZF_CSM_FLAG 	- MIN_YZF_CSM_FLAG 	),0,0.5,0.5 + 0.5 * ((CAST(YZF_CSM_FLAG 	AS FLOAT)	- MIN_YZF_CSM_FLAG 	) / (MAX_YZF_CSM_FLAG 	- MIN_YZF_CSM_FLAG 	))	 	) AS YZF_CSM_FLAG 		
	,decode((MAX_TYSX_ACT_FLAG 	- MIN_TYSX_ACT_FLAG 	),0,0.5,0.5 + 0.5 * ((CAST(TYSX_ACT_FLAG 	AS FLOAT)	- MIN_TYSX_ACT_FLAG 	) / (MAX_TYSX_ACT_FLAG 	- MIN_TYSX_ACT_FLAG 	))	) AS TYSX_ACT_FLAG
	,decode((MAX_HUANGO_ACT_FLAG 	- MIN_HUANGO_ACT_FLAG   ),0,0.5,0.5 + 0.5 * ((CAST(HUANGO_ACT_FLAG 	AS FLOAT)	- MIN_HUANGO_ACT_FLAG   ) / (MAX_HUANGO_ACT_FLAG 	- MIN_HUANGO_ACT_FLAG   ))) AS HUANGO_ACT_FLAG
	,decode((MAX_MARKET_NUM 	- MIN_MARKET_NUM 	),0,0.5,0.5 + 0.5 * ((CAST(MARKET_NUM 	AS FLOAT)	- MIN_MARKET_NUM 	) / (MAX_MARKET_NUM 	- MIN_MARKET_NUM 	))) AS MARKET_NUM 	 	
	,decode((MAX_JWQ_NUM 	- MIN_JWQ_NUM 	),0,0.5,0.5 + 0.5 * ((CAST(JWQ_NUM 	AS FLOAT)	- MIN_JWQ_NUM 	) / (MAX_JWQ_NUM 	- MIN_JWQ_NUM 	))) AS JWQ_NUM 	 		
	,decode((MAX_JWQ_TELE_PP 	- MIN_JWQ_TELE_PP 	),0,0.5,0.5 + 0.5 * ((CAST(JWQ_TELE_PP 	AS FLOAT)	- MIN_JWQ_TELE_PP 	) / (MAX_JWQ_TELE_PP 	- MIN_JWQ_TELE_PP 	))) AS JWQ_TELE_PP 	 	            
	,decode((MAX_MESS_FLAG 	- MIN_MESS_FLAG 	),0,0.5,0.5 + 0.5 * ((CAST(MESS_FLAG 	AS FLOAT)	- MIN_MESS_FLAG 	) / (MAX_MESS_FLAG 	- MIN_MESS_FLAG 	))) AS MESS_FLAG
	,decode((MAX_APP_NUMS 	- MIN_APP_NUMS 	),0,0.5,0.5 + 0.5 * ((CAST(APP_NUMS 	AS FLOAT)	- MIN_APP_NUMS 	) / (MAX_APP_NUMS 	- MIN_APP_NUMS 	))) AS APP_NUMS
	,decode((MAX_APP_VISIT_10_NUM	- MIN_APP_VISIT_10_NUM  ),0,0.5,0.5 + 0.5 * ((CAST(APP_VISIT_10_NUMS AS FLOAT)	- MIN_APP_VISIT_10_NUM  ) / (MAX_APP_VISIT_10_NUM	- MIN_APP_VISIT_10_NUM  ))) AS APP_VISIT_10_NUMS 	
	,decode((MAX_APP_5M_NUMS   		  - MIN_APP_5M_NUMS   	),0,0.5,0.5 + 0.5 * ((CAST(APP_5M_NUMS   	AS FLOAT)	- MIN_APP_5M_NUMS   	) / (MAX_APP_5M_NUMS   		  - MIN_APP_5M_NUMS   	))) AS APP_5M_NUMS
	,decode((MAX_APP_ACT_5D_NUMS 	- MIN_APP_ACT_5D_NUMS   ),0,0.5,0.5 + 0.5 * ((CAST(APP_ACT_5D_NUMS 	AS FLOAT)	- MIN_APP_ACT_5D_NUMS   ) /(MAX_APP_ACT_5D_NUMS 	- MIN_APP_ACT_5D_NUMS   ) )	) AS APP_ACT_5D_NUMS     		
	,YJDK_FLAG	 	
FROM 	$db_name.TB_MCUST_VALUE_CAL_MON_AF	A
,(
SELECT	 MAX(ARPU 		) AS MAX_ARPU 	 
	,MAX(ARPU_INCREASE_PP   ) AS MAX_ARPU_INCREASE_PP
	,MAX(DOU 		) AS MAX_DOU 		
	,MAX(DOU_INCREASE_PP 	) AS MAX_DOU_INCREASE_PP 
	,MAX(TERMN_PRICE 	) AS MAX_TERMN_PRICE 	
	,MAX(OTC_TIME 	) AS MAX_OTC_TIME 	 
	,MAX(CHENMO_FLAG  	) AS MAX_CHENMO_FLAG  	
	,MAX(G4_FLAG 	) AS MAX_G4_FLAG 	 
	,MAX(BILL_FLAG 	) AS MAX_BILL_FLAG 	 
	,MAX(USER_ONLINE_DURA  	) AS MAX_USER_ONLINE_DURA  	 
	,MAX(ZHUFUKA_FLAG 	) AS MAX_ZHUFUKA_FLAG 	
	,MAX(FLOW_PACKET_NUM 	) AS MAX_FLOW_PACKET_NUM 	
	,MAX(YZF_CSM_FLAG 	) AS MAX_YZF_CSM_FLAG 	
	,MAX(TYSX_ACT_FLAG 	) AS MAX_TYSX_ACT_FLAG 	
	,MAX(HUANGO_ACT_FLAG 	) AS MAX_HUANGO_ACT_FLAG 
	,MAX(MARKET_NUM 	) AS MAX_MARKET_NUM 	
	,MAX(JWQ_NUM 	) AS MAX_JWQ_NUM 	 
	,MAX(JWQ_TELE_PP 	) AS MAX_JWQ_TELE_PP 	
	,MAX(MESS_FLAG 	) AS MAX_MESS_FLAG 	 
	,MAX(APP_NUMS 	) AS MAX_APP_NUMS 	 
	,MAX(APP_VISIT_10_NUMS  ) AS MAX_APP_VISIT_10_NUM
	,MAX(APP_5M_NUMS   	) AS MAX_APP_5M_NUMS   	
	,MAX(APP_ACT_5D_NUMS 	) AS MAX_APP_ACT_5D_NUMS 
	,MAX(YJDK_FLAG 	) AS MAX_YJDK_FLAG 	 
	,MIN(ARPU 		) AS MIN_ARPU 	 
	,MIN(ARPU_INCREASE_PP   ) AS MIN_ARPU_INCREASE_PP
	,MIN(DOU 		) AS MIN_DOU 	
	,MIN(DOU_INCREASE_PP 	) AS MIN_DOU_INCREASE_PP 
	,MIN(TERMN_PRICE 	) AS MIN_TERMN_PRICE 	
	,MIN(OTC_TIME 	) AS MIN_OTC_TIME 	 
	,MIN(CHENMO_FLAG  	) AS MIN_CHENMO_FLAG  	
	,MIN(G4_FLAG 	) AS MIN_G4_FLAG 	 
	,MIN(BILL_FLAG 	) AS MIN_BILL_FLAG 	 
	,MIN(USER_ONLINE_DURA  	) AS MIN_USER_ONLINE_DURA  	 
	,MIN(ZHUFUKA_FLAG 	) AS MIN_ZHUFUKA_FLAG 	
	,MIN(FLOW_PACKET_NUM 	) AS MIN_FLOW_PACKET_NUM  	
	,MIN(YZF_CSM_FLAG 	) AS MIN_YZF_CSM_FLAG 	
	,MIN(TYSX_ACT_FLAG 	) AS MIN_TYSX_ACT_FLAG 	
	,MIN(HUANGO_ACT_FLAG 	) AS MIN_HUANGO_ACT_FLAG 
	,MIN(MARKET_NUM 	) AS MIN_MARKET_NUM 	
	,MIN(JWQ_NUM 	) AS MIN_JWQ_NUM 	 
	,MIN(JWQ_TELE_PP 	) AS MIN_JWQ_TELE_PP 	
	,MIN(MESS_FLAG 	) AS MIN_MESS_FLAG 	 
	,MIN(APP_NUMS 	) AS MIN_APP_NUMS 	 
	,MIN(APP_VISIT_10_NUMS  ) AS MIN_APP_VISIT_10_NUM
	,MIN(APP_5M_NUMS   	) AS MIN_APP_5M_NUMS   	
	,MIN(APP_ACT_5D_NUMS 	) AS MIN_APP_ACT_5D_NUMS 
	,MIN(YJDK_FLAG 	) AS MIN_YJDK_FLAG 	 	
FROM	$db_name.TB_MCUST_VALUE_CAL_MON_AF
WHERE	op_time=$TX_MONTH
)				B
WHERE	op_time=$TX_MONTH
;

alter session disable parallel dml

select 27,sysdate from dual;

---------------执行熵计算公式---------------------------
---------预付费
drop table $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD1;
CREATE  TABLE $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD1 AS(	
select
  OP_TIME
 ,USER_ID
 ,cast((case when ARPU               =0 then 0.0001 else  ARPU              end) as number(10,4))     ARPU             
 ,cast((case when ARPU_INCREASE_PP   =0 then 0.0001 else  ARPU_INCREASE_PP  end) as number(10,4))     ARPU_INCREASE_PP 
 ,cast((case when MOU                =0 then 0.0001 else  MOU               end) as number(10,4))     MOU              
 ,cast((case when MOU_INCREASE_PP    =0 then 0.0001 else  MOU_INCREASE_PP   end) as number(10,4))     MOU_INCREASE_PP  
 ,cast((case when DOU                =0 then 0.0001 else  DOU               end) as number(10,4))     DOU              
 ,cast((case when DOU_INCREASE_PP    =0 then 0.0001 else  DOU_INCREASE_PP   end) as number(10,4))     DOU_INCREASE_PP  
 ,cast((case when TERMN_PRICE        =0 then 0.0001 else  TERMN_PRICE       end) as number(10,4))     TERMN_PRICE   
 ,cast((case when OTC_TIME           =0 then 0.0001 else  OTC_TIME          end) as number(10,4))     OTC_TIME    
 ,cast((case when CHENMO_FLAG        =0 then 2      else  CHENMO_FLAG       end) as number(10,4))     CHENMO_FLAG      
 ,cast((case when G4_FLAG            =0 then 2      else  G4_FLAG           end) as number(10,4))     G4_FLAG          
 ,cast((case when QIANFEI_FLAG       =0 then 2      else  QIANFEI_FLAG      end) as number(10,4))     QIANFEI_FLAG     
 ,cast((case when DOU_STOP_FLAG      =0 then 2      else  DOU_STOP_FLAG     end) as number(10,4))     DOU_STOP_FLAG    
 ,cast((case when DOU_STOP_NUM       =0 then 0.0001 else  DOU_STOP_NUM      end) as number(10,4))     DOU_STOP_NUM     
 ,cast((case when USER_ONLINE_DURA           =0 then 0.0001 else  USER_ONLINE_DURA          end) as number(10,4))     USER_ONLINE_DURA         
 ,cast((case when ZHUFUKA_FLAG       =0 then 2      else  ZHUFUKA_FLAG      end) as number(10,4))     ZHUFUKA_FLAG     
 ,cast((case when FLOW_PACKET_NUM    =0 then 0.0001 else  FLOW_PACKET_NUM   end) as number(10,4))     FLOW_PACKET_NUM  
 ,cast((case when YZF_ACT_FLAG       =0 then 2      else  YZF_ACT_FLAG      end) as number(10,4))     YZF_ACT_FLAG     
 ,cast((case when YZF_CSM_FLAG       =0 then 2      else  YZF_CSM_FLAG      end) as number(10,4))     YZF_CSM_FLAG     
 ,cast((case when TYSX_ACT_FLAG      =0 then 2      else  TYSX_ACT_FLAG     end) as number(10,4))     TYSX_ACT_FLAG    
 ,cast((case when HUANGO_ACT_FLAG    =0 then 2      else  HUANGO_ACT_FLAG   end) as number(10,4))     HUANGO_ACT_FLAG  
 ,cast((case when MARKET_NUM         =0 then 0.0001 else  MARKET_NUM        end) as number(10,4))     MARKET_NUM       
 ,cast((case when JWQ_NUM            =0 then 0.0001 else  JWQ_NUM           end) as number(10,4))     JWQ_NUM          
 ,cast((case when JWQ_TELE_PP        =0 then 0.0001 else  JWQ_TELE_PP       end) as number(10,4))     JWQ_TELE_PP  
 ,cast((case when MESS_FLAG          =0 then 2      else  MESS_FLAG         end) as number(10,4))     MESS_FLAG    
 ,cast((case when APP_NUMS           =0 then 0.0001 else  APP_NUMS          end) as number(10,4))     APP_NUMS         
 ,cast((case when APP_VISIT_10_NUMS  =0 then 0.0001 else  APP_VISIT_10_NUMS end) as number(10,4))     APP_VISIT_10_NUMS
 ,cast((case when APP_5M_NUMS        =0 then 0.0001 else  APP_5M_NUMS       end) as number(10,4))     APP_5M_NUMS      
 ,cast((case when APP_ACT_5D_NUMS    =0 then 0.0001 else  APP_ACT_5D_NUMS   end) as number(10,4))     APP_ACT_5D_NUMS  
 ,cast((case when YJDK_FLAG          =0 then 0.0001 else  YJDK_FLAG         end) as number(10,4))     YJDK_FLAG    
from $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD
where OP_TIME=$TX_MONTH
);

select 28,sysdate from dual;

drop table $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD2;
CREATE  TABLE $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD2 AS(	
select
  OP_TIME
 ,USER_ID
 ,cast((case when ARPU               =0 then 0.0001 else  ARPU              end) as number(10,4))     ARPU             
 ,cast((case when ARPU_INCREASE_PP   =0 then 0.0001 else  ARPU_INCREASE_PP  end) as number(10,4))     ARPU_INCREASE_PP 
 ,cast((case when MOU                =0 then 0.0001 else  MOU               end) as number(10,4))     MOU              
 ,cast((case when MOU_INCREASE_PP    =0 then 0.0001 else  MOU_INCREASE_PP   end) as number(10,4))     MOU_INCREASE_PP  
 ,cast((case when DOU                =0 then 0.0001 else  DOU               end) as number(10,4))     DOU              
 ,cast((case when DOU_INCREASE_PP    =0 then 0.0001 else  DOU_INCREASE_PP   end) as number(10,4))     DOU_INCREASE_PP  
 ,cast((case when TERMN_PRICE        =0 then 0.0001 else  TERMN_PRICE       end) as number(10,4))     TERMN_PRICE  
 ,cast((case when OTC_TIME           =0 then 0.0001 else  OTC_TIME          end) as number(10,4))     OTC_TIME     
 ,cast((case when CHENMO_FLAG        =0 then 2      else  CHENMO_FLAG       end) as number(10,4))     CHENMO_FLAG      
 ,cast((case when G4_FLAG            =0 then 2      else  G4_FLAG           end) as number(10,4))     G4_FLAG          
 ,cast((case when QIANFEI_FLAG       =0 then 2      else  QIANFEI_FLAG      end) as number(10,4))     QIANFEI_FLAG     
 ,cast((case when DOU_STOP_FLAG      =0 then 2      else  DOU_STOP_FLAG     end) as number(10,4))     DOU_STOP_FLAG    
 ,cast((case when DOU_STOP_NUM       =0 then 0.0001 else  DOU_STOP_NUM      end) as number(10,4))     DOU_STOP_NUM     
 ,cast((case when USER_ONLINE_DURA           =0 then 0.0001 else  USER_ONLINE_DURA          end) as number(10,4))     USER_ONLINE_DURA         
 ,cast((case when ZHUFUKA_FLAG       =0 then 2      else  ZHUFUKA_FLAG      end) as number(10,4))     ZHUFUKA_FLAG     
 ,cast((case when FLOW_PACKET_NUM    =0 then 0.0001 else  FLOW_PACKET_NUM   end) as number(10,4))     FLOW_PACKET_NUM  
 ,cast((case when YZF_ACT_FLAG       =0 then 2      else  YZF_ACT_FLAG      end) as number(10,4))     YZF_ACT_FLAG     
 ,cast((case when YZF_CSM_FLAG       =0 then 2      else  YZF_CSM_FLAG      end) as number(10,4))     YZF_CSM_FLAG     
 ,cast((case when TYSX_ACT_FLAG      =0 then 2      else  TYSX_ACT_FLAG     end) as number(10,4))     TYSX_ACT_FLAG    
 ,cast((case when HUANGO_ACT_FLAG    =0 then 2      else  HUANGO_ACT_FLAG   end) as number(10,4))     HUANGO_ACT_FLAG  
 ,cast((case when MARKET_NUM         =0 then 0.0001 else  MARKET_NUM        end) as number(10,4))     MARKET_NUM       
 ,cast((case when JWQ_NUM            =0 then 0.0001 else  JWQ_NUM           end) as number(10,4))     JWQ_NUM          
 ,cast((case when JWQ_TELE_PP        =0 then 0.0001 else  JWQ_TELE_PP       end) as number(10,4))     JWQ_TELE_PP
 ,cast((case when MESS_FLAG          =0 then 2      else  MESS_FLAG         end) as number(10,4))     MESS_FLAG      
 ,cast((case when APP_NUMS           =0 then 0.0001 else  APP_NUMS          end) as number(10,4))     APP_NUMS         
 ,cast((case when APP_VISIT_10_NUMS  =0 then 0.0001 else  APP_VISIT_10_NUMS end) as number(10,4))     APP_VISIT_10_NUMS
 ,cast((case when APP_5M_NUMS        =0 then 0.0001 else  APP_5M_NUMS       end) as number(10,4))     APP_5M_NUMS      
 ,cast((case when APP_ACT_5D_NUMS    =0 then 0.0001 else  APP_ACT_5D_NUMS   end) as number(10,4))     APP_ACT_5D_NUMS  
 ,cast((case when YJDK_FLAG          =0 then 0.0001 else  YJDK_FLAG         end) as number(10,4))     YJDK_FLAG      
from $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD1
where OP_TIME=$TX_MONTH
);

select 29,sysdate from dual;

drop table $db_name.ndxs_temp_shang_sum_yu;
CREATE  TABLE $db_name.ndxs_temp_shang_sum_yu AS(	
select
  OP_TIME
 ,sum(ARPU               )      S_ARPU               
 ,sum(ARPU_INCREASE_PP   )      S_ARPU_INCREASE_PP   
 ,sum(MOU                )      S_MOU                
 ,sum(MOU_INCREASE_PP    )      S_MOU_INCREASE_PP    
 ,sum(DOU                )      S_DOU                
 ,sum(DOU_INCREASE_PP    )      S_DOU_INCREASE_PP    
 ,sum(TERMN_PRICE        )      S_TERMN_PRICE    
 ,sum(OTC_TIME           )      S_OTC_TIME  
 ,sum(CHENMO_FLAG        )      S_CHENMO_FLAG        
 ,sum(G4_FLAG            )      S_G4_FLAG            
 ,sum(QIANFEI_FLAG       )      S_QIANFEI_FLAG       
 ,sum(DOU_STOP_FLAG      )      S_DOU_STOP_FLAG      
 ,sum(DOU_STOP_NUM       )      S_DOU_STOP_NUM       
 ,sum(USER_ONLINE_DURA           )      S_USER_ONLINE_DURA           
 ,sum(ZHUFUKA_FLAG       )      S_ZHUFUKA_FLAG       
 ,sum(FLOW_PACKET_NUM    )      S_FLOW_PACKET_NUM    
 ,sum(YZF_ACT_FLAG       )      S_YZF_ACT_FLAG       
 ,sum(YZF_CSM_FLAG       )      S_YZF_CSM_FLAG       
 ,sum(TYSX_ACT_FLAG      )      S_TYSX_ACT_FLAG      
 ,sum(HUANGO_ACT_FLAG    )      S_HUANGO_ACT_FLAG    
 ,sum(MARKET_NUM         )      S_MARKET_NUM         
 ,sum(JWQ_NUM            )      S_JWQ_NUM            
 ,sum(JWQ_TELE_PP        )      S_JWQ_TELE_PP   
 ,sum(MESS_FLAG          )      S_MESS_FLAG      
 ,sum(APP_NUMS           )      S_APP_NUMS           
 ,sum(APP_VISIT_10_NUMS  )      S_APP_VISIT_10_NUMS  
 ,sum(APP_5M_NUMS        )      S_APP_5M_NUMS        
 ,sum(APP_ACT_5D_NUMS    )      S_APP_ACT_5D_NUMS    
 ,sum(YJDK_FLAG          )      S_YJDK_FLAG          
 ,count(distinct USER_ID)       CUST_NUM      
from $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD2
where OP_TIME=$TX_MONTH
group by OP_TIME
);

select 30,sysdate from dual;

drop table  $db_name.ndxs_temp_shang_user_yu;
CREATE  TABLE $db_name.ndxs_temp_shang_user_yu AS(	
select
  t1.OP_TIME
 ,USER_ID
 ,cast(ARPU               *1.00000000/S_ARPU                 as number(18,16))    A_ARPU               
 ,cast(ARPU_INCREASE_PP   *1.00000000/S_ARPU_INCREASE_PP     as number(18,16))    A_ARPU_INCREASE_PP   
 ,cast(MOU                *1.00000000/S_MOU                  as number(18,16))    A_MOU                
 ,cast(MOU_INCREASE_PP    *1.00000000/S_MOU_INCREASE_PP      as number(18,16))    A_MOU_INCREASE_PP    
 ,cast(DOU                *1.00000000/S_DOU                  as number(18,16))    A_DOU                
 ,cast(DOU_INCREASE_PP    *1.00000000/S_DOU_INCREASE_PP      as number(18,16))    A_DOU_INCREASE_PP    
 ,cast(TERMN_PRICE        *1.00000000/S_TERMN_PRICE          as number(18,16))    A_TERMN_PRICE    
 ,cast(OTC_TIME           *1.00000000/S_OTC_TIME             as number(18,16))    A_OTC_TIME     
 ,cast(CHENMO_FLAG        *1.00000000/S_CHENMO_FLAG          as number(18,16))    A_CHENMO_FLAG        
 ,cast(G4_FLAG            *1.00000000/S_G4_FLAG              as number(18,16))    A_G4_FLAG            
 ,cast(QIANFEI_FLAG       *1.00000000/S_QIANFEI_FLAG         as number(18,16))    A_QIANFEI_FLAG       
 ,cast(DOU_STOP_FLAG      *1.00000000/S_DOU_STOP_FLAG        as number(18,16))    A_DOU_STOP_FLAG      
 ,cast(DOU_STOP_NUM       *1.00000000/S_DOU_STOP_NUM         as number(18,16))    A_DOU_STOP_NUM       
 ,cast(USER_ONLINE_DURA           *1.00000000/S_USER_ONLINE_DURA             as number(18,16))    A_USER_ONLINE_DURA           
 ,cast(ZHUFUKA_FLAG       *1.00000000/S_ZHUFUKA_FLAG         as number(18,16))    A_ZHUFUKA_FLAG       
 ,cast(FLOW_PACKET_NUM    *1.00000000/S_FLOW_PACKET_NUM      as number(18,16))    A_FLOW_PACKET_NUM    
 ,cast(YZF_ACT_FLAG       *1.00000000/S_YZF_ACT_FLAG         as number(18,16))    A_YZF_ACT_FLAG       
 ,cast(YZF_CSM_FLAG       *1.00000000/S_YZF_CSM_FLAG         as number(18,16))    A_YZF_CSM_FLAG       
 ,cast(TYSX_ACT_FLAG      *1.00000000/S_TYSX_ACT_FLAG        as number(18,16))    A_TYSX_ACT_FLAG      
 ,cast(HUANGO_ACT_FLAG    *1.00000000/S_HUANGO_ACT_FLAG      as number(18,16))    A_HUANGO_ACT_FLAG    
 ,cast(MARKET_NUM         *1.00000000/S_MARKET_NUM           as number(18,16))    A_MARKET_NUM         
 ,cast(JWQ_NUM            *1.00000000/S_JWQ_NUM              as number(18,16))    A_JWQ_NUM            
 ,cast(JWQ_TELE_PP        *1.00000000/S_JWQ_TELE_PP          as number(18,16))    A_JWQ_TELE_PP  
 ,cast(MESS_FLAG          *1.00000000/S_MESS_FLAG            as number(18,16))    A_MESS_FLAG       
 ,cast(APP_NUMS           *1.00000000/S_APP_NUMS             as number(18,16))    A_APP_NUMS           
 ,cast(APP_VISIT_10_NUMS  *1.00000000/S_APP_VISIT_10_NUMS    as number(18,16))    A_APP_VISIT_10_NUMS  
 ,cast(APP_5M_NUMS        *1.00000000/S_APP_5M_NUMS          as number(18,16))    A_APP_5M_NUMS        
 ,cast(APP_ACT_5D_NUMS    *1.00000000/S_APP_ACT_5D_NUMS      as number(18,16))    A_APP_ACT_5D_NUMS    
 ,cast(YJDK_FLAG          *1.00000000/S_YJDK_FLAG            as number(18,16))    A_YJDK_FLAG          
from $db_name.TB_MCUST_VALUE_CAL_MON_BF_STD2  t1
left join $db_name.ndxs_temp_shang_sum_yu t2
on t1.OP_TIME=t2.OP_TIME
where t1.OP_TIME=$TX_MONTH
);

drop table $db_name.ndxs_temp_shang_sum2_yu;
CREATE  TABLE $db_name.ndxs_temp_shang_sum2_yu AS(	
select
 t1.OP_TIME
,SUM(cast(B_ARPU              *K_NUM as number(16,10)))     R_ARPU                
,SUM(cast(B_ARPU_INCREASE_PP  *K_NUM as number(16,10)))     R_ARPU_INCREASE_PP    
,SUM(cast(B_MOU               *K_NUM as number(16,10)))     R_MOU                 
,SUM(cast(B_MOU_INCREASE_PP   *K_NUM as number(16,10)))     R_MOU_INCREASE_PP     
,SUM(cast(B_DOU               *K_NUM as number(16,10)))     R_DOU                 
,SUM(cast(B_DOU_INCREASE_PP   *K_NUM as number(16,10)))     R_DOU_INCREASE_PP     
,SUM(cast(B_TERMN_PRICE       *K_NUM as number(16,10)))     R_TERMN_PRICE 
,SUM(cast(B_OTC_TIME          *K_NUM as number(16,10)))     R_OTC_TIME      
,SUM(cast(B_CHENMO_FLAG       *K_NUM as number(16,10)))     R_CHENMO_FLAG         
,SUM(cast(B_G4_FLAG           *K_NUM as number(16,10)))     R_G4_FLAG             
,SUM(cast(B_QIANFEI_FLAG      *K_NUM as number(16,10)))     R_QIANFEI_FLAG        
,SUM(cast(B_DOU_STOP_FLAG     *K_NUM as number(16,10)))     R_DOU_STOP_FLAG       
,SUM(cast(B_DOU_STOP_NUM      *K_NUM as number(16,10)))     R_DOU_STOP_NUM        
,SUM(cast(B_USER_ONLINE_DURA          *K_NUM as number(16,10)))     R_USER_ONLINE_DURA            
,SUM(cast(B_ZHUFUKA_FLAG      *K_NUM as number(16,10)))     R_ZHUFUKA_FLAG        
,SUM(cast(B_FLOW_PACKET_NUM   *K_NUM as number(16,10)))     R_FLOW_PACKET_NUM    
,SUM(cast(B_YZF_ACT_FLAG      *K_NUM as number(16,10)))     R_YZF_ACT_FLAG        
,SUM(cast(B_YZF_CSM_FLAG      *K_NUM as number(16,10)))     R_YZF_CSM_FLAG        
,SUM(cast(B_TYSX_ACT_FLAG     *K_NUM as number(16,10)))     R_TYSX_ACT_FLAG       
,SUM(cast(B_HUANGO_ACT_FLAG   *K_NUM as number(16,10)))     R_HUANGO_ACT_FLAG     
,SUM(cast(B_MARKET_NUM        *K_NUM as number(16,10)))     R_MARKET_NUM          
,SUM(cast(B_JWQ_NUM           *K_NUM as number(16,10)))     R_JWQ_NUM             
,SUM(cast(B_JWQ_TELE_PP       *K_NUM as number(16,10)))     R_JWQ_TELE_PP 
,SUM(cast(B_MESS_FLAG         *K_NUM as number(16,10)))     R_MESS_FLAG        
,SUM(cast(B_APP_NUMS          *K_NUM as number(16,10)))     R_APP_NUMS           
,SUM(cast(B_APP_VISIT_10_NUMS *K_NUM as number(16,10)))     R_APP_VISIT_10_NUMS  
,SUM(cast(B_APP_5M_NUMS       *K_NUM as number(16,10)))     R_APP_5M_NUMS       
,SUM(cast(B_APP_ACT_5D_NUMS   *K_NUM as number(16,10)))     R_APP_ACT_5D_NUMS   
,SUM(cast(B_YJDK_FLAG         *K_NUM as number(16,10)))     R_YJDK_FLAG         
from
(select
  t1.OP_TIME
 ,USER_ID
 ,(A_ARPU              )*(cast(LN(A_ARPU              ) as number(24,16)))   B_ARPU              
 ,(A_ARPU_INCREASE_PP  )*(cast(LN(A_ARPU_INCREASE_PP  ) as number(24,16)))   B_ARPU_INCREASE_PP  
 ,(A_MOU               )*(cast(LN(A_MOU               ) as number(24,16)))   B_MOU               
 ,(A_MOU_INCREASE_PP   )*(cast(LN(A_MOU_INCREASE_PP   ) as number(24,16)))   B_MOU_INCREASE_PP   
 ,(A_DOU               )*(cast(LN(A_DOU               ) as number(24,16)))   B_DOU               
 ,(A_DOU_INCREASE_PP   )*(cast(LN(A_DOU_INCREASE_PP   ) as number(24,16)))   B_DOU_INCREASE_PP   
 ,(A_TERMN_PRICE       )*(cast(LN(A_TERMN_PRICE       ) as number(24,16)))   B_TERMN_PRICE  
 ,(A_OTC_TIME          )*(cast(LN(A_OTC_TIME          ) as number(24,16)))   B_OTC_TIME      
 ,(A_CHENMO_FLAG       )*(cast(LN(A_CHENMO_FLAG       ) as number(24,16)))   B_CHENMO_FLAG       
 ,(A_G4_FLAG           )*(cast(LN(A_G4_FLAG           ) as number(24,16)))   B_G4_FLAG           
 ,(A_QIANFEI_FLAG      )*(cast(LN(A_QIANFEI_FLAG      ) as number(24,16)))   B_QIANFEI_FLAG      
 ,(A_DOU_STOP_FLAG     )*(cast(LN(A_DOU_STOP_FLAG     ) as number(24,16)))   B_DOU_STOP_FLAG     
 ,(A_DOU_STOP_NUM      )*(cast(LN(A_DOU_STOP_NUM      ) as number(24,16)))   B_DOU_STOP_NUM      
 ,(A_USER_ONLINE_DURA          )*(cast(LN(A_USER_ONLINE_DURA          ) as number(24,16)))   B_USER_ONLINE_DURA          
 ,(A_ZHUFUKA_FLAG      )*(cast(LN(A_ZHUFUKA_FLAG      ) as number(24,16)))   B_ZHUFUKA_FLAG      
 ,(A_FLOW_PACKET_NUM   )*(cast(LN(A_FLOW_PACKET_NUM   ) as number(24,16)))   B_FLOW_PACKET_NUM   
 ,(A_YZF_ACT_FLAG      )*(cast(LN(A_YZF_ACT_FLAG      ) as number(24,16)))   B_YZF_ACT_FLAG      
 ,(A_YZF_CSM_FLAG      )*(cast(LN(A_YZF_CSM_FLAG      ) as number(24,16)))   B_YZF_CSM_FLAG      
 ,(A_TYSX_ACT_FLAG     )*(cast(LN(A_TYSX_ACT_FLAG     ) as number(24,16)))   B_TYSX_ACT_FLAG     
 ,(A_HUANGO_ACT_FLAG   )*(cast(LN(A_HUANGO_ACT_FLAG   ) as number(24,16)))   B_HUANGO_ACT_FLAG   
 ,(A_MARKET_NUM        )*(cast(LN(A_MARKET_NUM        ) as number(24,16)))   B_MARKET_NUM        
 ,(A_JWQ_NUM           )*(cast(LN(A_JWQ_NUM           ) as number(24,16)))   B_JWQ_NUM           
 ,(A_JWQ_TELE_PP       )*(cast(LN(A_JWQ_TELE_PP       ) as number(24,16)))   B_JWQ_TELE_PP 
 ,(A_MESS_FLAG         )*(cast(LN(A_MESS_FLAG         ) as number(24,16)))   B_MESS_FLAG       
 ,(A_APP_NUMS          )*(cast(LN(A_APP_NUMS          ) as number(24,16)))   B_APP_NUMS          
 ,(A_APP_VISIT_10_NUMS )*(cast(LN(A_APP_VISIT_10_NUMS ) as number(24,10)))   B_APP_VISIT_10_NUMS 
 ,(A_APP_5M_NUMS       )*(cast(LN(A_APP_5M_NUMS       ) as number(24,16)))   B_APP_5M_NUMS       
 ,(A_APP_ACT_5D_NUMS   )*(cast(LN(A_APP_ACT_5D_NUMS   ) as number(24,16)))   B_APP_ACT_5D_NUMS   
 ,(A_YJDK_FLAG         )*(cast(LN(A_YJDK_FLAG         ) as number(24,10)))   B_YJDK_FLAG         
from $db_name.ndxs_temp_shang_user_yu  T1
where OP_TIME=$TX_MONTH
)t1
left join (
           select 
           op_time,
           -1/LN(cust_num) K_NUM
           from $db_name.ndxs_temp_shang_sum_yu 
           group by op_time,-1/LN(cust_num)
           )t2 
on t1.op_time=t2.op_time
group by t1.OP_TIME);

select 31,sysdate from dual;

---------后付费
drop table $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD1;
CREATE  TABLE $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD1 AS(	
select
  OP_TIME
 ,USER_ID
 ,cast((case when ARPU               =0 then 0.0001 else  ARPU              end) as number(10,4))     ARPU             
 ,cast((case when ARPU_INCREASE_PP   =0 then 0.0001 else  ARPU_INCREASE_PP  end) as number(10,4))     ARPU_INCREASE_PP 
 ,cast((case when MOU                =0 then 0.0001 else  MOU               end) as number(10,4))     MOU              
 ,cast((case when MOU_INCREASE_PP    =0 then 0.0001 else  MOU_INCREASE_PP   end) as number(10,4))     MOU_INCREASE_PP  
 ,cast((case when DOU                =0 then 0.0001 else  DOU               end) as number(10,4))     DOU              
 ,cast((case when DOU_INCREASE_PP    =0 then 0.0001 else  DOU_INCREASE_PP   end) as number(10,4))     DOU_INCREASE_PP  
 ,cast((case when TERMN_PRICE        =0 then 0.0001 else  TERMN_PRICE       end) as number(10,4))     TERMN_PRICE   
 ,cast((case when OTC_TIME           =0 then 0.0001 else  OTC_TIME          end) as number(10,4))     OTC_TIME    
 ,cast((case when CHENMO_FLAG        =0 then 2      else  CHENMO_FLAG       end) as number(10,4))     CHENMO_FLAG      
 ,cast((case when G4_FLAG            =0 then 2      else  G4_FLAG           end) as number(10,4))     G4_FLAG          
 ,cast((case when QIANFEI_FLAG       =0 then 2      else  QIANFEI_FLAG      end) as number(10,4))     QIANFEI_FLAG     
 ,cast((case when DOU_STOP_FLAG      =0 then 2      else  DOU_STOP_FLAG     end) as number(10,4))     DOU_STOP_FLAG    
 ,cast((case when DOU_STOP_NUM       =0 then 0.0001 else  DOU_STOP_NUM      end) as number(10,4))     DOU_STOP_NUM     
 ,cast((case when USER_ONLINE_DURA           =0 then 0.0001 else  USER_ONLINE_DURA          end) as number(10,4))     USER_ONLINE_DURA         
 ,cast((case when ZHUFUKA_FLAG       =0 then 2      else  ZHUFUKA_FLAG      end) as number(10,4))     ZHUFUKA_FLAG     
 ,cast((case when FLOW_PACKET_NUM    =0 then 0.0001 else  FLOW_PACKET_NUM   end) as number(10,4))     FLOW_PACKET_NUM  
 ,cast((case when YZF_ACT_FLAG       =0 then 2      else  YZF_ACT_FLAG      end) as number(10,4))     YZF_ACT_FLAG     
 ,cast((case when YZF_CSM_FLAG       =0 then 2      else  YZF_CSM_FLAG      end) as number(10,4))     YZF_CSM_FLAG     
 ,cast((case when TYSX_ACT_FLAG      =0 then 2      else  TYSX_ACT_FLAG     end) as number(10,4))     TYSX_ACT_FLAG    
 ,cast((case when HUANGO_ACT_FLAG    =0 then 2      else  HUANGO_ACT_FLAG   end) as number(10,4))     HUANGO_ACT_FLAG  
 ,cast((case when MARKET_NUM         =0 then 0.0001 else  MARKET_NUM        end) as number(10,4))     MARKET_NUM       
 ,cast((case when JWQ_NUM            =0 then 0.0001 else  JWQ_NUM           end) as number(10,4))     JWQ_NUM          
 ,cast((case when JWQ_TELE_PP        =0 then 0.0001 else  JWQ_TELE_PP       end) as number(10,4))     JWQ_TELE_PP  
 ,cast((case when MESS_FLAG          =0 then 2      else  MESS_FLAG         end) as number(10,4))     MESS_FLAG    
 ,cast((case when APP_NUMS           =0 then 0.0001 else  APP_NUMS          end) as number(10,4))     APP_NUMS         
 ,cast((case when APP_VISIT_10_NUMS  =0 then 0.0001 else  APP_VISIT_10_NUMS end) as number(10,4))     APP_VISIT_10_NUMS
 ,cast((case when APP_5M_NUMS        =0 then 0.0001 else  APP_5M_NUMS       end) as number(10,4))     APP_5M_NUMS      
 ,cast((case when APP_ACT_5D_NUMS    =0 then 0.0001 else  APP_ACT_5D_NUMS   end) as number(10,4))     APP_ACT_5D_NUMS  
 ,cast((case when YJDK_FLAG          =0 then 0.0001 else  YJDK_FLAG         end) as number(10,4))     YJDK_FLAG         
from $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD
where OP_TIME=$TX_MONTH
);

drop table $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD2;
CREATE  TABLE $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD2 AS(	
select
  OP_TIME
 ,USER_ID
 ,cast((case when ARPU               =0 then 0.0001 else  ARPU              end) as number(10,4))     ARPU             
 ,cast((case when ARPU_INCREASE_PP   =0 then 0.0001 else  ARPU_INCREASE_PP  end) as number(10,4))     ARPU_INCREASE_PP 
 ,cast((case when MOU                =0 then 0.0001 else  MOU               end) as number(10,4))     MOU              
 ,cast((case when MOU_INCREASE_PP    =0 then 0.0001 else  MOU_INCREASE_PP   end) as number(10,4))     MOU_INCREASE_PP  
 ,cast((case when DOU                =0 then 0.0001 else  DOU               end) as number(10,4))     DOU              
 ,cast((case when DOU_INCREASE_PP    =0 then 0.0001 else  DOU_INCREASE_PP   end) as number(10,4))     DOU_INCREASE_PP  
 ,cast((case when TERMN_PRICE        =0 then 0.0001 else  TERMN_PRICE       end) as number(10,4))     TERMN_PRICE   
 ,cast((case when OTC_TIME           =0 then 0.0001 else  OTC_TIME          end) as number(10,4))     OTC_TIME    
 ,cast((case when CHENMO_FLAG        =0 then 2      else  CHENMO_FLAG       end) as number(10,4))     CHENMO_FLAG      
 ,cast((case when G4_FLAG            =0 then 2      else  G4_FLAG           end) as number(10,4))     G4_FLAG          
 ,cast((case when QIANFEI_FLAG       =0 then 2      else  QIANFEI_FLAG      end) as number(10,4))     QIANFEI_FLAG     
 ,cast((case when DOU_STOP_FLAG      =0 then 2      else  DOU_STOP_FLAG     end) as number(10,4))     DOU_STOP_FLAG    
 ,cast((case when DOU_STOP_NUM       =0 then 0.0001 else  DOU_STOP_NUM      end) as number(10,4))     DOU_STOP_NUM     
 ,cast((case when USER_ONLINE_DURA           =0 then 0.0001 else  USER_ONLINE_DURA          end) as number(10,4))     USER_ONLINE_DURA         
 ,cast((case when ZHUFUKA_FLAG       =0 then 2      else  ZHUFUKA_FLAG      end) as number(10,4))     ZHUFUKA_FLAG     
 ,cast((case when FLOW_PACKET_NUM    =0 then 0.0001 else  FLOW_PACKET_NUM   end) as number(10,4))     FLOW_PACKET_NUM  
 ,cast((case when YZF_ACT_FLAG       =0 then 2      else  YZF_ACT_FLAG      end) as number(10,4))     YZF_ACT_FLAG     
 ,cast((case when YZF_CSM_FLAG       =0 then 2      else  YZF_CSM_FLAG      end) as number(10,4))     YZF_CSM_FLAG     
 ,cast((case when TYSX_ACT_FLAG      =0 then 2      else  TYSX_ACT_FLAG     end) as number(10,4))     TYSX_ACT_FLAG    
 ,cast((case when HUANGO_ACT_FLAG    =0 then 2      else  HUANGO_ACT_FLAG   end) as number(10,4))     HUANGO_ACT_FLAG  
 ,cast((case when MARKET_NUM         =0 then 0.0001 else  MARKET_NUM        end) as number(10,4))     MARKET_NUM       
 ,cast((case when JWQ_NUM            =0 then 0.0001 else  JWQ_NUM           end) as number(10,4))     JWQ_NUM          
 ,cast((case when JWQ_TELE_PP        =0 then 0.0001 else  JWQ_TELE_PP       end) as number(10,4))     JWQ_TELE_PP  
 ,cast((case when MESS_FLAG          =0 then 2      else  MESS_FLAG         end) as number(10,4))     MESS_FLAG    
 ,cast((case when APP_NUMS           =0 then 0.0001 else  APP_NUMS          end) as number(10,4))     APP_NUMS         
 ,cast((case when APP_VISIT_10_NUMS  =0 then 0.0001 else  APP_VISIT_10_NUMS end) as number(10,4))     APP_VISIT_10_NUMS
 ,cast((case when APP_5M_NUMS        =0 then 0.0001 else  APP_5M_NUMS       end) as number(10,4))     APP_5M_NUMS      
 ,cast((case when APP_ACT_5D_NUMS    =0 then 0.0001 else  APP_ACT_5D_NUMS   end) as number(10,4))     APP_ACT_5D_NUMS  
 ,cast((case when YJDK_FLAG          =0 then 0.0001 else  YJDK_FLAG         end) as number(10,4))     YJDK_FLAG    
from $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD1
where OP_TIME=$TX_MONTH
);

select 32,sysdate from dual;

drop table $db_name.ndxs_temp_shang_sum;
CREATE  TABLE $db_name.ndxs_temp_shang_sum AS(	
select
  OP_TIME
 ,sum(ARPU               )      S_ARPU               
 ,sum(ARPU_INCREASE_PP   )      S_ARPU_INCREASE_PP   
 ,sum(MOU                )      S_MOU                
 ,sum(MOU_INCREASE_PP    )      S_MOU_INCREASE_PP    
 ,sum(DOU                )      S_DOU                
 ,sum(DOU_INCREASE_PP    )      S_DOU_INCREASE_PP    
 ,sum(TERMN_PRICE        )      S_TERMN_PRICE    
 ,sum(OTC_TIME           )      S_OTC_TIME  
 ,sum(CHENMO_FLAG        )      S_CHENMO_FLAG        
 ,sum(G4_FLAG            )      S_G4_FLAG            
 ,sum(QIANFEI_FLAG       )      S_QIANFEI_FLAG       
 ,sum(DOU_STOP_FLAG      )      S_DOU_STOP_FLAG      
 ,sum(DOU_STOP_NUM       )      S_DOU_STOP_NUM       
 ,sum(USER_ONLINE_DURA           )      S_USER_ONLINE_DURA           
 ,sum(ZHUFUKA_FLAG       )      S_ZHUFUKA_FLAG       
 ,sum(FLOW_PACKET_NUM    )      S_FLOW_PACKET_NUM    
 ,sum(YZF_ACT_FLAG       )      S_YZF_ACT_FLAG       
 ,sum(YZF_CSM_FLAG       )      S_YZF_CSM_FLAG       
 ,sum(TYSX_ACT_FLAG      )      S_TYSX_ACT_FLAG      
 ,sum(HUANGO_ACT_FLAG    )      S_HUANGO_ACT_FLAG    
 ,sum(MARKET_NUM         )      S_MARKET_NUM         
 ,sum(JWQ_NUM            )      S_JWQ_NUM            
 ,sum(JWQ_TELE_PP        )      S_JWQ_TELE_PP   
 ,sum(MESS_FLAG          )      S_MESS_FLAG      
 ,sum(APP_NUMS           )      S_APP_NUMS           
 ,sum(APP_VISIT_10_NUMS  )      S_APP_VISIT_10_NUMS  
 ,sum(APP_5M_NUMS        )      S_APP_5M_NUMS        
 ,sum(APP_ACT_5D_NUMS    )      S_APP_ACT_5D_NUMS    
 ,sum(YJDK_FLAG          )      S_YJDK_FLAG          
 ,count(distinct USER_ID)       CUST_NUM   
from $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD2
where OP_TIME=$TX_MONTH
group by OP_TIME
);

drop table  $db_name.ndxs_temp_shang_user;
CREATE  TABLE $db_name.ndxs_temp_shang_user AS(	
select
  t1.OP_TIME
 ,USER_ID
 ,cast(ARPU               *1.00000000/S_ARPU                 as number(18,16))    A_ARPU               
 ,cast(ARPU_INCREASE_PP   *1.00000000/S_ARPU_INCREASE_PP     as number(18,16))    A_ARPU_INCREASE_PP   
 ,cast(MOU                *1.00000000/S_MOU                  as number(18,16))    A_MOU                
 ,cast(MOU_INCREASE_PP    *1.00000000/S_MOU_INCREASE_PP      as number(18,16))    A_MOU_INCREASE_PP    
 ,cast(DOU                *1.00000000/S_DOU                  as number(18,16))    A_DOU                
 ,cast(DOU_INCREASE_PP    *1.00000000/S_DOU_INCREASE_PP      as number(18,16))    A_DOU_INCREASE_PP    
 ,cast(TERMN_PRICE        *1.00000000/S_TERMN_PRICE          as number(18,16))    A_TERMN_PRICE    
 ,cast(OTC_TIME           *1.00000000/S_OTC_TIME             as number(18,16))    A_OTC_TIME     
 ,cast(CHENMO_FLAG        *1.00000000/S_CHENMO_FLAG          as number(18,16))    A_CHENMO_FLAG        
 ,cast(G4_FLAG            *1.00000000/S_G4_FLAG              as number(18,16))    A_G4_FLAG            
 ,cast(QIANFEI_FLAG       *1.00000000/S_QIANFEI_FLAG         as number(18,16))    A_QIANFEI_FLAG       
 ,cast(DOU_STOP_FLAG      *1.00000000/S_DOU_STOP_FLAG        as number(18,16))    A_DOU_STOP_FLAG      
 ,cast(DOU_STOP_NUM       *1.00000000/S_DOU_STOP_NUM         as number(18,16))    A_DOU_STOP_NUM       
 ,cast(USER_ONLINE_DURA           *1.00000000/S_USER_ONLINE_DURA             as number(18,16))    A_USER_ONLINE_DURA           
 ,cast(ZHUFUKA_FLAG       *1.00000000/S_ZHUFUKA_FLAG         as number(18,16))    A_ZHUFUKA_FLAG       
 ,cast(FLOW_PACKET_NUM    *1.00000000/S_FLOW_PACKET_NUM      as number(18,16))    A_FLOW_PACKET_NUM    
 ,cast(YZF_ACT_FLAG       *1.00000000/S_YZF_ACT_FLAG         as number(18,16))    A_YZF_ACT_FLAG       
 ,cast(YZF_CSM_FLAG       *1.00000000/S_YZF_CSM_FLAG         as number(18,16))    A_YZF_CSM_FLAG       
 ,cast(TYSX_ACT_FLAG      *1.00000000/S_TYSX_ACT_FLAG        as number(18,16))    A_TYSX_ACT_FLAG      
 ,cast(HUANGO_ACT_FLAG    *1.00000000/S_HUANGO_ACT_FLAG      as number(18,16))    A_HUANGO_ACT_FLAG    
 ,cast(MARKET_NUM         *1.00000000/S_MARKET_NUM           as number(18,16))    A_MARKET_NUM         
 ,cast(JWQ_NUM            *1.00000000/S_JWQ_NUM              as number(18,16))    A_JWQ_NUM            
 ,cast(JWQ_TELE_PP        *1.00000000/S_JWQ_TELE_PP          as number(18,16))    A_JWQ_TELE_PP  
 ,cast(MESS_FLAG          *1.00000000/S_MESS_FLAG            as number(18,16))    A_MESS_FLAG       
 ,cast(APP_NUMS           *1.00000000/S_APP_NUMS             as number(18,16))    A_APP_NUMS           
 ,cast(APP_VISIT_10_NUMS  *1.00000000/S_APP_VISIT_10_NUMS    as number(18,16))    A_APP_VISIT_10_NUMS  
 ,cast(APP_5M_NUMS        *1.00000000/S_APP_5M_NUMS          as number(18,16))    A_APP_5M_NUMS        
 ,cast(APP_ACT_5D_NUMS    *1.00000000/S_APP_ACT_5D_NUMS      as number(18,16))    A_APP_ACT_5D_NUMS    
 ,cast(YJDK_FLAG          *1.00000000/S_YJDK_FLAG            as number(18,16))    A_YJDK_FLAG        
from $db_name.TB_MCUST_VALUE_CAL_MON_AF_STD2  t1
left join $db_name.ndxs_temp_shang_sum t2
on t1.OP_TIME=t2.OP_TIME
where t1.OP_TIME=$TX_MONTH
);

select 33,sysdate from dual;

drop table $db_name.ndxs_temp_shang_sum2;
CREATE  TABLE $db_name.ndxs_temp_shang_sum2 AS(	
select
 t1.OP_TIME
,SUM(cast(B_ARPU              *K_NUM as number(16,10)))     R_ARPU                
,SUM(cast(B_ARPU_INCREASE_PP  *K_NUM as number(16,10)))     R_ARPU_INCREASE_PP    
,SUM(cast(B_MOU               *K_NUM as number(16,10)))     R_MOU                 
,SUM(cast(B_MOU_INCREASE_PP   *K_NUM as number(16,10)))     R_MOU_INCREASE_PP     
,SUM(cast(B_DOU               *K_NUM as number(16,10)))     R_DOU                 
,SUM(cast(B_DOU_INCREASE_PP   *K_NUM as number(16,10)))     R_DOU_INCREASE_PP     
,SUM(cast(B_TERMN_PRICE       *K_NUM as number(16,10)))     R_TERMN_PRICE 
,SUM(cast(B_OTC_TIME          *K_NUM as number(16,10)))     R_OTC_TIME      
,SUM(cast(B_CHENMO_FLAG       *K_NUM as number(16,10)))     R_CHENMO_FLAG         
,SUM(cast(B_G4_FLAG           *K_NUM as number(16,10)))     R_G4_FLAG             
,SUM(cast(B_QIANFEI_FLAG      *K_NUM as number(16,10)))     R_QIANFEI_FLAG        
,SUM(cast(B_DOU_STOP_FLAG     *K_NUM as number(16,10)))     R_DOU_STOP_FLAG       
,SUM(cast(B_DOU_STOP_NUM      *K_NUM as number(16,10)))     R_DOU_STOP_NUM        
,SUM(cast(B_USER_ONLINE_DURA          *K_NUM as number(16,10)))     R_USER_ONLINE_DURA            
,SUM(cast(B_ZHUFUKA_FLAG      *K_NUM as number(16,10)))     R_ZHUFUKA_FLAG        
,SUM(cast(B_FLOW_PACKET_NUM   *K_NUM as number(16,10)))     R_FLOW_PACKET_NUM    
,SUM(cast(B_YZF_ACT_FLAG      *K_NUM as number(16,10)))     R_YZF_ACT_FLAG        
,SUM(cast(B_YZF_CSM_FLAG      *K_NUM as number(16,10)))     R_YZF_CSM_FLAG        
,SUM(cast(B_TYSX_ACT_FLAG     *K_NUM as number(16,10)))     R_TYSX_ACT_FLAG       
,SUM(cast(B_HUANGO_ACT_FLAG   *K_NUM as number(16,10)))     R_HUANGO_ACT_FLAG     
,SUM(cast(B_MARKET_NUM        *K_NUM as number(16,10)))     R_MARKET_NUM          
,SUM(cast(B_JWQ_NUM           *K_NUM as number(16,10)))     R_JWQ_NUM             
,SUM(cast(B_JWQ_TELE_PP       *K_NUM as number(16,10)))     R_JWQ_TELE_PP 
,SUM(cast(B_MESS_FLAG         *K_NUM as number(16,10)))     R_MESS_FLAG        
,SUM(cast(B_APP_NUMS          *K_NUM as number(16,10)))     R_APP_NUMS           
,SUM(cast(B_APP_VISIT_10_NUMS *K_NUM as number(16,10)))     R_APP_VISIT_10_NUMS  
,SUM(cast(B_APP_5M_NUMS       *K_NUM as number(16,10)))     R_APP_5M_NUMS       
,SUM(cast(B_APP_ACT_5D_NUMS   *K_NUM as number(16,10)))     R_APP_ACT_5D_NUMS   
,SUM(cast(B_YJDK_FLAG         *K_NUM as number(16,10)))     R_YJDK_FLAG           
from
(select
  t1.OP_TIME
 ,USER_ID
 ,(A_ARPU              )*(cast(LN(A_ARPU              ) as number(24,16)))   B_ARPU              
 ,(A_ARPU_INCREASE_PP  )*(cast(LN(A_ARPU_INCREASE_PP  ) as number(24,16)))   B_ARPU_INCREASE_PP  
 ,(A_MOU               )*(cast(LN(A_MOU               ) as number(24,16)))   B_MOU               
 ,(A_MOU_INCREASE_PP   )*(cast(LN(A_MOU_INCREASE_PP   ) as number(24,16)))   B_MOU_INCREASE_PP   
 ,(A_DOU               )*(cast(LN(A_DOU               ) as number(24,16)))   B_DOU               
 ,(A_DOU_INCREASE_PP   )*(cast(LN(A_DOU_INCREASE_PP   ) as number(24,16)))   B_DOU_INCREASE_PP   
 ,(A_TERMN_PRICE       )*(cast(LN(A_TERMN_PRICE       ) as number(24,16)))   B_TERMN_PRICE  
 ,(A_OTC_TIME          )*(cast(LN(A_OTC_TIME          ) as number(24,16)))   B_OTC_TIME      
 ,(A_CHENMO_FLAG       )*(cast(LN(A_CHENMO_FLAG       ) as number(24,16)))   B_CHENMO_FLAG       
 ,(A_G4_FLAG           )*(cast(LN(A_G4_FLAG           ) as number(24,16)))   B_G4_FLAG           
 ,(A_QIANFEI_FLAG      )*(cast(LN(A_QIANFEI_FLAG      ) as number(24,16)))   B_QIANFEI_FLAG      
 ,(A_DOU_STOP_FLAG     )*(cast(LN(A_DOU_STOP_FLAG     ) as number(24,16)))   B_DOU_STOP_FLAG     
 ,(A_DOU_STOP_NUM      )*(cast(LN(A_DOU_STOP_NUM      ) as number(24,16)))   B_DOU_STOP_NUM      
 ,(A_USER_ONLINE_DURA          )*(cast(LN(A_USER_ONLINE_DURA          ) as number(24,16)))   B_USER_ONLINE_DURA          
 ,(A_ZHUFUKA_FLAG      )*(cast(LN(A_ZHUFUKA_FLAG      ) as number(24,16)))   B_ZHUFUKA_FLAG      
 ,(A_FLOW_PACKET_NUM   )*(cast(LN(A_FLOW_PACKET_NUM   ) as number(24,16)))   B_FLOW_PACKET_NUM   
 ,(A_YZF_ACT_FLAG      )*(cast(LN(A_YZF_ACT_FLAG      ) as number(24,16)))   B_YZF_ACT_FLAG      
 ,(A_YZF_CSM_FLAG      )*(cast(LN(A_YZF_CSM_FLAG      ) as number(24,16)))   B_YZF_CSM_FLAG      
 ,(A_TYSX_ACT_FLAG     )*(cast(LN(A_TYSX_ACT_FLAG     ) as number(24,16)))   B_TYSX_ACT_FLAG     
 ,(A_HUANGO_ACT_FLAG   )*(cast(LN(A_HUANGO_ACT_FLAG   ) as number(24,16)))   B_HUANGO_ACT_FLAG   
 ,(A_MARKET_NUM        )*(cast(LN(A_MARKET_NUM        ) as number(24,16)))   B_MARKET_NUM        
 ,(A_JWQ_NUM           )*(cast(LN(A_JWQ_NUM           ) as number(24,16)))   B_JWQ_NUM           
 ,(A_JWQ_TELE_PP       )*(cast(LN(A_JWQ_TELE_PP       ) as number(24,16)))   B_JWQ_TELE_PP 
 ,(A_MESS_FLAG         )*(cast(LN(A_MESS_FLAG         ) as number(24,16)))   B_MESS_FLAG       
 ,(A_APP_NUMS          )*(cast(LN(A_APP_NUMS          ) as number(24,16)))   B_APP_NUMS          
 ,(A_APP_VISIT_10_NUMS )*(cast(LN(A_APP_VISIT_10_NUMS ) as number(24,10)))   B_APP_VISIT_10_NUMS 
 ,(A_APP_5M_NUMS       )*(cast(LN(A_APP_5M_NUMS       ) as number(24,16)))   B_APP_5M_NUMS       
 ,(A_APP_ACT_5D_NUMS   )*(cast(LN(A_APP_ACT_5D_NUMS   ) as number(24,16)))   B_APP_ACT_5D_NUMS   
 ,(A_YJDK_FLAG         )*(cast(LN(A_YJDK_FLAG         ) as number(24,10)))   B_YJDK_FLAG          
from $db_name.ndxs_temp_shang_user  T1
where OP_TIME=$TX_MONTH
)t1
left join (
         select 
           op_time,
           -1/LN(cust_num) K_NUM
           from $db_name.ndxs_temp_shang_sum_yu 
           group by op_time,-1/LN(cust_num)
           )t2
on t1.op_time=t2.op_time
group by t1.op_time);

select 34,sysdate from dual;

----4、移动公众客户价值指数结果表-基于零壹标准化
----4.1、预付费用户【0.5~1】
DROP TABLE	$db_name.TB_MCUST_VALUE_RES_MON_BF;
CREATE TABLE $db_name.TB_MCUST_VALUE_RES_MON_BF
(
op_time	INTEGER 
,user_id	NUMBER(16,0) 
,acc_nbr 	VARCHAR(20) 
,USER_TYPE 	VARCHAR(20)  
,SELL_DEPT_NAME	VARCHAR(20)
,ARPU 	NUMBER(10,6) 
,ARPU_INCREASE_PP 	NUMBER(10,6) 
,DOU 	NUMBER(10,6) 
,DOU_INCREASE_PP 	NUMBER(10,6) 
,TERMN_PRICE 	NUMBER(10,6) 
,OTC_TIME 	NUMBER(10,6) 
,CHENMO_FLAG  	NUMBER(10,6) 
,G4_FLAG 	NUMBER(10,6) 
,BILL_FLAG	NUMBER(10,6)
,USER_ONLINE_DURA  	NUMBER(10,6) 
,ZHUFUKA_FLAG 	NUMBER(10,6) 
,FLOW_PACKET_NUM 	NUMBER(10,6) 
,YZF_CSM_FLAG 	NUMBER(10,6) 
,TYSX_ACT_FLAG 	NUMBER(10,6) 
,HUANGO_ACT_FLAG 	NUMBER(10,6) 
,MARKET_NUM 	NUMBER(10,6) 
,JWQ_NUM 	NUMBER(10,6) 
,JWQ_TELE_PP 	NUMBER(10,6) 
,MESS_FLAG 	NUMBER(10,6) 
,APP_NUMS 	NUMBER(10,6) 
,APP_VISIT_10_NUMS 	NUMBER(10,6) 
,APP_5M_NUMS   	NUMBER(10,6) 
,APP_ACT_5D_NUMS 	NUMBER(10,6) 
,YJDK_FLAG 	NUMBER(10,6) 
,XIANXING_ZHIJIE 	NUMBER(10,6) 
,XIANXING_YEWU 	NUMBER(10,6) 
,XIANXING_ZHONGDUAN 	NUMBER(10,6) 
,YINXING_YONGHU 	NUMBER(10,6) 
,YINXING_QIYE 	NUMBER(10,6) 
,YINXING_SHEJIAO 	NUMBER(10,6) 
,YINXING_DPI 	NUMBER(10,6) 
,XIANXING 	NUMBER(10,6) 
,YINXING 	NUMBER(10,6) 
,JIAZHI 	NUMBER(10,6) 
);
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.op_time  is  '处理日期'                              ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.user_id  is  '用户id'                                  ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.acc_nbr  is  '手机号码'                                 ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.USER_TYPE  is  '用户类型'                              ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.SELL_DEPT_NAME is  '销售部门'                          ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.ARPU  is  '总账收入'                                   ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.ARPU_INCREASE_PP  is  '总账收入增幅'                   ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.DOU  is  '流量'                                        ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.DOU_INCREASE_PP  is  '流量增幅'                        ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.TERMN_PRICE  is  '终端价格'                            ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.OTC_TIME  is  '终端机龄'                               ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.CHENMO_FLAG   is  '是否沉默用户'                       ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.G4_FLAG  is  '是否4G用户'                              ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.BILL_FLAG is  '是否出账'                               ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.USER_ONLINE_DURA   is  '在网时长'                      ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.ZHUFUKA_FLAG  is  '是否有副卡'                         ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.FLOW_PACKET_NUM  is  '订购流量包数量'                  ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.YZF_CSM_FLAG  is  '翼支付是否消费'                     ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.TYSX_ACT_FLAG  is  '天翼视讯活跃'                      ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.HUANGO_ACT_FLAG  is  '欢GO活跃'                        ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.MARKET_NUM  is  '营销活动参与数量'                     ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.JWQ_NUM  is  '交往圈个数'                              ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.JWQ_TELE_PP  is  '交往圈中电信号码占比'                ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.MESS_FLAG  is  '是否收到过银行/支付宝/微信行短'        ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.APP_NUMS  is  'APP使用个数'                            ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.APP_VISIT_10_NUMS  is  '访问次数大于10次的APP个数'     ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.APP_5M_NUMS    is  '使用流量大于5M的APP个数'           ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.APP_ACT_5D_NUMS  is  '活跃天数大于5的APP个数'          ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.YJDK_FLAG  is  '是否机卡异常'                          ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.XIANXING_ZHIJIE  is  '显性-直接收入指数'               ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.XIANXING_YEWU  is  '显性-业务价值指数'                 ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.XIANXING_ZHONGDUAN  is  '显性-终端价值指数'            ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.YINXING_YONGHU  is  '隐性-用户属性指数'                ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.YINXING_QIYE  is  '隐性-企业接触指数'                  ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.YINXING_SHEJIAO  is  '隐性-社交广度指数'               ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.YINXING_DPI  is  '隐性-DPI指数'                        ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.XIANXING  is  '显性价值指数'                           ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.YINXING  is  '隐性价值指数'                            ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_BF.JIAZHI  is  '价值指数'                                 ;

--------导入预付费价值评估结果表

delete from $db_name.TB_MCUST_VALUE_RES_MON_BF;

alter session force parallel dml

insert /*+append parallel(t,4) nologging*/ into $db_name.TB_MCUST_VALUE_RES_MON_BF
SELECT
	 T1.op_time 
	,T1.user_id 
	,T1.acc_nbr 
	,T1.USER_TYPE 
	,T2.DEPT_NAME
	,ARPU             	* 50 	XIANXING_ZHIJIE1
	,ARPU_INCREASE_PP 	* 5  	XIANXING_ZHIJIE2
	,DOU              	* 20 	XIANXING_YEWU1
	,DOU_INCREASE_PP  	* 5  	XIANXING_YEWU2
	,TERMN_PRICE      	* 2  	XIANXING_ZHONGDUAN1 
	,OTC_TIME         	* 3  	XIANXING_ZHONGDUAN2 
	,CHENMO_FLAG     	* 0.5	YINXING_YONGHU1
	,G4_FLAG          	* 1  	YINXING_YONGHU2 
	,BILL_FLAG    	* 0.5	YINXING_YONGHU3 
	,USER_ONLINE_DURA         	* 3  	YINXING_YONGHU4
	,ZHUFUKA_FLAG     	* 0  	YINXING_QIYE1
	,FLOW_PACKET_NUM  	* 1  	YINXING_QIYE2
	,YZF_CSM_FLAG     	* 2  	YINXING_QIYE3
	,TYSX_ACT_FLAG    	* 0.5	YINXING_QIYE4
	,HUANGO_ACT_FLAG  	* 0.5	YINXING_QIYE5
	,MARKET_NUM       	* 2  	YINXING_QIYE6
	,JWQ_NUM          	* 1  	YINXING_SHEJIAO1
	,JWQ_TELE_PP      	* 0.5	YINXING_SHEJIAO2
	,MESS_FLAG              * 0.5	YINXING_SHEJIAO3
	,APP_NUMS         	* 0.5	YINXING_DPI1
	,APP_VISIT_10_NUMS	* 0.5	YINXING_DPI2
	,APP_5M_NUMS      	* 0.5	YINXING_DPI3
	,APP_ACT_5D_NUMS  	* 0.5	YINXING_DPI4
	,YJDK_FLAG       	* (-5) 	YINXING_QITA
	,(ARPU             	* 50) 	+ (ARPU_INCREASE_PP 	* 5)	XIANXING_ZHIJIE
	,(DOU              	* 20 )	+ (DOU_INCREASE_PP  	* 5)	XIANXING_YEWU
	,(TERMN_PRICE      	* 2)	+ (OTC_TIME         	* 3)	XIANXING_ZHONGDUAN
	,(CHENMO_FLAG     	* 0.5)	+ (G4_FLAG          	* 1)	+ (BILL_FLAG    	* 0.5)	+ (USER_ONLINE_DURA         	* 3)	YINXING_YONGHU
	,(ZHUFUKA_FLAG     	* 0) 	+ (FLOW_PACKET_NUM  	* 1)	+ (YZF_CSM_FLAG     	* 2)	+ (TYSX_ACT_FLAG    	* 0.5)	+ (HUANGO_ACT_FLAG  	* 0.5)	+ (MARKET_NUM       	* 2)	YINXING_QIYE
	,(JWQ_NUM          	* 1)	+ (JWQ_TELE_PP      	* 0.5)	+ (MESS_FLAG              * 0.5	)	YINXING_SHEJIAO
	,(APP_NUMS         	* 0.5	)	+ (APP_VISIT_10_NUMS	* 0.5)	+ (APP_5M_NUMS      	* 0.5)	+ (APP_ACT_5D_NUMS  	* 0.5)	YINXING_DPI
	,((ARPU             	* 50) 	+ (ARPU_INCREASE_PP 	* 5))	+ ((DOU              	* 20 )	+ (DOU_INCREASE_PP  	* 5))	+ ((TERMN_PRICE      	* 2)	+ (OTC_TIME         	* 3))	XIANXING
	,((CHENMO_FLAG     	* 0.5)	+ (G4_FLAG          	* 1)	+ (BILL_FLAG    	* 0.5)	+ (USER_ONLINE_DURA         	* 3))	+ ((ZHUFUKA_FLAG     	* 0) 	+ (FLOW_PACKET_NUM  	* 1)	+ (YZF_CSM_FLAG     	* 2)	+ (TYSX_ACT_FLAG    	* 0.5)	+ (HUANGO_ACT_FLAG  	* 0.5)	+ (MARKET_NUM       	* 2))	+ ((JWQ_NUM          	* 1)	+ (JWQ_TELE_PP      	* 0.5)	+ (MESS_FLAG              * 0.5	))	+ ((APP_NUMS         	* 0.5	)	+ (APP_VISIT_10_NUMS	* 0.5)	+ (APP_5M_NUMS      	* 0.5)	+ (APP_ACT_5D_NUMS  	* 0.5))	YINXING
	,(((ARPU             	* 50) 	+ (ARPU_INCREASE_PP 	* 5))	+ ((DOU              	* 20 )	+ (DOU_INCREASE_PP  	* 5))	+ ((TERMN_PRICE      	* 2)	+ (OTC_TIME         	* 3))		+ 	((CHENMO_FLAG     	* 0.5)	+ (G4_FLAG          	* 1)	+ (BILL_FLAG    	* 0.5)	+ (USER_ONLINE_DURA         	* 3))	+ ((ZHUFUKA_FLAG     	* 0) 	+ (FLOW_PACKET_NUM  	* 1)	+ (YZF_CSM_FLAG     	* 2)	+ (TYSX_ACT_FLAG    	* 0.5)	+ (HUANGO_ACT_FLAG  	* 0.5)	+ (MARKET_NUM       	* 2))	+ ((JWQ_NUM          	* 1)	+ (JWQ_TELE_PP      	* 0.5)	+ (MESS_FLAG              * 0.5	))	+ ((APP_NUMS         	* 0.5	)	+ (APP_VISIT_10_NUMS	* 0.5)	+ (APP_5M_NUMS      	* 0.5)	+ (APP_ACT_5D_NUMS  	* 0.5))	+ (YJDK_FLAG       	* (-5)))	JIAZHI
FROM 	$db_name.TB_MCUST_VALUE_CAL_MON_BF_STD	T1
LEFT JOIN 	$db_name.TB_USER_SELL_DEPT_MON 	T2
ON	T1.op_time=T2.op_time AND T1.user_id=T2.user_id
where t1.op_time=$TX_MONTH;

alter session disable parallel dml

select 35,sysdate from dual;

----4.2、后付费用户【0.5~1】
DROP TABLE	$db_name.TB_MCUST_VALUE_RES_MON_AF;
CREATE TABLE $db_name.TB_MCUST_VALUE_RES_MON_AF
(
op_time 	INTEGER 
,user_id 	NUMBER(16,0) 
,acc_nbr 	VARCHAR(20) 
,USER_TYPE 	VARCHAR(20) 
,SELL_DEPT_NAME	VARCHAR(20)
,ARPU 	NUMBER(10,6) 
,ARPU_INCREASE_PP 	NUMBER(10,6) 
,DOU 	NUMBER(10,6) 
,DOU_INCREASE_PP 	NUMBER(10,6) 
,TERMN_PRICE 	NUMBER(10,6) 
,OTC_TIME 	NUMBER(10,6) 
,CHENMO_FLAG  	NUMBER(10,6) 
,G4_FLAG 	NUMBER(10,6) 
,BILL_FLAG	NUMBER(10,6)
,USER_ONLINE_DURA  	NUMBER(10,6) 
,ZHUFUKA_FLAG 	NUMBER(10,6) 
,FLOW_PACKET_NUM 	NUMBER(10,6) 
,YZF_CSM_FLAG 	NUMBER(10,6) 
,TYSX_ACT_FLAG 	NUMBER(10,6) 
,HUANGO_ACT_FLAG 	NUMBER(10,6) 
,MARKET_NUM 	NUMBER(10,6) 
,JWQ_NUM 	NUMBER(10,6) 
,JWQ_TELE_PP 	NUMBER(10,6) 
,MESS_FLAG 	NUMBER(10,6) 
,APP_NUMS 	NUMBER(10,6) 
,APP_VISIT_10_NUMS 	NUMBER(10,6) 
,APP_5M_NUMS   	NUMBER(10,6) 
,APP_ACT_5D_NUMS 	NUMBER(10,6) 
,YJDK_FLAG 	NUMBER(10,6) 
,XIANXING_ZHIJIE 	NUMBER(10,6) 
,XIANXING_YEWU 	NUMBER(10,6) 
,XIANXING_ZHONGDUAN 	NUMBER(10,6) 
,YINXING_YONGHU 	NUMBER(10,6) 
,YINXING_QIYE 	NUMBER(10,6) 
,YINXING_SHEJIAO 	NUMBER(10,6) 
,YINXING_DPI 	NUMBER(10,6) 
,XIANXING 	NUMBER(10,6) 
,YINXING 	NUMBER(10,6) 
,JIAZHI 	NUMBER(10,6) 
);
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.op_time  is  '处理日期'                             ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.user_id  is  '用户id'                                 ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.acc_nbr  is  '手机号码'                                ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.USER_TYPE  is  '用户类型'                             ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.SELL_DEPT_NAME is  '销售部门'                         ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.ARPU  is  '总账收入'                                  ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.ARPU_INCREASE_PP  is  '总账收入增幅'                  ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.DOU  is  '流量'                                       ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.DOU_INCREASE_PP  is  '流量增幅'                       ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.TERMN_PRICE  is  '终端价格'                           ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.OTC_TIME  is  '终端机龄'                              ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.CHENMO_FLAG   is  '是否沉默用户'                      ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.G4_FLAG  is  '是否4G用户'                             ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.BILL_FLAG is  '是否出账'                              ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.USER_ONLINE_DURA   is  '在网时长'                             ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.ZHUFUKA_FLAG  is  '是否有副卡'                        ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.FLOW_PACKET_NUM  is  '订购流量包数量'                 ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.YZF_CSM_FLAG  is  '翼支付是否消费'                    ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.TYSX_ACT_FLAG  is  '天翼视讯活跃'                     ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.HUANGO_ACT_FLAG  is  '欢GO活跃'                       ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.MARKET_NUM  is  '营销活动参与数量'                    ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.JWQ_NUM  is  '交往圈个数'                             ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.JWQ_TELE_PP  is  '交往圈中电信号码占比'               ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.MESS_FLAG  is  '是否收到过银行/支付宝/微信行短'       ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.APP_NUMS  is  'APP使用个数'                           ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.APP_VISIT_10_NUMS  is  '访问次数大于10次的APP个数'    ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.APP_5M_NUMS    is  '使用流量大于5M的APP个数'          ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.APP_ACT_5D_NUMS  is  '活跃天数大于5的APP个数'         ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.YJDK_FLAG  is  '是否机卡异常'                         ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.XIANXING_ZHIJIE  is  '显性-直接收入指数'              ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.XIANXING_YEWU  is  '显性-业务价值指数'                ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.XIANXING_ZHONGDUAN  is  '显性-终端价值指数'           ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.YINXING_YONGHU  is  '隐性-用户属性指数'               ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.YINXING_QIYE  is  '隐性-企业接触指数'                 ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.YINXING_SHEJIAO  is  '隐性-社交广度指数'              ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.YINXING_DPI  is  '隐性-DPI指数'                       ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.XIANXING  is  '显性价值指数'                          ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.YINXING  is  '隐性价值指数'                           ;
comment on column $db_name.TB_MCUST_VALUE_RES_MON_AF.JIAZHI  is  '价值指数'                                ;

DELETE FROM $db_name.TB_MCUST_VALUE_RES_MON_AF WHERE op_time=$TX_MONTH;

alter session force parallel dml

insert /*+append parallel(t,4) nologging*/ into $db_name.TB_MCUST_VALUE_RES_MON_AF
SELECT
	 T1.op_time 
	,T1.user_id 
	,T1.acc_nbr 
	,T1.USER_TYPE 
	,T2.DEPT_NAME
	,ARPU             	* 50 	XIANXING_ZHIJIE1
	,ARPU_INCREASE_PP * 2  	XIANXING_ZHIJIE2
	,DOU              	* 25 	XIANXING_YEWU1
	,DOU_INCREASE_PP  	* 3  	XIANXING_YEWU2
	,TERMN_PRICE      	* 2  	XIANXING_ZHONGDUAN1 
	,OTC_TIME         	* 3  	XIANXING_ZHONGDUAN2 
	,CHENMO_FLAG     	* 0.5	YINXING_YONGHU1
	,G4_FLAG          	* 1  	YINXING_YONGHU2 
	,BILL_FLAG    	* 0.5	YINXING_YONGHU3 
	,USER_ONLINE_DURA         	* 3  	YINXING_YONGHU4
	,ZHUFUKA_FLAG     	* 2  	YINXING_QIYE1
	,FLOW_PACKET_NUM  	* 0.5  	YINXING_QIYE2
	,YZF_CSM_FLAG     	* 2  	YINXING_QIYE3
	,TYSX_ACT_FLAG    	* 0.25	YINXING_QIYE4
	,HUANGO_ACT_FLAG  	* 0.25	YINXING_QIYE5
	,MARKET_NUM       	* 1  	YINXING_QIYE6
	,JWQ_NUM          	* 1  	YINXING_SHEJIAO1
	,JWQ_TELE_PP      	* 0.5	YINXING_SHEJIAO2
	,MESS_FLAG              * 0.5	YINXING_SHEJIAO3
	,APP_NUMS         	* 0.5	YINXING_DPI1
	,APP_VISIT_10_NUMS	* 0.5	YINXING_DPI2
	,APP_5M_NUMS      	* 0.5	YINXING_DPI3
	,APP_ACT_5D_NUMS  	* 0.5	YINXING_DPI4
	,YJDK_FLAG       	* (-5) 	YINXING_QITA
	,(ARPU             	* 50) 	+ (ARPU_INCREASE_PP 	* 2)	XIANXING_ZHIJIE
	,(DOU              	* 25 )	+ (DOU_INCREASE_PP  	* 3)	XIANXING_YEWU
	,(TERMN_PRICE      	* 2)	+ (OTC_TIME         	* 3)	XIANXING_ZHONGDUAN
	,(CHENMO_FLAG     	* 0.5)	+ (G4_FLAG          	* 1)	+ (BILL_FLAG    	* 0.5)	+ (USER_ONLINE_DURA         	* 3)	YINXING_YONGHU
	,(ZHUFUKA_FLAG     	* 2) 	+ (FLOW_PACKET_NUM  	* 0.5)	+ (YZF_CSM_FLAG     	* 2)	+ (TYSX_ACT_FLAG    	* 0.25)	+ (HUANGO_ACT_FLAG  	* 0.25)	+ (MARKET_NUM       	* 1)	YINXING_QIYE
	,(JWQ_NUM          	* 1)	+ (JWQ_TELE_PP      	* 0.5)	+ (MESS_FLAG              * 0.5	)	YINXING_SHEJIAO
	,(APP_NUMS         	* 0.5	)	+ (APP_VISIT_10_NUMS	* 0.5)	+ (APP_5M_NUMS      	* 0.5)	+ (APP_ACT_5D_NUMS  	* 0.5)	YINXING_DPI
	,((ARPU             	* 50) 	+ (ARPU_INCREASE_PP * 2))	+ ((DOU              	* 25 )	+ (DOU_INCREASE_PP  	* 3))	+ ((TERMN_PRICE      	* 2)	+ (OTC_TIME         	* 3))	XIANXING
	,((CHENMO_FLAG     	* 0.5)	+ (G4_FLAG          	* 1)	+ (BILL_FLAG    	* 0.5)	+ (USER_ONLINE_DURA         	* 3))	+ ((ZHUFUKA_FLAG     	* 2) 	+ (FLOW_PACKET_NUM  	* 0.5)	+ (YZF_CSM_FLAG     	* 2)	+ (TYSX_ACT_FLAG    	* 0.25)	+ (HUANGO_ACT_FLAG  	* 0.25)	+ (MARKET_NUM       	* 1))	+ ((JWQ_NUM          	* 1)	+ (JWQ_TELE_PP      	* 0.5)	+ (MESS_FLAG              * 0.5	))	+ ((APP_NUMS         	* 0.5	)	+ (APP_VISIT_10_NUMS	* 0.5)	+ (APP_5M_NUMS      	* 0.5)	+ (APP_ACT_5D_NUMS  	* 0.5))	YINXING
	,(((ARPU             	* 50) 	+ (ARPU_INCREASE_PP * 2))	+ ((DOU              	* 25 )	+ (DOU_INCREASE_PP  	* 3))	+ ((TERMN_PRICE      	* 2)	+ (OTC_TIME         	* 3))		+ 	((CHENMO_FLAG     	* 0.5)	+ (G4_FLAG          	* 1)	+ (BILL_FLAG    	* 0.5)	+ (USER_ONLINE_DURA         	* 3))	+ ((ZHUFUKA_FLAG     	* 2) 	+ (FLOW_PACKET_NUM  	* 0.5)	+ (YZF_CSM_FLAG     	* 2)	+ (TYSX_ACT_FLAG    	* 0.25)	+ (HUANGO_ACT_FLAG  	* 0.25)	+ (MARKET_NUM       	* 1))	+ ((JWQ_NUM          	* 1)	+ (JWQ_TELE_PP      	* 0.5)	+ (MESS_FLAG              * 0.5	))	+ ((APP_NUMS         	* 0.5	)	+ (APP_VISIT_10_NUMS	* 0.5)	+ (APP_5M_NUMS      	* 0.5)	+ (APP_ACT_5D_NUMS  	* 0.5))	+ (YJDK_FLAG       	* (-5)))	JIAZHI
FROM 	$db_name.TB_MCUST_VALUE_CAL_MON_AF_STD	T1
LEFT JOIN 	(select 
T2.user_id
,t2.MAIN_PROD_ID2
,T3.nkh_dept_name2 as DEPT_NAME
from edw_share.s_td_user_360_$TX_MONTH T2
LEFT JOIN    DIM.D_DEPT  T3
ON  T2.USER_DEVELOP_DEPART_ID = T3.dept_id) 	T2
ON	T1.op_time=T2.op_time AND T1.user_id=T2.user_id
where t1.op_time=$TX_MONTH;

alter session disable parallel dml

select 36,sysdate from dual;
----------------------------------------01-02价值指数-01移动公众用户 结束------

---------------------------5、家庭客户价值指数结果表 开始----
-----预付费  -----定基
delete   from $db_name.TB_MCUST_FIX_CAL_MON_BF_STD;

alter session force parallel dml

INSERT /*+append parallel(t,4) nologging*/ INTO $db_name.TB_MCUST_FIX_CAL_MON_BF_STD
SELECT    
    OP_TIME 
   ,USER_ID 
   ,ACC_NBR 
   ,BILL_FLAG 
   ,USER_TYPE 
   ,ARPU                 /20 
   ,ARPU_INCREASE_PP     /0.1 
   ,MOU                  /100
   ,MOU_INCREASE_PP      /0.1
   ,DOU                  /500
   ,DOU_INCREASE_PP      /0.2
   ,TERMN_PRICE 
   ,OTC_TIME
   ,CHENMO_FLAG  
   ,G4_FLAG 
   ,QIANFEI_FLAG 
   ,DOU_STOP_FLAG 
   ,DOU_STOP_NUM     
   ,USER_ONLINE_DURA             /48
   ,ZHUFUKA_FLAG 
   ,FLOW_PACKET_NUM      /2
   ,YZF_ACT_FLAG 
   ,YZF_CSM_FLAG 
   ,TYSX_ACT_FLAG 
   ,HUANGO_ACT_FLAG 
   ,MARKET_NUM 
   ,JWQ_NUM              /20
   ,JWQ_TELE_PP 
   ,MESS_FLAG 
   ,APP_NUMS             /5
   ,APP_VISIT_10_NUMS    /3
   ,APP_5M_NUMS          
   ,APP_ACT_5D_NUMS      /5
   ,YJDK_FLAG
from $db_name.TB_MCUST_VALUE_CAL_MON_BF
;

alter session disable parallel dml

select 37,sysdate from dual;

-----后付费   ----定基
delete   from $db_name.TB_MCUST_FIX_CAL_MON_AF_STD;

alter session force parallel dml

INSERT /*+append parallel(t,4) nologging*/ INTO $db_name.TB_MCUST_FIX_CAL_MON_AF_STD
SELECT    
    OP_TIME 
   ,USER_ID 
   ,ACC_NBR 
   ,BILL_FLAG 
   ,USER_TYPE 
   ,ARPU                 /80 
   ,ARPU_INCREASE_PP     /0.1 
   ,MOU                  /200
   ,MOU_INCREASE_PP      /0.1
   ,DOU                  /1500
   ,DOU_INCREASE_PP      /0.2
   ,TERMN_PRICE 
   ,OTC_TIME 
   ,CHENMO_FLAG  
   ,G4_FLAG 
   ,QIANFEI_FLAG 
   ,DOU_STOP_FLAG 
   ,DOU_STOP_NUM     
   ,USER_ONLINE_DURA             /24
   ,ZHUFUKA_FLAG 
   ,FLOW_PACKET_NUM      /3
   ,YZF_ACT_FLAG 
   ,YZF_CSM_FLAG 
   ,TYSX_ACT_FLAG 
   ,HUANGO_ACT_FLAG 
   ,MARKET_NUM 
   ,JWQ_NUM              /40
   ,JWQ_TELE_PP 
   ,MESS_FLAG 
   ,APP_NUMS             /7
   ,APP_VISIT_10_NUMS    /5
   ,APP_5M_NUMS          
   ,APP_ACT_5D_NUMS      /8
   ,YJDK_FLAG
from $db_name.TB_MCUST_VALUE_CAL_MON_AF
;

alter session disable parallel dml

select 38,sysdate from dual;

---------------执行熵计算公式-----定基------
---------预付费  ---定基
drop table $db_name.TB_MCUST_FIX_CAL_MON_BF_STD_1;
CREATE  TABLE $db_name.TB_MCUST_FIX_CAL_MON_BF_STD_1 AS(	
select
  OP_TIME
 ,USER_ID
 ,cast((case when ARPU               =0 then 0.0001 else  ARPU              end) as number(10,4))     ARPU             
 ,cast((case when ARPU_INCREASE_PP   =0 then 0.0001 else  ARPU_INCREASE_PP  end) as number(10,4))     ARPU_INCREASE_PP 
 ,cast((case when MOU                =0 then 0.0001 else  MOU               end) as number(10,4))     MOU              
 ,cast((case when MOU_INCREASE_PP    =0 then 0.0001 else  MOU_INCREASE_PP   end) as number(10,4))     MOU_INCREASE_PP  
 ,cast((case when DOU                =0 then 0.0001 else  DOU               end) as number(10,4))     DOU              
 ,cast((case when DOU_INCREASE_PP    =0 then 0.0001 else  DOU_INCREASE_PP   end) as number(10,4))     DOU_INCREASE_PP  
 ,cast((case when TERMN_PRICE        =0 then 0.0001 else  TERMN_PRICE       end) as number(10,4))     TERMN_PRICE    
 ,cast((case when OTC_TIME           =0 then 0.0001 else  OTC_TIME          end) as number(10,4))     OTC_TIME       
 ,cast((case when CHENMO_FLAG        =0 then 2      else  CHENMO_FLAG       end) as number(10,4))     CHENMO_FLAG      
 ,cast((case when G4_FLAG            =0 then 2      else  G4_FLAG           end) as number(10,4))     G4_FLAG          
 ,cast((case when QIANFEI_FLAG       =0 then 2      else  QIANFEI_FLAG      end) as number(10,4))     QIANFEI_FLAG     
 ,cast((case when DOU_STOP_FLAG      =0 then 2      else  DOU_STOP_FLAG     end) as number(10,4))     DOU_STOP_FLAG    
 ,cast((case when DOU_STOP_NUM       =0 then 0.0001 else  DOU_STOP_NUM      end) as number(10,4))     DOU_STOP_NUM     
 ,cast((case when USER_ONLINE_DURA           =0 then 0.0001 else  USER_ONLINE_DURA          end) as number(10,4))     USER_ONLINE_DURA         
 ,cast((case when ZHUFUKA_FLAG       =0 then 2      else  ZHUFUKA_FLAG      end) as number(10,4))     ZHUFUKA_FLAG     
 ,cast((case when FLOW_PACKET_NUM    =0 then 0.0001 else  FLOW_PACKET_NUM   end) as number(10,4))     FLOW_PACKET_NUM  
 ,cast((case when YZF_ACT_FLAG       =0 then 2      else  YZF_ACT_FLAG      end) as number(10,4))     YZF_ACT_FLAG     
 ,cast((case when YZF_CSM_FLAG       =0 then 2      else  YZF_CSM_FLAG      end) as number(10,4))     YZF_CSM_FLAG     
 ,cast((case when TYSX_ACT_FLAG      =0 then 2      else  TYSX_ACT_FLAG     end) as number(10,4))     TYSX_ACT_FLAG    
 ,cast((case when HUANGO_ACT_FLAG    =0 then 2      else  HUANGO_ACT_FLAG   end) as number(10,4))     HUANGO_ACT_FLAG  
 ,cast((case when MARKET_NUM         =0 then 0.0001 else  MARKET_NUM        end) as number(10,4))     MARKET_NUM       
 ,cast((case when JWQ_NUM            =0 then 0.0001 else  JWQ_NUM           end) as number(10,4))     JWQ_NUM          
 ,cast((case when JWQ_TELE_PP        =0 then 0.0001 else  JWQ_TELE_PP       end) as number(10,4))     JWQ_TELE_PP      
 ,cast((case when APP_NUMS           =0 then 0.0001 else  APP_NUMS          end) as number(10,4))     APP_NUMS         
 ,cast((case when APP_VISIT_10_NUMS  =0 then 0.0001 else  APP_VISIT_10_NUMS end) as number(10,4))     APP_VISIT_10_NUMS
 ,cast((case when APP_5M_NUMS        =0 then 0.0001 else  APP_5M_NUMS       end) as number(10,4))     APP_5M_NUMS      
 ,cast((case when APP_ACT_5D_NUMS    =0 then 0.0001 else  APP_ACT_5D_NUMS   end) as number(10,4))     APP_ACT_5D_NUMS  
 ,cast((case when YJDK_FLAG          =0 then 0.0001 else  YJDK_FLAG         end) as number(10,4))     YJDK_FLAG    
from $db_name.TB_MCUST_FIX_CAL_MON_BF_STD
where OP_TIME=$TX_MONTH
);

select 39,sysdate from dual;

drop table $db_name.TB_MCUST_FIX_CAL_MON_BF_STD_2;
CREATE  TABLE $db_name.TB_MCUST_FIX_CAL_MON_BF_STD_2 AS(	
select
  OP_TIME
 ,USER_ID
 ,cast((case when ARPU               =0 then 0.0001 else  ARPU              end) as number(10,4))     ARPU             
 ,cast((case when ARPU_INCREASE_PP   =0 then 0.0001 else  ARPU_INCREASE_PP  end) as number(10,4))     ARPU_INCREASE_PP 
 ,cast((case when MOU                =0 then 0.0001 else  MOU               end) as number(10,4))     MOU              
 ,cast((case when MOU_INCREASE_PP    =0 then 0.0001 else  MOU_INCREASE_PP   end) as number(10,4))     MOU_INCREASE_PP  
 ,cast((case when DOU                =0 then 0.0001 else  DOU               end) as number(10,4))     DOU              
 ,cast((case when DOU_INCREASE_PP    =0 then 0.0001 else  DOU_INCREASE_PP   end) as number(10,4))     DOU_INCREASE_PP  
 ,cast((case when TERMN_PRICE        =0 then 0.0001 else  TERMN_PRICE       end) as number(10,4))     TERMN_PRICE    
 ,cast((case when OTC_TIME           =0 then 0.0001 else  OTC_TIME          end) as number(10,4))     OTC_TIME    
 ,cast((case when CHENMO_FLAG        =0 then 2      else  CHENMO_FLAG       end) as number(10,4))     CHENMO_FLAG      
 ,cast((case when G4_FLAG            =0 then 2      else  G4_FLAG           end) as number(10,4))     G4_FLAG          
 ,cast((case when QIANFEI_FLAG       =0 then 2      else  QIANFEI_FLAG      end) as number(10,4))     QIANFEI_FLAG     
 ,cast((case when DOU_STOP_FLAG      =0 then 2      else  DOU_STOP_FLAG     end) as number(10,4))     DOU_STOP_FLAG    
 ,cast((case when DOU_STOP_NUM       =0 then 0.0001 else  DOU_STOP_NUM      end) as number(10,4))     DOU_STOP_NUM     
 ,cast((case when USER_ONLINE_DURA           =0 then 0.0001 else  USER_ONLINE_DURA          end) as number(10,4))     USER_ONLINE_DURA         
 ,cast((case when ZHUFUKA_FLAG       =0 then 2      else  ZHUFUKA_FLAG      end) as number(10,4))     ZHUFUKA_FLAG     
 ,cast((case when FLOW_PACKET_NUM    =0 then 0.0001 else  FLOW_PACKET_NUM   end) as number(10,4))     FLOW_PACKET_NUM  
 ,cast((case when YZF_ACT_FLAG       =0 then 2      else  YZF_ACT_FLAG      end) as number(10,4))     YZF_ACT_FLAG     
 ,cast((case when YZF_CSM_FLAG       =0 then 2      else  YZF_CSM_FLAG      end) as number(10,4))     YZF_CSM_FLAG     
 ,cast((case when TYSX_ACT_FLAG      =0 then 2      else  TYSX_ACT_FLAG     end) as number(10,4))     TYSX_ACT_FLAG    
 ,cast((case when HUANGO_ACT_FLAG    =0 then 2      else  HUANGO_ACT_FLAG   end) as number(10,4))     HUANGO_ACT_FLAG  
 ,cast((case when MARKET_NUM         =0 then 0.0001 else  MARKET_NUM        end) as number(10,4))     MARKET_NUM       
 ,cast((case when JWQ_NUM            =0 then 0.0001 else  JWQ_NUM           end) as number(10,4))     JWQ_NUM          
 ,cast((case when JWQ_TELE_PP        =0 then 0.0001 else  JWQ_TELE_PP       end) as number(10,4))     JWQ_TELE_PP      
 ,cast((case when APP_NUMS           =0 then 0.0001 else  APP_NUMS          end) as number(10,4))     APP_NUMS         
 ,cast((case when APP_VISIT_10_NUMS  =0 then 0.0001 else  APP_VISIT_10_NUMS end) as number(10,4))     APP_VISIT_10_NUMS
 ,cast((case when APP_5M_NUMS        =0 then 0.0001 else  APP_5M_NUMS       end) as number(10,4))     APP_5M_NUMS      
 ,cast((case when APP_ACT_5D_NUMS    =0 then 0.0001 else  APP_ACT_5D_NUMS   end) as number(10,4))     APP_ACT_5D_NUMS  
 ,cast((case when YJDK_FLAG          =0 then 0.0001 else  YJDK_FLAG         end) as number(10,4))     YJDK_FLAG      
from $db_name.TB_MCUST_FIX_CAL_MON_BF_STD_1
where OP_TIME=$TX_MONTH
);

select 40,sysdate from dual;

---------后付费   ------定基
drop table $db_name.TB_MCUST_FIX_CAL_MON_AF_STD_1;
CREATE  TABLE $db_name.TB_MCUST_FIX_CAL_MON_AF_STD_1 AS(	
select
  OP_TIME
 ,USER_ID
 ,cast((case when ARPU               =0 then 0.0001 else  ARPU              end) as number(10,4))     ARPU             
 ,cast((case when ARPU_INCREASE_PP   =0 then 0.0001 else  ARPU_INCREASE_PP  end) as number(10,4))     ARPU_INCREASE_PP 
 ,cast((case when MOU                =0 then 0.0001 else  MOU               end) as number(10,4))     MOU              
 ,cast((case when MOU_INCREASE_PP    =0 then 0.0001 else  MOU_INCREASE_PP   end) as number(10,4))     MOU_INCREASE_PP  
 ,cast((case when DOU                =0 then 0.0001 else  DOU               end) as number(10,4))     DOU              
 ,cast((case when DOU_INCREASE_PP    =0 then 0.0001 else  DOU_INCREASE_PP   end) as number(10,4))     DOU_INCREASE_PP  
 ,cast((case when TERMN_PRICE        =0 then 0.0001 else  TERMN_PRICE       end) as number(10,4))     TERMN_PRICE 
 ,cast((case when OTC_TIME           =0 then 0.0001 else  OTC_TIME          end) as number(10,4))     OTC_TIME  
 ,cast((case when CHENMO_FLAG        =0 then 2      else  CHENMO_FLAG       end) as number(10,4))     CHENMO_FLAG      
 ,cast((case when G4_FLAG            =0 then 2      else  G4_FLAG           end) as number(10,4))     G4_FLAG          
 ,cast((case when QIANFEI_FLAG       =0 then 2      else  QIANFEI_FLAG      end) as number(10,4))     QIANFEI_FLAG     
 ,cast((case when DOU_STOP_FLAG      =0 then 2      else  DOU_STOP_FLAG     end) as number(10,4))     DOU_STOP_FLAG    
 ,cast((case when DOU_STOP_NUM       =0 then 0.0001 else  DOU_STOP_NUM      end) as number(10,4))     DOU_STOP_NUM     
 ,cast((case when USER_ONLINE_DURA           =0 then 0.0001 else  USER_ONLINE_DURA          end) as number(10,4))     USER_ONLINE_DURA         
 ,cast((case when ZHUFUKA_FLAG       =0 then 2      else  ZHUFUKA_FLAG      end) as number(10,4))     ZHUFUKA_FLAG     
 ,cast((case when FLOW_PACKET_NUM    =0 then 0.0001 else  FLOW_PACKET_NUM   end) as number(10,4))     FLOW_PACKET_NUM  
 ,cast((case when YZF_ACT_FLAG       =0 then 2      else  YZF_ACT_FLAG      end) as number(10,4))     YZF_ACT_FLAG     
 ,cast((case when YZF_CSM_FLAG       =0 then 2      else  YZF_CSM_FLAG      end) as number(10,4))     YZF_CSM_FLAG     
 ,cast((case when TYSX_ACT_FLAG      =0 then 2      else  TYSX_ACT_FLAG     end) as number(10,4))     TYSX_ACT_FLAG    
 ,cast((case when HUANGO_ACT_FLAG    =0 then 2      else  HUANGO_ACT_FLAG   end) as number(10,4))     HUANGO_ACT_FLAG  
 ,cast((case when MARKET_NUM         =0 then 0.0001 else  MARKET_NUM        end) as number(10,4))     MARKET_NUM       
 ,cast((case when JWQ_NUM            =0 then 0.0001 else  JWQ_NUM           end) as number(10,4))     JWQ_NUM          
 ,cast((case when JWQ_TELE_PP        =0 then 0.0001 else  JWQ_TELE_PP       end) as number(10,4))     JWQ_TELE_PP      
 ,cast((case when APP_NUMS           =0 then 0.0001 else  APP_NUMS          end) as number(10,4))     APP_NUMS         
 ,cast((case when APP_VISIT_10_NUMS  =0 then 0.0001 else  APP_VISIT_10_NUMS end) as number(10,4))     APP_VISIT_10_NUMS
 ,cast((case when APP_5M_NUMS        =0 then 0.0001 else  APP_5M_NUMS       end) as number(10,4))     APP_5M_NUMS      
 ,cast((case when APP_ACT_5D_NUMS    =0 then 0.0001 else  APP_ACT_5D_NUMS   end) as number(10,4))     APP_ACT_5D_NUMS  
 ,cast((case when YJDK_FLAG          =0 then 0.0001 else  YJDK_FLAG         end) as number(10,4))     YJDK_FLAG        
from $db_name.TB_MCUST_FIX_CAL_MON_AF_STD
where OP_TIME=$TX_MONTH
);

select 41,sysdate from dual;

drop table $db_name.TB_MCUST_FIX_CAL_MON_AF_STD_2;
CREATE  TABLE $db_name.TB_MCUST_FIX_CAL_MON_AF_STD_2 AS(	
select
  OP_TIME
 ,USER_ID
 ,cast((case when ARPU               =0 then 0.0001 else  ARPU              end) as number(10,4))     ARPU             
 ,cast((case when ARPU_INCREASE_PP   =0 then 0.0001 else  ARPU_INCREASE_PP  end) as number(10,4))     ARPU_INCREASE_PP 
 ,cast((case when MOU                =0 then 0.0001 else  MOU               end) as number(10,4))     MOU              
 ,cast((case when MOU_INCREASE_PP    =0 then 0.0001 else  MOU_INCREASE_PP   end) as number(10,4))     MOU_INCREASE_PP  
 ,cast((case when DOU                =0 then 0.0001 else  DOU               end) as number(10,4))     DOU              
 ,cast((case when DOU_INCREASE_PP    =0 then 0.0001 else  DOU_INCREASE_PP   end) as number(10,4))     DOU_INCREASE_PP  
 ,cast((case when TERMN_PRICE        =0 then 0.0001 else  TERMN_PRICE       end) as number(10,4))     TERMN_PRICE   
 ,cast((case when OTC_TIME           =0 then 0.0001 else  OTC_TIME          end) as number(10,4))     OTC_TIME
 ,cast((case when CHENMO_FLAG        =0 then 2      else  CHENMO_FLAG       end) as number(10,4))     CHENMO_FLAG      
 ,cast((case when G4_FLAG            =0 then 2      else  G4_FLAG           end) as number(10,4))     G4_FLAG          
 ,cast((case when QIANFEI_FLAG       =0 then 2      else  QIANFEI_FLAG      end) as number(10,4))     QIANFEI_FLAG     
 ,cast((case when DOU_STOP_FLAG      =0 then 2      else  DOU_STOP_FLAG     end) as number(10,4))     DOU_STOP_FLAG    
 ,cast((case when DOU_STOP_NUM       =0 then 0.0001 else  DOU_STOP_NUM      end) as number(10,4))     DOU_STOP_NUM     
 ,cast((case when USER_ONLINE_DURA           =0 then 0.0001 else  USER_ONLINE_DURA          end) as number(10,4))     USER_ONLINE_DURA         
 ,cast((case when ZHUFUKA_FLAG       =0 then 2      else  ZHUFUKA_FLAG      end) as number(10,4))     ZHUFUKA_FLAG     
 ,cast((case when FLOW_PACKET_NUM    =0 then 0.0001 else  FLOW_PACKET_NUM   end) as number(10,4))     FLOW_PACKET_NUM  
 ,cast((case when YZF_ACT_FLAG       =0 then 2      else  YZF_ACT_FLAG      end) as number(10,4))     YZF_ACT_FLAG     
 ,cast((case when YZF_CSM_FLAG       =0 then 2      else  YZF_CSM_FLAG      end) as number(10,4))     YZF_CSM_FLAG     
 ,cast((case when TYSX_ACT_FLAG      =0 then 2      else  TYSX_ACT_FLAG     end) as number(10,4))     TYSX_ACT_FLAG    
 ,cast((case when HUANGO_ACT_FLAG    =0 then 2      else  HUANGO_ACT_FLAG   end) as number(10,4))     HUANGO_ACT_FLAG  
 ,cast((case when MARKET_NUM         =0 then 0.0001 else  MARKET_NUM        end) as number(10,4))     MARKET_NUM       
 ,cast((case when JWQ_NUM            =0 then 0.0001 else  JWQ_NUM           end) as number(10,4))     JWQ_NUM          
 ,cast((case when JWQ_TELE_PP        =0 then 0.0001 else  JWQ_TELE_PP       end) as number(10,4))     JWQ_TELE_PP      
 ,cast((case when APP_NUMS           =0 then 0.0001 else  APP_NUMS          end) as number(10,4))     APP_NUMS         
 ,cast((case when APP_VISIT_10_NUMS  =0 then 0.0001 else  APP_VISIT_10_NUMS end) as number(10,4))     APP_VISIT_10_NUMS
 ,cast((case when APP_5M_NUMS        =0 then 0.0001 else  APP_5M_NUMS       end) as number(10,4))     APP_5M_NUMS      
 ,cast((case when APP_ACT_5D_NUMS    =0 then 0.0001 else  APP_ACT_5D_NUMS   end) as number(10,4))     APP_ACT_5D_NUMS  
 ,cast((case when YJDK_FLAG          =0 then 0.0001 else  YJDK_FLAG         end) as number(10,4))     YJDK_FLAG        
from $db_name.TB_MCUST_FIX_CAL_MON_AF_STD_1
where OP_TIME=$TX_MONTH
);

select 42,sysdate from dual;

--------------------家庭客户评价系数—融合系数---------------------------------------------------------------------------------
------表结构
CREATE  TABLE $db_name.TB_KD_EVALUATE_SCORE_MON 
     (
OP_TIME	INTEGER
,USER_ID	NUMBER(16,0)
,ACC_NBR	VARCHAR(40)
,BILL_FLAG	VARCHAR(2)
,KD_SCORE	INTEGER
,PHONE_SCORE	INTEGER
,IPTV_SCORE	NUMBER(8,1)
,TOT_SCORE	NUMBER(8,1)
      );
comment on column $db_name.TB_KD_EVALUATE_SCORE_MON.OP_TIME is '处理日期'           ;
comment on column $db_name.TB_KD_EVALUATE_SCORE_MON.USER_ID is '用户id'             ;
comment on column $db_name.TB_KD_EVALUATE_SCORE_MON.ACC_NBR is '用户号码'           ;
comment on column $db_name.TB_KD_EVALUATE_SCORE_MON.BILL_FLAG is '出账标识'         ;
comment on column $db_name.TB_KD_EVALUATE_SCORE_MON.KD_SCORE is '宽带得分(自带1分)' ;
comment on column $db_name.TB_KD_EVALUATE_SCORE_MON.PHONE_SCORE is '手机融合得分'   ;
comment on column $db_name.TB_KD_EVALUATE_SCORE_MON.IPTV_SCORE is 'IPTV融合得分'    ;
comment on column $db_name.TB_KD_EVALUATE_SCORE_MON.TOT_SCORE is '总得分'           ;

--------从360中提取目标宽带用户(固网宽带，在网)
drop table $db_name.ndxs_temp_kd_user;
CREATE  TABLE $db_name.ndxs_temp_kd_user AS(	
   select
    OP_TIME
   ,USER_ID
   ,ACC_NBR
   ,case when BILL_FLAG=1 then '1' else '0' end  BILL_FLAG
   ,FEE/100                              KD_PAY
   from edw_share.s_td_user_360_$TX_MONTH  t1
   where INNET_FLAG = 1          --在网
   and t1.MAIN_PROD_ID2 = '21'      --固网宽带
   and OP_TIME = $TX_MONTH
);

select 43,sysdate from dual;
-------绑定手机用户
drop table $db_name.ndxs_temp_phone_user;
CREATE  TABLE $db_name.ndxs_temp_phone_user AS(
SELECT
     T1.OP_TIME
    ,T1.USER_ID                --固宽带网ID
FROM edw_share.s_td_user_360_$TX_MONTH T1
inner JOIN edw_share.s_td_user_360_$TX_MONTH T2
on T1.CUST_ID=T2.CUST_ID
AND T1.ACCT_ID = T2.ACCT_ID
AND T1.COM_PROD_INST_ID = T2.COM_PROD_INST_ID
AND T1.USER_ID<>T2.USER_ID
WHERE T1.COM_PROD_INST_ID IS NOT NULL --融合产品
and T2.COM_PROD_INST_ID IS NOT NULL
AND T1.INNET_FLAG = 1  --在网标识,融合产品只找在网的，不在网的没有意义
and T2.INNET_FLAG = 1
AND t1.OP_TIME = $TX_MONTH
and t2.OP_TIME = $TX_MONTH
and (T1.MAIN_PROD_ID2=21 or t1.MAIN_PROD_ID in (20130060,80000030,80000031,80000032))
and t1.MAIN_PROD_ID<>10023
AND T2.C_PSTN_FLAG=1 --C网用户
group by  T1.OP_TIME,T1.USER_ID
);


-------绑定IPTV用户
drop table $db_name.ndxs_temp_iptv_user;
CREATE  TABLE $db_name.ndxs_temp_iptv_user AS(
SELECT
     T1.USER_ID                --固宽带网ID
FROM edw_share.s_td_user_360_$TX_MONTH T1
inner JOIN edw_share.s_td_user_360_$TX_MONTH T2
on T1.CUST_ID=T2.CUST_ID
AND T1.ACCT_ID = T2.ACCT_ID
AND T1.COM_PROD_INST_ID = T2.COM_PROD_INST_ID
AND T1.USER_ID<>T2.USER_ID
WHERE T1.COM_PROD_INST_ID IS NOT NULL --融合产品
and T2.COM_PROD_INST_ID IS NOT NULL
AND T1.INNET_FLAG = 1  --在网标识,融合产品只找在网的，不在网的没有意义
and T2.INNET_FLAG = 1
AND t1.OP_TIME = $TX_MONTH
and t2.OP_TIME = $TX_MONTH
and (T1.MAIN_PROD_ID2=21 or t1.MAIN_PROD_ID in (20130060,80000030,80000031,80000032))
and t1.MAIN_PROD_ID<>10023
AND T2.MAIN_PROD_ID in (600000293)
group by T1.USER_ID                      
);

select 44,sysdate from dual;
------汇总结果
delete   from $db_name.TB_KD_EVALUATE_SCORE_MON
where OP_TIME=$TX_MONTH;
INSERT INTO $db_name.TB_KD_EVALUATE_SCORE_MON
select
 $TX_MONTH op_time
,t1.USER_ID
,t1.ACC_NBR
,t1.BILL_FLAG
,1  KD_SCORE
,case when t3.USER_ID is not null then 1 else 0 end            PHONE_SCORE 
,case when t4.USER_ID is not null then 0.5 else 0 end          IPTV_SCORE
,1+(case when t3.USER_ID is not null then 1 else 0 end)+(case when t4.USER_ID is not null then 0.5 else 0 end)   TOT_SCORE  
from $db_name.ndxs_temp_kd_user  t1
left join $db_name.ndxs_temp_phone_user  t3
on t1.USER_ID=t3.USER_ID
left join $db_name.ndxs_temp_iptv_user t4
on t1.USER_ID=t4.USER_ID
where t1.OP_TIME=$TX_MONTH
group by 
 $TX_MONTH
,t1.USER_ID
,t1.ACC_NBR
,t1.BILL_FLAG
,1  
,case when t3.USER_ID is not null then 1 else 0 end
,case when t4.USER_ID is not null then 0.5 else 0 end 
;

select 45,sysdate from dual;



---------------------------------------------------------------------------------------------------------------------------------------------------------
----------------家庭宽带用户价值评估----------------

----1、家庭宽带用户价值指数基础表
CREATE  TABLE $db_name.TB_FCUST_VALUE_BASE_MON 
     (
OP_TIME	INTEGER
,USER_ID	NUMBER(16,0)
,ACC_NBR	VARCHAR(40)
,BILL_FLAG	VARCHAR(2)
,TOT_INCOME	NUMBER(10,2)
,USE_TIME	NUMBER(10,2)
,USE_FLOW	NUMBER(10,2)
,FUSE_NUM	NUMBER(6,0)
,IPTV_FLAG	NUMBER(4,0)
,IPTV_NUM	NUMBER(6,0)
,KD_RATE	varchar(40,0)
,USER_ONLINE_DURA	NUMBER(6,0)
,HEYUE_DUR	NUMBER(6,0)
,FAMILY_PRO_NUM	NUMBER(6,0)
      );
comment on column $db_name.TB_FCUST_VALUE_BASE_MON.OP_TIME is '处理日期'                    ;
comment on column $db_name.TB_FCUST_VALUE_BASE_MON.USER_ID is '用户id'                      ;
comment on column $db_name.TB_FCUST_VALUE_BASE_MON.ACC_NBR is '用户号码'                    ;
comment on column $db_name.TB_FCUST_VALUE_BASE_MON.BILL_FLAG is '出账标识'                  ;
comment on column $db_name.TB_FCUST_VALUE_BASE_MON.TOT_INCOME is '月总收入(元)'             ;
comment on column $db_name.TB_FCUST_VALUE_BASE_MON.USE_TIME is '家宽产品使用时长(小时)'     ;
comment on column $db_name.TB_FCUST_VALUE_BASE_MON.USE_FLOW is '家宽产品使用流量(GB)'       ;
comment on column $db_name.TB_FCUST_VALUE_BASE_MON.FUSE_NUM is '融合的用户数量'             ;
comment on column $db_name.TB_FCUST_VALUE_BASE_MON.IPTV_FLAG is '是否订购IPTV'              ;
comment on column $db_name.TB_FCUST_VALUE_BASE_MON.IPTV_NUM is 'IPTV点播次数'               ;
comment on column $db_name.TB_FCUST_VALUE_BASE_MON.KD_RATE is '家宽产品速率'                ;
comment on column $db_name.TB_FCUST_VALUE_BASE_MON.USER_ONLINE_DURA is '在网时长'           ;
comment on column $db_name.TB_FCUST_VALUE_BASE_MON.HEYUE_DUR is '合约时长'                  ;
comment on column $db_name.TB_FCUST_VALUE_BASE_MON.FAMILY_PRO_NUM is '智慧家庭产品使用个数' ;

----2、处理极值
DROP TABLE	$db_name.TB_FCUST_VALUE_CAL_MON;
CREATE  TABLE $db_name.TB_FCUST_VALUE_CAL_MON
(
OP_TIME	INTEGER
,USER_ID	NUMBER(16,0)                                                                                          
,ACC_NBR	VARCHAR(40)
,BILL_FLAG	VARCHAR(2)
,TOT_INCOME	NUMBER(10,2)
,USE_TIME	NUMBER(10,2)
,FUSE_NUM	NUMBER(6,0)
,IPTV_FLAG	NUMBER(4,0)
,IPTV_NUM	NUMBER(6,0)
,KD_RATE	varchar(40)
,USER_ONLINE_DURA	NUMBER(6,0)
,HEYUE_DUR	NUMBER(6,0)
,FAMILY_PRO_NUM	NUMBER(6,0)
);
comment on column $db_name.TB_FCUST_VALUE_CAL_MON.OP_TIME is '处理日期'                    ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON.USER_ID is '用户id'                      ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON.ACC_NBR is '用户号码'                    ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON.BILL_FLAG is '出账标识'                  ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON.TOT_INCOME is '月总收入(元)'             ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON.USE_TIME is '家宽产品使用时长(小时)'     ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON.FUSE_NUM is '融合的用户数量'             ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON.IPTV_FLAG is '是否订购IPTV'              ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON.IPTV_NUM is 'IPTV点播次数'               ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON.KD_RATE is '家宽产品速率'                ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON.USER_ONLINE_DURA is '在网时长'           ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON.HEYUE_DUR is '合约时长'                  ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON.FAMILY_PRO_NUM is '智慧家庭产品使用个数' ;




DELETE FROM $db_name.TB_FCUST_VALUE_CAL_MON WHERE op_time=$TX_MONTH;

alter session force parallel dml

INSERT /*+append parallel(t,4) nologging*/ INTO $db_name.TB_FCUST_VALUE_CAL_MON 
SELECT
	 op_time 	
	,user_id 	
	,acc_nbr 	
	,BILL_FLAG 	
	,CASE WHEN TOT_INCOME		<30   THEN 30	WHEN TOT_INCOME	>150	THEN 150	ELSE TOT_INCOME	END
	,CASE WHEN USE_TIME		<120  THEN 120 	WHEN USE_TIME	>750 	THEN 750 	ELSE USE_TIME	END
	,CASE WHEN FUSE_NUM <=1  	THEN 1 	
	      WHEN FUSE_NUM = 2  	THEN 3  	
	      WHEN FUSE_NUM >=3		THEN 4
	      ELSE 			     0  	
	END AS FUSE_NUM
	,CASE WHEN IPTV_FLAG 		<0  THEN 0 	WHEN IPTV_FLAG 	>1   	THEN 1   	ELSE IPTV_FLAG 	END
	,CASE WHEN IPTV_NUM IS NULL  	    THEN 0  WHEN IPTV_NUM	>100	THEN 100	ELSE IPTV_NUM	END 
	,CASE WHEN regexp_substr(KD_RATE,'[0-9]+')>=0 and regexp_substr(KD_RATE,'[0-9]+')<10	THEN to_char('1')
	      WHEN regexp_substr(KD_RATE,'[0-9]+')>=10 and regexp_substr(KD_RATE,'[0-9]+')<20	THEN to_char('1')
	      WHEN regexp_substr(KD_RATE,'[0-9]+')>=20 and regexp_substr(KD_RATE,'[0-9]+')<30	THEN to_char('1')
	      WHEN regexp_substr(KD_RATE,'[0-9]+')>=30 and regexp_substr(KD_RATE,'[0-9]+')<50	THEN to_char('1')
	      WHEN regexp_substr(KD_RATE,'[0-9]+')>=50 and regexp_substr(KD_RATE,'[0-9]+')<100	THEN to_char('2')
	      WHEN regexp_substr(KD_RATE,'[0-9]+')>=100 and regexp_substr(KD_RATE,'[0-9]+')<200	THEN to_char('5')
	      WHEN regexp_substr(KD_RATE,'[0-9]+')>=200 		THEN to_char('6')
	      ELSE				     to_char('0')
	END AS KD_RATE
	,CASE WHEN USER_ONLINE_DURA>=0  AND USER_ONLINE_DURA < 12	THEN 1
	      WHEN USER_ONLINE_DURA>=12 AND USER_ONLINE_DURA < 24	THEN 2
	      WHEN USER_ONLINE_DURA>=24 AND USER_ONLINE_DURA < 36	THEN 3
	      WHEN USER_ONLINE_DURA>=36 AND USER_ONLINE_DURA < 48	THEN 4
	      WHEN USER_ONLINE_DURA>=48 AND USER_ONLINE_DURA < 60	THEN 5
	      WHEN USER_ONLINE_DURA>=60			THEN 6
	      ELSE				     0
	END AS USER_ONLINE_DURA
	,CASE WHEN HEYUE_DUR>=0  AND HEYUE_DUR<6	THEN 1
	      WHEN HEYUE_DUR>=6  AND HEYUE_DUR<12	THEN 1
	      WHEN HEYUE_DUR>=12 AND HEYUE_DUR<18	THEN 5
	      WHEN HEYUE_DUR>=18 AND HEYUE_DUR<24	THEN 5
	      WHEN HEYUE_DUR>=24 AND HEYUE_DUR<36	THEN 6
	      WHEN HEYUE_DUR>=36 		THEN 6
	      ELSE				     0
	END AS HEYUE_DUR
	,CASE WHEN FAMILY_PRO_NUM IS NULL	THEN 0	WHEN FAMILY_PRO_NUM>2   THEN 2   	ELSE FAMILY_PRO_NUM	END
FROM 	$db_name.TB_FCUST_VALUE_BASE_MON
WHERE	op_time=$TX_MONTH;

alter session disable parallel dml

select 46,sysdate from dual;

----3、零壹标准化【0.5~1】
DROP TABLE 	$db_name.TB_FCUST_VALUE_CAL_MON_STD;
CREATE  TABLE $db_name.TB_FCUST_VALUE_CAL_MON_STD
(
	op_time	INTEGER
,user_id	number(16,0)
,acc_nbr	VARCHAR(40)
,BILL_FLAG	VARCHAR(2)
,TOT_INCOME	FLOAT
,USE_TIME	FLOAT
,FUSE_NUM	FLOAT
,IPTV_FLAG	FLOAT
,IPTV_NUM	FLOAT
,KD_RATE	FLOAT
,USER_ONLINE_DURA	FLOAT
,HEYUE_DUR	FLOAT
,FAMILY_PRO_NUM	FLOAT
);
comment on column $db_name.TB_FCUST_VALUE_CAL_MON_STD.op_time is '处理日期'                    ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON_STD.user_id is '用户id'                      ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON_STD.acc_nbr is '用户号码'                    ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON_STD.BILL_FLAG is '出账标识'                  ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON_STD.TOT_INCOME is '月总收入(元)'             ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON_STD.USE_TIME is '家宽产品使用时长(小时)'     ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON_STD.FUSE_NUM is '融合的用户数量'             ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON_STD.IPTV_FLAG is 'IPTV是否订购'              ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON_STD.IPTV_NUM is 'IPTV点播次数'          ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON_STD.KD_RATE is '家宽产品速率'                ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON_STD.USER_ONLINE_DURA is '在网时长'           ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON_STD.HEYUE_DUR is '合约时长'                  ;
comment on column $db_name.TB_FCUST_VALUE_CAL_MON_STD.FAMILY_PRO_NUM is '智慧家庭产品使用个数' ;


DELETE FROM $db_name.TB_FCUST_VALUE_CAL_MON_STD WHERE op_time=$TX_MONTH;

alter session force parallel dml

INSERT /*+append parallel(t,4) nologging*/ INTO	$db_name.TB_FCUST_VALUE_CAL_MON_STD
SELECT	op_time 	
	,user_id 	
	,acc_nbr 	
	,BILL_FLAG 	
	,decode((MAX_TOT_INCOME  	- MIN_TOT_INCOME  	),0,0.5,0.5 + 0.5 * COALESCE((CAST(TOT_INCOME  	AS FLOAT)	- MIN_TOT_INCOME  	) / (MAX_TOT_INCOME  	- MIN_TOT_INCOME  	), 0)) AS TOT_INCOME 
  ,decode((MAX_USE_TIME  	- MIN_USE_TIME  	),0,0.5,0.5 + 0.5 * COALESCE((CAST(USE_TIME  	AS FLOAT)	- MIN_USE_TIME  	) / (MAX_USE_TIME  	- MIN_USE_TIME  	), 0)) AS USE_TIME 
	,decode((MAX_FUSE_NUM  	- MIN_FUSE_NUM  	),0,0.5,0.5 + 0.5 * COALESCE((CAST(IPTV_FLAG 	AS FLOAT)	- MIN_IPTV_FLAG 	) / (MAX_IPTV_FLAG 	- MIN_IPTV_FLAG 	), 0)) AS FUSE_NUM
	,decode((MAX_IPTV_FLAG 	- MIN_IPTV_FLAG 	),0,0.5,0.5 + 0.5 * COALESCE((CAST(IPTV_FLAG 	AS FLOAT)	- MIN_IPTV_FLAG 	) / (MAX_IPTV_FLAG 	- MIN_IPTV_FLAG 	), 0)) AS IPTV_FLAG 
	,decode((MAX_IPTV_NUM 	- MIN_IPTV_NUM 	),0,0.5,0.5 + 0.5 * COALESCE((CAST(IPTV_NUM 	AS FLOAT)	- MIN_IPTV_NUM 	) / (MAX_IPTV_NUM 	- MIN_IPTV_NUM 	), 0)) AS IPTV_NUM 
	,decode((MAX_KD_RATE   	- MIN_KD_RATE   	),0,0.5,0.5 + 0.5 * COALESCE((CAST(KD_RATE   	AS FLOAT)	- MIN_KD_RATE   	) / (MAX_KD_RATE   	- MIN_KD_RATE   	), 0)) AS KD_RATE
	,decode((MAX_USER_ONLINE_DURA  	- MIN_USER_ONLINE_DURA  	),0,0.5,0.5 + 0.5 * COALESCE((CAST(USER_ONLINE_DURA  	AS FLOAT)	- MIN_USER_ONLINE_DURA  	) / (MAX_USER_ONLINE_DURA  	- MIN_USER_ONLINE_DURA  	), 0)) AS USER_ONLINE_DURA 
	,decode((MAX_HEYUE_DUR 	- MIN_HEYUE_DUR 	),0,0.5,0.5 + 0.5 * COALESCE((CAST(HEYUE_DUR 	AS FLOAT)	- MIN_HEYUE_DUR 	) / (MAX_HEYUE_DUR 	- MIN_HEYUE_DUR 	), 0)	) AS HEYUE_DUR
	,decode((MAX_FAMILY_PRO_NUM	- MIN_FAMILY_PRO_NUM	),0,0.5,0.5 + 0.5 * COALESCE((CAST(FAMILY_PRO_NUM	AS FLOAT)	- MIN_FAMILY_PRO_NUM  	) / (MAX_FAMILY_PRO_NUM	- MIN_FAMILY_PRO_NUM	), 0)) AS FAMILY_PRO_NUM
FROM 	$db_name.TB_FCUST_VALUE_CAL_MON	A
,(
SELECT	 MAX(TOT_INCOME  	) AS MAX_TOT_INCOME  	 
	,MAX(USE_TIME  	) AS MAX_USE_TIME  	
	,MAX(FUSE_NUM  	) AS MAX_FUSE_NUM  	 
	,MAX(IPTV_FLAG 	) AS MAX_IPTV_FLAG 	
	,MAX(IPTV_NUM 	) AS MAX_IPTV_NUM 	
	,MAX(KD_RATE   	) AS MAX_KD_RATE   	
	,MAX(USER_ONLINE_DURA  	) AS MAX_USER_ONLINE_DURA  	 
	,MAX(HEYUE_DUR 	) AS MAX_HEYUE_DUR 	
	,MAX(FAMILY_PRO_NUM	) AS MAX_FAMILY_PRO_NUM	 
	,MIN(TOT_INCOME  	) AS MIN_TOT_INCOME  	 
	,MIN(USE_TIME  	) AS MIN_USE_TIME  	
	,MIN(FUSE_NUM  	) AS MIN_FUSE_NUM  	 
	,MIN(IPTV_FLAG 	) AS MIN_IPTV_FLAG 	
	,MIN(IPTV_NUM 	) AS MIN_IPTV_NUM	
	,MIN(KD_RATE   	) AS MIN_KD_RATE   	
	,MIN(USER_ONLINE_DURA  	) AS MIN_USER_ONLINE_DURA  	 
	,MIN(HEYUE_DUR 	) AS MIN_HEYUE_DUR 	
	,MIN(FAMILY_PRO_NUM	) AS MIN_FAMILY_PRO_NUM	
FROM	$db_name.TB_FCUST_VALUE_CAL_MON
WHERE	op_time=$TX_MONTH
)				B
WHERE	op_time=$TX_MONTH;

alter session disable parallel dml

------家庭宽带用户+特殊字符处理-------------------
drop table $db_name.ndxs_temp_base_kd_user;
CREATE  TABLE $db_name.ndxs_temp_base_kd_user AS(	
select
    t1.OP_TIME
   ,T1.USER_ID
   ,t1.ACC_NBR
   ,case when t1.BILL_FLAG='1' then '1' else '0' end   BILL_FLAG
   ,case when t1.BRD_LINE_RATE='512K->2M' then to_char('2M')
         when t1.BRD_LINE_RATE='4M->50M'  then to_char('50M')   
         when t1.BRD_LINE_RATE='4M->100M' then to_char('100M')
         when t1.BRD_LINE_RATE='2M->4M'   then to_char('4M')
         when t1.BRD_LINE_RATE='2M->20M'  then to_char('20M')
         when t1.BRD_LINE_RATE='1M->4M'   then to_char('4M')
         when t1.BRD_LINE_RATE='1M->2M'   then to_char('2M')
         when t1.BRD_LINE_RATE is null or t1.BRD_LINE_RATE=','   then to_char('2M')
         when t1.BRD_LINE_RATE='4M上'     then to_char('4M')
         when t1.BRD_LINE_RATE='100'      then to_char('100M')
         when t1.BRD_LINE_RATE='200'      then to_char('200M')
         when t1.BRD_LINE_RATE like '%M%' or t1.BRD_LINE_RATE like '%m%' then to_char(t1.BRD_LINE_RATE)
         when t1.BRD_LINE_RATE like '%K%' or t1.BRD_LINE_RATE like '%k%' then to_char(round((case when substr(t1.BRD_LINE_RATE,1,length(t1.BRD_LINE_RATE)-1)/1024 <=2 then 2 else substr(t1.BRD_LINE_RATE,1,length(t1.BRD_LINE_RATE)-1)/1024  end ),0)||'M')  
         else to_char('2M') end  BRD_LINE_RATE  --"宽带速率"        
        -- else cast(substr(t1.BRD_LINE_RATE,1,length(t1.BRD_LINE_RATE)-1) as number(4,0)) end  BRD_LINE_RATE   --"宽带速率"
   ,t1.FIX_BRD_DURA*1.00/60/60   FIX_BRD_DURA  --  "本月累计固网宽带时长"
   ,t1.FIX_BRD_FLUX*1.00/1024/1024      FIX_BRD_FLUX  --   "本月累计固网宽带流量"
   ,t1.USER_ONLINE_DURA
   ,case when t1.YPAY_MAIN_PP_ID ='0' then 1
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%宽带续1年套餐-赠送1个月%' then 13
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%宽带续2年套餐-赠送3个月%' then 27
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%一年送半年%' then 18
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%买12个月送12个%' then 24
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%买24个月送24个%' then 48
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%买6个月送6个%'   then 12
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%包年%'  then 12
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%(半年%' then 6
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%一年%' then 12
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%二年%' then 24
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%三年%' then 36
         else 1 end      HEYUE_DUR
   ,count(distinct t2.USER_ID)   NUM
   ,case when (count(distinct t2.USER_ID))<1 then 1 else count(distinct t2.USER_ID) end   NUMS
   ,sum(case when t1.COM_PROD_INST_ID is null then t1.FEE else t2.FEE end)/100      FEE
   from edw_share.s_td_user_360_$TX_MONTH t1
   left join edw_share.s_td_user_360_$TX_MONTH t2
   on t1.COM_PROD_INST_ID=t2.COM_PROD_INST_ID
   and t2.OP_TIME=$TX_MONTH
   and t2.INNET_FLAG = 1  
   left join dim.d_price_plan t3
   on t1.YPAY_MAIN_PP_ID=t3.PRICE_PLAN_CD
   where t1.INNET_FLAG = 1          --在网
   and t1.MAIN_PROD_ID2 = '21'      --固网宽带
   and t1.OP_TIME =$TX_MONTH
 group by 
    t1.OP_TIME
   ,T1.USER_ID
   ,t1.ACC_NBR
   ,case when t1.BILL_FLAG='1' then '1' else '0' end   
   ,case when t1.BRD_LINE_RATE='512K->2M' then to_char('2M')
         when t1.BRD_LINE_RATE='4M->50M'  then to_char('50M')   
         when t1.BRD_LINE_RATE='4M->100M' then to_char('100M')
         when t1.BRD_LINE_RATE='2M->4M'   then to_char('4M')
         when t1.BRD_LINE_RATE='2M->20M'  then to_char('20M')
         when t1.BRD_LINE_RATE='1M->4M'   then to_char('4M')
         when t1.BRD_LINE_RATE='1M->2M'   then to_char('2M')
         when t1.BRD_LINE_RATE is null or t1.BRD_LINE_RATE=','   then to_char('2M')
         when t1.BRD_LINE_RATE='4M上'     then to_char('4M')
         when t1.BRD_LINE_RATE='100'      then to_char('100M')
         when t1.BRD_LINE_RATE='200'      then to_char('200M')
         when t1.BRD_LINE_RATE like '%M%' or t1.BRD_LINE_RATE like '%m%' then to_char(t1.BRD_LINE_RATE)
         when t1.BRD_LINE_RATE like '%K%' or t1.BRD_LINE_RATE like '%k%' then to_char(round((case when substr(t1.BRD_LINE_RATE,1,length(t1.BRD_LINE_RATE)-1)/1024 <=2 then 2 else substr(t1.BRD_LINE_RATE,1,length(t1.BRD_LINE_RATE)-1)/1024  end ),0)||'M')  
         else to_char('2M') end    --"宽带速率"        
   ,t1.FIX_BRD_DURA*1.00/60/60     --  "本月累计固网宽带时长"
   ,t1.FIX_BRD_FLUX*1.00/1024/1024        --   "本月累计固网宽带流量"
   ,t1.USER_ONLINE_DURA
   ,case when t1.YPAY_MAIN_PP_ID ='0' then 1
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%宽带续1年套餐-赠送1个月%' then 13
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%宽带续2年套餐-赠送3个月%' then 27
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%一年送半年%' then 18
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%买12个月送12个%' then 24
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%买24个月送24个%' then 48
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%买6个月送6个%'   then 12
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%包年%'  then 12
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%(半年%' then 6
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%一年%' then 12
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%二年%' then 24
         when t1.YPAY_MAIN_PP_ID<>'0' and t3.name like '%三年%' then 36
         else 1 end   
);

select 47,sysdate from dual;

------智慧家庭产品
drop table $db_name.ndxs_temp_family_pro;
CREATE  TABLE $db_name.ndxs_temp_family_pro AS(	
select
 USER_ID 
,price_plan_cd
from integ.i_u_user_price_m
where user_id in (select USER_ID  from $db_name.ndxs_temp_kd_user where OP_TIME=$TX_MONTH group by USER_ID)
and OP_TIME=$TX_MONTH
   and (TRIM(EXTRACT(YEAR FROM start_dt))
       ||SUBSTR(TO_CHAR(start_dt,'yyyy-mm'),6,7))<=$TX_MONTH
   and (TRIM(EXTRACT(YEAR FROM end_dt))
       ||SUBSTR(TO_CHAR(start_dt,'yyyy-mm'),6,7))>=$TX_MONTH
and price_plan_cd in (993033986,993033966,993033987,993033967)
group by  USER_ID ,price_plan_cd
);


-------绑定IPTV用户
drop table $db_name.ndxs_temp_base_iptv_user;
CREATE  TABLE $db_name.ndxs_temp_base_iptv_user AS(
SELECT
T1.USER_ID                --固宽带网ID
FROM edw_share.s_td_user_360_$TX_MONTH T1
inner JOIN edw_share.s_td_user_360_$TX_MONTH T2
on T1.CUST_ID=T2.CUST_ID
AND T1.ACCT_ID = T2.ACCT_ID
AND T1.COM_PROD_INST_ID = T2.COM_PROD_INST_ID
AND T1.USER_ID<>T2.USER_ID
WHERE T1.COM_PROD_INST_ID IS NOT NULL --融合产品
and T2.COM_PROD_INST_ID IS NOT NULL
AND T1.INNET_FLAG = 1  --在网标识,融合产品只找在网的，不在网的没有意义
and T2.INNET_FLAG = 1
AND t1.OP_TIME = $TX_MONTH
and t2.OP_TIME = $TX_MONTH
and (T1.MAIN_PROD_ID2=21 or t1.MAIN_PROD_ID in (20130060,80000030,80000031,80000032))
and t1.MAIN_PROD_ID<>10023
AND T2.MAIN_PROD_ID in (600000293)
group by T1.USER_ID                     
);


-------汇总家庭宽带基础表
delete  from $db_name.TB_FCUST_VALUE_BASE_MON where OP_TIME=$TX_MONTH;

INSERT INTO $db_name.TB_FCUST_VALUE_BASE_MON
select
 t1.OP_TIME
,t1.USER_ID
,t1.ACC_NBR
,t1.BILL_FLAG
,nvl(t1.fee,0)
,nvl(t1.FIX_BRD_DURA,0)
,nvl(t1.FIX_BRD_FLUX,0)
,nvl(t1.NUMS,0)
,case when t2.USER_ID is not null then 1 else 0 end  
,null
,to_char(nvl(t1.BRD_LINE_RATE,0))
,nvl(t1.USER_ONLINE_DURA,0 )
,nvl(t1.HEYUE_DUR,0)
,null
FROM $db_name.ndxs_temp_base_kd_user t1
left join $db_name.ndxs_temp_base_iptv_user t2
on t1.USER_ID=t2.USER_ID
where t1.OP_TIME=$TX_MONTH
;

select 48,sysdate from dual;
----5、家庭客户价值指数结果表-基于零壹标准化【0.5~1】
DROP TABLE $db_name.TB_FCUST_VALUE_RES_MON;
CREATE  TABLE $db_name.TB_FCUST_VALUE_RES_MON
(
op_time	INTEGER
,user_id	number(16,0)
,acc_nbr	VARCHAR(40)
,BILL_FLAG	VARCHAR(2)
,SELL_DEPT_NAME	VARCHAR(20)
,TOT_INCOME	number(10,6)
,USE_TIME	number(10,6)
,FUSE_NUM	number(10,6)
,IPTV_FLAG	number(10,6)
,IPTV_NUM	number(10,6)
,KD_RATE	number(10,6)
,USER_ONLINE_DURA	number(10,6)
,HEYUE_DUR	number(10,6)
,FAMILY_PRO_NUM	number(10,6)
,X_INCOME	number(10,6)
,X_BUS_VALUE	number(10,6)
,Y_USER_FEATURE	number(10,6)
,Y_COM_TOUCH	number(10,6)
,Y_INTEL_FAMILY	number(10,6)
,X_VALUE	number(10,6)
,Y_VALUE	number(10,6)
,VALUE_INDEX	number(10,6)
);
comment on column $db_name.TB_FCUST_VALUE_RES_MON.op_time is '处理日期'                     ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.user_id is '用户id'                       ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.acc_nbr is '用户号码'                     ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.BILL_FLAG is '出账标识'                   ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.SELL_DEPT_NAME is '销售部门'              ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.TOT_INCOME is '月总收入(元)'              ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.USE_TIME is '家宽产品使用时长(小时)'      ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.FUSE_NUM is '融合的用户数量'              ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.IPTV_FLAG is 'IPTV是否订购'               ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.IPTV_NUM is 'IPTV点播次数'                ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.KD_RATE is '家宽产品速率'                 ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.USER_ONLINE_DURA is '在网时长'            ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.HEYUE_DUR is '合约时长'                   ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.FAMILY_PRO_NUM is '智慧家庭产品使用个数'  ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.X_INCOME is '显性-直接收入指数'           ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.X_BUS_VALUE is '显性-业务价值指数'        ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.Y_USER_FEATURE is '隐性-用户属性指数'     ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.Y_COM_TOUCH is '隐性-企业接触指数'        ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.Y_INTEL_FAMILY is '隐性-智慧家庭指数'     ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.X_VALUE is '显性价值指数'                 ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.Y_VALUE is '隐性价值指数'                 ;
comment on column $db_name.TB_FCUST_VALUE_RES_MON.VALUE_INDEX is '价值指数'                 ;  


DELETE FROM $db_name.TB_FCUST_VALUE_RES_MON WHERE op_time=$TX_MONTH;

INSERT INTO $db_name.TB_FCUST_VALUE_RES_MON
SELECT
	 T1.op_time 
	,T1.user_id 
	,T1.acc_nbr 
	,T1.BILL_FLAG 
	,T2.DEPT_NAME
	,TOT_INCOME  	* 20	XIANXING_SHOURU1
	,USE_TIME  		* 10	XIANXING_YEWU1
	,FUSE_NUM  		* 15	XIANXING_YEWU2
	,IPTV_FLAG		* 5 	XIANXING_YEWU3
	,IPTV_NUM	* 5 	XIANXING_YEWU4
	,KD_RATE   		* 20	YINXING_SHUXING1 
	,USER_ONLINE_DURA  		* 10	YINXING_SHUXING2 
	,HEYUE_DUR 		* 10	YINXING_JIECHU1
	,FAMILY_PRO_NUM	* 5 	YINXING_ZHIHUI1 
	,TOT_INCOME  	* 20 	XIANXING_SHOURU
	,(USE_TIME  		* 10	)	+ (FUSE_NUM  		* 15)	+ (IPTV_FLAG		* 5 )	+ (IPTV_NUM	* 5)	XIANXING_YEWU
	,(KD_RATE   		* 20)	+ (USER_ONLINE_DURA  		* 10)	YINXING_SHUXING
	,HEYUE_DUR 		* 10 	YINXING_JIECHU
	,FAMILY_PRO_NUM	* 5	YINXING_ZHIHUI
	,(TOT_INCOME  	* 20)	+ ((USE_TIME  		* 10	)	+ (FUSE_NUM  		* 15)	+ (IPTV_FLAG		* 5 )	+ (IPTV_NUM	* 5))	XIANXING
	,((KD_RATE   		* 20)	+ (USER_ONLINE_DURA  		* 10))	+ (HEYUE_DUR 		* 10 )	+ (FAMILY_PRO_NUM	* 5)	YINXING
	,((TOT_INCOME  	* 20)	+ ((USE_TIME  		* 10	)	+ (FUSE_NUM  		* 15)	+ (IPTV_FLAG		* 5 )	+ (IPTV_NUM	* 5)))		+ (((KD_RATE   		* 20)	+ (USER_ONLINE_DURA  		* 10))	+ (HEYUE_DUR 		* 10 )	+ (FAMILY_PRO_NUM	* 5))		JIAZHI
FROM 	$db_name.TB_FCUST_VALUE_CAL_MON_STD	T1
LEFT JOIN	$db_name.TB_USER_SELL_DEPT_MON	T2
ON	T1.op_time=T2.op_time AND T1.user_id=T2.USER_ID
WHERE	T1.op_time=$TX_MONTH;

select 49,sysdate from dual;
---------------------------5、家庭客户价值指数结果表 结束----

-------SQL 结束------------------------------------

	 exit;
eof
		};
	 print $result;
}


######################################################################
# Connect to DB by odbc method
######################################################################
sub DBconnect()
{
    my $connectString;
    my $DSOURCE = ${AUTO_DSN};
    my $OS   = $^O;
    $OS =~ tr [A-Z][a-z];
    if ( $OS eq "mswin32" || $OS eq "aix" || $OS eq "hpux" || $OS eq "linux") {
        $connectString = "dbi:Oracle:${DSOURCE}";
    }else{
        $connectString = "dbi:Oracle:${DSOURCE}";
    }
    #print "\$connectString is $connectString\n";
    open(LOGONFILE_H, "${LOGON_FILE}");
    my $LOGON_STR = <LOGONFILE_H>;
    close(LOGONFILE_H);
    $LOGON_STR = `${AUTO_HOME}/bin/IceCode.exe "$LOGON_STR"`;
    my ($logoncmd, $userpw) = split(' ',$LOGON_STR);
    chop($userpw);
    my ($USER, $PASSWD) = split(',' , $userpw);
    #print "\$USER is $USER\n";
    #print "\$PASSWD is $PASSWD\n";
    my $dbh = DBI->connect($connectString,$USER,$PASSWD);
    return $dbh;
}


sub DBDisconnect
{
   my ($dbh) = @_;

    if(defined($dbh)){
        $dbh->disconnect();
        print "数据库成功连接\n";
    }else{
        print "断开数据库连接：数据库连接为空\n";
    }
}

# main function
###########################################################################
sub main
{
   my $ret;
   open(LOGONFILE_H, "${LOGON_FILE}");
   $LOGON_STR = <LOGONFILE_H>;
   close(LOGONFILE_H);
   my ($logoncmd, $userpw) = split(' ',$LOGON_STR);
   chop($userpw);
   my ($USER, $PASSWD) = split(',' , $userpw);
   
   # Get the decoded logon string
   $LOGON_STR = `${AUTO_HOME}/bin/IceCode.exe "$LOGON_STR"`;

   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);
   my $today = "${year}${mon}${mday}";

   ####the date process function
   $dbh=DBconnect();	#Connect DB
   
   #取当前的天，日+1，月数-1
   my $sqlText ="select to_char(to_date('$TX_DATE' , 'YYYYMMDD'),'YYYYMMDD') from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $DATE_TODAY=$sth->fetchrow();
   $sth->finish();
   $THIS_YEAR_MON1=substr($DATE_TODAY,0,4)."01";
   $THIS_YEAR_FIRSTDAY=substr($DATE_TODAY,0,4)."0101";
   print "DATE_TODAY is $DATE_TODAY\n";

   #取头一天
   my $sqlText ="select to_char(to_date('$TX_DATE' , 'YYYYMMDD')-1,'YYYYMMDD') from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $DATE_TODAY_L1=$sth->fetchrow();
   $sth->finish();
   print "DATE_TODAY_L1 is $DATE_TODAY_L1    \n";

   #取头二天
   my $sqlText ="select to_char(to_date('$TX_DATE' , 'YYYYMMDD')-2,'YYYYMMDD') from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $DATE_TODAY_L2=$sth->fetchrow();
   $sth->finish();
   #print "$DATE_TODAY_L2    \n";

   #取头三天
   my $sqlText ="select to_char(to_date('$TX_DATE' , 'YYYYMMDD')-3,'YYYYMMDD') from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $DATE_TODAY_L3=$sth->fetchrow();
   $sth->finish();
   #print "$DATE_TODAY_L3    \n";

   #取头四天
   my $sqlText ="select to_char(to_date('$TX_DATE' , 'YYYYMMDD')-4,'YYYYMMDD') from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $DATE_TODAY_L4=$sth->fetchrow();
   $sth->finish();
   #print "$DATE_TODAY_L4    \n";

   #取头五天
   my $sqlText ="select to_char(to_date('$TX_DATE' , 'YYYYMMDD')-5,'YYYYMMDD') from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $DATE_TODAY_L5=$sth->fetchrow();
   $sth->finish();
   #print "$DATE_TODAY_L5    \n";

   #取头六天
   my $sqlText ="select to_char(to_date('$TX_DATE' , 'YYYYMMDD')-6,'YYYYMMDD') from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $DATE_TODAY_L6=$sth->fetchrow();
   $sth->finish();
   #print "$DATE_TODAY_L6    \n";
   
   #取本月的第一天
   my $sqlText ="select substr('$DATE_TODAY',1,6)||'01' from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $MONTH_FIRSTDAY=$sth->fetchrow();
   $sth->finish();
   #print "$MONTH_FIRSTDAY    \n";
 
   #取本月的最后一天
   my $sqlText ="select to_char(add_months(to_date(substr('$DATE_TODAY',1,6)||'01','YYYYMMDD'),1) - 1,'YYYYMMDD') from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $MONTH_LASTDAY=$sth->fetchrow();
   $sth->finish();
   #print "$MONTH_LASTDAY    \n";

  #取本月的char的标识方法
   my $sqlText ="select substr('$DATE_TODAY',1,6) from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $TX_MONTH=$sth->fetchrow();
   $sth->finish();
   #print "$TX_MONTH    \n";

   #取当月本天
   my $sqlText ="select to_char(add_months(to_date(substr('$DATE_TODAY',1,6)||'01','YYYYMMDD'),-1),'YYYYMMDD') from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $MONTH_TODAY=$sth->fetchrow();
   $sth->finish();
   #print "MONTH_TODAY    \n";

  #取上月的本天
   my $sqlText ="select to_char(add_months(to_date('$DATE_TODAY','YYYYMMDD'),-2) ,'YYYYMMDD') from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $LAST1MONTH_TODAY=$sth->fetchrow();
   $sth->finish();
   #print "$LAST1MONTH_TODAY    \n";

   #取上月的第一天
   my $sqlText ="select substr('$LAST1MONTH_TODAY',1,6)||'01' from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $LAST1MONTH_FIRSTDAY=$sth->fetchrow();
   $sth->finish();
   #print "$LAST1MONTH_FIRSTDAY    \n";

   #取上月的最后一天
   my $sqlText ="select to_char(add_months(to_date(substr('$LAST1MONTH_TODAY',1,6)||'01','YYYYMMDD'),1) - 1,'YYYYMMDD') from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $LAST1MONTH_LASTDAY=$sth->fetchrow();
   $sth->finish();
   #print "$LAST1MONTH_LASTDAY    \n";

  #取上月的char的标识方法
   my $sqlText ="select substr(to_char(ADD_MONTHS(to_date('$MONTH_TODAY' ,'YYYYMMDD'),-1),'YYYYMMDD'),1,6) from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $LAST1MONTH_CHAR=$sth->fetchrow();
   $sth->finish();
   #print "$LAST1MONTH_CHAR    \n";

   #取上2月的char的标识方法
   my $sqlText ="select substr(to_char(ADD_MONTHS(to_date('$LAST1MONTH_TODAY' ,'YYYYMMDD'),-1),'YYYYMMDD'),1,6) from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $LAST2MONTH_CHAR=$sth->fetchrow();
   $sth->finish();
   #print "$LAST2MONTH_CHAR    \n";

   #取上3月的char的标识方法
   my $sqlText ="select substr(to_char(ADD_MONTHS(to_date('$LAST1MONTH_TODAY' ,'YYYYMMDD'),-2),'YYYYMMDD'),1,6) from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $LAST3MONTH_CHAR=$sth->fetchrow();
   $sth->finish();

   #取上4月的char的标识方法
   my $sqlText ="select substr(to_char(ADD_MONTHS(to_date('$LAST1MONTH_TODAY' ,'YYYYMMDD'),-3),'YYYYMMDD'),1,6) from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $LAST4MONTH_CHAR=$sth->fetchrow();
   $sth->finish();
   #print "$LAST4MONTH_CHAR    \n";

   #取上5月的char的标识方法
   my $sqlText ="select substr(to_char(ADD_MONTHS(to_date('$LAST1MONTH_TODAY' ,'YYYYMMDD'),-4),'YYYYMMDD'),1,6) from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $LAST5MONTH_CHAR=$sth->fetchrow();
   $sth->finish();
   #print "$LAST5MONTH_CHAR    \n";

   #取下月的本天
   my $sqlText ="select to_char(add_months(to_date('$DATE_TODAY','YYYYMMDD'),1),'YYYYMMDD') from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $NEXT1MONTH_TODAY=$sth->fetchrow();
   $sth->finish();
   #print "$NEXT1MONTH_TODAY    \n";

   #取下月的第一天
   my $sqlText ="select substr('$NEXT1MONTH_TODAY',1,6)||'01' from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $NEXT1MONTH_FIRSTDAY=$sth->fetchrow();
   $sth->finish();
   #print "$NEXT1MONTH_FIRSTDAY    \n";

  #取下月的最后一天
   my $sqlText ="select to_char((add_months(to_date(substr('$NEXT1MONTH_TODAY',1,6)||'01','YYYYMMDD'),1) - 1),'YYYYMMDD') from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $NEXT1MONTH_LASTTDAY=$sth->fetchrow();
   $sth->finish();
   #print "$NEXT1MONTH_LASTTDAY    \n";

   #取下月的char的标识方法
   my $sqlText ="select substr('$NEXT1MONTH_TODAY',1,6) from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $NEXT1MONTH_CHAR=$sth->fetchrow();
   $sth->finish();
   #print "$NEXT1MONTH_CHAR    \n";

   #取下2月的char的标识方法
   my $sqlText ="select substr(to_char(ADD_MONTHS(to_date('$NEXT1MONTH_TODAY','YYYYMMDD'),1),'YYYYMMDD'),1,6) from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $NEXT2MONTH_CHAR=$sth->fetchrow();
   $sth->finish();
   #print "$NEXT2MONTH_CHAR    \n";

   #取下3月的char的标识方法
   my $sqlText ="select substr(to_char(ADD_MONTHS(to_date('$NEXT1MONTH_TODAY','YYYYMMDD'),2),'YYYYMMDD'),1,6) from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $NEXT3MONTH_CHAR=$sth->fetchrow();
   $sth->finish();
   #print "$NEXT3MONTH_CHAR    \n";
#
#   #取当年天数的方法
#   my $sqlText ="SELECT DAY_OF_YEAR FROM SYS_CALENDAR.CALENDAR WHERE CALENDAR_DATE=CAST(SUBSTR('$TX_MONTH',1,4)||'1231' AS DATE FORMAT 'YYYYMMDD')";
#   my $sth = $dbh->prepare($sqlText);
#   $sth->execute();
#   $YEARDAYNUM=$sth->fetchrow();
#   $sth->finish();
#
   #取当月天数的方法
   my $sqlText ="select substr('$MONTH_LASTDAY',7,2) from dual";
   my $sth = $dbh->prepare($sqlText);
   $sth->execute();
   $MONDAYNUM=$sth->fetchrow();
   $sth->finish();
   #print "$MONDAYNUM    \n";            }###            }###

   DBDisconnect($dbh);	#Disconnect DB

   # Call bteq command to load data
   $ret = run_bteq_command();
   return $ret;
}

################################################################
# program section

# To see if there is one parameter,
# if there is no parameter, exit program
if ( $#ARGV < 0 ) {
   exit(1);
}

# Get the first argument
$CONTROL_FILE = $ARGV[0];

$TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-8, 8);
if ( substr(${CONTROL_FILE}, length(${CONTROL_FILE})-3, 3) == 'dir' ) {
    $TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 8);
};
print "TX_DATE is $TX_DATE\n";
print "CONTROL_FILE is $CONTROL_FILE\n";
#print "Right here!\n";
#get provId,to decide what source table or target table is
$PROV = substr($CONTROL_FILE,(index($CONTROL_FILE,'_')+2),3);
print "provId is $PROV\n";

open(STDERR, ">&STDOUT");
my $ret = main();

exit($ret);
