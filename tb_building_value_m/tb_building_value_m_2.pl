#!/usr/bin/perl
######################################################################
# BTEQ script in Perl, generate by Script Wizard
# Date Time        : 15:12 2019/1/3
# Target Table     : 
# Script File      : tb_building_value_m.pl
# Interface Name   : 楼宇价值
# Refresh Frequency: monthly
# Refresh Mode     : 增量
# Authors          :
# Version Info     : 1.0
# Source Table     : 
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
 
     my $sqlplus_settings = 'set linesize 300;';
     my $result = qx { sqlplus $connect_string<<eof
     $sqlplus_settings
     -------SQL 开始-----------------------------------
--楼宇价值表 
---------------数据加工
--选取缺失数据较少的
DROP TABLE ${db_name}.TB_BUILDING_WORTH_D;
CREATE  TABLE ${db_name}.TB_BUILDING_WORTH_D compress AS (
SELECT $MONTH_FIRSTDAY  as deal_date,
      AREA_NAME,                                             --行政区
      NAME,                                                  --楼宇名称
      ADDRESS,                                               --楼宇地址 
      CODE,                                                  --楼宇编码
      to_number(REGEXP_REPLACE(ACREAGE,'[^0-9]','')) AS ACREAGE ,                                               --面积  
      to_number(REGEXP_REPLACE(RENTAL,'[^0-9]','')) AS RENTAL,                                                --租金:日租金（训练数据中要乘以天数）
      LIMIT,                                                --使用年限
      CHILD_BUL,                                             --字楼栋数
      BUL_TYPE,                                              --楼宇类型 ：写字楼、商住两用、园区、综合楼
      BUL_LEVEL,                                             --楼宇级别     
      YYS_COUNT,                                             --运营商数量     
      IF_TEL_ONLY,                                           --是否仅电信 
      to_number(REGEXP_REPLACE(COMPANIES,'[^0-9]','')) AS COMPANIES,                                             --可入住企业数 
      TRADE_AREA,                                            --所属商圈   
      AVG_FEE,                                              --月均收入   
      IF_HY_FLAG                                             --是否为行业客户 
FROM ods_share.building_worth_data_$MONTH_FIRSTDAY
WHERE  to_number(REGEXP_REPLACE(ACREAGE,'[^0-9]',''))<>'0'    ----剔除楼宇面积为空或为0
and to_number(REGEXP_REPLACE(RENTAL,'[^0-9]','')) <>'0'----剔除楼宇租金为空或为0
and to_number(REGEXP_REPLACE(COMPANIES,'[^0-9]',''))<>'0' ----剔除入住公司数为空或为0
AND CODE NOT IN ('L20064192952','L20160216170244','L20140416143239')
)
;


--数据处理
DROP TABLE ${db_name}.TB_LOUYU_$MONTH_FIRSTDAY;
CREATE  TABLE ${db_name}.TB_LOUYU_$MONTH_FIRSTDAY compress AS (
SELECT *
FROM ods_share.building_worth_data_$MONTH_FIRSTDAY
WHERE CODE  IN ('L20064192952','L20160216170244','L20140416143239')
)
;


---更新数据
UPDATE ${db_name}.TB_LOUYU_$MONTH_FIRSTDAY
SET  RENTAL=3
    ,YYS_COUNT=3
WHERE CODE ='L20064192952'
;
commit;

UPDATE ${db_name}.TB_LOUYU_$MONTH_FIRSTDAY
SET  ACREAGE=10000
WHERE CODE ='L20140416143239'
;
commit;

UPDATE ${db_name}.TB_LOUYU_$MONTH_FIRSTDAY
SET  ACREAGE=20000
    ,CHILD_BUL=2
    ,RENTAL=2.3
WHERE CODE ='L20160216170244'
;
commit;


INSERT  append nologging INTO ${db_name}.TB_BUILDING_WORTH_D
SELECT $MONTH_FIRSTDAY  as deal_date,
      AREA_NAME,                                             --行政区
      NAME,                                                  --楼宇名称
      ADDRESS,                                               --楼宇地址 
      CODE,                                                  --楼宇编码
      to_number(REGEXP_REPLACE(ACREAGE,'[^0-9]','')) ,        --面积  
      to_number(REGEXP_REPLACE(RENTAL,'[^0-9]',''))  ,        --租金:日租金（训练数据中要乘以天数）
      LIMIT,                                                   --使用年限
      CHILD_BUL,                                             --字楼栋数
      BUL_TYPE,                                              --楼宇类型 ：写字楼、商住两用、园区、综合楼
      BUL_LEVEL,                                             --楼宇级别     
      YYS_COUNT,                                             --运营商数量     
      IF_TEL_ONLY,                                           --是否仅电信 
      to_number(REGEXP_REPLACE(COMPANIES,'[^0-9]','')),                                              --可入住企业数 
      TRADE_AREA,                                            --所属商圈   
      AVG_FEE,                                              --月均收入   
      IF_HY_FLAG                                             --是否为行业客户 
FROM ${db_name}.TB_LOUYU_$MONTH_FIRSTDAY
WHERE  to_number(REGEXP_REPLACE(ACREAGE,'[^0-9]',''))<>'0'    ----剔除楼宇面积为空或为0
and to_number(REGEXP_REPLACE(RENTAL,'[^0-9]','')) <>'0'----剔除楼宇租金为空或为0
and to_number(REGEXP_REPLACE(COMPANIES,'[^0-9]',''))<>'0' ----剔除入住公司数为空或为0 
;
commit;

---看数据是否需要update（43个）

---20个
UPDATE ${db_name}.TB_BUILDING_WORTH_D
SET AREA_NAME='石景山区'
WHERE (AREA_NAME='北京市' OR AREA_NAME IS NULL) 
AND ADDRESS LIKE '%石景山%'
;
commit;

--7个
UPDATE ${db_name}.TB_BUILDING_WORTH_D
SET AREA_NAME='海淀区'
WHERE (AREA_NAME='北京市' OR AREA_NAME IS NULL )
AND ADDRESS LIKE '%海淀区%'
;
commit;

--1个
UPDATE ${db_name}.TB_BUILDING_WORTH_D
SET AREA_NAME='朝阳区'
WHERE (AREA_NAME='北京市' OR AREA_NAME IS NULL )
AND ADDRESS LIKE '%朝阳区%'
;
commit;

--7个
UPDATE ${db_name}.TB_BUILDING_WORTH_D
SET AREA_NAME='门头沟区'
WHERE (AREA_NAME='北京市' OR AREA_NAME IS NULL )
AND ADDRESS LIKE '%门头沟%'
;
commit;

--1个
UPDATE ${db_name}.TB_BUILDING_WORTH_D
SET AREA_NAME='西城区'
WHERE (AREA_NAME='北京市' OR AREA_NAME IS NULL )
AND ADDRESS LIKE '%西城区%'
;
commit;

--1个
UPDATE ${db_name}.TB_BUILDING_WORTH_D
SET AREA_NAME='东城区'
WHERE (AREA_NAME='北京市' OR AREA_NAME IS NULL )
AND ADDRESS LIKE '%东城区%'
;
commit;

--大兴区：1个
UPDATE ${db_name}.TB_BUILDING_WORTH_D
SET AREA_NAME='大兴区'
WHERE CODE='L2008415104351'
;
commit;

--石景山：1个
UPDATE ${db_name}.TB_BUILDING_WORTH_D
SET AREA_NAME='石景山区'
WHERE CODE='L20071916412'
;
commit;

--海淀区:4个
UPDATE ${db_name}.TB_BUILDING_WORTH_D
SET AREA_NAME='海淀区'
WHERE CODE in ('L2006419458','L20064195245','L2006512112312','L2009127102025')
;
commit;
---------------------------------------------
---------------------------------------------
-----导出各区的数据保存到data文件下 例如：predict_chaoyang20180803.txt 修改了日期 后 注意predict.py文件中读取数据也做相应修改
-------训练数据生成
-----chaoyang
DROP TABLE ${db_name}.TB_BUILDING_WORTH_CHAOYANG;
CREATE  TABLE ${db_name}.TB_BUILDING_WORTH_CHAOYANG compress AS (
SELECT
CODE,
CASE WHEN ACREAGE<1000 THEN ACREAGE*10000 ELSE ACREAGE END AS buildArea ,
RENTAL*30 AS rentPrice,
CASE WHEN LIMIT<1 THEN 1 ELSE LIMIT END AS years,
CASE WHEN (CAST(CHILD_BUL AS INTEGER)<1 or CHILD_BUL is null )THEN 1 ELSE CAST(CHILD_BUL AS INTEGER) END AS subNums,
CASE WHEN BUL_TYPE='产业园区' THEN 1
     WHEN BUL_TYPE='商住两用楼' THEN 2
     WHEN BUL_TYPE='写字楼' THEN 3
     WHEN BUL_TYPE='综合楼' THEN 4 ELSE 3 END AS buildType,
CASE WHEN BUL_LEVEL='5A级' THEN 1
     WHEN BUL_LEVEL='甲级' THEN 2
     WHEN BUL_LEVEL='乙级' THEN 3
     WHEN BUL_LEVEL='丙级' THEN 4
     ELSE 5 END AS buildLevel,
      YYS_COUNT as operNums ,
      IF_TEL_ONLY as ifUnique,
      COMPANIES AS inCompNums  ,
case when  TRADE_AREA='国贸商圈' then 1 else 0 end as guomao,
case when  TRADE_AREA='燕莎商圈' then 1 else 0 end as yansha,
case when  TRADE_AREA='劲松商圈' then 1 else 0 end as jingsong,
case when  TRADE_AREA='望京商圈' then 1 else 0 end as wangjing,
case when  TRADE_AREA='东直门商圈' then 1 else 0 end as dongzhimen,
case when  TRADE_AREA='建外soho商圈' then 1 else 0 end as jianwaisoho,
case when  TRADE_AREA='四惠商圈' then 1 else 0 end as sihui,
case when  TRADE_AREA='国展商圈' then 1 else 0 end as guozhan,
case when  TRADE_AREA='奥体商圈' then 1 else 0 end as aoti,
case when  TRADE_AREA='安贞商圈' then 1 else 0 end as anzhen,
CASE WHEN  IF_HY_FLAG IS NULL THEN 0 ELSE IF_HY_FLAG END as ifIndustCus
FROM ${db_name}.TB_BUILDING_WORTH_D
WHERE  AREA_NAME='朝阳区')
;


-----haidian
DROP TABLE ${db_name}.TB_BUILDING_WORTH_haidian;
CREATE  TABLE ${db_name}.TB_BUILDING_WORTH_haidian compress AS (
SELECT
CODE,
CASE WHEN ACREAGE<1000 THEN ACREAGE*10000 ELSE ACREAGE END AS buildArea ,
RENTAL*30 AS rentPrice,
CASE WHEN LIMIT<1 THEN 1 ELSE LIMIT END AS years,
CASE WHEN (CAST(CHILD_BUL AS INTEGER)<1 or CHILD_BUL is null )THEN 1 ELSE CAST(CHILD_BUL AS INTEGER) END AS subNums,
CASE WHEN BUL_TYPE='产业园区' THEN 1
     WHEN BUL_TYPE='商住两用楼' THEN 2
     WHEN BUL_TYPE='写字楼' THEN 3
     WHEN BUL_TYPE='综合楼' THEN 4 ELSE 3 END AS buildType,
CASE WHEN BUL_LEVEL='5A级' THEN 1
     WHEN BUL_LEVEL='甲级' THEN 2
     WHEN BUL_LEVEL='乙级' THEN 3
     WHEN BUL_LEVEL='丙级' THEN 4
     ELSE 5 END AS buildLevel,
    YYS_COUNT   as operNums ,
      IF_TEL_ONLY as ifUnique,
      COMPANIES AS inCompNums  ,
case when  TRADE_AREA='中关村商圈' then 1 else 0 end as zhongguancun,
case when  TRADE_AREA='上地商圈' then 1 else 0 end as shangdi,
case when  TRADE_AREA='公主坟商圈' then 1 else 0 end as gongzhufen,
case when  TRADE_AREA='车道沟商圈' then 1 else 0 end as chedaogou,
CASE WHEN  IF_HY_FLAG IS NULL THEN 0 ELSE IF_HY_FLAG END as ifIndustCus
FROM ${db_name}.TB_BUILDING_WORTH_D
WHERE  AREA_NAME='海淀区')
;

-----dongxi
DROP TABLE ${db_name}.TB_BUILDING_WORTH_dongxi;
CREATE  TABLE ${db_name}.TB_BUILDING_WORTH_dongxi compress AS (
SELECT
CODE,
CASE WHEN ACREAGE<1000 THEN ACREAGE*10000 ELSE ACREAGE END AS buildArea ,
RENTAL*30 AS rentPrice,
CASE WHEN LIMIT<1 THEN 1 ELSE LIMIT END AS years,
CASE WHEN (CAST(CHILD_BUL AS INTEGER)<1 or CHILD_BUL is null )THEN 1 ELSE CAST(CHILD_BUL AS INTEGER) END AS subNums,
CASE WHEN BUL_TYPE='产业园区' THEN 1
     WHEN BUL_TYPE='商住两用楼' THEN 2
     WHEN BUL_TYPE='写字楼' THEN 3
     WHEN BUL_TYPE='综合楼' THEN 4 ELSE 3 END AS buildType,
CASE WHEN BUL_LEVEL='5A级' THEN 1
     WHEN BUL_LEVEL='甲级' THEN 2
     WHEN BUL_LEVEL='乙级' THEN 3
     WHEN BUL_LEVEL='丙级' THEN 4
     ELSE 5 END AS buildLevel,
     YYS_COUNT as operNums ,
     IF_TEL_ONLY as ifUnique,
     COMPANIES AS inCompNums  ,
case when  TRADE_AREA='金融街商圈' then 1 else 0 end as jinrongjie,
case when  TRADE_AREA='西长安街商圈' then 1 else 0 end as xichanganjie,
case when  TRADE_AREA='崇文新世界商圈' then 1 else 0 end as xinshijie,
case when  TRADE_AREA='王府井商圈' then 1 else 0 end as wangfujin,
case when  TRADE_AREA='东四商圈' then 1 else 0 end as dongsi,
case when  TRADE_AREA='安定门商圈' then 1 else 0 end as andingmen,
case when  TRADE_AREA='宣武门商圈' then 1 else 0 end as xuanwumen,
case when  TRADE_AREA='东直门商圈' then 1 else 0 end as dongzhimen,
CASE WHEN  IF_HY_FLAG IS NULL THEN 0 ELSE IF_HY_FLAG END as ifIndustCus
FROM ${db_name}.TB_BUILDING_WORTH_D
WHERE  AREA_NAME in ('西城区','宣武区','东城区','崇文区'))
;

----jiaoqu
DROP TABLE ${db_name}.TB_BUILDING_WORTH_jiaoqu;
CREATE  TABLE ${db_name}.TB_BUILDING_WORTH_jiaoqu compress AS (
SELECT
CODE,
CASE WHEN AREA_NAME='丰台区' THEN 1
     WHEN AREA_NAME IN ('昌平区', '亦庄','大兴区','顺义区','通州区','门头沟区') THEN 2
     WHEN AREA_NAME='石景山区' THEN 3
     WHEN AREA_NAME='密云县' THEN 4
ELSE 5 END as area,
CASE WHEN ACREAGE<1000 THEN ACREAGE*10000 ELSE ACREAGE END AS buildArea ,
RENTAL*30 AS rentPrice,
CASE WHEN LIMIT<1 THEN 1 ELSE LIMIT END AS years,
CASE WHEN (CAST(CHILD_BUL AS INTEGER)<1 or CHILD_BUL is null )THEN 1 ELSE CAST(CHILD_BUL AS INTEGER) END AS subNums,
CASE WHEN BUL_TYPE='产业园区' THEN 1
     WHEN BUL_TYPE='商住两用楼' THEN 2
     WHEN BUL_TYPE='写字楼' THEN 3
     WHEN BUL_TYPE='综合楼' THEN 4 ELSE 3 END AS buildType,
CASE WHEN BUL_LEVEL='5A级' THEN 1
     WHEN BUL_LEVEL='甲级' THEN 2
     WHEN BUL_LEVEL='乙级' THEN 3
     WHEN BUL_LEVEL='丙级' THEN 4
     ELSE 5 END AS buildLevel,
      YYS_COUNT as operNums ,
      IF_TEL_ONLY as ifUnique,
      COMPANIES AS inCompNums  ,
IF_HY_FLAG as ifIndustCus
FROM ${db_name}.TB_BUILDING_WORTH_D
WHERE  AREA_NAME NOT in ('朝阳区','海淀区','西城区','宣武区','东城区','崇文区'))
;



---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
----用python预测

---创建预测表：朝阳区
DROP TABLE ${db_name}.TB_PREDICT_CY_D;
CREATE  TABLE ${db_name}.TB_PREDICT_CY_D (
CODE VARCHAR2(20),
commPrice NUMBER(16,4));
--import将Python生成数据导入


---创建预测表：东西城区
DROP TABLE ${db_name}.TB_PREDICT_DX_D;
CREATE  TABLE ${db_name}.TB_PREDICT_DX_D(
CODE VARCHAR2(20),
commPrice NUMBER(16,4));
--import将Python生成数据导入


---创建预测表：海淀区
DROP TABLE ${db_name}.TB_PREDICT_HD_D;
CREATE  TABLE ${db_name}.TB_PREDICT_HD_D(
CODE VARCHAR2(20),
commPrice NUMBER(16,4));
--import将Python生成数据导入


---创建预测表：郊区
DROP TABLE ${db_name}.TB_PREDICT_JQ_D;
CREATE  TABLE ${db_name}.TB_PREDICT_JQ_D (
CODE VARCHAR2(20),
commPrice NUMBER(16,4));
--import将Python生成数据导入

-----------------------------------------------------------------
-----------------------------------------------------------------
----输出数据
DROP  TABLE ${db_name}.TB_BUILDING_WORTH_YC_D;
CREATE  TABLE ${db_name}.TB_BUILDING_WORTH_YC_D 
(
 CODE          VARCHAR(20) not null,
 PREDICT_FLAG  INTEGER ,
 AREA_NAME     VARCHAR(64),    
 commPrice     NUMBER(16,4),
 KEY_VAR1      VARCHAR(250),
 VALUE1        NUMBER(16,4),
 KEY_VAR2     VARCHAR(250),
 VALUE2        NUMBER(16,4),
 KEY_VAR3      VARCHAR(250),
 VALUE3        NUMBER(16,4),
 KEY_VAR4      VARCHAR(250),
 VALUE4        NUMBER(16,4)
 )
 ;

DELETE FROM ${db_name}.TB_BUILDING_WORTH_YC_D;
commit;
--朝阳
INSERT  append nologging INTO ${db_name}.TB_BUILDING_WORTH_YC_D
SELECT
CODE,
PREDICT_FLAG,
'朝阳区' ,
commPrice,
KEY_VAR1,
ACREAGE AS VALUE1,
KEY_VAR2,
COMPANIES AS VALUE2,
KEY_VAR3,
RENTAL_PRICE AS VALUE3,
KEY_VAR4,
LIMIT AS VALUE4
FROM
(SELECT
A.CODE,
'1' AS PREDICT_FLAG,
A.commPrice,
'楼宇面积(31000.0, 100000.0)'    AS KEY_VAR1,
CASE WHEN B.ACREAGE<1000 THEN B.ACREAGE*10000 ELSE B.ACREAGE END as ACREAGE,
'可入住企业数(67.0, 200.0)'    AS KEY_VAR2,
B.COMPANIES,
'月租金(150.0,285.0)'   AS KEY_VAR3,
B.RENTAL*30 AS RENTAL_PRICE ,
'使用年限(7.0,14.0)'     AS KEY_VAR4,
B.LIMIT
FROM ${db_name}.TB_PREDICT_CY_D A
INNER JOIN ${db_name}.TB_BUILDING_WORTH_D   B
  ON A.CODE=B.CODE
 )A
;
commit;
 

---东西城区
INSERT  append nologging INTO ${db_name}.TB_BUILDING_WORTH_YC_D
SELECT
CODE,
PREDICT_FLAG,
'东西区' ,
commPrice,
KEY_VAR1,
RENTAL_PRICE AS VALUE1,
KEY_VAR2,
ACREAGE AS VALUE2,
KEY_VAR3,
COMPANIES AS VALUE3,
KEY_VAR4,
IF_HY_FLAG AS VALUE4
FROM
(SELECT
A.CODE,
'1' AS PREDICT_FLAG,
A.commPrice,
'月租金(180.0,296.25)'      AS KEY_VAR1,
B.RENTAL*30 AS RENTAL_PRICE ,
'楼宇面积(35500.0,100000.0)'     AS KEY_VAR2,
CASE WHEN B.ACREAGE<1000 THEN B.ACREAGE*10000 ELSE B.ACREAGE END as ACREAGE,
'可入住企业数(30.75,152.25)'   AS KEY_VAR3,
B.COMPANIES ,
'是否有行业客户'        AS KEY_VAR4,
CASE WHEN  IF_HY_FLAG IS NULL THEN 0 ELSE IF_HY_FLAG END as IF_HY_FLAG
FROM ${db_name}.TB_PREDICT_DX_D A
INNER JOIN ${db_name}.TB_BUILDING_WORTH_D   B
  ON A.CODE=B.CODE
 )A
;
commit;

---海淀区
INSERT  append nologging INTO ${db_name}.TB_BUILDING_WORTH_YC_D
SELECT
CODE,
PREDICT_FLAG,
'海淀区' AS AREA_NAME ,
commPrice,
KEY_VAR1,
IF_HY_FLAG AS VALUE1,
KEY_VAR2,
RENTAL_PRICE AS VALUE2,
KEY_VAR3,
COMPANIES AS VALUE3,
KEY_VAR4,
LIMIT AS VALUE4
FROM
(SELECT
A.CODE,
'1' AS PREDICT_FLAG,
A.commPrice,
'是否有行业客户'        AS KEY_VAR1,
CASE WHEN  IF_HY_FLAG IS NULL THEN 0 ELSE IF_HY_FLAG END as IF_HY_FLAG,
'月租金(150.0,225.0)'   AS KEY_VAR2,
B.RENTAL*30 AS RENTAL_PRICE ,
'可入住企业数(40.0,170.0)'     AS KEY_VAR3,
B.COMPANIES ,
'使用年限(9.0,12.0)'         AS KEY_VAR4,
B.LIMIT
FROM ${db_name}.TB_PREDICT_HD_D A
INNER JOIN ${db_name}.TB_BUILDING_WORTH_D   B
  ON A.CODE=B.CODE
 )A
;
commit;

---郊区
INSERT  append nologging INTO ${db_name}.TB_BUILDING_WORTH_YC_D
SELECT
CODE,
PREDICT_FLAG,
'郊区' AS AREA_NAME ,
commPrice,
KEY_VAR1,
RENTAL_PRICE AS VALUE1,
KEY_VAR2,
COMPANIES AS VALUE2,
KEY_VAR3,
CHILD_BUL AS VALUE3,
KEY_VAR4,
ACREAGE AS VALUE4
FROM
(SELECT
A.CODE,
'1' AS PREDICT_FLAG,
A.commPrice,
'月租金(75.0,120.0)'    AS KEY_VAR1,
B.RENTAL*30 AS RENTAL_PRICE ,
'可入住企业数(30.0,109.5)'     AS KEY_VAR2,
B.COMPANIES ,
'楼宇子栋数(1.0,4.0)'         AS KEY_VAR3,
CASE WHEN (CAST(CHILD_BUL AS INTEGER)<1 or CHILD_BUL is null )THEN 1 ELSE CAST(CHILD_BUL AS INTEGER) END AS CHILD_BUL,
'楼宇面积(21800.0,90000.0)'      AS KEY_VAR4,
CASE WHEN B.ACREAGE<1000 THEN B.ACREAGE*10000 ELSE B.ACREAGE END as ACREAGE
FROM ${db_name}.TB_PREDICT_JQ_D A
INNER JOIN ${db_name}.TB_BUILDING_WORTH_D   B
  ON A.CODE=B.CODE
 )A
;
commit;

/*
最后关联更新一下结果表：
如果预测渗透率>100%，则按95%计算，即此时 通信单价=楼宇月收入×95%÷楼宇面积×12。
预测渗透率计算方法为：
   预测渗透率 = 楼宇月收入 ÷ （预测通信单价×楼宇面积×10000÷12）×100%     （通信单价单位是元每平米每年，楼宇面积单位是万米） 
注：楼宇面积如果是小于1000的，则单位是万，需乘以10000.
*/
DROP TABLE ${db_name}.TB_BUILDING_WORTH_YC_CL;
CREATE  TABLE ${db_name}.TB_BUILDING_WORTH_YC_CL compress AS (
SELECT T1.CODE
      ,T1.commPrice AS commPrice1
      ,CASE WHEN T2.ACREAGE<1000 THEN T2.ACREAGE*10000 ELSE T2.ACREAGE END AS buildArea 
      ,T2.AVG_FEE AS BILL_FEE1
      ,T2.AVG_FEE*1.00/(T1.commPrice*CASE WHEN T2.ACREAGE<1000 THEN T2.ACREAGE*10000 ELSE T2.ACREAGE END/12) AS BL_RATE
      ,((T2.AVG_FEE*1.00/0.95)*12)/CASE WHEN T2.ACREAGE<1000 THEN T2.ACREAGE*10000 ELSE T2.ACREAGE END AS commPrice2
FROM ${db_name}.TB_BUILDING_WORTH_YC_D T1
INNER  JOIN ${db_name}.TB_BUILDING_WORTH_D T2
ON T1.CODE=T2.CODE
)
;




####将结果表更新
UPDATE ${db_name}.TB_BUILDING_WORTH_YC_D T1
SET  T1.commPrice =
(SELECT  DISTINCT T2.commPrice2
FROM  ${db_name}.TB_BUILDING_WORTH_YC_CL T2
WHERE T1.CODE=T2.CODE AND T2.BL_RATE>1);
commit;

/*
---输出数据
SELECT CODE AS "楼宇编号"
      ,PREDICT_FLAG AS "是否可预测"
      ,commPrice AS "通信单价"
      ,KEY_VAR1 AS "重要变量1"
      ,VALUE1   AS "当月值1"
      ,KEY_VAR2 AS "重要变量2"
      ,VALUE2   AS "当月值2"
      ,KEY_VAR3 AS "重要变量3"
      ,VALUE3   AS "当月值3"
      ,KEY_VAR4 AS "重要变量4"
      ,VALUE4   AS "当月值4"
FROM ${db_name}.TB_BUILDING_WORTH_YC_D
;
*/

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
   $dbh=DBconnect();    #Connect DB
   
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

   DBDisconnect($dbh);  #Disconnect DB

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

#get provId,to decide what source table or target table is
$PROV = substr($CONTROL_FILE,(index($CONTROL_FILE,'_')+2),3);
print "provId is $PROV\n";

open(STDERR, ">&STDOUT");
my $ret = main();
if($ret eq 1){
    $ret = 0;
}else{
    $ret = 1;
}
exit($ret);