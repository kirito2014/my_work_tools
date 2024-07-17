-- 层次表名: 聚合层-积存金账户持仓聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s04_dpsit_gold_acct_positions
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S04_DPSIT_GOLD_ACCT_POSITIONS
--     表中文名：积存金账户持仓聚合表
--     创建日期：2023-12-27 00:00:00
--     主键字段：积存金账号,法人机构编号
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含积存金客户的基本信息，积存账户信息，份额信息，产品信息以及合作机构的信息
--     更新记录：
--         2023-12-27 00:00:00 王穆军 新增SDM映射文档
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S04_DPSIT_GOLD_ACCT_POSITIONS drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 客户内码临时表信息
drop table if exists ${session}.TMP_S04_DPSIT_GOLD_ACCT_POSITIONS_01;

create table ${session}.TMP_S04_DPSIT_GOLD_ACCT_POSITIONS_01 (
      ACCT_NO varchar(30) -- 账号
    , CUST_IN_CD varchar(11) -- 客户内码
    , PT_DT varchar(10) -- 数据日期
)
comment '客户内码临时表信息'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S04_DPSIT_GOLD_ACCT_POSITIONS_01(
      ACCT_NO -- 账号
    , CUST_IN_CD -- 客户内码
    , PT_DT -- 数据日期
)
select
      P1.ACCT_NO as ACCT_NO -- 客户号
    , P1.CUST_INCOD as CUST_IN_CD -- 客户内码
    , '${process_date}' as PT_DT -- None
from
    (
	SELECT DISTINCT 
		trim(U1.AA01AC15) AS ACCT_NO 	--账号
		,trim(U1.AA03CSNO) AS CUST_INCOD 	--客户内码
		--trim(U1.AA04FLNM) AS CUST_NAME	--客户名称
	FROM 
		${ODS_CORE_SCHEMA}.ODS_CORE_BDFMHQAA U1
	WHERE 
		1=1
		AND U1.AA15ZHZT = '1' --正常状态
		AND U1.PT_DT='${process_date}' 
		AND U1.DELETED='0'
		AND TRIM(U1.RCSTRS1B)<>'9' --活期存款主档
	UNION 
	SELECT DISTINCT 
		 trim(U2.CDNOAC19) AS ACCT_NO --账号
		,trim(U2.CINOCSNO) AS CUST_INCOD --客户内码
		--,trim(U2.CHNMNM40) AS CUST_NAME--客户名称
	FROM 
		${ODS_CORE_SCHEMA}.ODS_CORE_BWFMDCIM U2
	WHERE 
		1=1
		AND U2.BSTPBUST = '101' --借记卡
		AND TRIM(U2.CRDSCRDS) IN('1','5') 	--正常状态未销户
		AND U2.PT_DT='${process_date}' 
		AND U2.DELETED='0'
		AND TRIM(U2.RCSTRS1B)<>'9' --（卡片状态1，记录状态9）
	)  as P1 -- None
;
-- ==============字段映射（第2组）==============
-- 积存金账户持仓聚合表

insert into table ${session}.S04_DPSIT_GOLD_ACCT_POSITIONS(
      业务术语待申请 -- 积存金账号
    , LP_ORG_NO -- 法人机构编号
    , CUST_IN_CD -- 客户内码
    , 业务术语待申请 -- 投资人类别代码
    , DOCTYP_CD -- 证件类型代码
    , DOC_NO -- 证件号码
    , 业务术语待申请 -- 银行卡号
    , 业务术语待申请 -- 银行账户编号
    , 业务术语待申请 -- 开卡支行机构编号
    , OPEN_CARD_BNKOUTLS_NO -- 开卡网点编号
    , 业务术语待申请 -- 积存金账户类型代码
    , 业务术语待申请 -- 积存金账户状态代码
    , 业务术语待申请 -- 积存金账户开户日期
    , 业务术语待申请 -- 主动积存份额
    , 业务术语待申请 -- 网络查控已冻结份额
    , 业务术语待申请 -- 积存金赎回份额
    , 业务术语待申请 -- 定期积存份额
    , 业务术语待申请 -- 积存金产品份额
    , 业务术语待申请 -- 份额最后更新日期
    , 业务术语待申请 -- 签约支行机构编号
    , SIGN_BNKOUTLS_NO -- 签约网点编号
    , 业务术语待申请 -- 积存金产品编号
    , 业务术语待申请 -- 积存金产品名称
    , 业务术语待申请 -- 积存金产品状态代码
    , 业务术语待申请 -- 合作银行编号
    , 业务术语待申请 -- 最新积存金价格
    , PT_DT -- 数据日期
)
select
      T1.PRODACC as 业务术语待申请 -- 积存账号（主键）
    , T1.UNIONCODE as LP_ORG_NO -- 农商行代码
    , T2. CUST_IN_CD as CUST_IN_CD -- 客户内码
    , CASE WHEN T3.INVTP IS NULL OR TRIM(T3.INVTP)='' 
     THEN '' 
ELSE COALESCE(TRIM(S1.TARGET_CD_VAL), '@'||TRIM(T3.INVTP),'') END  as 业务术语待申请 -- 投资人类别（0机构1个人）
    , NVL(t4.INVTP,"")||'_'||NVL(t4.CERTIFICATETYPE,"") as DOCTYP_CD -- 投资人类别&证件类型（10-身份证11-护照12-军官证13-士兵证14-回乡证15-户口本16-外国护照17-其它）
    , T4.CERTIFICATENO as DOC_NO -- 证件号码
    , T1.DEPOSITACCT as 业务术语待申请 -- 银行卡号
    , T3.DEPOSITACCOUNT as 业务术语待申请 -- 银行账号
    , T3.CARD_BRCHCODE as 业务术语待申请 -- 开卡支行
    , T3.CARD_SUBBRCHCODE as OPEN_CARD_BNKOUTLS_NO -- 开卡网点
    , T3.ACCT_FLAG as 业务术语待申请 -- 账户类型 0-主卡，1-附卡
    , T3.ACCT_STATUS as 业务术语待申请 -- 积存金账户状态 0-正常，1-冻结中， 2-已冻结， 3- 销户中, 4-已销户
    , T3.INSERTDATE as 业务术语待申请 -- 账户新增日期
    , T1.ACTIONTRANS_VOL as 业务术语待申请 -- 主动积存份额
    , T1.HOLD_FROZENVOL as 业务术语待申请 -- 网络查控已冻结份额
    , T1.REDEEMTRANS_VOL as 业务术语待申请 -- 赎回份额
    , T1.REGULATRANS_VOL as 业务术语待申请 -- 定期积存份额
    , T1.PROD_VOL as 业务术语待申请 -- 产品份额
    , T1.INSERTDATE as 业务术语待申请 -- 最后更新日期
    , T3.REG_BRCHCODE as 业务术语待申请 -- 签约支行
    , T3.REG_SUBBRCHCODE as SIGN_BNKOUTLS_NO -- 签约网点
    , T1.INSERTDATE as 业务术语待申请 -- 产品代码
    , T5.PROD_NAME as 业务术语待申请 -- 产品名称
    , T5.PROD_STATUS as 业务术语待申请 -- 产品状态
    , T1.DISTRIBUTORCODE as 业务术语待申请 -- 合作银行代码
    , T6.AMOUNT as 业务术语待申请 -- 价格金额
    , '${process_date}' as PT_DT -- None
from
    ${ODS_FMS_SCHEMA}.ODS_FMS_GOLD_BALVOL as T1 -- 产品份额表
    LEFT JOIN TMP_S04_DPSIT_GOLD_ACCT_POSITIONS_01 as T2 -- None
    on TRIM(T1.DEPOSITACCT) = T2.ACCT_NO  
    LEFT JOIN ${ODS_FMS_SCHEMA}.ODS_FMS_GOLD_PRODACC as T3 -- 积存账户表
    on T1.PRODACC=T3.PRODACC 
AND  T1.UNIONCODE=T3.UNIONCODE
AND T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN ${ODS_FMS_SCHEMA}.ODS_FMS_GOLD_CUSTINFO as T4 -- 客户信息表
    on T1.CUSTNO=T4.CUSTNO
AND T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN ${ODS_FMS_SCHEMA}.ODS_FMS_GOLD_PLANSET as T5 -- 积存参数信息表
    on T1.PROD_CODE=T5.PROD_CODE 
AND  T1.UNIONCODE=T5.UNIONCODE
AND T5.PT_DT='${process_date}' 
AND T5.DELETED='0' 
    LEFT JOIN ${ODS_FMS_SCHEMA}.ODS_FMS_GOLD_ACCUMULATED_PRICE as T6 -- 积存金价格信息记录表
    on T1.DISTRIBUTORCODE = T6.DISTRIBUTORCODE --todo 确认
AND T1.MODIDATE=T6.PRICETIME
AND T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
    LEFT JOIN 代码表待命名 as S1 -- 代码转换
    on T3.INVTP = S1.SRC_CD_VAL 
AND S1.SRC_SYS_CD = 'FMS'
AND S1.SRC_COL_NAME ='INVTP'
AND S1.SRC_TAB_NAME = 'ODS_FMS_GOLD_PRODACC' 
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;

-- 删除所有临时表
drop table ${session}.TMP_S04_DPSIT_GOLD_ACCT_POSITIONS_01;