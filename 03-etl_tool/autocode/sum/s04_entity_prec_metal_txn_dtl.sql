-- 层次表名: 聚合层-实物贵金属交易明细聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s04_entity_prec_metal_txn_dtl
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S04_ENTITY_PREC_METAL_TXN_DTL
--     表中文名：实物贵金属交易明细聚合表
--     创建日期：2023-12-26 00:00:00
--     主键字段：APP_FORM_SER_NO
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含所有个人实物贵金属交易明细记录
--     更新记录：
--         2023-12-26 00:00:00 王穆军 新增映射文档
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S04_ENTITY_PREC_METAL_TXN_DTL drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 客户内码临时表信息
drop table if exists ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_00;

create table ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_00 (
      CUST_NO varchar(4) -- 客户号
    , CUST_IN_CD varchar(11) -- 客户内码
    , CUST_NAME varchar(200) -- 客户名称
)
comment '客户内码临时表信息'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_00(
      CUST_NO -- 客户号
    , CUST_IN_CD -- 客户内码
    , CUST_NAME -- 客户名称
)
select
      T1.CUSTNO as CUST_NO -- 客户号
    , COALESCE(P1.CUST_INCOD,T2.BANK_CUST_CODE,NULL) as CUST_IN_CD -- 客户内码
    , COALESCE(P1.CUST_NAME,T2.ACCT_NAME,NULL) as CUST_NAME -- 客户名称
from
    ${ODS_FMS_SCHEMA}.ODS_FMS_GOLD_CUSTINFO as T1 -- 客户信息表
    LEFT JOIN ${ODS_FMS_SCHEMA}.ODS_FMS_COM_BANK_ACCT as T2 -- 交易账号表
    on T1.CUST_NO=T2.CUST_NO
AND T2.ACC_STATUS = '0' --账户状态正常
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN (
	SELECT DISTINCT 
		trim(U1.AA01AC15) AS ACCT_NO 	--账号
		,trim(U1.AA03CSNO) AS CUST_INCOD 	--客户内码
		,trim(U1.AA04FLNM) AS CUST_NAME	--客户名称
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
		,trim(U2.CHNMNM40) AS CUST_NAME--客户名称
	FROM 
		${ODS_CORE_SCHEMA}.ODS_CORE_BWFMDCIM U2
	WHERE 
		1=1
		AND U2.BSTPBUST = '101' --借记卡
		AND TRIM(U2.CRDSCRDS) IN('1','5') 	--正常状态未销户
		AND U2.PT_DT='${process_date}' 
		AND U2.DELETED='0'
		AND TRIM(U2.RCSTRS1B)<>'9' --（卡片状态1，记录状态9）
	)/*  T1 
WHERE 
	EXISTS(
		SELECT 1 FROM(  
						SELECT 
							 P1.CUST_INCOD 	--客户内码
							,P1.CUST_NO 	--客户号
						FROM 
							${ODS_ECIF_SCHEMA}.T2_EN_CUST_BASE_INFO P1 --对公客户信息
						WHERE 	
							P1.PT_DT='${process_date}' 
							AND P1.DELETED='0'
						UNION 
						SELECT 
							 P2.CUST_INCOD 	--客户内码
							,P2.CUST_NO 	--客户号
						FROM 
							${ODS_ECIF_SCHEMA}.T2_IND_CUST_BASE_INFO P2 --对私客户信息
						WHERE P2.PT_DT='${process_date}' 
							AND P2.DELETED='0'
						) T2 
						WHERE T1.CUST_NO = T2.CUST_NO 
							AND T1.CUST_INCOD = T2.CUST_INCOD)  */ as P1 -- None
    on T2.DEPOSIT_ACCT = P1.ACCT_NO 
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第2组）==============
-- 贵金属销售系统_交易流水聚合
drop table if exists ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_01;

create table ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_01 (
      APP_FORM_SER_NO varchar(255) -- 申请单流水号
    , CUST_IN_CD varchar(11) -- 客户内码
    , CUST_NO varchar(4) -- 客户号
    , CUST_NAME varchar(200) -- 客户名称
    , DOCTYP_CD varchar(30) -- 证件类型代码
    , DOC_NO varchar(100) -- 证件号码
    , 业务术语待申请 varchar(4) -- 投资人类型代码
    , LP_ORG_NO varchar(100) -- 法人机构编号
    , TXN_BNKOUTLS_NO varchar(100) -- 交易网点编号
    , 业务术语待申请 varchar(100) -- 交易支行机构编号
    , CURR_CD varchar(30) -- 币种代码
    , APP_AMT decimal(28,2) -- 申请金额
    , ORIG_APP_FORM_NO varchar(100) -- 原申请单编号
    , 业务术语待申请 char(6) -- 实物金业务类型代码
    , 业务术语待申请 varchar(30) -- 黄金公司编号
    , 业务术语待申请 varchar(100) -- 实物金产品代码
    , 业务术语待申请 varchar(100) -- 实物金产品名称
    , PROD_RISK_LVL_CD varchar(30) -- 产品风险等级代码
    , 业务术语待申请 varchar(30) -- 实物金产品状态代码
    , 业务术语待申请 varchar(30) -- 贵金属产品类型代码
    , 业务术语待申请 varchar(30) -- 实物金产品单位代码
    , 业务术语待申请 varchar(30) -- 实物金产品规格
    , RISK_LVL_MATCH_FLAG varchar(10) -- 风险等级匹配标志

    , 业务术语待申请 varchar(10) -- 实物金交易申请状态代码
    , 业务术语待申请 decimal(28,2) -- 成交价格
    , COMM_FEE_RATE decimal(20,7) -- 手续费率
    , ACPTD_MODE_CD varchar(10) -- 实物金业务受理方式代码
    , TXN_DT varchar(10) -- 交易日期
    , TXN_TM varchar(10) -- 交易时间  
    , SUMMARY_DESC varchar(300) -- 摘要描述
    , OPERR_DOC_NO varchar(100) -- 经办人证件号码
    , OPERR_DOCTYP_CD varchar(30) -- 经办人证件类型代码
    , OPERR_NAME varchar(200) -- 经办人名称
    , 业务术语待申请 varchar(200) -- 柜员流水号
    , 业务术语待申请 varchar(200) -- 复核柜员1
    , 业务术语待申请 varchar(200) -- 复核柜员2
    , CUST_MGR_NO varchar(30) -- 客户经理柜员编号
    , 业务术语待申请 decimal(28,2) -- 代理费
    , 业务术语待申请 decimal(8) -- 回购次数
    , 业务术语待申请 decimal(8) -- 回购剩余数量
    , 业务术语待申请 varchar(10) -- 实物金回购类型代码
    , 业务术语待申请 decimal(28,6) -- 基础金价
    , 业务术语待申请 varchar(10) -- 实物金对账状态代码
    , 业务术语待申请 decimal(28,6) -- 清算金额
    , 业务术语待申请 decimal(28,6) -- 清算单价
    , 业务术语待申请 decimal(28,2) -- 加工费
    , 业务术语待申请 varchar(100) -- 资金账号
    , 业务术语待申请 decimal(16,6) -- 最低折扣率
    , FIN_MGR_NO varchar(100) -- 理财经理编号
    , 业务术语待申请 varchar(10) -- 提金标志
    , 业务术语待申请 varchar(300) -- 发票抬头
    , 业务术语待申请 varchar(10) -- 操作日期
    , OPER_TELR_NO varchar(30) -- 操作柜员编号
    , 业务术语待申请 decimal(28,2) -- 其它费
    , 业务术语待申请 varchar(10) -- 实时入账标志
    , 业务术语待申请 decimal(8) -- 交易个数
    , 业务术语待申请 varchar(30) -- 移交接收柜员编号
    , 业务术语待申请 decimal(8) -- 移交次数
    , 业务术语待申请 decimal(28,2) -- VIP优惠金额
    , 业务术语待申请 varchar(10) -- VIP价格模式代码
    , PT_DT  varchar(10) -- 数据日期
)
comment '贵金属销售系统_交易流水聚合'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_01(
      APP_FORM_SER_NO -- 申请单流水号
    , CUST_IN_CD -- 客户内码
    , CUST_NO -- 客户号
    , CUST_NAME -- 客户名称
    , DOCTYP_CD -- 证件类型代码
    , DOC_NO -- 证件号码
    , 业务术语待申请 -- 投资人类型代码
    , LP_ORG_NO -- 法人机构编号
    , TXN_BNKOUTLS_NO -- 交易网点编号
    , 业务术语待申请 -- 交易支行机构编号
    , CURR_CD -- 币种代码
    , APP_AMT -- 申请金额
    , ORIG_APP_FORM_NO -- 原申请单编号
    , 业务术语待申请 -- 实物金业务类型代码
    , 业务术语待申请 -- 黄金公司编号
    , 业务术语待申请 -- 实物金产品代码
    , 业务术语待申请 -- 实物金产品名称
    , PROD_RISK_LVL_CD -- 产品风险等级代码
    , 业务术语待申请 -- 实物金产品状态代码
    , 业务术语待申请 -- 贵金属产品类型代码
    , 业务术语待申请 -- 实物金产品单位代码
    , 业务术语待申请 -- 实物金产品规格
    , RISK_LVL_MATCH_FLAG -- 风险等级匹配标志

    , 业务术语待申请 -- 实物金交易申请状态代码
    , 业务术语待申请 -- 成交价格
    , COMM_FEE_RATE -- 手续费率
    , ACPTD_MODE_CD -- 实物金业务受理方式代码
    , TXN_DT -- 交易日期
    , TXN_TM -- 交易时间  
    , SUMMARY_DESC -- 摘要描述
    , OPERR_DOC_NO -- 经办人证件号码
    , OPERR_DOCTYP_CD -- 经办人证件类型代码
    , OPERR_NAME -- 经办人名称
    , 业务术语待申请 -- 柜员流水号
    , 业务术语待申请 -- 复核柜员1
    , 业务术语待申请 -- 复核柜员2
    , CUST_MGR_NO -- 客户经理柜员编号
    , 业务术语待申请 -- 代理费
    , 业务术语待申请 -- 回购次数
    , 业务术语待申请 -- 回购剩余数量
    , 业务术语待申请 -- 实物金回购类型代码
    , 业务术语待申请 -- 基础金价
    , 业务术语待申请 -- 实物金对账状态代码
    , 业务术语待申请 -- 清算金额
    , 业务术语待申请 -- 清算单价
    , 业务术语待申请 -- 加工费
    , 业务术语待申请 -- 资金账号
    , 业务术语待申请 -- 最低折扣率
    , FIN_MGR_NO -- 理财经理编号
    , 业务术语待申请 -- 提金标志
    , 业务术语待申请 -- 发票抬头
    , 业务术语待申请 -- 操作日期
    , OPER_TELR_NO -- 操作柜员编号
    , 业务术语待申请 -- 其它费
    , 业务术语待申请 -- 实时入账标志
    , 业务术语待申请 -- 交易个数
    , 业务术语待申请 -- 移交接收柜员编号
    , 业务术语待申请 -- 移交次数
    , 业务术语待申请 -- VIP优惠金额
    , 业务术语待申请 -- VIP价格模式代码
    , PT_DT  -- 数据日期
)
select
      T1.APPSHEETSERIALNO as APP_FORM_SER_NO -- 申请单编号
    , T4.CUST_IN_CD as CUST_IN_CD -- 客户内码
    , T1.CUST_NO as CUST_NO -- 客户号
    , COALESCE(T2.CUSTNAME,T4.CUST_NAME) as CUST_NAME -- 投资人名称
    , CASE WHEN T2.CERTIFICATETYPE IS NULL OR TRIM(T2.CERTIFICATETYPE)='' 
     THEN '' 
ELSE COALESCE(TRIM(S1.TARGET_CD_VAL), '@'||TRIM(T2.CERTIFICATETYPE),'') END  as DOCTYP_CD -- 证件类型
    , T2.CERTIFICATENO as DOC_NO -- 证件号码
    , T2.INVTP as 业务术语待申请 -- 投资人类型
    , T1.UNIONCODE as LP_ORG_NO -- 交易发生农商行
    , T1.OPERORG as TXN_BNKOUTLS_NO -- 交易发生网点
    , T1.OPERORGCENTER as 业务术语待申请 -- 交易发生支行
    , 'CNY' as CURR_CD -- 币种
    , T1.TRANS_AMT as APP_AMT -- 交易金额
    , T1.ORIGINALAPPSHEETNO as ORIG_APP_FORM_NO -- 原申请单编号
    , T1.BUSINESSCODE as 业务术语待申请 -- 业务代码
    , T1.GOLD_ORGAN_CODE as 业务术语待申请 -- 黄金公司代码
    , T1.PROD_CODE as 业务术语待申请 -- 产品代码
    , T3.PROD_NAME as 业务术语待申请 -- 产品名称
    , T3.RISKLEVEL as PROD_RISK_LVL_CD -- 风险等级
    , T3.PROD_STATUS as 业务术语待申请 -- 产品状态
    , T3.PROD_TYPE as 业务术语待申请 -- 产品类型
    , T3.PROD_UNIT as 业务术语待申请 -- 产品单位
    , T3.PROD_CONTENT as 业务术语待申请 -- 产品规格
    , T1.RISKMATCHING as RISK_LVL_MATCH_FLAG -- 风险等级是否匹配
    , T1.STATUS as 业务术语待申请 -- 申请状态
    , T1.PRICE as 业务术语待申请 -- 成交价格
    , T1.CASHFEE as COMM_FEE_RATE -- 总手续费
    , T1.ACCEPTMETHOD as ACPTD_MODE_CD -- 受理方式
    , T1.TRANSACTIONDATE as TXN_DT -- 交易发生日期 
    , T1.TRANSACTIONTIME as TXN_TM -- 交易发生时间  
    , T1.SPECIFICATION as SUMMARY_DESC -- 摘要说明
    , T1.TRANSACTORCERTNO as OPERR_DOC_NO -- 经办人证件号码
    , T1.TRANSACTORCERTTYPE as OPERR_DOCTYP_CD -- 经办人证件类型
    , T1.TRANSACTORNAME as OPERR_NAME -- 经办人姓名
    , T1.TELLERSERIALNO as 业务术语待申请 -- 柜员流水号
    , T1.CONFIRMOPID1 as 业务术语待申请 -- 复核柜员1
    , T1.CONFIRMOPID2 as 业务术语待申请 -- 复核柜员2
    , T1.CUSTMANAGERID as CUST_MGR_NO -- 客户经理代码
    , T1.AGENT_FEE as 业务术语待申请 -- 代理费
    , T1.BACK_COUNT as 业务术语待申请 -- 回购次数
    , T1.BACK_NEXT_NUM as 业务术语待申请 -- 回购剩余数量
    , T1.BACKTYPE as 业务术语待申请 -- 回购类型
    , T1.BASE_PRICE as 业务术语待申请 -- 基础金价
    , T1.CHKSTATUS as 业务术语待申请 -- 对账状态
    , T1.CLEAR_AMT as 业务术语待申请 -- 清算金额
    , T1.CLEAR_PRICE as 业务术语待申请 -- 清算单价
    , T1.CONSTRUCT_FEE as 业务术语待申请 -- 加工费
    , T1.DEPOSITACCT as 业务术语待申请 -- 投资人在销售人处用于交易的资金帐号
    , T1.DISCOUNT as 业务术语待申请 -- 最低折扣
    , T1.FINANCINGMANAGERID as FIN_MGR_NO -- 理财经理
    , T1.GETFLAG as 业务术语待申请 -- 是否提金
    , T1.INVOICE_DEAD as 业务术语待申请 -- 发票抬头
    , T1.OPERDATE as 业务术语待申请 -- 操作日期
    , T1.OPERID as OPER_TELR_NO -- 操作柜员
    , T1.OTHER_FEE as 业务术语待申请 -- 其它费
    , T1.REALTIME_FLAG as 业务术语待申请 -- 是否实时入账
    , T1.TRANS_NOS as 业务术语待申请 -- 交易个数
    , T1.TURN_ACCEPT_OPER as 业务术语待申请 -- 移交接收柜员
    , T1.TURN_COUNT as 业务术语待申请 -- 移交次数
    , T1.VIP_AMT as 业务术语待申请 -- VIP优惠金额
    , T1.VIP_MODE as 业务术语待申请 -- VIP价格模式 
    , '${process_date}' as PT_DT  -- None
from
    ${ODS_FMS_SCHEMA}.ODS_FMS_GOLD_TRANSLOG as T1 -- 交易流水表
    LEFT JOIN ${ODS_FMS_SCHEMA}.ODS_FMS_GOLD_CUSTINFO as T2 -- 客户信息表
    on T1.CUST_NO = T2.CUST_NO 
AND T2.PT_DT='${process_date}' 
AND T2.STATUS='0'  --客户状态
AND T2.DELETED='0' 
    LEFT JOIN ${ODS_FMS_SCHEMA}.ODS_FMS_GOLD_PRODINFO as T3 -- 产品信息表
    on T1.UNIONCODE = T3.UNIONCODE
AND T1.GOLD_ORGAN_CODE = T3.GOLD_ORGAN_CODE
AND T1.PROD_CODE = T3.PROD_CODE
AND T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN TMP_S04_ENTITY_PREC_METAL_TXN_DTL_00 as T4 -- None
    on T1.CUST_NO = T4.CUST_NO 
    LEFT JOIN 代码表待命名 as S1 -- 代码转换
    on T2.CERTIFICATETYPE = S1.SRC_CD_VAL 
AND S1.SRC_SYS_CD = 'FMS'
AND S1.SRC_COL_NAME ='CERTIFICATETYPE'
AND S1.SRC_TAB_NAME = 'ODS_FMS_GOLD_CUSTINFO' 
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第3组）==============
-- 贵金属销售系统_历史交易流水聚合
drop table if exists ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_03;

create table ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_03 (
      APP_FORM_SER_NO varchar(255) -- 申请单流水号
    , CUST_IN_CD varchar(11) -- 客户内码
    , CUST_NO varchar(4) -- 客户号
    , CUST_NAME varchar(200) -- 客户名称
    , DOCTYP_CD varchar(30) -- 证件类型代码
    , DOC_NO varchar(100) -- 证件号码
    , 业务术语待申请 varchar(4) -- 投资人类型代码
    , LP_ORG_NO varchar(100) -- 法人机构编号
    , TXN_BNKOUTLS_NO varchar(100) -- 交易网点编号
    , 业务术语待申请 varchar(100) -- 交易支行机构编号
    , CURR_CD varchar(30) -- 币种代码
    , APP_AMT decimal(28,2) -- 申请金额
    , ORIG_APP_FORM_NO varchar(100) -- 原申请单编号
    , 业务术语待申请 char(6) -- 实物金业务类型代码
    , 业务术语待申请 varchar(30) -- 黄金公司编号
    , 业务术语待申请 varchar(100) -- 实物金产品代码
    , 业务术语待申请 varchar(100) -- 实物金产品名称
    , PROD_RISK_LVL_CD varchar(30) -- 产品风险等级代码
    , 业务术语待申请 varchar(30) -- 实物金产品状态代码
    , 业务术语待申请 varchar(30) -- 贵金属产品类型代码
    , 业务术语待申请 varchar(30) -- 实物金产品单位代码
    , 业务术语待申请 varchar(30) -- 实物金产品规格
    , RISK_LVL_MATCH_FLAG varchar(10) -- 风险等级匹配标志

    , 业务术语待申请 varchar(10) -- 实物金交易申请状态代码
    , 业务术语待申请 decimal(28,2) -- 成交价格
    , COMM_FEE_RATE decimal(20,7) -- 手续费率
    , ACPTD_MODE_CD varchar(10) -- 实物金业务受理方式代码
    , TXN_DT varchar(10) -- 交易日期
    , TXN_TM varchar(10) -- 交易时间  
    , SUMMARY_DESC varchar(300) -- 摘要描述
    , OPERR_DOC_NO varchar(100) -- 经办人证件号码
    , OPERR_DOCTYP_CD varchar(30) -- 经办人证件类型代码
    , OPERR_NAME varchar(200) -- 经办人名称
    , 业务术语待申请 varchar(200) -- 柜员流水号
    , 业务术语待申请 varchar(200) -- 复核柜员1
    , 业务术语待申请 varchar(200) -- 复核柜员2
    , CUST_MGR_NO varchar(30) -- 客户经理柜员编号
    , 业务术语待申请 decimal(28,2) -- 代理费
    , 业务术语待申请 decimal(8) -- 回购次数
    , 业务术语待申请 decimal(8) -- 回购剩余数量
    , 业务术语待申请 varchar(10) -- 实物金回购类型代码
    , 业务术语待申请 decimal(28,6) -- 基础金价
    , 业务术语待申请 varchar(10) -- 实物金对账状态代码
    , 业务术语待申请 decimal(28,6) -- 清算金额
    , 业务术语待申请 decimal(28,6) -- 清算单价
    , 业务术语待申请 decimal(28,2) -- 加工费
    , 业务术语待申请 varchar(100) -- 资金账号
    , 业务术语待申请 decimal(16,6) -- 最低折扣率
    , FIN_MGR_NO varchar(100) -- 理财经理编号
    , 业务术语待申请 varchar(10) -- 提金标志
    , 业务术语待申请 varchar(300) -- 发票抬头
    , 业务术语待申请 varchar(10) -- 操作日期
    , OPER_TELR_NO varchar(30) -- 操作柜员编号
    , 业务术语待申请 decimal(28,2) -- 其它费
    , 业务术语待申请 varchar(10) -- 实时入账标志
    , 业务术语待申请 decimal(8) -- 交易个数
    , 业务术语待申请 varchar(30) -- 移交接收柜员编号
    , 业务术语待申请 decimal(8) -- 移交次数
    , 业务术语待申请 decimal(28,2) -- VIP优惠金额
    , 业务术语待申请 varchar(10) -- VIP价格模式代码
    , PT_DT  varchar(10) -- 数据日期
)
comment '贵金属销售系统_历史交易流水聚合'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_03(
      APP_FORM_SER_NO -- 申请单流水号
    , CUST_IN_CD -- 客户内码
    , CUST_NO -- 客户号
    , CUST_NAME -- 客户名称
    , DOCTYP_CD -- 证件类型代码
    , DOC_NO -- 证件号码
    , 业务术语待申请 -- 投资人类型代码
    , LP_ORG_NO -- 法人机构编号
    , TXN_BNKOUTLS_NO -- 交易网点编号
    , 业务术语待申请 -- 交易支行机构编号
    , CURR_CD -- 币种代码
    , APP_AMT -- 申请金额
    , ORIG_APP_FORM_NO -- 原申请单编号
    , 业务术语待申请 -- 实物金业务类型代码
    , 业务术语待申请 -- 黄金公司编号
    , 业务术语待申请 -- 实物金产品代码
    , 业务术语待申请 -- 实物金产品名称
    , PROD_RISK_LVL_CD -- 产品风险等级代码
    , 业务术语待申请 -- 实物金产品状态代码
    , 业务术语待申请 -- 贵金属产品类型代码
    , 业务术语待申请 -- 实物金产品单位代码
    , 业务术语待申请 -- 实物金产品规格
    , RISK_LVL_MATCH_FLAG -- 风险等级匹配标志

    , 业务术语待申请 -- 实物金交易申请状态代码
    , 业务术语待申请 -- 成交价格
    , COMM_FEE_RATE -- 手续费率
    , ACPTD_MODE_CD -- 实物金业务受理方式代码
    , TXN_DT -- 交易日期
    , TXN_TM -- 交易时间  
    , SUMMARY_DESC -- 摘要描述
    , OPERR_DOC_NO -- 经办人证件号码
    , OPERR_DOCTYP_CD -- 经办人证件类型代码
    , OPERR_NAME -- 经办人名称
    , 业务术语待申请 -- 柜员流水号
    , 业务术语待申请 -- 复核柜员1
    , 业务术语待申请 -- 复核柜员2
    , CUST_MGR_NO -- 客户经理柜员编号
    , 业务术语待申请 -- 代理费
    , 业务术语待申请 -- 回购次数
    , 业务术语待申请 -- 回购剩余数量
    , 业务术语待申请 -- 实物金回购类型代码
    , 业务术语待申请 -- 基础金价
    , 业务术语待申请 -- 实物金对账状态代码
    , 业务术语待申请 -- 清算金额
    , 业务术语待申请 -- 清算单价
    , 业务术语待申请 -- 加工费
    , 业务术语待申请 -- 资金账号
    , 业务术语待申请 -- 最低折扣率
    , FIN_MGR_NO -- 理财经理编号
    , 业务术语待申请 -- 提金标志
    , 业务术语待申请 -- 发票抬头
    , 业务术语待申请 -- 操作日期
    , OPER_TELR_NO -- 操作柜员编号
    , 业务术语待申请 -- 其它费
    , 业务术语待申请 -- 实时入账标志
    , 业务术语待申请 -- 交易个数
    , 业务术语待申请 -- 移交接收柜员编号
    , 业务术语待申请 -- 移交次数
    , 业务术语待申请 -- VIP优惠金额
    , 业务术语待申请 -- VIP价格模式代码
    , PT_DT  -- 数据日期
)
select
      T1.APPSHEETSERIALNO as APP_FORM_SER_NO -- 申请单编号
    , T4.CUST_IN_CD as CUST_IN_CD -- 客户内码
    , T1.CUST_NO as CUST_NO -- 客户号
    , COALESCE(T2.CUSTNAME,T4.CUST_NAME) as CUST_NAME -- 投资人名称
    , CASE WHEN T2.CERTIFICATETYPE IS NULL OR TRIM(T2.CERTIFICATETYPE)='' 
     THEN '' 
ELSE COALESCE(TRIM(S1.TARGET_CD_VAL), '@'||TRIM(T2.CERTIFICATETYPE),'') END  as DOCTYP_CD -- 证件类型
    , T2.CERTIFICATENO as DOC_NO -- 证件号码
    , T2.INVTP as 业务术语待申请 -- 投资人类型
    , T1.UNIONCODE as LP_ORG_NO -- 交易发生农商行
    , T1.OPERORG as TXN_BNKOUTLS_NO -- 交易发生网点
    , T1.OPERORGCENTER as 业务术语待申请 -- 交易发生支行
    , 'CNY' as CURR_CD -- 币种
    , T1.TRANS_AMT as APP_AMT -- 交易金额
    , T1.ORIGINALAPPSHEETNO as ORIG_APP_FORM_NO -- 原申请单编号
    , T1.BUSINESSCODE as 业务术语待申请 -- 业务代码
    , T1.GOLD_ORGAN_CODE as 业务术语待申请 -- 黄金公司代码
    , T1.PROD_CODE as 业务术语待申请 -- 产品代码
    , T3.PROD_NAME as 业务术语待申请 -- 产品名称
    , T3.RISKLEVEL as PROD_RISK_LVL_CD -- 风险等级
    , T3.PROD_STATUS as 业务术语待申请 -- 产品状态
    , T3.PROD_TYPE as 业务术语待申请 -- 产品类型
    , T3.PROD_UNIT as 业务术语待申请 -- 产品单位
    , T3.PROD_CONTENT as 业务术语待申请 -- 产品规格
    , T1.RISKMATCHING as RISK_LVL_MATCH_FLAG -- 风险等级是否匹配
    , T1.STATUS as 业务术语待申请 -- 申请状态
    , T1.PRICE as 业务术语待申请 -- 成交价格
    , T1.CASHFEE as COMM_FEE_RATE -- 总手续费
    , T1.ACCEPTMETHOD as ACPTD_MODE_CD -- 受理方式
    , T1.TRANSACTIONDATE as TXN_DT -- 交易发生日期 
    , T1.TRANSACTIONTIME as TXN_TM -- 交易发生时间  
    , T1.SPECIFICATION as SUMMARY_DESC -- 摘要说明
    , T1.TRANSACTORCERTNO as OPERR_DOC_NO -- 经办人证件号码
    , T1.TRANSACTORCERTTYPE as OPERR_DOCTYP_CD -- 经办人证件类型
    , T1.TRANSACTORNAME as OPERR_NAME -- 经办人姓名
    , T1.TELLERSERIALNO as 业务术语待申请 -- 柜员流水号
    , T1.CONFIRMOPID1 as 业务术语待申请 -- 复核柜员1
    , T1.CONFIRMOPID2 as 业务术语待申请 -- 复核柜员2
    , T1.CUSTMANAGERID as CUST_MGR_NO -- 客户经理代码
    , T1.AGENT_FEE as 业务术语待申请 -- 代理费
    , T1.BACK_COUNT as 业务术语待申请 -- 回购次数
    , T1.BACK_NEXT_NUM as 业务术语待申请 -- 回购剩余数量
    , T1.BACKTYPE as 业务术语待申请 -- 回购类型
    , T1.BASE_PRICE as 业务术语待申请 -- 基础金价
    , T1.CHKSTATUS as 业务术语待申请 -- 对账状态
    , T1.CLEAR_AMT as 业务术语待申请 -- 清算金额
    , T1.CLEAR_PRICE as 业务术语待申请 -- 清算单价
    , T1.CONSTRUCT_FEE as 业务术语待申请 -- 加工费
    , T1.DEPOSITACCT as 业务术语待申请 -- 投资人在销售人处用于交易的资金帐号
    , T1.DISCOUNT as 业务术语待申请 -- 最低折扣
    , T1.FINANCINGMANAGERID as FIN_MGR_NO -- 理财经理
    , T1.GETFLAG as 业务术语待申请 -- 是否提金
    , T1.INVOICE_DEAD as 业务术语待申请 -- 发票抬头
    , T1.OPERDATE as 业务术语待申请 -- 操作日期
    , T1.OPERID as OPER_TELR_NO -- 操作柜员
    , T1.OTHER_FEE as 业务术语待申请 -- 其它费
    , T1.REALTIME_FLAG as 业务术语待申请 -- 是否实时入账
    , T1.TRANS_NOS as 业务术语待申请 -- 交易个数
    , T1.TURN_ACCEPT_OPER as 业务术语待申请 -- 移交接收柜员
    , T1.TURN_COUNT as 业务术语待申请 -- 移交次数
    , T1.VIP_AMT as 业务术语待申请 -- VIP优惠金额
    , T1.VIP_MODE as 业务术语待申请 -- VIP价格模式 
    , '${process_date}' as PT_DT  -- None
from
    ${ODS_FMS_SCHEMA}.ODS_FMS_GOLD_H_TRANSLOG as T1 -- 交易流水表
    LEFT JOIN ${ODS_FMS_SCHEMA}.ODS_FMS_GOLD_CUSTINFO as T2 -- 客户信息表
    on T1.CUST_NO = T2.CUST_NO 
AND T2.PT_DT='${process_date}' 
AND T2.STATUS='0'  --客户状态
AND T2.DELETED='0' 
    LEFT JOIN ${ODS_FMS_SCHEMA}.ODS_FMS_GOLD_PRODINFO as T3 -- 产品信息表
    on T1.UNIONCODE = T3.UNIONCODE
AND T1.GOLD_ORGAN_CODE = T3.GOLD_ORGAN_CODE
AND T1.PROD_CODE = T3.PROD_CODE
AND T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN TMP_S04_ENTITY_PREC_METAL_TXN_DTL_00 as T4 -- None
    on T1.CUST_NO = T4.CUST_NO 
    LEFT JOIN 代码表待命名 as S1 -- 代码转换
    on T2.CERTIFICATETYPE = S1.SRC_CD_VAL 
AND S1.SRC_SYS_CD = 'FMS'
AND S1.SRC_COL_NAME ='CERTIFICATETYPE'
AND S1.SRC_TAB_NAME = 'ODS_FMS_GOLD_CUSTINFO' 
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第4组）==============
-- 贵金属销售系统_线下（展销会）交易流水聚合
drop table if exists ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_04;

create table ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_04 (
      APP_FORM_SER_NO varchar(255) -- 申请单流水号
    , CUST_IN_CD varchar(11) -- 客户内码
    , CUST_NO varchar(4) -- 客户号
    , CUST_NAME varchar(200) -- 客户名称
    , DOCTYP_CD varchar(30) -- 证件类型代码
    , DOC_NO varchar(100) -- 证件号码
    , 业务术语待申请 varchar(4) -- 投资人类型代码
    , LP_ORG_NO varchar(100) -- 法人机构编号
    , TXN_BNKOUTLS_NO varchar(100) -- 交易网点编号
    , 业务术语待申请 varchar(100) -- 交易支行机构编号
    , CURR_CD varchar(30) -- 币种代码
    , APP_AMT decimal(28,2) -- 申请金额
    , ORIG_APP_FORM_NO varchar(100) -- 原申请单编号
    , 业务术语待申请 char(6) -- 实物金业务类型代码
    , 业务术语待申请 varchar(30) -- 黄金公司编号
    , 业务术语待申请 varchar(100) -- 实物金产品代码
    , 业务术语待申请 varchar(100) -- 实物金产品名称
    , PROD_RISK_LVL_CD varchar(30) -- 产品风险等级代码
    , 业务术语待申请 varchar(30) -- 实物金产品状态代码
    , 业务术语待申请 varchar(30) -- 贵金属产品类型代码
    , 业务术语待申请 varchar(30) -- 实物金产品单位代码
    , 业务术语待申请 varchar(30) -- 实物金产品规格
    , RISK_LVL_MATCH_FLAG varchar(10) -- 风险等级匹配标志

    , 业务术语待申请 varchar(10) -- 实物金交易申请状态代码
    , 业务术语待申请 decimal(28,2) -- 成交价格
    , COMM_FEE_RATE decimal(20,7) -- 手续费率
    , ACPTD_MODE_CD varchar(10) -- 实物金业务受理方式代码
    , TXN_DT varchar(10) -- 交易日期
    , TXN_TM varchar(10) -- 交易时间  
    , SUMMARY_DESC varchar(300) -- 摘要描述
    , OPERR_DOC_NO varchar(100) -- 经办人证件号码
    , OPERR_DOCTYP_CD varchar(30) -- 经办人证件类型代码
    , OPERR_NAME varchar(200) -- 经办人名称
    , 业务术语待申请 varchar(200) -- 柜员流水号
    , 业务术语待申请 varchar(200) -- 复核柜员1
    , 业务术语待申请 varchar(200) -- 复核柜员2
    , CUST_MGR_NO varchar(30) -- 客户经理柜员编号
    , 业务术语待申请 decimal(28,2) -- 代理费
    , 业务术语待申请 decimal(8) -- 回购次数
    , 业务术语待申请 decimal(8) -- 回购剩余数量
    , 业务术语待申请 varchar(10) -- 实物金回购类型代码
    , 业务术语待申请 decimal(28,6) -- 基础金价
    , 业务术语待申请 varchar(10) -- 实物金对账状态代码
    , 业务术语待申请 decimal(28,6) -- 清算金额
    , 业务术语待申请 decimal(28,6) -- 清算单价
    , 业务术语待申请 decimal(28,2) -- 加工费
    , 业务术语待申请 varchar(100) -- 资金账号
    , 业务术语待申请 decimal(16,6) -- 最低折扣率
    , FIN_MGR_NO varchar(100) -- 理财经理编号
    , 业务术语待申请 varchar(10) -- 提金标志
    , 业务术语待申请 varchar(300) -- 发票抬头
    , 业务术语待申请 varchar(10) -- 操作日期
    , OPER_TELR_NO varchar(30) -- 操作柜员编号
    , 业务术语待申请 decimal(28,2) -- 其它费
    , 业务术语待申请 varchar(10) -- 实时入账标志
    , 业务术语待申请 decimal(8) -- 交易个数
    , 业务术语待申请 varchar(30) -- 移交接收柜员编号
    , 业务术语待申请 decimal(8) -- 移交次数
    , 业务术语待申请 decimal(28,2) -- VIP优惠金额
    , 业务术语待申请 varchar(10) -- VIP价格模式代码
    , PT_DT  varchar(10) -- 数据日期
)
comment '贵金属销售系统_线下（展销会）交易流水聚合'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_04(
      APP_FORM_SER_NO -- 申请单流水号
    , CUST_IN_CD -- 客户内码
    , CUST_NO -- 客户号
    , CUST_NAME -- 客户名称
    , DOCTYP_CD -- 证件类型代码
    , DOC_NO -- 证件号码
    , 业务术语待申请 -- 投资人类型代码
    , LP_ORG_NO -- 法人机构编号
    , TXN_BNKOUTLS_NO -- 交易网点编号
    , 业务术语待申请 -- 交易支行机构编号
    , CURR_CD -- 币种代码
    , APP_AMT -- 申请金额
    , ORIG_APP_FORM_NO -- 原申请单编号
    , 业务术语待申请 -- 实物金业务类型代码
    , 业务术语待申请 -- 黄金公司编号
    , 业务术语待申请 -- 实物金产品代码
    , 业务术语待申请 -- 实物金产品名称
    , PROD_RISK_LVL_CD -- 产品风险等级代码
    , 业务术语待申请 -- 实物金产品状态代码
    , 业务术语待申请 -- 贵金属产品类型代码
    , 业务术语待申请 -- 实物金产品单位代码
    , 业务术语待申请 -- 实物金产品规格
    , RISK_LVL_MATCH_FLAG -- 风险等级匹配标志

    , 业务术语待申请 -- 实物金交易申请状态代码
    , 业务术语待申请 -- 成交价格
    , COMM_FEE_RATE -- 手续费率
    , ACPTD_MODE_CD -- 实物金业务受理方式代码
    , TXN_DT -- 交易日期
    , TXN_TM -- 交易时间  
    , SUMMARY_DESC -- 摘要描述
    , OPERR_DOC_NO -- 经办人证件号码
    , OPERR_DOCTYP_CD -- 经办人证件类型代码
    , OPERR_NAME -- 经办人名称
    , 业务术语待申请 -- 柜员流水号
    , 业务术语待申请 -- 复核柜员1
    , 业务术语待申请 -- 复核柜员2
    , CUST_MGR_NO -- 客户经理柜员编号
    , 业务术语待申请 -- 代理费
    , 业务术语待申请 -- 回购次数
    , 业务术语待申请 -- 回购剩余数量
    , 业务术语待申请 -- 实物金回购类型代码
    , 业务术语待申请 -- 基础金价
    , 业务术语待申请 -- 实物金对账状态代码
    , 业务术语待申请 -- 清算金额
    , 业务术语待申请 -- 清算单价
    , 业务术语待申请 -- 加工费
    , 业务术语待申请 -- 资金账号
    , 业务术语待申请 -- 最低折扣率
    , FIN_MGR_NO -- 理财经理编号
    , 业务术语待申请 -- 提金标志
    , 业务术语待申请 -- 发票抬头
    , 业务术语待申请 -- 操作日期
    , OPER_TELR_NO -- 操作柜员编号
    , 业务术语待申请 -- 其它费
    , 业务术语待申请 -- 实时入账标志
    , 业务术语待申请 -- 交易个数
    , 业务术语待申请 -- 移交接收柜员编号
    , 业务术语待申请 -- 移交次数
    , 业务术语待申请 -- VIP优惠金额
    , 业务术语待申请 -- VIP价格模式代码
    , PT_DT  -- 数据日期
)
select
      T1.APPSHEETSERIALNO as APP_FORM_SER_NO -- 申请单编号
    , T4.CUST_IN_CD as CUST_IN_CD -- 客户内码
    , COALESCE(T2.CUST_NO,'') as CUST_NO -- 客户号
    , COALESCE(T1.CUSTNAME,T4.CUST_NAME) as CUST_NAME -- 客户名称
    , CASE WHEN T1.CERTIFICATETYPE IS NULL OR TRIM(T1.CERTIFICATETYPE)='' 
     THEN '' 
ELSE COALESCE(TRIM(S1.TARGET_CD_VAL), '@'||TRIM(T1.CERTIFICATETYPE),'') END  as DOCTYP_CD -- 证件类型
    , T1.CERTIFICATENO as DOC_NO -- 证件号码
    , T1.INVTP as 业务术语待申请 -- 投资者类型
    , SUBSTR(T1.OPERORG,1,3)||'000' as LP_ORG_NO -- 销售网点
    , T1.OPERORG as TXN_BNKOUTLS_NO -- 销售网点
    , SUBSTR(T1.OPERORG,1,5)||'0' as 业务术语待申请 -- 销售网点
    , 'CNY' as CURR_CD -- None
    , T1.TRANS_AMT as APP_AMT -- 交易金额
    , '' as ORIG_APP_FORM_NO -- None
    , '' as 业务术语待申请 -- None
    , T1.GOLD_ORGAN_CODE as 业务术语待申请 -- 黄金公司代码
    , T1.PROD_CODE as 业务术语待申请 -- 产品代码
    , T1.PROD_NAME as 业务术语待申请 -- 产品名称
    , T3.RISKLEVEL as PROD_RISK_LVL_CD -- 风险等级
    , T3.PROD_STATUS as 业务术语待申请 -- 产品状态
    , T1.PROD_TYPE as 业务术语待申请 -- 产品类型
    , T3.PROD_UNIT as 业务术语待申请 -- 产品单位
    , T3.PROD_CONTENT as 业务术语待申请 -- 产品规格
    , '匹配' as RISK_LVL_MATCH_FLAG -- None
    , '成功' as 业务术语待申请 -- None
    , T1.PRICE as 业务术语待申请 -- 交易单价
    , '' as COMM_FEE_RATE -- None
    , '展销会' as ACPTD_MODE_CD -- None
    , T1.TRANSACTIONDATE as TXN_DT -- 交易日期 
    , '' as TXN_TM -- None
    , '' as SUMMARY_DESC -- None
    , '' as OPERR_DOC_NO -- None
    , '' as OPERR_DOCTYP_CD -- None
    , '' as OPERR_NAME -- None
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '' as CUST_MGR_NO -- None
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , T1.CLEAR_AMT as 业务术语待申请 -- 清算金额
    , T1.CLEAR_PRICE as 业务术语待申请 -- 清算单价
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '' as FIN_MGR_NO -- None
    , T1.GETFLAG as 业务术语待申请 -- 是否提金
    , T1.INVOICE_DEAD as 业务术语待申请 -- 发票名称
    , '' as 业务术语待申请 -- None
    , T1.OPERID as OPER_TELR_NO -- 操作柜员
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , T1.TRANS_NOS as 业务术语待申请 -- 交易个数
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '${process_date}' as PT_DT  -- None
from
    ${ODS_FMS_SCHEMA}.ODS_FMS_GOLD_OFFLINE_TRANS_LOG as T1 -- 交易流水表
    LEFT JOIN ${ODS_FMS_SCHEMA}.ODS_FMS_GOLD_CUSTINFO as T2 -- 客户信息表
    on T1.CERTIFICATETYPE = T2.CERTIFICATETYPE
AND T1.CERTIFICATENO = T2.CERTIFICATENO
AND T2.PT_DT='${process_date}' 
AND T2.STATUS='0'  --客户状态
AND T2.DELETED='0' 
    LEFT JOIN ${ODS_FMS_SCHEMA}.ODS_FMS_GOLD_PRODINFO as T3 -- 产品信息表
    on  T1.GOLD_ORGAN_CODE = T3.GOLD_ORGAN_CODE
AND T1.PROD_CODE = T3.PROD_CODE
AND T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN TMP_S04_ENTITY_PREC_METAL_TXN_DTL_00 as T4 -- None
    on T4.CUST_NO = T2.CUST_NO 
    LEFT JOIN 代码表待命名 as S1 -- 代码转换
    on T2.CERTIFICATETYPE = S1.SRC_CD_VAL 
AND S1.SRC_SYS_CD = 'FMS'
AND S1.SRC_COL_NAME ='CERTIFICATETYPE'
AND S1.SRC_TAB_NAME = 'ODS_FMS_GOLD_CUSTINFO' 
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第6组）==============
-- 贵金属销售系统_聚合汇总

insert into table ${session}.S04_ENTITY_PREC_METAL_TXN_DTL(
      APP_FORM_SER_NO -- 申请单流水号
    , CUST_IN_CD -- 客户内码
    , CUST_NO -- 客户号
    , CUST_NAME -- 客户名称
    , DOCTYP_CD -- 证件类型代码
    , DOC_NO -- 证件号码
    , 业务术语待申请 -- 投资人类型代码
    , LP_ORG_NO -- 法人机构编号
    , TXN_BNKOUTLS_NO -- 交易网点编号
    , 业务术语待申请 -- 交易支行机构编号
    , CURR_CD -- 币种代码
    , APP_AMT -- 申请金额
    , ORIG_APP_FORM_NO -- 原申请单编号
    , 业务术语待申请 -- 实物金业务类型代码
    , 业务术语待申请 -- 黄金公司编号
    , 业务术语待申请 -- 实物金产品代码
    , 业务术语待申请 -- 实物金产品名称
    , PROD_RISK_LVL_CD -- 产品风险等级代码
    , 业务术语待申请 -- 实物金产品状态代码
    , 业务术语待申请 -- 贵金属产品类型代码
    , 业务术语待申请 -- 实物金产品单位代码
    , 业务术语待申请 -- 实物金产品规格
    , RISK_LVL_MATCH_FLAG -- 风险等级匹配标志
    , 业务术语待申请 -- 实物金交易申请状态代码
    , 业务术语待申请 -- 成交价格
    , COMM_FEE_RATE -- 手续费率
    , ACPTD_MODE_CD -- 实物金业务受理方式代码
    , TXN_DT -- 交易日期
    , TXN_TM -- 交易时间  
    , SUMMARY_DESC -- 摘要描述
    , OPERR_DOC_NO -- 经办人证件号码
    , OPERR_DOCTYP_CD -- 经办人证件类型代码
    , OPERR_NAME -- 经办人名称
    , 业务术语待申请 -- 柜员流水号
    , 业务术语待申请 -- 复核柜员1
    , 业务术语待申请 -- 复核柜员2
    , CUST_MGR_NO -- 客户经理柜员编号
    , 业务术语待申请 -- 代理费
    , 业务术语待申请 -- 回购次数
    , 业务术语待申请 -- 回购剩余数量
    , 业务术语待申请 -- 实物金回购类型代码
    , 业务术语待申请 -- 基础金价
    , 业务术语待申请 -- 实物金对账状态代码
    , 业务术语待申请 -- 清算金额
    , 业务术语待申请 -- 清算单价
    , 业务术语待申请 -- 加工费
    , 业务术语待申请 -- 资金账号
    , 业务术语待申请 -- 最低折扣率
    , FIN_MGR_NO -- 理财经理编号
    , 业务术语待申请 -- 提金标志
    , 业务术语待申请 -- 发票抬头
    , 业务术语待申请 -- 操作日期
    , OPER_TELR_NO -- 操作柜员编号
    , 业务术语待申请 -- 其它费
    , 业务术语待申请 -- 实时入账标志
    , 业务术语待申请 -- 交易个数
    , 业务术语待申请 -- 移交接收柜员编号
    , 业务术语待申请 -- 移交次数
    , 业务术语待申请 -- VIP优惠金额
    , 业务术语待申请 -- VIP价格模式代码
    , PT_DT  -- 数据日期
)
select
      U1.APP_FORM_SER_NO         as APP_FORM_SER_NO -- 申请单流水号
    , U1.CUST_IN_CD              as CUST_IN_CD -- 客户内码
    , U1.CUST_NO                 as CUST_NO -- 客户号
    , U1.CUST_NAME               as CUST_NAME -- 客户名称
    , U1.DOCTYP_CD               as DOCTYP_CD -- 证件类型代码
    , U1.DOC_NO                  as DOC_NO -- 证件号码
    , U1.业务术语待申请          as 业务术语待申请 -- 投资人类型代码
    , U1.LP_ORG_NO               as LP_ORG_NO -- 法人机构编号
    , U1.TXN_BNKOUTLS_NO         as TXN_BNKOUTLS_NO -- 交易网点编号
    , U1.业务术语待申请          as 业务术语待申请 -- 交易支行机构编号
    , U1.CURR_CD                 as CURR_CD -- 币种代码
    , U1.APP_AMT                 as APP_AMT -- 申请金额
    , U1.ORIG_APP_FORM_NO        as ORIG_APP_FORM_NO -- 原申请单编号
    , U1.业务术语待申请          as 业务术语待申请 -- 实物金业务类型代码
    , U1.业务术语待申请          as 业务术语待申请 -- 黄金公司编号
    , U1.业务术语待申请          as 业务术语待申请 -- 实物金产品代码
    , U1.业务术语待申请          as 业务术语待申请 -- 实物金产品名称
    , U1.PROD_RISK_LVL_CD        as PROD_RISK_LVL_CD -- 产品风险等级代码
    , U1.业务术语待申请          as 业务术语待申请 -- 实物金产品状态代码
    , U1.业务术语待申请          as 业务术语待申请 -- 贵金属产品类型代码
    , U1.业务术语待申请          as 业务术语待申请 -- 实物金产品单位代码
    , U1.业务术语待申请          as 业务术语待申请 -- 实物金产品规格
    , U1.RISK_LVL_MATCH_FLAG     as RISK_LVL_MATCH_FLAG -- 风险等级匹配标志

    , U1.业务术语待申请          as 业务术语待申请 -- 实物金交易申请状态代码
    , U1.业务术语待申请          as 业务术语待申请 -- 成交价格
    , U1.COMM_FEE_RATE           as COMM_FEE_RATE -- 手续费率
    , U1.ACPTD_MODE_CD           as ACPTD_MODE_CD -- 实物金业务受理方式代码
    , U1.TXN_DT                  as TXN_DT -- 交易日期
    , U1.TXN_TM                  as TXN_TM -- 交易时间  
    , U1.SUMMARY_DESC            as SUMMARY_DESC -- 摘要描述
    , U1.OPERR_DOC_NO            as OPERR_DOC_NO -- 经办人证件号码
    , U1.OPERR_DOCTYP_CD         as OPERR_DOCTYP_CD -- 经办人证件类型代码
    , U1.OPERR_NAME              as OPERR_NAME -- 经办人名称
    , U1.业务术语待申请          as 业务术语待申请 -- 柜员流水号
    , U1.业务术语待申请          as 业务术语待申请 -- 复核柜员1
    , U1.业务术语待申请          as 业务术语待申请 -- 复核柜员2
    , U1.CUST_MGR_NO             as CUST_MGR_NO -- 客户经理柜员编号
    , U1.业务术语待申请          as 业务术语待申请 -- 代理费
    , U1.业务术语待申请          as 业务术语待申请 -- 回购次数
    , U1.业务术语待申请          as 业务术语待申请 -- 回购剩余数量
    , U1.业务术语待申请          as 业务术语待申请 -- 实物金回购类型代码
    , U1.业务术语待申请          as 业务术语待申请 -- 基础金价
    , U1.业务术语待申请          as 业务术语待申请 -- 实物金对账状态代码
    , U1.业务术语待申请          as 业务术语待申请 -- 清算金额
    , U1.业务术语待申请          as 业务术语待申请 -- 清算单价
    , U1.业务术语待申请          as 业务术语待申请 -- 加工费
    , U1.业务术语待申请          as 业务术语待申请 -- 资金账号
    , U1.业务术语待申请          as 业务术语待申请 -- 最低折扣率
    , U1.FIN_MGR_NO              as FIN_MGR_NO -- 理财经理编号
    , U1.业务术语待申请          as 业务术语待申请 -- 提金标志
    , U1.业务术语待申请          as 业务术语待申请 -- 发票抬头
    , U1.业务术语待申请          as 业务术语待申请 -- 操作日期
    , U1.OPER_TELR_NO            as OPER_TELR_NO -- 操作柜员编号
    , U1.业务术语待申请          as 业务术语待申请 -- 其它费
    , U1.业务术语待申请          as 业务术语待申请 -- 实时入账标志
    , U1.业务术语待申请          as 业务术语待申请 -- 交易个数
    , U1.业务术语待申请          as 业务术语待申请 -- 移交接收柜员编号
    , U1.业务术语待申请          as 业务术语待申请 -- 移交次数
    , U1.业务术语待申请          as 业务术语待申请 -- VIP优惠金额
    , U1.业务术语待申请          as 业务术语待申请 -- VIP价格模式代码
    , U1.PT_DT as PT_DT  -- None
from
    (
SELECT 
T1.APP_FORM_SER_NO           --申请单流水号
,T1.CUST_IN_CD                --客户内码
,T1.CUST_NO                   --客户号
,T1.CUST_NAME                 --客户名称
,T1.DOCTYP_CD                 --证件类型代码
,T1.DOC_NO                    --证件号码
,T1.业务术语待申请            --投资人类型代码
,T1.LP_ORG_NO                 --法人机构编号
,T1.TXN_BNKOUTLS_NO           --交易网点编号
,T1.业务术语待申请            --交易支行机构编号
,T1.CURR_CD                   --币种代码
,T1.APP_AMT                   --申请金额
,T1.ORIG_APP_FORM_NO          --原申请单编号
,T1.业务术语待申请            --实物金业务类型代码
,T1.业务术语待申请            --黄金公司编号
,T1.业务术语待申请            --实物金产品代码
,T1.业务术语待申请            --实物金产品名称
,T1.PROD_RISK_LVL_CD          --产品风险等级代码
,T1.业务术语待申请            --实物金产品状态代码
,T1.业务术语待申请            --贵金属产品类型代码
,T1.业务术语待申请            --实物金产品单位代码
,T1.业务术语待申请            --实物金产品规格
,T1.RISK_LVL_MATCH_FLAG       --风险等级匹配标志
,T1.业务术语待申请            --实物金交易申请状态代码
,T1.业务术语待申请            --成交价格
,T1.COMM_FEE_RATE             --手续费率
,T1.ACPTD_MODE_CD             --实物金业务受理方式代码
,T1.TXN_DT                    --交易日期
,T1.TXN_TM                    --交易时间  
,T1.SUMMARY_DESC              --摘要描述
,T1.OPERR_DOC_NO              --经办人证件号码
,T1.OPERR_DOCTYP_CD           --经办人证件类型代码
,T1.OPERR_NAME                --经办人名称
,T1.业务术语待申请            --柜员流水号
,T1.业务术语待申请            --复核柜员1
,T1.业务术语待申请            --复核柜员2
,T1.CUST_MGR_NO               --客户经理柜员编号
,T1.业务术语待申请            --代理费
,T1.业务术语待申请            --回购次数
,T1.业务术语待申请            --回购剩余数量
,T1.业务术语待申请            --实物金回购类型代码
,T1.业务术语待申请            --基础金价
,T1.业务术语待申请            --实物金对账状态代码
,T1.业务术语待申请            --清算金额
,T1.业务术语待申请            --清算单价
,T1.业务术语待申请            --加工费
,T1.业务术语待申请            --资金账号
,T1.业务术语待申请            --最低折扣率
,T1.FIN_MGR_NO                --理财经理编号
,T1.业务术语待申请            --提金标志
,T1.业务术语待申请            --发票抬头
,T1.业务术语待申请            --操作日期
,T1.OPER_TELR_NO              --操作柜员编号
,T1.业务术语待申请            --其它费
,T1.业务术语待申请            --实时入账标志
,T1.业务术语待申请            --交易个数
,T1.业务术语待申请            --移交接收柜员编号
,T1.业务术语待申请            --移交次数
,T1.业务术语待申请            --VIP优惠金额
,T1.业务术语待申请            --VIP价格模式代码
,T1.PT_DT                   --数据日期
FROM TMP_S04_ENTITY_PREC_METAL_TXN_DTL_02 T1 
UNION ALL 
SELECT 
T2.APP_FORM_SER_NO           --申请单流水号
,T2.CUST_IN_CD                --客户内码
,T2.CUST_NO                   --客户号
,T2.CUST_NAME                 --客户名称
,T2.DOCTYP_CD                 --证件类型代码
,T2.DOC_NO                    --证件号码
,T2.业务术语待申请            --投资人类型代码
,T2.LP_ORG_NO                 --法人机构编号
,T2.TXN_BNKOUTLS_NO           --交易网点编号
,T2.业务术语待申请            --交易支行机构编号
,T2.CURR_CD                   --币种代码
,T2.APP_AMT                   --申请金额
,T2.ORIG_APP_FORM_NO          --原申请单编号
,T2.业务术语待申请            --实物金业务类型代码
,T2.业务术语待申请            --黄金公司编号
,T2.业务术语待申请            --实物金产品代码
,T2.业务术语待申请            --实物金产品名称
,T2.PROD_RISK_LVL_CD          --产品风险等级代码
,T2.业务术语待申请            --实物金产品状态代码
,T2.业务术语待申请            --贵金属产品类型代码
,T2.业务术语待申请            --实物金产品单位代码
,T2.业务术语待申请            --实物金产品规格
,T2.RISK_LVL_MATCH_FLAG       --风险等级匹配标志
,T2.业务术语待申请            --实物金交易申请状态代码
,T2.业务术语待申请            --成交价格
,T2.COMM_FEE_RATE             --手续费率
,T2.ACPTD_MODE_CD             --实物金业务受理方式代码
,T2.TXN_DT                    --交易日期
,T2.TXN_TM                    --交易时间  
,T2.SUMMARY_DESC              --摘要描述
,T2.OPERR_DOC_NO              --经办人证件号码
,T2.OPERR_DOCTYP_CD           --经办人证件类型代码
,T2.OPERR_NAME                --经办人名称
,T2.业务术语待申请            --柜员流水号
,T2.业务术语待申请            --复核柜员1
,T2.业务术语待申请            --复核柜员2
,T2.CUST_MGR_NO               --客户经理柜员编号
,T2.业务术语待申请            --代理费
,T2.业务术语待申请            --回购次数
,T2.业务术语待申请            --回购剩余数量
,T2.业务术语待申请            --实物金回购类型代码
,T2.业务术语待申请            --基础金价
,T2.业务术语待申请            --实物金对账状态代码
,T2.业务术语待申请            --清算金额
,T2.业务术语待申请            --清算单价
,T2.业务术语待申请            --加工费
,T2.业务术语待申请            --资金账号
,T2.业务术语待申请            --最低折扣率
,T2.FIN_MGR_NO                --理财经理编号
,T2.业务术语待申请            --提金标志
,T2.业务术语待申请            --发票抬头
,T2.业务术语待申请            --操作日期
,T2.OPER_TELR_NO              --操作柜员编号
,T2.业务术语待申请            --其它费
,T2.业务术语待申请            --实时入账标志
,T2.业务术语待申请            --交易个数
,T2.业务术语待申请            --移交接收柜员编号
,T2.业务术语待申请            --移交次数
,T2.业务术语待申请            --VIP优惠金额
,T2.业务术语待申请            --VIP价格模式代码
,T2.PT_DT                   --数据日期
FROM TMP_S04_ENTITY_PREC_METAL_TXN_DTL_03 T2 

UNION ALL 
SELECT 
T3.APP_FORM_SER_NO           --申请单流水号
,T3.CUST_IN_CD                --客户内码
,T3.CUST_NO                   --客户号
,T3.CUST_NAME                 --客户名称
,T3.DOCTYP_CD                 --证件类型代码
,T3.DOC_NO                    --证件号码
,T3.业务术语待申请            --投资人类型代码
,T3.LP_ORG_NO                 --法人机构编号
,T3.TXN_BNKOUTLS_NO           --交易网点编号
,T3.业务术语待申请            --交易支行机构编号
,T3.CURR_CD                   --币种代码
,T3.APP_AMT                   --申请金额
,T3.ORIG_APP_FORM_NO          --原申请单编号
,T3.业务术语待申请            --实物金业务类型代码
,T3.业务术语待申请            --黄金公司编号
,T3.业务术语待申请            --实物金产品代码
,T3.业务术语待申请            --实物金产品名称
,T3.PROD_RISK_LVL_CD          --产品风险等级代码
,T3.业务术语待申请            --实物金产品状态代码
,T3.业务术语待申请            --贵金属产品类型代码
,T3.业务术语待申请            --实物金产品单位代码
,T3.业务术语待申请            --实物金产品规格
,T3.RISK_LVL_MATCH_FLAG       --风险等级匹配标志
,T3.业务术语待申请            --实物金交易申请状态代码
,T3.业务术语待申请            --成交价格
,T3.COMM_FEE_RATE             --手续费率
,T3.ACPTD_MODE_CD             --实物金业务受理方式代码
,T3.TXN_DT                    --交易日期
,T3.TXN_TM                    --交易时间  
,T3.SUMMARY_DESC              --摘要描述
,T3.OPERR_DOC_NO              --经办人证件号码
,T3.OPERR_DOCTYP_CD           --经办人证件类型代码
,T3.OPERR_NAME                --经办人名称
,T3.业务术语待申请            --柜员流水号
,T3.业务术语待申请            --复核柜员1
,T3.业务术语待申请            --复核柜员2
,T3.CUST_MGR_NO               --客户经理柜员编号
,T3.业务术语待申请            --代理费
,T3.业务术语待申请            --回购次数
,T3.业务术语待申请            --回购剩余数量
,T3.业务术语待申请            --实物金回购类型代码
,T3.业务术语待申请            --基础金价
,T3.业务术语待申请            --实物金对账状态代码
,T3.业务术语待申请            --清算金额
,T3.业务术语待申请            --清算单价
,T3.业务术语待申请            --加工费
,T3.业务术语待申请            --资金账号
,T3.业务术语待申请            --最低折扣率
,T3.FIN_MGR_NO                --理财经理编号
,T3.业务术语待申请            --提金标志
,T3.业务术语待申请            --发票抬头
,T3.业务术语待申请            --操作日期
,T3.OPER_TELR_NO              --操作柜员编号
,T3.业务术语待申请            --其它费
,T3.业务术语待申请            --实时入账标志
,T3.业务术语待申请            --交易个数
,T3.业务术语待申请            --移交接收柜员编号
,T3.业务术语待申请            --移交次数
,T3.业务术语待申请            --VIP优惠金额
,T3.业务术语待申请            --VIP价格模式代码
,T3.PT_DT                   --数据日期
FROM TMP_S04_ENTITY_PREC_METAL_TXN_DTL_04 T3 
) as P1 -- 流水表
;

-- 删除所有临时表
drop table ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_00;
drop table ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_01;
drop table ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_03;
drop table ${session}.TMP_S04_ENTITY_PREC_METAL_TXN_DTL_04;