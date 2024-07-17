-- 层次表名: 聚合层-代销信托交易明细表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s04_trust_txn_dtl
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S04_TRUST_TXN_DTL
--     表中文名：代销信托交易明细表
--     创建日期：2023-12-19 00:00:00
--     主键字段：申请单流水号,信托业务确认代码,TA流水号
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含信托代销客户购买、赎回、分红、到期兑付等的交易信息
包含信托代销个人和机构客户，对于所有类型产品的所有交易的交易流水信息
--     更新记录：
--         2023-12-19 00:00:00 王穆军 新增
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S04_TRUST_TXN_DTL drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 代销信托交易明细_数据组1
drop table if exists ${session}.TMP_S04_TRUST_TXN_DTL_01;

create table ${session}.TMP_S04_TRUST_TXN_DTL_01 (
      APP_FORM_SER_NO varchar(100) -- 申请单流水号
    , TRUST_BIZ_CFM_CD varchar(30) -- 信托业务确认代码
    , TA_SER_NO varchar(100) -- TA流水号
    , LP_ORG_NO varchar(100) -- 法人机构编号
    , TXN_HAPP_ORG_NO varchar(100) -- 交易发生机构编号
    , TRUST_BIZ_TYPE_CD varchar(30) -- 信托业务类别代码
    , APP_SHARE decimal(28,2) -- 申请份额
    , APP_AMT decimal(28,2) -- 申请金额
    , CFM_SHARE decimal(28,2) -- 确认份额
    , CFM_AMT decimal(28,2) -- 确认金额
    , TXN_DT varchar(10) -- 交易日期
    , TXN_TM varchar(100) -- 交易时间
    , ORDER_DT varchar(10) -- 下单日期
    , ORDER_TM varchar(100) -- 下单时间
    , CFM_DT varchar(10) -- 确认日期
    , STL_CURR_CD varchar(30) -- 结算币种代码
    , ORIG_APP_FORM_NO varchar(100) -- 原申请单编号
    , BANK_ACCT_NO varchar(100) -- 银行账户编号
    , TXN_HAPP_SELLER_NO varchar(100) -- 交易发生销售商编号
    , TA_CD varchar(100) -- TA代码
    , TA_ACCT_NO varchar(100) -- TA账户编号
    , TXN_ACCT_NO varchar(100) -- 交易账户编号
    , CUST_IN_CD varchar(100) -- 客户内码
    , CUST_TYPE_CD varchar(30) -- 客户类型代码
    , MAIN_DOCTYP_CD varchar(30) -- 主证件类型代码
    , MAIN_DOC_NO varchar(100) -- 主证件号码
    , OPERR_DOCTYP_CD varchar(30) -- 经办人证件类型代码
    , OPERR_DOC_NO varchar(100) -- 经办人证件号码
    , OPERR_NAME varchar(200) -- 经办人名称
    , SIGN_BNKOUTLS_NO varchar(100) -- 签约网点编号
    , OPEN_CARD_BNKOUTLS_NO varchar(100) -- 开卡网点编号
    , TRUST_PROD_NO varchar(100) -- 信托产品编号
    , TRUST_PROD_NAME varchar(500) -- 信托产品名称
    , TRUST_PROD_TYPE_CD varchar(30) -- 信托产品类别代码
    , TRUST_PROD_RISK_LVL_CD varchar(30) -- 信托产品风险等级代码
    , TRUST_PROD_NET_VAL decimal(20,7) -- 信托产品净值
    , YIELD_ACVMNT_COMP_BNCHMK_CEIL decimal(20,7) -- 收益率业绩比较基准上限
    , YIELD_ACVMNT_COMP_BNCHMK_FLOR decimal(20,7) -- 收益率业绩比较基准下限
    , TRUST_TXN_APP_STATUS_CD varchar(30) -- 信托交易申请状态代码
    , ACPTD_MODE_CD varchar(30) -- 受理方式代码
    , DIVDND_MODE_CD varchar(30) -- 分红方式代码
    , FRONT_BACK_END_CHARGE_MODE varchar(30) -- 前后端收费方式代码
    , FRZ_RSN_CD varchar(30) -- 冻结原因代码
    , FRZ_DEADLINE varchar(10) -- 冻结截止日期
    , HUGE_REDEM_DEAL_CD varchar(30) -- 巨额赎回处理代码
    , EXCEP_TXN_CD varchar(30) -- 异常交易代码
    , RISK_LVL_MATCH_FLAG varchar(10) -- 风险等级匹配标志
    , TXN_WITHDR_IDNT_CD varchar(30) -- 交易撤单标识代码
    , CNTPTY_TA_ACCT_NO varchar(100) -- 对方TA账户编号
    , CNTPTY_TXN_ACCT_NO varchar(100) -- 对方交易账户编号
    , CNTPTY_BNKOUTLS_NO varchar(100) -- 对方网点编号
    , CNTPTY_RGN_NO varchar(100) -- 对方地区编号
    , CNTPTY_SELLER_NO varchar(100) -- 对方销售商编号
    , CNTPTY_TARGET_PROD_NO varchar(100) -- 对方目标产品编号
    , CNTPTY_TARGET_CHARGE_MODE_CD varchar(30) -- 对方目标收费方式代码
    , MKTING_PSN_AFLT_BNKOUTLS_NO varchar(100) -- 营销人员所属网点编号
    , EACCT_MKTING_CUST_MGR_NO varchar(100) -- 电子账户营销客户经理编号
    , OPER_TELR_NO varchar(100) -- 操作柜员编号
    , TXN_AUTH_TELR_NO_1 varchar(100) -- 交易授权柜员1编号
    , TXN_AUTH_TELR_NO_2 varchar(100) -- 交易授权柜员2编号
    , TXN_DEAL_RETURN_ERR_INFO_DESC varchar(5000) -- 交易处理返回错误信息描述
    , TXN_DEAL_AFT_RTN_CD varchar(100) -- 交易处理后返回码
    , BIZ_PRDURE_CMPLT_END_CD varchar(30) -- 业务过程完全结束代码
    , SUMMARY_ILUS varchar(500) -- 摘要说明
)
comment '代销信托交易明细_数据组1'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S04_TRUST_TXN_DTL_01(
      APP_FORM_SER_NO -- 申请单流水号
    , TRUST_BIZ_CFM_CD -- 信托业务确认代码
    , TA_SER_NO -- TA流水号
    , LP_ORG_NO -- 法人机构编号
    , TXN_HAPP_ORG_NO -- 交易发生机构编号
    , TRUST_BIZ_TYPE_CD -- 信托业务类别代码
    , APP_SHARE -- 申请份额
    , APP_AMT -- 申请金额
    , CFM_SHARE -- 确认份额
    , CFM_AMT -- 确认金额
    , TXN_DT -- 交易日期
    , TXN_TM -- 交易时间
    , ORDER_DT -- 下单日期
    , ORDER_TM -- 下单时间
    , CFM_DT -- 确认日期
    , STL_CURR_CD -- 结算币种代码
    , ORIG_APP_FORM_NO -- 原申请单编号
    , BANK_ACCT_NO -- 银行账户编号
    , TXN_HAPP_SELLER_NO -- 交易发生销售商编号
    , TA_CD -- TA代码
    , TA_ACCT_NO -- TA账户编号
    , TXN_ACCT_NO -- 交易账户编号
    , CUST_IN_CD -- 客户内码
    , CUST_TYPE_CD -- 客户类型代码
    , MAIN_DOCTYP_CD -- 主证件类型代码
    , MAIN_DOC_NO -- 主证件号码
    , OPERR_DOCTYP_CD -- 经办人证件类型代码
    , OPERR_DOC_NO -- 经办人证件号码
    , OPERR_NAME -- 经办人名称
    , SIGN_BNKOUTLS_NO -- 签约网点编号
    , OPEN_CARD_BNKOUTLS_NO -- 开卡网点编号
    , TRUST_PROD_NO -- 信托产品编号
    , TRUST_PROD_NAME -- 信托产品名称
    , TRUST_PROD_TYPE_CD -- 信托产品类别代码
    , TRUST_PROD_RISK_LVL_CD -- 信托产品风险等级代码
    , TRUST_PROD_NET_VAL -- 信托产品净值
    , YIELD_ACVMNT_COMP_BNCHMK_CEIL -- 收益率业绩比较基准上限
    , YIELD_ACVMNT_COMP_BNCHMK_FLOR -- 收益率业绩比较基准下限
    , TRUST_TXN_APP_STATUS_CD -- 信托交易申请状态代码
    , ACPTD_MODE_CD -- 受理方式代码
    , DIVDND_MODE_CD -- 分红方式代码
    , FRONT_BACK_END_CHARGE_MODE -- 前后端收费方式代码
    , FRZ_RSN_CD -- 冻结原因代码
    , FRZ_DEADLINE -- 冻结截止日期
    , HUGE_REDEM_DEAL_CD -- 巨额赎回处理代码
    , EXCEP_TXN_CD -- 异常交易代码
    , RISK_LVL_MATCH_FLAG -- 风险等级匹配标志
    , TXN_WITHDR_IDNT_CD -- 交易撤单标识代码
    , CNTPTY_TA_ACCT_NO -- 对方TA账户编号
    , CNTPTY_TXN_ACCT_NO -- 对方交易账户编号
    , CNTPTY_BNKOUTLS_NO -- 对方网点编号
    , CNTPTY_RGN_NO -- 对方地区编号
    , CNTPTY_SELLER_NO -- 对方销售商编号
    , CNTPTY_TARGET_PROD_NO -- 对方目标产品编号
    , CNTPTY_TARGET_CHARGE_MODE_CD -- 对方目标收费方式代码
    , MKTING_PSN_AFLT_BNKOUTLS_NO -- 营销人员所属网点编号
    , EACCT_MKTING_CUST_MGR_NO -- 电子账户营销客户经理编号
    , OPER_TELR_NO -- 操作柜员编号
    , TXN_AUTH_TELR_NO_1 -- 交易授权柜员1编号
    , TXN_AUTH_TELR_NO_2 -- 交易授权柜员2编号
    , TXN_DEAL_RETURN_ERR_INFO_DESC -- 交易处理返回错误信息描述
    , TXN_DEAL_AFT_RTN_CD -- 交易处理后返回码
    , BIZ_PRDURE_CMPLT_END_CD -- 业务过程完全结束代码
    , SUMMARY_ILUS -- 摘要说明
)
select
      P1.APPSHEETSERIALNO as APP_FORM_SER_NO -- 申请单编号
    , P1.BUSINESSCODE_APP as TRUST_BIZ_CFM_CD -- 业务代码(申请)
    , '' as TA_SER_NO -- None
    , P1.UNIONCODE as LP_ORG_NO -- 总行代码
    , P1.OPERORG as TXN_HAPP_ORG_NO -- 交易发生网点
    , P1.BUSINESSCODE as TRUST_BIZ_TYPE_CD -- 业务代码
    , P1.APPLICATIONVOL as APP_SHARE -- 申请份额
    , P1.APPLICATIONAMOUNT as APP_AMT -- 申请金额
    , '0' as CFM_SHARE -- None
    , '0' as CFM_AMT -- None
    , P1.TRANSACTIONDATE as TXN_DT -- 交易发生日期格式为：YYYYMMDD
    , P1.TRANSACTIONTIME as TXN_TM -- 交易发生时间格式为：HHMMSS
    , P1.OPERDATE as ORDER_DT -- 下单发生日期格式为：YYYYMMDD
    , P1.OPERTIME as ORDER_TM -- 下单发生时间格式为：HHMMSS
    , P1.TRANSACTIONCFMDATE as CFM_DT -- 交易确认日期格式为：YYYYMMDD
    , P1.CURRENCYTYPE as STL_CURR_CD -- 结算币种
    , P1.ORIGINALAPPSHEETNO as ORIG_APP_FORM_NO -- 原申请单编号027业务使用，表示转出时的申请单编号
    , P1.DEPOSITACCT as BANK_ACCT_NO -- 投资人在销售人处用于交易的资金帐号
    , P1.DISTRIBUTORCODE as TXN_HAPP_SELLER_NO -- 销售商代码
    , P1.TANO as TA_CD -- TA代码
    , P1.TAACCOUNTID as TA_ACCT_NO -- 基金帐号
    , P1.TRANSACTIONACCOUNTID as TXN_ACCT_NO -- 交易帐号
    , P3.INNERCODE as CUST_IN_CD -- 客户内码
    , CASE WHEN P3.INVTP IS NULL OR TRIM(P3.INVTP)='' 
     THEN '' 
ELSE COALESCE(TRIM(S1.TARGET_CD_VAL), '@'||TRIM(P3.INVTP),'') END  as CUST_TYPE_CD -- 投资者类型
    , nvl(p3.INVTP,"")||'_'||nvl(p3.CERTIFICATETYPE,"") as MAIN_DOCTYP_CD -- 投资者类型/证件类型
    , P3.CERTIFICATENO as MAIN_DOC_NO -- 证件号码
    , CASE WHEN P1.TRANSACTORCERTTYPE IS NULL OR TRIM(P1.TRANSACTORCERTTYPE)='' 
     THEN '' 
ELSE COALESCE(TRIM(S3.TARGET_CD_VAL), '@'||TRIM(P1.TRANSACTORCERTTYPE),'') END 
 as OPERR_DOCTYP_CD -- 经办人证件类型
    , P1.TRANSACTORCERTNO as OPERR_DOC_NO -- 经办人证件号码
    , P1.TRANSACTORNAME as OPERR_NAME -- 经办人姓名
    , P1.NETNO as SIGN_BNKOUTLS_NO -- 签约网点
    , P1.BANKNO as OPEN_CARD_BNKOUTLS_NO -- 开卡网点
    , P1.FUNDCODE as TRUST_PROD_NO -- 基金代码
    , P2.FUNDNAME as TRUST_PROD_NAME -- 基金简称
    , P2.FUNDTYPE as TRUST_PROD_TYPE_CD -- 产品类型
    , P2.RISKLEVEL as TRUST_PROD_RISK_LVL_CD -- 风险等级
    , '' as TRUST_PROD_NET_VAL -- None
    , P2.INCOMEVALUEMIN as YIELD_ACVMNT_COMP_BNCHMK_CEIL -- 业绩比较基准下限
    , P2.INCOMEVALUEMAX as YIELD_ACVMNT_COMP_BNCHMK_FLOR -- 业绩比较基准上限
    , P1.STATUS as TRUST_TXN_APP_STATUS_CD -- 申请状态
    , P1.ACCEPTMETHOD as ACPTD_MODE_CD -- 受理方式
    , P1.DEFDIVIDENDMETHOD as DIVDND_MODE_CD -- 默认分红方式
    , P1.SHARETYPE as FRONT_BACK_END_CHARGE_MODE -- 收费方式
    , P1.FROZENCAUSE as FRZ_RSN_CD -- 冻结原因
    , P1.FREEZINGDEADLINE as FRZ_DEADLINE -- 冻结截止日期格式为：YYYYMMDD
    , P1.LARGEREDEMPTIONFLAG as HUGE_REDEM_DEAL_CD -- 巨额赎回处理标志
    , P1.EXCEPTIONALFLAG as EXCEP_TXN_CD -- 异常交易标志
    , P1.RISKMATCHING as RISK_LVL_MATCH_FLAG -- 风险等级是否匹配
    , P1.CANCELFLAG as TXN_WITHDR_IDNT_CD -- 撤单标志
    , P1.TARGETTAACCOUNTID as CNTPTY_TA_ACCT_NO -- 对方基金账号
    , P1.TARGETTRANSACTIONACCOUNTID as CNTPTY_TXN_ACCT_NO -- 对方交易帐号在销售商是一次完成时，为必须项
    , P1.TARGETBRANCHCODE as CNTPTY_BNKOUTLS_NO -- 对方网点号转销售商、非交易过户时使用
    , P1.TARGETREGIONCODE as CNTPTY_RGN_NO -- 对方地区编号具体编码依GB13497-92
    , P1.TARGETDISTRIBUTORCODE as CNTPTY_SELLER_NO -- 对方销售商代码
    , P1.TARGETFUNDCODE as CNTPTY_TARGET_PROD_NO -- 对方目标基金代码
    , P1.TARGETSHARETYPE as CNTPTY_TARGET_CHARGE_MODE_CD -- 对方目标收费方式
    , P4.BANKNODE as MKTING_PSN_AFLT_BNKOUTLS_NO -- 银行网点号码
    , P1.CUSTMANAGERID as EACCT_MKTING_CUST_MGR_NO -- 客户经理代码
    , P1.OPERID as OPER_TELR_NO -- 操作柜员
    , P1.CONFIRMOPID1 as TXN_AUTH_TELR_NO_1 -- 复核柜员1
    , P1.CONFIRMOPID2 as TXN_AUTH_TELR_NO_2 -- 复核柜员2
    , P1.RETURNMSG as TXN_DEAL_RETURN_ERR_INFO_DESC -- 交易处理返回错误信息
    , P1.RETURNCODE as TXN_DEAL_AFT_RTN_CD -- 交易处理后返回码
    , '' as BIZ_PRDURE_CMPLT_END_CD -- None
    , P1.SPECIFICATION as SUMMARY_ILUS -- 摘要说明
from
    ${ods_tss_schema}.ods_tss_trust_h_app_trans as P1 -- 交易申请历史表
    LEFT JOIN ${ods_tss_schema}.ods_tss_trust_cfg_fund as P2 -- 基金信息
    on P1.UNIONCODE = P2.UNIONCODE 
AND P1.TANO=P2.TANO 
AND P1.FUNDCODE = P2.FUNDCODE
AND P2.PT_DT='${process_date}' 
AND P2.DELETED='0' 
    LEFT JOIN ${ods_tss_schema}.ods_tss_trust_acct_cust as P3 -- 客户信息表
    on p3.UNIONCODE = p1.UNIONCODE   
and p3.CUSTNO = p1.CUSTNO
and P3.PT_DT='${process_date}' 
AND P3.DELETED='0' 
    LEFT JOIN ${ods_tss_schema}.ods_tss_trust_acct_custmanager as P4 -- 客户经理信息
    on AND P4.UNIONCODE = P1.UNIONCODE  
AND P4.CUSTMANAGERID = P1.CUSTMANAGERID
AND P4.PT_DT='${process_date}' 
AND P4.DELETED='0' 
    LEFT JOIN 代码表待命名 as S1 -- 代码转换
    on P3.CURRENCYTYPE = S1.SRC_CD_VAL 
AND S1.SRC_SYS_CD = 'TSS'
AND S1.SRC_COL_NAME ='INVTP'
AND S1.SRC_TAB_NAME = 'ODS_TSS_TRUST_ACCT_CUST' 
    LEFT JOIN 代码表待命名 as S2 -- 代码转换
    on P3.CERTIFICATETYPE = S2.SRC_CD_VAL 
AND S2.SRC_SYS_CD = 'TSS'
AND S2.SRC_COL_NAME ='CERTIFICATETYPE'
AND S2.SRC_TAB_NAME = 'ODS_TSS_TRUST_ACCT_CUST' 
    LEFT JOIN 代码表待命名 as S3 -- 代码转换
    on P1.TRANSACTORCERTTYPE = S3.SRC_CD_VAL 
AND S3.SRC_SYS_CD = 'TSS'
AND S3.SRC_COL_NAME ='TRANSACTORCERTTYPE'
AND S3.SRC_TAB_NAME = 'ODS_TSS_TRUST_H_APP_TRANS' 
where P1.PT_DT='${process_date}' 
AND P1.DELETED='0'
AND NOT EXISTS ( 
		SELECT  1 
		FROM ${ods_tss_schema}.ods_tss_trust_h_ack_trans T1     --客户经理信息	
		WHERE 1 = 1
		AND T1.APPSHEETSERIALNO = P1.APPSHEETSERIALNO 
) 
;
-- ==============字段映射（第2组）==============
-- 代销信交易明细_数据组2
drop table if exists ${session}.TMP_S04_TRUST_TXN_DTL_02;

create table ${session}.TMP_S04_TRUST_TXN_DTL_02 (
      APP_FORM_SER_NO varchar(100) -- 申请单流水号
    , TRUST_BIZ_CFM_CD varchar(30) -- 信托业务确认代码
    , TA_SER_NO varchar(100) -- TA流水号
    , LP_ORG_NO varchar(100) -- 法人机构编号
    , TXN_HAPP_ORG_NO varchar(100) -- 交易发生机构编号
    , TRUST_BIZ_TYPE_CD varchar(30) -- 信托业务类别代码
    , APP_SHARE decimal(28,2) -- 申请份额
    , APP_AMT decimal(28,2) -- 申请金额
    , CFM_SHARE decimal(28,2) -- 确认份额
    , CFM_AMT decimal(28,2) -- 确认金额
    , TXN_DT varchar(10) -- 交易日期
    , TXN_TM varchar(100) -- 交易时间
    , ORDER_DT varchar(10) -- 下单日期
    , ORDER_TM varchar(100) -- 下单时间
    , CFM_DT varchar(10) -- 确认日期
    , STL_CURR_CD varchar(30) -- 结算币种代码
    , ORIG_APP_FORM_NO varchar(100) -- 原申请单编号
    , BANK_ACCT_NO varchar(100) -- 银行账户编号
    , TXN_HAPP_SELLER_NO varchar(100) -- 交易发生销售商编号
    , TA_CD varchar(100) -- TA代码
    , TA_ACCT_NO varchar(100) -- TA账户编号
    , TXN_ACCT_NO varchar(100) -- 交易账户编号
    , CUST_IN_CD varchar(100) -- 客户内码
    , CUST_TYPE_CD varchar(30) -- 客户类型代码
    , MAIN_DOCTYP_CD varchar(30) -- 主证件类型代码
    , MAIN_DOC_NO varchar(100) -- 主证件号码
    , OPERR_DOCTYP_CD varchar(30) -- 经办人证件类型代码
    , OPERR_DOC_NO varchar(100) -- 经办人证件号码
    , OPERR_NAME varchar(200) -- 经办人名称
    , SIGN_BNKOUTLS_NO varchar(100) -- 签约网点编号
    , OPEN_CARD_BNKOUTLS_NO varchar(100) -- 开卡网点编号
    , TRUST_PROD_NO varchar(100) -- 信托产品编号
    , TRUST_PROD_NAME varchar(500) -- 信托产品名称
    , TRUST_PROD_TYPE_CD varchar(30) -- 信托产品类别代码
    , TRUST_PROD_RISK_LVL_CD varchar(30) -- 信托产品风险等级代码
    , TRUST_PROD_NET_VAL decimal(20,7) -- 信托产品净值
    , YIELD_ACVMNT_COMP_BNCHMK_CEIL decimal(20,7) -- 收益率业绩比较基准上限
    , YIELD_ACVMNT_COMP_BNCHMK_FLOR decimal(20,7) -- 收益率业绩比较基准下限
    , TRUST_TXN_APP_STATUS_CD varchar(30) -- 信托交易申请状态代码
    , ACPTD_MODE_CD varchar(30) -- 受理方式代码
    , DIVDND_MODE_CD varchar(30) -- 分红方式代码
    , FRONT_BACK_END_CHARGE_MODE varchar(30) -- 前后端收费方式代码
    , FRZ_RSN_CD varchar(30) -- 冻结原因代码
    , FRZ_DEADLINE varchar(10) -- 冻结截止日期
    , HUGE_REDEM_DEAL_CD varchar(30) -- 巨额赎回处理代码
    , EXCEP_TXN_CD varchar(30) -- 异常交易代码
    , RISK_LVL_MATCH_FLAG varchar(10) -- 风险等级匹配标志
    , TXN_WITHDR_IDNT_CD varchar(30) -- 交易撤单标识代码
    , CNTPTY_TA_ACCT_NO varchar(100) -- 对方TA账户编号
    , CNTPTY_TXN_ACCT_NO varchar(100) -- 对方交易账户编号
    , CNTPTY_BNKOUTLS_NO varchar(100) -- 对方网点编号
    , CNTPTY_RGN_NO varchar(100) -- 对方地区编号
    , CNTPTY_SELLER_NO varchar(100) -- 对方销售商编号
    , CNTPTY_TARGET_PROD_NO varchar(100) -- 对方目标产品编号
    , CNTPTY_TARGET_CHARGE_MODE_CD varchar(30) -- 对方目标收费方式代码
    , MKTING_PSN_AFLT_BNKOUTLS_NO varchar(100) -- 营销人员所属网点编号
    , EACCT_MKTING_CUST_MGR_NO varchar(100) -- 电子账户营销客户经理编号
    , OPER_TELR_NO varchar(100) -- 操作柜员编号
    , TXN_AUTH_TELR_NO_1 varchar(100) -- 交易授权柜员1编号
    , TXN_AUTH_TELR_NO_2 varchar(100) -- 交易授权柜员2编号
    , TXN_DEAL_RETURN_ERR_INFO_DESC varchar(5000) -- 交易处理返回错误信息描述
    , TXN_DEAL_AFT_RTN_CD varchar(100) -- 交易处理后返回码
    , BIZ_PRDURE_CMPLT_END_CD varchar(30) -- 业务过程完全结束代码
    , SUMMARY_ILUS varchar(500) -- 摘要说明
)
comment '代销信交易明细_数据组2'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S04_TRUST_TXN_DTL_02(
      APP_FORM_SER_NO -- 申请单流水号
    , TRUST_BIZ_CFM_CD -- 信托业务确认代码
    , TA_SER_NO -- TA流水号
    , LP_ORG_NO -- 法人机构编号
    , TXN_HAPP_ORG_NO -- 交易发生机构编号
    , TRUST_BIZ_TYPE_CD -- 信托业务类别代码
    , APP_SHARE -- 申请份额
    , APP_AMT -- 申请金额
    , CFM_SHARE -- 确认份额
    , CFM_AMT -- 确认金额
    , TXN_DT -- 交易日期
    , TXN_TM -- 交易时间
    , ORDER_DT -- 下单日期
    , ORDER_TM -- 下单时间
    , CFM_DT -- 确认日期
    , STL_CURR_CD -- 结算币种代码
    , ORIG_APP_FORM_NO -- 原申请单编号
    , BANK_ACCT_NO -- 银行账户编号
    , TXN_HAPP_SELLER_NO -- 交易发生销售商编号
    , TA_CD -- TA代码
    , TA_ACCT_NO -- TA账户编号
    , TXN_ACCT_NO -- 交易账户编号
    , CUST_IN_CD -- 客户内码
    , CUST_TYPE_CD -- 客户类型代码
    , MAIN_DOCTYP_CD -- 主证件类型代码
    , MAIN_DOC_NO -- 主证件号码
    , OPERR_DOCTYP_CD -- 经办人证件类型代码
    , OPERR_DOC_NO -- 经办人证件号码
    , OPERR_NAME -- 经办人名称
    , SIGN_BNKOUTLS_NO -- 签约网点编号
    , OPEN_CARD_BNKOUTLS_NO -- 开卡网点编号
    , TRUST_PROD_NO -- 信托产品编号
    , TRUST_PROD_NAME -- 信托产品名称
    , TRUST_PROD_TYPE_CD -- 信托产品类别代码
    , TRUST_PROD_RISK_LVL_CD -- 信托产品风险等级代码
    , TRUST_PROD_NET_VAL -- 信托产品净值
    , YIELD_ACVMNT_COMP_BNCHMK_CEIL -- 收益率业绩比较基准上限
    , YIELD_ACVMNT_COMP_BNCHMK_FLOR -- 收益率业绩比较基准下限
    , TRUST_TXN_APP_STATUS_CD -- 信托交易申请状态代码
    , ACPTD_MODE_CD -- 受理方式代码
    , DIVDND_MODE_CD -- 分红方式代码
    , FRONT_BACK_END_CHARGE_MODE -- 前后端收费方式代码
    , FRZ_RSN_CD -- 冻结原因代码
    , FRZ_DEADLINE -- 冻结截止日期
    , HUGE_REDEM_DEAL_CD -- 巨额赎回处理代码
    , EXCEP_TXN_CD -- 异常交易代码
    , RISK_LVL_MATCH_FLAG -- 风险等级匹配标志
    , TXN_WITHDR_IDNT_CD -- 交易撤单标识代码
    , CNTPTY_TA_ACCT_NO -- 对方TA账户编号
    , CNTPTY_TXN_ACCT_NO -- 对方交易账户编号
    , CNTPTY_BNKOUTLS_NO -- 对方网点编号
    , CNTPTY_RGN_NO -- 对方地区编号
    , CNTPTY_SELLER_NO -- 对方销售商编号
    , CNTPTY_TARGET_PROD_NO -- 对方目标产品编号
    , CNTPTY_TARGET_CHARGE_MODE_CD -- 对方目标收费方式代码
    , MKTING_PSN_AFLT_BNKOUTLS_NO -- 营销人员所属网点编号
    , EACCT_MKTING_CUST_MGR_NO -- 电子账户营销客户经理编号
    , OPER_TELR_NO -- 操作柜员编号
    , TXN_AUTH_TELR_NO_1 -- 交易授权柜员1编号
    , TXN_AUTH_TELR_NO_2 -- 交易授权柜员2编号
    , TXN_DEAL_RETURN_ERR_INFO_DESC -- 交易处理返回错误信息描述
    , TXN_DEAL_AFT_RTN_CD -- 交易处理后返回码
    , BIZ_PRDURE_CMPLT_END_CD -- 业务过程完全结束代码
    , SUMMARY_ILUS -- 摘要说明
)
select
      P1.APPSHEETSERIALNO as APP_FORM_SER_NO -- 申请单编号
    , P1.BUSINESSCODE_APP as TRUST_BIZ_CFM_CD -- 业务代码(确认)
    , P1.TASERIALNO as TA_SER_NO -- TA确认流水号
    , P1.UNIONCODE as LP_ORG_NO -- 总行代码
    , P1.OPERORG as TXN_HAPP_ORG_NO -- 交易发生网点
    , P1.BUSINESSCODE as TRUST_BIZ_TYPE_CD -- 业务代码(确认)
    , P1.APPLICATIONVOL as APP_SHARE -- 申请基金份数初始申报
    , P1.APPLICATIONAMOUNT as APP_AMT -- 申请金额初始申报
    , P1.CONFIRMEDVOL as CFM_SHARE -- 交易确认份数管理人最终确认[巨额赎回处理的最终结果]
    , P1.CONFIRMEDAMOUNT as CFM_AMT -- 交易确认金额金额为全额管理人最终确认[比例配售的最终结果]
    , P1.TRANSACTIONDATE as TXN_DT -- 交易发生日期格式为：YYYYMMDD
    , P1.TRANSACTIONTIME as TXN_TM -- 交易发生时间格式为：HHMMSS
    , P1.OPERDATE as ORDER_DT -- 下单发生日期格式为：YYYYMMDD
    , P1.OPERTIME as ORDER_TM -- 下单发生时间格式为：HHMMSS
    , P1.TRANSACTIONCFMDATE as CFM_DT -- 交易确认日期格式为：YYYYMMDDY申请倒入后就计算好
    , CASE WHEN P1.CURRENCYTYPE IS NULL OR TRIM(P1.CURRENCYTYPE)='' 
     THEN '' 
ELSE COALESCE(TRIM(S4.TARGET_CD_VAL), '@'||TRIM(P1.CURRENCYTYPE),'') END  as STL_CURR_CD -- 结算币种
    , P1.ORIGINALAPPSHEETNO as ORIG_APP_FORM_NO -- 原申请单编号027业务使用，表示转出时的申请单编号
    , P1.DEPOSITACCT as BANK_ACCT_NO -- 投资人在销售人处用于交易的资金帐号
    , P1.DISTRIBUTORCODE as TXN_HAPP_SELLER_NO -- 销售商代码
    , P1.TANO as TA_CD -- TA代码
    , P1.TAACCOUNTID as TA_ACCT_NO -- 基金帐号
    , P1.TRANSACTIONACCOUNTID as TXN_ACCT_NO -- 交易帐号
    , P3.INNERCODE as CUST_IN_CD -- 客户内码
    , CASE WHEN P1.INVTP IS NULL OR TRIM(P1.INVTP)='' 
     THEN '' 
ELSE COALESCE(TRIM(S1.TARGET_CD_VAL), '@'||TRIM(P1.INVTP),'') END  as CUST_TYPE_CD -- 投资者类型
    , nvl(P3.INVTP,"")||'_'||nvl(P3.CERTIFICATETYPE,"") as MAIN_DOCTYP_CD -- 投资者类型/证件类型
    , P3.CERTIFICATENO as MAIN_DOC_NO -- 证件号码
    , CASE WHEN P5.TRANSACTORCERTTYPE IS NULL OR TRIM(P5.TRANSACTORCERTTYPE)='' 
     THEN '' 
ELSE COALESCE(TRIM(S3.TARGET_CD_VAL), '@'||TRIM(P5.TRANSACTORCERTTYPE),'') END 
 as OPERR_DOCTYP_CD -- 经办人证件类型
    , P5.TRANSACTORCERTNO as OPERR_DOC_NO -- 经办人证件号码
    , P5.TRANSACTORNAME as OPERR_NAME -- 经办人姓名
    , P3.NETNO as SIGN_BNKOUTLS_NO -- 签约网点
    , P1.BANKNO as OPEN_CARD_BNKOUTLS_NO -- 开卡网点
    , P1.FUNDCODE as TRUST_PROD_NO -- 基金代码
    , COALESCE(TRIM(P2.fundname),'') as TRUST_PROD_NAME -- 基金简称
    , P2.FUNDTYPE as TRUST_PROD_TYPE_CD -- 产品类型
    , COALESCE(TRIM(P2.RISKLEVEL),'') as TRUST_PROD_RISK_LVL_CD -- 风险等级
    , P1.NAV as TRUST_PROD_NET_VAL -- 基金单位净值
    , P2.INCOMEVALUEMIN as YIELD_ACVMNT_COMP_BNCHMK_CEIL -- 业绩比较基准下限
    , P2.INCOMEVALUEMAX as YIELD_ACVMNT_COMP_BNCHMK_FLOR -- 业绩比较基准上限
    , '07' as TRUST_TXN_APP_STATUS_CD -- None
    , P1.ACCEPTMETHOD as ACPTD_MODE_CD -- 受理方式
    , COALESCE(TRIM(P5.DEFDIVIDENDMETHOD),TRIM(P1.DEFDIVIDENDMETHOD)) as DIVDND_MODE_CD -- 默认分红方式
    , P1.SHARETYPE as FRONT_BACK_END_CHARGE_MODE -- 收费方式
    , P1.FROZENCAUSE as FRZ_RSN_CD -- 冻结原因
    , P1.FREEZINGDEADLINE as FRZ_DEADLINE -- 冻结截止日期格式为：YYYYMMDD
    , P1.LARGEREDEMPTIONFLAG as HUGE_REDEM_DEAL_CD -- 巨额赎回处理标志
    , P1.EXCEPTIONALFLAG as EXCEP_TXN_CD -- 异常交易标志
    , P1.RISKMATCHING as RISK_LVL_MATCH_FLAG -- 风险等级是否匹配
    , 'F' as TXN_WITHDR_IDNT_CD -- None
    , P1.TARGETTAACCOUNTID as CNTPTY_TA_ACCT_NO -- 对方基金账号
    , P1.TARGETTRANSACTIONACCOUNTID as CNTPTY_TXN_ACCT_NO -- 对方交易帐号在销售商是一次完成时，为必须项
    , P1.TARGETBRANCHCODE as CNTPTY_BNKOUTLS_NO -- 对方网点号转销售商、非交易过户时使用
    , P1.TARGETREGIONCODE as CNTPTY_RGN_NO -- 对方地区编号具体编码依GB13497-92
    , P1.TARGETDISTRIBUTORCODE as CNTPTY_SELLER_NO -- 对方销售商代码
    , P1.TARGETFUNDCODE as CNTPTY_TARGET_PROD_NO -- 对方目标基金代码
    , P1.TARGETSHARETYPE as CNTPTY_TARGET_CHARGE_MODE_CD -- 对方目标收费方式
    , P4.BANKNODE as MKTING_PSN_AFLT_BNKOUTLS_NO -- 银行网点号码
    , P1.CUSTMANAGERID as EACCT_MKTING_CUST_MGR_NO -- 客户经理代码
    , P1.OPERID as OPER_TELR_NO -- 操作柜员
    , P5.CONFIRMOPID1 as TXN_AUTH_TELR_NO_1 -- 复核柜员1
    , P5.CONFIRMOPID2 as TXN_AUTH_TELR_NO_2 -- 复核柜员2
    , P1.RETURNMSG as TXN_DEAL_RETURN_ERR_INFO_DESC -- 交易处理返回代码系统处理后返回错误信息
    , P1.RETURNCODE as TXN_DEAL_AFT_RTN_CD -- 交易处理返回代码系统处理后返回码
    , P1.BUSINESSFINISHFLAG as BIZ_PRDURE_CMPLT_END_CD -- 业务过程完全结束标识
    , P1.SPECIFICATION as SUMMARY_ILUS -- 摘要说明
from
    ${ods_tss_schema}.ods_tss_trust_h_ack_trans as P1 -- 交易确认表
    LEFT JOIN ${ods_tss_schema}.ods_tss_trust_cfg_fund as P2 -- 基金信息
    on P1.UNIONCODE = P2.UNIONCODE 
AND P1.TANO=P2.TANO 
AND P1.FUNDCODE = P2.FUNDCODE
AND P2.PT_DT='${process_date}' 
AND P2.DELETED='0' 
    LEFT JOIN ${ods_tss_schema}.ods_tss_trust_acct_cust as P3 -- 客户信息表
    on p3.UNIONCODE = p1.UNIONCODE   
and p3.CUSTNO = p1.CUSTNO
and P3.PT_DT='${process_date}' 
AND P3.DELETED='0' 
    LEFT JOIN ${ods_tss_schema}.ods_tss_trust_acct_custmanager as P4 -- 客户经理信息
    on AND P4.UNIONCODE = P1.UNIONCODE  
AND P4.CUSTMANAGERID = P1.CUSTMANAGERID
AND P4.PT_DT='${process_date}' 
AND P4.DELETED='0' 
    LEFT JOIN ${ods_tss_schema}.ods_tss_trust_h_app_trans as P5 -- 交易申请历史表
    on P5.APPSHEETSERIALNO = P1.APPSHEETSERIALNO 
    LEFT JOIN 代码表待命名 as S1 -- 代码转换
    on P1.INVTP = S1.SRC_CD_VAL 
AND S1.SRC_SYS_CD = 'TSS'
AND S1.SRC_COL_NAME ='INVTP'
AND S1.SRC_TAB_NAME = 'ODS_TSS_TRUST_H_ACK_TRANS' 
    LEFT JOIN 代码表待命名 as S2 -- 代码转换
    on P3.CERTIFICATETYPE = S2.SRC_CD_VAL 
AND S2.SRC_SYS_CD = 'TSS'
AND S2.SRC_COL_NAME ='CERTIFICATETYPE'
AND S2.SRC_TAB_NAME = 'ODS_TSS_TRUST_ACCT_CUST' 
    LEFT JOIN 代码表待命名 as S3 -- 代码转换
    on P5.TRANSACTORCERTTYPE = S3.SRC_CD_VAL 
AND S3.SRC_SYS_CD = 'TSS'
AND S3.SRC_COL_NAME ='TRANSACTORCERTTYPE'
AND S3.SRC_TAB_NAME = 'ODS_TSS_TRUST_H_APP_TRANS' 
    LEFT JOIN 代码表待命名 as S4 -- 代码转换
    on P1.CURRENCYTYPE = S4.SRC_CD_VAL 
AND S4.SRC_SYS_CD = 'TSS'
AND S4.SRC_COL_NAME ='CURRENCYTYPE'
AND S4.SRC_TAB_NAME = 'ODS_TSS_TRUST_H_ACK_TRANS' 
where P1.PT_DT='${process_date}' 
AND P1.DELETED='0'
;
-- ==============字段映射（第3组）==============
-- 代销信交易明细_数据组3
drop table if exists ${session}.TMP_S04_TRUST_TXN_DTL_03;

create table ${session}.TMP_S04_TRUST_TXN_DTL_03 (
      APP_FORM_SER_NO varchar(100) -- 申请单流水号
    , TRUST_BIZ_CFM_CD varchar(30) -- 信托业务确认代码
    , TA_SER_NO varchar(100) -- TA流水号
    , LP_ORG_NO varchar(100) -- 法人机构编号
    , TXN_HAPP_ORG_NO varchar(100) -- 交易发生机构编号
    , TRUST_BIZ_TYPE_CD varchar(30) -- 信托业务类别代码
    , APP_SHARE decimal(28,2) -- 申请份额
    , APP_AMT decimal(28,2) -- 申请金额
    , CFM_SHARE decimal(28,2) -- 确认份额
    , CFM_AMT decimal(28,2) -- 确认金额
    , TXN_DT varchar(10) -- 交易日期
    , TXN_TM varchar(100) -- 交易时间
    , ORDER_DT varchar(10) -- 下单日期
    , ORDER_TM varchar(100) -- 下单时间
    , CFM_DT varchar(10) -- 确认日期
    , STL_CURR_CD varchar(30) -- 结算币种代码
    , ORIG_APP_FORM_NO varchar(100) -- 原申请单编号
    , BANK_ACCT_NO varchar(100) -- 银行账户编号
    , TXN_HAPP_SELLER_NO varchar(100) -- 交易发生销售商编号
    , TA_CD varchar(100) -- TA代码
    , TA_ACCT_NO varchar(100) -- TA账户编号
    , TXN_ACCT_NO varchar(100) -- 交易账户编号
    , CUST_IN_CD varchar(100) -- 客户内码
    , CUST_TYPE_CD varchar(30) -- 客户类型代码
    , MAIN_DOCTYP_CD varchar(30) -- 主证件类型代码
    , MAIN_DOC_NO varchar(100) -- 主证件号码
    , OPERR_DOCTYP_CD varchar(30) -- 经办人证件类型代码
    , OPERR_DOC_NO varchar(100) -- 经办人证件号码
    , OPERR_NAME varchar(200) -- 经办人名称
    , SIGN_BNKOUTLS_NO varchar(100) -- 签约网点编号
    , OPEN_CARD_BNKOUTLS_NO varchar(100) -- 开卡网点编号
    , TRUST_PROD_NO varchar(100) -- 信托产品编号
    , TRUST_PROD_NAME varchar(500) -- 信托产品名称
    , TRUST_PROD_TYPE_CD varchar(30) -- 信托产品类别代码
    , TRUST_PROD_RISK_LVL_CD varchar(30) -- 信托产品风险等级代码
    , TRUST_PROD_NET_VAL decimal(20,7) -- 信托产品净值
    , YIELD_ACVMNT_COMP_BNCHMK_CEIL decimal(20,7) -- 收益率业绩比较基准上限
    , YIELD_ACVMNT_COMP_BNCHMK_FLOR decimal(20,7) -- 收益率业绩比较基准下限
    , TRUST_TXN_APP_STATUS_CD varchar(30) -- 信托交易申请状态代码
    , ACPTD_MODE_CD varchar(30) -- 受理方式代码
    , DIVDND_MODE_CD varchar(30) -- 分红方式代码
    , FRONT_BACK_END_CHARGE_MODE varchar(30) -- 前后端收费方式代码
    , FRZ_RSN_CD varchar(30) -- 冻结原因代码
    , FRZ_DEADLINE varchar(10) -- 冻结截止日期
    , HUGE_REDEM_DEAL_CD varchar(30) -- 巨额赎回处理代码
    , EXCEP_TXN_CD varchar(30) -- 异常交易代码
    , RISK_LVL_MATCH_FLAG varchar(10) -- 风险等级匹配标志
    , TXN_WITHDR_IDNT_CD varchar(30) -- 交易撤单标识代码
    , CNTPTY_TA_ACCT_NO varchar(100) -- 对方TA账户编号
    , CNTPTY_TXN_ACCT_NO varchar(100) -- 对方交易账户编号
    , CNTPTY_BNKOUTLS_NO varchar(100) -- 对方网点编号
    , CNTPTY_RGN_NO varchar(100) -- 对方地区编号
    , CNTPTY_SELLER_NO varchar(100) -- 对方销售商编号
    , CNTPTY_TARGET_PROD_NO varchar(100) -- 对方目标产品编号
    , CNTPTY_TARGET_CHARGE_MODE_CD varchar(30) -- 对方目标收费方式代码
    , MKTING_PSN_AFLT_BNKOUTLS_NO varchar(100) -- 营销人员所属网点编号
    , EACCT_MKTING_CUST_MGR_NO varchar(100) -- 电子账户营销客户经理编号
    , OPER_TELR_NO varchar(100) -- 操作柜员编号
    , TXN_AUTH_TELR_NO_1 varchar(100) -- 交易授权柜员1编号
    , TXN_AUTH_TELR_NO_2 varchar(100) -- 交易授权柜员2编号
    , TXN_DEAL_RETURN_ERR_INFO_DESC varchar(5000) -- 交易处理返回错误信息描述
    , TXN_DEAL_AFT_RTN_CD varchar(100) -- 交易处理后返回码
    , BIZ_PRDURE_CMPLT_END_CD varchar(30) -- 业务过程完全结束代码
    , SUMMARY_ILUS varchar(500) -- 摘要说明
)
comment '代销信交易明细_数据组3'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S04_TRUST_TXN_DTL_03(
      APP_FORM_SER_NO -- 申请单流水号
    , TRUST_BIZ_CFM_CD -- 信托业务确认代码
    , TA_SER_NO -- TA流水号
    , LP_ORG_NO -- 法人机构编号
    , TXN_HAPP_ORG_NO -- 交易发生机构编号
    , TRUST_BIZ_TYPE_CD -- 信托业务类别代码
    , APP_SHARE -- 申请份额
    , APP_AMT -- 申请金额
    , CFM_SHARE -- 确认份额
    , CFM_AMT -- 确认金额
    , TXN_DT -- 交易日期
    , TXN_TM -- 交易时间
    , ORDER_DT -- 下单日期
    , ORDER_TM -- 下单时间
    , CFM_DT -- 确认日期
    , STL_CURR_CD -- 结算币种代码
    , ORIG_APP_FORM_NO -- 原申请单编号
    , BANK_ACCT_NO -- 银行账户编号
    , TXN_HAPP_SELLER_NO -- 交易发生销售商编号
    , TA_CD -- TA代码
    , TA_ACCT_NO -- TA账户编号
    , TXN_ACCT_NO -- 交易账户编号
    , CUST_IN_CD -- 客户内码
    , CUST_TYPE_CD -- 客户类型代码
    , MAIN_DOCTYP_CD -- 主证件类型代码
    , MAIN_DOC_NO -- 主证件号码
    , OPERR_DOCTYP_CD -- 经办人证件类型代码
    , OPERR_DOC_NO -- 经办人证件号码
    , OPERR_NAME -- 经办人名称
    , SIGN_BNKOUTLS_NO -- 签约网点编号
    , OPEN_CARD_BNKOUTLS_NO -- 开卡网点编号
    , TRUST_PROD_NO -- 信托产品编号
    , TRUST_PROD_NAME -- 信托产品名称
    , TRUST_PROD_TYPE_CD -- 信托产品类别代码
    , TRUST_PROD_RISK_LVL_CD -- 信托产品风险等级代码
    , TRUST_PROD_NET_VAL -- 信托产品净值
    , YIELD_ACVMNT_COMP_BNCHMK_CEIL -- 收益率业绩比较基准上限
    , YIELD_ACVMNT_COMP_BNCHMK_FLOR -- 收益率业绩比较基准下限
    , TRUST_TXN_APP_STATUS_CD -- 信托交易申请状态代码
    , ACPTD_MODE_CD -- 受理方式代码
    , DIVDND_MODE_CD -- 分红方式代码
    , FRONT_BACK_END_CHARGE_MODE -- 前后端收费方式代码
    , FRZ_RSN_CD -- 冻结原因代码
    , FRZ_DEADLINE -- 冻结截止日期
    , HUGE_REDEM_DEAL_CD -- 巨额赎回处理代码
    , EXCEP_TXN_CD -- 异常交易代码
    , RISK_LVL_MATCH_FLAG -- 风险等级匹配标志
    , TXN_WITHDR_IDNT_CD -- 交易撤单标识代码
    , CNTPTY_TA_ACCT_NO -- 对方TA账户编号
    , CNTPTY_TXN_ACCT_NO -- 对方交易账户编号
    , CNTPTY_BNKOUTLS_NO -- 对方网点编号
    , CNTPTY_RGN_NO -- 对方地区编号
    , CNTPTY_SELLER_NO -- 对方销售商编号
    , CNTPTY_TARGET_PROD_NO -- 对方目标产品编号
    , CNTPTY_TARGET_CHARGE_MODE_CD -- 对方目标收费方式代码
    , MKTING_PSN_AFLT_BNKOUTLS_NO -- 营销人员所属网点编号
    , EACCT_MKTING_CUST_MGR_NO -- 电子账户营销客户经理编号
    , OPER_TELR_NO -- 操作柜员编号
    , TXN_AUTH_TELR_NO_1 -- 交易授权柜员1编号
    , TXN_AUTH_TELR_NO_2 -- 交易授权柜员2编号
    , TXN_DEAL_RETURN_ERR_INFO_DESC -- 交易处理返回错误信息描述
    , TXN_DEAL_AFT_RTN_CD -- 交易处理后返回码
    , BIZ_PRDURE_CMPLT_END_CD -- 业务过程完全结束代码
    , SUMMARY_ILUS -- 摘要说明
)
select
      P1.APPSHEETSERIALNO as APP_FORM_SER_NO -- 申请单编号
    , P1.BUSINESSCODE_APP as TRUST_BIZ_CFM_CD -- 业务代码(申请)
    , '' as TA_SER_NO -- None
    , P1.UNIONCODE as LP_ORG_NO -- 总行代码
    , P1.OPERORG as TXN_HAPP_ORG_NO -- 交易发生网点
    , P1.BUSINESSCODE as TRUST_BIZ_TYPE_CD -- 业务代码
    , P1.APPLICATIONVOL as APP_SHARE -- 申请份额
    , P1.APPLICATIONAMOUNT as APP_AMT -- 申请金额
    , '0' as CFM_SHARE -- None
    , '0' as CFM_AMT -- None
    , P1.TRANSACTIONDATE as TXN_DT -- 交易发生日期格式为：YYYYMMDD
    , P1.TRANSACTIONTIME as TXN_TM -- 交易发生时间格式为：HHMMSS
    , P1.OPERDATE as ORDER_DT -- 下单发生日期格式为：YYYYMMDD
    , P1.OPERTIME as ORDER_TM -- 下单发生时间格式为：HHMMSS
    , P1.TRANSACTIONCFMDATE as CFM_DT -- 交易确认日期格式为：YYYYMMDD
    , CASE WHEN P1.CURRENCYTYPE IS NULL OR TRIM(P1.CURRENCYTYPE)='' 
     THEN '' 
ELSE COALESCE(TRIM(S4.TARGET_CD_VAL), '@'||TRIM(P1.CURRENCYTYPE),'') END  as STL_CURR_CD -- 结算币种
    , P1.ORIGINALAPPSHEETNO as ORIG_APP_FORM_NO -- 原申请单编号027业务使用，表示转出时的申请单编号
    , P1.DEPOSITACCT as BANK_ACCT_NO -- 投资人在销售人处用于交易的资金帐号
    , P1.DISTRIBUTORCODE as TXN_HAPP_SELLER_NO -- 销售商代码
    , P1.TANO as TA_CD -- TA代码
    , P1.TAACCOUNTID as TA_ACCT_NO -- 基金帐号
    , P1.TRANSACTIONACCOUNTID as TXN_ACCT_NO -- 交易帐号
    , P3.INNERCODE as CUST_IN_CD -- 客户内码
    , CASE WHEN P3.INVTP IS NULL OR TRIM(P3.INVTP)='' 
     THEN '' 
ELSE COALESCE(TRIM(S1.TARGET_CD_VAL), '@'||TRIM(P3.INVTP),'') END  as CUST_TYPE_CD -- 投资者类型
    , nvl(P3.INVTP,"")||'_'||nvl(P3.CERTIFICATETYPE,"") as MAIN_DOCTYP_CD -- 投资者类型/证件类型
    , P3.CERTIFICATENO as MAIN_DOC_NO -- 证件号码
    , P1.TRANSACTORCERTTYPE as OPERR_DOCTYP_CD -- 经办人证件类型
    , P1.TRANSACTORCERTNO as OPERR_DOC_NO -- 经办人证件号码
    , P1.TRANSACTORNAME as OPERR_NAME -- 经办人姓名
    , P1.NETNO as SIGN_BNKOUTLS_NO -- 签约网点
    , P1.BANKNO as OPEN_CARD_BNKOUTLS_NO -- 开卡网点
    , P1.FUNDCODE as TRUST_PROD_NO -- 基金代码
    , P2.FUNDNAME as TRUST_PROD_NAME -- 基金简称
    , P2.FUNDTYPE as TRUST_PROD_TYPE_CD -- 产品类型
    , P2.RISKLEVEL as TRUST_PROD_RISK_LVL_CD -- 风险等级
    , P2.INCOMEVALUEMIN as YIELD_ACVMNT_COMP_BNCHMK_CEIL -- 业绩比较基准下限
    , P2.INCOMEVALUEMAX as YIELD_ACVMNT_COMP_BNCHMK_FLOR -- 业绩比较基准上限
    , P1.STATUS as TRUST_TXN_APP_STATUS_CD -- 申请状态
    , P1.ACCEPTMETHOD as ACPTD_MODE_CD -- 受理方式
    , P1.DEFDIVIDENDMETHOD as DIVDND_MODE_CD -- 默认分红方式
    , P1.SHARETYPE as FRONT_BACK_END_CHARGE_MODE -- 收费方式
    , P1.FROZENCAUSE as FRZ_RSN_CD -- 冻结原因
    , P1.FREEZINGDEADLINE as FRZ_DEADLINE -- 冻结截止日期格式为：YYYYMMDD
    , P1.LARGEREDEMPTIONFLAG as HUGE_REDEM_DEAL_CD -- 巨额赎回处理标志
    , P1.EXCEPTIONALFLAG as EXCEP_TXN_CD -- 异常交易标志
    , P1.RISKMATCHING as RISK_LVL_MATCH_FLAG -- 风险等级是否匹配
    , P1.CANCELFLAG as TXN_WITHDR_IDNT_CD -- 撤单标志
    , P1.TARGETTAACCOUNTID as CNTPTY_TA_ACCT_NO -- 对方基金账号
    , P1.TARGETTRANSACTIONACCOUNTID as CNTPTY_TXN_ACCT_NO -- 对方交易帐号在销售商是一次完成时，为必须项
    , P1.TARGETBRANCHCODE as CNTPTY_BNKOUTLS_NO -- 对方网点号转销售商、非交易过户时使用
    , P1.TARGETREGIONCODE as CNTPTY_RGN_NO -- 对方地区编号具体编码依GB13497-92
    , P1.TARGETDISTRIBUTORCODE as CNTPTY_SELLER_NO -- 对方销售商代码
    , P1.TARGETFUNDCODE as CNTPTY_TARGET_PROD_NO -- 对方目标基金代码
    , P1.TARGETSHARETYPE as CNTPTY_TARGET_CHARGE_MODE_CD -- 对方目标收费方式
    , P4.BANKNODE as MKTING_PSN_AFLT_BNKOUTLS_NO -- 银行网点号码
    , P1.CUSTMANAGERID as EACCT_MKTING_CUST_MGR_NO -- 客户经理代码
    , P1.OPERID as OPER_TELR_NO -- 操作柜员
    , P1.CONFIRMOPID1 as TXN_AUTH_TELR_NO_1 -- 复核柜员1
    , P1.CONFIRMOPID2 as TXN_AUTH_TELR_NO_2 -- 复核柜员2
    , P1.RETURNMSG as TXN_DEAL_RETURN_ERR_INFO_DESC -- 交易处理返回错误信息
    , P1.RETURNCODE as TXN_DEAL_AFT_RTN_CD -- 交易处理后返回码
    , '' as BIZ_PRDURE_CMPLT_END_CD -- None
    , P1.SPECIFICATION as SUMMARY_ILUS -- 摘要说明
from
    ${ods_tss_schema}.ods_tss_trust_h_app_trans as P1 -- 交易申请历史表
    LEFT JOIN ${ods_tss_schema}.ods_tss_trust_cfg_fund as P2 -- 基金信息
    on P1.UNIONCODE = P2.UNIONCODE 
AND P1.TANO=P2.TANO 
AND P1.FUNDCODE = P2.FUNDCODE
AND P2.PT_DT='${process_date}' 
AND P2.DELETED='0' 
    LEFT JOIN ${ods_tss_schema}.ods_tss_trust_acct_cust as P3 -- 客户信息表
    on p3.UNIONCODE = p1.UNIONCODE   
and p3.CUSTNO = p1.CUSTNO
and P3.PT_DT='${process_date}' 
AND P3.DELETED='0' 
    LEFT JOIN ${ods_tss_schema}.ods_tss_trust_acct_custmanager as P4 -- 客户经理信息
    on AND P4.UNIONCODE = P1.UNIONCODE  
AND P4.CUSTMANAGERID = P1.CUSTMANAGERID
AND P4.PT_DT='${process_date}' 
AND P4.DELETED='0' 
    LEFT JOIN 代码表待命名 as S1 -- 代码转换
    on P3.INVTP = S1.SRC_CD_VAL 
AND S1.SRC_SYS_CD = 'TSS'
AND S1.SRC_COL_NAME ='INVTP'
AND S1.SRC_TAB_NAME = 'ODS_TSS_TRUST_ACCT_CUST' 
    LEFT JOIN 代码表待命名 as S2 -- 代码转换
    on P3.CERTIFICATETYPE = S2.SRC_CD_VAL 
AND S2.SRC_SYS_CD = 'TSS'
AND S2.SRC_COL_NAME ='CERTIFICATETYPE'
AND S2.SRC_TAB_NAME = 'ODS_TSS_TRUST_ACCT_CUST' 
    LEFT JOIN 代码表待命名 as S4 -- 代码转换
    on P1.CURRENCYTYPE = S4.SRC_CD_VAL 
AND S4.SRC_SYS_CD = 'TSS'
AND S4.SRC_COL_NAME ='CURRENCYTYPE'
AND S4.SRC_TAB_NAME = 'ODS_TSS_TRUST_H_ACK_TRANS' 
where P1.PT_DT='${process_date}' 
AND P1.DELETED='0'
;
-- ==============字段映射（第4组）==============
-- 代销信交易明细_数据汇总

insert into table ${session}.S04_TRUST_TXN_DTL(
      APP_FORM_SER_NO -- 申请单流水号
    , TRUST_BIZ_CFM_CD -- 信托业务确认代码
    , TA_SER_NO -- TA流水号
    , LP_ORG_NO -- 法人机构编号
    , TXN_HAPP_ORG_NO -- 交易发生机构编号
    , TRUST_BIZ_TYPE_CD -- 信托业务类别代码
    , APP_SHARE -- 申请份额
    , APP_AMT -- 申请金额
    , CFM_SHARE -- 确认份额
    , CFM_AMT -- 确认金额
    , TXN_DT -- 交易日期
    , TXN_TM -- 交易时间
    , ORDER_DT -- 下单日期
    , ORDER_TM -- 下单时间
    , CFM_DT -- 确认日期
    , STL_CURR_CD -- 结算币种代码
    , ORIG_APP_FORM_NO -- 原申请单编号
    , BANK_ACCT_NO -- 银行账户编号
    , TXN_HAPP_SELLER_NO -- 交易发生销售商编号
    , TA_CD -- TA代码
    , TA_ACCT_NO -- TA账户编号
    , TXN_ACCT_NO -- 交易账户编号
    , CUST_IN_CD -- 客户内码
    , CUST_TYPE_CD -- 客户类型代码
    , MAIN_DOCTYP_CD -- 主证件类型代码
    , MAIN_DOC_NO -- 主证件号码
    , OPERR_DOCTYP_CD -- 经办人证件类型代码
    , OPERR_DOC_NO -- 经办人证件号码
    , OPERR_NAME -- 经办人名称
    , SIGN_BNKOUTLS_NO -- 签约网点编号
    , OPEN_CARD_BNKOUTLS_NO -- 开卡网点编号
    , TRUST_PROD_NO -- 信托产品编号
    , TRUST_PROD_NAME -- 信托产品名称
    , TRUST_PROD_TYPE_CD -- 信托产品类别代码
    , TRUST_PROD_RISK_LVL_CD -- 信托产品风险等级代码
    , TRUST_PROD_NET_VAL -- 信托产品净值
    , YIELD_ACVMNT_COMP_BNCHMK_CEIL -- 收益率业绩比较基准上限
    , YIELD_ACVMNT_COMP_BNCHMK_FLOR -- 收益率业绩比较基准下限
    , TRUST_TXN_APP_STATUS_CD -- 信托交易申请状态代码
    , ACPTD_MODE_CD -- 受理方式代码
    , DIVDND_MODE_CD -- 分红方式代码
    , FRONT_BACK_END_CHARGE_MODE -- 前后端收费方式代码
    , FRZ_RSN_CD -- 冻结原因代码
    , FRZ_DEADLINE -- 冻结截止日期
    , HUGE_REDEM_DEAL_CD -- 巨额赎回处理代码
    , EXCEP_TXN_CD -- 异常交易代码
    , RISK_LVL_MATCH_FLAG -- 风险等级匹配标志
    , TXN_WITHDR_IDNT_CD -- 交易撤单标识代码
    , CNTPTY_TA_ACCT_NO -- 对方TA账户编号
    , CNTPTY_TXN_ACCT_NO -- 对方交易账户编号
    , CNTPTY_BNKOUTLS_NO -- 对方网点编号
    , CNTPTY_RGN_NO -- 对方地区编号
    , CNTPTY_SELLER_NO -- 对方销售商编号
    , CNTPTY_TARGET_PROD_NO -- 对方目标产品编号
    , CNTPTY_TARGET_CHARGE_MODE_CD -- 对方目标收费方式代码
    , MKTING_PSN_AFLT_BNKOUTLS_NO -- 营销人员所属网点编号
    , EACCT_MKTING_CUST_MGR_NO -- 电子账户营销客户经理编号
    , OPER_TELR_NO -- 操作柜员编号
    , TXN_AUTH_TELR_NO_1 -- 交易授权柜员1编号
    , TXN_AUTH_TELR_NO_2 -- 交易授权柜员2编号
    , TXN_DEAL_RETURN_ERR_INFO_DESC -- 交易处理返回错误信息描述
    , TXN_DEAL_AFT_RTN_CD -- 交易处理后返回码
    , BIZ_PRDURE_CMPLT_END_CD -- 业务过程完全结束代码
    , SUMMARY_ILUS -- 摘要说明
    , PT_DT -- 数据日期
)
select
      APP_FORM_SER_NO as APP_FORM_SER_NO -- 申请单流水号
    , 业务术语待申请 as TRUST_BIZ_CFM_CD -- 信托业务确认代码
    , TA_SER_NO as TA_SER_NO -- TA流水号
    , LP_ORG_NO as LP_ORG_NO -- 法人机构编号
    , 业务术语待申请 as TXN_HAPP_ORG_NO -- 交易发生机构编号
    , 业务术语待申请 as TRUST_BIZ_TYPE_CD -- 信托业务类别代码
    , APP_SHARE as APP_SHARE -- 申请份额
    , APP_AMT as APP_AMT -- 申请金额
    , CFM_SHARE as CFM_SHARE -- 确认份额
    , CFM_AMT as CFM_AMT -- 确认金额
    , 业务术语待申请 as TXN_DT -- 交易发生日期
    , 业务术语待申请 as TXN_TM -- 交易发生时间
    , 业务术语待申请 as ORDER_DT -- 下单发生日期
    , 业务术语待申请 as ORDER_TM -- 下单发生时间
    , CFM_DT as CFM_DT -- 确认日期
    , STL_CURR_CD as STL_CURR_CD -- 结算币种代码
    , ORIG_APP_FORM_NO as ORIG_APP_FORM_NO -- 原申请单编号
    , 业务术语待申请 as BANK_ACCT_NO -- 银行账号
    , TXN_HAPP_SELLER_NO as TXN_HAPP_SELLER_NO -- 交易发生销售商编号
    , 业务术语待申请 as TA_CD -- TA编号
    , TA_ACCT_NO as TA_ACCT_NO -- TA账户编号
    , 业务术语待申请 as TXN_ACCT_NO -- 交易账号
    , CUST_IN_CD as CUST_IN_CD -- 客户内码
    , 业务术语待申请 as CUST_TYPE_CD -- 客户类型代码
    , MAIN_DOCTYP_CD as MAIN_DOCTYP_CD -- 主证件类型代码
    , MAIN_DOC_NO as MAIN_DOC_NO -- 主证件号码
    , OPERR_DOCTYP_CD as OPERR_DOCTYP_CD -- 经办人证件类型代码
    , OPERR_DOC_NO as OPERR_DOC_NO -- 经办人证件号码
    , OPERR_NAME as OPERR_NAME -- 经办人名称
    , SIGN_BNKOUTLS_NO as SIGN_BNKOUTLS_NO -- 签约网点编号
    , OPEN_CARD_BNKOUTLS_NO as OPEN_CARD_BNKOUTLS_NO -- 开卡网点编号
    , 业务术语待申请 as TRUST_PROD_NO -- 信托产品代码
    , 业务术语待申请 as TRUST_PROD_NAME -- 信托产品名称
    , 业务术语待申请 as TRUST_PROD_TYPE_CD -- 信托产品类别代码
    , 业务术语待申请 as TRUST_PROD_RISK_LVL_CD -- 信托产品风险等级代码
    , 业务术语待申请 as TRUST_PROD_NET_VAL -- 信托产品净值
    , 业务术语待申请 as YIELD_ACVMNT_COMP_BNCHMK_CEIL -- 收益率业绩比较基准上限
    , 业务术语待申请 as YIELD_ACVMNT_COMP_BNCHMK_FLOR -- 收益率业绩比较基准下限
    , 业务术语待申请 as TRUST_TXN_APP_STATUS_CD -- 信托交易申请状态代码
    , ACPTD_MODE_CD as ACPTD_MODE_CD -- 受理方式代码
    , DIVDND_MODE_CD as DIVDND_MODE_CD -- 分红方式代码
    , 业务术语待申请 as FRONT_BACK_END_CHARGE_MODE -- 信托收费方式代码
    , FRZ_RSN_CD as FRZ_RSN_CD -- 冻结原因代码
    , 业务术语待申请 as FRZ_DEADLINE -- 冻结截止日期
    , 业务术语待申请 as HUGE_REDEM_DEAL_CD -- 巨额赎回处理代码
    , 业务术语待申请 as EXCEP_TXN_CD -- 异常交易代码
    , 业务术语待申请 as RISK_LVL_MATCH_FLAG -- 风险等级匹配标志
    , 业务术语待申请 as TXN_WITHDR_IDNT_CD -- 交易撤单标识代码
    , 业务术语待申请 as CNTPTY_TA_ACCT_NO -- 对方TA账号
    , 业务术语待申请 as CNTPTY_TXN_ACCT_NO -- 对方交易账号
    , 业务术语待申请 as CNTPTY_BNKOUTLS_NO -- 对方网点编号
    , 业务术语待申请 as CNTPTY_RGN_NO -- 对方地区编号
    , 业务术语待申请 as CNTPTY_SELLER_NO -- 对方销售商编号
    , 业务术语待申请 as CNTPTY_TARGET_PROD_NO -- 对方目标产品代码
    , 业务术语待申请 as CNTPTY_TARGET_CHARGE_MODE_CD -- 对方目标收费方式代码
    , 业务术语待申请 as MKTING_PSN_AFLT_BNKOUTLS_NO -- 营销人员所属网点编号
    , EACCT_MKTING_CUST_MGR_NO as EACCT_MKTING_CUST_MGR_NO -- 电子账户营销客户经理编号
    , OPER_TELR_NO as OPER_TELR_NO -- 操作柜员编号
    , TXN_AUTH_TELR_1_NO as TXN_AUTH_TELR_NO_1 -- 交易授权柜员1编号
    , TXN_AUTH_TELR_2_NO as TXN_AUTH_TELR_NO_2 -- 交易授权柜员2编号
    , 业务术语待申请 as TXN_DEAL_RETURN_ERR_INFO_DESC -- 交易处理返回错误信息
    , 业务术语待申请 as TXN_DEAL_AFT_RTN_CD -- 交易处理后返回码
    , 业务术语待申请 as BIZ_PRDURE_CMPLT_END_CD -- 业务过程完全结束代码
    , 业务术语待申请 as SUMMARY_ILUS -- 摘要说明
    , '${process_date}' as PT_DT -- None
from
    (
SELECT P1.* FROM 
TMP_S04_TRUST_TXN_DTL_01 P1 
UNION ALL 
SELECT P2.* FROM 
TMP_S04_TRUST_TXN_DTL_02 P2
SELECT P3.* FROM 
TMP_S04_TRUST_TXN_DTL_03 P3 ) as T1 -- 临时表聚合信息
;

-- 删除所有临时表
drop table ${session}.TMP_S04_TRUST_TXN_DTL_01;
drop table ${session}.TMP_S04_TRUST_TXN_DTL_02;
drop table ${session}.TMP_S04_TRUST_TXN_DTL_03;