-- 层次表名: 聚合层-公司贷款借据台账聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s03_lp_loan_dubil_stding_book_tab
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S03_LP_LOAN_DUBIL_STDING_BOOK_TAB
--     表中文名：公司贷款借据台账聚合表
--     创建日期：2023-12-28 00:00:00
--     主键字段：LOAN_CONT_NO,DUBIL_NO
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：业务范围涵盖资产负债表内对公贷款产品的借据信息，如农业工业贷款、国际/国内融资、垫款、透支账户等。
字段范围包括借据的基本信息、利率利息、合同信息、客户信息、归属信息等。
--     更新记录：
--         2023-12-28 00:00:00 王穆军 新增映射文档信息
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S03_LP_LOAN_DUBIL_STDING_BOOK_TAB drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 取折算币种汇率数据
drop table if exists ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_00;

create table ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_00 (
      YRCACCYC varchar(9) -- 币种
    , YRCAEXRT decimal(16,8) -- 年终报表汇率
)
comment '取折算币种汇率数据'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_00(
      YRCACCYC -- 币种
    , YRCAEXRT -- 年终报表汇率
)
select
      T1.YRCACCYC as YRCACCYC -- 币种
    , T1.YRCAEXRT as YRCAEXRT -- 年终报表汇率
from
    LEFT JOIN (
SELECT 
	 P1.YRCACCYC --币种
	,P1.YRCADATE
	,P1.YRCAEXRT
	,P1.YRCBSTAM
	ROW_NUMBER()OVER(PARTITION BY P1.YRCACCYC 
	ORDER BY P1.YRCBSTAM DESC) RN 
FROM ${ODS_CORE_SCHEMA}.AFFMYRPT P1
WHERE  T1.PT_DT='${process_date}' 
AND T1.DELETED='0' 
) as T1 -- 年度报表汇率文件
where 1=1 
AND T1.RN = 1 
;
-- ==============字段映射（第2组）==============
-- 柜面核心数据组整合数据
drop table if exists ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_01;

create table ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_01 (
      STAACONO varchar(16) -- 贷款合同编号
    , STABSN03 varchar(3) -- 借据编号
    , LNACCT_NO varchar(15) -- 贷款账户编号
    , CURR_CD varchar(3) -- 币种代码
    , STAACSNO varchar(11) -- 借款人客户内码
    , BORR_CUST_NO varchar(23) -- 借款人客户号
    , BORR_CUST_NAME varchar(100) -- 借款人客户名称
    , LOANPROD_NO varchar(5) -- 贷款产品编号
    , LOANPROD_NAME varchar(80) -- 贷款产品名称
    , LOAN_AMT decimal(20,2) -- 贷款发放金额
    , LOAN_BAL decimal(20,2) -- 贷款余额
    , CONVT_RMB_LOAN_BAL decimal(20,2) -- 折人民币贷款余额
    , YESTD_LOAN_BAL decimal(20,2) -- 昨日贷款余额
    , LOAN_DT date -- 贷款发放日期
    , LOAN_MATU_DT date -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD varchar(1) -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD varchar(1) -- 当前贷款十级形态代码
    , LNACCT_STATUS_CD varchar(1) -- 贷款账户状态代码
    , COMUT_DEBT_STATUS_CD varchar(1) -- 抵债状态代码
    , DISPLC_STATUS_CD varchar(1) -- 置换状态代码
    , TRAN_STATUS_CD varchar(1) -- 转让状态代码
    , LOAN_AST_SECUZT_STATUS_CD varchar(1) -- 贷款资产证券化状态代码
    , LOAN_CONT_INT_RATE_NO varchar(8) -- 贷款合同利率编号
    , LOAN_CONT_INT_RATE_ADJ_MODE_CD varchar(1) -- 贷款合同利率调整方式代码
    , NORMAL_EXC_INT_RATE decimal(20,7) -- 正常执行利率
    , OVDUE_EXC_INT_RATE decimal(20,7) -- 逾期执行利率
    , CRDT_DEFLT_INT_RATE decimal(20,7) -- 贷方违约利率
    , OCCUPY_APPRO_FLAG varchar(1) -- 挤占挪用标志
    , OCCUPY_APPRO_EXC_INT_RATE decimal(20,7) -- 挤占挪用执行利率
    , INT_RATE_FLOT_RATIO decimal(20,7) -- 利率浮动比
    , INTACR_FLAG varchar(1) -- 计息标志
    , COMPND_INT_FLAG varchar(1) -- 计复息标志
    , ACCTN_SUBJ_NO varchar(8) -- 会计科目编号
    , LOAN_CONT_AGREE_SETMENT_DAY varchar(2) -- 贷款合同约定结息日
    , LOAN_CONT_SETMENT_PERIOD_CD varchar(2) -- 贷款合同结息周期代码
    , MATU_AUTO_REPAY_FLAG varchar(1) -- 到期自动还款标志
    , OVDUE_DEBIT_INT_AUTO_REPAY_FLAG varchar(1) -- 逾期欠息自动还款标志
    , DUBIL_LAST_REPAY_DT date -- 借据上次还款日期
    , DUBIL_LAST_SETMENT_DT date -- 借据上次结息日期
    , LOAN_CONT_LOAN_MODE_CD varchar(1) -- 贷款合同放款方式代码
    , LOAN_REPAY_GRACE_TERM varchar(2) -- 贷款还款宽限期限
    , WRTOFF_STATUS_CD varchar(1) -- 核销状态代码
    , OFBS_BAL decimal(20,2) -- 表外余额
    , LOAN_GRP_CD varchar(1) -- 贷款分组代码
    , LOAN_CONT_CAP_SRC_CD varchar(1) -- 贷款合同资金来源代码
    , LOAN_CONT_TERM_CD varchar(1) -- 贷款合同期限代码
    , BORR_TYPE_CD varchar(2) -- 借款人类型代码
    , CUST_CRDT_CD varchar(1) -- 客户信用等级代码
    , BANK_ENTERP_FLAG varchar(1) -- 银企标志
    , REVOLV_LOAN_FLAG varchar(1) -- 循环贷款标志
    , CONVT_RMB_LOAN_APP_AMT decimal(20,2) -- 折人民币贷款申请金额
    , LOAN_CONT_GUAR_MODE_CD varchar(3) -- 贷款合同担保方式代码
    , LOAN_USAGE_CD varchar(3) -- 贷款用途代码
    , LOAN_INDUS_INVEST_CD varchar(4) -- 贷款行业投向代码
    , EXT_CONT_NO varchar(16) -- 展期合同编号
    , EXT_TMS varchar(3) -- 展期次数
    , LOAN_BELONG_ORG_NO varchar(6) -- 贷款归属机构编号
    , LOAN_BELONG_CUST_MGR_TELR_NO varchar(7) -- 贷款归属客户经理柜员编号
    , SETUP_ORG_NO varchar(6) -- 建立机构编号
    , SETUP_TELR_NO varchar(7) -- 建立柜员编号
    , SETUP_DT date -- 建立日期
    , MODIF_ORG_NO varchar(6) -- 修改机构编号
    , MODIF_TELR_NO varchar(7) -- 修改柜员编号
    , FINAL_MODIF_DT date -- 最后修改日期
    , PT_DT  varchar(10) -- 数据日期
)
comment '柜面核心数据组整合数据'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_01(
      STAACONO -- 贷款合同编号
    , STABSN03 -- 借据编号
    , LNACCT_NO -- 贷款账户编号
    , CURR_CD -- 币种代码
    , STAACSNO -- 借款人客户内码
    , BORR_CUST_NO -- 借款人客户号
    , BORR_CUST_NAME -- 借款人客户名称
    , LOANPROD_NO -- 贷款产品编号
    , LOANPROD_NAME -- 贷款产品名称
    , LOAN_AMT -- 贷款发放金额
    , LOAN_BAL -- 贷款余额
    , CONVT_RMB_LOAN_BAL -- 折人民币贷款余额
    , YESTD_LOAN_BAL -- 昨日贷款余额
    , LOAN_DT -- 贷款发放日期
    , LOAN_MATU_DT -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
    , LNACCT_STATUS_CD -- 贷款账户状态代码
    , COMUT_DEBT_STATUS_CD -- 抵债状态代码
    , DISPLC_STATUS_CD -- 置换状态代码
    , TRAN_STATUS_CD -- 转让状态代码
    , LOAN_AST_SECUZT_STATUS_CD -- 贷款资产证券化状态代码
    , LOAN_CONT_INT_RATE_NO -- 贷款合同利率编号
    , LOAN_CONT_INT_RATE_ADJ_MODE_CD -- 贷款合同利率调整方式代码
    , NORMAL_EXC_INT_RATE -- 正常执行利率
    , OVDUE_EXC_INT_RATE -- 逾期执行利率
    , CRDT_DEFLT_INT_RATE -- 贷方违约利率
    , OCCUPY_APPRO_FLAG -- 挤占挪用标志
    , OCCUPY_APPRO_EXC_INT_RATE -- 挤占挪用执行利率
    , INT_RATE_FLOT_RATIO -- 利率浮动比
    , INTACR_FLAG -- 计息标志
    , COMPND_INT_FLAG -- 计复息标志
    , ACCTN_SUBJ_NO -- 会计科目编号
    , LOAN_CONT_AGREE_SETMENT_DAY -- 贷款合同约定结息日
    , LOAN_CONT_SETMENT_PERIOD_CD -- 贷款合同结息周期代码
    , MATU_AUTO_REPAY_FLAG -- 到期自动还款标志
    , OVDUE_DEBIT_INT_AUTO_REPAY_FLAG -- 逾期欠息自动还款标志
    , DUBIL_LAST_REPAY_DT -- 借据上次还款日期
    , DUBIL_LAST_SETMENT_DT -- 借据上次结息日期
    , LOAN_CONT_LOAN_MODE_CD -- 贷款合同放款方式代码
    , LOAN_REPAY_GRACE_TERM -- 贷款还款宽限期限
    , WRTOFF_STATUS_CD -- 核销状态代码
    , OFBS_BAL -- 表外余额
    , LOAN_GRP_CD -- 贷款分组代码
    , LOAN_CONT_CAP_SRC_CD -- 贷款合同资金来源代码
    , LOAN_CONT_TERM_CD -- 贷款合同期限代码
    , BORR_TYPE_CD -- 借款人类型代码
    , CUST_CRDT_CD -- 客户信用等级代码
    , BANK_ENTERP_FLAG -- 银企标志
    , REVOLV_LOAN_FLAG -- 循环贷款标志
    , CONVT_RMB_LOAN_APP_AMT -- 折人民币贷款申请金额
    , LOAN_CONT_GUAR_MODE_CD -- 贷款合同担保方式代码
    , LOAN_USAGE_CD -- 贷款用途代码
    , LOAN_INDUS_INVEST_CD -- 贷款行业投向代码
    , EXT_CONT_NO -- 展期合同编号
    , EXT_TMS -- 展期次数
    , LOAN_BELONG_ORG_NO -- 贷款归属机构编号
    , LOAN_BELONG_CUST_MGR_TELR_NO -- 贷款归属客户经理柜员编号
    , SETUP_ORG_NO -- 建立机构编号
    , SETUP_TELR_NO -- 建立柜员编号
    , SETUP_DT -- 建立日期
    , MODIF_ORG_NO -- 修改机构编号
    , MODIF_TELR_NO -- 修改柜员编号
    , FINAL_MODIF_DT -- 最后修改日期
    , PT_DT  -- 数据日期
)
select
      T1.STAACONO as STAACONO -- 贷款合同号
    , T1.STABSN03 as STABSN03 -- 借据序号
    , T1.STAAAC15 as LNACCT_NO -- 贷款账号
    , T1.STAACCYC as CURR_CD -- 币种
    , T1.STAACSNO as STAACSNO -- 客户内码
    , T1.STABCSID as BORR_CUST_NO -- 客户号
    , T1.STANFLNM as BORR_CUST_NAME -- 客户名称
    , T1.STAAPRNO as LOANPROD_NO -- 贷款产品代码
    , T1.UTBCFLNM as LOANPROD_NAME -- 贷款产品名称
    , T1.STAAAMNT as LOAN_AMT -- 发放金额
    , T1.STAABLNC as LOAN_BAL -- 贷款余额
    , T1.STAABLNC * T3.YRCAEXRT as CONVT_RMB_LOAN_BAL -- 年终报表汇率
    , T1.STADBLNC as YESTD_LOAN_BAL -- 昨日余额
    , T1.STAIDATE as LOAN_DT -- 发放日期
    , T1.STACDATE as LOAN_MATU_DT -- 到期日期
    , T1.STADCLS4 as CURR_LOAN_LVL_FOUR_MODAL_CD -- 贷款当前形态4级
    , T1.STAECLS5 as CURR_LOAN_TEN_LVL_MODAL_CD -- 贷款当前形态5级
    , T1.STAAACST as LNACCT_STATUS_CD -- 贷款账户状态
    , SUBSTR(T1.STAAACWF,2,1) as COMUT_DEBT_STATUS_CD -- 账户处理状态
    , SUBSTR(T1.STAAACWF,3,1) as DISPLC_STATUS_CD -- 账户处理状态
    , SUBSTR(T1.STAAACWF,4,1) as TRAN_STATUS_CD -- 账户处理状态
    , SUBSTR(T1.STAANO24,24,1) as LOAN_AST_SECUZT_STATUS_CD -- 信息编码
    , T1.STAAIRCD as LOAN_CONT_INT_RATE_NO -- 贷款利率代码
    , T1.STAARTTY as LOAN_CONT_INT_RATE_ADJ_MODE_CD -- 利率调整方式
    , CASE WHEN T1.STAAPRNO IN('53311','53312','53313','53314','53315','53316','53317','53321','53322','53323','53324','53325','53327') THEN T4.INTERESTRATE 
ELSE T1.STAERATE 
END  as NORMAL_EXC_INT_RATE -- 正常执行利率，押汇利率
    , CASE WHEN T1.STAAPRNO IN('53311','53312','53313','53314','53315','53316','53317','53321','53322','53323','53324','53325','53327') THEN T4.OVERDUERATE 
ELSE T1.STAFRATE
END  as OVDUE_EXC_INT_RATE -- 逾期执行利率，违约利率
    , T1.STACRATE as CRDT_DEFLT_INT_RATE -- 贷方违约利率
    , T1.STBRFLAG as OCCUPY_APPRO_FLAG -- 挤占挪用标志
    , T1.STAGRATE as OCCUPY_APPRO_EXC_INT_RATE -- 挤占挪用执行利率
    , T1.STADRTIO as INT_RATE_FLOT_RATIO -- 利率浮动比
    , T1.STAABOOL as INTACR_FLAG -- 计息标志
    , T1.STAAFLAG as COMPND_INT_FLAG -- 计复息标志
    , T1.STAAACID as ACCTN_SUBJ_NO -- 科目代号
    , T1.STAACDIN as LOAN_CONT_AGREE_SETMENT_DAY -- 结息日
    , T1.STAAWITP as LOAN_CONT_SETMENT_PERIOD_CD -- 结息周期
    , T1.STAIFLAG as MATU_AUTO_REPAY_FLAG -- 到期自动还款标志
    , T1.STAJFLAG as OVDUE_DEBIT_INT_AUTO_REPAY_FLAG -- 预期欠息自动还款标志
    , T1.STALDATE as DUBIL_LAST_REPAY_DT -- 上次还款日期
    , T1.STAMDATE as DUBIL_LAST_SETMENT_DT -- 上次结息日期
    , T1.STADFLAG as LOAN_CONT_LOAN_MODE_CD -- 放款方式
    , T1.STAETERM as LOAN_REPAY_GRACE_TERM -- 宽限期 
    , SUBSTR(T1.STAAACWF,1,1) as WRTOFF_STATUS_CD -- 账户处理状态
    , CASE WHEN T1.STAAPRNO IN('53311','53312','53313','53314','53315','53316','53317','53321','53322','53323','53324','53325','53327') THEN T5.WRITEOFFAMT
ELSE T1.STBJAMT
END  as OFBS_BAL -- 表外余额，核销金额
    , T1.STAALNGP as LOAN_GRP_CD -- 贷款分组
    , T1.STAAFNFR as LOAN_CONT_CAP_SRC_CD -- 资金来源
    , SUBSTR(T1.STAANO24,1,1) as LOAN_CONT_TERM_CD -- 信息编码
    , SUBSTR(T1.STAANO24,15,2) as BORR_TYPE_CD -- 信息编码
    , SUBSTR(T1.STAANO20,14,1) as CUST_CRDT_CD -- 贷款属性码
    , T1.STAABSFG as BANK_ENTERP_FLAG -- 银企标志
    , DECODE(T1.STADFLAG,'1','0','3','1' ,T1.STADFLAG) as REVOLV_LOAN_FLAG -- 放款方式
    , T6.NFAAAMT * T3.YRCAEXRT as CONVT_RMB_LOAN_APP_AMT -- 年终报表汇率、申请金额
    , T6.NFAASSTY as LOAN_CONT_GUAR_MODE_CD -- 担保方式
    , SUBSTR(T1.STAANO24,11,3) as LOAN_USAGE_CD -- 信息编码
    , SUBSTR(T1.STAANO20,16,4) as LOAN_INDUS_INVEST_CD -- 贷款属性码
    , T1.STAICONO as EXT_CONT_NO -- 展期合同号
    , T1.STACSN03 as EXT_TMS -- 展期次数
    , T1.STAABRNO as LOAN_BELONG_ORG_NO -- 机构号
    , T1.STAASTAF as LOAN_BELONG_CUST_MGR_TELR_NO -- 信贷员
    , T1.STACBRNO as SETUP_ORG_NO -- 建立机构号
    , T1.STACSTAF as SETUP_TELR_NO -- 建立柜员号
    , T1.STAGDATE as SETUP_DT -- 建立日期
    , T1.STADBRNO as MODIF_ORG_NO -- 修改机构号
    , T1.STADSTAF as MODIF_TELR_NO -- 修改柜员号
    , T1.STAHDATE as FINAL_MODIF_DT -- 修改日期
    , '${process_date}'  as PT_DT  -- None
from
    ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMMAST as T1 -- 普通贷款分户文件
    LEFT JOIN ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMCPUT as T2 -- 贷款产品文件
    on T1.STAAPRNO = T2.UTAAPRNO 
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_00 as T3 -- None
    on T1.STAACCYC = T3.YRCACCYC 
    LEFT JOIN ${ODS_EBILLS_SCHEMA}.ODS_EBILLS_BU_LOANINFO as T4 -- 融资利息详细表
    on T1.STAACONO = T4.CONSTRACTNO
AND T1.STABSN03 = T4.DEBTNO
AND COALESCE(TRIM(T4.CONSTRACTNO),'') <>'' -去除为空的数据
AND T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN ${ODS_EBILLS_SCHEMA}.ODS_EBILLS_BU_WRITEOFFINFO as T5 -- 贷款核销信息表
    on T1.STAAAC15 = T5.LOANACCOUNT
AND T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMCONF as T6 -- 贷款合同文件
    on T6.NFAACONO = T1.STAAPRNO
AND T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
where 1=1 
AND SUBSTR(STAACONO,1,2) IN('82','83','91','92','93')
AND T1.STAAACST IN('1','3','4','5') --1 审批 3 正常 4 结清 5 销户状态 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第3组）==============
-- 柜面核心数据组整合数据
drop table if exists ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_02;

create table ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_02 (
      LOAN_CONT_NO varchar(16) -- 贷款合同编号
    , DUBIL_NO varchar(3) -- 借据编号
    , LNACCT_NO varchar(15) -- 贷款账户编号
    , CURR_CD varchar(3) -- 币种代码
    , BORR_CUST_IN_CD varchar(11) -- 借款人客户内码
    , BORR_CUST_NO varchar(23) -- 借款人客户号
    , BORR_CUST_NAME varchar(100) -- 借款人客户名称
    , LOANPROD_NO varchar(5) -- 贷款产品编号
    , LOANPROD_NAME varchar(80) -- 贷款产品名称
    , LOAN_AMT decimal(20,2) -- 贷款发放金额
    , LOAN_BAL decimal(20,2) -- 贷款余额
    , CONVT_RMB_LOAN_BAL decimal(20,2) -- 折人民币贷款余额
    , YESTD_LOAN_BAL decimal(20,2) -- 昨日贷款余额
    , LOAN_DT date -- 贷款发放日期
    , LOAN_MATU_DT date -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD varchar(1) -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD varchar(1) -- 当前贷款十级形态代码
    , LNACCT_STATUS_CD varchar(1) -- 贷款账户状态代码
    , COMUT_DEBT_STATUS_CD varchar(1) -- 抵债状态代码
    , DISPLC_STATUS_CD varchar(1) -- 置换状态代码
    , TRAN_STATUS_CD varchar(1) -- 转让状态代码
    , LOAN_AST_SECUZT_STATUS_CD varchar(1) -- 贷款资产证券化状态代码
    , LOAN_CONT_INT_RATE_NO varchar(8) -- 贷款合同利率编号
    , LOAN_CONT_INT_RATE_ADJ_MODE_CD varchar(1) -- 贷款合同利率调整方式代码
    , NORMAL_EXC_INT_RATE decimal(20,7) -- 正常执行利率
    , OVDUE_EXC_INT_RATE decimal(20,7) -- 逾期执行利率
    , CRDT_DEFLT_INT_RATE decimal(20,7) -- 贷方违约利率
    , OCCUPY_APPRO_FLAG varchar(1) -- 挤占挪用标志
    , OCCUPY_APPRO_EXC_INT_RATE decimal(20,7) -- 挤占挪用执行利率
    , INT_RATE_FLOT_RATIO decimal(20,7) -- 利率浮动比
    , INTACR_FLAG varchar(1) -- 计息标志
    , COMPND_INT_FLAG varchar(1) -- 计复息标志
    , ACCTN_SUBJ_NO varchar(8) -- 会计科目编号
    , LOAN_CONT_AGREE_SETMENT_DAY varchar(2) -- 贷款合同约定结息日
    , LOAN_CONT_SETMENT_PERIOD_CD varchar(2) -- 贷款合同结息周期代码
    , MATU_AUTO_REPAY_FLAG varchar(1) -- 到期自动还款标志
    , OVDUE_DEBIT_INT_AUTO_REPAY_FLAG varchar(1) -- 逾期欠息自动还款标志
    , DUBIL_LAST_REPAY_DT date -- 借据上次还款日期
    , DUBIL_LAST_SETMENT_DT date -- 借据上次结息日期
    , LOAN_CONT_LOAN_MODE_CD varchar(1) -- 贷款合同放款方式代码
    , LOAN_REPAY_GRACE_TERM varchar(2) -- 贷款还款宽限期限
    , WRTOFF_STATUS_CD varchar(1) -- 核销状态代码
    , OFBS_BAL decimal(20,2) -- 表外余额
    , LOAN_WRTOFF_DT date -- 贷款核销日期
    , LOAN_WRTOFF_RETRA_STATUS_CD varchar(1) -- 贷款核销收回状态代码
    , LOAN_GRP_CD varchar(1) -- 贷款分组代码
    , LOAN_CONT_CAP_SRC_CD varchar(1) -- 贷款合同资金来源代码
    , LOAN_CONT_TERM_CD varchar(1) -- 贷款合同期限代码
    , CUST_MDL_CATOGR_CD varchar(2) -- 客户中类代码
    , CORP_SCALE_CD varchar(1) -- 企业规模代码
    , CORP_CUST_REG_INDUS_CD varchar(4) -- 对公客户注册行业代码
    , BORR_TYPE_CD varchar(2) -- 借款人类型代码
    , CUST_CRDT_CD varchar(1) -- 客户信用等级代码
    , BANK_ENTERP_FLAG varchar(1) -- 银企标志
    , LOAN_INVSTG_CUST_MGR_TELR_NO varchar(7) -- 贷款调查客户经理柜员编号
    , REVOLV_LOAN_FLAG varchar(1) -- 循环贷款标志
    , GREEN_LOAN_FLAG varchar(1) -- 绿色贷款标志
    , CONVT_RMB_LOAN_APP_AMT decimal(20,2) -- 折人民币贷款申请金额
    , LOAN_CONT_GUAR_MODE_CD varchar(3) -- 贷款合同担保方式代码
    , LOAN_USAGE_CD varchar(3) -- 贷款用途代码
    , LOAN_INDUS_INVEST_CD varchar(4) -- 贷款行业投向代码
    , EXT_CONT_NO varchar(16) -- 展期合同编号
    , EXT_TMS varchar(3) -- 展期次数
    , LOAN_BELONG_ORG_NO varchar(6) -- 贷款归属机构编号
    , LOAN_BELONG_CUST_MGR_TELR_NO varchar(7) -- 贷款归属客户经理柜员编号
    , LOAN_ACCT_NO varchar(22) -- 放款账号
    , MTG_LOAN_REPAY_PERIOD_CD varchar(1) -- 按揭贷款还款周期代码
    , MTG_LOAN_CUM_REPAY_INT_AMT decimal(20,2) -- 按揭贷款累计还息金额
    , MTG_LOAN_TOTAL_TMS varchar(5) -- 按揭贷款总期数
    , MTG_LOAN_REPAY_CURR_PERIOD_TMS varchar(5) -- 按揭贷款还款当期期数
    , MTG_LOAN_REPAY_MODE_CD varchar(1) -- 按揭贷款还款方式代码
    , MTG_LOAN_OVDUE_CNT varchar(5) -- 按揭贷款逾期笔数
    , MTG_LOAN_AGREE_REPAY_DAY varchar(2) -- 按揭贷款约定还款日
    , MTG_LOAN_CURR_MONTHLY_PY_AMT decimal(20,2) -- 按揭贷款当前月供金额
    , DISCNT_TXN_TYPE_CD varchar(2) -- 贴现交易类型代码
    , INTRA_BANK_DISCNT_TXN_FLAG varchar(1) -- 系统内贴现交易标志
    , DISCNT_DRFT_TYPE_CD varchar(1) -- 贴现汇票类型代码
    , DISCNT_DRFT_SHEET_CNT varchar(3) -- 贴现汇票张数
    , DISCNT_INT_TOTAL_AMT decimal(20,2) -- 贴现利息总额
    , SUPCHA_ORDER_NO varchar(100) -- 供应链订单编号
    , SETUP_ORG_NO varchar(6) -- 建立机构编号
    , SETUP_TELR_NO varchar(7) -- 建立柜员编号
    , SETUP_DT date -- 建立日期
    , MODIF_ORG_NO varchar(6) -- 修改机构编号
    , MODIF_TELR_NO varchar(7) -- 修改柜员编号
    , FINAL_MODIF_DT date -- 最后修改日期
    , TAB_SRC_CD varchar(1) -- 来源表代码
    , PT_DT  varchar(10) -- 数据日期
)
comment '柜面核心数据组整合数据'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_02(
      LOAN_CONT_NO -- 贷款合同编号
    , DUBIL_NO -- 借据编号
    , LNACCT_NO -- 贷款账户编号
    , CURR_CD -- 币种代码
    , BORR_CUST_IN_CD -- 借款人客户内码
    , BORR_CUST_NO -- 借款人客户号
    , BORR_CUST_NAME -- 借款人客户名称
    , LOANPROD_NO -- 贷款产品编号
    , LOANPROD_NAME -- 贷款产品名称
    , LOAN_AMT -- 贷款发放金额
    , LOAN_BAL -- 贷款余额
    , CONVT_RMB_LOAN_BAL -- 折人民币贷款余额
    , YESTD_LOAN_BAL -- 昨日贷款余额
    , LOAN_DT -- 贷款发放日期
    , LOAN_MATU_DT -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
    , LNACCT_STATUS_CD -- 贷款账户状态代码
    , COMUT_DEBT_STATUS_CD -- 抵债状态代码
    , DISPLC_STATUS_CD -- 置换状态代码
    , TRAN_STATUS_CD -- 转让状态代码
    , LOAN_AST_SECUZT_STATUS_CD -- 贷款资产证券化状态代码
    , LOAN_CONT_INT_RATE_NO -- 贷款合同利率编号
    , LOAN_CONT_INT_RATE_ADJ_MODE_CD -- 贷款合同利率调整方式代码
    , NORMAL_EXC_INT_RATE -- 正常执行利率
    , OVDUE_EXC_INT_RATE -- 逾期执行利率
    , CRDT_DEFLT_INT_RATE -- 贷方违约利率
    , OCCUPY_APPRO_FLAG -- 挤占挪用标志
    , OCCUPY_APPRO_EXC_INT_RATE -- 挤占挪用执行利率
    , INT_RATE_FLOT_RATIO -- 利率浮动比
    , INTACR_FLAG -- 计息标志
    , COMPND_INT_FLAG -- 计复息标志
    , ACCTN_SUBJ_NO -- 会计科目编号
    , LOAN_CONT_AGREE_SETMENT_DAY -- 贷款合同约定结息日
    , LOAN_CONT_SETMENT_PERIOD_CD -- 贷款合同结息周期代码
    , MATU_AUTO_REPAY_FLAG -- 到期自动还款标志
    , OVDUE_DEBIT_INT_AUTO_REPAY_FLAG -- 逾期欠息自动还款标志
    , DUBIL_LAST_REPAY_DT -- 借据上次还款日期
    , DUBIL_LAST_SETMENT_DT -- 借据上次结息日期
    , LOAN_CONT_LOAN_MODE_CD -- 贷款合同放款方式代码
    , LOAN_REPAY_GRACE_TERM -- 贷款还款宽限期限
    , WRTOFF_STATUS_CD -- 核销状态代码
    , OFBS_BAL -- 表外余额
    , LOAN_WRTOFF_DT -- 贷款核销日期
    , LOAN_WRTOFF_RETRA_STATUS_CD -- 贷款核销收回状态代码
    , LOAN_GRP_CD -- 贷款分组代码
    , LOAN_CONT_CAP_SRC_CD -- 贷款合同资金来源代码
    , LOAN_CONT_TERM_CD -- 贷款合同期限代码
    , CUST_MDL_CATOGR_CD -- 客户中类代码
    , CORP_SCALE_CD -- 企业规模代码
    , CORP_CUST_REG_INDUS_CD -- 对公客户注册行业代码
    , BORR_TYPE_CD -- 借款人类型代码
    , CUST_CRDT_CD -- 客户信用等级代码
    , BANK_ENTERP_FLAG -- 银企标志
    , LOAN_INVSTG_CUST_MGR_TELR_NO -- 贷款调查客户经理柜员编号
    , REVOLV_LOAN_FLAG -- 循环贷款标志
    , GREEN_LOAN_FLAG -- 绿色贷款标志
    , CONVT_RMB_LOAN_APP_AMT -- 折人民币贷款申请金额
    , LOAN_CONT_GUAR_MODE_CD -- 贷款合同担保方式代码
    , LOAN_USAGE_CD -- 贷款用途代码
    , LOAN_INDUS_INVEST_CD -- 贷款行业投向代码
    , EXT_CONT_NO -- 展期合同编号
    , EXT_TMS -- 展期次数
    , LOAN_BELONG_ORG_NO -- 贷款归属机构编号
    , LOAN_BELONG_CUST_MGR_TELR_NO -- 贷款归属客户经理柜员编号
    , LOAN_ACCT_NO -- 放款账号
    , MTG_LOAN_REPAY_PERIOD_CD -- 按揭贷款还款周期代码
    , MTG_LOAN_CUM_REPAY_INT_AMT -- 按揭贷款累计还息金额
    , MTG_LOAN_TOTAL_TMS -- 按揭贷款总期数
    , MTG_LOAN_REPAY_CURR_PERIOD_TMS -- 按揭贷款还款当期期数
    , MTG_LOAN_REPAY_MODE_CD -- 按揭贷款还款方式代码
    , MTG_LOAN_OVDUE_CNT -- 按揭贷款逾期笔数
    , MTG_LOAN_AGREE_REPAY_DAY -- 按揭贷款约定还款日
    , MTG_LOAN_CURR_MONTHLY_PY_AMT -- 按揭贷款当前月供金额
    , DISCNT_TXN_TYPE_CD -- 贴现交易类型代码
    , INTRA_BANK_DISCNT_TXN_FLAG -- 系统内贴现交易标志
    , DISCNT_DRFT_TYPE_CD -- 贴现汇票类型代码
    , DISCNT_DRFT_SHEET_CNT -- 贴现汇票张数
    , DISCNT_INT_TOTAL_AMT -- 贴现利息总额
    , SUPCHA_ORDER_NO -- 供应链订单编号
    , SETUP_ORG_NO -- 建立机构编号
    , SETUP_TELR_NO -- 建立柜员编号
    , SETUP_DT -- 建立日期
    , MODIF_ORG_NO -- 修改机构编号
    , MODIF_TELR_NO -- 修改柜员编号
    , FINAL_MODIF_DT -- 最后修改日期
    , TAB_SRC_CD -- 来源表代码
    , PT_DT  -- 数据日期
)
select
      T1.STAACONO as LOAN_CONT_NO -- 贷款合同编号
    , T1.STABSN03 as DUBIL_NO -- 借据编号
    , T1.LNACCT_NO as LNACCT_NO -- 贷款账户编号
    , T1.CURR_CD as CURR_CD -- 币种代码
    , T1.STAACSNO as BORR_CUST_IN_CD -- 借款人客户内码
    , T1.BORR_CUST_NO as BORR_CUST_NO -- 借款人客户号
    , T1.BORR_CUST_NAME as BORR_CUST_NAME -- 借款人客户名称
    , T1.LOANPROD_NO as LOANPROD_NO -- 贷款产品编号
    , T1.LOANPROD_NAME as LOANPROD_NAME -- 贷款产品名称
    , T1.LOAN_AMT as LOAN_AMT -- 贷款发放金额
    , T1.LOAN_BAL as LOAN_BAL -- 贷款余额
    , T1.CONVT_RMB_LOAN_BAL as CONVT_RMB_LOAN_BAL -- 折人民币贷款余额
    , T1.YESTD_LOAN_BAL as YESTD_LOAN_BAL -- 昨日贷款余额
    , T1.LOAN_DT as LOAN_DT -- 贷款发放日期
    , T1.LOAN_MATU_DT as LOAN_MATU_DT -- 贷款到期日期
    , T1.CURR_LOAN_LVL_FOUR_MODAL_CD as CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
    , T1.CURR_LOAN_TEN_LVL_MODAL_CD as CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
    , T1.LNACCT_STATUS_CD as LNACCT_STATUS_CD -- 贷款账户状态代码
    , T1.COMUT_DEBT_STATUS_CD as COMUT_DEBT_STATUS_CD -- 抵债状态代码
    , T1.DISPLC_STATUS_CD as DISPLC_STATUS_CD -- 置换状态代码
    , T1.TRAN_STATUS_CD as TRAN_STATUS_CD -- 转让状态代码
    , T1.LOAN_AST_SECUZT_STATUS_CD as LOAN_AST_SECUZT_STATUS_CD -- 贷款资产证券化状态代码
    , T1.LOAN_CONT_INT_RATE_NO as LOAN_CONT_INT_RATE_NO -- 贷款合同利率编号
    , T1.LOAN_CONT_INT_RATE_ADJ_MODE_CD as LOAN_CONT_INT_RATE_ADJ_MODE_CD -- 贷款合同利率调整方式代码
    , T1.NORMAL_EXC_INT_RATE as NORMAL_EXC_INT_RATE -- 正常执行利率
    , T1.OVDUE_EXC_INT_RATE as OVDUE_EXC_INT_RATE -- 逾期执行利率
    , T1.CRDT_DEFLT_INT_RATE as CRDT_DEFLT_INT_RATE -- 贷方违约利率
    , T1.OCCUPY_APPRO_FLAG as OCCUPY_APPRO_FLAG -- 挤占挪用标志
    , T1.OCCUPY_APPRO_EXC_INT_RATE as OCCUPY_APPRO_EXC_INT_RATE -- 挤占挪用执行利率
    , T1.INT_RATE_FLOT_RATIO as INT_RATE_FLOT_RATIO -- 利率浮动比
    , T1.INTACR_FLAG as INTACR_FLAG -- 计息标志
    , T1.COMPND_INT_FLAG as COMPND_INT_FLAG -- 计复息标志
    , T1.ACCTN_SUBJ_NO as ACCTN_SUBJ_NO -- 会计科目编号
    , T1.LOAN_CONT_AGREE_SETMENT_DAY as LOAN_CONT_AGREE_SETMENT_DAY -- 贷款合同约定结息日
    , T1.LOAN_CONT_SETMENT_PERIOD_CD as LOAN_CONT_SETMENT_PERIOD_CD -- 贷款合同结息周期代码
    , T1.MATU_AUTO_REPAY_FLAG as MATU_AUTO_REPAY_FLAG -- 到期自动还款标志
    , T1.OVDUE_DEBIT_INT_AUTO_REPAY_FLAG as OVDUE_DEBIT_INT_AUTO_REPAY_FLAG -- 逾期欠息自动还款标志
    , T1.DUBIL_LAST_REPAY_DT as DUBIL_LAST_REPAY_DT -- 借据上次还款日期
    , T1.DUBIL_LAST_SETMENT_DT as DUBIL_LAST_SETMENT_DT -- 借据上次结息日期
    , T1.LOAN_CONT_LOAN_MODE_CD as LOAN_CONT_LOAN_MODE_CD -- 贷款合同放款方式代码
    , T1.LOAN_REPAY_GRACE_TERM as LOAN_REPAY_GRACE_TERM -- 贷款还款宽限期限
    , T1.WRTOFF_STATUS_CD as WRTOFF_STATUS_CD -- 核销状态代码
    , T1.OFBS_BAL as OFBS_BAL -- 表外余额
    , CASE WHEN T1.STAAPRNO IN('53311','53312','53313','53314','53315','53316','53317','53321','53322','53323','53324','53325','53327') THEN T6.CHANGEDATE
ELSE T1.AFBJDATE
END  as LOAN_WRTOFF_DT -- 核销日期，核销日期
    , T6.AFCGFLAG as LOAN_WRTOFF_RETRA_STATUS_CD -- 收回标志
    , T1.LOAN_GRP_CD as LOAN_GRP_CD -- None
    , T1.LOAN_CONT_CAP_SRC_CD as LOAN_CONT_CAP_SRC_CD -- None
    , T1.LOAN_CONT_TERM_CD as LOAN_CONT_TERM_CD -- None
    , T7.SUB_CST_TP as CUST_MDL_CATOGR_CD -- 客户类型（中类）
    , T7.ORG_SCALE as CORP_SCALE_CD -- 企业规模
    , T7.REG_INDS as CORP_CUST_REG_INDUS_CD -- 注册行业
    , T1.BORR_TYPE_CD as BORR_TYPE_CD -- None
    , T1.CUST_CRDT_CD as CUST_CRDT_CD -- None
    , T1.BANK_ENTERP_FLAG as BANK_ENTERP_FLAG -- None
    , T8.CREDITOR_NO as LOAN_INVSTG_CUST_MGR_TELR_NO -- 信贷员号
    , T1.REVOLV_LOAN_FLAG as REVOLV_LOAN_FLAG -- None
    , T9.GREEN_LOAN_SIGN as GREEN_LOAN_FLAG -- 绿色贷款标志
    , T11.NFAAAMT * T3.YRCAEXRT as CONVT_RMB_LOAN_APP_AMT -- 年终报表汇率、申请金额
    , T11.NFAASSTY as LOAN_CONT_GUAR_MODE_CD -- 担保方式
    , T1.LOAN_USAGE_CD as LOAN_USAGE_CD -- None
    , T1.LOAN_INDUS_INVEST_CD as LOAN_INDUS_INVEST_CD -- None
    , T1.EXT_CONT_NO as EXT_CONT_NO -- None
    , T1.EXT_TMS as EXT_TMS -- None
    , T1.LOAN_BELONG_ORG_NO as LOAN_BELONG_ORG_NO -- None
    , T1.LOAN_BELONG_CUST_MGR_TELR_NO as LOAN_BELONG_CUST_MGR_TELR_NO -- None
    , T12.SVADAC22 as LOAN_ACCT_NO -- 放款账号
    , '' as MTG_LOAN_REPAY_PERIOD_CD -- None
    , '' as MTG_LOAN_CUM_REPAY_INT_AMT -- None
    , '' as MTG_LOAN_TOTAL_TMS -- None
    , '' as MTG_LOAN_REPAY_CURR_PERIOD_TMS -- None
    , '' as MTG_LOAN_REPAY_MODE_CD -- None
    , '' as MTG_LOAN_OVDUE_CNT -- None
    , '' as MTG_LOAN_AGREE_REPAY_DAY -- None
    , '' as MTG_LOAN_CURR_MONTHLY_PY_AMT -- None
    , '' as DISCNT_TXN_TYPE_CD -- None
    , '' as INTRA_BANK_DISCNT_TXN_FLAG -- None
    , '' as DISCNT_DRFT_TYPE_CD -- None
    , '' as DISCNT_DRFT_SHEET_CNT -- None
    , '' as DISCNT_INT_TOTAL_AMT -- None
    , T13.order_no as SUPCHA_ORDER_NO -- 订单编号
    , T1.SETUP_ORG_NO as SETUP_ORG_NO -- 建立机构编号
    , T1.SETUP_TELR_NO as SETUP_TELR_NO -- 建立柜员编号
    , T1.SETUP_DT as SETUP_DT -- 建立日期
    , T1.MODIF_ORG_NO as MODIF_ORG_NO -- 修改机构编号
    , T1.MODIF_TELR_NO as MODIF_TELR_NO -- 修改柜员编号
    , T1.FINAL_MODIF_DT as FINAL_MODIF_DT -- 最后修改日期
    , '1' as TAB_SRC_CD -- None
    , '${process_date}'  as PT_DT  -- None
from
    TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_01 as T1 -- None
    LEFT JOIN ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMCNAF as T6 -- 贷款核销文件
    on T1.STAACONO = T6.AFAACONO
AND T1.STABSN03 = T6.AFABSN03
AND T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
    LEFT JOIN (SELECT 
	 P1.CUST_INCOD --客户内码
	,P1.SUB_CST_TP	--客户类别
	,P1.REG_INDS	--注册地址
	,p2.ORG_SCALE	--企业规模

FROM 
	${ODS_ECIF_SCHEMA}.ODS_ECIF_T2_EN_CUST_BASE_INFO P1
LEFT JOIN
	${ODS_ECIF_SCHEMA}.ODS_ECIF_T2_EN_CUST_DETAIL_INFO P2 
 ON P1.CUST_INCOD = P2.CUST_INCOD
	AND  P2.PT_DT='${process_date}' 
	AND P2.DELETED='0'
WHERE  P1.PT_DT='${process_date}' 
	AND P1.DELETED='0'	) as T7 -- 对公客户基本信息
    on T1.TZAACSNO  = T7.CUST_INCOD 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_LOAN_REQUISITION_PUBLIC as T8 -- 借款申请.公共资料
    on T1.STAACONO = T8.CONTRACT_NO
AND T8.PT_DT='${process_date}' 
AND T8.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_LOAN_REQUISITION_EXT as T9 -- 借款申请.关联信息
    on T1.STAACONO = T9.CONTENT_KEY
AND T9.PT_DT='${process_date}' 
AND T9.DELETED='0' 
    LEFT JOIN ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMCONF as T11 -- 贷款合同文件
    on T11.NFAACONO = T1.STAAPRNO
AND T11.PT_DT='${process_date}' 
AND T11.DELETED='0' 
    LEFT JOIN ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMACSV as T12 -- 贷款账户结算文件
    on T12.SVAACONO = T1.STAACONO
AND T1.STABSN03 = T12.SVABSN03
AND T12.PT_DT='${process_date}' 
AND T12.DELETED='0'
AND T12.SVBLFLG = '1' 
    LEFT JOIN ${ODS_SCB_SCHEMA}.ODS_SCB_online_lnci_base_info as T13 -- 借据表
    on T13.mast_contno = T1.STAACONO
AND T1.STABSN03 = T13.debet_no
AND T13.PT_DT='${process_date}' 
AND T13.DELETED='0' 
;
-- ==============字段映射（第4组）==============
-- 按揭贷款数据组整合数据
drop table if exists ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_03;

create table ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_03 (
      LOAN_CONT_NO varchar(16) -- 贷款合同编号
    , DUBIL_NO varchar(3) -- 借据编号
    , LNACCT_NO varchar(15) -- 贷款账户编号
    , CURR_CD varchar(3) -- 币种代码
    , BORR_CUST_IN_CD varchar(11) -- 借款人客户内码
    , BORR_CUST_NO varchar(23) -- 借款人客户号
    , BORR_CUST_NAME varchar(100) -- 借款人客户名称
    , LOANPROD_NO varchar(5) -- 贷款产品编号
    , LOANPROD_NAME varchar(80) -- 贷款产品名称
    , LOAN_AMT decimal(20,2) -- 贷款发放金额
    , LOAN_BAL decimal(20,2) -- 贷款余额
    , CONVT_RMB_LOAN_BAL decimal(20,2) -- 折人民币贷款余额
    , YESTD_LOAN_BAL decimal(20,2) -- 昨日贷款余额
    , LOAN_DT date -- 贷款发放日期
    , LOAN_MATU_DT date -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD varchar(1) -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD varchar(1) -- 当前贷款十级形态代码
    , LNACCT_STATUS_CD varchar(1) -- 贷款账户状态代码
    , COMUT_DEBT_STATUS_CD varchar(1) -- 抵债状态代码
    , DISPLC_STATUS_CD varchar(1) -- 置换状态代码
    , TRAN_STATUS_CD varchar(1) -- 转让状态代码
    , LOAN_AST_SECUZT_STATUS_CD varchar(1) -- 贷款资产证券化状态代码
    , LOAN_CONT_INT_RATE_NO varchar(8) -- 贷款合同利率编号
    , LOAN_CONT_INT_RATE_ADJ_MODE_CD varchar(1) -- 贷款合同利率调整方式代码
    , NORMAL_EXC_INT_RATE decimal(20,7) -- 正常执行利率
    , OVDUE_EXC_INT_RATE decimal(20,7) -- 逾期执行利率
    , CRDT_DEFLT_INT_RATE decimal(20,7) -- 贷方违约利率
    , OCCUPY_APPRO_FLAG varchar(1) -- 挤占挪用标志
    , OCCUPY_APPRO_EXC_INT_RATE decimal(20,7) -- 挤占挪用执行利率
    , INT_RATE_FLOT_RATIO decimal(20,7) -- 利率浮动比
    , INTACR_FLAG varchar(1) -- 计息标志
    , COMPND_INT_FLAG varchar(1) -- 计复息标志
    , ACCTN_SUBJ_NO varchar(8) -- 会计科目编号
    , LOAN_CONT_AGREE_SETMENT_DAY varchar(2) -- 贷款合同约定结息日
    , LOAN_CONT_SETMENT_PERIOD_CD varchar(2) -- 贷款合同结息周期代码
    , MATU_AUTO_REPAY_FLAG varchar(1) -- 到期自动还款标志
    , OVDUE_DEBIT_INT_AUTO_REPAY_FLAG varchar(1) -- 逾期欠息自动还款标志
    , DUBIL_LAST_REPAY_DT date -- 借据上次还款日期
    , DUBIL_LAST_SETMENT_DT date -- 借据上次结息日期
    , LOAN_CONT_LOAN_MODE_CD varchar(1) -- 贷款合同放款方式代码
    , LOAN_REPAY_GRACE_TERM varchar(2) -- 贷款还款宽限期限
    , WRTOFF_STATUS_CD varchar(1) -- 核销状态代码
    , OFBS_BAL decimal(20,2) -- 表外余额
    , LOAN_WRTOFF_DT date -- 贷款核销日期
    , LOAN_WRTOFF_RETRA_STATUS_CD varchar(1) -- 贷款核销收回状态代码
    , LOAN_GRP_CD varchar(1) -- 贷款分组代码
    , LOAN_CONT_CAP_SRC_CD varchar(1) -- 贷款合同资金来源代码
    , LOAN_CONT_TERM_CD varchar(1) -- 贷款合同期限代码
    , CUST_MDL_CATOGR_CD varchar(2) -- 客户中类代码
    , CORP_SCALE_CD varchar(1) -- 企业规模代码
    , CORP_CUST_REG_INDUS_CD varchar(4) -- 对公客户注册行业代码
    , BORR_TYPE_CD varchar(2) -- 借款人类型代码
    , CUST_CRDT_CD varchar(1) -- 客户信用等级代码
    , BANK_ENTERP_FLAG varchar(1) -- 银企标志
    , LOAN_INVSTG_CUST_MGR_TELR_NO varchar(7) -- 贷款调查客户经理柜员编号
    , REVOLV_LOAN_FLAG varchar(1) -- 循环贷款标志
    , GREEN_LOAN_FLAG varchar(1) -- 绿色贷款标志
    , CONVT_RMB_LOAN_APP_AMT decimal(20,2) -- 折人民币贷款申请金额
    , LOAN_CONT_GUAR_MODE_CD varchar(3) -- 贷款合同担保方式代码
    , LOAN_USAGE_CD varchar(3) -- 贷款用途代码
    , LOAN_INDUS_INVEST_CD varchar(4) -- 贷款行业投向代码
    , EXT_CONT_NO varchar(16) -- 展期合同编号
    , EXT_TMS varchar(3) -- 展期次数
    , LOAN_BELONG_ORG_NO varchar(6) -- 贷款归属机构编号
    , LOAN_BELONG_CUST_MGR_TELR_NO varchar(7) -- 贷款归属客户经理柜员编号
    , LOAN_ACCT_NO varchar(22) -- 放款账号
    , MTG_LOAN_REPAY_PERIOD_CD varchar(1) -- 按揭贷款还款周期代码
    , MTG_LOAN_CUM_REPAY_INT_AMT decimal(20,2) -- 按揭贷款累计还息金额
    , MTG_LOAN_TOTAL_TMS varchar(5) -- 按揭贷款总期数
    , MTG_LOAN_REPAY_CURR_PERIOD_TMS varchar(5) -- 按揭贷款还款当期期数
    , MTG_LOAN_REPAY_MODE_CD varchar(1) -- 按揭贷款还款方式代码
    , MTG_LOAN_OVDUE_CNT varchar(5) -- 按揭贷款逾期笔数
    , MTG_LOAN_AGREE_REPAY_DAY varchar(2) -- 按揭贷款约定还款日
    , MTG_LOAN_CURR_MONTHLY_PY_AMT decimal(20,2) -- 按揭贷款当前月供金额
    , DISCNT_TXN_TYPE_CD varchar(2) -- 贴现交易类型代码
    , INTRA_BANK_DISCNT_TXN_FLAG varchar(1) -- 系统内贴现交易标志
    , DISCNT_DRFT_TYPE_CD varchar(1) -- 贴现汇票类型代码
    , DISCNT_DRFT_SHEET_CNT varchar(3) -- 贴现汇票张数
    , DISCNT_INT_TOTAL_AMT decimal(20,2) -- 贴现利息总额
    , SUPCHA_ORDER_NO varchar(100) -- 供应链订单编号
    , SETUP_ORG_NO varchar(6) -- 建立机构编号
    , SETUP_TELR_NO varchar(7) -- 建立柜员编号
    , SETUP_DT date -- 建立日期
    , MODIF_ORG_NO varchar(6) -- 修改机构编号
    , MODIF_TELR_NO varchar(7) -- 修改柜员编号
    , FINAL_MODIF_DT date -- 最后修改日期
    , TAB_SRC_CD varchar(1) -- 来源表代码
    , PT_DT  varchar(10) -- 数据日期
)
comment '按揭贷款数据组整合数据'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_03(
      LOAN_CONT_NO -- 贷款合同编号
    , DUBIL_NO -- 借据编号
    , LNACCT_NO -- 贷款账户编号
    , CURR_CD -- 币种代码
    , BORR_CUST_IN_CD -- 借款人客户内码
    , BORR_CUST_NO -- 借款人客户号
    , BORR_CUST_NAME -- 借款人客户名称
    , LOANPROD_NO -- 贷款产品编号
    , LOANPROD_NAME -- 贷款产品名称
    , LOAN_AMT -- 贷款发放金额
    , LOAN_BAL -- 贷款余额
    , CONVT_RMB_LOAN_BAL -- 折人民币贷款余额
    , YESTD_LOAN_BAL -- 昨日贷款余额
    , LOAN_DT -- 贷款发放日期
    , LOAN_MATU_DT -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
    , LNACCT_STATUS_CD -- 贷款账户状态代码
    , COMUT_DEBT_STATUS_CD -- 抵债状态代码
    , DISPLC_STATUS_CD -- 置换状态代码
    , TRAN_STATUS_CD -- 转让状态代码
    , LOAN_AST_SECUZT_STATUS_CD -- 贷款资产证券化状态代码
    , LOAN_CONT_INT_RATE_NO -- 贷款合同利率编号
    , LOAN_CONT_INT_RATE_ADJ_MODE_CD -- 贷款合同利率调整方式代码
    , NORMAL_EXC_INT_RATE -- 正常执行利率
    , OVDUE_EXC_INT_RATE -- 逾期执行利率
    , CRDT_DEFLT_INT_RATE -- 贷方违约利率
    , OCCUPY_APPRO_FLAG -- 挤占挪用标志
    , OCCUPY_APPRO_EXC_INT_RATE -- 挤占挪用执行利率
    , INT_RATE_FLOT_RATIO -- 利率浮动比
    , INTACR_FLAG -- 计息标志
    , COMPND_INT_FLAG -- 计复息标志
    , ACCTN_SUBJ_NO -- 会计科目编号
    , LOAN_CONT_AGREE_SETMENT_DAY -- 贷款合同约定结息日
    , LOAN_CONT_SETMENT_PERIOD_CD -- 贷款合同结息周期代码
    , MATU_AUTO_REPAY_FLAG -- 到期自动还款标志
    , OVDUE_DEBIT_INT_AUTO_REPAY_FLAG -- 逾期欠息自动还款标志
    , DUBIL_LAST_REPAY_DT -- 借据上次还款日期
    , DUBIL_LAST_SETMENT_DT -- 借据上次结息日期
    , LOAN_CONT_LOAN_MODE_CD -- 贷款合同放款方式代码
    , LOAN_REPAY_GRACE_TERM -- 贷款还款宽限期限
    , WRTOFF_STATUS_CD -- 核销状态代码
    , OFBS_BAL -- 表外余额
    , LOAN_WRTOFF_DT -- 贷款核销日期
    , LOAN_WRTOFF_RETRA_STATUS_CD -- 贷款核销收回状态代码
    , LOAN_GRP_CD -- 贷款分组代码
    , LOAN_CONT_CAP_SRC_CD -- 贷款合同资金来源代码
    , LOAN_CONT_TERM_CD -- 贷款合同期限代码
    , CUST_MDL_CATOGR_CD -- 客户中类代码
    , CORP_SCALE_CD -- 企业规模代码
    , CORP_CUST_REG_INDUS_CD -- 对公客户注册行业代码
    , BORR_TYPE_CD -- 借款人类型代码
    , CUST_CRDT_CD -- 客户信用等级代码
    , BANK_ENTERP_FLAG -- 银企标志
    , LOAN_INVSTG_CUST_MGR_TELR_NO -- 贷款调查客户经理柜员编号
    , REVOLV_LOAN_FLAG -- 循环贷款标志
    , GREEN_LOAN_FLAG -- 绿色贷款标志
    , CONVT_RMB_LOAN_APP_AMT -- 折人民币贷款申请金额
    , LOAN_CONT_GUAR_MODE_CD -- 贷款合同担保方式代码
    , LOAN_USAGE_CD -- 贷款用途代码
    , LOAN_INDUS_INVEST_CD -- 贷款行业投向代码
    , EXT_CONT_NO -- 展期合同编号
    , EXT_TMS -- 展期次数
    , LOAN_BELONG_ORG_NO -- 贷款归属机构编号
    , LOAN_BELONG_CUST_MGR_TELR_NO -- 贷款归属客户经理柜员编号
    , LOAN_ACCT_NO -- 放款账号
    , MTG_LOAN_REPAY_PERIOD_CD -- 按揭贷款还款周期代码
    , MTG_LOAN_CUM_REPAY_INT_AMT -- 按揭贷款累计还息金额
    , MTG_LOAN_TOTAL_TMS -- 按揭贷款总期数
    , MTG_LOAN_REPAY_CURR_PERIOD_TMS -- 按揭贷款还款当期期数
    , MTG_LOAN_REPAY_MODE_CD -- 按揭贷款还款方式代码
    , MTG_LOAN_OVDUE_CNT -- 按揭贷款逾期笔数
    , MTG_LOAN_AGREE_REPAY_DAY -- 按揭贷款约定还款日
    , MTG_LOAN_CURR_MONTHLY_PY_AMT -- 按揭贷款当前月供金额
    , DISCNT_TXN_TYPE_CD -- 贴现交易类型代码
    , INTRA_BANK_DISCNT_TXN_FLAG -- 系统内贴现交易标志
    , DISCNT_DRFT_TYPE_CD -- 贴现汇票类型代码
    , DISCNT_DRFT_SHEET_CNT -- 贴现汇票张数
    , DISCNT_INT_TOTAL_AMT -- 贴现利息总额
    , SUPCHA_ORDER_NO -- 供应链订单编号
    , SETUP_ORG_NO -- 建立机构编号
    , SETUP_TELR_NO -- 建立柜员编号
    , SETUP_DT -- 建立日期
    , MODIF_ORG_NO -- 修改机构编号
    , MODIF_TELR_NO -- 修改柜员编号
    , FINAL_MODIF_DT -- 最后修改日期
    , TAB_SRC_CD -- 来源表代码
    , PT_DT  -- 数据日期
)
select
      T1.TZAACONO as LOAN_CONT_NO -- 贷款合同号
    , T1.TZABSN03 as DUBIL_NO -- 借据序号
    , T1.TZAAAC15 as LNACCT_NO -- 贷款账号
    , T1.TZAACCYC as CURR_CD -- 币种
    , T1.TZAACSNO as BORR_CUST_IN_CD -- 客户内码
    , T1.TZABCSID as BORR_CUST_NO -- 客户号
    , T1.TZANFLNM as BORR_CUST_NAME -- 客户名称
    , T1.TZAAPRNO as LOANPROD_NO -- 贷款产品代码
    , T2.UTBCFLNM as LOANPROD_NAME -- 贷款产品名称
    , T1.TZAAAMNT as LOAN_AMT -- 发放金额
    , T1.TZAABLNC as LOAN_BAL -- 贷款余额
    , T1.TZAABLNC * T3.YRCAEXRT as CONVT_RMB_LOAN_BAL -- 年终报表汇率
    , T1.TZADBLNC as YESTD_LOAN_BAL -- 昨日余额
    , T1.TZAIDATE as LOAN_DT -- 发放日期
    , T1.TZACDATE as LOAN_MATU_DT -- 到期日期
    , T1.TZADCLS4 as CURR_LOAN_LVL_FOUR_MODAL_CD -- 贷款当前形态4级
    , T1.TZAECLS5 as CURR_LOAN_TEN_LVL_MODAL_CD -- 贷款当前形态5级
    , T1.TZAAACST as LNACCT_STATUS_CD -- 贷款账户状态
    , T1.TZAAACWF as COMUT_DEBT_STATUS_CD -- 账户处理状态
    , T1.TZAAACWF as DISPLC_STATUS_CD -- 账户处理状态
    , T1.TZAAACWF as TRAN_STATUS_CD -- 账户处理状态
    , T1.TZAANO24 as LOAN_AST_SECUZT_STATUS_CD -- 信息编码
    , T1.TZAAIRCD as LOAN_CONT_INT_RATE_NO -- 贷款利率代码
    , T1.TZAARTTY as LOAN_CONT_INT_RATE_ADJ_MODE_CD -- 利率调整方式
    , T1.TZAERATE as NORMAL_EXC_INT_RATE -- 正常执行利率
    , T1.TZAFRATE as OVDUE_EXC_INT_RATE -- 逾期执行利率
    , T1.TZACRATE as CRDT_DEFLT_INT_RATE -- 贷方违约利率
    , T1.TZBRFLAG as OCCUPY_APPRO_FLAG -- 挤占挪用标志
    , T1.TZAGRATE as OCCUPY_APPRO_EXC_INT_RATE -- 挤占挪用执行利率
    , T1.TZADRTIO as INT_RATE_FLOT_RATIO -- 利率浮动比
    , '' as INTACR_FLAG -- None
    , T1.TZAAFLAG as COMPND_INT_FLAG -- 计复息标志
    , T1.TZAAACID as ACCTN_SUBJ_NO -- 科目代号
    , '' as LOAN_CONT_AGREE_SETMENT_DAY -- None
    , '' as LOAN_CONT_SETMENT_PERIOD_CD -- None
    , T1.TZAIFLAG as MATU_AUTO_REPAY_FLAG -- 到期自动还款标志
    , T1.TZAKBOOL as OVDUE_DEBIT_INT_AUTO_REPAY_FLAG -- 逾期是否自动还款
    , T1.TZAKDATE as DUBIL_LAST_REPAY_DT -- 最后交易日期
    , '' as DUBIL_LAST_SETMENT_DT -- None
    , '' as LOAN_CONT_LOAN_MODE_CD -- None
    , T1.TZAETERM as LOAN_REPAY_GRACE_TERM -- 宽限期
    , SUBSTR(T1.TZAAACWF,1,1) as WRTOFF_STATUS_CD -- 账户处理状态
    , T1.TZBJAMT as OFBS_BAL -- 表外余额
    , T6.AFBJDATE as LOAN_WRTOFF_DT -- 核销日期
    , T6.AFCGFLAG as LOAN_WRTOFF_RETRA_STATUS_CD -- 收回标志
    , T1.TZAALNGP as LOAN_GRP_CD -- 贷款分组
    , T1.TZAAFNFR as LOAN_CONT_CAP_SRC_CD -- 资金来源
    , SUBSTR(T1.TZAANO24,1,1) as LOAN_CONT_TERM_CD -- 信息编码
    , T7.SUB_CST_TP as CUST_MDL_CATOGR_CD -- 客户类型（中类）
    , T7.ORG_SCALE as CORP_SCALE_CD -- 企业规模
    , T7.REG_INDS as CORP_CUST_REG_INDUS_CD -- 注册行业
    , SUBSTR(T1.TZAANO24,15,2) as BORR_TYPE_CD -- 信息编码
    , SUBSTR(T1.TZAANO20,14,1) as CUST_CRDT_CD -- 贷款属性码
    , T1.TZAABSFG as BANK_ENTERP_FLAG -- 银企标志
    , T8.CREDITOR_NO as LOAN_INVSTG_CUST_MGR_TELR_NO -- 信贷员号
    , '' as REVOLV_LOAN_FLAG -- None
    , T9.GREEN_LOAN_SIGN as GREEN_LOAN_FLAG -- 绿色贷款标志
    , T10.NFAAAMT * T3.YRCAEXRT as CONVT_RMB_LOAN_APP_AMT -- 年终报表汇率、申请金额
    , T10.NFAASSTY as LOAN_CONT_GUAR_MODE_CD -- 担保方式
    , SUBSTR(T1.TZAANO24,11,3) as LOAN_USAGE_CD -- 信息编码
    , SUBSTR(T1.TZABNO20,16,4) as LOAN_INDUS_INVEST_CD -- 贷款属性码
    , '' as EXT_CONT_NO -- None
    , '' as EXT_TMS -- None
    , T1.TZAABRNO as LOAN_BELONG_ORG_NO -- 机构号
    , T1.TZAASTAF as LOAN_BELONG_CUST_MGR_TELR_NO -- 信贷员
    , T11.SVADAC22 as LOAN_ACCT_NO -- 放款账号
    , T1.TZAAPAYP as MTG_LOAN_REPAY_PERIOD_CD -- 还款周期
    , T1.TZAAIAM2 as MTG_LOAN_CUM_REPAY_INT_AMT -- 累计还息金额
    , T1.TZAAPERD as MTG_LOAN_TOTAL_TMS -- 总期数
    , T1.TZABPERD as MTG_LOAN_REPAY_CURR_PERIOD_TMS -- 还款当期期数
    , T1.TZAAPAYT as MTG_LOAN_REPAY_MODE_CD -- 按揭还款方式
    , T1.TZAEPERD as MTG_LOAN_OVDUE_CNT -- 逾期笔数
    , T1.TZABCDIN as MTG_LOAN_AGREE_REPAY_DAY -- 还款日
    , T1.TZABAMT as MTG_LOAN_CURR_MONTHLY_PY_AMT -- 当前月供金额
    , '' as DISCNT_TXN_TYPE_CD -- None
    , '' as INTRA_BANK_DISCNT_TXN_FLAG -- None
    , '' as DISCNT_DRFT_TYPE_CD -- None
    , '' as DISCNT_DRFT_SHEET_CNT -- None
    , '' as DISCNT_INT_TOTAL_AMT -- None
    , T12.order_no as SUPCHA_ORDER_NO -- 订单编号
    , T1.TZACBRNO as SETUP_ORG_NO -- 建立机构号
    , T1.TZACSTAF as SETUP_TELR_NO -- 建立柜员
    , T1.TZAGDATE as SETUP_DT -- 建立日期
    , T1.TZADBRNO as MODIF_ORG_NO -- 修改机构号
    , T1.TZADSTAF as MODIF_TELR_NO -- 修改柜员
    , T1.TZAHDATE as FINAL_MODIF_DT -- 修改日期
    , '2' as TAB_SRC_CD -- None
    , '${process_date}'  as PT_DT  -- None
from
    ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMAMTZ as T1 -- 按揭贷款分户文件
    LEFT JOIN ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMCPUT as T2 -- 贷款产品文件
    on T1.TZAAPRNO = T2.UTAAPRNO 
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_00 as T3 -- None
    on T1.STAACCYC = T3.YRCACCYC 
    LEFT JOIN ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMCNAF as T6 -- 贷款核销文件
    on T1.TZAACONO = T6.AFAACONO
AND T1.TZABSN03 = T6.AFABSN03
AND T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
    LEFT JOIN (SELECT 
	 P1.CUST_INCOD --客户内码
	,P1.SUB_CST_TP	--客户类别
	,P1.REG_INDS	--注册地址
	,p2.ORG_SCALE	--企业规模

FROM 
	${ODS_ECIF_SCHEMA}.ODS_ECIF_T2_EN_CUST_BASE_INFO P1
LEFT JOIN
	${ODS_ECIF_SCHEMA}.ODS_ECIF_T2_EN_CUST_DETAIL_INFO P2 
 ON P1.CUST_INCOD = P2.CUST_INCOD
	AND  P2.PT_DT='${process_date}' 
	AND P2.DELETED='0'
WHERE  P1.PT_DT='${process_date}' 
	AND P1.DELETED='0'	) as T7 -- 对公客户基本信息
    on T1.TZAACSNO  = T7.CUST_INCOD 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_LOAN_REQUISITION_PUBLIC as T8 -- 借款申请.公共资料
    on T1.TZAACONO = T8.CONTRACT_NO
AND T8.PT_DT='${process_date}' 
AND T8.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_LOAN_REQUISITION_EXT as T9 -- 借款申请.关联信息
    on T1.TZAACONO = T9.CONTENT_KEY
AND T9.PT_DT='${process_date}' 
AND T9.DELETED='0' 
    LEFT JOIN ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMCONF as T10 -- 贷款合同文件
    on T10.NFAACONO = T1.TZAAPRNO
AND T10.PT_DT='${process_date}' 
AND T10.DELETED='0' 
    LEFT JOIN ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMACSV as T11 -- 贷款账户结算文件
    on T11.SVAACONO = T1.TZAACONO
AND T1.TZABSN03 = T11.SVABSN03
AND T11.PT_DT='${process_date}' 
AND T11.DELETED='0'
AND T11.SVBLFLG = '1' 
    LEFT JOIN ${ODS_SCB_SCHEMA}.ODS_SCB_online_lnci_base_info as T12 -- 借据表
    on T12.mast_contno = T1.TZAACONO
AND T1.TZABSN03 = T12.debet_no
AND T12.PT_DT='${process_date}' 
AND T12.DELETED='0' 
where 1=1 
AND SUBSTR(TZAACONO,1,2) IN('82','83','91','92','93')
AND T1.TZAAACST IN('1','3','4','5') --1 审批 3 正常 4 结清 5 销户状态 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第5组）==============
-- 贴现分户文件
drop table if exists ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_04;

create table ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_04 (
      LOAN_CONT_NO varchar(16) -- 贷款合同编号
    , DUBIL_NO varchar(3) -- 借据编号
    , LNACCT_NO varchar(15) -- 贷款账户编号
    , CURR_CD varchar(3) -- 币种代码
    , BORR_CUST_IN_CD varchar(11) -- 借款人客户内码
    , BORR_CUST_NO varchar(23) -- 借款人客户号
    , BORR_CUST_NAME varchar(100) -- 借款人客户名称
    , LOANPROD_NO varchar(5) -- 贷款产品编号
    , LOANPROD_NAME varchar(80) -- 贷款产品名称
    , LOAN_AMT decimal(20,2) -- 贷款发放金额
    , LOAN_BAL decimal(20,2) -- 贷款余额
    , CONVT_RMB_LOAN_BAL decimal(20,2) -- 折人民币贷款余额
    , YESTD_LOAN_BAL decimal(20,2) -- 昨日贷款余额
    , LOAN_DT date -- 贷款发放日期
    , LOAN_MATU_DT date -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD varchar(1) -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD varchar(1) -- 当前贷款十级形态代码
    , LNACCT_STATUS_CD varchar(1) -- 贷款账户状态代码
    , COMUT_DEBT_STATUS_CD varchar(1) -- 抵债状态代码
    , DISPLC_STATUS_CD varchar(1) -- 置换状态代码
    , TRAN_STATUS_CD varchar(1) -- 转让状态代码
    , LOAN_AST_SECUZT_STATUS_CD varchar(1) -- 贷款资产证券化状态代码
    , LOAN_CONT_INT_RATE_NO varchar(8) -- 贷款合同利率编号
    , LOAN_CONT_INT_RATE_ADJ_MODE_CD varchar(1) -- 贷款合同利率调整方式代码
    , NORMAL_EXC_INT_RATE decimal(20,7) -- 正常执行利率
    , OVDUE_EXC_INT_RATE decimal(20,7) -- 逾期执行利率
    , CRDT_DEFLT_INT_RATE decimal(20,7) -- 贷方违约利率
    , OCCUPY_APPRO_FLAG varchar(1) -- 挤占挪用标志
    , OCCUPY_APPRO_EXC_INT_RATE decimal(20,7) -- 挤占挪用执行利率
    , INT_RATE_FLOT_RATIO decimal(20,7) -- 利率浮动比
    , INTACR_FLAG varchar(1) -- 计息标志
    , COMPND_INT_FLAG varchar(1) -- 计复息标志
    , ACCTN_SUBJ_NO varchar(8) -- 会计科目编号
    , LOAN_CONT_AGREE_SETMENT_DAY varchar(2) -- 贷款合同约定结息日
    , LOAN_CONT_SETMENT_PERIOD_CD varchar(2) -- 贷款合同结息周期代码
    , MATU_AUTO_REPAY_FLAG varchar(1) -- 到期自动还款标志
    , OVDUE_DEBIT_INT_AUTO_REPAY_FLAG varchar(1) -- 逾期欠息自动还款标志
    , DUBIL_LAST_REPAY_DT date -- 借据上次还款日期
    , DUBIL_LAST_SETMENT_DT date -- 借据上次结息日期
    , LOAN_CONT_LOAN_MODE_CD varchar(1) -- 贷款合同放款方式代码
    , LOAN_REPAY_GRACE_TERM varchar(2) -- 贷款还款宽限期限
    , WRTOFF_STATUS_CD varchar(1) -- 核销状态代码
    , OFBS_BAL decimal(20,2) -- 表外余额
    , LOAN_WRTOFF_DT date -- 贷款核销日期
    , LOAN_WRTOFF_RETRA_STATUS_CD varchar(1) -- 贷款核销收回状态代码
    , LOAN_GRP_CD varchar(1) -- 贷款分组代码
    , LOAN_CONT_CAP_SRC_CD varchar(1) -- 贷款合同资金来源代码
    , LOAN_CONT_TERM_CD varchar(1) -- 贷款合同期限代码
    , CUST_MDL_CATOGR_CD varchar(2) -- 客户中类代码
    , CORP_SCALE_CD varchar(1) -- 企业规模代码
    , CORP_CUST_REG_INDUS_CD varchar(4) -- 对公客户注册行业代码
    , BORR_TYPE_CD varchar(2) -- 借款人类型代码
    , CUST_CRDT_CD varchar(1) -- 客户信用等级代码
    , BANK_ENTERP_FLAG varchar(1) -- 银企标志
    , LOAN_INVSTG_CUST_MGR_TELR_NO varchar(7) -- 贷款调查客户经理柜员编号
    , REVOLV_LOAN_FLAG varchar(1) -- 循环贷款标志
    , GREEN_LOAN_FLAG varchar(1) -- 绿色贷款标志
    , CONVT_RMB_LOAN_APP_AMT decimal(20,2) -- 折人民币贷款申请金额
    , LOAN_CONT_GUAR_MODE_CD varchar(3) -- 贷款合同担保方式代码
    , LOAN_USAGE_CD varchar(3) -- 贷款用途代码
    , LOAN_INDUS_INVEST_CD varchar(4) -- 贷款行业投向代码
    , EXT_CONT_NO varchar(16) -- 展期合同编号
    , EXT_TMS varchar(3) -- 展期次数
    , LOAN_BELONG_ORG_NO varchar(6) -- 贷款归属机构编号
    , LOAN_BELONG_CUST_MGR_TELR_NO varchar(7) -- 贷款归属客户经理柜员编号
    , LOAN_ACCT_NO varchar(22) -- 放款账号
    , MTG_LOAN_REPAY_PERIOD_CD varchar(1) -- 按揭贷款还款周期代码
    , MTG_LOAN_CUM_REPAY_INT_AMT decimal(20,2) -- 按揭贷款累计还息金额
    , MTG_LOAN_TOTAL_TMS varchar(5) -- 按揭贷款总期数
    , MTG_LOAN_REPAY_CURR_PERIOD_TMS varchar(5) -- 按揭贷款还款当期期数
    , MTG_LOAN_REPAY_MODE_CD varchar(1) -- 按揭贷款还款方式代码
    , MTG_LOAN_OVDUE_CNT varchar(5) -- 按揭贷款逾期笔数
    , MTG_LOAN_AGREE_REPAY_DAY varchar(2) -- 按揭贷款约定还款日
    , MTG_LOAN_CURR_MONTHLY_PY_AMT decimal(20,2) -- 按揭贷款当前月供金额
    , DISCNT_TXN_TYPE_CD varchar(2) -- 贴现交易类型代码
    , INTRA_BANK_DISCNT_TXN_FLAG varchar(1) -- 系统内贴现交易标志
    , DISCNT_DRFT_TYPE_CD varchar(1) -- 贴现汇票类型代码
    , DISCNT_DRFT_SHEET_CNT varchar(3) -- 贴现汇票张数
    , DISCNT_INT_TOTAL_AMT decimal(20,2) -- 贴现利息总额
    , SUPCHA_ORDER_NO varchar(100) -- 供应链订单编号
    , SETUP_ORG_NO varchar(6) -- 建立机构编号
    , SETUP_TELR_NO varchar(7) -- 建立柜员编号
    , SETUP_DT date -- 建立日期
    , MODIF_ORG_NO varchar(6) -- 修改机构编号
    , MODIF_TELR_NO varchar(7) -- 修改柜员编号
    , FINAL_MODIF_DT date -- 最后修改日期
    , TAB_SRC_CD varchar(1) -- 来源表代码
    , PT_DT  varchar(10) -- 数据日期
)
comment '贴现分户文件'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_04(
      LOAN_CONT_NO -- 贷款合同编号
    , DUBIL_NO -- 借据编号
    , LNACCT_NO -- 贷款账户编号
    , CURR_CD -- 币种代码
    , BORR_CUST_IN_CD -- 借款人客户内码
    , BORR_CUST_NO -- 借款人客户号
    , BORR_CUST_NAME -- 借款人客户名称
    , LOANPROD_NO -- 贷款产品编号
    , LOANPROD_NAME -- 贷款产品名称
    , LOAN_AMT -- 贷款发放金额
    , LOAN_BAL -- 贷款余额
    , CONVT_RMB_LOAN_BAL -- 折人民币贷款余额
    , YESTD_LOAN_BAL -- 昨日贷款余额
    , LOAN_DT -- 贷款发放日期
    , LOAN_MATU_DT -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
    , LNACCT_STATUS_CD -- 贷款账户状态代码
    , COMUT_DEBT_STATUS_CD -- 抵债状态代码
    , DISPLC_STATUS_CD -- 置换状态代码
    , TRAN_STATUS_CD -- 转让状态代码
    , LOAN_AST_SECUZT_STATUS_CD -- 贷款资产证券化状态代码
    , LOAN_CONT_INT_RATE_NO -- 贷款合同利率编号
    , LOAN_CONT_INT_RATE_ADJ_MODE_CD -- 贷款合同利率调整方式代码
    , NORMAL_EXC_INT_RATE -- 正常执行利率
    , OVDUE_EXC_INT_RATE -- 逾期执行利率
    , CRDT_DEFLT_INT_RATE -- 贷方违约利率
    , OCCUPY_APPRO_FLAG -- 挤占挪用标志
    , OCCUPY_APPRO_EXC_INT_RATE -- 挤占挪用执行利率
    , INT_RATE_FLOT_RATIO -- 利率浮动比
    , INTACR_FLAG -- 计息标志
    , COMPND_INT_FLAG -- 计复息标志
    , ACCTN_SUBJ_NO -- 会计科目编号
    , LOAN_CONT_AGREE_SETMENT_DAY -- 贷款合同约定结息日
    , LOAN_CONT_SETMENT_PERIOD_CD -- 贷款合同结息周期代码
    , MATU_AUTO_REPAY_FLAG -- 到期自动还款标志
    , OVDUE_DEBIT_INT_AUTO_REPAY_FLAG -- 逾期欠息自动还款标志
    , DUBIL_LAST_REPAY_DT -- 借据上次还款日期
    , DUBIL_LAST_SETMENT_DT -- 借据上次结息日期
    , LOAN_CONT_LOAN_MODE_CD -- 贷款合同放款方式代码
    , LOAN_REPAY_GRACE_TERM -- 贷款还款宽限期限
    , WRTOFF_STATUS_CD -- 核销状态代码
    , OFBS_BAL -- 表外余额
    , LOAN_WRTOFF_DT -- 贷款核销日期
    , LOAN_WRTOFF_RETRA_STATUS_CD -- 贷款核销收回状态代码
    , LOAN_GRP_CD -- 贷款分组代码
    , LOAN_CONT_CAP_SRC_CD -- 贷款合同资金来源代码
    , LOAN_CONT_TERM_CD -- 贷款合同期限代码
    , CUST_MDL_CATOGR_CD -- 客户中类代码
    , CORP_SCALE_CD -- 企业规模代码
    , CORP_CUST_REG_INDUS_CD -- 对公客户注册行业代码
    , BORR_TYPE_CD -- 借款人类型代码
    , CUST_CRDT_CD -- 客户信用等级代码
    , BANK_ENTERP_FLAG -- 银企标志
    , LOAN_INVSTG_CUST_MGR_TELR_NO -- 贷款调查客户经理柜员编号
    , REVOLV_LOAN_FLAG -- 循环贷款标志
    , GREEN_LOAN_FLAG -- 绿色贷款标志
    , CONVT_RMB_LOAN_APP_AMT -- 折人民币贷款申请金额
    , LOAN_CONT_GUAR_MODE_CD -- 贷款合同担保方式代码
    , LOAN_USAGE_CD -- 贷款用途代码
    , LOAN_INDUS_INVEST_CD -- 贷款行业投向代码
    , EXT_CONT_NO -- 展期合同编号
    , EXT_TMS -- 展期次数
    , LOAN_BELONG_ORG_NO -- 贷款归属机构编号
    , LOAN_BELONG_CUST_MGR_TELR_NO -- 贷款归属客户经理柜员编号
    , LOAN_ACCT_NO -- 放款账号
    , MTG_LOAN_REPAY_PERIOD_CD -- 按揭贷款还款周期代码
    , MTG_LOAN_CUM_REPAY_INT_AMT -- 按揭贷款累计还息金额
    , MTG_LOAN_TOTAL_TMS -- 按揭贷款总期数
    , MTG_LOAN_REPAY_CURR_PERIOD_TMS -- 按揭贷款还款当期期数
    , MTG_LOAN_REPAY_MODE_CD -- 按揭贷款还款方式代码
    , MTG_LOAN_OVDUE_CNT -- 按揭贷款逾期笔数
    , MTG_LOAN_AGREE_REPAY_DAY -- 按揭贷款约定还款日
    , MTG_LOAN_CURR_MONTHLY_PY_AMT -- 按揭贷款当前月供金额
    , DISCNT_TXN_TYPE_CD -- 贴现交易类型代码
    , INTRA_BANK_DISCNT_TXN_FLAG -- 系统内贴现交易标志
    , DISCNT_DRFT_TYPE_CD -- 贴现汇票类型代码
    , DISCNT_DRFT_SHEET_CNT -- 贴现汇票张数
    , DISCNT_INT_TOTAL_AMT -- 贴现利息总额
    , SUPCHA_ORDER_NO -- 供应链订单编号
    , SETUP_ORG_NO -- 建立机构编号
    , SETUP_TELR_NO -- 建立柜员编号
    , SETUP_DT -- 建立日期
    , MODIF_ORG_NO -- 修改机构编号
    , MODIF_TELR_NO -- 修改柜员编号
    , FINAL_MODIF_DT -- 最后修改日期
    , TAB_SRC_CD -- 来源表代码
    , PT_DT  -- 数据日期
)
select
      T1.NTAACONO as LOAN_CONT_NO -- 贷款合同号
    , T1.NTABSN03 as DUBIL_NO -- 借据序号
    , T1.NTAAAC15 as LNACCT_NO -- 贷款账号
    , T1.NTAACCYC as CURR_CD -- 币种
    , T1.NTAACSNO as BORR_CUST_IN_CD -- 客户内码
    , T1.NTABCSID as BORR_CUST_NO -- 客户号
    , T1.NTANFLNM as BORR_CUST_NAME -- 客户名称
    , T1.NTAAPRNO as LOANPROD_NO -- 贷款产品代码
    , T2.UTBCFLNM as LOANPROD_NAME -- 贷款产品名称
    , T1.NTADAMT as LOAN_AMT -- 汇票总金额
    , T1.NTACBLNC as LOAN_BAL -- 贴现余额
    , T1.NTACBLNC*T3.YRCAEXRT as CONVT_RMB_LOAN_BAL -- 年终报表汇率
    , t1.NTADBLNC as YESTD_LOAN_BAL -- 昨日余额
    , t1.NTASDATE as LOAN_DT -- 贴现日期
    , t1.NTATDATE as LOAN_MATU_DT -- 贴现到期日期
    , t1.NTAFCLS4 as CURR_LOAN_LVL_FOUR_MODAL_CD -- 贷款形态4级
    , t1.NTAHCLS5 as CURR_LOAN_TEN_LVL_MODAL_CD -- 贷款形态5级
    , t1.NTAAACST as LNACCT_STATUS_CD -- 贷款账户状态
    , t1.NTAAACWF as COMUT_DEBT_STATUS_CD -- 账户处理状态2
    , t1.NTAAACWF as DISPLC_STATUS_CD -- 账户处理状态3
    , t1.NTAAACWF as TRAN_STATUS_CD -- 账户处理状态4
    , '' as LOAN_AST_SECUZT_STATUS_CD -- None
    , t1.NTAAIRCD as LOAN_CONT_INT_RATE_NO -- 贷款利率代码
    , t1.NTAARTTY as LOAN_CONT_INT_RATE_ADJ_MODE_CD -- 利率调整方式
    , t1.NTAERATE as NORMAL_EXC_INT_RATE -- 正常执行利率
    , t1.NTAFRATE as OVDUE_EXC_INT_RATE -- 逾期执行利率
    , t1.NTACRATE as CRDT_DEFLT_INT_RATE -- 贷方违约利率
    , t1.NTBRFLAG as OCCUPY_APPRO_FLAG -- 挤占挪用标志
    , t1.NTAGRATE as OCCUPY_APPRO_EXC_INT_RATE -- 挤占挪用执行利率
    , t1.NTADRTIO as INT_RATE_FLOT_RATIO -- 利率浮动比
    , t1.NTAAFLAG as COMPND_INT_FLAG -- 计复息标志
    , t1.NTAAACID as ACCTN_SUBJ_NO -- 科目代号
    , '' as LOAN_CONT_AGREE_SETMENT_DAY -- None
    , '' as LOAN_CONT_SETMENT_PERIOD_CD -- None
    , '' as MATU_AUTO_REPAY_FLAG -- None
    , '' as OVDUE_DEBIT_INT_AUTO_REPAY_FLAG -- None
    , '' as DUBIL_LAST_REPAY_DT -- None
    , '' as DUBIL_LAST_SETMENT_DT -- None
    , '' as LOAN_CONT_LOAN_MODE_CD -- None
    , '' as LOAN_REPAY_GRACE_TERM -- None
    , substr(T1.NTAAACWF,1,1) as WRTOFF_STATUS_CD -- 账户处理状态
    , '' as OFBS_BAL -- None
    , T6.AFBJDATE as LOAN_WRTOFF_DT -- 核销日期
    , T6.AFCGFLAG as LOAN_WRTOFF_RETRA_STATUS_CD -- 收回标志
    , T1.NTBJAMT as LOAN_GRP_CD -- 表外余额
    , T1.NTAAFNFR as LOAN_CONT_CAP_SRC_CD -- 资金来源
    , SUBSTR(T1.NTAANO24,1,1) as LOAN_CONT_TERM_CD -- 信息编码
    , T7.SUB_CST_TP as CUST_MDL_CATOGR_CD -- 客户类型（中类）
    , T7.ORG_SCALE as CORP_SCALE_CD -- 企业规模
    , T7.REG_INDS as CORP_CUST_REG_INDUS_CD -- 注册行业
    , SUBSTR(T1.NTAANO24,15,2) as BORR_TYPE_CD -- 信息编码
    , SUBSTR(T1.NTACNO20,14,1) as CUST_CRDT_CD -- 贷款属性编码14
    , T1.NTAABSFG as BANK_ENTERP_FLAG -- 银企标志
    , T8.CREDITOR_NO as LOAN_INVSTG_CUST_MGR_TELR_NO -- 信贷员号
    , '' as REVOLV_LOAN_FLAG -- None
    , T9.GREEN_LOAN_SIGN as GREEN_LOAN_FLAG -- 绿色贷款标志
    , T10.NFAAAMT*T3.YRCAEXRT as CONVT_RMB_LOAN_APP_AMT -- 年终报表汇率、申请金额
    , T10.NFAASSTY as LOAN_CONT_GUAR_MODE_CD -- 担保方式
    , SUBSTR(T1.NTAANO24,11,3) as LOAN_USAGE_CD -- 信息编码
    , SUBSTR(NTACNO20,16,4) as LOAN_INDUS_INVEST_CD -- 贷款属性码16-19
    , '' as EXT_CONT_NO -- None
    , '' as EXT_TMS -- None
    , T1.NTAABRNO as LOAN_BELONG_ORG_NO -- 机构号
    , T1.NTAASTAF as LOAN_BELONG_CUST_MGR_TELR_NO -- 信贷员
    , T11.SVADAC22 as LOAN_ACCT_NO -- 放款账号
    , '' as MTG_LOAN_REPAY_PERIOD_CD -- None
    , '' as MTG_LOAN_CUM_REPAY_INT_AMT -- None
    , '' as MTG_LOAN_TOTAL_TMS -- None
    , '' as MTG_LOAN_REPAY_CURR_PERIOD_TMS -- None
    , '' as MTG_LOAN_REPAY_MODE_CD -- None
    , '' as MTG_LOAN_OVDUE_CNT -- None
    , '' as MTG_LOAN_AGREE_REPAY_DAY -- None
    , '' as MTG_LOAN_CURR_MONTHLY_PY_AMT -- None
    , T1.NTAAINVT as DISCNT_TXN_TYPE_CD -- 交易类型
    , decode(T1.NTAOFLAG,'1','1','2','0',T1.NTAOFLAG) as INTRA_BANK_DISCNT_TXN_FLAG -- 系统内标志
    , T1.NTAABTTP as DISCNT_DRFT_TYPE_CD -- 汇票类型
    , T1.NTAAMAG3 as DISCNT_DRFT_SHEET_CNT -- 汇票张数
    , T1.NTAMIAM2 as DISCNT_INT_TOTAL_AMT -- 贴现利息总额
    , T12.order_no as SUPCHA_ORDER_NO -- 订单编号
    , T1.NTACBRNO as SETUP_ORG_NO -- 建立机构号
    , T1.NTACSTAF as SETUP_TELR_NO -- 建立柜员
    , T1.NTAGDATE as SETUP_DT -- 建立日期
    , T1.NTADBRNO as MODIF_ORG_NO -- 修改机构号
    , T1.NTADSTAF as MODIF_TELR_NO -- 修改柜员
    , T1.NTAHDATE as FINAL_MODIF_DT -- 修改日期
    , '3' as TAB_SRC_CD -- None
    , '${process_date}'  as PT_DT  -- None
from
    ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMDCNT as T1 -- 贴现分户文件
    LEFT JOIN ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMCPUT as T2 -- 贷款产品文件
    on T1.NTAAPRNO = T2.UTAAPRNO 
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_00 as T3 -- None
    on T1.NTAACCYC = T3.YRCACCYC 
    LEFT JOIN ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMCNAF as T6 -- 贷款核销文件
    on T1.NTAACONO = T6.AFAACONO
AND T1.NTABSN03 = T6.AFABSN03
AND T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
    LEFT JOIN (SELECT 
	 P1.CUST_INCOD --客户内码
	,P1.SUB_CST_TP	--客户类别
	,P1.REG_INDS	--注册地址
	,p2.ORG_SCALE	--企业规模

FROM 
	${ODS_ECIF_SCHEMA}.ODS_ECIF_T2_EN_CUST_BASE_INFO P1
LEFT JOIN
	${ODS_ECIF_SCHEMA}.ODS_ECIF_T2_EN_CUST_DETAIL_INFO P2 
 ON P1.CUST_INCOD = P2.CUST_INCOD
	AND  P2.PT_DT='${process_date}' 
	AND P2.DELETED='0'
WHERE  P1.PT_DT='${process_date}' 
	AND P1.DELETED='0'	) as T7 -- 对公客户基本信息
    on T1.TZAACSNO  = T7.CUST_INCOD 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_LOAN_REQUISITION_PUBLIC as T8 -- 借款申请.公共资料
    on T1.NTAACONO = T8.CONTRACT_NO
AND T8.PT_DT='${process_date}' 
AND T8.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_LOAN_REQUISITION_EXT as T9 -- 借款申请.关联信息
    on T1.NTAACONO = T9.CONTENT_KEY
AND T9.PT_DT='${process_date}' 
AND T9.DELETED='0' 
    LEFT JOIN ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMCONF as T10 -- 贷款合同文件
    on T10.NFAACONO = T1.NTAAPRNO
AND T10.PT_DT='${process_date}' 
AND T10.DELETED='0' 
    LEFT JOIN ${ODS_CORE_SCHEMA}.ODS_CORE_BLFMACSV as T11 -- 贷款账户结算文件
    on T11.SVAACONO = T1.NTAACONO
AND T1.NTABSN03 = T11.SVABSN03
AND T11.PT_DT='${process_date}' 
AND T11.DELETED='0'
AND T11.SVBLFLG = '1' 
    LEFT JOIN ${ODS_SCB_SCHEMA}.ODS_SCB_online_lnci_base_info as T12 -- 借据表
    on T12.mast_contno = T1.NTAACONO
AND T1.NTABSN03 = T12.debet_no
AND T12.PT_DT='${process_date}' 
AND T12.DELETED='0' 
where 1=1 
AND SUBSTR(NTAACONO,1,2) IN('82','83','91','92','93')
AND T1.NTAAACST IN('1','3','4','5') --1 审批 3 正常 4 结清 5 销户状态 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第6组）==============
-- 数据汇总

insert into table ${session}.S03_LP_LOAN_DUBIL_STDING_BOOK(
      LOAN_CONT_NO -- 贷款合同编号
    , DUBIL_NO -- 借据编号
    , LNACCT_NO -- 贷款账户编号
    , CURR_CD -- 币种代码
    , BORR_CUST_IN_CD -- 借款人客户内码
    , BORR_CUST_NO -- 借款人客户号
    , BORR_CUST_NAME -- 借款人客户名称
    , LOANPROD_NO -- 贷款产品编号
    , LOANPROD_NAME -- 贷款产品名称
    , LOAN_AMT -- 贷款发放金额
    , LOAN_BAL -- 贷款余额
    , CONVT_RMB_LOAN_BAL -- 折人民币贷款余额
    , YESTD_LOAN_BAL -- 昨日贷款余额
    , LOAN_DT -- 贷款发放日期
    , LOAN_MATU_DT -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
    , LNACCT_STATUS_CD -- 贷款账户状态代码
    , COMUT_DEBT_STATUS_CD -- 抵债状态代码
    , DISPLC_STATUS_CD -- 置换状态代码
    , TRAN_STATUS_CD -- 转让状态代码
    , LOAN_AST_SECUZT_STATUS_CD -- 贷款资产证券化状态代码
    , LOAN_CONT_INT_RATE_NO -- 贷款合同利率编号
    , LOAN_CONT_INT_RATE_ADJ_MODE_CD -- 贷款合同利率调整方式代码
    , NORMAL_EXC_INT_RATE -- 正常执行利率
    , OVDUE_EXC_INT_RATE -- 逾期执行利率
    , CRDT_DEFLT_INT_RATE -- 贷方违约利率
    , OCCUPY_APPRO_FLAG -- 挤占挪用标志
    , OCCUPY_APPRO_EXC_INT_RATE -- 挤占挪用执行利率
    , INT_RATE_FLOT_RATIO -- 利率浮动比
    , INTACR_FLAG -- 计息标志
    , COMPND_INT_FLAG -- 计复息标志
    , ACCTN_SUBJ_NO -- 会计科目编号
    , LOAN_CONT_AGREE_SETMENT_DAY -- 贷款合同约定结息日
    , LOAN_CONT_SETMENT_PERIOD_CD -- 贷款合同结息周期代码
    , MATU_AUTO_REPAY_FLAG -- 到期自动还款标志
    , OVDUE_DEBIT_INT_AUTO_REPAY_FLAG -- 逾期欠息自动还款标志
    , DUBIL_LAST_REPAY_DT -- 借据上次还款日期
    , DUBIL_LAST_SETMENT_DT -- 借据上次结息日期
    , LOAN_CONT_LOAN_MODE_CD -- 贷款合同放款方式代码
    , LOAN_REPAY_GRACE_TERM -- 贷款还款宽限期限
    , WRTOFF_STATUS_CD -- 核销状态代码
    , OFBS_BAL -- 表外余额
    , LOAN_WRTOFF_DT -- 贷款核销日期
    , LOAN_WRTOFF_RETRA_STATUS_CD -- 贷款核销收回状态代码
    , LOAN_GRP_CD -- 贷款分组代码
    , LOAN_CONT_CAP_SRC_CD -- 贷款合同资金来源代码
    , LOAN_CONT_TERM_CD -- 贷款合同期限代码
    , CUST_MDL_CATOGR_CD -- 客户中类代码
    , CORP_SCALE_CD -- 企业规模代码
    , CORP_CUST_REG_INDUS_CD -- 对公客户注册行业代码
    , BORR_TYPE_CD -- 借款人类型代码
    , CUST_CRDT_CD -- 客户信用等级代码
    , BANK_ENTERP_FLAG -- 银企标志
    , LOAN_INVSTG_CUST_MGR_TELR_NO -- 贷款调查客户经理柜员编号
    , REVOLV_LOAN_FLAG -- 循环贷款标志
    , GREEN_LOAN_FLAG -- 绿色贷款标志
    , CONVT_RMB_LOAN_APP_AMT -- 折人民币贷款申请金额
    , LOAN_CONT_GUAR_MODE_CD -- 贷款合同担保方式代码
    , LOAN_USAGE_CD -- 贷款用途代码
    , LOAN_INDUS_INVEST_CD -- 贷款行业投向代码
    , EXT_CONT_NO -- 展期合同编号
    , EXT_TMS -- 展期次数
    , LOAN_BELONG_ORG_NO -- 贷款归属机构编号
    , LOAN_BELONG_CUST_MGR_TELR_NO -- 贷款归属客户经理柜员编号
    , LOAN_ACCT_NO -- 放款账号
    , MTG_LOAN_REPAY_PERIOD_CD -- 按揭贷款还款周期代码
    , MTG_LOAN_CUM_REPAY_INT_AMT -- 按揭贷款累计还息金额
    , MTG_LOAN_TOTAL_TMS -- 按揭贷款总期数
    , MTG_LOAN_REPAY_CURR_PERIOD_TMS -- 按揭贷款还款当期期数
    , MTG_LOAN_REPAY_MODE_CD -- 按揭贷款还款方式代码
    , MTG_LOAN_OVDUE_CNT -- 按揭贷款逾期笔数
    , MTG_LOAN_AGREE_REPAY_DAY -- 按揭贷款约定还款日
    , MTG_LOAN_CURR_MONTHLY_PY_AMT -- 按揭贷款当前月供金额
    , DISCNT_TXN_TYPE_CD -- 贴现交易类型代码
    , INTRA_BANK_DISCNT_TXN_FLAG -- 系统内贴现交易标志
    , DISCNT_DRFT_TYPE_CD -- 贴现汇票类型代码
    , DISCNT_DRFT_SHEET_CNT -- 贴现汇票张数
    , DISCNT_INT_TOTAL_AMT -- 贴现利息总额
    , SUPCHA_ORDER_NO -- 供应链订单编号
    , SETUP_ORG_NO -- 建立机构编号
    , SETUP_TELR_NO -- 建立柜员编号
    , SETUP_DT -- 建立日期
    , MODIF_ORG_NO -- 修改机构编号
    , MODIF_TELR_NO -- 修改柜员编号
    , FINAL_MODIF_DT -- 最后修改日期
    , TAB_SRC_CD -- 来源表代码
    , PT_DT  -- 数据日期
)
select
      t1.LOAN_CONT_NO as LOAN_CONT_NO -- 贷款合同编号
    , t1.DUBIL_NO as DUBIL_NO -- 借据编号
    , t1.LNACCT_NO as LNACCT_NO -- 贷款账户编号
    , t1.CURR_CD as CURR_CD -- 币种代码
    , t1.BORR_CUST_IN_CD as BORR_CUST_IN_CD -- 借款人客户内码
    , t1.BORR_CUST_NO as BORR_CUST_NO -- 借款人客户号
    , t1.BORR_CUST_NAME as BORR_CUST_NAME -- 借款人客户名称
    , t1.LOANPROD_NO as LOANPROD_NO -- 贷款产品编号
    , t1.LOANPROD_NAME as LOANPROD_NAME -- 贷款产品名称
    , t1.LOAN_AMT as LOAN_AMT -- 贷款发放金额
    , t1.LOAN_BAL as LOAN_BAL -- 贷款余额
    , t1.CONVT_RMB_LOAN_BAL as CONVT_RMB_LOAN_BAL -- 折人民币贷款余额
    , t1.YESTD_LOAN_BAL as YESTD_LOAN_BAL -- 昨日贷款余额
    , t1.LOAN_DT as LOAN_DT -- 贷款发放日期
    , t1.LOAN_MATU_DT as LOAN_MATU_DT -- 贷款到期日期
    , t1.CURR_LOAN_LVL_FOUR_MODAL_CD as CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
    , t1.CURR_LOAN_TEN_LVL_MODAL_CD as CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
    , t1.LNACCT_STATUS_CD as LNACCT_STATUS_CD -- 贷款账户状态代码
    , t1.COMUT_DEBT_STATUS_CD as COMUT_DEBT_STATUS_CD -- 抵债状态代码
    , t1.DISPLC_STATUS_CD as DISPLC_STATUS_CD -- 置换状态代码
    , t1.TRAN_STATUS_CD as TRAN_STATUS_CD -- 转让状态代码
    , t1.LOAN_AST_SECUZT_STATUS_CD as LOAN_AST_SECUZT_STATUS_CD -- 贷款资产证券化状态代码
    , t1.LOAN_CONT_INT_RATE_NO as LOAN_CONT_INT_RATE_NO -- 贷款合同利率编号
    , t1.LOAN_CONT_INT_RATE_ADJ_MODE_CD as LOAN_CONT_INT_RATE_ADJ_MODE_CD -- 贷款合同利率调整方式代码
    , t1.NORMAL_EXC_INT_RATE as NORMAL_EXC_INT_RATE -- 正常执行利率
    , t1.OVDUE_EXC_INT_RATE as OVDUE_EXC_INT_RATE -- 逾期执行利率
    , t1.CRDT_DEFLT_INT_RATE as CRDT_DEFLT_INT_RATE -- 贷方违约利率
    , t1.OCCUPY_APPRO_FLAG as OCCUPY_APPRO_FLAG -- 挤占挪用标志
    , t1.OCCUPY_APPRO_EXC_INT_RATE as OCCUPY_APPRO_EXC_INT_RATE -- 挤占挪用执行利率
    , t1.INT_RATE_FLOT_RATIO as INT_RATE_FLOT_RATIO -- 利率浮动比
    , t1.INTACR_FLAG as INTACR_FLAG -- 计息标志
    , t1.COMPND_INT_FLAG as COMPND_INT_FLAG -- 计复息标志
    , t1.ACCTN_SUBJ_NO as ACCTN_SUBJ_NO -- 会计科目编号
    , t1.LOAN_CONT_AGREE_SETMENT_DAY as LOAN_CONT_AGREE_SETMENT_DAY -- 贷款合同约定结息日
    , t1.LOAN_CONT_SETMENT_PERIOD_CD as LOAN_CONT_SETMENT_PERIOD_CD -- 贷款合同结息周期代码
    , t1.MATU_AUTO_REPAY_FLAG as MATU_AUTO_REPAY_FLAG -- 到期自动还款标志
    , t1.OVDUE_DEBIT_INT_AUTO_REPAY_FLAG as OVDUE_DEBIT_INT_AUTO_REPAY_FLAG -- 逾期欠息自动还款标志
    , t1.DUBIL_LAST_REPAY_DT as DUBIL_LAST_REPAY_DT -- 借据上次还款日期
    , t1.DUBIL_LAST_SETMENT_DT as DUBIL_LAST_SETMENT_DT -- 借据上次结息日期
    , t1.LOAN_CONT_LOAN_MODE_CD as LOAN_CONT_LOAN_MODE_CD -- 贷款合同放款方式代码
    , t1.LOAN_REPAY_GRACE_TERM as LOAN_REPAY_GRACE_TERM -- 贷款还款宽限期限
    , t1.WRTOFF_STATUS_CD as WRTOFF_STATUS_CD -- 核销状态代码
    , t1.OFBS_BAL as OFBS_BAL -- 表外余额
    , t1.LOAN_WRTOFF_DT as LOAN_WRTOFF_DT -- 贷款核销日期
    , t1.LOAN_WRTOFF_RETRA_STATUS_CD as LOAN_WRTOFF_RETRA_STATUS_CD -- 贷款核销收回状态代码
    , t1.LOAN_GRP_CD as LOAN_GRP_CD -- 贷款分组代码
    , t1.LOAN_CONT_CAP_SRC_CD as LOAN_CONT_CAP_SRC_CD -- 贷款合同资金来源代码
    , t1.LOAN_CONT_TERM_CD as LOAN_CONT_TERM_CD -- 贷款合同期限代码
    , t1.CUST_MDL_CATOGR_CD as CUST_MDL_CATOGR_CD -- 客户中类代码
    , t1.CORP_SCALE_CD as CORP_SCALE_CD -- 企业规模代码
    , t1.CORP_CUST_REG_INDUS_CD as CORP_CUST_REG_INDUS_CD -- 对公客户注册行业代码
    , t1.BORR_TYPE_CD as BORR_TYPE_CD -- 借款人类型代码
    , t1.CUST_CRDT_CD as CUST_CRDT_CD -- 客户信用等级代码
    , t1.BANK_ENTERP_FLAG as BANK_ENTERP_FLAG -- 银企标志
    , t1.LOAN_INVSTG_CUST_MGR_TELR_NO as LOAN_INVSTG_CUST_MGR_TELR_NO -- 贷款调查客户经理柜员编号
    , t1.REVOLV_LOAN_FLAG as REVOLV_LOAN_FLAG -- 循环贷款标志
    , t1.GREEN_LOAN_FLAG as GREEN_LOAN_FLAG -- 绿色贷款标志
    , t1.CONVT_RMB_LOAN_APP_AMT as CONVT_RMB_LOAN_APP_AMT -- 折人民币贷款申请金额
    , t1.LOAN_CONT_GUAR_MODE_CD as LOAN_CONT_GUAR_MODE_CD -- 贷款合同担保方式代码
    , t1.LOAN_USAGE_CD as LOAN_USAGE_CD -- 贷款用途代码
    , t1.LOAN_INDUS_INVEST_CD as LOAN_INDUS_INVEST_CD -- 贷款行业投向代码
    , t1.EXT_CONT_NO as EXT_CONT_NO -- 展期合同编号
    , t1.EXT_TMS as EXT_TMS -- 展期次数
    , t1.LOAN_BELONG_ORG_NO as LOAN_BELONG_ORG_NO -- 贷款归属机构编号
    , t1.LOAN_BELONG_CUST_MGR_TELR_NO as LOAN_BELONG_CUST_MGR_TELR_NO -- 贷款归属客户经理柜员编号
    , t1.LOAN_ACCT_NO as LOAN_ACCT_NO -- 放款账号
    , t1.MTG_LOAN_REPAY_PERIOD_CD as MTG_LOAN_REPAY_PERIOD_CD -- 按揭贷款还款周期代码
    , t1.MTG_LOAN_CUM_REPAY_INT_AMT as MTG_LOAN_CUM_REPAY_INT_AMT -- 按揭贷款累计还息金额
    , t1.MTG_LOAN_TOTAL_TMS as MTG_LOAN_TOTAL_TMS -- 按揭贷款总期数
    , t1.MTG_LOAN_REPAY_CURR_PERIOD_TMS as MTG_LOAN_REPAY_CURR_PERIOD_TMS -- 按揭贷款还款当期期数
    , t1.MTG_LOAN_REPAY_MODE_CD as MTG_LOAN_REPAY_MODE_CD -- 按揭贷款还款方式代码
    , t1.MTG_LOAN_OVDUE_CNT as MTG_LOAN_OVDUE_CNT -- 按揭贷款逾期笔数
    , t1.MTG_LOAN_AGREE_REPAY_DAY as MTG_LOAN_AGREE_REPAY_DAY -- 按揭贷款约定还款日
    , t1.MTG_LOAN_CURR_MONTHLY_PY_AMT as MTG_LOAN_CURR_MONTHLY_PY_AMT -- 按揭贷款当前月供金额
    , t1.DISCNT_TXN_TYPE_CD as DISCNT_TXN_TYPE_CD -- 贴现交易类型代码
    , t1.INTRA_BANK_DISCNT_TXN_FLAG as INTRA_BANK_DISCNT_TXN_FLAG -- 系统内贴现交易标志
    , t1.DISCNT_DRFT_TYPE_CD as DISCNT_DRFT_TYPE_CD -- 贴现汇票类型代码
    , t1.DISCNT_DRFT_SHEET_CNT as DISCNT_DRFT_SHEET_CNT -- 贴现汇票张数
    , t1.DISCNT_INT_TOTAL_AMT as DISCNT_INT_TOTAL_AMT -- 贴现利息总额
    , t1.SUPCHA_ORDER_NO as SUPCHA_ORDER_NO -- 供应链订单编号
    , t1.SETUP_ORG_NO as SETUP_ORG_NO -- 建立机构编号
    , t1.SETUP_TELR_NO as SETUP_TELR_NO -- 建立柜员编号
    , t1.SETUP_DT as SETUP_DT -- 建立日期
    , t1.MODIF_ORG_NO as MODIF_ORG_NO -- 修改机构编号
    , t1.MODIF_TELR_NO as MODIF_TELR_NO -- 修改柜员编号
    , t1.FINAL_MODIF_DT as FINAL_MODIF_DT -- 最后修改日期
    , t1.TAB_SRC_CD as TAB_SRC_CD -- 来源表代码
    , t1.PT_DT  as PT_DT  -- 数据日期
from
    (
SELECT 
	P1.LOAN_CONT_NO
	P1.DUBIL_NO
	P1.LNACCT_NO
	P1.CURR_CD
	P1.BORR_CUST_IN_CD
	P1.BORR_CUST_NO
	P1.BORR_CUST_NAME
	P1.LOANPROD_NO
	P1.LOANPROD_NAME
	P1.LOAN_AMT
	P1.LOAN_BAL
	P1.CONVT_RMB_LOAN_BAL
	P1.YESTD_LOAN_BAL
	P1.LOAN_DT
	P1.LOAN_MATU_DT
	P1.CURR_LOAN_LVL_FOUR_MODAL_CD
	P1.CURR_LOAN_TEN_LVL_MODAL_CD
	P1.LNACCT_STATUS_CD
	P1.COMUT_DEBT_STATUS_CD
	P1.DISPLC_STATUS_CD
	P1.TRAN_STATUS_CD
	P1.LOAN_AST_SECUZT_STATUS_CD
	P1.LOAN_CONT_INT_RATE_NO
	P1.LOAN_CONT_INT_RATE_ADJ_MODE_CD
	P1.NORMAL_EXC_INT_RATE
	P1.OVDUE_EXC_INT_RATE
	P1.CRDT_DEFLT_INT_RATE
	P1.OCCUPY_APPRO_FLAG
	P1.OCCUPY_APPRO_EXC_INT_RATE
	P1.INT_RATE_FLOT_RATIO
	P1.INTACR_FLAG
	P1.COMPND_INT_FLAG
	P1.ACCTN_SUBJ_NO
	P1.LOAN_CONT_AGREE_SETMENT_DAY
	P1.LOAN_CONT_SETMENT_PERIOD_CD
	P1.MATU_AUTO_REPAY_FLAG
	P1.OVDUE_DEBIT_INT_AUTO_REPAY_FLAG
	P1.DUBIL_LAST_REPAY_DT
	P1.DUBIL_LAST_SETMENT_DT
	P1.LOAN_CONT_LOAN_MODE_CD
	P1.LOAN_REPAY_GRACE_TERM
	P1.WRTOFF_STATUS_CD
	P1.OFBS_BAL
	P1.LOAN_WRTOFF_DT
	P1.LOAN_WRTOFF_RETRA_STATUS_CD
	P1.LOAN_GRP_CD
	P1.LOAN_CONT_CAP_SRC_CD
	P1.LOAN_CONT_TERM_CD
	P1.CUST_MDL_CATOGR_CD
	P1.CORP_SCALE_CD
	P1.CORP_CUST_REG_INDUS_CD
	P1.BORR_TYPE_CD
	P1.CUST_CRDT_CD
	P1.BANK_ENTERP_FLAG
	P1.LOAN_INVSTG_CUST_MGR_TELR_NO
	P1.REVOLV_LOAN_FLAG
	P1.GREEN_LOAN_FLAG
	P1.CONVT_RMB_LOAN_APP_AMT
	P1.LOAN_CONT_GUAR_MODE_CD
	P1.LOAN_USAGE_CD
	P1.LOAN_INDUS_INVEST_CD
	P1.EXT_CONT_NO
	P1.EXT_TMS
	P1.LOAN_BELONG_ORG_NO
	P1.LOAN_BELONG_CUST_MGR_TELR_NO
	P1.LOAN_ACCT_NO
	P1.MTG_LOAN_REPAY_PERIOD_CD
	P1.MTG_LOAN_CUM_REPAY_INT_AMT
	P1.MTG_LOAN_TOTAL_TMS
	P1.MTG_LOAN_REPAY_CURR_PERIOD_TMS
	P1.MTG_LOAN_REPAY_MODE_CD
	P1.MTG_LOAN_OVDUE_CNT
	P1.MTG_LOAN_AGREE_REPAY_DAY
	P1.MTG_LOAN_CURR_MONTHLY_PY_AMT
	P1.DISCNT_TXN_TYPE_CD
	P1.INTRA_BANK_DISCNT_TXN_FLAG
	P1.DISCNT_DRFT_TYPE_CD
	P1.DISCNT_DRFT_SHEET_CNT
	P1.DISCNT_INT_TOTAL_AMT
	P1.SUPCHA_ORDER_NO
	P1.SETUP_ORG_NO
	P1.SETUP_TELR_NO
	P1.SETUP_DT
	P1.MODIF_ORG_NO
	P1.MODIF_TELR_NO
	P1.FINAL_MODIF_DT
	P1.TAB_SRC_CD
	P1.PT_DT 
FROM 
TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_04 P1

UNION ALL 

SELECT 
	P2.LOAN_CONT_NO
	P2.DUBIL_NO
	P2.LNACCT_NO
	P2.CURR_CD
	P2.BORR_CUST_IN_CD
	P2.BORR_CUST_NO
	P2.BORR_CUST_NAME
	P2.LOANPROD_NO
	P2.LOANPROD_NAME
	P2.LOAN_AMT
	P2.LOAN_BAL
	P2.CONVT_RMB_LOAN_BAL
	P2.YESTD_LOAN_BAL
	P2.LOAN_DT
	P2.LOAN_MATU_DT
	P2.CURR_LOAN_LVL_FOUR_MODAL_CD
	P2.CURR_LOAN_TEN_LVL_MODAL_CD
	P2.LNACCT_STATUS_CD
	P2.COMUT_DEBT_STATUS_CD
	P2.DISPLC_STATUS_CD
	P2.TRAN_STATUS_CD
	P2.LOAN_AST_SECUZT_STATUS_CD
	P2.LOAN_CONT_INT_RATE_NO
	P2.LOAN_CONT_INT_RATE_ADJ_MODE_CD
	P2.NORMAL_EXC_INT_RATE
	P2.OVDUE_EXC_INT_RATE
	P2.CRDT_DEFLT_INT_RATE
	P2.OCCUPY_APPRO_FLAG
	P2.OCCUPY_APPRO_EXC_INT_RATE
	P2.INT_RATE_FLOT_RATIO
	P2.INTACR_FLAG
	P2.COMPND_INT_FLAG
	P2.ACCTN_SUBJ_NO
	P2.LOAN_CONT_AGREE_SETMENT_DAY
	P2.LOAN_CONT_SETMENT_PERIOD_CD
	P2.MATU_AUTO_REPAY_FLAG
	P2.OVDUE_DEBIT_INT_AUTO_REPAY_FLAG
	P2.DUBIL_LAST_REPAY_DT
	P2.DUBIL_LAST_SETMENT_DT
	P2.LOAN_CONT_LOAN_MODE_CD
	P2.LOAN_REPAY_GRACE_TERM
	P2.WRTOFF_STATUS_CD
	P2.OFBS_BAL
	P2.LOAN_WRTOFF_DT
	P2.LOAN_WRTOFF_RETRA_STATUS_CD
	P2.LOAN_GRP_CD
	P2.LOAN_CONT_CAP_SRC_CD
	P2.LOAN_CONT_TERM_CD
	P2.CUST_MDL_CATOGR_CD
	P2.CORP_SCALE_CD
	P2.CORP_CUST_REG_INDUS_CD
	P2.BORR_TYPE_CD
	P2.CUST_CRDT_CD
	P2.BANK_ENTERP_FLAG
	P2.LOAN_INVSTG_CUST_MGR_TELR_NO
	P2.REVOLV_LOAN_FLAG
	P2.GREEN_LOAN_FLAG
	P2.CONVT_RMB_LOAN_APP_AMT
	P2.LOAN_CONT_GUAR_MODE_CD
	P2.LOAN_USAGE_CD
	P2.LOAN_INDUS_INVEST_CD
	P2.EXT_CONT_NO
	P2.EXT_TMS
	P2.LOAN_BELONG_ORG_NO
	P2.LOAN_BELONG_CUST_MGR_TELR_NO
	P2.LOAN_ACCT_NO
	P2.MTG_LOAN_REPAY_PERIOD_CD
	P2.MTG_LOAN_CUM_REPAY_INT_AMT
	P2.MTG_LOAN_TOTAL_TMS
	P2.MTG_LOAN_REPAY_CURR_PERIOD_TMS
	P2.MTG_LOAN_REPAY_MODE_CD
	P2.MTG_LOAN_OVDUE_CNT
	P2.MTG_LOAN_AGREE_REPAY_DAY
	P2.MTG_LOAN_CURR_MONTHLY_PY_AMT
	P2.DISCNT_TXN_TYPE_CD
	P2.INTRA_BANK_DISCNT_TXN_FLAG
	P2.DISCNT_DRFT_TYPE_CD
	P2.DISCNT_DRFT_SHEET_CNT
	P2.DISCNT_INT_TOTAL_AMT
	P2.SUPCHA_ORDER_NO
	P2.SETUP_ORG_NO
	P2.SETUP_TELR_NO
	P2.SETUP_DT
	P2.MODIF_ORG_NO
	P2.MODIF_TELR_NO
	P2.FINAL_MODIF_DT
	P2.TAB_SRC_CD
	P2.PT_DT 
FROM 
TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_03 P2
UNION ALL 

SELECT 
	P3.LOAN_CONT_NO
	P3.DUBIL_NO
	P3.LNACCT_NO
	P3.CURR_CD
	P3.BORR_CUST_IN_CD
	P3.BORR_CUST_NO
	P3.BORR_CUST_NAME
	P3.LOANPROD_NO
	P3.LOANPROD_NAME
	P3.LOAN_AMT
	P3.LOAN_BAL
	P3.CONVT_RMB_LOAN_BAL
	P3.YESTD_LOAN_BAL
	P3.LOAN_DT
	P3.LOAN_MATU_DT
	P3.CURR_LOAN_LVL_FOUR_MODAL_CD
	P3.CURR_LOAN_TEN_LVL_MODAL_CD
	P3.LNACCT_STATUS_CD
	P3.COMUT_DEBT_STATUS_CD
	P3.DISPLC_STATUS_CD
	P3.TRAN_STATUS_CD
	P3.LOAN_AST_SECUZT_STATUS_CD
	P3.LOAN_CONT_INT_RATE_NO
	P3.LOAN_CONT_INT_RATE_ADJ_MODE_CD
	P3.NORMAL_EXC_INT_RATE
	P3.OVDUE_EXC_INT_RATE
	P3.CRDT_DEFLT_INT_RATE
	P3.OCCUPY_APPRO_FLAG
	P3.OCCUPY_APPRO_EXC_INT_RATE
	P3.INT_RATE_FLOT_RATIO
	P3.INTACR_FLAG
	P3.COMPND_INT_FLAG
	P3.ACCTN_SUBJ_NO
	P3.LOAN_CONT_AGREE_SETMENT_DAY
	P3.LOAN_CONT_SETMENT_PERIOD_CD
	P3.MATU_AUTO_REPAY_FLAG
	P3.OVDUE_DEBIT_INT_AUTO_REPAY_FLAG
	P3.DUBIL_LAST_REPAY_DT
	P3.DUBIL_LAST_SETMENT_DT
	P3.LOAN_CONT_LOAN_MODE_CD
	P3.LOAN_REPAY_GRACE_TERM
	P3.WRTOFF_STATUS_CD
	P3.OFBS_BAL
	P3.LOAN_WRTOFF_DT
	P3.LOAN_WRTOFF_RETRA_STATUS_CD
	P3.LOAN_GRP_CD
	P3.LOAN_CONT_CAP_SRC_CD
	P3.LOAN_CONT_TERM_CD
	P3.CUST_MDL_CATOGR_CD
	P3.CORP_SCALE_CD
	P3.CORP_CUST_REG_INDUS_CD
	P3.BORR_TYPE_CD
	P3.CUST_CRDT_CD
	P3.BANK_ENTERP_FLAG
	P3.LOAN_INVSTG_CUST_MGR_TELR_NO
	P3.REVOLV_LOAN_FLAG
	P3.GREEN_LOAN_FLAG
	P3.CONVT_RMB_LOAN_APP_AMT
	P3.LOAN_CONT_GUAR_MODE_CD
	P3.LOAN_USAGE_CD
	P3.LOAN_INDUS_INVEST_CD
	P3.EXT_CONT_NO
	P3.EXT_TMS
	P3.LOAN_BELONG_ORG_NO
	P3.LOAN_BELONG_CUST_MGR_TELR_NO
	P3.LOAN_ACCT_NO
	P3.MTG_LOAN_REPAY_PERIOD_CD
	P3.MTG_LOAN_CUM_REPAY_INT_AMT
	P3.MTG_LOAN_TOTAL_TMS
	P3.MTG_LOAN_REPAY_CURR_PERIOD_TMS
	P3.MTG_LOAN_REPAY_MODE_CD
	P3.MTG_LOAN_OVDUE_CNT
	P3.MTG_LOAN_AGREE_REPAY_DAY
	P3.MTG_LOAN_CURR_MONTHLY_PY_AMT
	P3.DISCNT_TXN_TYPE_CD
	P3.INTRA_BANK_DISCNT_TXN_FLAG
	P3.DISCNT_DRFT_TYPE_CD
	P3.DISCNT_DRFT_SHEET_CNT
	P3.DISCNT_INT_TOTAL_AMT
	P3.SUPCHA_ORDER_NO
	P3.SETUP_ORG_NO
	P3.SETUP_TELR_NO
	P3.SETUP_DT
	P3.MODIF_ORG_NO
	P3.MODIF_TELR_NO
	P3.FINAL_MODIF_DT
	P3.TAB_SRC_CD
	P3.PT_DT 
FROM 
TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_02 P3
) as T1 -- 汇总数据
;

-- 删除所有临时表
drop table ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_00;
drop table ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_01;
drop table ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_02;
drop table ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_03;
drop table ${session}.TMP_S03_LP_LOAN_DUBIL_STDING_BOOK_04;