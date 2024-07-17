-- 层次表名: 聚合层-公司贷款资产分类形态变更聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s03_lp_loan_biz_qlty_cls_tab
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S03_LP_LOAN_BIZ_QLTY_CLS_TAB
--     表中文名：公司贷款资产分类形态变更聚合表
--     创建日期：2023-01-04 00:00:00
--     主键字段：LOAN_CONT_NO,DUBIL_NO,贷款形态修改通知书编号
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：业务范围涵盖对公贷款产品如农、工业、商业和服务业、房地产、项目开发、国际贸易、垫款等表内借据资产分类形态变更信息，包括原四、五级形态，新四、五级形态，涉及到柜面核心、大信贷平台、供应链系统、在线融资系统、国际结算业务系统
--     更新记录：
--         2023-01-04 00:00:00 王穆军 new
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S03_LP_LOAN_BIZ_QLTY_CLS_TAB drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 公司贷款资产分类形态变更聚合表 数据组1

insert into table ${session}.S03_LP_LOAN_BIZ_QLTY_CLS_TAB(
      LOAN_CONT_NO -- 贷款合同编号
    , DUBIL_NO -- 借据编号
    , 业务术语待申请 -- 贷款形态修改通知书编号
    , LNACCT_NO -- 贷款账户编号
    , CUST_IN_CD -- 客户内码
    , CUST_NO -- 客户号
    , CUST_NAME -- 客户名称
    , CURR_CD -- 币种代码
    , LOANPROD_NO -- 贷款产品编号
    , 业务术语待申请 -- 原十级形态代码
    , 业务术语待申请 -- 新十级形态代码
    , 业务术语待申请 -- 原十级形态转出金额
    , 业务术语待申请 -- 新十级形态转入金额
    , 业务术语待申请 -- 原四级形态代码
    , 业务术语待申请 -- 新四级形态代码
    , 业务术语待申请 -- 原四级形态转出金额
    , 业务术语待申请 -- 新四级形态转入金额
    , 业务术语待申请 -- 经办客户经理柜员编号
    , 业务术语待申请 -- 调整柜员编号
    , 业务术语待申请 -- 审批柜员编号
    , 业务术语待申请 -- 交易来源代码
    , TXN_HAPP_BELONG_ORG_NO -- 交易发生归属机构编号
    , 业务术语待申请 -- 新贷款产品科目代码
    , 业务术语待申请 -- 原贷款产品科目代码
    , 业务术语待申请 -- 贷款四级形态逾期利率
    , FINAL_MODIF_DT -- 最后修改日期
    , PT_DT -- 数据日期
)
select
      T1.VCAACONO as LOAN_CONT_NO -- 贷款合同号
    , T1.VCABSN03 as DUBIL_NO -- 借据序号
    , T1.VCAANO18 as 业务术语待申请 -- 通知书编号
    , T1.VCAAAC15 as LNACCT_NO -- 贷款账号
    , T1.VCAACSNO as CUST_IN_CD -- 客户内码
    , T1.VCABCSID as CUST_NO -- 客户号
    , T1.VCANFLNM as CUST_NAME -- 客户名称
    , T1.VCAACCYC as CURR_CD -- 币种
    , T2.STAAPRNO as LOANPROD_NO -- 贷款产品代码
    , T1.VCAICLS5 as 业务术语待申请 -- 原五级形态
    , T1.VCAJCLS5 as 业务术语待申请 -- 新五级形态
    , T1.VCBUAMT as 业务术语待申请 -- 转入金额
    , T1.VCBVAMT as 业务术语待申请 -- 转出金额
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , T1.VCAHSTAF as 业务术语待申请 -- 经办柜员
    , T1.VCAGSTAF as 业务术语待申请 -- 调整柜员
    , T1.VCAISTAF as 业务术语待申请 -- 审批柜员
    , T1.VCAABSSC as 业务术语待申请 -- 交易来源
    , T1.VCAABRNO as TXN_HAPP_BELONG_ORG_NO -- 机构号
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , T1.VCAVDATE as FINAL_MODIF_DT -- 调整日期
    , '${process_date}' as PT_DT -- None
from
    ${ODS_CORE_SCHEMA}.BLFMRGVC as T1 -- 贷款五级形态调整登记簿
    LEFT JOIN ${ODS_CORE_SCHEMA}.BLFMMAST as T2 -- 普通贷款分户文件
    on T2.STAACONO = T1.VCAACONO
AND T2.STABSN03=T1.VCABSN03 
AND T2.STAAPRNO NOT IN ('53318','5671D','53324','56714','5671B','56717','53323','56716')
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0'
 
where 1=1 
AND substr( T1.VCAACSNO,1,2)  IN ('82','83','91','92','93') 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第2组）==============
-- 公司贷款资产分类形态变更聚合表 数据组2

insert into table ${session}.S03_LP_LOAN_BIZ_QLTY_CLS_TAB(
      LOAN_CONT_NO -- 贷款合同编号
    , DUBIL_NO -- 借据编号
    , 业务术语待申请 -- 贷款形态修改通知书编号
    , LNACCT_NO -- 贷款账户编号
    , CUST_IN_CD -- 客户内码
    , CUST_NO -- 客户号
    , CUST_NAME -- 客户名称
    , CURR_CD -- 币种代码
    , LOANPROD_NO -- 贷款产品编号
    , 业务术语待申请 -- 原十级形态代码
    , 业务术语待申请 -- 新十级形态代码
    , 业务术语待申请 -- 原十级形态转出金额
    , 业务术语待申请 -- 新十级形态转入金额
    , 业务术语待申请 -- 原四级形态代码
    , 业务术语待申请 -- 新四级形态代码
    , 业务术语待申请 -- 原四级形态转出金额
    , 业务术语待申请 -- 新四级形态转入金额
    , 业务术语待申请 -- 经办客户经理柜员编号
    , 业务术语待申请 -- 调整柜员编号
    , 业务术语待申请 -- 审批柜员编号
    , 业务术语待申请 -- 交易来源代码
    , TXN_HAPP_BELONG_ORG_NO -- 交易发生归属机构编号
    , 业务术语待申请 -- 新贷款产品科目代码
    , 业务术语待申请 -- 原贷款产品科目代码
    , 业务术语待申请 -- 贷款四级形态逾期利率
    , FINAL_MODIF_DT -- 最后修改日期
    , PT_DT -- 数据日期
)
select
      T1.FCAACONO as LOAN_CONT_NO -- 贷款合同号
    , T1.FCABSN03 as DUBIL_NO -- 借据序号
    , T1.FCAANO18 as 业务术语待申请 -- 通知书编号
    , T1.FCAACSNO as LNACCT_NO -- 客户内码
    , T1.FCABCSID as CUST_IN_CD -- 客户号
    , T1.FCANFLNM as CUST_NO -- 客户名称
    , T1.FCAAAC15 as CUST_NAME -- 贷款账号
    , T1.FCAACCYC as CURR_CD -- 币种
    , T2.STAAPRNO as LOANPROD_NO -- 贷款产品代码
    , T1.FCACACID as 业务术语待申请 -- 新科目代号
    , T1.FCABACID as 业务术语待申请 -- 原科目代号
    , T1.FCBHRATE as 业务术语待申请 -- 本形态利率
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , '' as 业务术语待申请 -- None
    , T1.FCAGCLS4 as 业务术语待申请 -- 原四级形态
    , T1.FCAHCLS4 as 业务术语待申请 -- 新四级形态
    , T1.FCBUAMT as 业务术语待申请 -- 转入金额
    , T1.FCBVAMT as 业务术语待申请 -- 转出金额
    , T1.FCAHSTAF as 业务术语待申请 -- 经办柜员
    , T1.FCAGSTAF as TXN_HAPP_BELONG_ORG_NO -- 调整柜员
    , T1.FCAISTAF as 业务术语待申请 -- 审批柜员
    , T1.FCAABSSC as 业务术语待申请 -- 交易来源
    , T1.FCAABRNO as 业务术语待申请 -- 机构号
    , T1.FCAVDATE as FINAL_MODIF_DT -- 调整日期
    , '${process_date}' as PT_DT -- None
from
    ${ODS_CORE_SCHEMA}.BLFMRGFC as T1 -- 贷款四级形态调整登记簿
    LEFT JOIN ${ODS_CORE_SCHEMA}.BLFMMAST as T2 -- 普通贷款分户文件
    on T2.STAACONO = T1.FCAACONO
AND T2.STABSN03=T1.FCABSN03 
AND T2.STAAPRNO NOT IN ('53318','5671D','53324','56714','5671B','56717','53323','56716')
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0'
 
where 1=1 
AND substr( T1.FCAACSNO,1,2)  IN ('82','83','91','92','93') 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;

-- 删除所有临时表