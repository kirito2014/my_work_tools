-- 层次表名: 聚合层-简易财务报表资产负债信息聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s03_fin_stat_ast_liab_info_tab
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S03_FIN_STAT_AST_LIAB_INFO_TAB
--     表中文名：简易财务报表资产负债信息聚合表
--     创建日期：2023-01-04 00:00:00
--     主键字段：CUST_IN_CD,REPORT_YM,BELONG_ORG_NO
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含简易报表的企业客户财务报表中资产负债相关信息、贷前企业评级应用字段以及贷后财务检查涉及的资产负债字段。同时包含客户基本信息、归属机构、注册行业币种、信贷员信息等公共字段信息。不包含标准报表企业客户。
--     更新记录：
--         2023-01-04 00:00:00 王穆军 new
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S03_FIN_STAT_AST_LIAB_INFO_TAB drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 简易财务报表资产负债信息聚合表

insert into table ${session}.S03_FIN_STAT_AST_LIAB_INFO_TAB(
      CUST_IN_CD -- 客户内码
    , REPORT_YM -- 报表年月
    , BELONG_ORG_NO -- 归属机构编号
    , REPORT_AUDIT_FLAG -- 报表审计标志
    , REPORT_TYPE_CD -- 报表类型代码
    , CUST_NAME -- 客户名称
    , CUST_NO -- 客户号
    , REG_INDUS_CD -- 注册行业代码
    , REG_CURR_CD -- 注册币种代码
    , TOTAL_AST -- 总资产
    , TOTAL_LIAB -- 总负债
    , OTHER_AST -- 其他资产
    , OTHER_LIAB -- 其他负债
    , OTHER_LIAB_SHORT_TERM_PART_ -- 其他负债(短期部分)
    , ESTATE_LIAB_SHORT_TERM_PART_ -- 房地产负债(短期部分)
    , CAR_LOAN_LIAB_SHORT_TERM_PART_ -- 汽车贷款负债(短期部分)
    , IN_OBANK_LOAN_SHORT_TERM_PART_ -- 在我行的贷款(短期部分)
    , OTHER_BANK_LOAN_SHORT_TERM_PART_ -- 其他银行贷款(短期部分)
    , INVTRY_CASH -- 库存现金
    , BANK_DPSIT -- 银行存款
    , ACCT_RECVBL -- 应收账款
    , ADV_ACCT -- 预付账款
    , RAW_MATRL -- 原材料
    , PROD_AND_INVTRY_PROD -- 在产品及库存产品
    , INVEST_AST -- 投资性资产
    , ESTATE_CLS_AST -- 房产类资产
    , EQUIP_CLS_AST -- 设备类资产
    , VEHIC_CLS_AST -- 交通工具类资产
    , PAYBL_BIL -- 应付票据
    , PAYBL_ACCT -- 应付账款
    , ADV_RECVD_ACCT -- 预收账款
    , ESTATE_LIAB -- 房地产负债
    , CAR_LOAN_LIAB -- 汽车贷款负债
    , IN_OBANK_LOAN -- 在我行的贷款
    , OTHER_BANK_LOAN -- 其他银行贷款
    , PAYBL_TAX -- 应付税款
    , PVT_LOAN -- 民间借款
    , CREATE_TM -- 创建时间
    , CREATE_CUST_MGR_TELR_NO -- 创建客户经理柜员编号
    , CREATE_CUST_MGR_NAME -- 创建客户经理名称
    , CREATE_ORG_NO -- 创建机构编号
    , FINAL_MATN_CUST_MGR_NO -- 最后维护客户经理编号
    , FINAL_MATN_CUST_MGR_NAME -- 最后维护客户经理姓名
    , FINAL_MODIF_TM -- 最后修改时间
    , FINAL_MODIF_ORG_NO -- 最后修改机构编号
    , PT_DT -- 数据日期
)
select
      T1.CUST_ISN as CUST_IN_CD -- 客户内码
    , T1.RPT_DT as REPORT_YM -- 报表年月
    , T1.BEL_TO_ORG as BELONG_ORG_NO -- 归属机构
    , T2.RPT_AUDIT_IND as REPORT_AUDIT_FLAG -- 报表是否审计
    , T1.RPT_DT_TYP as REPORT_TYPE_CD -- 报表类型
    , T3.CUST_NAM as CUST_NAME -- 客户名称
    , T1.CUST_ID as CUST_NO -- 客户号
    , T4.BEL_TO_IDY as REG_INDUS_CD -- 注册行业
    , T5.REG_CCY_ID as REG_CURR_CD -- 注册登记币种
    , T1.TTL_AST as TOTAL_AST -- 总资产
    , T1.TTL_LBT as TOTAL_LIAB -- 总负债
    , T1.OTH_AST as OTHER_AST -- 其他资产
    , T1.OTH_LBT as OTHER_LIAB -- 其他负债
    , T1.OTH_LBT_ST as OTHER_LIAB_SHORT_TERM_PART_ -- 其他负债(短期部分)
    , T1.RLT_LBT_ST as ESTATE_LIAB_SHORT_TERM_PART_ -- 房地产负债(短期部分)
    , T1.CAR_LAN_LBT_ST as CAR_LOAN_LIAB_SHORT_TERM_PART_ -- 汽车贷款负债(短期部分)
    , T1.OUR_BK_LAN_ST as IN_OBANK_LOAN_SHORT_TERM_PART_ -- 在我行的贷款(短期部分)
    , T1.OTH_BK_LAN_ST as OTHER_BANK_LOAN_SHORT_TERM_PART_ -- 其他银行贷款(短期部分)
    , T1.CASH as INVTRY_CASH -- 库存现金
    , T1.CASH_IN_BANK as BANK_DPSIT -- 银行存款
    , T1.ACCT_RCV as ACCT_RECVBL -- 应收账款
    , T1.ADV_MNY as ADV_ACCT -- 预付账款
    , T1.RAW_MTR as RAW_MATRL -- 原材料
    , T1.PRT_AND_STG_PRT as PROD_AND_INVTRY_PROD -- 在产品及库存产品
    , T1.IVTMT_HS_PRT as INVEST_AST -- 投资性资产
    , T1.HS_PRT_AST as ESTATE_CLS_AST -- 房产类资产
    , T1.EQMT_AST as EQUIP_CLS_AST -- 设备类资产
    , T1.VKL_AST as VEHIC_CLS_AST -- 交通工具类资产
    , T1.NOTES_PYB as PAYBL_BIL -- 应付票据
    , T1.ACCT_PYB as PAYBL_ACCT -- 应付账款
    , T1.ADV_FROM_CST as ADV_RECVD_ACCT -- 预收账款
    , T1.RLT_LBT as ESTATE_LIAB -- 房地产负债
    , T1.CAR_LAN_LBT as CAR_LOAN_LIAB -- 汽车贷款负债
    , T1.OUR_BK_LAN as IN_OBANK_LOAN -- 在我行的贷款
    , T1.OTH_BK_LAN as OTHER_BANK_LOAN -- 其他银行贷款
    , T1.TAX_RCV as PAYBL_TAX -- 应付税款
    , T1.FOLK_LOANS as PVT_LOAN -- 民间借款
    , T1.CRT_TM as CREATE_TM -- 创建时间
    , T1.CRT_LOAN_OFF as CREATE_CUST_MGR_TELR_NO -- 创建客户经理
    , T6.NAME as CREATE_CUST_MGR_NAME -- 信贷人员姓名
    , T1.CRT_ORGAN as CREATE_ORG_NO -- 创建机构
    , T1.LAST_LOAN_OFF as FINAL_MATN_CUST_MGR_NO -- 最后维护客户经理
    , T7.NAME as FINAL_MATN_CUST_MGR_NAME -- 信贷人员姓名
    , T1.LAST_TM as FINAL_MODIF_TM -- 最后维护时间
    , T1.LAST_ORGAN as FINAL_MODIF_ORG_NO -- 最后维护机构
    , '${process_date}' as PT_DT -- None
from
    ${ODS_XDZX_SCHEMA}.EN_SM_BAL_SHT as T1 -- 小企业资产负债表
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_FNC_DOC_INFO as T2 -- 财务资料公共信息表
    on T1.CUST_ISN=T2.CUST_ISN
AND T1.RPT_DT=T2.RPT_DT
AND T1.BEL_TO_ORG=T2.BEL_TO_ORG
AND  T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.CREDIT_CUST_BASE_INFO as T3 -- 信贷客户基础信息表
    on T1.CUST_ISN=T2.CUST_ISN
AND   T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_CUST_BASE_INFO as T4 -- 客户公共基础信息表
    on T1.CUST_ISN=T4.CUST_ISN
AND T1.BEL_TO_ORG=T4.BEL_TO_ORG
AND   T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.EN_CUST_EXPD_ECIF as T5 -- 对公客户扩展信息表
    on T1.CUST_ISN=T5.CUST_ISN
AND T1.BEL_TO_ORG=T5.BEL_TO_ORG
AND   T5.PT_DT='${process_date}' 
AND T5.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.USERS as T6 -- 用户表
    on T1.CRT_LOAN_OFF = T6.STAFFER_NO
AND   T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.USERS as T7 -- 用户表
    on T1.LAST_LOAN_OFF = T7.STAFFER_NO
AND   T7.PT_DT='${process_date}' 
AND T7.DELETED='0' 
where T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;

-- 删除所有临时表