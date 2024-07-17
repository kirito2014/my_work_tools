-- 层次表名: 聚合层-简易财务报表利润信息聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s03_corp_fin_simple_stat_prft
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S03_CORP_FIN_SIMPLE_STAT_PRFT
--     表中文名：简易财务报表利润信息聚合表
--     创建日期：2023-12-28 00:00:00
--     主键字段：CUST_INCD,REPORT_YM,FIN_TABLE_TYPE,AFLT_ORG
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含简易报表的企业客户财务报表中利润相关信息、贷前企业评级应用字段以及贷后财务检查涉及的利润字段。同时包含客户基本信息、归属机构、注册行业币种、信贷员信息等公共字段信息。不包含标准报表企业客户。
--     更新记录：
--         2023-12-28 00:00:00 王穆军 新增映射文档
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S03_CORP_FIN_SIMPLE_STAT_PRFT drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 数据组1
drop table if exists ${session}.TMP_S03_CORP_FIN_SIMPLE_STAT_PRFT_01;

create table ${session}.TMP_S03_CORP_FIN_SIMPLE_STAT_PRFT_01 (
      CUST_INCD varchar(11) -- 客户内码
    , REPORT_YM varchar(6) -- 报表年月
    , AFLT_ORG varchar(6) -- 归属机构编号
    , FIN_TABLE_TYPE varchar(1) -- 来源表类型代码
    , IF_AUDIT varchar(1) -- 报表审计标志
    , REPORT_TYPE varchar(1) -- 报表类型代码
    , CUST_NAME varchar(255) -- 客户名称
    , CUST_NO varchar(23) -- 客户号
    , REG_INDUS char(4) -- 注册行业代码
    , REG_CURR varchar(3) -- 注册币种代码
    , TTL_ICM decimal(18,2) -- 总收入
    , TTL_EXP decimal(18,2) -- 总支出
    , ADD_IVTMT_ICM_CHECK decimal(18,2) -- 投资收益
    , LS_ICM_TAX_EXP_CHECK decimal(18,2) -- 所得税
    , ADM_EXP_CHECK decimal(18,2) -- 财务费用
    , SELL_EXP_CHECK decimal(18,2) -- 销售费用
    , WT_RT decimal(18,2) -- 水费
    , ELE_BILL decimal(18,2) -- 电费
    , OUTWARD_INVTMT_ICM decimal(18,2) -- 对外投资收入
    , RTL_ICM decimal(18,2) -- 租金收入
    , ACR_ICM decimal(18,2) -- 利息收入
    , BOS_ICM decimal(18,2) -- 红利收入
    , PTS_ICM decimal(18,2) -- 合伙收入
    , YYW_ICM decimal(18,2) -- 营业外收入
    , OTH_ICM decimal(18,2) -- 其他收入
    , RPM_EXP decimal(18,2) -- 原材料支出
    , EMP_SLS_EXP decimal(18,2) -- 职工薪酬支出
    , FXD_AST_DEP decimal(18,2) -- 固定资产折旧
    , RTEXP_AND_PMF decimal(18,2) -- 租金支出和物业管理费
    , WTH_INR decimal(18,2) -- 财产保险费
    , OTH_FEE decimal(18,2) -- 其他费用
    , CREATE_TM timestamp(26,6) -- 创建时间
    , CREATE_LOAN_OFFICER_NO varchar(7) -- 创建客户经理编号
    , CREATE_LOAN_OFFICER_NAME varchar(8) -- 创建客户经理姓名
    , CREATE_ORG varchar(6) -- 创建机构编号
    , FINAL_MATN_LOAN_OFFICER_NO varchar(7) -- 最后维护客户经理编号
    , FINAL_MATN_LOAN_OFFICER_NAME varchar(8) -- 最后维护客户经理姓名
    , FINAL_MATN_TM timestamp(26,6) -- 最后维护时间
    , FINAL_MATN_ORG varchar(6) -- 最后维护机构编号
    , PT_DT varchar(10) -- 数据日期
)
comment '数据组1'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_CORP_FIN_SIMPLE_STAT_PRFT_01(
      CUST_INCD -- 客户内码
    , REPORT_YM -- 报表年月
    , AFLT_ORG -- 归属机构编号
    , FIN_TABLE_TYPE -- 来源表类型代码
    , IF_AUDIT -- 报表审计标志
    , REPORT_TYPE -- 报表类型代码
    , CUST_NAME -- 客户名称
    , CUST_NO -- 客户号
    , REG_INDUS -- 注册行业代码
    , REG_CURR -- 注册币种代码
    , TTL_ICM -- 总收入
    , TTL_EXP -- 总支出
    , ADD_IVTMT_ICM_CHECK -- 投资收益
    , LS_ICM_TAX_EXP_CHECK -- 所得税
    , ADM_EXP_CHECK -- 财务费用
    , SELL_EXP_CHECK -- 销售费用
    , WT_RT -- 水费
    , ELE_BILL -- 电费
    , OUTWARD_INVTMT_ICM -- 对外投资收入
    , RTL_ICM -- 租金收入
    , ACR_ICM -- 利息收入
    , BOS_ICM -- 红利收入
    , PTS_ICM -- 合伙收入
    , YYW_ICM -- 营业外收入
    , OTH_ICM -- 其他收入
    , RPM_EXP -- 原材料支出
    , EMP_SLS_EXP -- 职工薪酬支出
    , FXD_AST_DEP -- 固定资产折旧
    , RTEXP_AND_PMF -- 租金支出和物业管理费
    , WTH_INR -- 财产保险费
    , OTH_FEE -- 其他费用
    , CREATE_TM -- 创建时间
    , CREATE_LOAN_OFFICER_NO -- 创建客户经理编号
    , CREATE_LOAN_OFFICER_NAME -- 创建客户经理姓名
    , CREATE_ORG -- 创建机构编号
    , FINAL_MATN_LOAN_OFFICER_NO -- 最后维护客户经理编号
    , FINAL_MATN_LOAN_OFFICER_NAME -- 最后维护客户经理姓名
    , FINAL_MATN_TM -- 最后维护时间
    , FINAL_MATN_ORG -- 最后维护机构编号
    , PT_DT -- 数据日期
)
select
      T1.CUST_ISN as CUST_INCD -- 客户内码
    , T1.RPT_DT as REPORT_YM -- 报表年月
    , T1.BEL_TO_ORG as AFLT_ORG -- 归属机构
    , '1' as FIN_TABLE_TYPE -- None
    , T2.RPT_AUDIT_IND as IF_AUDIT -- 报表是否审计、客户内码、报表年月、归属机构
    , T1.RPT_DT_TYP as REPORT_TYPE -- 报表类型,年报，季报，月报
    , T3.CUST_NAM as CUST_NAME -- 客户名称、客户内码
    , T1.CUST_ID as CUST_NO -- 客户号
    , T4.BEL_TO_IDY as REG_INDUS -- 注册行业、客户内码、归属机构
    , T5.REG_CCY_ID as REG_CURR -- 注册登记币种、客户内码、归属机构
    , T1.TTL_ICM as TTL_ICM -- 总收入
    , T1.TTL_EXP as TTL_EXP -- 总支出
    , T1.IVT_ICM as ADD_IVTMT_ICM_CHECK -- 投资收益
    , T1.ICM_TAX_OTH_TAX as LS_ICM_TAX_EXP_CHECK -- 所得税和其他税负
    , T1.FNC_EXP as ADM_EXP_CHECK -- 财务费用支出
    , T1.SELL_EXP as SELL_EXP_CHECK -- 销售费用支出
    , T1.WT_RT as WT_RT -- 其中：水费
    , T1.ELE_BILL as ELE_BILL -- 电费
    , T1.OUTWARD_INVTMT_ICM as OUTWARD_INVTMT_ICM -- 对外投资收入
    , T1.RTL_ICM as RTL_ICM -- 租金收入
    , T1.ACR_ICM as ACR_ICM -- 利息收入
    , T1.BOS_ICM as BOS_ICM -- 红利收入
    , T1.PTS_ICM as PTS_ICM -- 合伙收入
    , T1.YYW_ICM as YYW_ICM -- 营业外收入
    , T1.OTH_ICM as OTH_ICM -- 其他收入
    , T1.RPM_EXP as RPM_EXP -- 原材料支出
    , T1.EMP_SLS_EXP as EMP_SLS_EXP -- 职工薪酬支出
    , T1.FXD_AST_DEP as FXD_AST_DEP -- 固定资产折旧
    , T1.RTEXP_AND_PMF as RTEXP_AND_PMF -- 租金支出和物业管理费
    , T1.WTH_INR as WTH_INR -- 财产保险费
    , T1.OTH_FEE as OTH_FEE -- 其他费用
    , T1.CRT_TM as CREATE_TM -- 创建时间
    , T1.CRT_LOAN_OFF as CREATE_LOAN_OFFICER_NO -- 创建客户经理
    , T6.NAME as CREATE_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.CRT_ORGAN as CREATE_ORG -- 创建机构
    , T1.LAST_LOAN_OFF as FINAL_MATN_LOAN_OFFICER_NO -- 最后维护客户经理
    , T7.NAME as FINAL_MATN_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.LAST_TM as FINAL_MATN_TM -- 最后维护时间
    , T1.LAST_ORGAN as FINAL_MATN_ORG -- 最后维护机构
    , '${PROCESS_DATE}' as PT_DT -- None
from
    ${ODS_XDZX_SCHEMA}.EN_SM_ICM_EXP as T1 -- 小企业收入支出表
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
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第2组）==============
-- 数据组2
drop table if exists ${session}.TMP_S03_CORP_FIN_SIMPLE_STAT_PRFT_02;

create table ${session}.TMP_S03_CORP_FIN_SIMPLE_STAT_PRFT_02 (
      CUST_INCD varchar(11) -- 客户内码
    , REPORT_YM varchar(6) -- 报表年月
    , AFLT_ORG varchar(6) -- 归属机构编号
    , FIN_TABLE_TYPE varchar(1) -- 来源表类型代码
    , IF_AUDIT varchar(1) -- 报表审计标志
    , REPORT_TYPE varchar(1) -- 报表类型代码
    , CUST_NAME varchar(255) -- 客户名称
    , CUST_NO varchar(23) -- 客户号
    , REG_INDUS char(4) -- 注册行业代码
    , REG_CURR varchar(3) -- 注册币种代码
    , TTL_ICM decimal(18,2) -- 总收入
    , TTL_EXP decimal(18,2) -- 总支出
    , ADD_IVTMT_ICM_CHECK decimal(18,2) -- 投资收益
    , LS_ICM_TAX_EXP_CHECK decimal(18,2) -- 所得税
    , ADM_EXP_CHECK decimal(18,2) -- 财务费用
    , SELL_EXP_CHECK decimal(18,2) -- 销售费用
    , WT_RT decimal(18,2) -- 水费
    , ELE_BILL decimal(18,2) -- 电费
    , OUTWARD_INVTMT_ICM decimal(18,2) -- 对外投资收入
    , RTL_ICM decimal(18,2) -- 租金收入
    , ACR_ICM decimal(18,2) -- 利息收入
    , BOS_ICM decimal(18,2) -- 红利收入
    , PTS_ICM decimal(18,2) -- 合伙收入
    , YYW_ICM decimal(18,2) -- 营业外收入
    , OTH_ICM decimal(18,2) -- 其他收入
    , RPM_EXP decimal(18,2) -- 原材料支出
    , EMP_SLS_EXP decimal(18,2) -- 职工薪酬支出
    , FXD_AST_DEP decimal(18,2) -- 固定资产折旧
    , RTEXP_AND_PMF decimal(18,2) -- 租金支出和物业管理费
    , WTH_INR decimal(18,2) -- 财产保险费
    , OTH_FEE decimal(18,2) -- 其他费用
    , CREATE_TM timestamp(26,6) -- 创建时间
    , CREATE_LOAN_OFFICER_NO varchar(7) -- 创建客户经理编号
    , CREATE_LOAN_OFFICER_NAME varchar(8) -- 创建客户经理姓名
    , CREATE_ORG varchar(6) -- 创建机构编号
    , FINAL_MATN_LOAN_OFFICER_NO varchar(7) -- 最后维护客户经理编号
    , FINAL_MATN_LOAN_OFFICER_NAME varchar(8) -- 最后维护客户经理姓名
    , FINAL_MATN_TM timestamp(26,6) -- 最后维护时间
    , FINAL_MATN_ORG varchar(6) -- 最后维护机构编号
    , PT_DT varchar(10) -- 数据日期
)
comment '数据组2'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_CORP_FIN_SIMPLE_STAT_PRFT_02(
      CUST_INCD -- 客户内码
    , REPORT_YM -- 报表年月
    , AFLT_ORG -- 归属机构编号
    , FIN_TABLE_TYPE -- 来源表类型代码
    , IF_AUDIT -- 报表审计标志
    , REPORT_TYPE -- 报表类型代码
    , CUST_NAME -- 客户名称
    , CUST_NO -- 客户号
    , REG_INDUS -- 注册行业代码
    , REG_CURR -- 注册币种代码
    , TTL_ICM -- 总收入
    , TTL_EXP -- 总支出
    , ADD_IVTMT_ICM_CHECK -- 投资收益
    , LS_ICM_TAX_EXP_CHECK -- 所得税
    , ADM_EXP_CHECK -- 财务费用
    , SELL_EXP_CHECK -- 销售费用
    , WT_RT -- 水费
    , ELE_BILL -- 电费
    , OUTWARD_INVTMT_ICM -- 对外投资收入
    , RTL_ICM -- 租金收入
    , ACR_ICM -- 利息收入
    , BOS_ICM -- 红利收入
    , PTS_ICM -- 合伙收入
    , YYW_ICM -- 营业外收入
    , OTH_ICM -- 其他收入
    , RPM_EXP -- 原材料支出
    , EMP_SLS_EXP -- 职工薪酬支出
    , FXD_AST_DEP -- 固定资产折旧
    , RTEXP_AND_PMF -- 租金支出和物业管理费
    , WTH_INR -- 财产保险费
    , OTH_FEE -- 其他费用
    , CREATE_TM -- 创建时间
    , CREATE_LOAN_OFFICER_NO -- 创建客户经理编号
    , CREATE_LOAN_OFFICER_NAME -- 创建客户经理姓名
    , CREATE_ORG -- 创建机构编号
    , FINAL_MATN_LOAN_OFFICER_NO -- 最后维护客户经理编号
    , FINAL_MATN_LOAN_OFFICER_NAME -- 最后维护客户经理姓名
    , FINAL_MATN_TM -- 最后维护时间
    , FINAL_MATN_ORG -- 最后维护机构编号
    , PT_DT -- 数据日期
)
select
      T1.CUST_ISN as CUST_INCD -- 客户内码
    , T1.RPT_DT as REPORT_YM -- 报表年月
    , T1.BEL_TO_ORG as AFLT_ORG -- 归属机构
    , '2' as FIN_TABLE_TYPE -- None
    , T2.RPT_AUDIT_IND as IF_AUDIT -- 报表是否审计、客户内码、报表年月、归属机构
    , T1.RPT_DT_TYP as REPORT_TYPE -- 报表类型,年报，季报，月报
    , T3.CUST_NAM as CUST_NAME -- 客户名称、客户内码
    , T1.CUST_ID as CUST_NO -- 客户号
    , T4.BEL_TO_IDY as REG_INDUS -- 注册行业、客户内码、归属机构
    , T5.REG_CCY_ID as REG_CURR -- 注册登记币种、客户内码、归属机构
    , T1.TTL_ICM as TTL_ICM -- 总收入
    , T1.TTL_EXP as TTL_EXP -- 总支出
    , T1.IVT_ICM as ADD_IVTMT_ICM_CHECK -- 投资收益
    , T1.ICM_TAX_OTH_TAX as LS_ICM_TAX_EXP_CHECK -- 所得税和其他税负
    , T1.FNC_EXP as ADM_EXP_CHECK -- 财务费用支出
    , T1.SELL_EXP as SELL_EXP_CHECK -- 销售费用支出
    , T1.WT_RT as WT_RT -- 其中：水费
    , T1.ELE_BILL as ELE_BILL -- 电费
    , T1.OUTWARD_INVTMT_ICM as OUTWARD_INVTMT_ICM -- 对外投资收入
    , T1.RTL_ICM as RTL_ICM -- 租金收入
    , T1.ACR_ICM as ACR_ICM -- 利息收入
    , T1.BOS_ICM as BOS_ICM -- 红利收入
    , T1.PTS_ICM as PTS_ICM -- 合伙收入
    , T1.YYW_ICM as YYW_ICM -- 营业外收入
    , T1.OTH_ICM as OTH_ICM -- 其他收入
    , T1.RPM_EXP as RPM_EXP -- 原材料支出
    , T1.EMP_SLS_EXP as EMP_SLS_EXP -- 职工薪酬支出
    , T1.FXD_AST_DEP as FXD_AST_DEP -- 固定资产折旧
    , T1.RTEXP_AND_PMF as RTEXP_AND_PMF -- 租金支出和物业管理费
    , T1.WTH_INR as WTH_INR -- 财产保险费
    , T1.OTH_FEE as OTH_FEE -- 其他费用
    , T1.CRT_TM as CREATE_TM -- 创建时间
    , T1.CRT_LOAN_OFF as CREATE_LOAN_OFFICER_NO -- 创建客户经理
    , T6.NAME as CREATE_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.CRT_ORGAN as CREATE_ORG -- 创建机构
    , T1.LAST_LOAN_OFF as FINAL_MATN_LOAN_OFFICER_NO -- 最后维护客户经理
    , T7.NAME as FINAL_MATN_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.LAST_TM as FINAL_MATN_TM -- 最后维护时间
    , T1.LAST_ORGAN as FINAL_MATN_ORG -- 最后维护机构
    , '${PROCESS_DATE}' as PT_DT -- None
from
    ${ODS_XDZX_SCHEMA}.SC_EN_SM_ICM_EXP as T1 -- 小企业收入支出表
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
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第3组）==============
-- 数据汇总

insert into table ${session}.S03_CORP_FIN_SIMPLE_STAT_PRFT(
      CUST_INCD -- 客户内码
    , REPORT_YM -- 报表年月
    , AFLT_ORG -- 归属机构编号
    , FIN_TABLE_TYPE -- 来源表类型代码
    , IF_AUDIT -- 报表审计标志
    , REPORT_TYPE -- 报表类型代码
    , CUST_NAME -- 客户名称
    , CUST_NO -- 客户号
    , REG_INDUS -- 注册行业代码
    , REG_CURR -- 注册币种代码
    , TTL_ICM -- 总收入
    , TTL_EXP -- 总支出
    , ADD_IVTMT_ICM_CHECK -- 投资收益
    , LS_ICM_TAX_EXP_CHECK -- 所得税
    , ADM_EXP_CHECK -- 财务费用
    , SELL_EXP_CHECK -- 销售费用
    , WT_RT -- 水费
    , ELE_BILL -- 电费
    , OUTWARD_INVTMT_ICM -- 对外投资收入
    , RTL_ICM -- 租金收入
    , ACR_ICM -- 利息收入
    , BOS_ICM -- 红利收入
    , PTS_ICM -- 合伙收入
    , YYW_ICM -- 营业外收入
    , OTH_ICM -- 其他收入
    , RPM_EXP -- 原材料支出
    , EMP_SLS_EXP -- 职工薪酬支出
    , FXD_AST_DEP -- 固定资产折旧
    , RTEXP_AND_PMF -- 租金支出和物业管理费
    , WTH_INR -- 财产保险费
    , OTH_FEE -- 其他费用
    , CREATE_TM -- 创建时间
    , CREATE_LOAN_OFFICER_NO -- 创建客户经理编号
    , CREATE_LOAN_OFFICER_NAME -- 创建客户经理姓名
    , CREATE_ORG -- 创建机构编号
    , FINAL_MATN_LOAN_OFFICER_NO -- 最后维护客户经理编号
    , FINAL_MATN_LOAN_OFFICER_NAME -- 最后维护客户经理姓名
    , FINAL_MATN_TM -- 最后维护时间
    , FINAL_MATN_ORG -- 最后维护机构编号
    , PT_DT -- 数据日期
)
select
      T1.CUST_INCD as CUST_INCD -- 客户内码
    , T1.REPORT_YM as REPORT_YM -- 报表年月
    , T1.AFLT_ORG as AFLT_ORG -- 归属机构编号
    , T1.FIN_TABLE_TYPE as FIN_TABLE_TYPE -- 来源表类型代码
    , T1.IF_AUDIT as IF_AUDIT -- 报表审计标志
    , T1.REPORT_TYPE as REPORT_TYPE -- 报表类型代码
    , T1.CUST_NAME as CUST_NAME -- 客户名称
    , T1.CUST_NO as CUST_NO -- 客户号
    , T1.REG_INDUS as REG_INDUS -- 注册行业代码
    , T1.REG_CURR as REG_CURR -- 注册币种代码
    , T1.TTL_ICM as TTL_ICM -- 总收入
    , T1.TTL_EXP as TTL_EXP -- 总支出
    , T1.ADD_IVTMT_ICM_CHECK as ADD_IVTMT_ICM_CHECK -- 投资收益
    , T1.LS_ICM_TAX_EXP_CHECK as LS_ICM_TAX_EXP_CHECK -- 所得税
    , T1.ADM_EXP_CHECK as ADM_EXP_CHECK -- 财务费用
    , T1.SELL_EXP_CHECK as SELL_EXP_CHECK -- 销售费用
    , T1.WT_RT as WT_RT -- 水费
    , T1.ELE_BILL as ELE_BILL -- 电费
    , T1.OUTWARD_INVTMT_ICM as OUTWARD_INVTMT_ICM -- 对外投资收入
    , T1.RTL_ICM as RTL_ICM -- 租金收入
    , T1.ACR_ICM as ACR_ICM -- 利息收入
    , T1.BOS_ICM as BOS_ICM -- 红利收入
    , T1.PTS_ICM as PTS_ICM -- 合伙收入
    , T1.YYW_ICM as YYW_ICM -- 营业外收入
    , T1.OTH_ICM as OTH_ICM -- 其他收入
    , T1.RPM_EXP as RPM_EXP -- 原材料支出
    , T1.EMP_SLS_EXP as EMP_SLS_EXP -- 职工薪酬支出
    , T1.FXD_AST_DEP as FXD_AST_DEP -- 固定资产折旧
    , T1.RTEXP_AND_PMF as RTEXP_AND_PMF -- 租金支出和物业管理费
    , T1.WTH_INR as WTH_INR -- 财产保险费
    , T1.OTH_FEE as OTH_FEE -- 其他费用
    , T1.CREATE_TM as CREATE_TM -- 创建时间
    , T1.CREATE_LOAN_OFFICER_NO as CREATE_LOAN_OFFICER_NO -- 创建客户经理编号
    , T1.CREATE_LOAN_OFFICER_NAME as CREATE_LOAN_OFFICER_NAME -- 创建客户经理姓名
    , T1.CREATE_ORG as CREATE_ORG -- 创建机构编号
    , T1.FINAL_MATN_LOAN_OFFICER_NO as FINAL_MATN_LOAN_OFFICER_NO -- 最后维护客户经理编号
    , T1.FINAL_MATN_LOAN_OFFICER_NAME as FINAL_MATN_LOAN_OFFICER_NAME -- 最后维护客户经理姓名
    , T1.FINAL_MATN_TM as FINAL_MATN_TM -- 最后维护时间
    , T1.FINAL_MATN_ORG as FINAL_MATN_ORG -- 最后维护机构编号
    , T1.PT_DT as PT_DT -- 数据日期
from
    (
SELECT
	P1.CUST_INCD
	P1.REPORT_YM
	P1.AFLT_ORG
	P1.FIN_TABLE_TYPE
	P1.IF_AUDIT
	P1.REPORT_TYPE
	P1.CUST_NAME
	P1.CUST_NO
	P1.REG_INDUS
	P1.REG_CURR
	P1.TTL_ICM
	P1.TTL_EXP
	P1.ADD_IVTMT_ICM_CHECK
	P1.LS_ICM_TAX_EXP_CHECK
	P1.ADM_EXP_CHECK
	P1.SELL_EXP_CHECK
	P1.WT_RT
	P1.ELE_BILL
	P1.OUTWARD_INVTMT_ICM
	P1.RTL_ICM
	P1.ACR_ICM
	P1.BOS_ICM
	P1.PTS_ICM
	P1.YYW_ICM
	P1.OTH_ICM
	P1.RPM_EXP
	P1.EMP_SLS_EXP
	P1.FXD_AST_DEP
	P1.RTEXP_AND_PMF
	P1.WTH_INR
	P1.OTH_FEE
	P1.CREATE_TM
	P1.CREATE_LOAN_OFFICER_NO
	P1.CREATE_LOAN_OFFICER_NAME
	P1.CREATE_ORG
	P1.FINAL_MATN_LOAN_OFFICER_NO
	P1.FINAL_MATN_LOAN_OFFICER_NAME
	P1.FINAL_MATN_TM
	P1.FINAL_MATN_ORG
	P1.PT_DT
FROM TMP_S03_CORP_FIN_SIMPLE_STAT_PRFT_01 P1 
UNION ALL 
SELECT
	P2.CUST_INCD
	P2.REPORT_YM
	P2.AFLT_ORG
	P2.FIN_TABLE_TYPE
	P2.IF_AUDIT
	P2.REPORT_TYPE
	P2.CUST_NAME
	P2.CUST_NO
	P2.REG_INDUS
	P2.REG_CURR
	P2.TTL_ICM
	P2.TTL_EXP
	P2.ADD_IVTMT_ICM_CHECK
	P2.LS_ICM_TAX_EXP_CHECK
	P2.ADM_EXP_CHECK
	P2.SELL_EXP_CHECK
	P2.WT_RT
	P2.ELE_BILL
	P2.OUTWARD_INVTMT_ICM
	P2.RTL_ICM
	P2.ACR_ICM
	P2.BOS_ICM
	P2.PTS_ICM
	P2.YYW_ICM
	P2.OTH_ICM
	P2.RPM_EXP
	P2.EMP_SLS_EXP
	P2.FXD_AST_DEP
	P2.RTEXP_AND_PMF
	P2.WTH_INR
	P2.OTH_FEE
	P2.CREATE_TM
	P2.CREATE_LOAN_OFFICER_NO
	P2.CREATE_LOAN_OFFICER_NAME
	P2.CREATE_ORG
	P2.FINAL_MATN_LOAN_OFFICER_NO
	P2.FINAL_MATN_LOAN_OFFICER_NAME
	P2.FINAL_MATN_TM
	P2.FINAL_MATN_ORG
	P2.PT_DT
FROM TMP_S03_CORP_FIN_SIMPLE_STAT_PRFT_02 P2 
) as T1 -- None
;

-- 删除所有临时表
drop table ${session}.TMP_S03_CORP_FIN_SIMPLE_STAT_PRFT_01;
drop table ${session}.TMP_S03_CORP_FIN_SIMPLE_STAT_PRFT_02;