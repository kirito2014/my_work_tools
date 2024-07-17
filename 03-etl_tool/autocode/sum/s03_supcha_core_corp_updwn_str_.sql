-- 层次表名: 聚合层-供应链核心企业上下游客户信息聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s03_supcha_core_corp_updwn_str_cust_info
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S03_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO
--     表中文名：供应链核心企业上下游客户信息聚合表
--     创建日期：2023-01-03 00:00:00
--     主键字段：SUPCHA_CORE_CORP_NO,SUPCHA_SUB_CUST_NO
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：提供供应链相关产品（订单E融、应收E融、政采E融、保理E融、链贷通、应付E融、仓单E融、链汇E融）中核心企业及其上下游企业的基本信息。
--     更新记录：
--         2023-01-03 00:00:00 王穆军 new
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S03_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 供应链核心企业上下游客户信息临时表1
drop table if exists ${session}.TMP_S03_CUST_INFO_01;

create table ${session}.TMP_S03_CUST_INFO_01 (
      CUST_INCOD varchar(30) -- 客户内码
    , CUST_NO varchar(30) -- 客户编号
    , REG_CAP decimal(23,2) -- 注册资金
    , REG_INDS varchar(30) -- 注册行业
    , OPER_SCOPE varchar(200) -- 经营范围
    , ORG_SCALE varchar(1) -- 企业规模
    , ORG_CRT_DT date -- 成立日期
    , OWNER_FLG varchar(1) -- 经营场地所有权
    , REG_DISTRICT_TYP varchar(1) -- 注册区域类型
    , BUSINESS_STATUS varchar(1) -- 经营状态
    , OPERATING_RECEIPT decimal(18,2) -- 营业收入
    , ASSETS_GENERAL decimal(18,2) -- 资产总额
)
comment '供应链核心企业上下游客户信息临时表1'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_CUST_INFO_01(
      CUST_INCOD -- 客户内码
    , CUST_NO -- 客户编号
    , REG_CAP -- 注册资金
    , REG_INDS -- 注册行业
    , OPER_SCOPE -- 经营范围
    , ORG_SCALE -- 企业规模
    , ORG_CRT_DT -- 成立日期
    , OWNER_FLG -- 经营场地所有权
    , REG_DISTRICT_TYP -- 注册区域类型
    , BUSINESS_STATUS -- 经营状态
    , OPERATING_RECEIPT -- 营业收入
    , ASSETS_GENERAL -- 资产总额
)
select
      T1.CUST_INCOD as CUST_INCOD -- 客户内码
    , T1.CUST_NO as CUST_NO -- 客户编号
    , T1.REG_CAP as REG_CAP -- 注册资金
    , T1.REG_INDS as REG_INDS -- 注册行业
    , T1.OPER_SCOPE as OPER_SCOPE -- 经营范围
    , T2.ORG_SCALE as ORG_SCALE -- 企业规模
    , T2.ORG_CRT_DT as ORG_CRT_DT -- 成立日期
    , T2.OWNER_FLG as OWNER_FLG -- 经营场地所有权
    , T2.REG_DISTRICT_TYP as REG_DISTRICT_TYP -- 注册区域类型
    , T2.BUSINESS_STATUS as BUSINESS_STATUS -- 经营状态
    , T2.OPERATING_RECEIPT as OPERATING_RECEIPT -- 营业收入
    , T2.ASSETS_GENERAL as ASSETS_GENERAL -- 资产总额
from
    LEFT JOIN ${ODS_ECIF_SCHEMA}.T2_EN_CUST_BASE_INFO as T1 -- 对公客户基本信息
    LEFT JOIN (
SELECT 
	 P2.CUST_INCOD		   --客户内码
	,P2.ORG_SCALE          --企业规模
	,P2.ORG_CRT_DT         --成立日期
	,P2.OWNER_FLG          --经营场地所有权
	,P2.REG_DISTRICT_TYP   --注册区域类型
	,P2.BUSINESS_STATUS    --经营状态
	,P2.OPERATING_RECEIPT  --营业收入
	,P2.ASSETS_GENERAL     --资产总额
	,P2.BK_ID			--行社机构编号
	,ROW_NUMBER()OVER(PARTITION BY P2.CUST_INCOD ORDER BY P2.UPDATED_TIME DESC ) RN 
FROM ${ODS_ECIF_SCHEMA}.T2_EN_CUST_DETAIL_INFO P2 --对公客户详细信息	
WHERE P2.PT_DT='${process_date}' 
	AND P2.DELETED='0'
) as T2 -- 对公客户详细信息
    on T1.CUST_INCOD = T2.CUST_INCOD 
AND T1.BK_ID = T2.BK_ID
AND T2.RN = 1 
where T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第2组）==============
-- 供应链核心企业上下游客户信息聚合表

insert into table ${session}.S03_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO(
      SUPCHA_CORE_CORP_NO -- 供应链核心企业编号
    , SUPCHA_SUB_CUST_NO -- 供应链子客户编号
    , SUB_CUST_DOCTYP_CD -- 子客户证件类型代码
    , SUB_CUST_DOC_NO -- 子客户证件号
    , SUB_CUST_BELONG_ORG_NO -- 子客户归属机构编号
    , SUPCHA_CUST_RELA_CATE_CD -- 供应链客户关系种类代码
    , CORE_CORP_NAME -- 核心企业名称
    , CORE_CORP_DOCTYP_CD -- 核心企业证件类型代码
    , CORE_CORP_DOC_NO -- 核心企业证件号
    , SUB_CUST_NAME -- 子客户名称
    , SUB_CUST_PRE_CRDT_LINE -- 子客户预授信额度
    , SUB_CUST_CRDT_TERM -- 子客户授信期限
    , COMP_INT_RATE_ADD_SUB_BASE_POINT -- 较LPR利率加减基点
    , SUB_CUST_WHITE_LIST_VALID -- 子客户白名单有效期
    , SUB_CUST_REG_CAP -- 子客户注册资金
    , SUB_CUST_INDS_CD -- 子客户所属行业代码
    , SUB_CUST_OPER_SCOPE -- 子客户经营范围
    , SUB_CUST_CORP_SCALE_CD -- 子客户企业规模代码
    , SUB_CUST_FOUND_DT -- 子客户成立日期
    , SUB_CUST_OPER_PLACE_OWNSHIP_CD -- 子客户经营场地所有权代码
    , SUB_CUST_REG_RGN_TYPE_CD -- 子客户注册区域类型代码
    , SUB_CUST_OPER_STATUS_CD -- 子客户经营状态代码
    , SUB_CUST_OPEN_INCOME -- 子客户营业收入
    , SUB_CUST_TOTAL_AST -- 子客户资产总额
    , SUB_CUST_BELONG_CUST_MGR_NO -- 子客户归属客户经理编号
    , SETUP_TELR_NO -- 建立柜员编号
    , SETUP_DT -- 建立日期
    , FINAL_MODIF_TELR_NO -- 最后修改柜员编号
    , FINAL_MODIF_DT -- 最后修改日期
    , DATA_DT -- 数据日期
)
select
      t1.ptno as SUPCHA_CORE_CORP_NO -- 平台码
    , t1.id as SUPCHA_SUB_CUST_NO -- 主键
    , t1.id_type as SUB_CUST_DOCTYP_CD -- 证件类型
    , t1.id_no as SUB_CUST_DOC_NO -- 证件号码
    , t1.brcode as SUB_CUST_BELONG_ORG_NO -- 机构号
    , '1' as SUPCHA_CUST_RELA_CATE_CD -- None
    , t2.mername as CORE_CORP_NAME -- 企业名称
    , t2.id_type as CORE_CORP_DOCTYP_CD -- 证件类型
    , t2.id_no as CORE_CORP_DOC_NO -- 证件号码
    , t1.cust_name as SUB_CUST_NAME -- 客户名称
    , t1.credit_line as SUB_CUST_PRE_CRDT_LINE -- 预授信额度
    , t1.credit_term as SUB_CUST_CRDT_TERM -- 授信期限
    , t1.float_rate as COMP_INT_RATE_ADD_SUB_BASE_POINT -- 较LPR利率加减基点
    , t1.whitelist_vali_period as SUB_CUST_WHITE_LIST_VALID -- 白名单有效期
    , T3.REG_CAP as SUB_CUST_REG_CAP -- 注册资金
    , T3.REG_INDS as SUB_CUST_INDS_CD -- 注册行业
    , T3.OPER_SCOPE as SUB_CUST_OPER_SCOPE -- 经营范围
    , T3.ORG_SCALE as SUB_CUST_CORP_SCALE_CD -- 企业规模
    , T3.ORG_CRT_DT as SUB_CUST_FOUND_DT -- 成立日期
    , T3.OWNER_FLG as SUB_CUST_OPER_PLACE_OWNSHIP_CD -- 经营场地所有权
    , T3.REG_DISTRICT_TYP as SUB_CUST_REG_RGN_TYPE_CD -- 注册区域类型
    , T3.BUSINESS_STATUS as SUB_CUST_OPER_STATUS_CD -- 经营状态
    , T3.OPERATING_RECEIPT as SUB_CUST_OPEN_INCOME -- 营业收入
    , T3.ASSETS_GENERAL as SUB_CUST_TOTAL_AST -- 资产总额
    , t1.belong_mgrno as SUB_CUST_BELONG_CUST_MGR_NO -- 归属客户经理
    , t1.create_unid as SETUP_TELR_NO -- 创建机构
    , t1.create_date as SETUP_DT -- 创建时间
    , t1.checked_unid as FINAL_MODIF_TELR_NO -- 复核机构号
    , t1.checked_date as FINAL_MODIF_DT -- 复核日期
    , '${process_date}' as DATA_DT -- None
from
    ${ODS_SCB_SCHEMA}.tbl_credit_cust_order_loan as T1 -- 链贷通授信白名单信息表
    LEFT JOIN ${ODS_SCB_SCHEMA}.tbl_merchant_info as T2 -- 第三方平台信息表
    on T1.ptno = T2.ptno
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN TMP_S03_CUST_INFO_01 as T3 -- None
    on CONCAT(TRIM(T1.id_type), TRIM(T1.id_no)) = TRIM(T3.CUST_NO) 
where T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第3组）==============
-- 供应链核心企业上下游客户信息聚合表

insert into table ${session}.S03_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO(
      SUPCHA_CORE_CORP_NO -- 供应链核心企业编号
    , SUPCHA_SUB_CUST_NO -- 供应链子客户编号
    , SUB_CUST_DOCTYP_CD -- 子客户证件类型代码
    , SUB_CUST_DOC_NO -- 子客户证件号
    , SUB_CUST_BELONG_ORG_NO -- 子客户归属机构编号
    , SUPCHA_CUST_RELA_CATE_CD -- 供应链客户关系种类代码
    , CORE_CORP_NAME -- 核心企业名称
    , CORE_CORP_DOCTYP_CD -- 核心企业证件类型代码
    , CORE_CORP_DOC_NO -- 核心企业证件号
    , SUB_CUST_NAME -- 子客户名称
    , SUB_CUST_PRE_CRDT_LINE -- 子客户预授信额度
    , SUB_CUST_CRDT_TERM -- 子客户授信期限
    , COMP_INT_RATE_ADD_SUB_BASE_POINT -- 较LPR利率加减基点
    , SUB_CUST_WHITE_LIST_VALID -- 子客户白名单有效期
    , SUB_CUST_REG_CAP -- 子客户注册资金
    , SUB_CUST_INDS_CD -- 子客户所属行业代码
    , SUB_CUST_OPER_SCOPE -- 子客户经营范围
    , SUB_CUST_CORP_SCALE_CD -- 子客户企业规模代码
    , SUB_CUST_FOUND_DT -- 子客户成立日期
    , SUB_CUST_OPER_PLACE_OWNSHIP_CD -- 子客户经营场地所有权代码
    , SUB_CUST_REG_RGN_TYPE_CD -- 子客户注册区域类型代码
    , SUB_CUST_OPER_STATUS_CD -- 子客户经营状态代码
    , SUB_CUST_OPEN_INCOME -- 子客户营业收入
    , SUB_CUST_TOTAL_AST -- 子客户资产总额
    , SUB_CUST_BELONG_CUST_MGR_NO -- 子客户归属客户经理编号
    , SETUP_TELR_NO -- 建立柜员编号
    , SETUP_DT -- 建立日期
    , FINAL_MODIF_TELR_NO -- 最后修改柜员编号
    , FINAL_MODIF_DT -- 最后修改日期
    , DATA_DT -- 数据日期
)
select
      t1.ptno as SUPCHA_CORE_CORP_NO -- 平台码
    , t1.id as SUPCHA_SUB_CUST_NO -- 主键
    , t1.id_type as SUB_CUST_DOCTYP_CD -- 证件类型
    , t1.id_no as SUB_CUST_DOC_NO -- 证件号码
    , t1.brcode as SUB_CUST_BELONG_ORG_NO -- 机构号
    , '1' as SUPCHA_CUST_RELA_CATE_CD -- None
    , t2.mername as CORE_CORP_NAME -- 企业名称
    , t2.id_type as CORE_CORP_DOCTYP_CD -- 证件类型
    , t2.id_no as CORE_CORP_DOC_NO -- 证件号码
    , t1.cust_name as SUB_CUST_NAME -- 客户名称
    , t1.credit_line as SUB_CUST_PRE_CRDT_LINE -- 预授信额度
    , t1.credit_term as SUB_CUST_CRDT_TERM -- 授信期限
    , t1.float_rate as COMP_INT_RATE_ADD_SUB_BASE_POINT -- 较LPR利率加减基点
    , t1.whitelist_vali_period as SUB_CUST_WHITE_LIST_VALID -- 白名单有效期
    , T3.REG_CAP as SUB_CUST_REG_CAP -- 注册资金
    , T3.REG_INDS as SUB_CUST_INDS_CD -- 注册行业
    , T3.OPER_SCOPE as SUB_CUST_OPER_SCOPE -- 经营范围
    , T3.ORG_SCALE as SUB_CUST_CORP_SCALE_CD -- 企业规模
    , T3.ORG_CRT_DT as SUB_CUST_FOUND_DT -- 成立日期
    , T3.OWNER_FLG as SUB_CUST_OPER_PLACE_OWNSHIP_CD -- 经营场地所有权
    , T3.REG_DISTRICT_TYP as SUB_CUST_REG_RGN_TYPE_CD -- 注册区域类型
    , T3.BUSINESS_STATUS as SUB_CUST_OPER_STATUS_CD -- 经营状态
    , T3.OPERATING_RECEIPT as SUB_CUST_OPEN_INCOME -- 营业收入
    , T3.ASSETS_GENERAL as SUB_CUST_TOTAL_AST -- 资产总额
    , t1.belong_mgrno as SUB_CUST_BELONG_CUST_MGR_NO -- 归属客户经理
    , t1.create_unid as SETUP_TELR_NO -- 创建机构
    , t1.create_date as SETUP_DT -- 创建时间
    , t1.checked_unid as FINAL_MODIF_TELR_NO -- 复核机构号
    , t1.checked_date as FINAL_MODIF_DT -- 复核日期
    , '${process_date}' as DATA_DT -- None
from
    ${ODS_SCB_SCHEMA}.tbl_credit_cust_order_chain_efinance as T1 -- 链汇E融白名单表
    LEFT JOIN ${ODS_SCB_SCHEMA}.tbl_merchant_info as T2 -- 第三方平台信息表
    on T1.ptno = T2.ptno
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN TMP_S03_CUST_INFO_01 as T3 -- None
    on CONCAT(TRIM(T1.id_type), TRIM(T1.id_no)) = TRIM(T3.CUST_NO) 
where T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第4组）==============
-- 供应链核心企业上下游客户信息聚合表3

insert into table ${session}.S03_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO(
      SUPCHA_CORE_CORP_NO -- 供应链核心企业编号
    , SUPCHA_SUB_CUST_NO -- 供应链子客户编号
    , SUB_CUST_DOCTYP_CD -- 子客户证件类型代码
    , SUB_CUST_DOC_NO -- 子客户证件号
    , SUB_CUST_BELONG_ORG_NO -- 子客户归属机构编号
    , SUPCHA_CUST_RELA_CATE_CD -- 供应链客户关系种类代码
    , CORE_CORP_NAME -- 核心企业名称
    , CORE_CORP_DOCTYP_CD -- 核心企业证件类型代码
    , CORE_CORP_DOC_NO -- 核心企业证件号
    , SUB_CUST_NAME -- 子客户名称
    , SUB_CUST_PRE_CRDT_LINE -- 子客户预授信额度
    , SUB_CUST_CRDT_TERM -- 子客户授信期限
    , COMP_INT_RATE_ADD_SUB_BASE_POINT -- 较LPR利率加减基点
    , SUB_CUST_WHITE_LIST_VALID -- 子客户白名单有效期
    , SUB_CUST_REG_CAP -- 子客户注册资金
    , SUB_CUST_INDS_CD -- 子客户所属行业代码
    , SUB_CUST_OPER_SCOPE -- 子客户经营范围
    , SUB_CUST_CORP_SCALE_CD -- 子客户企业规模代码
    , SUB_CUST_FOUND_DT -- 子客户成立日期
    , SUB_CUST_OPER_PLACE_OWNSHIP_CD -- 子客户经营场地所有权代码
    , SUB_CUST_REG_RGN_TYPE_CD -- 子客户注册区域类型代码
    , SUB_CUST_OPER_STATUS_CD -- 子客户经营状态代码
    , SUB_CUST_OPEN_INCOME -- 子客户营业收入
    , SUB_CUST_TOTAL_AST -- 子客户资产总额
    , SUB_CUST_BELONG_CUST_MGR_NO -- 子客户归属客户经理编号
    , SETUP_TELR_NO -- 建立柜员编号
    , SETUP_DT -- 建立日期
    , FINAL_MODIF_TELR_NO -- 最后修改柜员编号
    , FINAL_MODIF_DT -- 最后修改日期
    , DATA_DT -- 数据日期
)
select
      t1.ptno as SUPCHA_CORE_CORP_NO -- 平台码
    , t1.id as SUPCHA_SUB_CUST_NO -- 主键
    , t1.id_type as SUB_CUST_DOCTYP_CD -- 证件类型
    , t1.id_no as SUB_CUST_DOC_NO -- 证件号码
    , t1.brcode as SUB_CUST_BELONG_ORG_NO -- 机构号
    , '1' as SUPCHA_CUST_RELA_CATE_CD -- None
    , t2.mername as CORE_CORP_NAME -- 企业名称
    , t2.id_type as CORE_CORP_DOCTYP_CD -- 证件类型
    , t2.id_no as CORE_CORP_DOC_NO -- 证件号码
    , t1.cust_name as SUB_CUST_NAME -- 客户名称
    , t1.credit_line as SUB_CUST_PRE_CRDT_LINE -- 预授信额度
    , t1.credit_term as SUB_CUST_CRDT_TERM -- 授信期限
    , t1.float_rate as COMP_INT_RATE_ADD_SUB_BASE_POINT -- 较LPR利率加减基点
    , t1.whitelist_vali_period as SUB_CUST_WHITE_LIST_VALID -- 白名单有效期
    , T3.REG_CAP as SUB_CUST_REG_CAP -- 注册资金
    , T3.REG_INDS as SUB_CUST_INDS_CD -- 注册行业
    , T3.OPER_SCOPE as SUB_CUST_OPER_SCOPE -- 经营范围
    , T3.ORG_SCALE as SUB_CUST_CORP_SCALE_CD -- 企业规模
    , T3.ORG_CRT_DT as SUB_CUST_FOUND_DT -- 成立日期
    , T3.OWNER_FLG as SUB_CUST_OPER_PLACE_OWNSHIP_CD -- 经营场地所有权
    , T3.REG_DISTRICT_TYP as SUB_CUST_REG_RGN_TYPE_CD -- 注册区域类型
    , T3.BUSINESS_STATUS as SUB_CUST_OPER_STATUS_CD -- 经营状态
    , T3.OPERATING_RECEIPT as SUB_CUST_OPEN_INCOME -- 营业收入
    , T3.ASSETS_GENERAL as SUB_CUST_TOTAL_AST -- 资产总额
    , t1.belong_mgrno as SUB_CUST_BELONG_CUST_MGR_NO -- 归属客户经理
    , t1.create_unid as SETUP_TELR_NO -- 创建机构
    , t1.create_date as SETUP_DT -- 创建时间
    , t1.checked_unid as FINAL_MODIF_TELR_NO -- 复核机构号
    , t1.checked_date as FINAL_MODIF_DT -- 复核日期
    , '${process_date}' as DATA_DT -- None
from
    ${ODS_SCB_SCHEMA}.tbl_credit_cust_no_order_efinance as T1 -- 链汇E融白名单表
    LEFT JOIN ${ODS_SCB_SCHEMA}.tbl_merchant_info as T2 -- 第三方平台信息表
    on T1.ptno = T2.ptno
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN TMP_S03_CUST_INFO_01 as T3 -- None
    on CONCAT(TRIM(T1.id_type), TRIM(T1.id_no)) = TRIM(T3.CUST_NO) 
where T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第5组）==============
-- 供应链核心企业上下游客户信息聚合表4

insert into table ${session}.S03_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO(
      SUPCHA_CORE_CORP_NO -- 供应链核心企业编号
    , SUPCHA_SUB_CUST_NO -- 供应链子客户编号
    , SUB_CUST_DOCTYP_CD -- 子客户证件类型代码
    , SUB_CUST_DOC_NO -- 子客户证件号
    , SUB_CUST_BELONG_ORG_NO -- 子客户归属机构编号
    , SUPCHA_CUST_RELA_CATE_CD -- 供应链客户关系种类代码
    , CORE_CORP_NAME -- 核心企业名称
    , CORE_CORP_DOCTYP_CD -- 核心企业证件类型代码
    , CORE_CORP_DOC_NO -- 核心企业证件号
    , SUB_CUST_NAME -- 子客户名称
    , SUB_CUST_PRE_CRDT_LINE -- 子客户预授信额度
    , SUB_CUST_CRDT_TERM -- 子客户授信期限
    , COMP_INT_RATE_ADD_SUB_BASE_POINT -- 较LPR利率加减基点
    , SUB_CUST_WHITE_LIST_VALID -- 子客户白名单有效期
    , SUB_CUST_REG_CAP -- 子客户注册资金
    , SUB_CUST_INDS_CD -- 子客户所属行业代码
    , SUB_CUST_OPER_SCOPE -- 子客户经营范围
    , SUB_CUST_CORP_SCALE_CD -- 子客户企业规模代码
    , SUB_CUST_FOUND_DT -- 子客户成立日期
    , SUB_CUST_OPER_PLACE_OWNSHIP_CD -- 子客户经营场地所有权代码
    , SUB_CUST_REG_RGN_TYPE_CD -- 子客户注册区域类型代码
    , SUB_CUST_OPER_STATUS_CD -- 子客户经营状态代码
    , SUB_CUST_OPEN_INCOME -- 子客户营业收入
    , SUB_CUST_TOTAL_AST -- 子客户资产总额
    , SUB_CUST_BELONG_CUST_MGR_NO -- 子客户归属客户经理编号
    , SETUP_TELR_NO -- 建立柜员编号
    , SETUP_DT -- 建立日期
    , FINAL_MODIF_TELR_NO -- 最后修改柜员编号
    , FINAL_MODIF_DT -- 最后修改日期
    , DATA_DT -- 数据日期
)
select
      t1.ptno as SUPCHA_CORE_CORP_NO -- 平台码
    , t1.id as SUPCHA_SUB_CUST_NO -- 主键
    , t1.id_type as SUB_CUST_DOCTYP_CD -- 证件类型
    , t1.id_no as SUB_CUST_DOC_NO -- 证件号码
    , t1.brcode as SUB_CUST_BELONG_ORG_NO -- 机构号
    , '1' as SUPCHA_CUST_RELA_CATE_CD -- None
    , t2.mername as CORE_CORP_NAME -- 企业名称
    , t2.id_type as CORE_CORP_DOCTYP_CD -- 证件类型
    , t2.id_no as CORE_CORP_DOC_NO -- 证件号码
    , t1.cust_name as SUB_CUST_NAME -- 客户名称
    , t1.credit_line as SUB_CUST_PRE_CRDT_LINE -- 预授信额度
    , SUBSTR(t1.end_date,1,4) - SUBSTR(t1.start_date,1,4) as SUB_CUST_CRDT_TERM -- 1.授信起始日
2.授信到期日
    , t1.float_rate as COMP_INT_RATE_ADD_SUB_BASE_POINT -- 较LPR利率加减基点
    , t1.whitelist_vali_period as SUB_CUST_WHITE_LIST_VALID -- 白名单有效期
    , T3.REG_CAP as SUB_CUST_REG_CAP -- 注册资金
    , T3.REG_INDS as SUB_CUST_INDS_CD -- 注册行业
    , T3.OPER_SCOPE as SUB_CUST_OPER_SCOPE -- 经营范围
    , T3.ORG_SCALE as SUB_CUST_CORP_SCALE_CD -- 企业规模
    , T3.ORG_CRT_DT as SUB_CUST_FOUND_DT -- 成立日期
    , T3.OWNER_FLG as SUB_CUST_OPER_PLACE_OWNSHIP_CD -- 经营场地所有权
    , T3.REG_DISTRICT_TYP as SUB_CUST_REG_RGN_TYPE_CD -- 注册区域类型
    , T3.BUSINESS_STATUS as SUB_CUST_OPER_STATUS_CD -- 经营状态
    , T3.OPERATING_RECEIPT as SUB_CUST_OPEN_INCOME -- 营业收入
    , T3.ASSETS_GENERAL as SUB_CUST_TOTAL_AST -- 资产总额
    , t1.belong_mgrno as SUB_CUST_BELONG_CUST_MGR_NO -- 归属客户经理
    , t1.create_unid as SETUP_TELR_NO -- 创建机构
    , t1.create_date as SETUP_DT -- 创建时间
    , t1.checked_unid as FINAL_MODIF_TELR_NO -- 复核机构号
    , t1.checked_date as FINAL_MODIF_DT -- 复核日期
    , '${process_date}' as DATA_DT -- None
from
    ${ODS_SCB_SCHEMA}.tbl_warehouse_credit_register_info as T1 -- 链汇E融白名单表
    LEFT JOIN ${ODS_SCB_SCHEMA}.tbl_merchant_info as T2 -- 第三方平台信息表
    on T1.ptno = T2.ptno
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN TMP_S03_CUST_INFO_01 as T3 -- None
    on CONCAT(TRIM(T1.id_type), TRIM(T1.id_no)) = TRIM(T3.CUST_NO) 
where T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第6组）==============
-- 供应链核心企业上下游客户信息聚合表5

insert into table ${session}.S03_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO(
      SUPCHA_CORE_CORP_NO -- 供应链核心企业编号
    , SUPCHA_SUB_CUST_NO -- 供应链子客户编号
    , SUB_CUST_DOCTYP_CD -- 子客户证件类型代码
    , SUB_CUST_DOC_NO -- 子客户证件号
    , SUB_CUST_BELONG_ORG_NO -- 子客户归属机构编号
    , SUPCHA_CUST_RELA_CATE_CD -- 供应链客户关系种类代码
    , CORE_CORP_NAME -- 核心企业名称
    , CORE_CORP_DOCTYP_CD -- 核心企业证件类型代码
    , CORE_CORP_DOC_NO -- 核心企业证件号
    , SUB_CUST_NAME -- 子客户名称
    , SUB_CUST_PRE_CRDT_LINE -- 子客户预授信额度
    , SUB_CUST_CRDT_TERM -- 子客户授信期限
    , COMP_INT_RATE_ADD_SUB_BASE_POINT -- 较LPR利率加减基点
    , SUB_CUST_WHITE_LIST_VALID -- 子客户白名单有效期
    , SUB_CUST_REG_CAP -- 子客户注册资金
    , SUB_CUST_INDS_CD -- 子客户所属行业代码
    , SUB_CUST_OPER_SCOPE -- 子客户经营范围
    , SUB_CUST_CORP_SCALE_CD -- 子客户企业规模代码
    , SUB_CUST_FOUND_DT -- 子客户成立日期
    , SUB_CUST_OPER_PLACE_OWNSHIP_CD -- 子客户经营场地所有权代码
    , SUB_CUST_REG_RGN_TYPE_CD -- 子客户注册区域类型代码
    , SUB_CUST_OPER_STATUS_CD -- 子客户经营状态代码
    , SUB_CUST_OPEN_INCOME -- 子客户营业收入
    , SUB_CUST_TOTAL_AST -- 子客户资产总额
    , SUB_CUST_BELONG_CUST_MGR_NO -- 子客户归属客户经理编号
    , SETUP_TELR_NO -- 建立柜员编号
    , SETUP_DT -- 建立日期
    , FINAL_MODIF_TELR_NO -- 最后修改柜员编号
    , FINAL_MODIF_DT -- 最后修改日期
    , DATA_DT -- 数据日期
)
select
      t1.ptno as SUPCHA_CORE_CORP_NO -- 平台码
    , t1.id as SUPCHA_SUB_CUST_NO -- 主键
    , t1.id_type as SUB_CUST_DOCTYP_CD -- 证件类型
    , t1.id_no as SUB_CUST_DOC_NO -- 证件号码
    , t1.brcode as SUB_CUST_BELONG_ORG_NO -- 机构号
    , '1' as SUPCHA_CUST_RELA_CATE_CD -- None
    , t2.mername as CORE_CORP_NAME -- 企业名称
    , t2.id_type as CORE_CORP_DOCTYP_CD -- 证件类型
    , t2.id_no as CORE_CORP_DOC_NO -- 证件号码
    , t1.cust_name as SUB_CUST_NAME -- 客户名称
    , t1.credit_line as SUB_CUST_PRE_CRDT_LINE -- 预授信额度
    , T1.credit_term as SUB_CUST_CRDT_TERM -- 授信期限
    , t1.float_rate as COMP_INT_RATE_ADD_SUB_BASE_POINT -- 较LPR利率加减基点
    , t1.whitelist_vali_period as SUB_CUST_WHITE_LIST_VALID -- 白名单有效期
    , T3.REG_CAP as SUB_CUST_REG_CAP -- 注册资金
    , T3.REG_INDS as SUB_CUST_INDS_CD -- 注册行业
    , T3.OPER_SCOPE as SUB_CUST_OPER_SCOPE -- 经营范围
    , T3.ORG_SCALE as SUB_CUST_CORP_SCALE_CD -- 企业规模
    , T3.ORG_CRT_DT as SUB_CUST_FOUND_DT -- 成立日期
    , T3.OWNER_FLG as SUB_CUST_OPER_PLACE_OWNSHIP_CD -- 经营场地所有权
    , T3.REG_DISTRICT_TYP as SUB_CUST_REG_RGN_TYPE_CD -- 注册区域类型
    , T3.BUSINESS_STATUS as SUB_CUST_OPER_STATUS_CD -- 经营状态
    , T3.OPERATING_RECEIPT as SUB_CUST_OPEN_INCOME -- 营业收入
    , T3.ASSETS_GENERAL as SUB_CUST_TOTAL_AST -- 资产总额
    , t1.belong_mgrno as SUB_CUST_BELONG_CUST_MGR_NO -- 归属客户经理
    , t1.create_unid as SETUP_TELR_NO -- 创建机构
    , t1.create_date as SETUP_DT -- 创建时间
    , t1.checked_unid as FINAL_MODIF_TELR_NO -- 复核机构号
    , t1.checked_date as FINAL_MODIF_DT -- 复核日期
    , '${process_date}' as DATA_DT -- None
from
    ${ODS_SCB_SCHEMA}.tbl_credit_cust_no_order_loan as T1 -- 链汇E融白名单表
    LEFT JOIN ${ODS_SCB_SCHEMA}.tbl_merchant_info as T2 -- 第三方平台信息表
    on T1.ptno = T2.ptno
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN TMP_S03_CUST_INFO_01 as T3 -- None
    on CONCAT(TRIM(T1.id_type), TRIM(T1.id_no)) = TRIM(T3.CUST_NO) 
where T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第7组）==============
-- 供应链核心企业上下游客户信息聚合表6

insert into table ${session}.S03_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO(
      SUPCHA_CORE_CORP_NO -- 供应链核心企业编号
    , SUPCHA_SUB_CUST_NO -- 供应链子客户编号
    , SUB_CUST_DOCTYP_CD -- 子客户证件类型代码
    , SUB_CUST_DOC_NO -- 子客户证件号
    , SUB_CUST_BELONG_ORG_NO -- 子客户归属机构编号
    , SUPCHA_CUST_RELA_CATE_CD -- 供应链客户关系种类代码
    , CORE_CORP_NAME -- 核心企业名称
    , CORE_CORP_DOCTYP_CD -- 核心企业证件类型代码
    , CORE_CORP_DOC_NO -- 核心企业证件号
    , SUB_CUST_NAME -- 子客户名称
    , SUB_CUST_PRE_CRDT_LINE -- 子客户预授信额度
    , SUB_CUST_CRDT_TERM -- 子客户授信期限
    , COMP_INT_RATE_ADD_SUB_BASE_POINT -- 较LPR利率加减基点
    , SUB_CUST_WHITE_LIST_VALID -- 子客户白名单有效期
    , SUB_CUST_REG_CAP -- 子客户注册资金
    , SUB_CUST_INDS_CD -- 子客户所属行业代码
    , SUB_CUST_OPER_SCOPE -- 子客户经营范围
    , SUB_CUST_CORP_SCALE_CD -- 子客户企业规模代码
    , SUB_CUST_FOUND_DT -- 子客户成立日期
    , SUB_CUST_OPER_PLACE_OWNSHIP_CD -- 子客户经营场地所有权代码
    , SUB_CUST_REG_RGN_TYPE_CD -- 子客户注册区域类型代码
    , SUB_CUST_OPER_STATUS_CD -- 子客户经营状态代码
    , SUB_CUST_OPEN_INCOME -- 子客户营业收入
    , SUB_CUST_TOTAL_AST -- 子客户资产总额
    , SUB_CUST_BELONG_CUST_MGR_NO -- 子客户归属客户经理编号
    , SETUP_TELR_NO -- 建立柜员编号
    , SETUP_DT -- 建立日期
    , FINAL_MODIF_TELR_NO -- 最后修改柜员编号
    , FINAL_MODIF_DT -- 最后修改日期
    , DATA_DT -- 数据日期
)
select
      t1.ptno as SUPCHA_CORE_CORP_NO -- 平台码
    , t1.id as SUPCHA_SUB_CUST_NO -- 主键
    , t1.id_type as SUB_CUST_DOCTYP_CD -- 证件类型
    , t1.id_no as SUB_CUST_DOC_NO -- 证件号码
    , t1.brcode as SUB_CUST_BELONG_ORG_NO -- 机构号
    , '1' as SUPCHA_CUST_RELA_CATE_CD -- None
    , t2.mername as CORE_CORP_NAME -- 企业名称
    , t2.id_type as CORE_CORP_DOCTYP_CD -- 证件类型
    , t2.id_no as CORE_CORP_DOC_NO -- 证件号码
    , t1.cust_name as SUB_CUST_NAME -- 客户名称
    , t1.credit_line as SUB_CUST_PRE_CRDT_LINE -- 预授信额度
    , T1.credit_term as SUB_CUST_CRDT_TERM -- 授信期限
    , t1.float_rate as COMP_INT_RATE_ADD_SUB_BASE_POINT -- 较LPR利率加减基点
    , t1.whitelist_vali_period as SUB_CUST_WHITE_LIST_VALID -- 白名单有效期
    , T3.REG_CAP as SUB_CUST_REG_CAP -- 注册资金
    , T3.REG_INDS as SUB_CUST_INDS_CD -- 注册行业
    , T3.OPER_SCOPE as SUB_CUST_OPER_SCOPE -- 经营范围
    , T3.ORG_SCALE as SUB_CUST_CORP_SCALE_CD -- 企业规模
    , T3.ORG_CRT_DT as SUB_CUST_FOUND_DT -- 成立日期
    , T3.OWNER_FLG as SUB_CUST_OPER_PLACE_OWNSHIP_CD -- 经营场地所有权
    , T3.REG_DISTRICT_TYP as SUB_CUST_REG_RGN_TYPE_CD -- 注册区域类型
    , T3.BUSINESS_STATUS as SUB_CUST_OPER_STATUS_CD -- 经营状态
    , T3.OPERATING_RECEIPT as SUB_CUST_OPEN_INCOME -- 营业收入
    , T3.ASSETS_GENERAL as SUB_CUST_TOTAL_AST -- 资产总额
    , t1.belong_mgrno as SUB_CUST_BELONG_CUST_MGR_NO -- 归属客户经理
    , t1.create_unid as SETUP_TELR_NO -- 创建机构
    , t1.create_date as SETUP_DT -- 创建时间
    , t1.checked_unid as FINAL_MODIF_TELR_NO -- 复核机构号
    , t1.checked_date as FINAL_MODIF_DT -- 复核日期
    , '${process_date}' as DATA_DT -- None
from
    ${ODS_SCB_SCHEMA}.tbl_credit_cust_order_efinance as T1 -- 链汇E融白名单表
    LEFT JOIN ${ODS_SCB_SCHEMA}.tbl_merchant_info as T2 -- 第三方平台信息表
    on T1.ptno = T2.ptno
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN TMP_S03_CUST_INFO_01 as T3 -- None
    on CONCAT(TRIM(T1.id_type), TRIM(T1.id_no)) = TRIM(T3.CUST_NO) 
where T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;

-- 删除所有临时表
drop table ${session}.TMP_S03_CUST_INFO_01;