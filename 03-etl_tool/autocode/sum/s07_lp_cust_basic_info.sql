-- 层次表名: 聚合层-法人客户基本信息聚合
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s07_lp_cust_basic_info
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S07_LP_CUST_BASIC_INFO
--     表中文名：法人客户基本信息聚合
--     创建日期：2023-12-22 00:00:00
--     主键字段：CUST_IN_CD,LP_ORG_NO
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：取ecif内所有法人客户基本信息、扩展信息，大信贷取股东信息、eicc的一些关联人信息
--     更新记录：
--         2023-12-19 00:00:00 王穆军 新增映射文件信息
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S07_LP_CUST_BASIC_INFO drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 对公客户证件信息临时表处理
drop table if exists ${session}.TMP_S07_LP_CUST_BASIC_INFO_01;

create table ${session}.TMP_S07_LP_CUST_BASIC_INFO_01 (
      CUST_IN_CD varchar(100) -- 客户内码
    , MAIN_DOCTYP_CD varchar(30) -- 主证件类型代码
    , MAIN_DOC_NO varchar(100) -- 主证件号码
    , MAIN_DOC_MATU_DT varchar(10) -- 主证件到期日期
    , MAIN_DOC_ISSUE_ORG_CTRY_RGN_CD varchar(30) -- 主证件签发机关国家地区代码
    , NATION_TAX_REG_CERT_NO varchar(23) -- 国税登记证号码
    , LOCAL_TAX_REG_CERT_NO varchar(23) -- 地税登记证号码
    , ORG_CD varchar(23) -- 组织机构代码证号码
    , BIZ_LIC_NO varchar(23) -- 营业执照号码
    , BIZ_LIC_MATU_DT varchar(10) -- 营业执照到期日期
)
comment '对公客户证件信息临时表处理'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S07_LP_CUST_BASIC_INFO_01(
      CUST_IN_CD -- 客户内码
    , MAIN_DOCTYP_CD -- 主证件类型代码
    , MAIN_DOC_NO -- 主证件号码
    , MAIN_DOC_MATU_DT -- 主证件到期日期
    , MAIN_DOC_ISSUE_ORG_CTRY_RGN_CD -- 主证件签发机关国家地区代码
    , NATION_TAX_REG_CERT_NO -- 国税登记证号码
    , LOCAL_TAX_REG_CERT_NO -- 地税登记证号码
    , ORG_CD -- 组织机构代码证号码
    , BIZ_LIC_NO -- 营业执照号码
    , BIZ_LIC_MATU_DT -- 营业执照到期日期
)
select
      P1.CUST_INCOD as CUST_IN_CD -- 客户内码
    , P2.ALT_TYPE as MAIN_DOCTYP_CD -- 企业证件类型
    , P2.ALT_ID as MAIN_DOC_NO -- 企业证件号码
    , P2.END_DT as MAIN_DOC_MATU_DT -- 企业证件到期日期
    , P2.ISSUED_CTY_ID as MAIN_DOC_ISSUE_ORG_CTRY_RGN_CD -- 企业证件签发国家或地区
    , P3.ALT_ID as NATION_TAX_REG_CERT_NO -- 国税登记证号码
    , P4.ALT_ID as LOCAL_TAX_REG_CERT_NO -- 地税登记证号码
    , P5.ALT_ID as ORG_CD -- 组织机构代码
    , P6.ALT_ID as BIZ_LIC_NO -- 营业执照号
    , P6.END_DT as BIZ_LIC_MATU_DT -- 营业执照到期日期
from
    ${ods_ecif_schema}.T2_EN_CUST_BASE_INFO as T1 -- 对公客户基本信息
    LEFT JOIN 	(
	SELECT 
		 P1.CUST_INCOD   	--客户内码
		,P1.ALT_TYPE    	--主证件类型
		,P1.ALT_ID      	--主证件号码
		,P1.IS_MST_ID   	--主证件标志
		,P1.VALID_FLAG  	--有效标志
		,P1.END_DT      	--证件失效日期
		,P1.ISSUED_CITY_ID  --证件签发国家或地区
		,ROW_NUMBER()OVER(PARTITION BY P1.CUST_INCOD,P1.ALT_TYPE
	ORDER BY P1.UPDATE_TIME DESC ) RN
	FROM ${ods_ecif_schema}.T2_EN_CUST_ALT_INFO P1 --对公客户证件信息
	WHERE 1=1
		AND P1.VALID_FLAG = '1'
		AND P1.IS_MST_ID = '1'
		AND P1.PT_DT='${process_date}' 
		AND P1.DELETED='0'
	) as T2 -- 对公客户证件信息
    on T1.CUST_INCOD = T2.CUST_INCOD
AND T2.RN = 1  
    LEFT JOIN 	(
	SELECT 
		 P2.CUST_INCOD   	--客户内码
		,P2.ALT_TYPE    	--主证件类型
		,P2.ALT_ID      	--主证件号码
		,P2.END_DT      	--证件失效日期
		,ROW_NUMBER()OVER(PARTITION BY P2.CUST_INCOD,P2.ALT_TYPE
	ORDER BY P2.UPDATE_TIME DESC ) RN
	FROM ${ods_ecif_schema}.T2_EN_CUST_ALT_INFO P2 --对公客户证件信息
	WHERE 1=1
		AND P2.ALT_TYPE = '401' --国税证件信息
		AND P2.PT_DT='${process_date}' 
		AND P2.DELETED='0'
	) as T3 -- 对公客户证件信息
    on T1.CUST_INCOD=T3.CUST_INCOD 
AND T3.RN = 1  
    LEFT JOIN 	(
	SELECT 
		 P3.CUST_INCOD   	--客户内码
		,P3.ALT_TYPE    	--主证件类型
		,P3.ALT_ID      	--主证件号码
		,P3.END_DT      	--证件失效日期
		,ROW_NUMBER()OVER(PARTITION BY P3.CUST_INCOD,P3.ALT_TYPE
	ORDER BY P3.UPDATE_TIME DESC ) RN
	FROM ${ods_ecif_schema}.T2_EN_CUST_ALT_INFO P3 --对公客户证件信息
	WHERE 1=1
		AND P3.ALT_TYPE = '402' --地税证件信息
		AND P3.PT_DT='${process_date}' 
		AND P3.DELETED='0'
	) as T4 -- 对公客户证件信息
    on T1.CUST_INCOD=T4.CUST_INCOD 
AND T4.RN = 1 
    LEFT JOIN 	(
	SELECT 
		 P4.CUST_INCOD   	--客户内码
		,P4.ALT_TYPE    	--主证件类型
		,P4.ALT_ID      	--主证件号码
		,P4.END_DT      	--证件失效日期
		,ROW_NUMBER()OVER(PARTITION BY P4.CUST_INCOD,P4.ALT_TYPE
	ORDER BY P4.UPDATE_TIME DESC ) RN
	FROM ${ods_ecif_schema}.T2_EN_CUST_ALT_INFO P4 --对公客户证件信息
	WHERE 1=1
		AND P4.ALT_TYPE = '203' --组织机构代码信息
		AND P4.PT_DT='${process_date}' 
		AND P4.DELETED='0'
	)  as T5 -- 对公客户证件信息
    on T1.CUST_INCOD=T5.CUST_INCOD 
AND  T5.RN = 1 
    LEFT JOIN 	(
	SELECT 
		 P5.CUST_INCOD   	--客户内码
		,P5.ALT_TYPE    	--主证件类型
		,P5.ALT_ID      	--主证件号码
		,P5.END_DT      	--证件失效日期
		,ROW_NUMBER()OVER(PARTITION BY P5.CUST_INCOD,P5.ALT_TYPE
	ORDER BY P5.UPDATE_TIME DESC ) RN
	FROM ${ods_ecif_schema}.T2_EN_CUST_ALT_INFO P5 --对公客户证件信息
	WHERE 1=1
		AND P5.ALT_TYPE = '202' --营业执照信息
		AND P5.PT_DT='${process_date}' 
		AND P5.DELETED='0'
	) as T6 -- 对公客户证件信息
    on T1.CUST_INCOD=T6.CUST_INCOD 
AND  T6.RN = 1 
where 1=1 
AND T1.MCIP_CST_TP IN('2','3')
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第2组）==============
-- 对公客户联系人信息临时表处理
drop table if exists ${session}.TMP_S07_LP_CUST_BASIC_INFO_02;

create table ${session}.TMP_S07_LP_CUST_BASIC_INFO_02 (
      CUST_IN_CD varchar(100) -- 客户内码
    , GENERAL_MGER_CUST_IN_CD varchar(100) -- 总经理客户内码
    , GENERAL_MGER_NAME varchar(200) -- 总经理名称
    , FIN_PRINC_CUST_IN_CD varchar(100) -- 财务负责人客户内码
    , FIN_PRINC_NAME varchar(200) -- 财务负责人名称
    , OTHER_CONT_CUST_IN_CD varchar(100) -- 其他联系人客户内码
    , OTHER_CONT_NAME varchar(200) -- 其他联系人名称
    , ACTL_OPER_PSN_CUST_IN_CD varchar(100) -- 实际经营人客户内码
    , ACTL_OPER_PSN_NAME varchar(200) -- 实际经营人名称
    , BOD_CHAIR_CUST_IN_CD varchar(100) -- 董事长客户内码
    , BOD_CHAIR_NAME varchar(200) -- 董事长名称
    , ACTL_CTRLER_CUST_IN_CD varchar(100) -- 实际控制人客户内码
    , ACTL_CTRLER_NAME varchar(200) -- 实际控制人姓名
    , LEGAL_REP_CUST_IN_CD varchar(100) -- 法定代表人客户内码
    , LEGAL_REP_CUST_TYPE_NO character(2) -- 法定代表人客户类型代码
    , LEGAL_REP_NAME varchar(200) -- 法定代表人名称
    , LEGAL_REP_DOCTYP_CD character(30) -- 法定代表人证件类型代码
    , LEGAL_REP_DOC_NO character(100) -- 法定代表人证件号码
    , LEGAL_REP_DOC_ISSUE_DT varchar(10) -- 法定代表人证件签发日期
    , LEGAL_REP_DOC_MATU_DT varchar(10) -- 法定代表人证件到期日期
    , AUTH_PROC_BIZ_PSN_CUST_IN_CD varchar(100) -- 授权办理业务人客户内码
    , AUTH_PROC_BIZ_PSN_NAME varchar(200) -- 授权办理业务人名称
    , AUTH_PROC_BIZ_PSN_DOCTYP_CD varchar(30) -- 授权办理业务人证件类型代码
    , AUTH_PROC_BIZ_PSN_DOC_NO varchar(100) -- 授权办理业务人证件号码
    , STOCKHOLDER_ISN varchar(100) -- 控股股东股东客户内码
    , INVEST_TYPE char(1) -- 控股股东出资方式
    , INVEST_CCY varchar(30) -- 控股股东出资币种
    , INVEST_AMOUNT decimal(28,2) -- 控股股东出资金额
    , INVEST_CAPITAL decimal(7,4) -- 控股股东出资占比
    , STOCK_HOLD_QUANTITY bigint -- 控股股东持股数量
    , STOCK_HOLD_RATE decimal(7,4) -- 控股股东持股比例
    , HOLDING_SIGN char(1) -- 控股股东控股标志
)
comment '对公客户联系人信息临时表处理'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S07_LP_CUST_BASIC_INFO_02(
      CUST_IN_CD -- 客户内码
    , GENERAL_MGER_CUST_IN_CD -- 总经理客户内码
    , GENERAL_MGER_NAME -- 总经理名称
    , FIN_PRINC_CUST_IN_CD -- 财务负责人客户内码
    , FIN_PRINC_NAME -- 财务负责人名称
    , OTHER_CONT_CUST_IN_CD -- 其他联系人客户内码
    , OTHER_CONT_NAME -- 其他联系人名称
    , ACTL_OPER_PSN_CUST_IN_CD -- 实际经营人客户内码
    , ACTL_OPER_PSN_NAME -- 实际经营人名称
    , BOD_CHAIR_CUST_IN_CD -- 董事长客户内码
    , BOD_CHAIR_NAME -- 董事长名称
    , ACTL_CTRLER_CUST_IN_CD -- 实际控制人客户内码
    , ACTL_CTRLER_NAME -- 实际控制人姓名
    , LEGAL_REP_CUST_IN_CD -- 法定代表人客户内码
    , LEGAL_REP_CUST_TYPE_NO -- 法定代表人客户类型代码
    , LEGAL_REP_NAME -- 法定代表人名称
    , LEGAL_REP_DOCTYP_CD -- 法定代表人证件类型代码
    , LEGAL_REP_DOC_NO -- 法定代表人证件号码
    , LEGAL_REP_DOC_ISSUE_DT -- 法定代表人证件签发日期
    , LEGAL_REP_DOC_MATU_DT -- 法定代表人证件到期日期
    , AUTH_PROC_BIZ_PSN_CUST_IN_CD -- 授权办理业务人客户内码
    , AUTH_PROC_BIZ_PSN_NAME -- 授权办理业务人名称
    , AUTH_PROC_BIZ_PSN_DOCTYP_CD -- 授权办理业务人证件类型代码
    , AUTH_PROC_BIZ_PSN_DOC_NO -- 授权办理业务人证件号码
    , STOCKHOLDER_ISN -- 控股股东股东客户内码
    , INVEST_TYPE -- 控股股东出资方式
    , INVEST_CCY -- 控股股东出资币种
    , INVEST_AMOUNT -- 控股股东出资金额
    , INVEST_CAPITAL -- 控股股东出资占比
    , STOCK_HOLD_QUANTITY -- 控股股东持股数量
    , STOCK_HOLD_RATE -- 控股股东持股比例
    , HOLDING_SIGN -- 控股股东控股标志
)
select
      P1.CUST_INCOD as CUST_IN_CD -- 客户内码
    , COALESCE(T2.PER_CUST_INCOD,'') as GENERAL_MGER_CUST_IN_CD -- 联系客户内码
    , COALESCE(T2.AFFLTD_PERS_NM,'') as GENERAL_MGER_NAME -- 关联人姓名
    , COALESCE(T3.PER_CUST_INCOD,'') as FIN_PRINC_CUST_IN_CD -- 联系客户内码
    , COALESCE(T3.AFFLTD_PERS_NM,'') as FIN_PRINC_NAME -- 关联人姓名
    , COALESCE(T4.PER_CUST_INCOD,'') as OTHER_CONT_CUST_IN_CD -- 联系客户内码
    , COALESCE(T4.AFFLTD_PERS_NM,'') as OTHER_CONT_NAME -- 关联人姓名
    , COALESCE(T5.PER_CUST_INCOD,'') as ACTL_OPER_PSN_CUST_IN_CD -- 联系客户内码
    , COALESCE(T5.AFFLTD_PERS_NM,'') as ACTL_OPER_PSN_NAME -- 关联人姓名
    , COALESCE(T6.PER_CUST_INCOD,'') as BOD_CHAIR_CUST_IN_CD -- 联系客户内码
    , COALESCE(T6.AFFLTD_PERS_NM,'') as BOD_CHAIR_NAME -- 关联人姓名
    , COALESCE(T7.PER_CUST_INCOD,'') as ACTL_CTRLER_CUST_IN_CD -- 联系客户内码
    , COALESCE(T7.AFFLTD_PERS_NM,'') as ACTL_CTRLER_NAME -- 关联人姓名
    , COALESCE(T8.PER_CUST_INCOD,T9.CC01CSNO) as LEGAL_REP_CUST_IN_CD -- 法定代表人客户内码
    , COALESCE(T8.AFFLTD_PERS_TP,'') as LEGAL_REP_CUST_TYPE_NO -- 法定代表人客户类型编号
    , COALESCE(T8.AFFLTD_PERS_NM,T9.CC01FLNM) as LEGAL_REP_NAME -- 法定代表人姓名
    , COALESCE(T8.ID_TP,T9.CC01CFTP) as LEGAL_REP_DOCTYP_CD -- 法定代表人证件类型代码
    , COALESCE(T8.ID_NO,T9.CC01CFNO) as LEGAL_REP_DOC_NO -- 法定代表人证件号码
    , T8.ISSUED_DT as LEGAL_REP_DOC_ISSUE_DT -- 法定代表人证件签发日期
    , T8.END_DT as LEGAL_REP_DOC_MATU_DT -- 法定代表人证件到期日期
    , T9.CC03CSNO as AUTH_PROC_BIZ_PSN_CUST_IN_CD -- 授权办理业务人客户内码
    , T9.CC03FLNM as AUTH_PROC_BIZ_PSN_NAME -- 授权办理业务人姓名
    , T9.CC03CFTP as AUTH_PROC_BIZ_PSN_DOCTYP_CD -- 授权办理业务人证件类型代码
    , T9.CC03CFNO as AUTH_PROC_BIZ_PSN_DOC_NO -- 授权办理业务人证件号码
    , T10.STOCKHOLDER_ISN as STOCKHOLDER_ISN -- 股东1内码
    , T10.INVEST_TYPE as INVEST_TYPE -- 出资方式
    , T10.INVEST_CCY as INVEST_CCY -- 出资币种
    , T10.INVEST_AMOUNT as INVEST_AMOUNT -- 出资金额
    , T10.INVEST_CAPITAL as INVEST_CAPITAL -- 出资占比
    , T10.STOCK_HOLD_QUANTITY as STOCK_HOLD_QUANTITY -- 持股数量
    , T10.STOCK_HOLD_RATE as STOCK_HOLD_RATE -- 持股比例
    , T10.HOLDING_SIGN as HOLDING_SIGN -- 控股标志
from
    ${ods_ecif_schema}.T2_EN_CUST_BASE_INFO as T1 -- 对公客户基本信息
    LEFT JOIN 	(
	SELECT 
	 P2.CUST_INCOD
	,P2.PER_CUST_INCOD
	,P2.AFFLTD_PERS_NM
	,P2.AFFLTD_PERS_TP
	,P2.BK_ID
	,ROW_NUMBER()OVER(PARTITION BY P1.CUST_INCOD,
	P1.AFFLTD_PERS_TP,P1.BK_ID
ORDER BY
	P1.END_TIME) RN 
FROM ${ods_ecif_schema}.T2_EN_AFFLTD_PERS_INFO P2 
WHERE 
	1=1
	AND P1.VALID_FLAG = '1' --有效数据
	AND P1.AFFLTD_PERS_TP = '4' --总经理
	AND P1.PT_DT='${process_date}' 
	AND P1.DELETED='0'
	) as T2  -- 对公重要关联人信息
    on T1.CUST_INCOD=  T2.CUST_INCOD 
AND  T1.BK_ID=T2.BK_ID
AND T2.RN =1  
    LEFT JOIN 	(
	SELECT 
	 P2.CUST_INCOD
	,P2.PER_CUST_INCOD
	,P2.AFFLTD_PERS_NM
	,P2.AFFLTD_PERS_TP
	,P2.BK_ID
	,ROW_NUMBER()OVER(PARTITION BY P2.CUST_INCOD,
	P2.AFFLTD_PERS_TP,P2.BK_ID
ORDER BY
	P2.END_TIME) RN 
FROM ${ods_ecif_schema}.T2_EN_AFFLTD_PERS_INFO P2 
WHERE 
	1=1
	AND P2.VALID_FLAG = '1' --有效数据
	AND P2.AFFLTD_PERS_TP = '5' --财务负责人
	AND P2.PT_DT='${process_date}' 
	AND P2.DELETED='0'
	) as T3 -- 对公重要关联人信息
    on T1.CUST_INCOD=  T3.CUST_INCOD 
AND  T1.BK_ID=T3.BK_ID
AND T3.RN = 1 
    LEFT JOIN 	(
	SELECT 
	 P1.CUST_INCOD
	,P1.PER_CUST_INCOD
	,P1.AFFLTD_PERS_NM
	,P1.AFFLTD_PERS_TP
	,P1.BK_ID
	,ROW_NUMBER()OVER(PARTITION BY P1.CUST_INCOD,
	P1.AFFLTD_PERS_TP,P1.BK_ID
ORDER BY
	P1.END_TIME) RN 
FROM ${ods_ecif_schema}.T2_EN_AFFLTD_PERS_INFO P2 
WHERE 
	1=1
	AND P1.VALID_FLAG = '1' --有效数据
	AND P1.AFFLTD_PERS_TP = '8' --其他联系人
	AND P1.PT_DT='${process_date}' 
	AND P1.DELETED='0'
	) as T4 -- 对公重要关联人信息
    on T1.CUST_INCOD=  T4.CUST_INCOD 
AND  T1.BK_ID=T4.BK_ID
AND T4.RN =1 
    LEFT JOIN 	(
	SELECT 
	 P1.CUST_INCOD
	,P1.PER_CUST_INCOD
	,P1.AFFLTD_PERS_NM
	,P1.AFFLTD_PERS_TP
	,P1.BK_ID
	,ROW_NUMBER()OVER(PARTITION BY P1.CUST_INCOD,
	P1.AFFLTD_PERS_TP,P1.BK_ID
ORDER BY
	P1.END_TIME) RN 
FROM ${ods_ecif_schema}.T2_EN_AFFLTD_PERS_INFO P2 
WHERE 
	1=1
	AND P1.VALID_FLAG = '1' --有效数据
	AND P1.AFFLTD_PERS_TP = '2' --其他联系人
	AND P1.PT_DT='${process_date}' 
	AND P1.DELETED='0'
	) as T5 -- 对公重要关联人信息
    on T1.CUST_INCOD=  T5.CUST_INCOD 
AND  T1.BK_ID=T5.BK_ID
AND T5.RN = 1 
    LEFT JOIN 	(
	SELECT 
	 P1.CUST_INCOD
	,P1.PER_CUST_INCOD
	,P1.AFFLTD_PERS_NM
	,P1.AFFLTD_PERS_TP
	,P1.BK_ID
	,ROW_NUMBER()OVER(PARTITION BY P1.CUST_INCOD,
	P1.AFFLTD_PERS_TP,P1.BK_ID
ORDER BY
	P1.END_TIME) RN 
FROM ${ods_ecif_schema}.T2_EN_AFFLTD_PERS_INFO P2 
WHERE 
	1=1
	AND P1.VALID_FLAG = '1' --有效数据
	AND P1.AFFLTD_PERS_TP = '3' --董事长
	AND P1.PT_DT='${process_date}' 
	AND P1.DELETED='0'
	) as T6 -- 对公重要关联人信息
    on T1.CUST_INCOD=  T6.CUST_INCOD 
AND  T1.BK_ID=T6.BK_ID
AND T6.RN = 1 
    LEFT JOIN 	(
	SELECT 
	 P1.CUST_INCOD
	,P1.PER_CUST_INCOD
	,P1.AFFLTD_PERS_NM
	,P1.AFFLTD_PERS_TP
	,P1.BK_ID
	,ROW_NUMBER()OVER(PARTITION BY P1.CUST_INCOD,
	P1.AFFLTD_PERS_TP,P1.BK_ID
ORDER BY
	P1.END_TIME) RN 
FROM ${ods_ecif_schema}.T2_EN_AFFLTD_PERS_INFO P2 
WHERE 
	1=1
	AND P1.VALID_FLAG = '1' --有效数据
	AND P1.AFFLTD_PERS_TP = '7' --实际控制人
	AND P1.PT_DT='${process_date}' 
	AND P1.DELETED='0'
	) as T7 -- 对公重要关联人信息
    on T1.CUST_INCOD=  T7.CUST_INCOD 
AND  T1.BK_ID=T7.BK_ID
AND T7.RN =1  
    LEFT JOIN (
	SELECT 
		 P1.CUST_INCOD
	,p1.PER_CUST_INCOD
	,p1.AFFLTD_PERS_TP
	,p1.AFFLTD_PERS_NM
	,p1.ID_TP
	,p1.ID_NO
	,p1.ISSUED_DT
	,p1.END_DT
	,p1.bk_id
	,ROW_NUMBER()OVER(PARTITION BY P1.CUST_INCOD,P1.BK_ID,P1.AFFLTD_PERS_TP
ORDER BY
	P1.END_TIME) RN 
FROM ${ods_ecif_schema}.T2_EN_LEGAL_REPRESENTATIVE_INFO P1 
WHERE 
	1=1
	AND P1.VALID_FLAG = '1' --有效数据
	AND P1.AFFLTD_PERS_TP = '1' --法定代表人
	AND P1.PT_DT='${process_date}' 
	AND P1.DELETED='0'
	) as T8 -- 企业法人信息
    on T1.CUST_INCOD=  T8.CUST_INCOD 
AND  T1.BK_ID=T8.BK_ID
AND T8.RN =1 
    LEFT JOIN ${ods_core_schema}.BCFMCCBI as T9 -- 客户辅助信息文件(对公）
    on T1.CUST_INCOD=  T9.CINOCSNO 
AND T9.CCBIBRNO=T1.BK_ID
AND T9.PT_DT='${process_date}' 
AND T9.DELETED='0' 
    LEFT JOIN 	(
	SELECT 
	 P1.CUST_ISN --客户内码
	 P1.BEL_ORG --归属机构号
	,P1.STOCKHOLDER_ISN --股东内码
	,P1.INVEST_TYPE	--出资方式
	,P1.INVEST_CCY	--出资币种
	,P1.INVEST_AMOUNT --出资金额
	,P1.INVEST_CAPITAL --出资占比
	,P1.STOCK_HOLD_QUANTITY	--持股数量
	,P1.STOCK_HOLD_RATE	--持股比例


	,ROW_NUMBER()OVER(PARTITION BY P1.CUST_ISN,P1.BEL_ORG,P1.ADR_TYPE
ORDER BY
	P1.INVEST_AMOUNT,P1.INVEST_CAPITAL DESC ) RN 
FROM ${ods_core_schema}.EN_RL_SH_INFO P1  --对公客户股东信息
WHERE 
	1=1
	AND P1.PT_DT='${process_date}' 
	AND P1.DELETED='0'
	AND holding_sign = '1' --是否持股
	) as T10 -- 对公客户股东信息表
    on T1.CUST_INCOD=  T10.CUST_ISN 
AND  T10.BEL_ORG=T1.BK_ID 
AND T10.RN = 1 
where 1=1 
AND T1.MCIP_CST_TP IN('2','3')
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第3组）==============
-- 对公客户地址信息临时表处理
drop table if exists ${session}.TMP_S07_LP_CUST_BASIC_INFO_03;

create table ${session}.TMP_S07_LP_CUST_BASIC_INFO_03 (
      CUST_IN_CD varchar(100) -- 客户内码
    , CORP_OFFICE_ADDR varchar(500) -- 经营地址
    , OFFICE_ADDR_DIST varchar(30) -- 经营地址行政区划
    , OFFICE_ADDR_ZIP_CD varchar(100) -- 经营地址邮政编码
    , CORP_REG_ADDR_ varchar(500) -- 注册地址
    , REG_ADDR_DIST varchar(30) -- 注册地址行政区划
    , REG_ADDR_ZIP_CD varchar(100) -- 注册地址邮编
)
comment '对公客户地址信息临时表处理'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S07_LP_CUST_BASIC_INFO_03(
      CUST_IN_CD -- 客户内码
    , CORP_OFFICE_ADDR -- 经营地址
    , OFFICE_ADDR_DIST -- 经营地址行政区划
    , OFFICE_ADDR_ZIP_CD -- 经营地址邮政编码
    , CORP_REG_ADDR_ -- 注册地址
    , REG_ADDR_DIST -- 注册地址行政区划
    , REG_ADDR_ZIP_CD -- 注册地址邮编
)
select
      P1.CUST_INCOD as CUST_IN_CD -- 客户内码
    , T2.ADR_INFO_DESC as CORP_OFFICE_ADDR -- 地址信息描述
    , T2.ADR_ADM_DIV as OFFICE_ADDR_DIST -- 地址行政区划
    , T2.POST_CODE as OFFICE_ADDR_ZIP_CD -- 邮政编码
    , T3.ADR_INFO_DESC as CORP_REG_ADDR_ -- 地址信息描述
    , T3.ADR_ADM_DIV as REG_ADDR_DIST -- 地址行政区划
    , T3.POST_CODE as REG_ADDR_ZIP_CD -- 邮政编码
from
    ${ods_ecif_schema}.T2_EN_CUST_BASE_INFO as T1 -- 对公客户基本信息
    LEFT JOIN 	(
	SELECT 
	 P1.CUST_INCOD
	,p1.ADR_TYPE
	,P1.ADR_INFO_DESC
	,P1.ADR_ADM_DIV
	,P1.POST_CODE
	,p1.bk_id

	,ROW_NUMBER()OVER(PARTITION BY P1.CUST_INCOD,P1.BK_ID,P1.ADR_TYPE
ORDER BY
	P1.ADR_NO,P1.UPDATE_TIME DESC ) RN 
FROM ${ods_ecif_schema}.T2_EN_CUST_ADDR_INFO P1  --对公客户地址信息
WHERE 
	1=1
	AND P1.VALID_FLAG = '1' --有效数据
	AND P1.PT_DT='${process_date}' 
	AND P1.DELETED='0'
	AND P1.ADR_TYPE IN('6','4') --6 办公 4 注册
	) as T2 -- 对公客户地址信息
    on T1.CUST_INCOD= T2.CUST_INCOD 
AND T1.BK_ID=T2.BK_ID 
AND T2.ADR_TYPE='6'
AND T2.RN = 1 
    LEFT JOIN 	(
	SELECT 
	 P1.CUST_INCOD
	,p1.ADR_TYPE
	,P1.ADR_INFO_DESC
	,P1.ADR_ADM_DIV
	,P1.POST_CODE
	,p1.bk_id

	,ROW_NUMBER()OVER(PARTITION BY P1.CUST_INCOD,P1.BK_ID,P1.ADR_TYPE
ORDER BY
	P1.ADR_NO,P1.UPDATE_TIME DESC ) RN 
FROM ${ods_ecif_schema}.T2_EN_CUST_ADDR_INFO P1  --对公客户地址信息
WHERE 
	1=1
	AND P1.VALID_FLAG = '1' --有效数据
	AND P1.PT_DT='${process_date}' 
	AND P1.DELETED='0'
	AND P1.ADR_TYPE IN('6','4') --6 办公 4 注册
	) as T3 -- 对公客户地址信息
    on T1.CUST_INCOD= T3.CUST_INCOD 
AND T1.BK_ID=T3.BK_ID 
AND T3.ADR_TYPE='4'
AND T3.RN = 1 
where 1=1 
AND T1.MCIP_CST_TP IN('2','3')
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第4组）==============
-- 对公客户基本信息临时表处理
drop table if exists ${session}.TMP_S07_LP_CUST_BASIC_INFO_04;

create table ${session}.TMP_S07_LP_CUST_BASIC_INFO_04 (
      CUST_IN_CD varchar(100) -- 客户内码
    , LP_ORG_NO varchar(100) -- 法人机构编号
    , CUST_NAME varchar(200) -- 客户名称
    , CUST_ABB varchar(30) -- 客户简称
    , CUST_EN_NAME varchar(200) -- 客户英文名称
    , CUST_MCLS_CD character(1) -- 客户大类代码
    , CUST_NATION_CD varchar(30) -- 国籍代码
    , CUST_STATUS_CD varchar(30) -- 客户状态代码
    , CORP_REG_CAP decimal(23,2) -- 注册资金
    , REG_CAP_MNTY_CD varchar(30) -- 注册资金币种代码
    , CRDT_CUST_FLAG character(1) -- 信贷客户标志
    , OPER_SCOPE varchar(200) -- 经营范围
    , REG_INDUS_CD character(4) -- 注册行业类型代码
    , CORP_SCALE_CD character(1) -- 企业规模代码
    , CORP_EMPLY_NUM integer(4) -- 公司员工人数
    , CUST_OPEN_INCMAMT decimal(28,2) -- 营业收入金额
    , TOTAL_AST decimal(28,2) -- 资产总额
    , CRDT_CD varchar(30) -- 信用等级代码
    , FNC_GUAR_CORP varchar(10) -- 融资性担保公司标志
    , FIN_CUST_TYPE character(2) -- 金融客户类型代码
    , MICRO_LOAN_CORP_FLAG varchar(10) -- 小额贷款公司标志
    , CORP_FLAG varchar(10) -- 文创企业标志
    , SCI_TECH_CORP_FLAG varchar(10) -- 科技企业标志
    , FORGN_TRADE_CORP_FLAG varchar(10) -- 外贸企业标志
    , OPER_STATUS character(1) -- 经营状态代码
    , LEGAL_PROPTY_CLS character(1) -- 法律性质类标志
    , MEM_RELA_CLS character(1) -- 隶属关系类型代码
    , SOLVENCY_CAP_SRC_CLS character(1) -- 偿债资金来源类型代码
    , FIN_PLAT varchar(10) -- 融资平台标志
    , OPER_PLACE_OWNSHIP character(1) -- 经营场地所有权
    , OPER_PLACE_AREA decimal(28) -- 经营场地面积
    , STOCK_CD character(10) -- 股票代码
    , STOCK_NAME varchar(100) -- 股票名称
    , BUSS_TYPE varchar(30) -- 业务类型代码
    , SYSTEM_NO character(2) -- 归属系统编码
    , PROVE_FLG varchar(10) -- 自证声明标志
    , ORGAN_TYPE character(1) -- 机构类别代码
    , ORGAN_DWELLER_FLG character(1) -- 机构税收居民身份标志
    , UN_RESDNT_FLAG varchar(10) -- 非居民标志
    , CUST_CTRLER_NAME varchar(200) -- 客户控制人名称
    , CTRLER_SER_NO varchar(2) -- 控制人序号
    , TAXER_IDT_1 varchar(40) -- 纳税人识别号1
    , TAXER_IDT_2 varchar(40) -- 纳税人识别号2
    , TAXER_IDT_3 varchar(40) -- 纳税人识别号3
    , NO_TAX_IDT_SPEC_RSN_DESC varchar(100) -- 无纳税识别号具体原因描述
    , NO_TAX_IDT_RSN_CD varchar(1) -- 无纳税识别号原因代码
    , CREATED_TIME varchar(100) -- 记录创建时间
    , CREATED_BY_EMPLOYEE character(7) -- 创建员工号
    , CREATED_BRNO character(6) -- 创建机构号
    , CREATED_SYSTEM character(2) -- 创建记录的系统
    , CREATED_CHANNEL character(2) -- 创建记录的渠道
)
comment '对公客户基本信息临时表处理'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S07_LP_CUST_BASIC_INFO_04(
      CUST_IN_CD -- 客户内码
    , LP_ORG_NO -- 法人机构编号
    , CUST_NAME -- 客户名称
    , CUST_ABB -- 客户简称
    , CUST_EN_NAME -- 客户英文名称
    , CUST_MCLS_CD -- 客户大类代码
    , CUST_NATION_CD -- 国籍代码
    , CUST_STATUS_CD -- 客户状态代码
    , CORP_REG_CAP -- 注册资金
    , REG_CAP_MNTY_CD -- 注册资金币种代码
    , CRDT_CUST_FLAG -- 信贷客户标志
    , OPER_SCOPE -- 经营范围
    , REG_INDUS_CD -- 注册行业类型代码
    , CORP_SCALE_CD -- 企业规模代码
    , CORP_EMPLY_NUM -- 公司员工人数
    , CUST_OPEN_INCMAMT -- 营业收入金额
    , TOTAL_AST -- 资产总额
    , CRDT_CD -- 信用等级代码
    , FNC_GUAR_CORP -- 融资性担保公司标志
    , FIN_CUST_TYPE -- 金融客户类型代码
    , MICRO_LOAN_CORP_FLAG -- 小额贷款公司标志
    , CORP_FLAG -- 文创企业标志
    , SCI_TECH_CORP_FLAG -- 科技企业标志
    , FORGN_TRADE_CORP_FLAG -- 外贸企业标志
    , OPER_STATUS -- 经营状态代码
    , LEGAL_PROPTY_CLS -- 法律性质类标志
    , MEM_RELA_CLS -- 隶属关系类型代码
    , SOLVENCY_CAP_SRC_CLS -- 偿债资金来源类型代码
    , FIN_PLAT -- 融资平台标志
    , OPER_PLACE_OWNSHIP -- 经营场地所有权
    , OPER_PLACE_AREA -- 经营场地面积
    , STOCK_CD -- 股票代码
    , STOCK_NAME -- 股票名称
    , BUSS_TYPE -- 业务类型代码
    , SYSTEM_NO -- 归属系统编码
    , PROVE_FLG -- 自证声明标志
    , ORGAN_TYPE -- 机构类别代码
    , ORGAN_DWELLER_FLG -- 机构税收居民身份标志
    , UN_RESDNT_FLAG -- 非居民标志
    , CUST_CTRLER_NAME -- 客户控制人名称
    , CTRLER_SER_NO -- 控制人序号
    , TAXER_IDT_1 -- 纳税人识别号1
    , TAXER_IDT_2 -- 纳税人识别号2
    , TAXER_IDT_3 -- 纳税人识别号3
    , NO_TAX_IDT_SPEC_RSN_DESC -- 无纳税识别号具体原因描述
    , NO_TAX_IDT_RSN_CD -- 无纳税识别号原因代码
    , CREATED_TIME -- 记录创建时间
    , CREATED_BY_EMPLOYEE -- 创建员工号
    , CREATED_BRNO -- 创建机构号
    , CREATED_SYSTEM -- 创建记录的系统
    , CREATED_CHANNEL -- 创建记录的渠道
)
select
      T1.CUST_INCOD as CUST_IN_CD -- 客户内码
    , T2.BK_ID as LP_ORG_NO -- 行社编号
    , T1.CUST_NAME as CUST_NAME -- 客户名称
    , T1.CUST_SHRT_NM as CUST_ABB -- 客户简称
    , T1.CUST_EN_NM as CUST_EN_NAME -- 客户英文名称
    , T1.MCIP_CST_TP as CUST_MCLS_CD -- 客户大类
    , T1.CUST_NATION as CUST_NATION_CD -- 客户国籍代码
    , T1.CUST_STAT as CUST_STATUS_CD -- 客户状态代码
    , T1.REG_CAP as CORP_REG_CAP -- 企业注册资金
    , T2.REG_CCY_ID as REG_CAP_MNTY_CD -- 注册资金货币代码
    , T3.CUST_FLG as CRDT_CUST_FLAG -- 信贷客户标志
    , T1.OPER_SCOPE as OPER_SCOPE -- 经营范围
    , T1.REG_INDS as REG_INDUS_CD -- 行业代码
    , T2.ORG_SCALE as CORP_SCALE_CD -- 企业规模代码
    , T2.NBR_EMP as CORP_EMPLY_NUM -- 公司员工人数
    , T2.OPERATING_RECEIPT as CUST_OPEN_INCMAMT -- 客户营业收入金额
    , T2.ASSETS_GENERAL as TOTAL_AST -- 资产总额
    , T4.CRD_GRD as CRDT_CD -- 信用等级代码
    , T2.CREATED_TIME as FNC_GUAR_CORP -- 是否融资性担保公司
    , T2.CREATED_BY_EMPLOYEE as FIN_CUST_TYPE -- 金融客户类型
    , T2.CREATED_BRNO as MICRO_LOAN_CORP_FLAG -- 小额贷款公司标志
    , T2.CREATED_SYSTEM as CORP_FLAG -- 文创企业标志
    , T2.CREATED_CHANNEL as SCI_TECH_CORP_FLAG -- 科技企业标志
    , T2.CREATED_TIME as FORGN_TRADE_CORP_FLAG -- 外贸企业标志
    , T2.CREATED_BY_EMPLOYEE as OPER_STATUS -- 经营状态
    , T2.CREATED_BRNO as LEGAL_PROPTY_CLS -- 法律性质类
    , T2.CREATED_SYSTEM as MEM_RELA_CLS -- 隶属关系类
    , T2.CREATED_CHANNEL as SOLVENCY_CAP_SRC_CLS -- 偿债资金来源类
    , T2.CREATED_TIME as FIN_PLAT -- 是否融资平台
    , T2.CREATED_BY_EMPLOYEE as OPER_PLACE_OWNSHIP -- 经营场地所有权
    , T2.CREATED_BRNO as OPER_PLACE_AREA -- 经营场地面积
    , T2.CREATED_SYSTEM as STOCK_CD -- 股票代码
    , T2.CREATED_CHANNEL as STOCK_NAME -- 股票名称
    , T5.BUSS_TYPE as BUSS_TYPE -- 业务类型
    , T5.SYSTEM_NO as SYSTEM_NO -- 归属系统
    , T5.PROVE_FLG as PROVE_FLG -- 自证声明
    , T5.ORGAN_TYPE as ORGAN_TYPE -- 机构类别
    , T5.ORGAN_DWELLER_FLG as ORGAN_DWELLER_FLG -- 机构税收居民身份 

    , T6.DWELLER_FLG as UN_RESDNT_FLAG -- 非居民标志 1-仅为中国税收居民 2-仅为非居民 3-即是中国税收居民又是其他国家（地区）税收居民
    , T6.CUST_NAME as CUST_CTRLER_NAME -- 客户（控制人）名称
    , T6.ID as CTRLER_SER_NO -- 控制人序号
    , T6.TAX_IDENT_NO1 as TAXER_IDT_1 -- 纳税人识别号1
    , T6.TAX_IDENT_NO2 as TAXER_IDT_2 -- 纳税人识别号2
    , T6.TAX_IDENT_NO3 as TAXER_IDT_3 -- 纳税人识别号3
    , T6.REASON_DESC as NO_TAX_IDT_SPEC_RSN_DESC -- 无纳税识别号具体原因描述
    , T6.NO_TAX_IDENT_REASON as NO_TAX_IDT_RSN_CD -- 无纳税识别号原因码
    , T2.CREATED_TIME as CREATED_TIME -- 记录创建时间
    , T2.CREATED_BY_EMPLOYEE as CREATED_BY_EMPLOYEE -- 创建员工号
    , T2.CREATED_BRNO as CREATED_BRNO -- 创建机构号
    , T2.CREATED_SYSTEM as CREATED_SYSTEM -- 创建记录的系统
    , T2.CREATED_CHANNEL as CREATED_CHANNEL -- 创建记录的渠道
from
    ${ods_ecif_schema}.T2_EN_CUST_BASE_INFO as T1 -- 对公客户基本信息
    LEFT JOIN ${ods_ecif_schema}.T2_EN_CUST_DETAIL_INFO as T2 -- 对公客户详细信息
    on T1.CUST_INCOD=  T2.CUST_INCOD 
AND  T1.BK_ID = T2.BK_ID
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0'
AND VALID_FLAG = '1' --有效标志 
    LEFT JOIN ${ods_xdzx_schema}.PUB_CUST_BASE_INFO as T3 -- 客户公共基础信息表
    on T1.CUST_INCOD=  T3.CUST_ISN
AND T1.BK_ID = T3.BELONG_ORG
AND T3.PT_DT='${process_date}' 
AND T3.DELETED='0'
 
    LEFT JOIN ${ods_xdzx_schema}.EN_CUST_EXPD_ECIF as T4 -- 对公客户扩展信息
    on T1.CUST_INCOD=  T4.CUST_INCOD 
AND  T4.BEL_ORG=T1.BK_ID
AND T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN ${ods_ecif_schema}.T2_EN_UNDWELLER_FNC_ACCT_INFO as T5 -- 对公非居民金融账户信息表
    on T1.CUST_INCOD=  T5.CUST_INCOD 
AND T5.BK_ID=T1.BK_ID 
AND T5.PT_DT='${process_date}' 
AND T5.DELETED='0' 
    LEFT JOIN ${ods_ecif_schema}.T2_EN_UNDWELLER_MAGER_TAX_INFO as T6 -- 控制人涉税信息表
    on T1.CUST_INCOD=  T6.CUST_INCOD 
AND T6.BK_ID=T1.BK_ID 
AND T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
where 1=1 
AND T1.MCIP_CST_TP IN('2','3')
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第4组）==============
-- 法人客户基本信息聚合

insert into table ${session}.S07_LP_CUST_BASIC_INFO(
      CUST_IN_CD -- 客户内码
    , LP_ORG_NO -- 法人机构编号
    , CUST_NAME -- 客户名称
    , CUST_ABB -- 客户简称
    , CUST_EN_NAME -- 客户英文名称
    , CUST_MCLS_CD -- 客户大类代码
    , CUST_NATION_CD -- 国籍代码
    , MAIN_DOCTYP_CD -- 主证件类型代码
    , MAIN_DOC_NO -- 主证件号码
    , MAIN_DOC_MATU_DT -- 主证件到期日期
    , MAIN_DOC_ISSUE_ORG_CTRY_RGN_CD -- 主证件签发机关国家地区代码
    , CUST_STATUS_CD -- 客户状态代码
    , CORP_REG_CAP -- 注册资金
    , REG_CAP_MNTY_CD -- 注册资金币种代码
    , CRDT_CUST_FLAG -- 信贷客户标志
    , OPER_SCOPE -- 经营范围
    , NATION_TAX_REG_CERT_NO -- 国税登记证号码
    , LOCAL_TAX_REG_CERT_NO -- 地税登记证号码
    , ORG_CD -- 组织机构代码证号码
    , BIZ_LIC_NO -- 营业执照号码
    , BIZ_LIC_MATU_DT -- 营业执照到期日期
    , LEGAL_REP_CUST_IN_CD -- 法定代表人客户内码
    , LEGAL_REP_CUST_TYPE_NO -- 法定代表人客户类型代码
    , LEGAL_REP_NAME -- 法定代表人名称
    , LEGAL_REP_DOCTYP_CD -- 法定代表人证件类型代码
    , LEGAL_REP_DOC_NO -- 法定代表人证件号码
    , LEGAL_REP_DOC_ISSUE_DT -- 法定代表人证件签发日期
    , LEGAL_REP_DOC_MATU_DT -- 法定代表人证件到期日期
    , AUTH_PROC_BIZ_PSN_CUST_IN_CD -- 授权办理业务人客户内码
    , AUTH_PROC_BIZ_PSN_NAME -- 授权办理业务人名称
    , AUTH_PROC_BIZ_PSN_DOCTYP_CD -- 授权办理业务人证件类型代码
    , AUTH_PROC_BIZ_PSN_DOC_NO -- 授权办理业务人证件号码
    , REG_INDUS_CD -- 注册行业类型代码
    , CORP_SCALE_CD -- 企业规模代码
    , CORP_EMPLY_NUM -- 公司员工人数
    , CUST_OPEN_INCMAMT -- 营业收入金额
    , TOTAL_AST -- 资产总额
    , CRDT_CD -- 信用等级代码
    , CORP_OFFICE_ADDR -- 经营地址
    , OFFICE_ADDR_DIST -- 经营地址行政区划
    , OFFICE_ADDR_ZIP_CD -- 经营地址邮政编码
    , CORP_REG_ADDR_ -- 注册地址
    , REG_ADDR_DIST -- 注册地址行政区划
    , REG_ADDR_ZIP_CD -- 注册地址邮编
    , GENERAL_MGER_CUST_IN_CD -- 总经理客户内码
    , GENERAL_MGER_NAME -- 总经理名称
    , FIN_PRINC_CUST_IN_CD -- 财务负责人客户内码
    , FIN_PRINC_NAME -- 财务负责人名称
    , OTHER_CONT_CUST_IN_CD -- 其他联系人客户内码
    , OTHER_CONT_NAME -- 其他联系人名称
    , ACTL_OPER_PSN_CUST_IN_CD -- 实际经营人客户内码
    , ACTL_OPER_PSN_NAME -- 实际经营人名称
    , BOD_CHAIR_CUST_IN_CD -- 董事长客户内码
    , BOD_CHAIR_NAME -- 董事长名称
    , STOCKHOLDER_ISN -- 控股股东股东客户内码
    , INVEST_TYPE -- 控股股东出资方式
    , INVEST_CCY -- 控股股东出资币种
    , INVEST_AMOUNT -- 控股股东出资金额
    , INVEST_CAPITAL -- 控股股东出资占比
    , STOCK_HOLD_QUANTITY -- 控股股东持股数量
    , STOCK_HOLD_RATE -- 控股股东持股比例
    , HOLDING_SIGN -- 控股股东控股标志
    , ACTL_CTRLER_CUST_IN_CD -- 实际控制人客户内码
    , ACTL_CTRLER_NAME -- 实际控制人姓名
    , FNC_GUAR_CORP -- 融资性担保公司标志
    , FIN_CUST_TYPE -- 金融客户类型代码
    , MICRO_LOAN_CORP_FLAG -- 小额贷款公司标志
    , CORP_FLAG -- 文创企业标志
    , SCI_TECH_CORP_FLAG -- 科技企业标志
    , FORGN_TRADE_CORP_FLAG -- 外贸企业标志
    , OPER_STATUS -- 经营状态代码
    , LEGAL_PROPTY_CLS -- 法律性质类标志
    , MEM_RELA_CLS -- 隶属关系类型代码
    , SOLVENCY_CAP_SRC_CLS -- 偿债资金来源类型代码
    , FIN_PLAT -- 融资平台标志
    , OPER_PLACE_OWNSHIP -- 经营场地所有权
    , OPER_PLACE_AREA -- 经营场地面积
    , STOCK_CD -- 股票代码
    , STOCK_NAME -- 股票名称
    , BUSS_TYPE -- 业务类型代码
    , SYSTEM_NO -- 归属系统编码
    , PROVE_FLG -- 自证声明标志
    , ORGAN_TYPE -- 机构类别代码
    , ORGAN_DWELLER_FLG -- 机构税收居民身份标志
    , UN_RESDNT_FLAG -- 非居民标志
    , CUST_CTRLER_NAME -- 客户控制人名称
    , CTRLER_SER_NO -- 控制人序号
    , TAXER_IDT_1 -- 纳税人识别号1
    , TAXER_IDT_2 -- 纳税人识别号2
    , TAXER_IDT_3 -- 纳税人识别号3
    , NO_TAX_IDT_SPEC_RSN_DESC -- 无纳税识别号具体原因描述
    , NO_TAX_IDT_RSN_CD -- 无纳税识别号原因代码
    , CREATED_TIME -- 记录创建时间
    , CREATED_BY_EMPLOYEE -- 创建员工号
    , CREATED_BRNO -- 创建机构号
    , CREATED_SYSTEM -- 创建记录的系统
    , CREATED_CHANNEL -- 创建记录的渠道
    , PT_DT  -- 数据日期
)
select
      T1.CUST_IN_CD as CUST_IN_CD -- 客户内码
    , T1.LP_ORG_NO as LP_ORG_NO -- 行社编号
    , T1.CUST_NAME as CUST_NAME -- 客户名称
    , T1.CUST_ABB as CUST_ABB -- 客户简称
    , T1.CUST_EN_NAME as CUST_EN_NAME -- 客户英文名称
    , T1.CUST_MCLS_CD as CUST_MCLS_CD -- 客户大类
    , T1.CUST_NATION_CD as CUST_NATION_CD -- 客户国籍代码
    , T3.MAIN_DOCTYP_CD as MAIN_DOCTYP_CD -- 企业证件类型
    , T3.MAIN_DOC_NO as MAIN_DOC_NO -- 企业证件号码
    , T3.MAIN_DOC_MATU_DT as MAIN_DOC_MATU_DT -- 企业证件到期日期
    , T3.MAIN_DOC_ISSUE_ORG_CTRY_RGN_CD as MAIN_DOC_ISSUE_ORG_CTRY_RGN_CD -- 企业证件签发国家或地区
    , T1.CUST_STATUS_CD as CUST_STATUS_CD -- 客户状态代码
    , T1.CORP_REG_CAP as CORP_REG_CAP -- 企业注册资金
    , T1.REG_CAP_MNTY_CD as REG_CAP_MNTY_CD -- 注册资金货币代码
    , T1.CRDT_CUST_FLAG as CRDT_CUST_FLAG -- 信贷客户标志
    , T1.OPER_SCOPE as OPER_SCOPE -- 经营范围
    , T3.NATION_TAX_REG_CERT_NO as NATION_TAX_REG_CERT_NO -- 国税登记证号码
    , T3.LOCAL_TAX_REG_CERT_NO as LOCAL_TAX_REG_CERT_NO -- 地税登记证号码
    , T3.ORG_CD as ORG_CD -- 组织机构代码
    , T3.BIZ_LIC_NO as BIZ_LIC_NO -- 营业执照号
    , T3.BIZ_LIC_MATU_DT as BIZ_LIC_MATU_DT -- 营业执照到期日期
    , T2.LEGAL_REP_CUST_IN_CD as LEGAL_REP_CUST_IN_CD -- 法定代表人客户内码
    , T2.LEGAL_REP_CUST_TYPE_NO as LEGAL_REP_CUST_TYPE_NO -- 法定代表人客户类型编号
    , T2.LEGAL_REP_NAME as LEGAL_REP_NAME -- 法定代表人姓名
    , T2.LEGAL_REP_DOCTYP_CD as LEGAL_REP_DOCTYP_CD -- 法定代表人证件类型代码
    , T2.LEGAL_REP_DOC_NO as LEGAL_REP_DOC_NO -- 法定代表人证件号码
    , T2.LEGAL_REP_DOC_ISSUE_DT as LEGAL_REP_DOC_ISSUE_DT -- 法定代表人证件签发日期
    , T2.LEGAL_REP_DOC_MATU_DT as LEGAL_REP_DOC_MATU_DT -- 法定代表人证件到期日期
    , T2.AUTH_PROC_BIZ_PSN_CUST_IN_CD as AUTH_PROC_BIZ_PSN_CUST_IN_CD -- 授权办理业务人客户内码
    , T2.AUTH_PROC_BIZ_PSN_NAME as AUTH_PROC_BIZ_PSN_NAME -- 授权办理业务人姓名
    , T2.AUTH_PROC_BIZ_PSN_DOCTYP_CD as AUTH_PROC_BIZ_PSN_DOCTYP_CD -- 授权办理业务人证件类型代码
    , T2.AUTH_PROC_BIZ_PSN_DOC_NO as AUTH_PROC_BIZ_PSN_DOC_NO -- 授权办理业务人证件号码
    , T1.REG_INDUS_CD as REG_INDUS_CD -- 行业代码
    , T1.CORP_SCALE_CD as CORP_SCALE_CD -- 企业规模代码
    , T1.CORP_EMPLY_NUM as CORP_EMPLY_NUM -- 公司员工人数
    , T1.CUST_OPEN_INCMAMT as CUST_OPEN_INCMAMT -- 客户营业收入金额
    , T1.TOTAL_AST as TOTAL_AST -- 资产总额
    , T1.CRDT_CD as CRDT_CD -- 信用等级代码
    , T4.CORP_OFFICE_ADDR as CORP_OFFICE_ADDR -- 企业办公地址（是不是经营地址）
    , T4.OFFICE_ADDR_DIST as OFFICE_ADDR_DIST -- 办公地址行政区划
    , T4.OFFICE_ADDR_ZIP_CD as OFFICE_ADDR_ZIP_CD -- 办公地址邮编
    , T4.CORP_REG_ADDR_ as CORP_REG_ADDR_ -- 企业注册地址
    , T4.REG_ADDR_DIST as REG_ADDR_DIST -- 注册地址行政区划
    , T4.REG_ADDR_ZIP_CD as REG_ADDR_ZIP_CD -- 注册地址邮编
    , T2.GENERAL_MGER_CUST_IN_CD as GENERAL_MGER_CUST_IN_CD -- 总经理客户内码
    , T2.GENERAL_MGER_NAME as GENERAL_MGER_NAME -- 总经理姓名
    , T2.FIN_PRINC_CUST_IN_CD as FIN_PRINC_CUST_IN_CD -- 财务负责人客户内码
    , T2.FIN_PRINC_NAME as FIN_PRINC_NAME -- 财务负责人姓名
    , T2.OTHER_CONT_CUST_IN_CD as OTHER_CONT_CUST_IN_CD -- 其他联系人客户内码
    , T2.OTHER_CONT_NAME as OTHER_CONT_NAME -- 其他联系人姓名
    , T2.ACTL_OPER_PSN_CUST_IN_CD as ACTL_OPER_PSN_CUST_IN_CD -- 实际经营人客户内码
    , T2.ACTL_OPER_PSN_NAME as ACTL_OPER_PSN_NAME -- 实际经营人姓名
    , T2.BOD_CHAIR_CUST_IN_CD as BOD_CHAIR_CUST_IN_CD -- 董事长客户内码
    , T2.BOD_CHAIR_NAME as BOD_CHAIR_NAME -- 董事长姓名
    , T2.STOCKHOLDER_ISN as STOCKHOLDER_ISN -- 股东1内码
    , T2.INVEST_TYPE as INVEST_TYPE -- 出资方式
    , T2.INVEST_CCY as INVEST_CCY -- 出资币种
    , T2.INVEST_AMOUNT as INVEST_AMOUNT -- 出资金额
    , T2.INVEST_CAPITAL as INVEST_CAPITAL -- 出资占比
    , T2.STOCK_HOLD_QUANTITY as STOCK_HOLD_QUANTITY -- 持股数量
    , T2.STOCK_HOLD_RATE as STOCK_HOLD_RATE -- 持股比例
    , T2.HOLDING_SIGN as HOLDING_SIGN -- 控股标志
    , T2.ACTL_CTRLER_CUST_IN_CD as ACTL_CTRLER_CUST_IN_CD -- 实际控制人客户内码
    , T2.ACTL_CTRLER_NAME as ACTL_CTRLER_NAME -- 实际控制人姓名
    , T1.FNC_GUAR_CORP as FNC_GUAR_CORP -- 是否融资性担保公司
    , T1.FIN_CUST_TYPE as FIN_CUST_TYPE -- 金融客户类型
    , T1.MICRO_LOAN_CORP_FLAG as MICRO_LOAN_CORP_FLAG -- 小额贷款公司标志
    , T1.CORP_FLAG as CORP_FLAG -- 文创企业标志
    , T1.SCI_TECH_CORP_FLAG as SCI_TECH_CORP_FLAG -- 科技企业标志
    , T1.FORGN_TRADE_CORP_FLAG as FORGN_TRADE_CORP_FLAG -- 外贸企业标志
    , T1.OPER_STATUS as OPER_STATUS -- 经营状态
    , T1.LEGAL_PROPTY_CLS as LEGAL_PROPTY_CLS -- 法律性质类
    , T1.MEM_RELA_CLS as MEM_RELA_CLS -- 隶属关系类
    , T1.SOLVENCY_CAP_SRC_CLS as SOLVENCY_CAP_SRC_CLS -- 偿债资金来源类
    , T1.FIN_PLAT as FIN_PLAT -- 是否融资平台
    , T1.OPER_PLACE_OWNSHIP as OPER_PLACE_OWNSHIP -- 经营场地所有权
    , T1.OPER_PLACE_AREA as OPER_PLACE_AREA -- 经营场地面积
    , T1.STOCK_CD as STOCK_CD -- 股票代码
    , T1.STOCK_NAME as STOCK_NAME -- 股票名称
    , T1.BUSS_TYPE as BUSS_TYPE -- 业务类型
    , T1.SYSTEM_NO as SYSTEM_NO -- 归属系统
    , T1.PROVE_FLG as PROVE_FLG -- 自证声明
    , T1.ORGAN_TYPE as ORGAN_TYPE -- 机构类别
    , T1.ORGAN_DWELLER_FLG as ORGAN_DWELLER_FLG -- 机构税收居民身份 

    , T1.UN_RESDNT_FLAG as UN_RESDNT_FLAG -- 非居民标志 1-仅为中国税收居民 2-仅为非居民 3-即是中国税收居民又是其他国家（地区）税收居民
    , T1.CUST_CTRLER_NAME as CUST_CTRLER_NAME -- 客户（控制人）名称
    , T1.CTRLER_SER_NO as CTRLER_SER_NO -- 控制人序号
    , T1.TAXER_IDT_1 as TAXER_IDT_1 -- 纳税人识别号1
    , T1.TAXER_IDT_2 as TAXER_IDT_2 -- 纳税人识别号2
    , T1.TAXER_IDT_3 as TAXER_IDT_3 -- 纳税人识别号3
    , T1.NO_TAX_IDT_SPEC_RSN_DESC as NO_TAX_IDT_SPEC_RSN_DESC -- 无纳税识别号具体原因描述
    , T1.NO_TAX_IDT_RSN_CD as NO_TAX_IDT_RSN_CD -- 无纳税识别号原因码
    , T1.CREATED_TIME as CREATED_TIME -- 记录创建时间
    , T1.CREATED_BY_EMPLOYEE as CREATED_BY_EMPLOYEE -- 创建员工号
    , T1.CREATED_BRNO as CREATED_BRNO -- 创建机构号
    , T1.CREATED_SYSTEM as CREATED_SYSTEM -- 创建记录的系统
    , T1.CREATED_CHANNEL as CREATED_CHANNEL -- 创建记录的渠道
    , '${process_date}' as PT_DT  -- None
from
    TMP_S07_LP_CUST_BASIC_INFO_04 as T1 -- 对公客户基本信息
    LEFT JOIN TMP_S07_LP_CUST_BASIC_INFO_02 as T2 -- 对公客户地址信息
    on T1.CUST_INCOD = T3.CUST_INCOD 
    LEFT JOIN TMP_S07_LP_CUST_BASIC_INFO_01 as T3 -- 对公客户证件信息
    on T1.CUST_INCOD = T3.CUST_INCOD 
    LEFT JOIN TMP_S07_LP_CUST_BASIC_INFO_03 as T4 -- 对公重要关联人信息
    on T1.CUST_INCOD = T3.CUST_INCOD 
;

-- 删除所有临时表
drop table ${session}.TMP_S07_LP_CUST_BASIC_INFO_01;
drop table ${session}.TMP_S07_LP_CUST_BASIC_INFO_02;
drop table ${session}.TMP_S07_LP_CUST_BASIC_INFO_03;
drop table ${session}.TMP_S07_LP_CUST_BASIC_INFO_04;