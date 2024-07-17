-- 层次表名: 聚合层-法人不良贷款核销信息聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s03_corp_np_loan_wrtoff
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S03_CORP_NP_LOAN_WRTOFF
--     表中文名：法人不良贷款核销信息聚合表
--     创建日期：2023-12-25 00:00:00
--     主键字段：LOAN_CONT_NO, DUBIL_SER_NO
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：主要包含法人信息、法人不良贷款借据信息及其核销明细。其中法人信息包含企业规模、信用等级、行业、是否上市等信息不良贷款信息及处置明细包含贷款形态、担保方式、不良借据核销金额利息等相关信息。
--     更新记录：
--         2023-12-25 00:00:00 王穆军 新增映射文件信息
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S03_CORP_NP_LOAN_WRTOFF drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 不良日期临时表信息
drop table if exists ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_01;

create table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_01 (
      LOAN_CONT_NO varchar(16) -- 贷款合同编号
    , DUBIL_SER_NO varchar(3) -- 借据序号
    , LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT date -- 四级形态首次转不良日期
    , LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT date -- 四级形态最近一次转不良日期
    , TEN_LVL_MODAL_FIRST_TRAN_BAD_DT date -- 十级形态首次转不良日期
    , TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT date -- 十级形态最近一次转不良日期
)
comment '不良日期临时表信息'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_01(
      LOAN_CONT_NO -- 贷款合同编号
    , DUBIL_SER_NO -- 借据序号
    , LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
    , LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
    , TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
    , TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
)
select
      T1.STAACONO as LOAN_CONT_NO -- 贷款合同号
    , T1.STABSN03 as DUBIL_SER_NO -- 借据序号
    , T2.ADJUST_DATE as LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 调整日期
    , T3.ADJUST_DATE as LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 调整日期
    , T4.ADJUST_DATE as TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 调整日期
    , T5.ADJUST_DATE as TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 调整日期
from
    ${ods_core_schema}.ODS_CORE_BLFMMAST as T1 -- 普通贷款分户文件
    LEFT JOIN (
	SELECT 
	 P1.FCAACONO 	-- 合同编号
	,P1.FCABSN03 	-- 借据序号
	,CAST(TO_CHAR(P1.FCAVDATE) AS DATE) AS ADJUST_DATE,
	,P1.FCAGCLS4
	,P1.FCAHCLS4
	,P1.FCAACSNO
	,ROW_NUMBER()OVER(PARTITION BY  P1.FCAACONO,
		P1.FCABSN03
	ORDER BY 
		P1.FCAVDATE ASC) RN1 
	,ROW_NUMBER()OVER(PARTITION BY  P1.FCAACONO,
		P1.FCABSN03
	ORDER BY 
		P1.FCAVDATE DESC) RN2
FROM ${ods_core_schema}.ODS_CORE_BLFMRGFC P1 --贷款四级形态调整登记簿
WHERE 
	1=1
	AND P1.FCAGCLS4 = '1'	--调整前
	AND P1.FCAHCLS4 IN('2','3','4') --调整后
	AND P1.PT_DT='${process_date}' 	
	AND P1.DELETED='0'
	) as T2 -- 贷款四级形态调整登记簿
    on T1.STAACONO = T2.FCAACONO
AND T1.STABSN03 = T2.FCABSN03
AND T2.RN1 = 1 
    LEFT JOIN (
	SELECT 
	 P1.FCAACONO 	-- 合同编号
	,P1.FCABSN03 	-- 借据序号
	,CAST(TO_CHAR(P1.FCAVDATE) AS DATE) AS ADJUST_DATE,
	,P1.FCAGCLS4
	,P1.FCAHCLS4
	,P1.FCAACSNO
	,ROW_NUMBER()OVER(PARTITION BY  P1.FCAACONO,
		P1.FCABSN03
	ORDER BY 
		P1.FCAVDATE ASC) RN1 
	,ROW_NUMBER()OVER(PARTITION BY  P1.FCAACONO,
		P1.FCABSN03
	ORDER BY 
		P1.FCAVDATE DESC) RN2
FROM ${ods_core_schema}.ODS_CORE_BLFMRGFC P1 --贷款四级形态调整登记簿
WHERE 
	1=1
	AND P1.FCAGCLS4 = '1'	--调整前
	AND P1.FCAHCLS4 IN('2','3','4') --调整后
	AND P1.PT_DT='${process_date}' 	
	AND P1.DELETED='0'
	) as T3 -- 贷款四级形态调整登记簿
    on T1.STAACONO = T3.FCAACONO
AND T1.STABSN03 = T3.FCABSN03
AND T3.RN2 = 1 
    LEFT JOIN 
(
	SELECT 
	 P1.VCAACONO 	-- 合同编号
	,P1.VCABSN03 	-- 借据序号
	,CAST(TO_CHAR(P1.VCAVDATE) AS DATE) AS ADJUST_DATE,
	,P1.VCAICLS5
	,P1.VCAJCLS5
	,P1.VCAACSNO
	,ROW_NUMBER()OVER(PARTITION BY  P1.VCAACONO,
		P1.VCABSN03
	ORDER BY 
		P1.VCAVDATE ASC) RN1 
	,ROW_NUMBER()OVER(PARTITION BY  P1.VCAACONO,
		P1.VCABSN03
	ORDER BY 
		P1.VCAVDATE DESC) RN2
FROM ${ods_core_schema}.ODS_CORE_BLFMRGVC P1 --贷款五级形态调整登记簿
WHERE 
	1=1
	AND P1.VCAICLS5 IN('1','2','7','8','9','0')	--调整前
	AND P1.VCAJCLS5 IN('3','4','5','6') --调整后
	AND P1.PT_DT='${process_date}' 	
	AND P1.DELETED='0'
	) as T4 -- 贷款五级形态调整登记簿
    on T1.STAACONO = T4.VCAACONO
AND T1.STABSN03 = T4.VCABSN03
AND T4.RN1 = 1 
    LEFT JOIN 
(
	SELECT 
	 P1.VCAACONO 	-- 合同编号
	,P1.VCABSN03 	-- 借据序号
	,CAST(TO_CHAR(P1.VCAVDATE) AS DATE) AS ADJUST_DATE,
	,P1.VCAICLS5
	,P1.VCAJCLS5
	,P1.VCAACSNO
	,ROW_NUMBER()OVER(PARTITION BY  P1.VCAACONO,
		P1.VCABSN03
	ORDER BY 
		P1.VCAVDATE ASC) RN1 
	,ROW_NUMBER()OVER(PARTITION BY  P1.VCAACONO,
		P1.VCABSN03
	ORDER BY 
		P1.VCAVDATE DESC) RN2
FROM ${ods_core_schema}.ODS_CORE_BLFMRGVC P1 --贷款五级形态调整登记簿
WHERE 
	1=1
	AND P1.VCAICLS5 IN('1','2','7','8','9','0')	--调整前
	AND P1.VCAJCLS5 IN('3','4','5','6') --调整后
	AND P1.PT_DT='${process_date}' 	
	AND P1.DELETED='0'
	) as T5 -- 贷款五级形态调整登记簿
    on T1.STAACONO = T5.VCAACONO
AND T1.STABSN03 = T5.VCABSN03
AND T5.RN2 = 1 
where 1=1 
AND T1.STADCLS4 IN('2','3','4') OR T1.STAECLS5 IN('3','4','5','6')
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第2组）==============
-- 普通贷款分户临时表信息
drop table if exists ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_02;

create table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_02 (
      LOAN_CONT_NO varchar(16) -- 贷款合同编号
    , DUBIL_SER_NO varchar(3) -- 借据序号
    , WRTOFF_STATUS_CD varchar(1) -- 核销状态代码
    , CURR_CD varchar(3) -- 币种代码
    , BELONG_ORG_NO varchar(6) -- 归属机构编号
    , CUST_IN_CD varchar(11) -- 客户内码
    , CUST_NAME varchar(80) -- 客户名称
    , CUST_NO varchar(23) -- 客户号
    , BELONG_CUST_MGR_NO varchar(7) -- 归属客户经理编号
    , CORP_CRDT_CD varchar(1) -- 企业信用等级代码
    , CORP_SCALE_CD varchar(1) -- 企业规模代码
    , CORP_REG_INDUS_CD varchar(4) -- 企业注册行业代码
    , LIST_CORP_FLAG varchar(1) -- 上市公司标志
    , LOANPROD_CD varchar(5) -- 贷款产品代码
    , SPNSR_CD varchar(3) -- 担保方式代码
    , LOAN_SUBJ_CD varchar(8) -- 贷款科目代码
    , LOAN_DT date -- 贷款发放日期
    , LOAN_MATU_DT date -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD varchar(1) -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD varchar(1) -- 当前贷款十级形态代码
    , LOAN_TERM_TYPE_CD varchar(1) -- 贷款期限类型代码
    , LOAN_USAGE_NO varchar(3) -- 贷款用途编号
    , LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT date -- 四级形态首次转不良日期
    , LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT date -- 四级形态最近一次转不良日期
    , TEN_LVL_MODAL_FIRST_TRAN_BAD_DT date -- 十级形态首次转不良日期
    , TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT date -- 十级形态最近一次转不良日期
    , AUDIT_LOAN_MNY_PRIN_AMT decimal(15,2) -- 实核贷款本金金额
    , ON_BS_INT decimal(11,2) -- 实核表内利息
    , ON_BS_COMP_INT decimal(11,2) -- 实核表内复息
    , OFBS_INT decimal(11,2) -- 实核表外利息
    , OFBS_COMP_INT decimal(11,2) -- 实核表外复息
    , WRTOFF_DT date -- 核销日期
    , WRTOFF_RETRA_PRIN_AMT decimal(15,2) -- 核销收回本金金额
    , WRTOFF_RETRA_ON_BS_INT decimal(11,2) -- 核销收回表内利息
    , WRTOFF_RETRA_ON_BS_CINT decimal(11,2) -- 核销收回表内复利
    , WRTOFF_RETRA_OFBS_INT decimal(11,2) -- 核销收回表外利息
    , WRTOFF_RETRA_OFBS_CINT decimal(11,2) -- 核销收回表外复利
    , WRTOFF_AFT_NEW_STL_INT decimal(11,2) -- 核销后新结利息
    , RETRA_WRTOFF_AFT_INT decimal(11,2) -- 收回核销后利息
    , RETRA_TYPE_CD varchar(1) -- 收回类型代码
    , WRTOFF_RETRA_TELR_NO varchar(7) -- 核销收回柜员编号
    , WRTOFF_RETRA_DT date -- 核销收回日期
    , PT_DT  varchar(10) -- 数据日期
)
comment '普通贷款分户临时表信息'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_02(
      LOAN_CONT_NO -- 贷款合同编号
    , DUBIL_SER_NO -- 借据序号
    , WRTOFF_STATUS_CD -- 核销状态代码
    , CURR_CD -- 币种代码
    , BELONG_ORG_NO -- 归属机构编号
    , CUST_IN_CD -- 客户内码
    , CUST_NAME -- 客户名称
    , CUST_NO -- 客户号
    , BELONG_CUST_MGR_NO -- 归属客户经理编号
    , CORP_CRDT_CD -- 企业信用等级代码
    , CORP_SCALE_CD -- 企业规模代码
    , CORP_REG_INDUS_CD -- 企业注册行业代码
    , LIST_CORP_FLAG -- 上市公司标志
    , LOANPROD_CD -- 贷款产品代码
    , SPNSR_CD -- 担保方式代码
    , LOAN_SUBJ_CD -- 贷款科目代码
    , LOAN_DT -- 贷款发放日期
    , LOAN_MATU_DT -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
    , LOAN_TERM_TYPE_CD -- 贷款期限类型代码
    , LOAN_USAGE_NO -- 贷款用途编号
    , LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
    , LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
    , TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
    , TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
    , AUDIT_LOAN_MNY_PRIN_AMT -- 实核贷款本金金额
    , ON_BS_INT -- 实核表内利息
    , ON_BS_COMP_INT -- 实核表内复息
    , OFBS_INT -- 实核表外利息
    , OFBS_COMP_INT -- 实核表外复息
    , WRTOFF_DT -- 核销日期
    , WRTOFF_RETRA_PRIN_AMT -- 核销收回本金金额
    , WRTOFF_RETRA_ON_BS_INT -- 核销收回表内利息
    , WRTOFF_RETRA_ON_BS_CINT -- 核销收回表内复利
    , WRTOFF_RETRA_OFBS_INT -- 核销收回表外利息
    , WRTOFF_RETRA_OFBS_CINT -- 核销收回表外复利
    , WRTOFF_AFT_NEW_STL_INT -- 核销后新结利息
    , RETRA_WRTOFF_AFT_INT -- 收回核销后利息
    , RETRA_TYPE_CD -- 收回类型代码
    , WRTOFF_RETRA_TELR_NO -- 核销收回柜员编号
    , WRTOFF_RETRA_DT -- 核销收回日期
    , PT_DT  -- 数据日期
)
select
      T1.BLFMMAST as LOAN_CONT_NO -- 贷款合同号
    , T1.BLFMMAST as DUBIL_SER_NO -- 借据序号
    , SUBSTR(T1.STAAACWF,1,1) as WRTOFF_STATUS_CD -- 账户处理状态（第一位是核销状态）
    , T1.STAACCYC as CURR_CD -- 币种
    , T1.STAABRNO as BELONG_ORG_NO -- 机构号
    , T1.STAACSNO as CUST_IN_CD -- 客户内码
    , T1.STANFLNM as CUST_NAME -- 客户名称
    , T1.STABCSID as CUST_NO -- 客户号
    , T1.STAASTAF as BELONG_CUST_MGR_NO -- 信贷员
    , substr(T1.STABNO20,14,1)  as CORP_CRDT_CD -- 贷款属性码（第14位为信用等级代码）
    , T2.ORG_SCALE as CORP_SCALE_CD -- 企业规模
    , T3.REG_INDS as CORP_REG_INDUS_CD -- 注册行业
    , T2.LISTED_CO_FLG as LIST_CORP_FLAG -- 上市公司标志
    , T1.STAAPRNO as LOANPROD_CD -- 贷款产品代码
    , T4.SSTY as SPNSR_CD -- 担保方式
    , T1.STAAACID as LOAN_SUBJ_CD -- 科目代号
    , CAST(TO_CHAR(T1.STAIDATE)  AS DATE) as LOAN_DT -- 发放日期
    , CAST(TO_CHAR(T1.STACDATE) AS DATE) as LOAN_MATU_DT -- 到期日期
    , T1.STADCLS4 as CURR_LOAN_LVL_FOUR_MODAL_CD -- 贷款当前形态4级
    , T1.STAECLS5 as CURR_LOAN_TEN_LVL_MODAL_CD -- 贷款当前形态5级
    , substr(T1.STAANO24,1,1) as LOAN_TERM_TYPE_CD -- 信息编码（第1位为贷款期限代码）
    , substr(T1.STAANO24,11,3) as LOAN_USAGE_NO -- 信息编码（第11-13位为贷款用途编号）
    , T4.LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT as LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
    , T4.LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT as LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
    , T4.TEN_LVL_MODAL_FIRST_TRAN_BAD_DT as TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
    , T4.TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT as TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
    , T6.AFAEPRN as AUDIT_LOAN_MNY_PRIN_AMT -- 实核贷款本金
    , T6.AFBFIAM2 as ON_BS_INT -- 实核表内利息
    , T6.AFBRIAM2 as ON_BS_COMP_INT -- 实核表内复息
    , T6.AFBGIAM2 as OFBS_INT -- 实核表外利息
    , T6.AFBSIAM2 as OFBS_COMP_INT -- 实核表外复息
    , CAST(TO_CHAR(T6.AFBJDATE) AS DATE) as WRTOFF_DT -- 核销日期
    , T6.AFASAMT as WRTOFF_RETRA_PRIN_AMT -- 核销收回本金
    , T6.AFBIIAM2 as WRTOFF_RETRA_ON_BS_INT -- 核销收回表内利息
    , T6.AFBVIAM2 as WRTOFF_RETRA_ON_BS_CINT -- 核销收回表内复利
    , T6.AFBJIAM2 as WRTOFF_RETRA_OFBS_INT -- 核销收回表外利息
    , T6.AFBWIAM2 as WRTOFF_RETRA_OFBS_CINT -- 核销收回表外复利
    , T6.AFEIIAM2 as WRTOFF_AFT_NEW_STL_INT -- 核销后新结利息
    , T6.AFBXIAM2 as RETRA_WRTOFF_AFT_INT -- 收回核销后利息
    , T6.AFCGFLAG as RETRA_TYPE_CD -- 收回标志
    , T6.AFAQSTAF as WRTOFF_RETRA_TELR_NO -- 核销收回柜员
    , CAST(TO_CHAR(T6.AFBKDATE) AS DATE) as WRTOFF_RETRA_DT -- 核销收回日期
    , '${process_date}' as PT_DT  -- None
from
    ${ods_core_schema}.ODS_CORE_BLFMMAST as T1 -- 普通贷款分户文件
    LEFT JOIN ${ods_ecif_schema}.ODS_ECIF_T2_EN_CUST_DETAIL_INFO as T2 -- 对公客户详细信息
    on T1.STAACSNO = T2.CUST_INCOD
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN ${ods_ecif_schema}.ODS_ECIF_T2_EN_CUST_BASE_INFO as T3 -- 对公客户基本信息
    on T3.CUST_INCOD = T2.CUST_INCOD
AND T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMCONF as T4 -- 贷款合同文件
    on T4.NFAACONO=T1.STAACONO 
AND T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN TMP_S03_CORP_NP_LOAN_WRTOFF_01 as T5 -- 普通贷款分户临时表信息
    on T1.STAACONO = T5.STAACONO
AND T1.STABSN03 = T5.STABSN03 
    LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMCNAF as T6 -- 贷款核销文件
    on T6.AFAACONO=T1.STAACONO  
AND T6.AFABSN03=T1.STABSN03
AND T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
where 1=1 
AND T1.STADCLS4 IN('2','3','4') OR T1.STAECLS5 IN('3','4','5','6')
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第3组）==============
-- 不良日期临时表信息 贷款按揭数据组
drop table if exists ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_03;

create table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_03 (
      LOAN_CONT_NO varchar(16) -- 贷款合同编号
    , DUBIL_SER_NO varchar(3) -- 借据序号
    , LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT date -- 四级形态首次转不良日期
    , LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT date -- 四级形态最近一次转不良日期
    , TEN_LVL_MODAL_FIRST_TRAN_BAD_DT date -- 十级形态首次转不良日期
    , TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT date -- 十级形态最近一次转不良日期
)
comment '不良日期临时表信息 贷款按揭数据组'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_03(
      LOAN_CONT_NO -- 贷款合同编号
    , DUBIL_SER_NO -- 借据序号
    , LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
    , LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
    , TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
    , TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
)
select
      T1.STAACONO as LOAN_CONT_NO -- 贷款合同号
    , T1.STABSN03 as DUBIL_SER_NO -- 借据序号
    , T2.ADJUST_DATE as LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 调整日期
    , T3.ADJUST_DATE as LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 调整日期
    , T4.ADJUST_DATE as TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 调整日期
    , T5.ADJUST_DATE as TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 调整日期
from
    ${ods_core_schema}.ODS_CORE_BLFMAMTZ as T1 -- 按揭贷款分户文件
    LEFT JOIN (
	SELECT 
	 P1.FCAACONO 	-- 合同编号
	,P1.FCABSN03 	-- 借据序号
	,CAST(TO_CHAR(P1.FCAVDATE) AS DATE) AS ADJUST_DATE,
	,P1.FCAGCLS4
	,P1.FCAHCLS4
	,P1.FCAACSNO
	,ROW_NUMBER()OVER(PARTITION BY  P1.FCAACONO,
		P1.FCABSN03
	ORDER BY 
		P1.FCAVDATE ASC) RN1 
	,ROW_NUMBER()OVER(PARTITION BY  P1.FCAACONO,
		P1.FCABSN03
	ORDER BY 
		P1.FCAVDATE DESC) RN2
FROM ${ods_core_schema}.ODS_CORE_BLFMRGFC P1 --贷款四级形态调整登记簿
WHERE 
	1=1
	AND P1.FCAGCLS4 = '1'	--调整前
	AND P1.FCAHCLS4 IN('2','3','4') --调整后
	AND P1.PT_DT='${process_date}' 	
	AND P1.DELETED='0'
	) as T2 -- 贷款四级形态调整登记簿
    on T1.TZAACONO = T2.FCAACONO
AND T1.TZABSN03 = T2.FCABSN03
AND T2.RN1 = 1 
    LEFT JOIN (
	SELECT 
	 P1.FCAACONO 	-- 合同编号
	,P1.FCABSN03 	-- 借据序号
	,CAST(TO_CHAR(P1.FCAVDATE) AS DATE) AS ADJUST_DATE,
	,P1.FCAGCLS4
	,P1.FCAHCLS4
	,P1.FCAACSNO
	,ROW_NUMBER()OVER(PARTITION BY  P1.FCAACONO,
		P1.FCABSN03
	ORDER BY 
		P1.FCAVDATE ASC) RN1 
	,ROW_NUMBER()OVER(PARTITION BY  P1.FCAACONO,
		P1.FCABSN03
	ORDER BY 
		P1.FCAVDATE DESC) RN2
FROM ${ods_core_schema}.ODS_CORE_BLFMRGFC P1 --贷款四级形态调整登记簿
WHERE 
	1=1
	AND P1.FCAGCLS4 = '1'	--调整前
	AND P1.FCAHCLS4 IN('2','3','4') --调整后
	AND P1.PT_DT='${process_date}' 	
	AND P1.DELETED='0'
	) as T3 -- 贷款四级形态调整登记簿
    on T1.TZAACONO = T3.FCAACONO
AND T1.TZABSN03 = T3.FCABSN03
AND T3.RN2 = 1 
    LEFT JOIN 
(
	SELECT 
	 P1.VCAACONO 	-- 合同编号
	,P1.VCABSN03 	-- 借据序号
	,CAST(TO_CHAR(P1.VCAVDATE) AS DATE) AS ADJUST_DATE,
	,P1.VCAICLS5
	,P1.VCAJCLS5
	,P1.VCAACSNO
	,ROW_NUMBER()OVER(PARTITION BY  P1.VCAACONO,
		P1.VCABSN03
	ORDER BY 
		P1.VCAVDATE ASC) RN1 
	,ROW_NUMBER()OVER(PARTITION BY  P1.VCAACONO,
		P1.VCABSN03
	ORDER BY 
		P1.VCAVDATE DESC) RN2
FROM ${ods_core_schema}.ODS_CORE_BLFMRGVC P1 --贷款五级形态调整登记簿
WHERE 
	1=1
	AND P1.VCAICLS5 IN('1','2','7','8','9','0')	--调整前
	AND P1.VCAJCLS5 IN('3','4','5','6') --调整后
	AND P1.PT_DT='${process_date}' 	
	AND P1.DELETED='0'
	) as T4 -- 贷款五级形态调整登记簿
    on T1.TZAACONO = T4.VCAACONO
AND T1.TZABSN03 = T4.VCABSN03
AND T4.RN1 = 1 
    LEFT JOIN 
(
	SELECT 
	 P1.VCAACONO 	-- 合同编号
	,P1.VCABSN03 	-- 借据序号
	,CAST(TO_CHAR(P1.VCAVDATE) AS DATE) AS ADJUST_DATE,
	,P1.VCAICLS5
	,P1.VCAJCLS5
	,P1.VCAACSNO
	,ROW_NUMBER()OVER(PARTITION BY  P1.VCAACONO,
		P1.VCABSN03
	ORDER BY 
		P1.VCAVDATE ASC) RN1 
	,ROW_NUMBER()OVER(PARTITION BY  P1.VCAACONO,
		P1.VCABSN03
	ORDER BY 
		P1.VCAVDATE DESC) RN2
FROM ${ods_core_schema}.ODS_CORE_BLFMRGVC P1 --贷款五级形态调整登记簿
WHERE 
	1=1
	AND P1.VCAICLS5 IN('1','2','7','8','9','0')	--调整前
	AND P1.VCAJCLS5 IN('3','4','5','6') --调整后
	AND P1.PT_DT='${process_date}' 	
	AND P1.DELETED='0'
	) as T5 -- 贷款五级形态调整登记簿
    on T1.TZAACONO = T5.VCAACONO
AND T1.TZABSN03 = T5.VCABSN03
AND T5.RN2 = 1 
where 1=1 
AND T1.TZADCLS4 IN('2','3','4') OR T1.TZAECLS5 IN('3','4','5','6')
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第4组）==============
-- 按揭贷款分户临时表信息
drop table if exists ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_04;

create table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_04 (
      LOAN_CONT_NO varchar(16) -- 贷款合同编号
    , DUBIL_SER_NO varchar(3) -- 借据序号
    , WRTOFF_STATUS_CD varchar(1) -- 核销状态代码
    , CURR_CD varchar(3) -- 币种代码
    , BELONG_ORG_NO varchar(6) -- 归属机构编号
    , CUST_IN_CD varchar(11) -- 客户内码
    , CUST_NAME varchar(80) -- 客户名称
    , CUST_NO varchar(23) -- 客户号
    , BELONG_CUST_MGR_NO varchar(7) -- 归属客户经理编号
    , CORP_CRDT_CD varchar(1) -- 企业信用等级代码
    , CORP_SCALE_CD varchar(1) -- 企业规模代码
    , CORP_REG_INDUS_CD varchar(4) -- 企业注册行业代码
    , LIST_CORP_FLAG varchar(1) -- 上市公司标志
    , LOANPROD_CD varchar(5) -- 贷款产品代码
    , SPNSR_CD varchar(3) -- 担保方式代码
    , LOAN_SUBJ_CD varchar(8) -- 贷款科目代码
    , LOAN_DT date -- 贷款发放日期
    , LOAN_MATU_DT date -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD varchar(1) -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD varchar(1) -- 当前贷款十级形态代码
    , LOAN_TERM_TYPE_CD varchar(1) -- 贷款期限类型代码
    , LOAN_USAGE_NO varchar(3) -- 贷款用途编号
    , LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT date -- 四级形态首次转不良日期
    , LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT date -- 四级形态最近一次转不良日期
    , TEN_LVL_MODAL_FIRST_TRAN_BAD_DT date -- 十级形态首次转不良日期
    , TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT date -- 十级形态最近一次转不良日期
    , AUDIT_LOAN_MNY_PRIN_AMT decimal(15,2) -- 实核贷款本金金额
    , ON_BS_INT decimal(11,2) -- 实核表内利息
    , ON_BS_COMP_INT decimal(11,2) -- 实核表内复息
    , OFBS_INT decimal(11,2) -- 实核表外利息
    , OFBS_COMP_INT decimal(11,2) -- 实核表外复息
    , WRTOFF_DT date -- 核销日期
    , WRTOFF_RETRA_PRIN_AMT decimal(15,2) -- 核销收回本金金额
    , WRTOFF_RETRA_ON_BS_INT decimal(11,2) -- 核销收回表内利息
    , WRTOFF_RETRA_ON_BS_CINT decimal(11,2) -- 核销收回表内复利
    , WRTOFF_RETRA_OFBS_INT decimal(11,2) -- 核销收回表外利息
    , WRTOFF_RETRA_OFBS_CINT decimal(11,2) -- 核销收回表外复利
    , WRTOFF_AFT_NEW_STL_INT decimal(11,2) -- 核销后新结利息
    , RETRA_WRTOFF_AFT_INT decimal(11,2) -- 收回核销后利息
    , RETRA_TYPE_CD varchar(1) -- 收回类型代码
    , WRTOFF_RETRA_TELR_NO varchar(7) -- 核销收回柜员编号
    , WRTOFF_RETRA_DT date -- 核销收回日期
    , PT_DT  varchar(10) -- 数据日期
)
comment '按揭贷款分户临时表信息'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_04(
      LOAN_CONT_NO -- 贷款合同编号
    , DUBIL_SER_NO -- 借据序号
    , WRTOFF_STATUS_CD -- 核销状态代码
    , CURR_CD -- 币种代码
    , BELONG_ORG_NO -- 归属机构编号
    , CUST_IN_CD -- 客户内码
    , CUST_NAME -- 客户名称
    , CUST_NO -- 客户号
    , BELONG_CUST_MGR_NO -- 归属客户经理编号
    , CORP_CRDT_CD -- 企业信用等级代码
    , CORP_SCALE_CD -- 企业规模代码
    , CORP_REG_INDUS_CD -- 企业注册行业代码
    , LIST_CORP_FLAG -- 上市公司标志
    , LOANPROD_CD -- 贷款产品代码
    , SPNSR_CD -- 担保方式代码
    , LOAN_SUBJ_CD -- 贷款科目代码
    , LOAN_DT -- 贷款发放日期
    , LOAN_MATU_DT -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
    , LOAN_TERM_TYPE_CD -- 贷款期限类型代码
    , LOAN_USAGE_NO -- 贷款用途编号
    , LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
    , LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
    , TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
    , TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
    , AUDIT_LOAN_MNY_PRIN_AMT -- 实核贷款本金金额
    , ON_BS_INT -- 实核表内利息
    , ON_BS_COMP_INT -- 实核表内复息
    , OFBS_INT -- 实核表外利息
    , OFBS_COMP_INT -- 实核表外复息
    , WRTOFF_DT -- 核销日期
    , WRTOFF_RETRA_PRIN_AMT -- 核销收回本金金额
    , WRTOFF_RETRA_ON_BS_INT -- 核销收回表内利息
    , WRTOFF_RETRA_ON_BS_CINT -- 核销收回表内复利
    , WRTOFF_RETRA_OFBS_INT -- 核销收回表外利息
    , WRTOFF_RETRA_OFBS_CINT -- 核销收回表外复利
    , WRTOFF_AFT_NEW_STL_INT -- 核销后新结利息
    , RETRA_WRTOFF_AFT_INT -- 收回核销后利息
    , RETRA_TYPE_CD -- 收回类型代码
    , WRTOFF_RETRA_TELR_NO -- 核销收回柜员编号
    , WRTOFF_RETRA_DT -- 核销收回日期
    , PT_DT  -- 数据日期
)
select
      T1.TZAACONO as LOAN_CONT_NO -- 贷款合同号
    , T1.TZABSN03 as DUBIL_SER_NO -- 借据序号
    , SUBSTRING(T1.TZAAACWF,1,1) as WRTOFF_STATUS_CD -- 账户处理状态（第一位是核销状态）
    , T1.TZAACCYC as CURR_CD -- 币种
    , T1.TZAABRNO as BELONG_ORG_NO -- 机构号
    , T1.TZAACSNO as CUST_IN_CD -- 客户内码
    , T1.TZANFLNM as CUST_NAME -- 客户名称
    , T1.TZABCSID as CUST_NO -- 客户号
    , T1.TZAASTAF as BELONG_CUST_MGR_NO -- 信贷员
    , substr(T1.TZABNO20,14,1) as CORP_CRDT_CD -- 贷款属性码（第15位为信用等级代码）
    , T2.ORG_SCALE as CORP_SCALE_CD -- 企业规模
    , T3.REG_INDS as CORP_REG_INDUS_CD -- 注册行业
    , T2.LISTED_CO_FLG as LIST_CORP_FLAG -- 上市公司标志
    , T1.TZAAPRNO as LOANPROD_CD -- 贷款产品代码
    , T4.SSTY as SPNSR_CD -- 担保方式
    , T1.TZAAACID as LOAN_SUBJ_CD -- 科目代号
    , CAST(TO_CHAR(T1.TZAIDATE) AS DATE) as LOAN_DT -- 发放日期
    , CAST(TO_CHAR(T1.TZACDATE) AS DATE) as LOAN_MATU_DT -- 到期日期
    , T1.TZADCLS4 as CURR_LOAN_LVL_FOUR_MODAL_CD -- 贷款当前形态4级
    , T1.TZAECLS5 as CURR_LOAN_TEN_LVL_MODAL_CD -- 贷款当前形态5级
    , substr(TZAANO24,1,1) as LOAN_TERM_TYPE_CD -- 信息编码（第1位为贷款期限代码）
    , substr(TZAANO24,11,3) as LOAN_USAGE_NO -- 信息编码（第11-13位为贷款用途编号）
    , T4.LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT as LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
    , T4.LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT as LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
    , T4.TEN_LVL_MODAL_FIRST_TRAN_BAD_DT as TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
    , T4.TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT as TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
    , T6.AFAEPRN as AUDIT_LOAN_MNY_PRIN_AMT -- 实核贷款本金
    , T6.AFBFIAM2 as ON_BS_INT -- 实核表内利息
    , T6.AFBRIAM2 as ON_BS_COMP_INT -- 实核表内复息
    , T6.AFBGIAM2 as OFBS_INT -- 实核表外利息
    , T6.AFBSIAM2 as OFBS_COMP_INT -- 实核表外复息
    , CAST(TO_CHAR(T6.AFBJDATE ) AS DATE) as WRTOFF_DT -- 核销日期
    , T6.AFASAMT as WRTOFF_RETRA_PRIN_AMT -- 核销收回本金
    , T6.AFBIIAM2 as WRTOFF_RETRA_ON_BS_INT -- 核销收回表内利息
    , T6.AFBVIAM2 as WRTOFF_RETRA_ON_BS_CINT -- 核销收回表内复利
    , T6.AFBJIAM2 as WRTOFF_RETRA_OFBS_INT -- 核销收回表外利息
    , T6.AFBWIAM2 as WRTOFF_RETRA_OFBS_CINT -- 核销收回表外复利
    , T6.AFEIIAM2 as WRTOFF_AFT_NEW_STL_INT -- 核销后新结利息
    , T6.AFBXIAM2 as RETRA_WRTOFF_AFT_INT -- 收回核销后利息
    , T6.AFCGFLAG as RETRA_TYPE_CD -- 收回标志
    , T6.AFAQSTAF as WRTOFF_RETRA_TELR_NO -- 核销收回柜员
    , CAST(TO_CHAR(T6.AFBKDATE) AS DATE) as WRTOFF_RETRA_DT -- 核销收回日期
    , '${process_date}' as PT_DT  -- None
from
    ${ods_core_schema}.ODS_CORE_BLFMAMTZ as T1 -- 按揭贷款分户文件
    LEFT JOIN ${ods_ecif_schema}.T2_EN_CUST_DETAIL_INFO as T2 -- 对公客户详细信息
    on T1.TZAACSNO = T2.CUST_INCOD
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN ${ods_ecif_schema}.T2_EN_CUST_BASE_INFO as T3 -- 对公客户基本信息
    on T3.CUST_INCOD = T2.CUST_INCOD
AND T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMCONF as T4 -- 贷款合同文件
    on T4.NFAACONO=T1.TZAACONO 
AND T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN TMP_S03_CORP_NP_LOAN_WRTOFF_03 as T5 -- 普通贷款分户临时表信息
    on T1.TZAACONO = T5.TZAACONO
AND T1.TZABSN03 = T5.TZABSN03 
    LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMCNAF as T6 -- 贷款核销文件
    on T6.AFAACONO=T1.TZAACONO  
AND T6.AFABSN03=T1.TZABSN03
AND T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
where 1=1 
AND T1.TZADCLS4 IN('2','3','4') OR T1.TZAECLS5 IN('3','4','5','6')
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第5组）==============
-- 不良日期临时表信息贴现分户数据组
drop table if exists ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_05;

create table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_05 (
      LOAN_CONT_NO varchar(16) -- 贷款合同编号
    , DUBIL_SER_NO varchar(3) -- 借据序号
    , LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT date -- 四级形态首次转不良日期
    , LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT date -- 四级形态最近一次转不良日期
    , TEN_LVL_MODAL_FIRST_TRAN_BAD_DT date -- 十级形态首次转不良日期
    , TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT date -- 十级形态最近一次转不良日期
)
comment '不良日期临时表信息贴现分户数据组'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_05(
      LOAN_CONT_NO -- 贷款合同编号
    , DUBIL_SER_NO -- 借据序号
    , LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
    , LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
    , TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
    , TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
)
select
      T1.STAACONO as LOAN_CONT_NO -- 贷款合同号
    , T1.STABSN03 as DUBIL_SER_NO -- 借据序号
    , T2.ADJUST_DATE as LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 调整日期
    , T3.ADJUST_DATE as LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 调整日期
    , T4.ADJUST_DATE as TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 调整日期
    , T5.ADJUST_DATE as TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 调整日期
from
    ${ods_core_schema}.ODS_CORE_BLFMDCNT as T1 -- 贴现分户文件
    LEFT JOIN (
	SELECT 
	 P1.FCAACONO 	-- 合同编号
	,P1.FCABSN03 	-- 借据序号
	,CAST(TO_CHAR(P1.FCAVDATE) AS DATE) AS ADJUST_DATE,
	,P1.FCAGCLS4
	,P1.FCAHCLS4
	,P1.FCAACSNO
	,ROW_NUMBER()OVER(PARTITION BY  P1.FCAACONO,
		P1.FCABSN03
	ORDER BY 
		P1.FCAVDATE ASC) RN1 
	,ROW_NUMBER()OVER(PARTITION BY  P1.FCAACONO,
		P1.FCABSN03
	ORDER BY 
		P1.FCAVDATE DESC) RN2
FROM ${ods_core_schema}.ODS_CORE_BLFMRGFC P1 --贷款四级形态调整登记簿
WHERE 
	1=1
	AND P1.FCAGCLS4 = '1'	--调整前
	AND P1.FCAHCLS4 IN('2','3','4') --调整后
	AND P1.PT_DT='${process_date}' 	
	AND P1.DELETED='0'
	) as T2 -- 贷款四级形态调整登记簿
    on T1.NTAACONO = T2.FCAACONO
AND T1.NTABSN03 = T2.FCABSN03
AND T2.RN1 = 1 
    LEFT JOIN (
	SELECT 
	 P1.FCAACONO 	-- 合同编号
	,P1.FCABSN03 	-- 借据序号
	,CAST(TO_CHAR(P1.FCAVDATE) AS DATE) AS ADJUST_DATE,
	,P1.FCAGCLS4
	,P1.FCAHCLS4
	,P1.FCAACSNO
	,ROW_NUMBER()OVER(PARTITION BY  P1.FCAACONO,
		P1.FCABSN03
	ORDER BY 
		P1.FCAVDATE ASC) RN1 
	,ROW_NUMBER()OVER(PARTITION BY  P1.FCAACONO,
		P1.FCABSN03
	ORDER BY 
		P1.FCAVDATE DESC) RN2
FROM ${ods_core_schema}.ODS_CORE_BLFMRGFC P1 --贷款四级形态调整登记簿
WHERE 
	1=1
	AND P1.FCAGCLS4 = '1'	--调整前
	AND P1.FCAHCLS4 IN('2','3','4') --调整后
	AND P1.PT_DT='${process_date}' 	
	AND P1.DELETED='0'
	) as T3 -- 贷款四级形态调整登记簿
    on T1.NTAACONO = T3.FCAACONO
AND T1.NTABSN03 = T3.FCABSN03
AND T3.RN2 = 1 
    LEFT JOIN 
(
	SELECT 
	 P1.VCAACONO 	-- 合同编号
	,P1.VCABSN03 	-- 借据序号
	,CAST(TO_CHAR(P1.VCAVDATE) AS DATE) AS ADJUST_DATE,
	,P1.VCAICLS5
	,P1.VCAJCLS5
	,P1.VCAACSNO
	,ROW_NUMBER()OVER(PARTITION BY  P1.VCAACONO,
		P1.VCABSN03
	ORDER BY 
		P1.VCAVDATE ASC) RN1 
	,ROW_NUMBER()OVER(PARTITION BY  P1.VCAACONO,
		P1.VCABSN03
	ORDER BY 
		P1.VCAVDATE DESC) RN2
FROM ${ods_core_schema}.ODS_CORE_BLFMRGVC P1 --贷款五级形态调整登记簿
WHERE 
	1=1
	AND P1.VCAICLS5 IN('1','2','7','8','9','0')	--调整前
	AND P1.VCAJCLS5 IN('3','4','5','6') --调整后
	AND P1.PT_DT='${process_date}' 	
	AND P1.DELETED='0'
	) as T4 -- 贷款五级形态调整登记簿
    on T1.NTAACONO = T4.VCAACONO
AND T1.NTABSN03 = T4.VCABSN03
AND T4.RN1 = 1 
    LEFT JOIN 
(
	SELECT 
	 P1.VCAACONO 	-- 合同编号
	,P1.VCABSN03 	-- 借据序号
	,CAST(TO_CHAR(P1.VCAVDATE) AS DATE) AS ADJUST_DATE,
	,P1.VCAICLS5
	,P1.VCAJCLS5
	,P1.VCAACSNO
	,ROW_NUMBER()OVER(PARTITION BY  P1.VCAACONO,
		P1.VCABSN03
	ORDER BY 
		P1.VCAVDATE ASC) RN1 
	,ROW_NUMBER()OVER(PARTITION BY  P1.VCAACONO,
		P1.VCABSN03
	ORDER BY 
		P1.VCAVDATE DESC) RN2
FROM ${ods_core_schema}.ODS_CORE_BLFMRGVC P1 --贷款五级形态调整登记簿
WHERE 
	1=1
	AND P1.VCAICLS5 IN('1','2','7','8','9','0')	--调整前
	AND P1.VCAJCLS5 IN('3','4','5','6') --调整后
	AND P1.PT_DT='${process_date}' 	
	AND P1.DELETED='0'
	) as T5 -- 贷款五级形态调整登记簿
    on T1.NTAACONO = T5.VCAACONO
AND T1.NTABSN03 = T5.VCABSN03
AND T5.RN2 = 1 
where 1=1 
AND T1.NTAFCLS4 IN('2','3','4') OR T1.NTAECLS5 IN('3','4','5','6')
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第6组）==============
-- 贴现分户临时表信息
drop table if exists ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_06;

create table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_06 (
      LOAN_CONT_NO varchar(16) -- 贷款合同编号
    , DUBIL_SER_NO varchar(3) -- 借据序号
    , WRTOFF_STATUS_CD varchar(1) -- 核销状态代码
    , CURR_CD varchar(3) -- 币种代码
    , BELONG_ORG_NO varchar(6) -- 归属机构编号
    , CUST_IN_CD varchar(11) -- 客户内码
    , CUST_NAME varchar(80) -- 客户名称
    , CUST_NO varchar(23) -- 客户号
    , BELONG_CUST_MGR_NO varchar(7) -- 归属客户经理编号
    , CORP_CRDT_CD varchar(1) -- 企业信用等级代码
    , CORP_SCALE_CD varchar(1) -- 企业规模代码
    , CORP_REG_INDUS_CD varchar(4) -- 企业注册行业代码
    , LIST_CORP_FLAG varchar(1) -- 上市公司标志
    , LOANPROD_CD varchar(5) -- 贷款产品代码
    , SPNSR_CD varchar(3) -- 担保方式代码
    , LOAN_SUBJ_CD varchar(8) -- 贷款科目代码
    , LOAN_DT date -- 贷款发放日期
    , LOAN_MATU_DT date -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD varchar(1) -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD varchar(1) -- 当前贷款十级形态代码
    , LOAN_TERM_TYPE_CD varchar(1) -- 贷款期限类型代码
    , LOAN_USAGE_NO varchar(3) -- 贷款用途编号
    , LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT date -- 四级形态首次转不良日期
    , LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT date -- 四级形态最近一次转不良日期
    , TEN_LVL_MODAL_FIRST_TRAN_BAD_DT date -- 十级形态首次转不良日期
    , TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT date -- 十级形态最近一次转不良日期
    , AUDIT_LOAN_MNY_PRIN_AMT decimal(15,2) -- 实核贷款本金金额
    , ON_BS_INT decimal(11,2) -- 实核表内利息
    , ON_BS_COMP_INT decimal(11,2) -- 实核表内复息
    , OFBS_INT decimal(11,2) -- 实核表外利息
    , OFBS_COMP_INT decimal(11,2) -- 实核表外复息
    , WRTOFF_DT date -- 核销日期
    , WRTOFF_RETRA_PRIN_AMT decimal(15,2) -- 核销收回本金金额
    , WRTOFF_RETRA_ON_BS_INT decimal(11,2) -- 核销收回表内利息
    , WRTOFF_RETRA_ON_BS_CINT decimal(11,2) -- 核销收回表内复利
    , WRTOFF_RETRA_OFBS_INT decimal(11,2) -- 核销收回表外利息
    , WRTOFF_RETRA_OFBS_CINT decimal(11,2) -- 核销收回表外复利
    , WRTOFF_AFT_NEW_STL_INT decimal(11,2) -- 核销后新结利息
    , RETRA_WRTOFF_AFT_INT decimal(11,2) -- 收回核销后利息
    , RETRA_TYPE_CD varchar(1) -- 收回类型代码
    , WRTOFF_RETRA_TELR_NO varchar(7) -- 核销收回柜员编号
    , WRTOFF_RETRA_DT date -- 核销收回日期
    , PT_DT  varchar(10) -- 数据日期
)
comment '贴现分户临时表信息'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_06(
      LOAN_CONT_NO -- 贷款合同编号
    , DUBIL_SER_NO -- 借据序号
    , WRTOFF_STATUS_CD -- 核销状态代码
    , CURR_CD -- 币种代码
    , BELONG_ORG_NO -- 归属机构编号
    , CUST_IN_CD -- 客户内码
    , CUST_NAME -- 客户名称
    , CUST_NO -- 客户号
    , BELONG_CUST_MGR_NO -- 归属客户经理编号
    , CORP_CRDT_CD -- 企业信用等级代码
    , CORP_SCALE_CD -- 企业规模代码
    , CORP_REG_INDUS_CD -- 企业注册行业代码
    , LIST_CORP_FLAG -- 上市公司标志
    , LOANPROD_CD -- 贷款产品代码
    , SPNSR_CD -- 担保方式代码
    , LOAN_SUBJ_CD -- 贷款科目代码
    , LOAN_DT -- 贷款发放日期
    , LOAN_MATU_DT -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
    , LOAN_TERM_TYPE_CD -- 贷款期限类型代码
    , LOAN_USAGE_NO -- 贷款用途编号
    , LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
    , LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
    , TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
    , TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
    , AUDIT_LOAN_MNY_PRIN_AMT -- 实核贷款本金金额
    , ON_BS_INT -- 实核表内利息
    , ON_BS_COMP_INT -- 实核表内复息
    , OFBS_INT -- 实核表外利息
    , OFBS_COMP_INT -- 实核表外复息
    , WRTOFF_DT -- 核销日期
    , WRTOFF_RETRA_PRIN_AMT -- 核销收回本金金额
    , WRTOFF_RETRA_ON_BS_INT -- 核销收回表内利息
    , WRTOFF_RETRA_ON_BS_CINT -- 核销收回表内复利
    , WRTOFF_RETRA_OFBS_INT -- 核销收回表外利息
    , WRTOFF_RETRA_OFBS_CINT -- 核销收回表外复利
    , WRTOFF_AFT_NEW_STL_INT -- 核销后新结利息
    , RETRA_WRTOFF_AFT_INT -- 收回核销后利息
    , RETRA_TYPE_CD -- 收回类型代码
    , WRTOFF_RETRA_TELR_NO -- 核销收回柜员编号
    , WRTOFF_RETRA_DT -- 核销收回日期
    , PT_DT  -- 数据日期
)
select
      T1.NTAACONO as LOAN_CONT_NO -- 贷款合同号
    , T1.NTABSN03 as DUBIL_SER_NO -- 借据序号
    , SUBSTR(T1.NTAAACWF,1,1) as WRTOFF_STATUS_CD -- 账户处理状态（第一位是核销状态）
    , T1.NTAACCYC as CURR_CD -- 币种
    , T1.NTAABRNO as BELONG_ORG_NO -- 机构号
    , T1.NTAACSNO as CUST_IN_CD -- 客户内码
    , T1.NTANFLNM as CUST_NAME -- 客户名称
    , T1.NTABCSID as CUST_NO -- 客户号
    , T1.NTAASTAF as BELONG_CUST_MGR_NO -- 信贷员
    , substr(NTACNO20,14,1) as CORP_CRDT_CD -- 贷款属性码（第16位为信用等级代码）
    , T2.ORG_SCALE as CORP_SCALE_CD -- 企业规模
    , T3.REG_INDS as CORP_REG_INDUS_CD -- 注册行业
    , T2.LISTED_CO_FLG as LIST_CORP_FLAG -- 上市公司标志
    , T1.NTAAPRNO as LOANPROD_CD -- 贷款产品代码
    , T4.SSTY as SPNSR_CD -- 担保方式
    , T1.NTAAACID as LOAN_SUBJ_CD -- 科目代号
    , CAST(TO_CHAR(T1.NTASDATE) AS DATE) as LOAN_DT -- 贴现日期
    , CAST(TO_CHAR(T1.NTATDATE) AS DATE) as LOAN_MATU_DT -- 贴现到期日期
    , T1.NTAFCLS4 as CURR_LOAN_LVL_FOUR_MODAL_CD -- 贷款当前形态4级
    , T1.NTAHCLS5 as CURR_LOAN_TEN_LVL_MODAL_CD -- 贷款当前形态5级
    , substr(NTAANO24,1,1) as LOAN_TERM_TYPE_CD -- 信息编码（第1位为贷款期限代码）
    , substr(NTAANO24,11,3) as LOAN_USAGE_NO -- 信息编码（第11-13位为贷款用途编号）
    , T4.LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT as LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
    , T4.LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT as LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
    , T4.TEN_LVL_MODAL_FIRST_TRAN_BAD_DT as TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
    , T4.TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT as TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
    , T6.AFAEPRN as AUDIT_LOAN_MNY_PRIN_AMT -- 实核贷款本金
    , T6.AFBFIAM2 as ON_BS_INT -- 实核表内利息
    , T6.AFBRIAM2 as ON_BS_COMP_INT -- 实核表内复息
    , T6.AFBGIAM2 as OFBS_INT -- 实核表外利息
    , T6.AFBSIAM2 as OFBS_COMP_INT -- 实核表外复息
    , CAST(TO_CHAR(T6.AFBJDATE ) AS DATE) as WRTOFF_DT -- 核销日期
    , T6.AFASAMT as WRTOFF_RETRA_PRIN_AMT -- 核销收回本金
    , T6.AFBIIAM2 as WRTOFF_RETRA_ON_BS_INT -- 核销收回表内利息
    , T6.AFBVIAM2 as WRTOFF_RETRA_ON_BS_CINT -- 核销收回表内复利
    , T6.AFBJIAM2 as WRTOFF_RETRA_OFBS_INT -- 核销收回表外利息
    , T6.AFBWIAM2 as WRTOFF_RETRA_OFBS_CINT -- 核销收回表外复利
    , T6.AFEIIAM2 as WRTOFF_AFT_NEW_STL_INT -- 核销后新结利息
    , T6.AFBXIAM2 as RETRA_WRTOFF_AFT_INT -- 收回核销后利息
    , T6.AFCGFLAG as RETRA_TYPE_CD -- 收回标志
    , T6.AFAQSTAF as WRTOFF_RETRA_TELR_NO -- 核销收回柜员
    , CAST(TO_CHAR(T6.AFBKDATE) AS DATE) as WRTOFF_RETRA_DT -- 核销收回日期
    , '${process_date}' as PT_DT  -- None
from
    ${ods_core_schema}.ODS_CORE_BLFMAMTZ as T1 -- 按揭贷款分户文件
    LEFT JOIN ${ods_ecif_schema}.T2_EN_CUST_DETAIL_INFO as T2 -- 对公客户详细信息
    on T1.NTAACSNO = T2.CUST_INCOD
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN ${ods_ecif_schema}.T2_EN_CUST_BASE_INFO as T3 -- 对公客户基本信息
    on T3.CUST_INCOD = T2.CUST_INCOD
AND T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMCONF as T4 -- 贷款合同文件
    on T4.NFAACONO=T1.NTAACONO 
AND T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN TMP_S03_CORP_NP_LOAN_WRTOFF_03 as T5 -- 普通贷款分户临时表信息
    on T1.NTAACONO = T5.NTAACONO
AND T1.NTABSN03 = T5.NTABSN03 
    LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMCNAF as T6 -- 贷款核销文件
    on T6.AFAACONO=T1.NTAACONO  
AND T6.AFABSN03=T1.NTABSN03
AND T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
where 1=1 
AND T1.NTAFCLS4 IN('2','3','4') OR T1.NTAECLS5 IN('3','4','5','6')
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第7组）==============
-- 法人不良贷款核销信息聚合_汇总

insert into table ${session}.S03_CORP_NP_LOAN_WRTOFF(
      LOAN_CONT_NO -- 贷款合同编号
    , DUBIL_SER_NO -- 借据序号
    , WRTOFF_STATUS_CD -- 核销状态代码
    , CURR_CD -- 币种代码
    , BELONG_ORG_NO -- 归属机构编号
    , CUST_IN_CD -- 客户内码
    , CUST_NAME -- 客户名称
    , CUST_NO -- 客户号
    , BELONG_CUST_MGR_NO -- 归属客户经理编号
    , CORP_CRDT_CD -- 企业信用等级代码
    , CORP_SCALE_CD -- 企业规模代码
    , CORP_REG_INDUS_CD -- 企业注册行业代码
    , LIST_CORP_FLAG -- 上市公司标志
    , LOANPROD_CD -- 贷款产品代码
    , SPNSR_CD -- 担保方式代码
    , LOAN_SUBJ_CD -- 贷款科目代码
    , LOAN_DT -- 贷款发放日期
    , LOAN_MATU_DT -- 贷款到期日期
    , CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
    , CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
    , LOAN_TERM_TYPE_CD -- 贷款期限类型代码
    , LOAN_USAGE_NO -- 贷款用途编号
    , LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
    , LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
    , TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
    , TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
    , AUDIT_LOAN_MNY_PRIN_AMT -- 实核贷款本金金额
    , ON_BS_INT -- 实核表内利息
    , ON_BS_COMP_INT -- 实核表内复息
    , OFBS_INT -- 实核表外利息
    , OFBS_COMP_INT -- 实核表外复息
    , WRTOFF_DT -- 核销日期
    , WRTOFF_RETRA_PRIN_AMT -- 核销收回本金金额
    , WRTOFF_RETRA_ON_BS_INT -- 核销收回表内利息
    , WRTOFF_RETRA_ON_BS_CINT -- 核销收回表内复利
    , WRTOFF_RETRA_OFBS_INT -- 核销收回表外利息
    , WRTOFF_RETRA_OFBS_CINT -- 核销收回表外复利
    , WRTOFF_AFT_NEW_STL_INT -- 核销后新结利息
    , RETRA_WRTOFF_AFT_INT -- 收回核销后利息
    , RETRA_TYPE_CD -- 收回类型代码
    , WRTOFF_RETRA_TELR_NO -- 核销收回柜员编号
    , WRTOFF_RETRA_DT -- 核销收回日期
    , PT_DT  -- 数据日期
)
select
      T1.LOAN_CONT_NO as LOAN_CONT_NO -- 贷款合同编号
    , T1.DUBIL_SER_NO as DUBIL_SER_NO -- 借据序号
    , T1.WRTOFF_STATUS_CD as WRTOFF_STATUS_CD -- 核销状态代码
    , T1.CURR_CD as CURR_CD -- 币种代码
    , T1.BELONG_ORG_NO as BELONG_ORG_NO -- 归属机构编号
    , T1.CUST_IN_CD as CUST_IN_CD -- 客户内码
    , T1.CUST_NAME as CUST_NAME -- 客户名称
    , T1.CUST_NO as CUST_NO -- 客户号
    , T1.BELONG_CUST_MGR_NO as BELONG_CUST_MGR_NO -- 归属客户经理编号
    , T1.CORP_CRDT_CD as CORP_CRDT_CD -- 企业信用等级代码
    , T1.CORP_SCALE_CD as CORP_SCALE_CD -- 企业规模代码
    , T1.CORP_REG_INDUS_CD as CORP_REG_INDUS_CD -- 企业注册行业代码
    , T1.LIST_CORP_FLAG as LIST_CORP_FLAG -- 上市公司标志
    , T1.LOANPROD_CD as LOANPROD_CD -- 贷款产品代码
    , T1.SPNSR_CD as SPNSR_CD -- 担保方式代码
    , T1.LOAN_SUBJ_CD as LOAN_SUBJ_CD -- 贷款科目代码
    , T1.LOAN_DT as LOAN_DT -- 贷款发放日期
    , T1.LOAN_MATU_DT as LOAN_MATU_DT -- 贷款到期日期
    , T1.CURR_LOAN_LVL_FOUR_MODAL_CD as CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
    , T1.CURR_LOAN_TEN_LVL_MODAL_CD as CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
    , T1.LOAN_TERM_TYPE_CD as LOAN_TERM_TYPE_CD -- 贷款期限类型代码
    , T1.LOAN_USAGE_NO as LOAN_USAGE_NO -- 贷款用途编号
    , T1.LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT as LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
    , T1.LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT as LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
    , T1.TEN_LVL_MODAL_FIRST_TRAN_BAD_DT as TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
    , T1.TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT as TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
    , T1.AUDIT_LOAN_MNY_PRIN_AMT as AUDIT_LOAN_MNY_PRIN_AMT -- 实核贷款本金金额
    , T1.ON_BS_INT as ON_BS_INT -- 实核表内利息
    , T1.ON_BS_COMP_INT as ON_BS_COMP_INT -- 实核表内复息
    , T1.OFBS_INT as OFBS_INT -- 实核表外利息
    , T1.OFBS_COMP_INT as OFBS_COMP_INT -- 实核表外复息
    , T1.WRTOFF_DT as WRTOFF_DT -- 核销日期
    , T1.WRTOFF_RETRA_PRIN_AMT as WRTOFF_RETRA_PRIN_AMT -- 核销收回本金金额
    , T1.WRTOFF_RETRA_ON_BS_INT as WRTOFF_RETRA_ON_BS_INT -- 核销收回表内利息
    , T1.WRTOFF_RETRA_ON_BS_CINT as WRTOFF_RETRA_ON_BS_CINT -- 核销收回表内复利
    , T1.WRTOFF_RETRA_OFBS_INT as WRTOFF_RETRA_OFBS_INT -- 核销收回表外利息
    , T1.WRTOFF_RETRA_OFBS_CINT as WRTOFF_RETRA_OFBS_CINT -- 核销收回表外复利
    , T1.WRTOFF_AFT_NEW_STL_INT as WRTOFF_AFT_NEW_STL_INT -- 核销后新结利息
    , T1.RETRA_WRTOFF_AFT_INT as RETRA_WRTOFF_AFT_INT -- 收回核销后利息
    , T1.RETRA_TYPE_CD as RETRA_TYPE_CD -- 收回类型代码
    , T1.WRTOFF_RETRA_TELR_NO as WRTOFF_RETRA_TELR_NO -- 核销收回柜员编号
    , T1.WRTOFF_RETRA_DT as WRTOFF_RETRA_DT -- 核销收回日期
    , T1.PT_DT as PT_DT  -- None
from
    (
SELECT 
	P1.LOAN_CONT_NO                                -- 贷款合同编号
	P1.DUBIL_SER_NO                                -- 借据序号
	P1.WRTOFF_STATUS_CD                            -- 核销状态代码
	P1.CURR_CD                                     -- 币种代码
	P1.BELONG_ORG_NO                               -- 归属机构编号
	P1.CUST_IN_CD                                  -- 客户内码
	P1.CUST_NAME                                   -- 客户名称
	P1.CUST_NO                                     -- 客户号
	P1.BELONG_CUST_MGR_NO                          -- 归属客户经理编号
	P1.CORP_CRDT_CD                                -- 企业信用等级代码
	P1.CORP_SCALE_CD                               -- 企业规模代码
	P1.CORP_REG_INDUS_CD                           -- 企业注册行业代码
	P1.LIST_CORP_FLAG                              -- 上市公司标志
	P1.LOANPROD_CD                                 -- 贷款产品代码
	P1.SPNSR_CD                                    -- 担保方式代码
	P1.LOAN_SUBJ_CD                                -- 贷款科目代码
	P1.LOAN_DT                                     -- 贷款发放日期
	P1.LOAN_MATU_DT                                -- 贷款到期日期
	P1.CURR_LOAN_LVL_FOUR_MODAL_CD                 -- 当前贷款四级形态代码
	P1.CURR_LOAN_TEN_LVL_MODAL_CD                  -- 当前贷款十级形态代码
	P1.LOAN_TERM_TYPE_CD                           -- 贷款期限类型代码
	P1.LOAN_USAGE_NO                               -- 贷款用途编号
	P1.LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT            -- 四级形态首次转不良日期
	P1.LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT       -- 四级形态最近一次转不良日期
	P1.TEN_LVL_MODAL_FIRST_TRAN_BAD_DT             -- 十级形态首次转不良日期
	P1.TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT        -- 十级形态最近一次转不良日期
	P1.AUDIT_LOAN_MNY_PRIN_AMT                     -- 实核贷款本金金额
	P1.ON_BS_INT                                   -- 实核表内利息
	P1.ON_BS_COMP_INT                              -- 实核表内复息
	P1.OFBS_INT                                    -- 实核表外利息
	P1.OFBS_COMP_INT                               -- 实核表外复息
	P1.WRTOFF_DT                                   -- 核销日期
	P1.WRTOFF_RETRA_PRIN_AMT                       -- 核销收回本金金额
	P1.WRTOFF_RETRA_ON_BS_INT                      -- 核销收回表内利息
	P1.WRTOFF_RETRA_ON_BS_CINT                     -- 核销收回表内复利
	P1.WRTOFF_RETRA_OFBS_INT                       -- 核销收回表外利息
	P1.WRTOFF_RETRA_OFBS_CINT                      -- 核销收回表外复利
	P1.WRTOFF_AFT_NEW_STL_INT                      -- 核销后新结利息
	P1.RETRA_WRTOFF_AFT_INT                        -- 收回核销后利息
	P1.RETRA_TYPE_CD                               -- 收回类型代码
	P1.WRTOFF_RETRA_TELR_NO                        -- 核销收回柜员编号
	P1.WRTOFF_RETRA_DT                             -- 核销收回日期
	P1.PT_DT                                     -- 数据日期
FROM TMP_S03_CORP_NP_LOAN_WRTOFF_02 P1	--贷款
UNION ALL 
SELECT 
	P2.LOAN_CONT_NO                                -- 贷款合同编号
	P2.DUBIL_SER_NO                                -- 借据序号
	P2.WRTOFF_STATUS_CD                            -- 核销状态代码
	P2.CURR_CD                                     -- 币种代码
	P2.BELONG_ORG_NO                               -- 归属机构编号
	P2.CUST_IN_CD                                  -- 客户内码
	P2.CUST_NAME                                   -- 客户名称
	P2.CUST_NO                                     -- 客户号
	P2.BELONG_CUST_MGR_NO                          -- 归属客户经理编号
	P2.CORP_CRDT_CD                                -- 企业信用等级代码
	P2.CORP_SCALE_CD                               -- 企业规模代码
	P2.CORP_REG_INDUS_CD                           -- 企业注册行业代码
	P2.LIST_CORP_FLAG                              -- 上市公司标志
	P2.LOANPROD_CD                                 -- 贷款产品代码
	P2.SPNSR_CD                                    -- 担保方式代码
	P2.LOAN_SUBJ_CD                                -- 贷款科目代码
	P2.LOAN_DT                                     -- 贷款发放日期
	P2.LOAN_MATU_DT                                -- 贷款到期日期
	P2.CURR_LOAN_LVL_FOUR_MODAL_CD                 -- 当前贷款四级形态代码
	P2.CURR_LOAN_TEN_LVL_MODAL_CD                  -- 当前贷款十级形态代码
	P2.LOAN_TERM_TYPE_CD                           -- 贷款期限类型代码
	P2.LOAN_USAGE_NO                               -- 贷款用途编号
	P2.LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT            -- 四级形态首次转不良日期
	P2.LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT       -- 四级形态最近一次转不良日期
	P2.TEN_LVL_MODAL_FIRST_TRAN_BAD_DT             -- 十级形态首次转不良日期
	P2.TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT        -- 十级形态最近一次转不良日期
	P2.AUDIT_LOAN_MNY_PRIN_AMT                     -- 实核贷款本金金额
	P2.ON_BS_INT                                   -- 实核表内利息
	P2.ON_BS_COMP_INT                              -- 实核表内复息
	P2.OFBS_INT                                    -- 实核表外利息
	P2.OFBS_COMP_INT                               -- 实核表外复息
	P2.WRTOFF_DT                                   -- 核销日期
	P2.WRTOFF_RETRA_PRIN_AMT                       -- 核销收回本金金额
	P2.WRTOFF_RETRA_ON_BS_INT                      -- 核销收回表内利息
	P2.WRTOFF_RETRA_ON_BS_CINT                     -- 核销收回表内复利
	P2.WRTOFF_RETRA_OFBS_INT                       -- 核销收回表外利息
	P2.WRTOFF_RETRA_OFBS_CINT                      -- 核销收回表外复利
	P2.WRTOFF_AFT_NEW_STL_INT                      -- 核销后新结利息
	P2.RETRA_WRTOFF_AFT_INT                        -- 收回核销后利息
	P2.RETRA_TYPE_CD                               -- 收回类型代码
	P2.WRTOFF_RETRA_TELR_NO                        -- 核销收回柜员编号
	P2.WRTOFF_RETRA_DT                             -- 核销收回日期
	P2.PT_DT                                     -- 数据日期
FROM TMP_S03_CORP_NP_LOAN_WRTOFF_04 P2	--按揭
UNION ALL 
SELECT 
	P3.LOAN_CONT_NO                                 -- 贷款合同编号
	P3.DUBIL_SER_NO                                 -- 借据序号
	P3.WRTOFF_STATUS_CD                             -- 核销状态代码
	P3.CURR_CD                                      -- 币种代码
	P3.BELONG_ORG_NO                                -- 归属机构编号
	P3.CUST_IN_CD                                   -- 客户内码
	P3.CUST_NAME                                    -- 客户名称
	P3.CUST_NO                                      -- 客户号
	P3.BELONG_CUST_MGR_NO                           -- 归属客户经理编号
	P3.CORP_CRDT_CD                                 -- 企业信用等级代码
	P3.CORP_SCALE_CD                                -- 企业规模代码
	P3.CORP_REG_INDUS_CD                            -- 企业注册行业代码
	P3.LIST_CORP_FLAG                               -- 上市公司标志
	P3.LOANPROD_CD                                  -- 贷款产品代码
	P3.SPNSR_CD                                     -- 担保方式代码
	P3.LOAN_SUBJ_CD                                 -- 贷款科目代码
	P3.LOAN_DT                                      -- 贷款发放日期
	P3.LOAN_MATU_DT                                 -- 贷款到期日期
	P3.CURR_LOAN_LVL_FOUR_MODAL_CD                  -- 当前贷款四级形态代码
	P3.CURR_LOAN_TEN_LVL_MODAL_CD                   -- 当前贷款十级形态代码
	P3.LOAN_TERM_TYPE_CD                            -- 贷款期限类型代码
	P3.LOAN_USAGE_NO                                -- 贷款用途编号
	P3.LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT             -- 四级形态首次转不良日期
	P3.LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT        -- 四级形态最近一次转不良日期
	P3.TEN_LVL_MODAL_FIRST_TRAN_BAD_DT              -- 十级形态首次转不良日期
	P3.TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT         -- 十级形态最近一次转不良日期
	P3.AUDIT_LOAN_MNY_PRIN_AMT                      -- 实核贷款本金金额
	P3.ON_BS_INT                                    -- 实核表内利息
	P3.ON_BS_COMP_INT                               -- 实核表内复息
	P3.OFBS_INT                                     -- 实核表外利息
	P3.OFBS_COMP_INT                                -- 实核表外复息
	P3.WRTOFF_DT                                    -- 核销日期
	P3.WRTOFF_RETRA_PRIN_AMT                        -- 核销收回本金金额
	P3.WRTOFF_RETRA_ON_BS_INT                       -- 核销收回表内利息
	P3.WRTOFF_RETRA_ON_BS_CINT                      -- 核销收回表内复利
	P3.WRTOFF_RETRA_OFBS_INT                        -- 核销收回表外利息
	P3.WRTOFF_RETRA_OFBS_CINT                       -- 核销收回表外复利
	P3.WRTOFF_AFT_NEW_STL_INT                       -- 核销后新结利息
	P3.RETRA_WRTOFF_AFT_INT                         -- 收回核销后利息
	P3.RETRA_TYPE_CD                                -- 收回类型代码
	P3.WRTOFF_RETRA_TELR_NO                         -- 核销收回柜员编号
	P3.WRTOFF_RETRA_DT                              -- 核销收回日期
	P3.PT_DT                                      -- 数据日期
FROM TMP_S03_CORP_NP_LOAN_WRTOFF_04 P3 -- 贴现
) as T1 -- 法人不良贷款核销信息聚合_汇总
;

-- 删除所有临时表
drop table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_01;
drop table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_02;
drop table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_03;
drop table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_04;
drop table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_05;
drop table ${session}.TMP_S03_CORP_NP_LOAN_WRTOFF_06;