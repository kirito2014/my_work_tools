/*
*********************************************************************** 
Purpose:       主题聚合层-加工快照表脚本
Author:        Sunline
Usage:         python $ETL_HOME/script/main.py yyyymmdd ${icl_schema}_s03_corp_np_loan_wrtoff
CreateDate:    2023-12-25 00:00:00
FileType:      DML
logs:
       表英文名：S03_CORP_NP_LOAN_WRTOFF
       表中文名：法人不良贷款核销信息聚合表
       创建日期：2023-12-25 00:00:00
       主键字段：LOAN_CONT_NO、DUBIL_SER_NO
       归属层次：聚合层
       归属主题：信贷
       主要应用：公司贷款、不良贷款、核销
       分析人员：王穆军
       时间粒度：日
       保留周期：13M
       描述信息：主要包含法人信息、法人不良贷款借据信息及其核销明细。其中法人信息包含企业规模、信用等级、行业、是否上市等信息；不良贷款信息及处置明细包含贷款形态、担保方式、不良借据核销金额利息等相关信息。
*************************************************************************/ 

\timing 
/*创建当日分区*/
   call ${itl_schema}.partition_add('${icl_schema}.S03_CORP_NP_LOAN_WRTOFF','pt_${batch_date}','${batch_date}'); 

/*删除当前批次历史数据*/
   call ${itl_schema}.partition_drop('${icl_schema}.S03_CORP_NP_LOAN_WRTOFF','pt_${batch_date}'); 



/*===================第1组====================*/

DROP TABLE IF EXISTS ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_01;
CREATE GLOBAL TEMPORARY TABLE ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_01 (
 LOAN_CONT_NO  VARCHAR(16) -- 贷款合同编号
  ,DUBIL_SER_NO  VARCHAR(3) -- 借据序号
  ,LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT  DATE -- 四级形态首次转不良日期
  ,LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT  DATE -- 四级形态最近一次转不良日期
  ,TEN_LVL_MODAL_FIRST_TRAN_BAD_DT  DATE -- 十级形态首次转不良日期
  ,TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT  DATE -- 十级形态最近一次转不良日期
)
compress(5,5)
DISTRIBUTED BY ( LOAN_CONT_NO );

INSERT INTO ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_01(
  LOAN_CONT_NO -- 贷款合同编号
  ,DUBIL_SER_NO -- 借据序号
  ,LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
  ,LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
  ,TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
  ,TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
)
SELECT
  T1.STAACONO AS STAACONO -- 贷款合同号
  ,T1.STABSN03 AS STABSN03 -- 借据序号
  ,T2.ADJUST_DATE AS FCAVDATE -- 调整日期
  ,T3.ADJUST_DATE AS FCAVDATE -- 调整日期
  ,T4.ADJUST_DATE AS VCAVDATE -- 调整日期
  ,T5.ADJUST_DATE AS VCAVDATE -- 调整日期
 FROM ${ods_core_schema}.ODS_CORE_BLFMMAST  T1 -- 普通贷款分户文件 
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
	AND P1.FCAHCLS4 IN ('2','3','4') --调整后
	AND P1.PT_DT='${batch_date}' 	
	AND P1.DELETED='0'
	)  T2 -- 贷款四级形态调整登记簿 
 ON T1.STAACONO = T2.FCAACONO
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
	AND P1.FCAHCLS4 IN ('2','3','4') --调整后
	AND P1.PT_DT='${batch_date}' 	
	AND P1.DELETED='0'
	)  T3 -- 贷款四级形态调整登记簿 
 ON T1.STAACONO = T3.FCAACONO
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
	AND P1.VCAICLS5 IN ('1','2','7','8','9','0')	--调整前
	AND P1.VCAJCLS5 IN ('3','4','5','6') --调整后
	AND P1.PT_DT='${batch_date}' 	
	AND P1.DELETED='0'
	)  T4 -- 贷款五级形态调整登记簿 
 ON T1.STAACONO = T4.VCAACONO
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
	AND P1.VCAICLS5 IN ('1','2','7','8','9','0')	--调整前
	AND P1.VCAJCLS5 IN ('3','4','5','6') --调整后
	AND P1.PT_DT='${batch_date}' 	
	AND P1.DELETED='0'
	)  T5 -- 贷款五级形态调整登记簿 
 ON T1.STAACONO = T5.VCAACONO
AND T1.STABSN03 = T5.VCABSN03
AND T5.RN2 = 1
 WHERE 1=1 
AND T1.STADCLS4 IN ('2','3','4') OR T1.STAECLS5 IN ('3','4','5','6')
AND T1.PT_DT='${batch_date}' 
AND T1.DELETED='0'
 
;


/*添加表分析*/ 
ANALYZE TABLE ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_01;


/*===================第2组====================*/

DROP TABLE IF EXISTS ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_02;
CREATE GLOBAL TEMPORARY TABLE ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_02 (
 LOAN_CONT_NO  VARCHAR(16) -- 贷款合同编号
  ,DUBIL_SER_NO  VARCHAR(3) -- 借据序号
  ,WRTOFF_STATUS_CD  VARCHAR(1) -- 核销状态代码
  ,CURR_CD  VARCHAR(3) -- 币种代码
  ,BELONG_ORG_NO  VARCHAR(6) -- 归属机构编号
  ,CUST_IN_CD  VARCHAR(11) -- 客户内码
  ,CUST_NAME  VARCHAR(80) -- 客户名称
  ,CUST_NO  VARCHAR(23) -- 客户号
  ,BELONG_CUST_MGR_NO  VARCHAR(7) -- 归属客户经理编号
  ,CORP_CRDT_CD  VARCHAR(1) -- 企业信用等级代码
  ,CORP_SCALE_CD  VARCHAR(1) -- 企业规模代码
  ,CORP_REG_INDUS_CD  VARCHAR(4) -- 企业注册行业代码
  ,LIST_CORP_FLAG  VARCHAR(1) -- 上市公司标志
  ,LOANPROD_CD  VARCHAR(5) -- 贷款产品代码
  ,SPNSR_CD  VARCHAR(3) -- 担保方式代码
  ,LOAN_SUBJ_CD  VARCHAR(8) -- 贷款科目代码
  ,LOAN_DT  DATE -- 贷款发放日期
  ,LOAN_MATU_DT  DATE -- 贷款到期日期
  ,CURR_LOAN_LVL_FOUR_MODAL_CD  VARCHAR(1) -- 当前贷款四级形态代码
  ,CURR_LOAN_TEN_LVL_MODAL_CD  VARCHAR(1) -- 当前贷款十级形态代码
  ,LOAN_TERM_TYPE_CD  VARCHAR(1) -- 贷款期限类型代码
  ,LOAN_USAGE_NO  VARCHAR(3) -- 贷款用途编号
  ,LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT  DATE -- 四级形态首次转不良日期
  ,LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT  DATE -- 四级形态最近一次转不良日期
  ,TEN_LVL_MODAL_FIRST_TRAN_BAD_DT  DATE -- 十级形态首次转不良日期
  ,TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT  DATE -- 十级形态最近一次转不良日期
  ,AUDIT_LOAN_MNY_PRIN_AMT  DECIMAL(15,2) -- 实核贷款本金金额
  ,ON_BS_INT  DECIMAL(11,2) -- 实核表内利息
  ,ON_BS_COMP_INT  DECIMAL(11,2) -- 实核表内复息
  ,OFBS_INT  DECIMAL(11,2) -- 实核表外利息
  ,OFBS_COMP_INT  DECIMAL(11,2) -- 实核表外复息
  ,WRTOFF_DT  DATE -- 核销日期
  ,WRTOFF_RETRA_PRIN_AMT  DECIMAL(15,2) -- 核销收回本金金额
  ,WRTOFF_RETRA_ON_BS_INT  DECIMAL(11,2) -- 核销收回表内利息
  ,WRTOFF_RETRA_ON_BS_CINT  DECIMAL(11,2) -- 核销收回表内复利
  ,WRTOFF_RETRA_OFBS_INT  DECIMAL(11,2) -- 核销收回表外利息
  ,WRTOFF_RETRA_OFBS_CINT  DECIMAL(11,2) -- 核销收回表外复利
  ,WRTOFF_AFT_NEW_STL_INT  DECIMAL(11,2) -- 核销后新结利息
  ,RETRA_WRTOFF_AFT_INT  DECIMAL(11,2) -- 收回核销后利息
  ,RETRA_TYPE_CD  VARCHAR(1) -- 收回类型代码
  ,WRTOFF_RETRA_TELR_NO  VARCHAR(7) -- 核销收回柜员编号
  ,WRTOFF_RETRA_DT  DATE -- 核销收回日期
  ,DATA_DT  DATE -- 数据日期
)
compress(5,5)
DISTRIBUTED BY ( LOAN_CONT_NO );

INSERT INTO ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_02(
  LOAN_CONT_NO -- 贷款合同编号
  ,DUBIL_SER_NO -- 借据序号
  ,WRTOFF_STATUS_CD -- 核销状态代码
  ,CURR_CD -- 币种代码
  ,BELONG_ORG_NO -- 归属机构编号
  ,CUST_IN_CD -- 客户内码
  ,CUST_NAME -- 客户名称
  ,CUST_NO -- 客户号
  ,BELONG_CUST_MGR_NO -- 归属客户经理编号
  ,CORP_CRDT_CD -- 企业信用等级代码
  ,CORP_SCALE_CD -- 企业规模代码
  ,CORP_REG_INDUS_CD -- 企业注册行业代码
  ,LIST_CORP_FLAG -- 上市公司标志
  ,LOANPROD_CD -- 贷款产品代码
  ,SPNSR_CD -- 担保方式代码
  ,LOAN_SUBJ_CD -- 贷款科目代码
  ,LOAN_DT -- 贷款发放日期
  ,LOAN_MATU_DT -- 贷款到期日期
  ,CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
  ,CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
  ,LOAN_TERM_TYPE_CD -- 贷款期限类型代码
  ,LOAN_USAGE_NO -- 贷款用途编号
  ,LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
  ,LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
  ,TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
  ,TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
  ,AUDIT_LOAN_MNY_PRIN_AMT -- 实核贷款本金金额
  ,ON_BS_INT -- 实核表内利息
  ,ON_BS_COMP_INT -- 实核表内复息
  ,OFBS_INT -- 实核表外利息
  ,OFBS_COMP_INT -- 实核表外复息
  ,WRTOFF_DT -- 核销日期
  ,WRTOFF_RETRA_PRIN_AMT -- 核销收回本金金额
  ,WRTOFF_RETRA_ON_BS_INT -- 核销收回表内利息
  ,WRTOFF_RETRA_ON_BS_CINT -- 核销收回表内复利
  ,WRTOFF_RETRA_OFBS_INT -- 核销收回表外利息
  ,WRTOFF_RETRA_OFBS_CINT -- 核销收回表外复利
  ,WRTOFF_AFT_NEW_STL_INT -- 核销后新结利息
  ,RETRA_WRTOFF_AFT_INT -- 收回核销后利息
  ,RETRA_TYPE_CD -- 收回类型代码
  ,WRTOFF_RETRA_TELR_NO -- 核销收回柜员编号
  ,WRTOFF_RETRA_DT -- 核销收回日期
  ,DATA_DT -- 数据日期
)
SELECT
  T1.BLFMMAST AS STAACONO -- 贷款合同号
  ,T1.BLFMMAST AS STABSN03 -- 借据序号
  ,SUBSTR(T1.STAAACWF,1,1) AS STAAACWF -- 账户处理状态（第一位是核销状态）
  ,T1.STAACCYC AS STAACCYC -- 币种
  ,T1.STAABRNO AS STAABRNO -- 机构号
  ,T1.STAACSNO AS STAACSNO -- 客户内码
  ,T1.STANFLNM AS STANFLNM -- 客户名称
  ,T1.STABCSID AS STABCSID -- 客户号
  ,T1.STAASTAF AS STAASTAF -- 信贷员
  ,substr(T1.STABNO20,14,1)  AS STABNO20 -- 贷款属性码（第14位为信用等级代码）
  ,T2.ORG_SCALE AS ORG_SCALE -- 企业规模
  ,T3.REG_INDS AS REG_INDS -- 注册行业
  ,T2.LISTED_CO_FLG AS LISTED_CO_FLG -- 上市公司标志
  ,T1.STAAPRNO AS STAAPRNO -- 贷款产品代码
  ,T4.SSTY AS SSTY -- 担保方式
  ,T1.STAAACID AS STAAACID -- 科目代号
  ,CAST(TO_CHAR(T1.STAIDATE)  AS DATE) AS STAIDATE -- 发放日期
  ,CAST(TO_CHAR(T1.STACDATE) AS DATE) AS STACDATE -- 到期日期
  ,T1.STADCLS4 AS STADCLS4 -- 贷款当前形态4级
  ,T1.STAECLS5 AS STAECLS5 -- 贷款当前形态5级
  ,substr(T1.STAANO24,1,1) AS STAANO24 -- 信息编码（第1位为贷款期限代码）
  ,substr(T1.STAANO24,11,3) AS STAANO24 -- 信息编码（第11-13位为贷款用途编号）
  ,T4.LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT AS LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
  ,T4.LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT AS LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
  ,T4.TEN_LVL_MODAL_FIRST_TRAN_BAD_DT AS TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
  ,T4.TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT AS TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
  ,T6.AFAEPRN AS AFAEPRN -- 实核贷款本金
  ,T6.AFBFIAM2 AS AFBFIAM2 -- 实核表内利息
  ,T6.AFBRIAM2 AS AFBRIAM2 -- 实核表内复息
  ,T6.AFBGIAM2 AS AFBGIAM2 -- 实核表外利息
  ,T6.AFBSIAM2 AS AFBSIAM2 -- 实核表外复息
  ,CAST(TO_CHAR(T6.AFBJDATE) AS DATE) AS AFBJDATE -- 核销日期
  ,T6.AFASAMT AS AFASAMT -- 核销收回本金
  ,T6.AFBIIAM2 AS AFBIIAM2 -- 核销收回表内利息
  ,T6.AFBVIAM2 AS AFBVIAM2 -- 核销收回表内复利
  ,T6.AFBJIAM2 AS AFBJIAM2 -- 核销收回表外利息
  ,T6.AFBWIAM2 AS AFBWIAM2 -- 核销收回表外复利
  ,T6.AFEIIAM2 AS AFEIIAM2 -- 核销后新结利息
  ,T6.AFBXIAM2 AS AFBXIAM2 -- 收回核销后利息
  ,T6.AFCGFLAG AS AFCGFLAG -- 收回标志
  ,T6.AFAQSTAF AS AFAQSTAF -- 核销收回柜员
  ,CAST(TO_CHAR(T6.AFBKDATE) AS DATE) AS AFBKDATE -- 核销收回日期
  ,DATE'${batch_date}' AS None -- None
 FROM ${ods_core_schema}.ODS_CORE_BLFMMAST  T1 -- 普通贷款分户文件 
LEFT JOIN ${ods_ecif_schema}.ODS_ECIF_T2_EN_CUST_DETAIL_INFO  T2 -- 对公客户详细信息 
 ON T1.STAACSNO = T2.CUST_INCOD
AND T2.PT_DT='${batch_date}' 
AND T2.DELETED='0'
LEFT JOIN ${ods_ecif_schema}.ODS_ECIF_T2_EN_CUST_BASE_INFO  T3 -- 对公客户基本信息 
 ON T3.CUST_INCOD = T2.CUST_INCOD
AND T3.PT_DT='${batch_date}' 
AND T3.DELETED='0'
LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMCONF  T4 -- 贷款合同文件 
 ON T4.NFAACONO=T1.STAACONO 
AND T4.PT_DT='${batch_date}' 
AND T4.DELETED='0'
LEFT JOIN TMP_S03_CORP_NP_LOAN_WRTOFF_01  T5 -- 普通贷款分户临时表信息 
 ON T1.STAACONO = T5.STAACONO
AND T1.STABSN03 = T5.STABSN03
LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMCNAF  T6 -- 贷款核销文件 
 ON T6.AFAACONO=T1.STAACONO  
AND T6.AFABSN03=T1.STABSN03
AND T6.PT_DT='${batch_date}' 
AND T6.DELETED='0'
 WHERE 1=1 
AND T1.STADCLS4 IN ('2','3','4') OR T1.STAECLS5 IN ('3','4','5','6')
AND T1.PT_DT='${batch_date}' 
AND T1.DELETED='0'
 
;


/*添加表分析*/ 
ANALYZE TABLE ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_02;


/*===================第3组====================*/

DROP TABLE IF EXISTS ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_03;
CREATE GLOBAL TEMPORARY TABLE ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_03 (
 LOAN_CONT_NO  VARCHAR(16) -- 贷款合同编号
  ,DUBIL_SER_NO  VARCHAR(3) -- 借据序号
  ,LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT  DATE -- 四级形态首次转不良日期
  ,LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT  DATE -- 四级形态最近一次转不良日期
  ,TEN_LVL_MODAL_FIRST_TRAN_BAD_DT  DATE -- 十级形态首次转不良日期
  ,TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT  DATE -- 十级形态最近一次转不良日期
)
compress(5,5)
DISTRIBUTED BY ( LOAN_CONT_NO );

INSERT INTO ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_03(
  LOAN_CONT_NO -- 贷款合同编号
  ,DUBIL_SER_NO -- 借据序号
  ,LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
  ,LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
  ,TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
  ,TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
)
SELECT
  T1.STAACONO AS TZAACONO -- 贷款合同号
  ,T1.STABSN03 AS TZABSN03 -- 借据序号
  ,T2.ADJUST_DATE AS FCAVDATE -- 调整日期
  ,T3.ADJUST_DATE AS FCAVDATE -- 调整日期
  ,T4.ADJUST_DATE AS VCAVDATE -- 调整日期
  ,T5.ADJUST_DATE AS VCAVDATE -- 调整日期
 FROM ${ods_core_schema}.ODS_CORE_BLFMAMTZ  T1 -- 按揭贷款分户文件 
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
	AND P1.FCAHCLS4 IN ('2','3','4') --调整后
	AND P1.PT_DT='${batch_date}' 	
	AND P1.DELETED='0'
	)  T2 -- 贷款四级形态调整登记簿 
 ON T1.TZAACONO = T2.FCAACONO
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
	AND P1.FCAHCLS4 IN ('2','3','4') --调整后
	AND P1.PT_DT='${batch_date}' 	
	AND P1.DELETED='0'
	)  T3 -- 贷款四级形态调整登记簿 
 ON T1.TZAACONO = T3.FCAACONO
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
	AND P1.VCAICLS5 IN ('1','2','7','8','9','0')	--调整前
	AND P1.VCAJCLS5 IN ('3','4','5','6') --调整后
	AND P1.PT_DT='${batch_date}' 	
	AND P1.DELETED='0'
	)  T4 -- 贷款五级形态调整登记簿 
 ON T1.TZAACONO = T4.VCAACONO
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
	AND P1.VCAICLS5 IN ('1','2','7','8','9','0')	--调整前
	AND P1.VCAJCLS5 IN ('3','4','5','6') --调整后
	AND P1.PT_DT='${batch_date}' 	
	AND P1.DELETED='0'
	)  T5 -- 贷款五级形态调整登记簿 
 ON T1.TZAACONO = T5.VCAACONO
AND T1.TZABSN03 = T5.VCABSN03
AND T5.RN2 = 1
 WHERE 1=1 
AND T1.TZADCLS4 IN ('2','3','4') OR T1.TZAECLS5 IN ('3','4','5','6')
AND T1.PT_DT='${batch_date}' 
AND T1.DELETED='0'
 
;


/*添加表分析*/ 
ANALYZE TABLE ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_03;


/*===================第4组====================*/

DROP TABLE IF EXISTS ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_04;
CREATE GLOBAL TEMPORARY TABLE ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_04 (
 LOAN_CONT_NO  VARCHAR(16) -- 贷款合同编号
  ,DUBIL_SER_NO  VARCHAR(3) -- 借据序号
  ,WRTOFF_STATUS_CD  VARCHAR(1) -- 核销状态代码
  ,CURR_CD  VARCHAR(3) -- 币种代码
  ,BELONG_ORG_NO  VARCHAR(6) -- 归属机构编号
  ,CUST_IN_CD  VARCHAR(11) -- 客户内码
  ,CUST_NAME  VARCHAR(80) -- 客户名称
  ,CUST_NO  VARCHAR(23) -- 客户号
  ,BELONG_CUST_MGR_NO  VARCHAR(7) -- 归属客户经理编号
  ,CORP_CRDT_CD  VARCHAR(1) -- 企业信用等级代码
  ,CORP_SCALE_CD  VARCHAR(1) -- 企业规模代码
  ,CORP_REG_INDUS_CD  VARCHAR(4) -- 企业注册行业代码
  ,LIST_CORP_FLAG  VARCHAR(1) -- 上市公司标志
  ,LOANPROD_CD  VARCHAR(5) -- 贷款产品代码
  ,SPNSR_CD  VARCHAR(3) -- 担保方式代码
  ,LOAN_SUBJ_CD  VARCHAR(8) -- 贷款科目代码
  ,LOAN_DT  DATE -- 贷款发放日期
  ,LOAN_MATU_DT  DATE -- 贷款到期日期
  ,CURR_LOAN_LVL_FOUR_MODAL_CD  VARCHAR(1) -- 当前贷款四级形态代码
  ,CURR_LOAN_TEN_LVL_MODAL_CD  VARCHAR(1) -- 当前贷款十级形态代码
  ,LOAN_TERM_TYPE_CD  VARCHAR(1) -- 贷款期限类型代码
  ,LOAN_USAGE_NO  VARCHAR(3) -- 贷款用途编号
  ,LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT  DATE -- 四级形态首次转不良日期
  ,LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT  DATE -- 四级形态最近一次转不良日期
  ,TEN_LVL_MODAL_FIRST_TRAN_BAD_DT  DATE -- 十级形态首次转不良日期
  ,TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT  DATE -- 十级形态最近一次转不良日期
  ,AUDIT_LOAN_MNY_PRIN_AMT  DECIMAL(15,2) -- 实核贷款本金金额
  ,ON_BS_INT  DECIMAL(11,2) -- 实核表内利息
  ,ON_BS_COMP_INT  DECIMAL(11,2) -- 实核表内复息
  ,OFBS_INT  DECIMAL(11,2) -- 实核表外利息
  ,OFBS_COMP_INT  DECIMAL(11,2) -- 实核表外复息
  ,WRTOFF_DT  DATE -- 核销日期
  ,WRTOFF_RETRA_PRIN_AMT  DECIMAL(15,2) -- 核销收回本金金额
  ,WRTOFF_RETRA_ON_BS_INT  DECIMAL(11,2) -- 核销收回表内利息
  ,WRTOFF_RETRA_ON_BS_CINT  DECIMAL(11,2) -- 核销收回表内复利
  ,WRTOFF_RETRA_OFBS_INT  DECIMAL(11,2) -- 核销收回表外利息
  ,WRTOFF_RETRA_OFBS_CINT  DECIMAL(11,2) -- 核销收回表外复利
  ,WRTOFF_AFT_NEW_STL_INT  DECIMAL(11,2) -- 核销后新结利息
  ,RETRA_WRTOFF_AFT_INT  DECIMAL(11,2) -- 收回核销后利息
  ,RETRA_TYPE_CD  VARCHAR(1) -- 收回类型代码
  ,WRTOFF_RETRA_TELR_NO  VARCHAR(7) -- 核销收回柜员编号
  ,WRTOFF_RETRA_DT  DATE -- 核销收回日期
  ,DATA_DT  DATE -- 数据日期
)
compress(5,5)
DISTRIBUTED BY ( LOAN_CONT_NO );

INSERT INTO ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_04(
  LOAN_CONT_NO -- 贷款合同编号
  ,DUBIL_SER_NO -- 借据序号
  ,WRTOFF_STATUS_CD -- 核销状态代码
  ,CURR_CD -- 币种代码
  ,BELONG_ORG_NO -- 归属机构编号
  ,CUST_IN_CD -- 客户内码
  ,CUST_NAME -- 客户名称
  ,CUST_NO -- 客户号
  ,BELONG_CUST_MGR_NO -- 归属客户经理编号
  ,CORP_CRDT_CD -- 企业信用等级代码
  ,CORP_SCALE_CD -- 企业规模代码
  ,CORP_REG_INDUS_CD -- 企业注册行业代码
  ,LIST_CORP_FLAG -- 上市公司标志
  ,LOANPROD_CD -- 贷款产品代码
  ,SPNSR_CD -- 担保方式代码
  ,LOAN_SUBJ_CD -- 贷款科目代码
  ,LOAN_DT -- 贷款发放日期
  ,LOAN_MATU_DT -- 贷款到期日期
  ,CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
  ,CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
  ,LOAN_TERM_TYPE_CD -- 贷款期限类型代码
  ,LOAN_USAGE_NO -- 贷款用途编号
  ,LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
  ,LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
  ,TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
  ,TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
  ,AUDIT_LOAN_MNY_PRIN_AMT -- 实核贷款本金金额
  ,ON_BS_INT -- 实核表内利息
  ,ON_BS_COMP_INT -- 实核表内复息
  ,OFBS_INT -- 实核表外利息
  ,OFBS_COMP_INT -- 实核表外复息
  ,WRTOFF_DT -- 核销日期
  ,WRTOFF_RETRA_PRIN_AMT -- 核销收回本金金额
  ,WRTOFF_RETRA_ON_BS_INT -- 核销收回表内利息
  ,WRTOFF_RETRA_ON_BS_CINT -- 核销收回表内复利
  ,WRTOFF_RETRA_OFBS_INT -- 核销收回表外利息
  ,WRTOFF_RETRA_OFBS_CINT -- 核销收回表外复利
  ,WRTOFF_AFT_NEW_STL_INT -- 核销后新结利息
  ,RETRA_WRTOFF_AFT_INT -- 收回核销后利息
  ,RETRA_TYPE_CD -- 收回类型代码
  ,WRTOFF_RETRA_TELR_NO -- 核销收回柜员编号
  ,WRTOFF_RETRA_DT -- 核销收回日期
  ,DATA_DT -- 数据日期
)
SELECT
  T1.TZAACONO AS TZAACONO -- 贷款合同号
  ,T1.TZABSN03 AS TZABSN03 -- 借据序号
  ,SUBSTRING(T1.TZAAACWF,1,1) AS TZAAACWF -- 账户处理状态（第一位是核销状态）
  ,T1.TZAACCYC AS TZAACCYC -- 币种
  ,T1.TZAABRNO AS TZAABRNO -- 机构号
  ,T1.TZAACSNO AS TZAACSNO -- 客户内码
  ,T1.TZANFLNM AS TZANFLNM -- 客户名称
  ,T1.TZABCSID AS TZABCSID -- 客户号
  ,T1.TZAASTAF AS TZAASTAF -- 信贷员
  ,substr(T1.TZABNO20,14,1) AS TZABNO20 -- 贷款属性码（第15位为信用等级代码）
  ,T2.ORG_SCALE AS ORG_SCALE -- 企业规模
  ,T3.REG_INDS AS REG_INDS -- 注册行业
  ,T2.LISTED_CO_FLG AS LISTED_CO_FLG -- 上市公司标志
  ,T1.TZAAPRNO AS TZAAPRNO -- 贷款产品代码
  ,T4.SSTY AS SSTY -- 担保方式
  ,T1.TZAAACID AS TZAAACID -- 科目代号
  ,CAST(TO_CHAR(T1.TZAIDATE) AS DATE) AS TZAIDATE -- 发放日期
  ,CAST(TO_CHAR(T1.TZACDATE) AS DATE) AS TZACDATE -- 到期日期
  ,T1.TZADCLS4 AS TZADCLS4 -- 贷款当前形态4级
  ,T1.TZAECLS5 AS TZAECLS5 -- 贷款当前形态5级
  ,substr(TZAANO24,1,1) AS TZAANO24 -- 信息编码（第1位为贷款期限代码）
  ,substr(TZAANO24,11,3) AS TZAANO24 -- 信息编码（第11-13位为贷款用途编号）
  ,T4.LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT AS LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
  ,T4.LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT AS LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
  ,T4.TEN_LVL_MODAL_FIRST_TRAN_BAD_DT AS TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
  ,T4.TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT AS TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
  ,T6.AFAEPRN AS AFAEPRN -- 实核贷款本金
  ,T6.AFBFIAM2 AS AFBFIAM2 -- 实核表内利息
  ,T6.AFBRIAM2 AS AFBRIAM2 -- 实核表内复息
  ,T6.AFBGIAM2 AS AFBGIAM2 -- 实核表外利息
  ,T6.AFBSIAM2 AS AFBSIAM2 -- 实核表外复息
  ,CAST(TO_CHAR(T6.AFBJDATE ) AS DATE) AS AFBJDATE -- 核销日期
  ,T6.AFASAMT AS AFASAMT -- 核销收回本金
  ,T6.AFBIIAM2 AS AFBIIAM2 -- 核销收回表内利息
  ,T6.AFBVIAM2 AS AFBVIAM2 -- 核销收回表内复利
  ,T6.AFBJIAM2 AS AFBJIAM2 -- 核销收回表外利息
  ,T6.AFBWIAM2 AS AFBWIAM2 -- 核销收回表外复利
  ,T6.AFEIIAM2 AS AFEIIAM2 -- 核销后新结利息
  ,T6.AFBXIAM2 AS AFBXIAM2 -- 收回核销后利息
  ,T6.AFCGFLAG AS AFCGFLAG -- 收回标志
  ,T6.AFAQSTAF AS AFAQSTAF -- 核销收回柜员
  ,CAST(TO_CHAR(T6.AFBKDATE) AS DATE) AS AFBKDATE -- 核销收回日期
  ,DATE'${batch_date}' AS None -- None
 FROM ${ods_core_schema}.ODS_CORE_BLFMAMTZ  T1 -- 按揭贷款分户文件 
LEFT JOIN ${ods_ecif_schema}.T2_EN_CUST_DETAIL_INFO  T2 -- 对公客户详细信息 
 ON T1.TZAACSNO = T2.CUST_INCOD
AND T2.PT_DT='${batch_date}' 
AND T2.DELETED='0'
LEFT JOIN ${ods_ecif_schema}.T2_EN_CUST_BASE_INFO  T3 -- 对公客户基本信息 
 ON T3.CUST_INCOD = T2.CUST_INCOD
AND T3.PT_DT='${batch_date}' 
AND T3.DELETED='0'
LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMCONF  T4 -- 贷款合同文件 
 ON T4.NFAACONO=T1.TZAACONO 
AND T4.PT_DT='${batch_date}' 
AND T4.DELETED='0'
LEFT JOIN TMP_S03_CORP_NP_LOAN_WRTOFF_03  T5 -- 普通贷款分户临时表信息 
 ON T1.TZAACONO = T5.TZAACONO
AND T1.TZABSN03 = T5.TZABSN03
LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMCNAF  T6 -- 贷款核销文件 
 ON T6.AFAACONO=T1.TZAACONO  
AND T6.AFABSN03=T1.TZABSN03
AND T6.PT_DT='${batch_date}' 
AND T6.DELETED='0'
 WHERE 1=1 
AND T1.TZADCLS4 IN ('2','3','4') OR T1.TZAECLS5 IN ('3','4','5','6')
AND T1.PT_DT='${batch_date}' 
AND T1.DELETED='0'
 
;


/*添加表分析*/ 
ANALYZE TABLE ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_04;


/*===================第5组====================*/

DROP TABLE IF EXISTS ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_05;
CREATE GLOBAL TEMPORARY TABLE ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_05 (
 LOAN_CONT_NO  VARCHAR(16) -- 贷款合同编号
  ,DUBIL_SER_NO  VARCHAR(3) -- 借据序号
  ,LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT  DATE -- 四级形态首次转不良日期
  ,LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT  DATE -- 四级形态最近一次转不良日期
  ,TEN_LVL_MODAL_FIRST_TRAN_BAD_DT  DATE -- 十级形态首次转不良日期
  ,TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT  DATE -- 十级形态最近一次转不良日期
)
compress(5,5)
DISTRIBUTED BY ( LOAN_CONT_NO );

INSERT INTO ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_05(
  LOAN_CONT_NO -- 贷款合同编号
  ,DUBIL_SER_NO -- 借据序号
  ,LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
  ,LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
  ,TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
  ,TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
)
SELECT
  T1.STAACONO AS NTAACONO -- 贷款合同号
  ,T1.STABSN03 AS NTABSN03 -- 借据序号
  ,T2.ADJUST_DATE AS FCAVDATE -- 调整日期
  ,T3.ADJUST_DATE AS FCAVDATE -- 调整日期
  ,T4.ADJUST_DATE AS VCAVDATE -- 调整日期
  ,T5.ADJUST_DATE AS VCAVDATE -- 调整日期
 FROM ${ods_core_schema}.ODS_CORE_BLFMDCNT  T1 -- 贴现分户文件 
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
	AND P1.FCAHCLS4 IN ('2','3','4') --调整后
	AND P1.PT_DT='${batch_date}' 	
	AND P1.DELETED='0'
	)  T2 -- 贷款四级形态调整登记簿 
 ON T1.NTAACONO = T2.FCAACONO
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
	AND P1.FCAHCLS4 IN ('2','3','4') --调整后
	AND P1.PT_DT='${batch_date}' 	
	AND P1.DELETED='0'
	)  T3 -- 贷款四级形态调整登记簿 
 ON T1.NTAACONO = T3.FCAACONO
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
	AND P1.VCAICLS5 IN ('1','2','7','8','9','0')	--调整前
	AND P1.VCAJCLS5 IN ('3','4','5','6') --调整后
	AND P1.PT_DT='${batch_date}' 	
	AND P1.DELETED='0'
	)  T4 -- 贷款五级形态调整登记簿 
 ON T1.NTAACONO = T4.VCAACONO
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
	AND P1.VCAICLS5 IN ('1','2','7','8','9','0')	--调整前
	AND P1.VCAJCLS5 IN ('3','4','5','6') --调整后
	AND P1.PT_DT='${batch_date}' 	
	AND P1.DELETED='0'
	)  T5 -- 贷款五级形态调整登记簿 
 ON T1.NTAACONO = T5.VCAACONO
AND T1.NTABSN03 = T5.VCABSN03
AND T5.RN2 = 1
 WHERE 1=1 
AND T1.NTAFCLS4 IN ('2','3','4') OR T1.NTAECLS5 IN ('3','4','5','6')
AND T1.PT_DT='${batch_date}' 
AND T1.DELETED='0'
 
;


/*添加表分析*/ 
ANALYZE TABLE ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_05;


/*===================第6组====================*/

DROP TABLE IF EXISTS ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_06;
CREATE GLOBAL TEMPORARY TABLE ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_06 (
 LOAN_CONT_NO  VARCHAR(16) -- 贷款合同编号
  ,DUBIL_SER_NO  VARCHAR(3) -- 借据序号
  ,WRTOFF_STATUS_CD  VARCHAR(1) -- 核销状态代码
  ,CURR_CD  VARCHAR(3) -- 币种代码
  ,BELONG_ORG_NO  VARCHAR(6) -- 归属机构编号
  ,CUST_IN_CD  VARCHAR(11) -- 客户内码
  ,CUST_NAME  VARCHAR(80) -- 客户名称
  ,CUST_NO  VARCHAR(23) -- 客户号
  ,BELONG_CUST_MGR_NO  VARCHAR(7) -- 归属客户经理编号
  ,CORP_CRDT_CD  VARCHAR(1) -- 企业信用等级代码
  ,CORP_SCALE_CD  VARCHAR(1) -- 企业规模代码
  ,CORP_REG_INDUS_CD  VARCHAR(4) -- 企业注册行业代码
  ,LIST_CORP_FLAG  VARCHAR(1) -- 上市公司标志
  ,LOANPROD_CD  VARCHAR(5) -- 贷款产品代码
  ,SPNSR_CD  VARCHAR(3) -- 担保方式代码
  ,LOAN_SUBJ_CD  VARCHAR(8) -- 贷款科目代码
  ,LOAN_DT  DATE -- 贷款发放日期
  ,LOAN_MATU_DT  DATE -- 贷款到期日期
  ,CURR_LOAN_LVL_FOUR_MODAL_CD  VARCHAR(1) -- 当前贷款四级形态代码
  ,CURR_LOAN_TEN_LVL_MODAL_CD  VARCHAR(1) -- 当前贷款十级形态代码
  ,LOAN_TERM_TYPE_CD  VARCHAR(1) -- 贷款期限类型代码
  ,LOAN_USAGE_NO  VARCHAR(3) -- 贷款用途编号
  ,LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT  DATE -- 四级形态首次转不良日期
  ,LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT  DATE -- 四级形态最近一次转不良日期
  ,TEN_LVL_MODAL_FIRST_TRAN_BAD_DT  DATE -- 十级形态首次转不良日期
  ,TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT  DATE -- 十级形态最近一次转不良日期
  ,AUDIT_LOAN_MNY_PRIN_AMT  DECIMAL(15,2) -- 实核贷款本金金额
  ,ON_BS_INT  DECIMAL(11,2) -- 实核表内利息
  ,ON_BS_COMP_INT  DECIMAL(11,2) -- 实核表内复息
  ,OFBS_INT  DECIMAL(11,2) -- 实核表外利息
  ,OFBS_COMP_INT  DECIMAL(11,2) -- 实核表外复息
  ,WRTOFF_DT  DATE -- 核销日期
  ,WRTOFF_RETRA_PRIN_AMT  DECIMAL(15,2) -- 核销收回本金金额
  ,WRTOFF_RETRA_ON_BS_INT  DECIMAL(11,2) -- 核销收回表内利息
  ,WRTOFF_RETRA_ON_BS_CINT  DECIMAL(11,2) -- 核销收回表内复利
  ,WRTOFF_RETRA_OFBS_INT  DECIMAL(11,2) -- 核销收回表外利息
  ,WRTOFF_RETRA_OFBS_CINT  DECIMAL(11,2) -- 核销收回表外复利
  ,WRTOFF_AFT_NEW_STL_INT  DECIMAL(11,2) -- 核销后新结利息
  ,RETRA_WRTOFF_AFT_INT  DECIMAL(11,2) -- 收回核销后利息
  ,RETRA_TYPE_CD  VARCHAR(1) -- 收回类型代码
  ,WRTOFF_RETRA_TELR_NO  VARCHAR(7) -- 核销收回柜员编号
  ,WRTOFF_RETRA_DT  DATE -- 核销收回日期
  ,DATA_DT  DATE -- 数据日期
)
compress(5,5)
DISTRIBUTED BY ( LOAN_CONT_NO );

INSERT INTO ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_06(
  LOAN_CONT_NO -- 贷款合同编号
  ,DUBIL_SER_NO -- 借据序号
  ,WRTOFF_STATUS_CD -- 核销状态代码
  ,CURR_CD -- 币种代码
  ,BELONG_ORG_NO -- 归属机构编号
  ,CUST_IN_CD -- 客户内码
  ,CUST_NAME -- 客户名称
  ,CUST_NO -- 客户号
  ,BELONG_CUST_MGR_NO -- 归属客户经理编号
  ,CORP_CRDT_CD -- 企业信用等级代码
  ,CORP_SCALE_CD -- 企业规模代码
  ,CORP_REG_INDUS_CD -- 企业注册行业代码
  ,LIST_CORP_FLAG -- 上市公司标志
  ,LOANPROD_CD -- 贷款产品代码
  ,SPNSR_CD -- 担保方式代码
  ,LOAN_SUBJ_CD -- 贷款科目代码
  ,LOAN_DT -- 贷款发放日期
  ,LOAN_MATU_DT -- 贷款到期日期
  ,CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
  ,CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
  ,LOAN_TERM_TYPE_CD -- 贷款期限类型代码
  ,LOAN_USAGE_NO -- 贷款用途编号
  ,LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
  ,LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
  ,TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
  ,TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
  ,AUDIT_LOAN_MNY_PRIN_AMT -- 实核贷款本金金额
  ,ON_BS_INT -- 实核表内利息
  ,ON_BS_COMP_INT -- 实核表内复息
  ,OFBS_INT -- 实核表外利息
  ,OFBS_COMP_INT -- 实核表外复息
  ,WRTOFF_DT -- 核销日期
  ,WRTOFF_RETRA_PRIN_AMT -- 核销收回本金金额
  ,WRTOFF_RETRA_ON_BS_INT -- 核销收回表内利息
  ,WRTOFF_RETRA_ON_BS_CINT -- 核销收回表内复利
  ,WRTOFF_RETRA_OFBS_INT -- 核销收回表外利息
  ,WRTOFF_RETRA_OFBS_CINT -- 核销收回表外复利
  ,WRTOFF_AFT_NEW_STL_INT -- 核销后新结利息
  ,RETRA_WRTOFF_AFT_INT -- 收回核销后利息
  ,RETRA_TYPE_CD -- 收回类型代码
  ,WRTOFF_RETRA_TELR_NO -- 核销收回柜员编号
  ,WRTOFF_RETRA_DT -- 核销收回日期
  ,DATA_DT -- 数据日期
)
SELECT
  T1.NTAACONO AS NTAACONO -- 贷款合同号
  ,T1.NTABSN03 AS NTABSN03 -- 借据序号
  ,SUBSTR(T1.NTAAACWF,1,1) AS NTAAACWF -- 账户处理状态（第一位是核销状态）
  ,T1.NTAACCYC AS NTAACCYC -- 币种
  ,T1.NTAABRNO AS NTAABRNO -- 机构号
  ,T1.NTAACSNO AS NTAACSNO -- 客户内码
  ,T1.NTANFLNM AS NTANFLNM -- 客户名称
  ,T1.NTABCSID AS NTABCSID -- 客户号
  ,T1.NTAASTAF AS NTAASTAF -- 信贷员
  ,substr(NTACNO20,14,1) AS NTACNO20 -- 贷款属性码（第16位为信用等级代码）
  ,T2.ORG_SCALE AS ORG_SCALE -- 企业规模
  ,T3.REG_INDS AS REG_INDS -- 注册行业
  ,T2.LISTED_CO_FLG AS LISTED_CO_FLG -- 上市公司标志
  ,T1.NTAAPRNO AS NTAAPRNO -- 贷款产品代码
  ,T4.SSTY AS SSTY -- 担保方式
  ,T1.NTAAACID AS NTAAACID -- 科目代号
  ,CAST(TO_CHAR(T1.NTASDATE) AS DATE) AS NTASDATE -- 贴现日期
  ,CAST(TO_CHAR(T1.NTATDATE) AS DATE) AS NTATDATE -- 贴现到期日期
  ,T1.NTAFCLS4 AS NTAFCLS4 -- 贷款当前形态4级
  ,T1.NTAHCLS5 AS NTAHCLS5 -- 贷款当前形态5级
  ,substr(NTAANO24,1,1) AS NTAANO24 -- 信息编码（第1位为贷款期限代码）
  ,substr(NTAANO24,11,3) AS NTAANO24 -- 信息编码（第11-13位为贷款用途编号）
  ,T4.LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT AS LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
  ,T4.LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT AS LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
  ,T4.TEN_LVL_MODAL_FIRST_TRAN_BAD_DT AS TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
  ,T4.TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT AS TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
  ,T6.AFAEPRN AS AFAEPRN -- 实核贷款本金
  ,T6.AFBFIAM2 AS AFBFIAM2 -- 实核表内利息
  ,T6.AFBRIAM2 AS AFBRIAM2 -- 实核表内复息
  ,T6.AFBGIAM2 AS AFBGIAM2 -- 实核表外利息
  ,T6.AFBSIAM2 AS AFBSIAM2 -- 实核表外复息
  ,CAST(TO_CHAR(T6.AFBJDATE ) AS DATE) AS AFBJDATE -- 核销日期
  ,T6.AFASAMT AS AFASAMT -- 核销收回本金
  ,T6.AFBIIAM2 AS AFBIIAM2 -- 核销收回表内利息
  ,T6.AFBVIAM2 AS AFBVIAM2 -- 核销收回表内复利
  ,T6.AFBJIAM2 AS AFBJIAM2 -- 核销收回表外利息
  ,T6.AFBWIAM2 AS AFBWIAM2 -- 核销收回表外复利
  ,T6.AFEIIAM2 AS AFEIIAM2 -- 核销后新结利息
  ,T6.AFBXIAM2 AS AFBXIAM2 -- 收回核销后利息
  ,T6.AFCGFLAG AS AFCGFLAG -- 收回标志
  ,T6.AFAQSTAF AS AFAQSTAF -- 核销收回柜员
  ,CAST(TO_CHAR(T6.AFBKDATE) AS DATE) AS AFBKDATE -- 核销收回日期
  ,DATE'${batch_date}' AS None -- None
 FROM ${ods_core_schema}.ODS_CORE_BLFMAMTZ  T1 -- 按揭贷款分户文件 
LEFT JOIN ${ods_ecif_schema}.T2_EN_CUST_DETAIL_INFO  T2 -- 对公客户详细信息 
 ON T1.NTAACSNO = T2.CUST_INCOD
AND T2.PT_DT='${batch_date}' 
AND T2.DELETED='0'
LEFT JOIN ${ods_ecif_schema}.T2_EN_CUST_BASE_INFO  T3 -- 对公客户基本信息 
 ON T3.CUST_INCOD = T2.CUST_INCOD
AND T3.PT_DT='${batch_date}' 
AND T3.DELETED='0'
LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMCONF  T4 -- 贷款合同文件 
 ON T4.NFAACONO=T1.NTAACONO 
AND T4.PT_DT='${batch_date}' 
AND T4.DELETED='0'
LEFT JOIN TMP_S03_CORP_NP_LOAN_WRTOFF_03  T5 -- 普通贷款分户临时表信息 
 ON T1.NTAACONO = T5.NTAACONO
AND T1.NTABSN03 = T5.NTABSN03
LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMCNAF  T6 -- 贷款核销文件 
 ON T6.AFAACONO=T1.NTAACONO  
AND T6.AFABSN03=T1.NTABSN03
AND T6.PT_DT='${batch_date}' 
AND T6.DELETED='0'
 WHERE 1=1 
AND T1.NTAFCLS4 IN ('2','3','4') OR T1.NTAECLS5 IN ('3','4','5','6')
AND T1.PT_DT='${batch_date}' 
AND T1.DELETED='0'
 
;


/*添加表分析*/ 
ANALYZE TABLE ${icl_schema}.TMP_S03_CORP_NP_LOAN_WRTOFF_06;


/*===================第7组====================*/

INSERT INTO ${icl_schema}.S03_CORP_NP_LOAN_WRTOFF(
  LOAN_CONT_NO -- 贷款合同编号
  ,DUBIL_SER_NO -- 借据序号
  ,WRTOFF_STATUS_CD -- 核销状态代码
  ,CURR_CD -- 币种代码
  ,BELONG_ORG_NO -- 归属机构编号
  ,CUST_IN_CD -- 客户内码
  ,CUST_NAME -- 客户名称
  ,CUST_NO -- 客户号
  ,BELONG_CUST_MGR_NO -- 归属客户经理编号
  ,CORP_CRDT_CD -- 企业信用等级代码
  ,CORP_SCALE_CD -- 企业规模代码
  ,CORP_REG_INDUS_CD -- 企业注册行业代码
  ,LIST_CORP_FLAG -- 上市公司标志
  ,LOANPROD_CD -- 贷款产品代码
  ,SPNSR_CD -- 担保方式代码
  ,LOAN_SUBJ_CD -- 贷款科目代码
  ,LOAN_DT -- 贷款发放日期
  ,LOAN_MATU_DT -- 贷款到期日期
  ,CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
  ,CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
  ,LOAN_TERM_TYPE_CD -- 贷款期限类型代码
  ,LOAN_USAGE_NO -- 贷款用途编号
  ,LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
  ,LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
  ,TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
  ,TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
  ,AUDIT_LOAN_MNY_PRIN_AMT -- 实核贷款本金金额
  ,ON_BS_INT -- 实核表内利息
  ,ON_BS_COMP_INT -- 实核表内复息
  ,OFBS_INT -- 实核表外利息
  ,OFBS_COMP_INT -- 实核表外复息
  ,WRTOFF_DT -- 核销日期
  ,WRTOFF_RETRA_PRIN_AMT -- 核销收回本金金额
  ,WRTOFF_RETRA_ON_BS_INT -- 核销收回表内利息
  ,WRTOFF_RETRA_ON_BS_CINT -- 核销收回表内复利
  ,WRTOFF_RETRA_OFBS_INT -- 核销收回表外利息
  ,WRTOFF_RETRA_OFBS_CINT -- 核销收回表外复利
  ,WRTOFF_AFT_NEW_STL_INT -- 核销后新结利息
  ,RETRA_WRTOFF_AFT_INT -- 收回核销后利息
  ,RETRA_TYPE_CD -- 收回类型代码
  ,WRTOFF_RETRA_TELR_NO -- 核销收回柜员编号
  ,WRTOFF_RETRA_DT -- 核销收回日期
  ,DATA_DT -- 数据日期
)
SELECT 
  T1.LOAN_CONT_NO AS LOAN_CONT_NO -- 贷款合同编号
  ,T1.DUBIL_SER_NO AS DUBIL_SER_NO -- 借据序号
  ,T1.WRTOFF_STATUS_CD AS WRTOFF_STATUS_CD -- 核销状态代码
  ,T1.CURR_CD AS CURR_CD -- 币种代码
  ,T1.BELONG_ORG_NO AS BELONG_ORG_NO -- 归属机构编号
  ,T1.CUST_IN_CD AS CUST_IN_CD -- 客户内码
  ,T1.CUST_NAME AS CUST_NAME -- 客户名称
  ,T1.CUST_NO AS CUST_NO -- 客户号
  ,T1.BELONG_CUST_MGR_NO AS BELONG_CUST_MGR_NO -- 归属客户经理编号
  ,T1.CORP_CRDT_CD AS CORP_CRDT_CD -- 企业信用等级代码
  ,T1.CORP_SCALE_CD AS CORP_SCALE_CD -- 企业规模代码
  ,T1.CORP_REG_INDUS_CD AS CORP_REG_INDUS_CD -- 企业注册行业代码
  ,T1.LIST_CORP_FLAG AS LIST_CORP_FLAG -- 上市公司标志
  ,T1.LOANPROD_CD AS LOANPROD_CD -- 贷款产品代码
  ,T1.SPNSR_CD AS SPNSR_CD -- 担保方式代码
  ,T1.LOAN_SUBJ_CD AS LOAN_SUBJ_CD -- 贷款科目代码
  ,T1.LOAN_DT AS LOAN_DT -- 贷款发放日期
  ,T1.LOAN_MATU_DT AS LOAN_MATU_DT -- 贷款到期日期
  ,T1.CURR_LOAN_LVL_FOUR_MODAL_CD AS CURR_LOAN_LVL_FOUR_MODAL_CD -- 当前贷款四级形态代码
  ,T1.CURR_LOAN_TEN_LVL_MODAL_CD AS CURR_LOAN_TEN_LVL_MODAL_CD -- 当前贷款十级形态代码
  ,T1.LOAN_TERM_TYPE_CD AS LOAN_TERM_TYPE_CD -- 贷款期限类型代码
  ,T1.LOAN_USAGE_NO AS LOAN_USAGE_NO -- 贷款用途编号
  ,T1.LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT AS LVL_FOUR_MODAL_FIRST_TRAN_BAD_DT -- 四级形态首次转不良日期
  ,T1.LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT AS LVL_FOUR_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 四级形态最近一次转不良日期
  ,T1.TEN_LVL_MODAL_FIRST_TRAN_BAD_DT AS TEN_LVL_MODAL_FIRST_TRAN_BAD_DT -- 十级形态首次转不良日期
  ,T1.TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT AS TEN_LVL_MODAL_RECNT_ONCE_TRAN_BAD_DT -- 十级形态最近一次转不良日期
  ,T1.AUDIT_LOAN_MNY_PRIN_AMT AS AUDIT_LOAN_MNY_PRIN_AMT -- 实核贷款本金金额
  ,T1.ON_BS_INT AS ON_BS_INT -- 实核表内利息
  ,T1.ON_BS_COMP_INT AS ON_BS_COMP_INT -- 实核表内复息
  ,T1.OFBS_INT AS OFBS_INT -- 实核表外利息
  ,T1.OFBS_COMP_INT AS OFBS_COMP_INT -- 实核表外复息
  ,T1.WRTOFF_DT AS WRTOFF_DT -- 核销日期
  ,T1.WRTOFF_RETRA_PRIN_AMT AS WRTOFF_RETRA_PRIN_AMT -- 核销收回本金金额
  ,T1.WRTOFF_RETRA_ON_BS_INT AS WRTOFF_RETRA_ON_BS_INT -- 核销收回表内利息
  ,T1.WRTOFF_RETRA_ON_BS_CINT AS WRTOFF_RETRA_ON_BS_CINT -- 核销收回表内复利
  ,T1.WRTOFF_RETRA_OFBS_INT AS WRTOFF_RETRA_OFBS_INT -- 核销收回表外利息
  ,T1.WRTOFF_RETRA_OFBS_CINT AS WRTOFF_RETRA_OFBS_CINT -- 核销收回表外复利
  ,T1.WRTOFF_AFT_NEW_STL_INT AS WRTOFF_AFT_NEW_STL_INT -- 核销后新结利息
  ,T1.RETRA_WRTOFF_AFT_INT AS RETRA_WRTOFF_AFT_INT -- 收回核销后利息
  ,T1.RETRA_TYPE_CD AS RETRA_TYPE_CD -- 收回类型代码
  ,T1.WRTOFF_RETRA_TELR_NO AS WRTOFF_RETRA_TELR_NO -- 核销收回柜员编号
  ,T1.WRTOFF_RETRA_DT AS WRTOFF_RETRA_DT -- 核销收回日期
  ,T1.DATA_DT AS DATA_DT -- 数据日期 
 FROM (
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
	P1.DATA_DT                                     -- 数据日期
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
	P2.DATA_DT                                     -- 数据日期
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
	P3.DATA_DT                                      -- 数据日期
FROM TMP_S03_CORP_NP_LOAN_WRTOFF_04 P3 -- 贴现
)  T1 -- 法人不良贷款核销信息聚合_汇总 
   
 
;



/*添加目标表分析*/ 
\echo "4.analyze table" 
ANALYZE TABLE ${icl_schema}.S03_CORP_NP_LOAN_WRTOFF;
