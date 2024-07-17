/*
*********************************************************************** 
Purpose:       主题聚合层-加工快照表脚本
Author:        Sunline
Usage:         python $ETL_HOME/script/main.py yyyymmdd ${icl_schema}_c_pt_corp_cust_perf_rela
CreateDate:    2019-10-12 00:00:00
FileType:      DML
logs:
       表英文名：c_pt_corp_cust_perf_rela
       表中文名：对公客户绩效关系信息
       创建日期：2019-10-12 00:00:00
       主键字段：ETL_DT,LP_ORG_NO,ECF_PARTY_ID,PERF_RELA_TYPE_CD,CUST_MGR_EMPLY_NO
       归属层次：ICL
       归属主题：PT
       主要应用：大数据平台
       分析人员：开发平台
       时间粒度：日
       保留周期：13M
       描述信息：此为更新注释，在脚本里进行更新
2022/11/13 wangmujun  对公ECIF改造
*************************************************************************/ 

\timing 
/*创建当日分区*/
   call ${itl_schema}.partition_add('${icl_schema}.c_pt_corp_cust_perf_rela','pt_${batch_date}','${batch_date}'); 

/*删除当前批次历史数据*/
   call ${itl_schema}.partition_drop('${icl_schema}.c_pt_corp_cust_perf_rela','pt_${batch_date}'); 



/*===================第1组====================*/

DROP TABLE IF EXISTS ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_00;
CREATE GLOBAL TEMPORARY TABLE ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_00 (
 emply_no  VARCHAR(80) -- 员工编号
  ,emply_name  VARCHAR(300) -- 员工名称
  ,mgmt_org_id  VARCHAR(30) -- 管理机构编号
  ,mgmt_org_name  VARCHAR(300) -- 管理机构名称
  ,acct_org_id  VARCHAR(30) -- 账务机构编号
  ,acct_org_name  VARCHAR(300) -- 账务机构名称
  ,post_cd  VARCHAR(300) -- 岗位代码
  ,post_name  VARCHAR(100) -- 岗位名称
  ,post_lvl_cd  VARCHAR(20) -- 岗位级别代码
  ,post_lvl_name  VARCHAR(100) -- 岗位级别名称
  ,rank_no  VARCHAR(10) -- 排序
)
compress(5,5)
DISTRIBUTED BY ( emply_no );

INSERT INTO ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_00(
  emply_no -- 员工编号
  ,emply_name -- 员工名称
  ,mgmt_org_id -- 管理机构编号
  ,mgmt_org_name -- 管理机构名称
  ,acct_org_id -- 账务机构编号
  ,acct_org_name -- 账务机构名称
  ,post_cd -- 岗位代码
  ,post_name -- 岗位名称
  ,post_lvl_cd -- 岗位级别代码
  ,post_lvl_name -- 岗位级别名称
  ,rank_no -- 排序
)
SELECT
  T1.emply_no
   AS emply_no -- 员工编号
  ,T1.emply_name
   AS emply_name  -- 员工名称
  ,T3.org_no
   AS mgmt_org_id  -- 管理机构编号
  ,T3.org_name
   AS mgmt_org_name  -- 管理机构名称
  ,T5.org_no AS acct_org_id  -- 账务机构编号
  ,T5.org_name AS acct_org_name  -- 账务机构名称
  ,T6.post_cd AS post_cd  -- 岗位代码
  ,T6.post_name AS post_name  -- 岗位名称
  ,T7.post_lvl_cd AS post_lvl_cd  -- 岗位级别代码
  ,T7.post_lvl_name
 AS post_lvl_name  -- 岗位级别名称
  ,RANK() OVER (PARTITION BY T3.org_no ORDER BY T7.post_lvl_name) AS rank_no -- 排序
 FROM ${iml_schema}.M_PT_EMPLY  T1 -- 员工 
LEFT JOIN  ${iml_schema}.M_IO_ORG_RELA_H  T2 -- 机构关系历史 
 ON 'HRS'||T1.belong_org_no = T2.io_id
       AND T2.org_rela_type_cd = '91'            --绩效系统机构转管理机构
       AND T2.start_dt <= DATE'${batch_date}'
       AND T2.end_dt > DATE'${batch_date}'
LEFT JOIN  ${icl_schema}.C_PB_MGMT_ORG  T3 -- 管理机构信息 
 ON REPLACE(T2.rela_io_id, '888', '') = T3.org_no
       AND T3.etl_dt = DATE'${batch_date}'
LEFT JOIN  ${iml_schema}.M_IO_ORG_RELA_H  T4 -- 机构关系历史 
 ON T2.rela_io_id = T4.io_id
       AND T4.org_rela_type_cd = '93'            --管理机构转账务机构
       AND T4.start_dt <= DATE'${batch_date}'
       AND T4.end_dt > DATE'${batch_date}'
  
LEFT JOIN  ${icl_schema}.C_PB_ACCT_ORG  T5 -- 账务机构信息 
 ON T4.rela_io_id = T5.org_no
       AND T5.etl_dt = DATE'${batch_date}'
LEFT JOIN  ${iml_schema}.M_PB_HR_POST_INFO_CD  T6 -- 人力资源岗位信息代码 
 ON T1.post_cd = T6.post_cd
       AND T6.etl_dt = DATE'${batch_date}'
       AND T6.id_mark = 'I'
LEFT JOIN  ${iml_schema}.M_PB_HR_POST_LVL_CD  T7 -- 人力资源岗位级别代码 
 ON  T1.post_lvl_cd = T7.post_lvl_cd
       AND T7.etl_dt = DATE'${batch_date}'
       AND T7.id_mark = 'I'
 WHERE T1.etl_dt = DATE'${batch_date}'
       AND T1.id_mark = 'I'
 
;


/*添加表分析*/ 
ANALYZE TABLE ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_00;


/*===================第2组====================*/

DROP TABLE IF EXISTS ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_01;
CREATE GLOBAL TEMPORARY TABLE ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_01 (
 emply_no  VARCHAR(80) -- 员工编号
  ,emply_name  VARCHAR(300) -- 员工名称
  ,mgmt_org_id  VARCHAR(30) -- 管理机构编号
  ,mgmt_org_name  VARCHAR(300) -- 管理机构名称
  ,acct_org_id  VARCHAR(30) -- 账务机构编号
  ,acct_org_name  VARCHAR(300) -- 账务机构名称
  ,post_cd  VARCHAR(300) -- 岗位代码
  ,post_name  VARCHAR(100) -- 岗位名称
  ,post_lvl_cd  VARCHAR(20) -- 岗位级别代码
  ,post_lvl_name  VARCHAR(100) -- 岗位级别名称
)
compress(5,5)
DISTRIBUTED BY ( emply_no );

INSERT INTO ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_01(
  emply_no -- 员工编号
  ,emply_name -- 员工名称
  ,mgmt_org_id -- 管理机构编号
  ,mgmt_org_name -- 管理机构名称
  ,acct_org_id -- 账务机构编号
  ,acct_org_name -- 账务机构名称
  ,post_cd -- 岗位代码
  ,post_name -- 岗位名称
  ,post_lvl_cd -- 岗位级别代码
  ,post_lvl_name -- 岗位级别名称
)
SELECT
  P1.emply_no
   AS emply_no -- 员工编号
  ,P1.emply_name  
   AS emply_name  -- 员工名称
  ,P1.mgmt_org_id 
   AS mgmt_org_id  -- 管理机构编号
  ,P1.mgmt_org_name 
   AS mgmt_org_name  -- 管理机构名称
  ,P1.acct_org_id   AS acct_org_id  -- 账务机构编号
  ,P1.acct_org_name  AS acct_org_name  -- 账务机构名称
  ,P1.post_cd  AS post_cd  -- 岗位代码
  ,P1.post_name  AS post_name  -- 岗位名称
  ,P1.post_lvl_cd AS post_lvl_cd  -- 岗位级别代码
  ,P1.post_lvl_name 
 AS post_lvl_name  -- 岗位级别名称
 FROM tmp_c_pt_corp_cust_perf_rela_00  P1 -- None 
 WHERE P1.post_lvl_name LIKE 'B%' OR P1.post_lvl_name LIKE 'C%' OR P1.post_lvl_name LIKE 'D%'
AND P1.rank_no = 1
 
;


/*添加表分析*/ 
ANALYZE TABLE ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_01;


/*===================第3组====================*/

DROP TABLE IF EXISTS ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_02;
CREATE GLOBAL TEMPORARY TABLE ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_02 (
 org_no  VARCHAR(80) -- 绩效系统机构号
  ,mgmt_org_id  VARCHAR(80) -- 管理机构编号
  ,mgmt_org_name  VARCHAR(100) -- 管理机构名称
)
compress(5,5)
DISTRIBUTED BY ( org_no );

INSERT INTO ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_02(
  org_no -- 绩效系统机构号
  ,mgmt_org_id -- 管理机构编号
  ,mgmt_org_name -- 管理机构名称
)
SELECT
  SUBSTR(P1.io_id, 4) AS org_no -- 绩效系统机构号
  ,P1.rela_io_id  AS mgmt_org_id -- 管理机构编号
  ,P2.org_name  AS mgmt_org_name  -- 管理机构名称
 FROM ${iml_schema}.M_IO_ORG_RELA_H  P1 -- 机构关系历史 
 WHERE P1.io_id LIKE 'PAS%'  
  AND P1.org_rela_type_cd = '91'      
  AND P1.start_dt <= DATE'${batch_date}'
  AND P1.end_dt > DATE'${batch_date}'



 
;


/*添加表分析*/ 
ANALYZE TABLE ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_02;


/*===================第4组====================*/

DROP TABLE IF EXISTS ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_03;
CREATE GLOBAL TEMPORARY TABLE ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_03 (
 ar_agt_id  VARCHAR(80) -- 协议编号
  ,party_id  VARCHAR(80) -- 当事人编号
  ,emply_no  VARCHAR(60) -- 员工编号
  ,emply_name  VARCHAR(100) -- 员工姓名
  ,org_no  VARCHAR(80) -- 绩效系统机构号  
  ,mgmt_org_id  VARCHAR(80) -- 管理机构编号
  ,mgmt_org_name  VARCHAR(100) -- 管理机构名称
  ,ld_emply_no  VARCHAR(60) -- 管理机构领导编号
  ,ld_emply_name  VARCHAR(100) -- 管理机构领导姓名
  ,acvmnt_distri_ratio  DECIMAL(18,6) -- 业绩分配比例
)
compress(5,5)
DISTRIBUTED BY ( ar_agt_id );

INSERT INTO ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_03(
  ar_agt_id -- 协议编号
  ,party_id -- 当事人编号
  ,emply_no -- 员工编号
  ,emply_name -- 员工姓名
  ,org_no -- 绩效系统机构号  
  ,mgmt_org_id -- 管理机构编号
  ,mgmt_org_name -- 管理机构名称
  ,ld_emply_no -- 管理机构领导编号
  ,ld_emply_name -- 管理机构领导姓名
  ,acvmnt_distri_ratio -- 业绩分配比例
)
SELECT
  P1.ar_agt_id 
   AS ar_agt_id  -- 协议编号
  ,P2.party_id 
   AS party_id -- 当事人编号
  ,P1.emply_no 
   AS emply_no  -- 员工编号
  ,(CASE 
   WHEN P1.emply_no ~ '^[a-zA-Z]{1}' OR TRIM(COALESCE(P1.emply_no, '')) = ''
    THEN '虚拟行员' 
   ELSE P3.emply_name 
    END
   ) 
   AS emply_name  -- 员工姓名
  ,P1.org_no  AS org_no  -- 绩效系统机构号  
  ,P4.mgmt_org_id 
   AS mgmt_org_id  -- 管理机构编号
  ,P4.mgmt_org_name
   AS mgmt_org_name -- 管理机构名称
  ,P5.emply_no 
   AS ld_emply_no -- 管理机构领导编号
  ,P5.emply_name 
   AS ld_emply_name  -- 管理机构领导姓名
  ,P1.dpsit_acvmnt_distri_ratio
 AS acvmnt_distri_ratio  -- 业绩分配比例
 FROM ${iml_schema}.M_AR_DPSIT_PERF_BONS_H  P1 -- 存款业绩分成历史 
 WHERE EXISTS 
   (SELECT 1 
      FROM ${iml_schema}.M_PT_CORP S1 
     WHERE P2.party_id = S1.party_id
       AND S1.fin_org_type_cd IS NULL --代表公司客户
       AND S1.src_table_name = 'ecf_t01_cust_info_org' --限制来源表为公司客户表，不含同业客户
       AND S1.etl_dt = DATE'${batch_date}'
       AND S1.id_mark = 'I'
       AND S1.etl_task_no = 'ecff1'
   )



 
;


/*添加表分析*/ 
ANALYZE TABLE ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_03;


/*===================第5组====================*/

DROP TABLE IF EXISTS ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_04;
CREATE GLOBAL TEMPORARY TABLE ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_04 (
 party_id  VARCHAR(80) -- 当事人编号
  ,ar_agt_id  VARCHAR(80) -- 协议编号 
  ,emply_no  VARCHAR(60) -- 员工编号
  ,emply_name  VARCHAR(100) -- 员工姓名
  ,org_no  VARCHAR(80) -- 绩效系统机构号
  ,mgmt_org_id  VARCHAR(80) -- 管理机构编号
  ,mgmt_org_name  VARCHAR(100) -- 管理机构名称
  ,acvmnt_distri_ratio  DECIMAL(18,6) -- 业绩分配比例
)
compress(5,5)
DISTRIBUTED BY ( party_id );

INSERT INTO ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_04(
  party_id -- 当事人编号
  ,ar_agt_id -- 协议编号 
  ,emply_no -- 员工编号
  ,emply_name -- 员工姓名
  ,org_no -- 绩效系统机构号
  ,mgmt_org_id -- 管理机构编号
  ,mgmt_org_name -- 管理机构名称
  ,acvmnt_distri_ratio -- 业绩分配比例
)
SELECT
  P1.PARTY_ID    AS party_id  -- 当事人编号
  ,P1.AR_AGT_ID  AS ar_agt_id  -- 协议编号 
  ,P1.EMPLY_NO    AS emply_no  -- 员工编号
  ,P1.EMPLY_NAME    AS emply_name  -- 员工姓名
  ,P1.ORG_NO    AS org_no  -- 绩效系统机构号
  ,P1.MGMT_ORG_ID    AS mgmt_org_id -- 管理机构编号
  ,P1.MGMT_ORG_NAME    AS mgmt_org_name  -- 管理机构名称
  ,P1.ACVMNT_DISTRI_RATIO AS acvmnt_distri_ratio  -- 业绩分配比例
 FROM SELECT 
 party_id
   ,ar_agt_id
   ,ld_emply_no AS emply_no
   ,ld_emply_name AS emply_name
   ,org_no
   ,mgmt_org_id
   ,mgmt_org_name
   ,SUM(acvmnt_distri_ratio) AS acvmnt_distri_ratio
FROM TMP_C_PT_CORP_CUST_PERF_RELA_03
WHERE emply_name = '虚拟行员'
AND emply_no != null --虚拟行员向上汇总没有领导
GROUP BY 1,2,3,4,5,6,7 
UNION ALL 
SELECT 
 party_id
   ,ar_agt_id
   ,emply_no
   ,emply_name
   ,org_no
   ,mgmt_org_id
   ,mgmt_org_name
   ,acvmnt_distri_ratio
 FROM TMP_C_PT_CORP_CUST_PERF_RELA_03
 WHERE emply_name != '虚拟行员'
  P1 -- None 
  
 
;


/*添加表分析*/ 
ANALYZE TABLE ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_04;


/*===================第6组====================*/

DROP TABLE IF EXISTS ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_05;
CREATE GLOBAL TEMPORARY TABLE ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_05 (
 party_id  VARCHAR(80) -- 当事人编号
  ,perf_rela_type_cd  VARCHAR(20) -- 绩效关系类型代码
  ,emply_no  VARCHAR(60) -- 员工编号
  ,emply_name  VARCHAR(100) -- 员工姓名
)
compress(5,5)
DISTRIBUTED BY ( party_id );

INSERT INTO ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_05(
  party_id -- 当事人编号
  ,perf_rela_type_cd -- 绩效关系类型代码
  ,emply_no -- 员工编号
  ,emply_name -- 员工姓名
)
SELECT
  PARTY_ID    AS party_id  -- None
  ,PERF_RELA_TYPE_CD    AS perf_rela_type_cd  -- None
  ,EMPLY_NO    AS emply_no  -- None
  ,EMPLY_NAME 
 AS emply_name  -- None
 FROM SELECT DISTINCT
 P1.party_id
 ,'01'as perf_rela_type_cd
 ,P1.cust_mgr_emply_no as emply_no
 ,P1.cust_mgr_name as emply_name
FROM (SELECT
  party_id
  ,CASE WHEN emply_name = '虚拟行员' THEN DECODE(TRIM(COALESCE(ld_emply_no, '')), '', NULL, ld_emply_no) ELSE emply_no END AS cust_mgr_emply_no
  ,CASE WHEN emply_name = '虚拟行员' THEN DECODE(TRIM(COALESCE(ld_emply_name, '')), '', NULL, ld_emply_name) ELSE emply_name END AS cust_mgr_name
 FROM TMP_C_PT_CORP_CUST_PERF_RELA_03 
    ) P1
WHERE P1.cust_mgr_emply_no IS NOT NULL
AND P1.cust_mgr_name IS NOT NULL
  T -- None 
UNION ALL (
SELECT DISTINCT  --对公客户存款营销人关系信息
 P1.party_id
 ,'02'as perf_rela_type_cd
 ,P1.cust_mgr_emply_no
 ,P1.cust_mgr_name
FROM (SELECT 
  party_id
  ,emply_no AS cust_mgr_emply_no
  ,emply_name AS cust_mgr_name
  ,RANK() OVER (PARTITION BY party_id ORDER BY acvmnt_distri_ratio DESC) AS rank_no --排名
 FROM TMP_C_PT_CORP_CUST_PERF_RELA_04
    ) P1
WHERE P1.rank_no = 1
)  None -- None 
 ON None
  
 
;


/*添加表分析*/ 
ANALYZE TABLE ${icl_schema}.tmp_c_pt_corp_cust_perf_rela_05;


/*===================第7组====================*/

INSERT INTO ${icl_schema}.c_pt_corp_cust_perf_rela(
  etl_dt -- 数据日期
  ,lp_org_no -- 法人机构编号
  ,ecf_party_id -- 客户编号
  ,perf_rela_type_cd -- 绩效关系类型代码
  ,cust_mgr_emply_no -- 客户经理工号
  ,cust_mgr_name -- 客户经理名称
  ,mgmt_org_no -- 管理机构编号
)
SELECT 
  DATE'${batch_date}' 
   AS etl_dt  -- None
  ,'9999'  AS lp_org_no    -- None
  ,P1.party_id  AS ecf_party_id  -- None
  ,P1.perf_rela_type_cd    AS perf_rela_type_cd  -- None
  ,P1.emply_no    AS cust_mgr_emply_no  -- None
  ,P1.emply_name    AS cust_mgr_name  -- None
  ,P4.org_no  AS mgmt_org_no  -- None 
 FROM TMP_C_PT_CORP_CUST_PERF_RELA_05  P1 -- None 
   
 
;



/*添加目标表分析*/ 
\echo "4.analyze table" 
ANALYZE TABLE ${icl_schema}.c_pt_corp_cust_perf_rela;
