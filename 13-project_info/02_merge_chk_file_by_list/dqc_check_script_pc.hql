 -- 本次合并涉及的表名：agl_corp_cust_info_tf, agl_corp_np_loan_wrtoff_info_tf, agl_entity_prec_metal_txn_dtl_ta, agl_csn_trust_acct_pos_info_tf, agl_amass_dpsit_gold_acct_pos_info_tf, agl_corp_loan_estate_proj_info_tf, agl_coloan_dubil_info_tf, agl_corp_cust_simple_prft_tab_info_tf, agl_corp_cust_std_fin_prft_info_tf, agl_corp_cust_cashflow_tab_info_tf, agl_csn_fin_acct_pos_info_tf, agl_csn_trust_txn_dtl_ta, agl_csn_fund_aip_agt_info_tf, agl_np_loan_cust_ast_comut_debt_tf, agl_issue_bond_and_ibank_dpstrcp_info_tf, agl_ibank_txn_cntra_info_tf, agl_amass_dpsit_gold_rgl_agt_info_tf, agl_indv_loan_pledge_archive_info_tf, agl_supcha_core_corp_updwn_str_cust_info_tf, agl_corp_cust_simple_ast_liab_tab_info_tf, agl_coloan_ast_cls_modal_chg_info_tf, agl_indv_debit_card_info_tf, agl_imp_guar_letter_info_old_tf, agl_indv_agen_fx_fwd_txn_dtl_tf, agl_indv_dpsit_acct_info_tf, agl_indv_curr_dpsit_acct_txn_dtl_restore_ta, agl_debit_card_change_dtl_tf, agl_recv_bil_spc_argmt_mercht_info_tf, agl_csn_fin_txn_dtl_ta, agl_self_biz_fin_acct_pos_info_tf, agl_amass_dpsit_gold_txn_dtl_ta, agl_indv_curr_dpsit_acct_txn_dtl_ta, agl_recv_bil_agt_info_tf, agl_corp_rgl_dpsit_acct_info_tf, agl_corp_curr_dpsit_acct_info_tf, agl_indv_curr_dpsit_acct_info_tf, agl_csn_fund_txn_dtl_ta, agl_corp_agen_fx_fwd_txn_dtl_tf, agl_indv_cust_mch_ver_info_tf, agl_indv_cust_info_tf, agl_coloan_loan_ext_ext_info_tf, agl_coloan_unpr_agt_grp_mem_info_tf, agl_coloan_ast_risk_cls_info_tf, agl_all_mkt_bond_issue_main_info_tf, agl_bond_rat_info_tf, agl_bil_pledge_dtl_tf

 -- 本次合并共涉及 46 张表

 
-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-对公客户信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CORP_CUST_INFO_TF
--     表中文名：对公客户信息聚合
--     创建日期：2023-12-22 00:00:00
--     主键字段：CUST_IN_CD, BELONG_LP_ORG_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：取ecif内所有法人客户聚合层基本信息、扩展信息，大信贷取股东信息、eicc的一些关联人信息
--     更新记录：
--         2023-12-19 00:00:00 王穆军 新增映射文件信息



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_01
SELECT
       '对公客户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CORP_CUST_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_02
SELECT
       '对公客户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD, BELONG_LP_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CORP_CUST_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 CUST_IN_CD, BELONG_LP_ORG_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_03
SELECT
       '对公客户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD, BELONG_LP_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CORP_CUST_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(CUST_IN_CD,'') AS STRING), CAST(COALESCE(BELONG_LP_ORG_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_04
SELECT
       '对公客户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CORP_CUST_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_CUST_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_CUST_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CORP_CUST_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_CORP_CUST_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CORP_CUST_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'对公客户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('CUST_IN_CD, BELONG_LP_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-对公不良贷款核销信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CORP_NP_LOAN_WRTOFF_INFO_TF
--     表中文名：对公不良贷款核销信息聚合
--     创建日期：2023-12-25 00:00:00
--     主键字段：LOAN_CONT_NO, DUBIL_SER_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：主要包含法人信息、法人不良贷款借据信息及其核销明细。其中法人信息包含企业规模、信用等级、行业、是否上市等信息不良贷款信息及处置明细包含贷款形态、担保方式、不良借据核销金额利息等相关信息。
--     更新记录：
--         2023-12-25 00:00:00 王穆军 新增映射文件信息
--         2024-07-22 00:00:00 王穆军 修改文件与设计文档一致



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_01
SELECT
       '对公不良贷款核销信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_NP_LOAN_WRTOFF_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CORP_NP_LOAN_WRTOFF_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_02
SELECT
       '对公不良贷款核销信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_NP_LOAN_WRTOFF_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('LOAN_CONT_NO, DUBIL_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CORP_NP_LOAN_WRTOFF_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 LOAN_CONT_NO, DUBIL_SER_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_03
SELECT
       '对公不良贷款核销信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_NP_LOAN_WRTOFF_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('LOAN_CONT_NO, DUBIL_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CORP_NP_LOAN_WRTOFF_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(LOAN_CONT_NO,'') AS STRING), CAST(COALESCE(DUBIL_SER_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_04
SELECT
       '对公不良贷款核销信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_NP_LOAN_WRTOFF_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CORP_NP_LOAN_WRTOFF_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_NP_LOAN_WRTOFF_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_NP_LOAN_WRTOFF_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CORP_NP_LOAN_WRTOFF_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'对公不良贷款核销信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('LOAN_CONT_NO, DUBIL_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_NP_LOAN_WRTOFF_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-EVI-实物贵金属交易明细聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_ENTITY_PREC_METAL_TXN_DTL_TA
--     表中文名：实物贵金属交易明细聚合
--     创建日期：2023-12-26 00:00:00
--     主键字段：APP_FORM_SER_NO
--     归属层次：AGL-EVI
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含所有个人实物贵金属交易明细记录
--     更新记录：
--         2023-12-26 00:00:00 王穆军 新增映射文档
--         2024-01-17 00:00:00 王穆军 对标 删除【等级风险代码】字段



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_01
SELECT
       '实物贵金属交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_ENTITY_PREC_METAL_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_ENTITY_PREC_METAL_TXN_DTL_TA
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_02
SELECT
       '实物贵金属交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_ENTITY_PREC_METAL_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('APP_FORM_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_ENTITY_PREC_METAL_TXN_DTL_TA
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 APP_FORM_SER_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_03
SELECT
       '实物贵金属交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_ENTITY_PREC_METAL_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('APP_FORM_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_ENTITY_PREC_METAL_TXN_DTL_TA
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(APP_FORM_SER_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_04
SELECT
       '实物贵金属交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_ENTITY_PREC_METAL_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_ENTITY_PREC_METAL_TXN_DTL_TA 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_ENTITY_PREC_METAL_TXN_DTL_TA');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_ENTITY_PREC_METAL_TXN_DTL_TA')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_ENTITY_PREC_METAL_TXN_DTL_TA_PC' AS TASK_NAME -- 作业名
      ,'AGL_ENTITY_PREC_METAL_TXN_DTL_TA_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_ENTITY_PREC_METAL_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,'实物贵金属交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('APP_FORM_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ENTITY_PREC_METAL_TXN_DTL_TA_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-代销信托账户持仓信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CSN_TRUST_ACCT_POS_INFO_TF
--     表中文名：代销信托账户持仓信息聚合
--     创建日期：2023-12-26 00:00:00
--     主键字段：LP_ORG_NO, TXN_ACCT_NO, TA_ACCT_NO, TRUST_PROD_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：None
--     更新记录：
--         2023-12-26 00:00:00 王穆军 新增映射信息



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_01
SELECT
       '代销信托账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_TRUST_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CSN_TRUST_ACCT_POS_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_02
SELECT
       '代销信托账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_TRUST_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('LP_ORG_NO, TXN_ACCT_NO, TA_ACCT_NO, TRUST_PROD_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CSN_TRUST_ACCT_POS_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 LP_ORG_NO, TXN_ACCT_NO, TA_ACCT_NO, TRUST_PROD_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_03
SELECT
       '代销信托账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_TRUST_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('LP_ORG_NO, TXN_ACCT_NO, TA_ACCT_NO, TRUST_PROD_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CSN_TRUST_ACCT_POS_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(LP_ORG_NO,'') AS STRING), CAST(COALESCE(TXN_ACCT_NO,'') AS STRING), CAST(COALESCE(TA_ACCT_NO,'') AS STRING), CAST(COALESCE(TRUST_PROD_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_04
SELECT
       '代销信托账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_TRUST_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CSN_TRUST_ACCT_POS_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CSN_TRUST_ACCT_POS_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CSN_TRUST_ACCT_POS_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CSN_TRUST_ACCT_POS_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_CSN_TRUST_ACCT_POS_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CSN_TRUST_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'代销信托账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('LP_ORG_NO, TXN_ACCT_NO, TA_ACCT_NO, TRUST_PROD_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_ACCT_POS_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-积存金账户持仓信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF
--     表中文名：积存金账户持仓信息聚合
--     创建日期：2023-12-27 00:00:00
--     主键字段：AMASS_DPSIT_GOLD_ACCT_NO, LP_ORG_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含积存金客户的基本信息，积存账户信息，份额信息，产品信息以及合作机构的信息
--     更新记录：
--         2023-12-27 00:00:00 王穆军 新增SDM映射文档



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_01
SELECT
       '积存金账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_02
SELECT
       '积存金账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('AMASS_DPSIT_GOLD_ACCT_NO, LP_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 AMASS_DPSIT_GOLD_ACCT_NO, LP_ORG_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_03
SELECT
       '积存金账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('AMASS_DPSIT_GOLD_ACCT_NO, LP_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(AMASS_DPSIT_GOLD_ACCT_NO,'') AS STRING), CAST(COALESCE(LP_ORG_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_04
SELECT
       '积存金账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'积存金账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('AMASS_DPSIT_GOLD_ACCT_NO, LP_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_ACCT_POS_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-对公贷款房地产项目信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF
--     表中文名：对公贷款房地产项目信息聚合
--     创建日期：2023-12-28 00:00:00
--     主键字段：CUST_IN_CD, PROJ_CATEGORY_DESC, PROJ_SER_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包括了土地储备项目贷款、开发园区项目贷款、房地产项目贷款的信息（只包含瑞丰的数据）
--     更新记录：
--         2023-12-28 00:00:00 王穆军 新增映射文档



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_01
SELECT
       '对公贷款房地产项目信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_02
SELECT
       '对公贷款房地产项目信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD, PROJ_CATEGORY_DESC, PROJ_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 CUST_IN_CD, PROJ_CATEGORY_DESC, PROJ_SER_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_03
SELECT
       '对公贷款房地产项目信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD, PROJ_CATEGORY_DESC, PROJ_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(CUST_IN_CD,'') AS STRING), CAST(COALESCE(PROJ_CATEGORY_DESC,'') AS STRING), CAST(COALESCE(PROJ_SER_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_04
SELECT
       '对公贷款房地产项目信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'对公贷款房地产项目信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('CUST_IN_CD, PROJ_CATEGORY_DESC, PROJ_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_LOAN_ESTATE_PROJ_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-对公贷款借据信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_COLOAN_DUBIL_INFO_TF
--     表中文名：对公贷款借据信息聚合
--     创建日期：2023-12-28 00:00:00
--     主键字段：LOAN_CONT_NO, DUBIL_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：业务范围涵盖资产负债表内对公贷款产品的借据信息，如农业工业贷款、国际/国内融资、垫款、透支账户等。字段范围包括借据的基本信息、利率利息、合同信息、客户信息、归属信息等。
--     更新记录：
--         2023-12-28 00:00:00 王穆军 新增映射文档信息



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_01
SELECT
       '对公贷款借据信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_DUBIL_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_COLOAN_DUBIL_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_02
SELECT
       '对公贷款借据信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_DUBIL_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('LOAN_CONT_NO, DUBIL_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_COLOAN_DUBIL_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 LOAN_CONT_NO, DUBIL_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_03
SELECT
       '对公贷款借据信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_DUBIL_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('LOAN_CONT_NO, DUBIL_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_COLOAN_DUBIL_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(LOAN_CONT_NO,'') AS STRING), CAST(COALESCE(DUBIL_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_04
SELECT
       '对公贷款借据信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_DUBIL_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_COLOAN_DUBIL_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_COLOAN_DUBIL_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_COLOAN_DUBIL_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_COLOAN_DUBIL_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_COLOAN_DUBIL_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_COLOAN_DUBIL_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'对公贷款借据信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('LOAN_CONT_NO, DUBIL_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_DUBIL_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-对公客户简易利润表信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF
--     表中文名：对公客户简易利润表信息聚合
--     创建日期：2023-12-28 00:00:00
--     主键字段：CUST_IN_CD, REPORT_YM, BELONG_ORG_NO, SRC_TAB_NAME
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含简易报表的企业客户财务报表中利润相关信息、贷前企业评级应用字段以及贷后财务检查涉及的利润字段。同时包含客户基本信息、归属机构、注册行业币种、信贷员信息等公共字段信息。不包含标准报表企业客户。
--     更新记录：
--         2023-12-28 00:00:00 王穆军 新增映射文档



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_01
SELECT
       '对公客户简易利润表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_02
SELECT
       '对公客户简易利润表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD, REPORT_YM, BELONG_ORG_NO, SRC_TAB_NAME' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 CUST_IN_CD, REPORT_YM, BELONG_ORG_NO, SRC_TAB_NAME
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_03
SELECT
       '对公客户简易利润表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD, REPORT_YM, BELONG_ORG_NO, SRC_TAB_NAME' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(CUST_IN_CD,'') AS STRING), CAST(COALESCE(REPORT_YM,'') AS STRING), CAST(COALESCE(BELONG_ORG_NO,'') AS STRING), CAST(COALESCE(SRC_TAB_NAME,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_04
SELECT
       '对公客户简易利润表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'对公客户简易利润表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('CUST_IN_CD, REPORT_YM, BELONG_ORG_NO, SRC_TAB_NAME' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_PRFT_TAB_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-对公客户标准利润表信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF
--     表中文名：对公客户标准利润表信息聚合
--     创建日期：2023-12-28 00:00:00
--     主键字段：CUST_IN_CD, REPORT_YM, BELONG_ORG_NO, SRC_TAB_NAME
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含标准报表、金融报表、人行报表、瑞丰报表企业客户财务报表中利润相关信息、贷前评分应用字段以及贷后财务检查涉及的利润字段。同时包含客户基本信息、归属机构、注册行业币种、信贷员信息等公共字段信息。不包含简易报表企业客户。
--     更新记录：
--         2023-12-28 00:00:00 王穆军 NEW



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_01
SELECT
       '对公客户标准利润表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_02
SELECT
       '对公客户标准利润表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD, REPORT_YM, BELONG_ORG_NO, SRC_TAB_NAME' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 CUST_IN_CD, REPORT_YM, BELONG_ORG_NO, SRC_TAB_NAME
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_03
SELECT
       '对公客户标准利润表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD, REPORT_YM, BELONG_ORG_NO, SRC_TAB_NAME' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(CUST_IN_CD,'') AS STRING), CAST(COALESCE(REPORT_YM,'') AS STRING), CAST(COALESCE(BELONG_ORG_NO,'') AS STRING), CAST(COALESCE(SRC_TAB_NAME,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_04
SELECT
       '对公客户标准利润表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'对公客户标准利润表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('CUST_IN_CD, REPORT_YM, BELONG_ORG_NO, SRC_TAB_NAME' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_STD_FIN_PRFT_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-对公客户现金流量表信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF
--     表中文名：对公客户现金流量表信息聚合
--     创建日期：2023-12-28 00:00:00
--     主键字段：CUST_IN_CD, REPORT_DT, BELONG_ORG_NO, B_SRC_TAB_EN_NAME, FIN_STAT_PERIOD_CD
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含金融类、大中型企业的经营、投资、筹资活动产生的现金流量，以及贷前评分、贷后财务健康检查、风险预警涉及的现金流量字段。同时包含客户基本信息、注册行业、币种、归属信息、表维护时间等公共字段信息。
--     更新记录：
--         2023-12-28 00:00:00 王穆军 new



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_01
SELECT
       '对公客户现金流量表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_02
SELECT
       '对公客户现金流量表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD, REPORT_DT, BELONG_ORG_NO, B_SRC_TAB_EN_NAME, FIN_STAT_PERIOD_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 CUST_IN_CD, REPORT_DT, BELONG_ORG_NO, B_SRC_TAB_EN_NAME, FIN_STAT_PERIOD_CD
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_03
SELECT
       '对公客户现金流量表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD, REPORT_DT, BELONG_ORG_NO, B_SRC_TAB_EN_NAME, FIN_STAT_PERIOD_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(CUST_IN_CD,'') AS STRING), CAST(COALESCE(REPORT_DT,'') AS STRING), CAST(COALESCE(BELONG_ORG_NO,'') AS STRING), CAST(COALESCE(B_SRC_TAB_EN_NAME,'') AS STRING), CAST(COALESCE(FIN_STAT_PERIOD_CD,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_04
SELECT
       '对公客户现金流量表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'对公客户现金流量表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('CUST_IN_CD, REPORT_DT, BELONG_ORG_NO, B_SRC_TAB_EN_NAME, FIN_STAT_PERIOD_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_CASHFLOW_TAB_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-代销理财账户持仓信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CSN_FIN_ACCT_POS_INFO_TF
--     表中文名：代销理财账户持仓信息聚合
--     创建日期：2023-12-19 00:00:00
--     主键字段：TXN_ACCT_NO, FIN_PROD_CD, TXN_HAPP_SELLER_NO, TA_CD
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：代销理财，当前账户持仓的信息，包含该账户持有份额、冻结份额等信息
--     更新记录：
--         2023-12-19 00:00:00 王穆军 新增
--         2024-01-03 00:00:00 王穆军 修改
--         2024-01-12 00:00:00 王穆军 修改对标 主键名称等信息



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_01
SELECT
       '代销理财账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FIN_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CSN_FIN_ACCT_POS_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_02
SELECT
       '代销理财账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FIN_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('TXN_ACCT_NO, FIN_PROD_CD, TXN_HAPP_SELLER_NO, TA_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CSN_FIN_ACCT_POS_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 TXN_ACCT_NO, FIN_PROD_CD, TXN_HAPP_SELLER_NO, TA_CD
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_03
SELECT
       '代销理财账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FIN_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('TXN_ACCT_NO, FIN_PROD_CD, TXN_HAPP_SELLER_NO, TA_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CSN_FIN_ACCT_POS_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(TXN_ACCT_NO,'') AS STRING), CAST(COALESCE(FIN_PROD_CD,'') AS STRING), CAST(COALESCE(TXN_HAPP_SELLER_NO,'') AS STRING), CAST(COALESCE(TA_CD,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_04
SELECT
       '代销理财账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FIN_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CSN_FIN_ACCT_POS_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CSN_FIN_ACCT_POS_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CSN_FIN_ACCT_POS_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CSN_FIN_ACCT_POS_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_CSN_FIN_ACCT_POS_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CSN_FIN_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'代销理财账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('TXN_ACCT_NO, FIN_PROD_CD, TXN_HAPP_SELLER_NO, TA_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_ACCT_POS_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-EVI-代销信托交易明细聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CSN_TRUST_TXN_DTL_TA
--     表中文名：代销信托交易明细聚合
--     创建日期：2023-12-19 00:00:00
--     主键字段：APP_FORM_SER_NO, TRUST_BIZ_CFM_CD, TA_SER_NO
--     归属层次：AGL-EVI
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含信托代销客户购买、赎回、分红、到期兑付等的交易信息 包含信托代销个人和机构客户，对于所有类型产品的所有交易的交易流水信息
--     更新记录：
--         2023-12-19 00:00:00 王穆军 新增
--         2024-08-05 00:00:00 王穆军 修改筛选逻辑



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_01
SELECT
       '代销信托交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_TRUST_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CSN_TRUST_TXN_DTL_TA
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_02
SELECT
       '代销信托交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_TRUST_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('APP_FORM_SER_NO, TRUST_BIZ_CFM_CD, TA_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CSN_TRUST_TXN_DTL_TA
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 APP_FORM_SER_NO, TRUST_BIZ_CFM_CD, TA_SER_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_03
SELECT
       '代销信托交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_TRUST_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('APP_FORM_SER_NO, TRUST_BIZ_CFM_CD, TA_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CSN_TRUST_TXN_DTL_TA
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(APP_FORM_SER_NO,'') AS STRING), CAST(COALESCE(TRUST_BIZ_CFM_CD,'') AS STRING), CAST(COALESCE(TA_SER_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_04
SELECT
       '代销信托交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_TRUST_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CSN_TRUST_TXN_DTL_TA 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CSN_TRUST_TXN_DTL_TA');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CSN_TRUST_TXN_DTL_TA')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CSN_TRUST_TXN_DTL_TA_PC' AS TASK_NAME -- 作业名
      ,'AGL_CSN_TRUST_TXN_DTL_TA_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CSN_TRUST_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,'代销信托交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('APP_FORM_SER_NO, TRUST_BIZ_CFM_CD, TA_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_TRUST_TXN_DTL_TA_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-代销基金定投协议信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CSN_FUND_AIP_AGT_INFO_TF
--     表中文名：代销基金定投协议信息聚合
--     创建日期：2023-12-19 00:00:00
--     主键字段：FUND_AIP_AGT_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：通过基金定投协议表获取定投的相关信息，通过定投协议与基金账户、交易账号信息及基金基本信息、TA信息及包含客户内码的相关表进行关联获取基金开销户日期、基金账号、基金代码名称、客户内码等具体信息
--     更新记录：
--         2023-12-19 00:00:00 王穆军 新增 新财富代销系统
--         2023-01-09 00:00:00 王穆军 码值 探查 条件修改



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_01
SELECT
       '代销基金定投协议信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FUND_AIP_AGT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CSN_FUND_AIP_AGT_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_02
SELECT
       '代销基金定投协议信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FUND_AIP_AGT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('FUND_AIP_AGT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CSN_FUND_AIP_AGT_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 FUND_AIP_AGT_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_03
SELECT
       '代销基金定投协议信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FUND_AIP_AGT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('FUND_AIP_AGT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CSN_FUND_AIP_AGT_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(FUND_AIP_AGT_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_04
SELECT
       '代销基金定投协议信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FUND_AIP_AGT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CSN_FUND_AIP_AGT_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CSN_FUND_AIP_AGT_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CSN_FUND_AIP_AGT_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CSN_FUND_AIP_AGT_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_CSN_FUND_AIP_AGT_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CSN_FUND_AIP_AGT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'代销基金定投协议信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('FUND_AIP_AGT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_AIP_AGT_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-不良贷款客户资产抵债信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF
--     表中文名：不良贷款客户资产抵债信息聚合
--     创建日期：2023-12-21 00:00:00
--     主键字段：COMUT_DEBT_AST_NO, COMUT_DEBT_AST_DISP_BATCH_NO, COMUT_DEBT_AST_COMUT_DEBT_SER_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：主要包含抵债资产基本及明细信息、资产抵债时明细以及资产处置时明细。
--     更新记录：
--         2023-12-19 00:00:00 王穆军 新增映射文件信息



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_01
SELECT
       '不良贷款客户资产抵债信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_02
SELECT
       '不良贷款客户资产抵债信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('COMUT_DEBT_AST_NO, COMUT_DEBT_AST_DISP_BATCH_NO, COMUT_DEBT_AST_COMUT_DEBT_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 COMUT_DEBT_AST_NO, COMUT_DEBT_AST_DISP_BATCH_NO, COMUT_DEBT_AST_COMUT_DEBT_SER_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_03
SELECT
       '不良贷款客户资产抵债信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('COMUT_DEBT_AST_NO, COMUT_DEBT_AST_DISP_BATCH_NO, COMUT_DEBT_AST_COMUT_DEBT_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(COMUT_DEBT_AST_NO,'') AS STRING), CAST(COALESCE(COMUT_DEBT_AST_DISP_BATCH_NO,'') AS STRING), CAST(COALESCE(COMUT_DEBT_AST_COMUT_DEBT_SER_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_04
SELECT
       '不良贷款客户资产抵债信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF' AS TAB_EN_NAME -- 表英文名
      ,'不良贷款客户资产抵债信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('COMUT_DEBT_AST_NO, COMUT_DEBT_AST_DISP_BATCH_NO, COMUT_DEBT_AST_COMUT_DEBT_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_NP_LOAN_CUST_AST_COMUT_DEBT_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-发行债券及同业存单信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF
--     表中文名：发行债券及同业存单信息聚合
--     创建日期：2023-01-03 00:00:00
--     主键字段：APP_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含债券的基本信息，债券的发行信息，发行机构信息，发行机构评级信息
--     更新记录：
--         2023-01-03 00:00:00 王穆军 新增
--         2024-01-12 00:00:00 wmj 对标修改



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_01
SELECT
       '发行债券及同业存单信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_02
SELECT
       '发行债券及同业存单信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('APP_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 APP_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_03
SELECT
       '发行债券及同业存单信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('APP_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(APP_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_04
SELECT
       '发行债券及同业存单信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'发行债券及同业存单信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('APP_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ISSUE_BOND_AND_IBANK_DPSTRCP_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-同业交易对手信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_IBANK_TXN_CNTRA_INFO_TF
--     表中文名：同业交易对手信息聚合
--     创建日期：2023-01-03 00:00:00
--     主键字段：TXN_CNTRA_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含交易对手信息表的全部数据，以及交易对手总行表里有效的数据。详细的 交易对手信息、交易对手联系人信息、交易对手总行信息。
--     更新记录：
--         2023-01-03 00:00:00 王穆军 新增



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_01
SELECT
       '同业交易对手信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_IBANK_TXN_CNTRA_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_IBANK_TXN_CNTRA_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_02
SELECT
       '同业交易对手信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_IBANK_TXN_CNTRA_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('TXN_CNTRA_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_IBANK_TXN_CNTRA_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 TXN_CNTRA_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_03
SELECT
       '同业交易对手信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_IBANK_TXN_CNTRA_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('TXN_CNTRA_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_IBANK_TXN_CNTRA_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(TXN_CNTRA_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_04
SELECT
       '同业交易对手信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_IBANK_TXN_CNTRA_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_IBANK_TXN_CNTRA_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_IBANK_TXN_CNTRA_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_IBANK_TXN_CNTRA_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_IBANK_TXN_CNTRA_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_IBANK_TXN_CNTRA_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_IBANK_TXN_CNTRA_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'同业交易对手信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('TXN_CNTRA_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IBANK_TXN_CNTRA_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-积存金定期协议信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF
--     表中文名：积存金定期协议信息聚合
--     创建日期：2023-01-03 00:00:00
--     主键字段：RGL_AMASS_DPSIT_AGT_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含积存金的定期协议的协议等信息
--     更新记录：
--         2023-01-03 00:00:00 王穆军 新增



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_01
SELECT
       '积存金定期协议信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_02
SELECT
       '积存金定期协议信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('RGL_AMASS_DPSIT_AGT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 RGL_AMASS_DPSIT_AGT_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_03
SELECT
       '积存金定期协议信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('RGL_AMASS_DPSIT_AGT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(RGL_AMASS_DPSIT_AGT_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_04
SELECT
       '积存金定期协议信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'积存金定期协议信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('RGL_AMASS_DPSIT_AGT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_RGL_AGT_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-个人押品档案信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF
--     表中文名：个人押品档案信息聚合
--     创建日期：2024-04-26 00:00:00
--     主键字段：PLEDGE_NO, MTG_SER_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：记录全部个人贷款中抵质押品档案的各分类的详细信息，如房产，车辆，土地，机器设备，船舶，票据，存单等抵质押物的详细信息；取抵质押物功能层面应用记录的详细信息
--     更新记录：
--         2024-04-26 00:00:00 王穆军 拆分个人贷款押品档案信息聚合



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_01
SELECT
       '个人押品档案信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_02
SELECT
       '个人押品档案信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('PLEDGE_NO, MTG_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 PLEDGE_NO, MTG_SER_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_03
SELECT
       '个人押品档案信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('PLEDGE_NO, MTG_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(PLEDGE_NO,'') AS STRING), CAST(COALESCE(MTG_SER_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_04
SELECT
       '个人押品档案信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'个人押品档案信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('PLEDGE_NO, MTG_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_LOAN_PLEDGE_ARCHIVE_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-供应链核心企业上下游客户信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF
--     表中文名：供应链核心企业上下游客户信息聚合
--     创建日期：2023-01-03 00:00:00
--     主键字段：CORE_CORP_NO, B_SRC_TAB_EN_NAME, UPDWN_STR_CORP_NO, UPDWN_STR_CORP_DOCTYP_CD, UPDWN_STR_CORP_DOC_NO, UPDWN_STR_CORP_BELONG_ORG_NO

--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：提供供应链相关产品（订单E融、应收E融、政采E融、保理E融、链贷通、应付E融、仓单E融、链汇E融）中核心企业及其上下游企业的基本信息。
--     更新记录：
--         2023-01-03 00:00:00 王穆军 new



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_01
SELECT
       '供应链核心企业上下游客户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_02
SELECT
       '供应链核心企业上下游客户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CORE_CORP_NO, B_SRC_TAB_EN_NAME, UPDWN_STR_CORP_NO, UPDWN_STR_CORP_DOCTYP_CD, UPDWN_STR_CORP_DOC_NO, UPDWN_STR_CORP_BELONG_ORG_NO
' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 CORE_CORP_NO, B_SRC_TAB_EN_NAME, UPDWN_STR_CORP_NO, UPDWN_STR_CORP_DOCTYP_CD, UPDWN_STR_CORP_DOC_NO, UPDWN_STR_CORP_BELONG_ORG_NO

 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_03
SELECT
       '供应链核心企业上下游客户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CORE_CORP_NO, B_SRC_TAB_EN_NAME, UPDWN_STR_CORP_NO, UPDWN_STR_CORP_DOCTYP_CD, UPDWN_STR_CORP_DOC_NO, UPDWN_STR_CORP_BELONG_ORG_NO
' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(CORE_CORP_NO,'') AS STRING), CAST(COALESCE(B_SRC_TAB_EN_NAME,'') AS STRING), CAST(COALESCE(UPDWN_STR_CORP_NO,'') AS STRING), CAST(COALESCE(UPDWN_STR_CORP_DOCTYP_CD,'') AS STRING), CAST(COALESCE(UPDWN_STR_CORP_DOC_NO,'') AS STRING), CAST(COALESCE(UPDWN_STR_CORP_BELONG_ORG_NO
,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_04
SELECT
       '供应链核心企业上下游客户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'供应链核心企业上下游客户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('CORE_CORP_NO, B_SRC_TAB_EN_NAME, UPDWN_STR_CORP_NO, UPDWN_STR_CORP_DOCTYP_CD, UPDWN_STR_CORP_DOC_NO, UPDWN_STR_CORP_BELONG_ORG_NO
' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-对公客户简易资产负债表信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF
--     表中文名：对公客户简易资产负债表信息聚合
--     创建日期：2023-01-04 00:00:00
--     主键字段：CUST_IN_CD, REPORT_YM, BELONG_ORG_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含简易报表的企业客户财务报表中资产负债相关信息、贷前企业评级应用字段以及贷后财务检查涉及的资产负债字段。同时包含客户基本信息、归属机构、注册行业币种、信贷员信息等公共字段信息。不包含标准报表企业客户。
--     更新记录：
--         2023-01-04 00:00:00 王穆军 new



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_01
SELECT
       '对公客户简易资产负债表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_02
SELECT
       '对公客户简易资产负债表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD, REPORT_YM, BELONG_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 CUST_IN_CD, REPORT_YM, BELONG_ORG_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_03
SELECT
       '对公客户简易资产负债表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD, REPORT_YM, BELONG_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(CUST_IN_CD,'') AS STRING), CAST(COALESCE(REPORT_YM,'') AS STRING), CAST(COALESCE(BELONG_ORG_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_04
SELECT
       '对公客户简易资产负债表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'对公客户简易资产负债表信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('CUST_IN_CD, REPORT_YM, BELONG_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CUST_SIMPLE_AST_LIAB_TAB_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-对公贷款资产分类形态变更信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF
--     表中文名：对公贷款资产分类形态变更信息聚合
--     创建日期：2023-01-04 00:00:00
--     主键字段：LOAN_CONT_NO, DUBIL_NO, LOAN_MODAL_MODIF_NOTICE_NO, SRC_TAB_TYPE_CD
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：业务范围涵盖对公贷款产品如农、工业、商业和服务业、房地产、项目开发、国际贸易、垫款等表内借据资产分类形态变更信息，包括原四、五级形态，新四、五级形态，涉及到柜面核心、大信贷平台、供应链系统、在线融资系统、国际结算业务系统
--     更新记录：
--         2023-01-04 00:00:00 王穆军 new



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_01
SELECT
       '对公贷款资产分类形态变更信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_02
SELECT
       '对公贷款资产分类形态变更信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('LOAN_CONT_NO, DUBIL_NO, LOAN_MODAL_MODIF_NOTICE_NO, SRC_TAB_TYPE_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 LOAN_CONT_NO, DUBIL_NO, LOAN_MODAL_MODIF_NOTICE_NO, SRC_TAB_TYPE_CD
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_03
SELECT
       '对公贷款资产分类形态变更信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('LOAN_CONT_NO, DUBIL_NO, LOAN_MODAL_MODIF_NOTICE_NO, SRC_TAB_TYPE_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(LOAN_CONT_NO,'') AS STRING), CAST(COALESCE(DUBIL_NO,'') AS STRING), CAST(COALESCE(LOAN_MODAL_MODIF_NOTICE_NO,'') AS STRING), CAST(COALESCE(SRC_TAB_TYPE_CD,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_04
SELECT
       '对公贷款资产分类形态变更信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'对公贷款资产分类形态变更信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('LOAN_CONT_NO, DUBIL_NO, LOAN_MODAL_MODIF_NOTICE_NO, SRC_TAB_TYPE_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_CLS_MODAL_CHG_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-个人借记卡信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_INDV_DEBIT_CARD_INFO_TF
--     表中文名：个人借记卡信息聚合
--     创建日期：2023-12-13 00:00:00
--     主键字段：DEBIT_CARD_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：None
--     更新记录：
--         2023-12-13 00:00:00 游崔龙 新增
--         2024-01-11 00:00:00 游崔龙 对标
--         2024-02-20 00:00:00 王穆军 修改映射信息
--         2024-04-19 00:00:00 王穆军 修改客户内码去除空值的逻辑
--         2024-07-23 00:00:00 王穆军 修改与设计文档一致



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_01
SELECT
       '个人借记卡信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_DEBIT_CARD_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_INDV_DEBIT_CARD_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_02
SELECT
       '个人借记卡信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_DEBIT_CARD_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('DEBIT_CARD_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_INDV_DEBIT_CARD_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 DEBIT_CARD_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_03
SELECT
       '个人借记卡信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_DEBIT_CARD_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('DEBIT_CARD_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_INDV_DEBIT_CARD_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(DEBIT_CARD_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_04
SELECT
       '个人借记卡信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_DEBIT_CARD_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_INDV_DEBIT_CARD_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_DEBIT_CARD_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_DEBIT_CARD_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_INDV_DEBIT_CARD_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_INDV_DEBIT_CARD_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_INDV_DEBIT_CARD_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'个人借记卡信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('DEBIT_CARD_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DEBIT_CARD_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-进口保函信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_IMP_GUAR_LETTER_INFO_OLD_TF
--     表中文名：进口保函信息聚合
--     创建日期：2023-12-27 00:00:00
--     主键字段：GUAR_LETTER_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：None
--     更新记录：
--         2023-12-27 00:00:00 游崔龙 新增
--         2024-01-16 00:00:00 游崔龙 对标
--         2024-02-21 00:00:00 王穆军 修改错误



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_01
SELECT
       '进口保函信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_IMP_GUAR_LETTER_INFO_OLD_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_IMP_GUAR_LETTER_INFO_OLD_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_02
SELECT
       '进口保函信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_IMP_GUAR_LETTER_INFO_OLD_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('GUAR_LETTER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_IMP_GUAR_LETTER_INFO_OLD_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 GUAR_LETTER_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_03
SELECT
       '进口保函信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_IMP_GUAR_LETTER_INFO_OLD_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('GUAR_LETTER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_IMP_GUAR_LETTER_INFO_OLD_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(GUAR_LETTER_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_04
SELECT
       '进口保函信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_IMP_GUAR_LETTER_INFO_OLD_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_IMP_GUAR_LETTER_INFO_OLD_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_IMP_GUAR_LETTER_INFO_OLD_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_IMP_GUAR_LETTER_INFO_OLD_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_IMP_GUAR_LETTER_INFO_OLD_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_IMP_GUAR_LETTER_INFO_OLD_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_IMP_GUAR_LETTER_INFO_OLD_TF' AS TAB_EN_NAME -- 表英文名
      ,'进口保函信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('GUAR_LETTER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_IMP_GUAR_LETTER_INFO_OLD_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-EVI-个人代客外汇远期交易明细聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF
--     表中文名：个人代客外汇远期交易明细聚合
--     创建日期：2024-01-04 00:00:00
--     主键字段：
--     归属层次：AGL-EVI
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：None
--     更新记录：
--         2024-01-05 00:00:00 游崔龙 新增
--         2024-01-18 00:00:00 游崔龙 对标
--         2024-02-20 00:00:00 王穆军 优化SDM文件，修改关联方式



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_01
SELECT
       '个人代客外汇远期交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_02
SELECT
       '个人代客外汇远期交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CORE_ORIG_TXN_SER_NO, CORE_SUB_TXN_SER_NO, TXN_DT, CUST_IN_CD, CUST_NAME, DOCTYP_CD, DOC_NO, TXN_ORG_NO, LP_ORG_NO, TXN_TELR_NO, TXN_AUTH_TELR_NO_1, TXN_AUTH_TELR_NO_2, INTER_STL_CUST_MGR_NO, BUY_AMT, BUY_QUOT, QUOT_TYPE_CD, BUY_CURR_CD, BUY_CASH_RMT_CD, BUY_ACCT_NO_TYPE_CD, BUY_ACCT_NO, BUY_RBMRK_CD, SELL_AMT, SELL_CURR_CD, SELL_QUOT, SELL_CASH_RMT_CD, SELL_ACCT_NO_TYPE_CD, SELL_ACCT_NO, SELL_RBMRK_CD, EXCH_RATE_BNCHMK_CURR_CD, FX_STL_SALE_STAT_CD, AGEN_DERIV_TXN_SRC_CD, AGEN_DERIV_TXN_CATE_CD, SPOT_EXCH_RATE, OBANK_PRFT_LOSS_AMT, CUST_PRFT_LOSS_AMT, NOT_YET_DLVY_AMT, INIT_TXN_SER_NO, FINAL_TXN_STEP_CD, ESPEC_TXN_TYPE_CD, RECORD_TYPE_CD, SUMMARY_CD, REM, MRG_RATIO, MRG_AMT, VAL_DT, MATU_DT, CHS_TERM_BEGIN_DT, TXN_TM_STAMP, TXN_TERM_MODE_CD, GRACE_MATU_DT, CO_DEAL_DT' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
      CAST(COALESCE(CORE_ORIG_TXN_SER_NO,'') AS STRING), CAST(COALESCE(CORE_SUB_TXN_SER_NO,'') AS STRING), CAST(COALESCE(TXN_DT,'') AS STRING), CAST(COALESCE(CUST_IN_CD,'') AS STRING), CAST(COALESCE(CUST_NAME,'') AS STRING), CAST(COALESCE(DOCTYP_CD,'') AS STRING), CAST(COALESCE(DOC_NO,'') AS STRING), CAST(COALESCE(TXN_ORG_NO,'') AS STRING), CAST(COALESCE(LP_ORG_NO,'') AS STRING), CAST(COALESCE(TXN_TELR_NO,'') AS STRING), CAST(COALESCE(TXN_AUTH_TELR_NO_1,'') AS STRING), CAST(COALESCE(TXN_AUTH_TELR_NO_2,'') AS STRING), CAST(COALESCE(INTER_STL_CUST_MGR_NO,'') AS STRING), CAST(COALESCE(CAST(BUY_AMT AS STRING),'') AS STRING), CAST(COALESCE(CAST(BUY_QUOT AS STRING),'') AS STRING), CAST(COALESCE(QUOT_TYPE_CD,'') AS STRING), CAST(COALESCE(BUY_CURR_CD,'') AS STRING), CAST(COALESCE(BUY_CASH_RMT_CD,'') AS STRING), CAST(COALESCE(BUY_ACCT_NO_TYPE_CD,'') AS STRING), CAST(COALESCE(BUY_ACCT_NO,'') AS STRING), CAST(COALESCE(BUY_RBMRK_CD,'') AS STRING), CAST(COALESCE(CAST(SELL_AMT AS STRING),'') AS STRING), CAST(COALESCE(SELL_CURR_CD,'') AS STRING), CAST(COALESCE(CAST(SELL_QUOT AS STRING),'') AS STRING), CAST(COALESCE(SELL_CASH_RMT_CD,'') AS STRING), CAST(COALESCE(SELL_ACCT_NO_TYPE_CD,'') AS STRING), CAST(COALESCE(SELL_ACCT_NO,'') AS STRING), CAST(COALESCE(SELL_RBMRK_CD,'') AS STRING), CAST(COALESCE(EXCH_RATE_BNCHMK_CURR_CD,'') AS STRING), CAST(COALESCE(FX_STL_SALE_STAT_CD,'') AS STRING), CAST(COALESCE(AGEN_DERIV_TXN_SRC_CD,'') AS STRING), CAST(COALESCE(AGEN_DERIV_TXN_CATE_CD,'') AS STRING), CAST(COALESCE(CAST(SPOT_EXCH_RATE AS STRING),'') AS STRING), CAST(COALESCE(CAST(OBANK_PRFT_LOSS_AMT AS STRING),'') AS STRING), CAST(COALESCE(CAST(CUST_PRFT_LOSS_AMT AS STRING),'') AS STRING), CAST(COALESCE(CAST(NOT_YET_DLVY_AMT AS STRING),'') AS STRING), CAST(COALESCE(INIT_TXN_SER_NO,'') AS STRING), CAST(COALESCE(FINAL_TXN_STEP_CD,'') AS STRING), CAST(COALESCE(ESPEC_TXN_TYPE_CD,'') AS STRING), CAST(COALESCE(RECORD_TYPE_CD,'') AS STRING), CAST(COALESCE(SUMMARY_CD,'') AS STRING), CAST(COALESCE(REM,'') AS STRING), CAST(COALESCE(CAST(MRG_RATIO AS STRING),'') AS STRING), CAST(COALESCE(CAST(MRG_AMT AS STRING),'') AS STRING), CAST(COALESCE(VAL_DT,'') AS STRING), CAST(COALESCE(MATU_DT,'') AS STRING), CAST(COALESCE(CHS_TERM_BEGIN_DT,'') AS STRING), CAST(COALESCE(TXN_TM_STAMP,'') AS STRING), CAST(COALESCE(TXN_TERM_MODE_CD,'') AS STRING), CAST(COALESCE(GRACE_MATU_DT,'') AS STRING), CAST(COALESCE(CO_DEAL_DT,'') AS STRING)
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_03
SELECT
       '个人代客外汇远期交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CORE_ORIG_TXN_SER_NO, CORE_SUB_TXN_SER_NO, TXN_DT, CUST_IN_CD, CUST_NAME, DOCTYP_CD, DOC_NO, TXN_ORG_NO, LP_ORG_NO, TXN_TELR_NO, TXN_AUTH_TELR_NO_1, TXN_AUTH_TELR_NO_2, INTER_STL_CUST_MGR_NO, BUY_AMT, BUY_QUOT, QUOT_TYPE_CD, BUY_CURR_CD, BUY_CASH_RMT_CD, BUY_ACCT_NO_TYPE_CD, BUY_ACCT_NO, BUY_RBMRK_CD, SELL_AMT, SELL_CURR_CD, SELL_QUOT, SELL_CASH_RMT_CD, SELL_ACCT_NO_TYPE_CD, SELL_ACCT_NO, SELL_RBMRK_CD, EXCH_RATE_BNCHMK_CURR_CD, FX_STL_SALE_STAT_CD, AGEN_DERIV_TXN_SRC_CD, AGEN_DERIV_TXN_CATE_CD, SPOT_EXCH_RATE, OBANK_PRFT_LOSS_AMT, CUST_PRFT_LOSS_AMT, NOT_YET_DLVY_AMT, INIT_TXN_SER_NO, FINAL_TXN_STEP_CD, ESPEC_TXN_TYPE_CD, RECORD_TYPE_CD, SUMMARY_CD, REM, MRG_RATIO, MRG_AMT, VAL_DT, MATU_DT, CHS_TERM_BEGIN_DT, TXN_TM_STAMP, TXN_TERM_MODE_CD, GRACE_MATU_DT, CO_DEAL_DT' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
 
      CAST(COALESCE(CORE_ORIG_TXN_SER_NO,'') AS STRING), CAST(COALESCE(CORE_SUB_TXN_SER_NO,'') AS STRING), CAST(COALESCE(TXN_DT,'') AS STRING), CAST(COALESCE(CUST_IN_CD,'') AS STRING), CAST(COALESCE(CUST_NAME,'') AS STRING), CAST(COALESCE(DOCTYP_CD,'') AS STRING), CAST(COALESCE(DOC_NO,'') AS STRING), CAST(COALESCE(TXN_ORG_NO,'') AS STRING), CAST(COALESCE(LP_ORG_NO,'') AS STRING), CAST(COALESCE(TXN_TELR_NO,'') AS STRING), CAST(COALESCE(TXN_AUTH_TELR_NO_1,'') AS STRING), CAST(COALESCE(TXN_AUTH_TELR_NO_2,'') AS STRING), CAST(COALESCE(INTER_STL_CUST_MGR_NO,'') AS STRING), CAST(COALESCE(CAST(BUY_AMT AS STRING),'') AS STRING), CAST(COALESCE(CAST(BUY_QUOT AS STRING),'') AS STRING), CAST(COALESCE(QUOT_TYPE_CD,'') AS STRING), CAST(COALESCE(BUY_CURR_CD,'') AS STRING), CAST(COALESCE(BUY_CASH_RMT_CD,'') AS STRING), CAST(COALESCE(BUY_ACCT_NO_TYPE_CD,'') AS STRING), CAST(COALESCE(BUY_ACCT_NO,'') AS STRING), CAST(COALESCE(BUY_RBMRK_CD,'') AS STRING), CAST(COALESCE(CAST(SELL_AMT AS STRING),'') AS STRING), CAST(COALESCE(SELL_CURR_CD,'') AS STRING), CAST(COALESCE(CAST(SELL_QUOT AS STRING),'') AS STRING), CAST(COALESCE(SELL_CASH_RMT_CD,'') AS STRING), CAST(COALESCE(SELL_ACCT_NO_TYPE_CD,'') AS STRING), CAST(COALESCE(SELL_ACCT_NO,'') AS STRING), CAST(COALESCE(SELL_RBMRK_CD,'') AS STRING), CAST(COALESCE(EXCH_RATE_BNCHMK_CURR_CD,'') AS STRING), CAST(COALESCE(FX_STL_SALE_STAT_CD,'') AS STRING), CAST(COALESCE(AGEN_DERIV_TXN_SRC_CD,'') AS STRING), CAST(COALESCE(AGEN_DERIV_TXN_CATE_CD,'') AS STRING), CAST(COALESCE(CAST(SPOT_EXCH_RATE AS STRING),'') AS STRING), CAST(COALESCE(CAST(OBANK_PRFT_LOSS_AMT AS STRING),'') AS STRING), CAST(COALESCE(CAST(CUST_PRFT_LOSS_AMT AS STRING),'') AS STRING), CAST(COALESCE(CAST(NOT_YET_DLVY_AMT AS STRING),'') AS STRING), CAST(COALESCE(INIT_TXN_SER_NO,'') AS STRING), CAST(COALESCE(FINAL_TXN_STEP_CD,'') AS STRING), CAST(COALESCE(ESPEC_TXN_TYPE_CD,'') AS STRING), CAST(COALESCE(RECORD_TYPE_CD,'') AS STRING), CAST(COALESCE(SUMMARY_CD,'') AS STRING), CAST(COALESCE(REM,'') AS STRING), CAST(COALESCE(CAST(MRG_RATIO AS STRING),'') AS STRING), CAST(COALESCE(CAST(MRG_AMT AS STRING),'') AS STRING), CAST(COALESCE(VAL_DT,'') AS STRING), CAST(COALESCE(MATU_DT,'') AS STRING), CAST(COALESCE(CHS_TERM_BEGIN_DT,'') AS STRING), CAST(COALESCE(TXN_TM_STAMP,'') AS STRING), CAST(COALESCE(TXN_TERM_MODE_CD,'') AS STRING), CAST(COALESCE(GRACE_MATU_DT,'') AS STRING), CAST(COALESCE(CO_DEAL_DT,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_04
SELECT
       '个人代客外汇远期交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,'个人代客外汇远期交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('CORE_ORIG_TXN_SER_NO, CORE_SUB_TXN_SER_NO, TXN_DT, CUST_IN_CD, CUST_NAME, DOCTYP_CD, DOC_NO, TXN_ORG_NO, LP_ORG_NO, TXN_TELR_NO, TXN_AUTH_TELR_NO_1, TXN_AUTH_TELR_NO_2, INTER_STL_CUST_MGR_NO, BUY_AMT, BUY_QUOT, QUOT_TYPE_CD, BUY_CURR_CD, BUY_CASH_RMT_CD, BUY_ACCT_NO_TYPE_CD, BUY_ACCT_NO, BUY_RBMRK_CD, SELL_AMT, SELL_CURR_CD, SELL_QUOT, SELL_CASH_RMT_CD, SELL_ACCT_NO_TYPE_CD, SELL_ACCT_NO, SELL_RBMRK_CD, EXCH_RATE_BNCHMK_CURR_CD, FX_STL_SALE_STAT_CD, AGEN_DERIV_TXN_SRC_CD, AGEN_DERIV_TXN_CATE_CD, SPOT_EXCH_RATE, OBANK_PRFT_LOSS_AMT, CUST_PRFT_LOSS_AMT, NOT_YET_DLVY_AMT, INIT_TXN_SER_NO, FINAL_TXN_STEP_CD, ESPEC_TXN_TYPE_CD, RECORD_TYPE_CD, SUMMARY_CD, REM, MRG_RATIO, MRG_AMT, VAL_DT, MATU_DT, CHS_TERM_BEGIN_DT, TXN_TM_STAMP, TXN_TERM_MODE_CD, GRACE_MATU_DT, CO_DEAL_DT' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_AGEN_FX_FWD_TXN_DTL_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-个人存款账户信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_INDV_DPSIT_ACCT_INFO_TF
--     表中文名：个人存款账户信息聚合
--     创建日期：2023-12-29 00:00:00
--     主键字段：ACCT_NO, CASH_RMT_CD, CURR_CD
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：None
--     更新记录：
--         2023-12-29 00:00:00 游崔龙 新增
--         2024-01-11 00:00:00 游崔龙 对标
--         2024-02-05 00:00:00 王穆军 单元测试 代码映射
--         2024-07-23 00:00:00 王穆军 修改同步设计文档



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_01
SELECT
       '个人存款账户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_INDV_DPSIT_ACCT_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_02
SELECT
       '个人存款账户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('ACCT_NO, CASH_RMT_CD, CURR_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_INDV_DPSIT_ACCT_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 ACCT_NO, CASH_RMT_CD, CURR_CD
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_03
SELECT
       '个人存款账户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('ACCT_NO, CASH_RMT_CD, CURR_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_INDV_DPSIT_ACCT_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(ACCT_NO,'') AS STRING), CAST(COALESCE(CASH_RMT_CD,'') AS STRING), CAST(COALESCE(CURR_CD,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_04
SELECT
       '个人存款账户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_INDV_DPSIT_ACCT_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_DPSIT_ACCT_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_DPSIT_ACCT_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_INDV_DPSIT_ACCT_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_INDV_DPSIT_ACCT_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_INDV_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'个人存款账户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('ACCT_NO, CASH_RMT_CD, CURR_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_DPSIT_ACCT_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-EVI-个人活期存款账户交易明细还原聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA
--     表中文名：个人活期存款账户交易明细还原聚合
--     创建日期：2023-12-29 00:00:00
--     主键字段：ORIG_TXN_SER_NO, SUB_TXN_SER_NO, TXN_ACCTN_DT, TXN_ACCT_NO
--     归属层次：AGL-EVI
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：None
--     更新记录：
--         2023-12-29 00:00:00 游崔龙 新增
--         2024-01-08 00:00:00 游崔龙 对标



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_01
SELECT
       '个人活期存款账户交易明细还原聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_02
SELECT
       '个人活期存款账户交易明细还原聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('ORIG_TXN_SER_NO, SUB_TXN_SER_NO, TXN_ACCTN_DT, TXN_ACCT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 ORIG_TXN_SER_NO, SUB_TXN_SER_NO, TXN_ACCTN_DT, TXN_ACCT_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_03
SELECT
       '个人活期存款账户交易明细还原聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('ORIG_TXN_SER_NO, SUB_TXN_SER_NO, TXN_ACCTN_DT, TXN_ACCT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(ORIG_TXN_SER_NO,'') AS STRING), CAST(COALESCE(SUB_TXN_SER_NO,'') AS STRING), CAST(COALESCE(TXN_ACCTN_DT,'') AS STRING), CAST(COALESCE(TXN_ACCT_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_04
SELECT
       '个人活期存款账户交易明细还原聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_PC' AS TASK_NAME -- 作业名
      ,'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA' AS TAB_EN_NAME -- 表英文名
      ,'个人活期存款账户交易明细还原聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('ORIG_TXN_SER_NO, SUB_TXN_SER_NO, TXN_ACCTN_DT, TXN_ACCT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_RESTORE_TA_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-个人借记卡变更明细聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_DEBIT_CARD_CHANGE_DTL_TF
--     表中文名：个人借记卡变更明细聚合
--     创建日期：2023-12-14 00:00:00
--     主键字段：CARD_NO, CARD_CHG_TM_STAMP, CARD_CHG_EVENT_NO, CARD_CHG_TYPE_CD
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：None
--     更新记录：
--         2023-12-14 00:00:00 游崔龙 新增
--         2023-01-11 00:00:00 游崔龙 对标
--         2024-02-22 00:00:00 王穆军 修改问题
--         2024-07-23 00:00:00 王穆军 修改同步设计文档
--         2024-08-05 00:00:00 王穆军 修改第7-8组字段映射 拼接000,补充缺失码值映射



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_01
SELECT
       '个人借记卡变更明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_DEBIT_CARD_CHANGE_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_DEBIT_CARD_CHANGE_DTL_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_02
SELECT
       '个人借记卡变更明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_DEBIT_CARD_CHANGE_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CARD_NO, CARD_CHG_TM_STAMP, CARD_CHG_EVENT_NO, CARD_CHG_TYPE_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_DEBIT_CARD_CHANGE_DTL_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 CARD_NO, CARD_CHG_TM_STAMP, CARD_CHG_EVENT_NO, CARD_CHG_TYPE_CD
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_03
SELECT
       '个人借记卡变更明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_DEBIT_CARD_CHANGE_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CARD_NO, CARD_CHG_TM_STAMP, CARD_CHG_EVENT_NO, CARD_CHG_TYPE_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_DEBIT_CARD_CHANGE_DTL_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(CARD_NO,'') AS STRING), CAST(COALESCE(CARD_CHG_TM_STAMP,'') AS STRING), CAST(COALESCE(CARD_CHG_EVENT_NO,'') AS STRING), CAST(COALESCE(CARD_CHG_TYPE_CD,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_04
SELECT
       '个人借记卡变更明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_DEBIT_CARD_CHANGE_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_DEBIT_CARD_CHANGE_DTL_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_DEBIT_CARD_CHANGE_DTL_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_DEBIT_CARD_CHANGE_DTL_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_DEBIT_CARD_CHANGE_DTL_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_DEBIT_CARD_CHANGE_DTL_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_DEBIT_CARD_CHANGE_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,'个人借记卡变更明细聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('CARD_NO, CARD_CHG_TM_STAMP, CARD_CHG_EVENT_NO, CARD_CHG_TYPE_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_DEBIT_CARD_CHANGE_DTL_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-收单特约商户信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF
--     表中文名：收单特约商户信息聚合
--     创建日期：2023-12-05 00:00:00
--     主键字段：SPC_ARGMT_MERCHT_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：None
--     描述信息：存储特约商户聚合
--     更新记录：
--         2023-12-04 00:00:00 沈健 新增
--         2024-01-27 00:00:00 沈健 调通语法测试
--         2024-02-05 00:00:00 沈健 修改UNION2,UNION3,UNION4客户内码逻辑，补全码值转换
--         2024-02-19 00:00:00 沈健 修改客户内码取值逻辑
--         2024-02-29 00:00:00 沈健 修改取值逻辑，按第一段，第二段如果重复出现相同商户，后面就剔除掉
--         2024-03-20 00:00:00 沈健 使用函数unifiedTime日期格式化
--         2024-04-09 00:00:00 沈健 剔除掉UNION4中商户号为92953993475010M，修改UNON4中关联客户证件逻辑，修改UNION2中关联联系方式逻辑,加上DEL_F=0
--         2024-07-25 00:00:00 王穆军 修改保持与设计文档一致



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_01
SELECT
       '收单特约商户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_02
SELECT
       '收单特约商户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('SPC_ARGMT_MERCHT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 SPC_ARGMT_MERCHT_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_03
SELECT
       '收单特约商户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('SPC_ARGMT_MERCHT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(SPC_ARGMT_MERCHT_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_04
SELECT
       '收单特约商户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'收单特约商户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('SPC_ARGMT_MERCHT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_SPC_ARGMT_MERCHT_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-EVI-代销理财交易明细聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CSN_FIN_TXN_DTL_TA
--     表中文名：代销理财交易明细聚合
--     创建日期：2023-12-05 00:00:00
--     主键字段：APP_FORM_SER_NO, TA_SER_NO, CSN_FIN_BIZ_CFM_CD
--     归属层次：AGL-EVI
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：沈健
--     时间粒度：日
--     保留周期：None
--     描述信息：存储代销理财交易明细聚合
--     更新记录：
--         2023-12-04 00:00:00 沈健 新增
--         2024-01-27 00:00:00 沈健 调通语法测试
--         2024-03-07 00:00:00 沈健 修改代码转换取值逻辑
--         2024-04-09 00:00:00 沈健 剔除交易表与历史交易表重复的申请流水号,加上DEL_F=0



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_01
SELECT
       '代销理财交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FIN_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CSN_FIN_TXN_DTL_TA
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_02
SELECT
       '代销理财交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FIN_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('APP_FORM_SER_NO, TA_SER_NO, CSN_FIN_BIZ_CFM_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CSN_FIN_TXN_DTL_TA
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 APP_FORM_SER_NO, TA_SER_NO, CSN_FIN_BIZ_CFM_CD
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_03
SELECT
       '代销理财交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FIN_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('APP_FORM_SER_NO, TA_SER_NO, CSN_FIN_BIZ_CFM_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CSN_FIN_TXN_DTL_TA
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(APP_FORM_SER_NO,'') AS STRING), CAST(COALESCE(TA_SER_NO,'') AS STRING), CAST(COALESCE(CSN_FIN_BIZ_CFM_CD,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_04
SELECT
       '代销理财交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FIN_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CSN_FIN_TXN_DTL_TA 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CSN_FIN_TXN_DTL_TA');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CSN_FIN_TXN_DTL_TA')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CSN_FIN_TXN_DTL_TA_PC' AS TASK_NAME -- 作业名
      ,'AGL_CSN_FIN_TXN_DTL_TA_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CSN_FIN_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,'代销理财交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('APP_FORM_SER_NO, TA_SER_NO, CSN_FIN_BIZ_CFM_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FIN_TXN_DTL_TA_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-自营理财账户持仓信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF
--     表中文名：自营理财账户持仓信息聚合
--     创建日期：2023-12-07 00:00:00
--     主键字段：TA_ACCT_NO, SELF_BIZ_FIN_PROD_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：None
--     描述信息：None
--     更新记录：
--         2023-12-07 00:00:00 沈健 新增
--         2024-01-27 00:00:00 沈健 调通语法测试
--         2024-02-22 00:00:00 沈健 修改代码转换代码，拆分SDM
--         2024-04-09 00:00:00 沈健 加上DEL_F=0



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_01
SELECT
       '自营理财账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_02
SELECT
       '自营理财账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('TA_ACCT_NO, SELF_BIZ_FIN_PROD_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 TA_ACCT_NO, SELF_BIZ_FIN_PROD_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_03
SELECT
       '自营理财账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('TA_ACCT_NO, SELF_BIZ_FIN_PROD_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(TA_ACCT_NO,'') AS STRING), CAST(COALESCE(SELF_BIZ_FIN_PROD_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_04
SELECT
       '自营理财账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'自营理财账户持仓信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('TA_ACCT_NO, SELF_BIZ_FIN_PROD_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_SELF_BIZ_FIN_ACCT_POS_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-EVI-积存金交易明细聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA
--     表中文名：积存金交易明细聚合
--     创建日期：2023-12-07 00:00:00
--     主键字段：APP_FORM_SER_NO
--     归属层次：AGL-EVI
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：None
--     描述信息：None
--     更新记录：
--         2023-12-07 00:00:00 沈健 新增
--         2024-01-27 00:00:00 沈健 调通语法测试
--         2024-02-26 00:00:00 沈健 修改关联ODS_FMS_GOLD_PRODACC_TF条件
--         2024-03-07 00:00:00 沈健 修改交易发生法人机构编号（TXNHAPPY_LP_ORG_NO)改为法人机构编号(LP_ORG_NO),交易发生归属机构编号（TXN_HAPP_BELONG_ORG_NO)改为归属机构编号(BELONG_ORG_NO),交易发生支行机构编号（TXN_HAPPY_SUB_BRCH_ORG_NO)改为支行机构编号(SUB_BRCH_ORG_NO)
--         2024-04-09 00:00:00 沈健 加上DEL_F=0



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_01
SELECT
       '积存金交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_02
SELECT
       '积存金交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('APP_FORM_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 APP_FORM_SER_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_03
SELECT
       '积存金交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('APP_FORM_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(APP_FORM_SER_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_04
SELECT
       '积存金交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_PC' AS TASK_NAME -- 作业名
      ,'AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,'积存金交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('APP_FORM_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_AMASS_DPSIT_GOLD_TXN_DTL_TA_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-EVI-个人活期存款账户交易明细聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA
--     表中文名：个人活期存款账户交易明细聚合
--     创建日期：2023-12-19 00:00:00
--     主键字段：ORIG_TXN_SER_NO, SUB_TXN_SER_NO, TXN_ACCTN_DT, TXN_ACCT_NO
--     归属层次：AGL-EVI
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：沈健
--     时间粒度：日
--     保留周期：None
--     描述信息：存储个人活期存款账户交易明细聚合
--     更新记录：
--         2023-12-19 00:00:00 沈健 新增
--         2024-01-27 00:00:00 沈健 调通语法测试
--         2024-01-29 00:00:00 沈健 删除多余的码值映射
--         2024-02-20 00:00:00 沈健 拆分SDM
--         2024-03-06 00:00:00 沈健 修改第2-5组会计科目编号、存款组合产品代码取值逻辑
--         2024-03-11 00:00:00 沈健 修改UNION2 对方行号取值逻辑
--         2024-04-09 00:00:00 沈健 加上DEL_F=0
--         2024-01-12 00:00:00 王穆军 修改逻辑，合并小文件
--         2024-06-03 00:00:00 王穆军 新增渠道分类代码，修改逻辑



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_01
SELECT
       '个人活期存款账户交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_02
SELECT
       '个人活期存款账户交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('ORIG_TXN_SER_NO, SUB_TXN_SER_NO, TXN_ACCTN_DT, TXN_ACCT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 ORIG_TXN_SER_NO, SUB_TXN_SER_NO, TXN_ACCTN_DT, TXN_ACCT_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_03
SELECT
       '个人活期存款账户交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('ORIG_TXN_SER_NO, SUB_TXN_SER_NO, TXN_ACCTN_DT, TXN_ACCT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(ORIG_TXN_SER_NO,'') AS STRING), CAST(COALESCE(SUB_TXN_SER_NO,'') AS STRING), CAST(COALESCE(TXN_ACCTN_DT,'') AS STRING), CAST(COALESCE(TXN_ACCT_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_04
SELECT
       '个人活期存款账户交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_PC' AS TASK_NAME -- 作业名
      ,'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,'个人活期存款账户交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('ORIG_TXN_SER_NO, SUB_TXN_SER_NO, TXN_ACCTN_DT, TXN_ACCT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_TXN_DTL_TA_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-收单协议信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_RECV_BIL_AGT_INFO_TF
--     表中文名：收单协议信息聚合
--     创建日期：2023-12-19 00:00:00
--     主键字段：MERCHT_IDTFY_NO, RECV_BIL_PROD_NO, SPC_ARGMT_MERCHT_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：None
--     描述信息：None
--     更新记录：
--         2023-12-19 00:00:00 沈健 新增
--         2024-01-27 00:00:00 沈健 调通语法测试
--         2024-02-29 00:00:00 沈健 修改取值逻辑，按第一段，第二段如果重复出现相同商户识别号，后面就剔除掉
--         2024-04-09 00:00:00 沈健 剔除掉商户号为92953993475010M,加上DEL_F=0
--         2024-04-12 00:00:00 王穆军 添加主键，修改产品编号
--         2024-07-29 00:00:00 王穆军 修改同步设计文档



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_01
SELECT
       '收单协议信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_RECV_BIL_AGT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_RECV_BIL_AGT_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_02
SELECT
       '收单协议信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_RECV_BIL_AGT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('MERCHT_IDTFY_NO, RECV_BIL_PROD_NO, SPC_ARGMT_MERCHT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_RECV_BIL_AGT_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 MERCHT_IDTFY_NO, RECV_BIL_PROD_NO, SPC_ARGMT_MERCHT_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_03
SELECT
       '收单协议信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_RECV_BIL_AGT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('MERCHT_IDTFY_NO, RECV_BIL_PROD_NO, SPC_ARGMT_MERCHT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_RECV_BIL_AGT_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(MERCHT_IDTFY_NO,'') AS STRING), CAST(COALESCE(RECV_BIL_PROD_NO,'') AS STRING), CAST(COALESCE(SPC_ARGMT_MERCHT_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_04
SELECT
       '收单协议信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_RECV_BIL_AGT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_RECV_BIL_AGT_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_RECV_BIL_AGT_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_RECV_BIL_AGT_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_RECV_BIL_AGT_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_RECV_BIL_AGT_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_RECV_BIL_AGT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'收单协议信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('MERCHT_IDTFY_NO, RECV_BIL_PROD_NO, SPC_ARGMT_MERCHT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_RECV_BIL_AGT_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-对公定期存款账户信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CORP_RGL_DPSIT_ACCT_INFO_TF
--     表中文名：对公定期存款账户信息聚合
--     创建日期：2023-12-04 00:00:00
--     主键字段：ACCT_NO, CURR_CD
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：None
--     描述信息：存储对公定期存款账户信息聚合
--     更新记录：
--         2023-12-04 00:00:00 沈健 新增
--         2024-01-27 00:00:00 沈健 调通语法测试
--         2024-02-02 00:00:00 沈健 增加月积数、年积数、旬积数、季积数
--         2024-02-22 00:00:00 沈健 删除计息利息字段
--         2024-02-26 00:00:00 沈健 调整基准利率取值逻辑
--         2024-03-13 00:00:00 沈健 修改存单编号
--         2024-03-18 00:00:00 沈健 修改月积数、年积数、旬积数、季积数
--         2024-03-19 00:00:00 沈健 修改分行产品利率表取值逻辑
--         2024-03-20 00:00:00 沈健 使用函数unifiedTime日期格式化
--         2024-04-07 00:00:00 沈健 修改昨日余额折人民币取值逻辑
--         2024-06-07 00:00:00 王穆军 新增字段 ，修改逻辑
--         2024-07-01 00:00:00 王穆军 新增字段 ，修改逻辑，修改表名



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_01
SELECT
       '对公定期存款账户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_RGL_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CORP_RGL_DPSIT_ACCT_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_02
SELECT
       '对公定期存款账户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_RGL_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('ACCT_NO, CURR_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CORP_RGL_DPSIT_ACCT_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 ACCT_NO, CURR_CD
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_03
SELECT
       '对公定期存款账户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_RGL_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('ACCT_NO, CURR_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CORP_RGL_DPSIT_ACCT_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(ACCT_NO,'') AS STRING), CAST(COALESCE(CURR_CD,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_04
SELECT
       '对公定期存款账户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_RGL_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CORP_RGL_DPSIT_ACCT_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_RGL_DPSIT_ACCT_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_RGL_DPSIT_ACCT_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CORP_RGL_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'对公定期存款账户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('ACCT_NO, CURR_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_RGL_DPSIT_ACCT_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-对公活期存款账户信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CORP_CURR_DPSIT_ACCT_INFO_TF
--     表中文名：对公活期存款账户信息聚合
--     创建日期：2023-12-05 00:00:00
--     主键字段：ACCT_NO, CURR_CD, CASH_RMT_CD
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：沈健
--     时间粒度：日
--     保留周期：None
--     描述信息：存储对公活期存款账户信息聚合
--     更新记录：
--         2023-12-04 00:00:00 沈健 新增
--         2024-01-27 00:00:00 沈健 调通语法测试
--         2024-02-02 00:00:00 沈健 增加月积数、年积数、旬积数、季积数
--         2024-02-26 00:00:00 沈健 修改基准利率取值逻辑
--         2024-03-06 00:00:00 沈健 修改销户机构编号等字段取值逻辑
--         2024-03-11 00:00:00 沈健 修改应税利息取值逻辑
--         2024-03-12 00:00:00 沈健 修改开户渠道取值逻辑,删除计提利息
--         2024-03-18 00:00:00 沈健 修改月积数、年积数、旬积数、季积数
--         2024-03-20 00:00:00 沈健 使用函数unifiedTime日期格式化
--         2024-03-29 00:00:00 沈健 修改企业基本户取消开户许可证标志字段
--         2024-06-07 00:00:00 王穆军 修复数据重复，新增客户类型字段
--         2024-07-01 00:00:00 王穆军 新增字段 ，修改逻辑,修改表名



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_01
SELECT
       '对公活期存款账户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CURR_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CORP_CURR_DPSIT_ACCT_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_02
SELECT
       '对公活期存款账户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CURR_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('ACCT_NO, CURR_CD, CASH_RMT_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CORP_CURR_DPSIT_ACCT_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 ACCT_NO, CURR_CD, CASH_RMT_CD
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_03
SELECT
       '对公活期存款账户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CURR_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('ACCT_NO, CURR_CD, CASH_RMT_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CORP_CURR_DPSIT_ACCT_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(ACCT_NO,'') AS STRING), CAST(COALESCE(CURR_CD,'') AS STRING), CAST(COALESCE(CASH_RMT_CD,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_04
SELECT
       '对公活期存款账户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_CURR_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CORP_CURR_DPSIT_ACCT_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_CURR_DPSIT_ACCT_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_CURR_DPSIT_ACCT_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CORP_CURR_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'对公活期存款账户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('ACCT_NO, CURR_CD, CASH_RMT_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_CURR_DPSIT_ACCT_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-个人活期存款账户聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_INDV_CURR_DPSIT_ACCT_INFO_TF
--     表中文名：个人活期存款账户聚合
--     创建日期：2023-02-15 00:00:00
--     主键字段：ACCT_NO, CURR_CD, CASH_RMT_CD
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：None
--     描述信息：存储个人活期存款账户聚合
--     更新记录：
--         2024-04-09 00:00:00 沈健 加上DEL_F=0,流水补充文件表注释
--         2024-04-22 00:00:00 王穆军 修改利息累计加工逻辑
--         2024-07-04 00:00:00 王穆军 优化性能逻辑



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_01
SELECT
       '个人活期存款账户聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CURR_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_INDV_CURR_DPSIT_ACCT_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_02
SELECT
       '个人活期存款账户聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CURR_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('ACCT_NO, CURR_CD, CASH_RMT_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_INDV_CURR_DPSIT_ACCT_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 ACCT_NO, CURR_CD, CASH_RMT_CD
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_03
SELECT
       '个人活期存款账户聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CURR_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('ACCT_NO, CURR_CD, CASH_RMT_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_INDV_CURR_DPSIT_ACCT_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(ACCT_NO,'') AS STRING), CAST(COALESCE(CURR_CD,'') AS STRING), CAST(COALESCE(CASH_RMT_CD,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_04
SELECT
       '个人活期存款账户聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CURR_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_INDV_CURR_DPSIT_ACCT_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_CURR_DPSIT_ACCT_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_CURR_DPSIT_ACCT_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_INDV_CURR_DPSIT_ACCT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'个人活期存款账户聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('ACCT_NO, CURR_CD, CASH_RMT_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CURR_DPSIT_ACCT_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-EVI-代销基金交易明细聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CSN_FUND_TXN_DTL_TA
--     表中文名：代销基金交易明细聚合
--     创建日期：2023-12-07 00:00:00
--     主键字段：APP_FORM_SER_NO, TA_SER_NO, LP_ORG_NO
--     归属层次：AGL-EVI
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：None
--     描述信息：None
--     更新记录：
--         2023-12-07 00:00:00 沈健 新增
--         2024-03-06 00:00:00 沈健 修改码值转换逻辑



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_01
SELECT
       '代销基金交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FUND_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CSN_FUND_TXN_DTL_TA
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_02
SELECT
       '代销基金交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FUND_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('APP_FORM_SER_NO, TA_SER_NO, LP_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CSN_FUND_TXN_DTL_TA
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 APP_FORM_SER_NO, TA_SER_NO, LP_ORG_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_03
SELECT
       '代销基金交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FUND_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('APP_FORM_SER_NO, TA_SER_NO, LP_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CSN_FUND_TXN_DTL_TA
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(APP_FORM_SER_NO,'') AS STRING), CAST(COALESCE(TA_SER_NO,'') AS STRING), CAST(COALESCE(LP_ORG_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_04
SELECT
       '代销基金交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CSN_FUND_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CSN_FUND_TXN_DTL_TA 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CSN_FUND_TXN_DTL_TA');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CSN_FUND_TXN_DTL_TA')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CSN_FUND_TXN_DTL_TA_PC' AS TASK_NAME -- 作业名
      ,'AGL_CSN_FUND_TXN_DTL_TA_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CSN_FUND_TXN_DTL_TA' AS TAB_EN_NAME -- 表英文名
      ,'代销基金交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('APP_FORM_SER_NO, TA_SER_NO, LP_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CSN_FUND_TXN_DTL_TA_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-EVI-对公代客外汇远期交易明细聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF
--     表中文名：对公代客外汇远期交易明细聚合
--     创建日期：2024-07-02 00:00:00
--     主键字段：
--     归属层次：AGL-EVI
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：None
--     描述信息：存储对公代客外汇远期交易明细聚合
--     更新记录：
--         2023|12|7 沈健 新增
--         2024|1|27 沈健 调通语法测试



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_01
SELECT
       '对公代客外汇远期交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_02
SELECT
       '对公代客外汇远期交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('FX_DERIV_TXN_SER_NO, FX_DERIV_SUB_TXN_SER_NO, TXN_DT, CUST_IN_CD, CUST_NAME, DOCTYP_CD, DOC_NO, TXN_ORG_NO, LP_ORG_NO, TXN_TELR_NO, TXN_AUTH_TELR_NO_1, TXN_AUTH_TELR_NO_2, INTER_STL_CUST_MGR_NO, BUY_AMT, BUY_QUOT, QUOT_TYPE_CD, BUY_CURR_CD, BUY_CASH_RMT_CD, BUY_ACCT_NO_TYPE_CD, BUY_ACCT_NO, BUY_RBMRK_CD, SELL_AMT, SELL_CURR_CD, SELL_QUOT, SELL_CASH_RMT_CD, SELL_ACCT_NO_TYPE_CD, SELL_ACCT_NO, SELL_RBMRK_CD, EXCH_RATE_BNCHMK_CURR_CD, FX_STL_SALE_STAT_CD, AGEN_DERIV_TXN_SRC_CD, AGEN_DERIV_TXN_CATE_CD, DLVY_MODE_CD, SPOT_EXCH_RATE, OBANK_PRFT_LOSS_AMT, CONVT_USD_AMT, CONVT_RMB_AMT, BRCH_PRFT_POINT, CUST_PREFR_POINT, ORIG_CURR_BRCH_PRFT_LOSS_MNTY_CD, ORIG_TXN_SER_NO, INIT_TXN_SER_NO, FINAL_TXN_STEP_CD, ESPEC_TXN_TYPE_CD, RECORD_TYPE_CD, CLOSE_POS_RSN_CD, CLOSE_POS_RSN_ILUS, FWD_TXN_CD_CD, SUMMARY_CD, REM, MRG_RATIO, MRG_AMT, ACTL_RECV_MRG_CURR_CD, CRDT_AMT, GUAR_SUM_CURR_CD, GUAR_SUM_AMT, FREE_CHARGE_MRG_FLAG, GUAR_COMB_MODE_CD, VAL_DT, MATU_DT, CHS_TERM_BEGIN_DT, TXN_TM_STAMP, TXN_TERM_MODE_CD, GRACE_MATU_DT, TXN_HAPP_TM_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
      CAST(COALESCE(FX_DERIV_TXN_SER_NO,'') AS STRING), CAST(COALESCE(FX_DERIV_SUB_TXN_SER_NO,'') AS STRING), CAST(COALESCE(TXN_DT,'') AS STRING), CAST(COALESCE(CUST_IN_CD,'') AS STRING), CAST(COALESCE(CUST_NAME,'') AS STRING), CAST(COALESCE(DOCTYP_CD,'') AS STRING), CAST(COALESCE(DOC_NO,'') AS STRING), CAST(COALESCE(TXN_ORG_NO,'') AS STRING), CAST(COALESCE(LP_ORG_NO,'') AS STRING), CAST(COALESCE(TXN_TELR_NO,'') AS STRING), CAST(COALESCE(TXN_AUTH_TELR_NO_1,'') AS STRING), CAST(COALESCE(TXN_AUTH_TELR_NO_2,'') AS STRING), CAST(COALESCE(INTER_STL_CUST_MGR_NO,'') AS STRING), CAST(COALESCE(CAST(BUY_AMT AS STRING),'') AS STRING), CAST(COALESCE(CAST(BUY_QUOT AS STRING),'') AS STRING), CAST(COALESCE(QUOT_TYPE_CD,'') AS STRING), CAST(COALESCE(BUY_CURR_CD,'') AS STRING), CAST(COALESCE(BUY_CASH_RMT_CD,'') AS STRING), CAST(COALESCE(BUY_ACCT_NO_TYPE_CD,'') AS STRING), CAST(COALESCE(BUY_ACCT_NO,'') AS STRING), CAST(COALESCE(BUY_RBMRK_CD,'') AS STRING), CAST(COALESCE(CAST(SELL_AMT AS STRING),'') AS STRING), CAST(COALESCE(SELL_CURR_CD,'') AS STRING), CAST(COALESCE(CAST(SELL_QUOT AS STRING),'') AS STRING), CAST(COALESCE(SELL_CASH_RMT_CD,'') AS STRING), CAST(COALESCE(SELL_ACCT_NO_TYPE_CD,'') AS STRING), CAST(COALESCE(SELL_ACCT_NO,'') AS STRING), CAST(COALESCE(SELL_RBMRK_CD,'') AS STRING), CAST(COALESCE(EXCH_RATE_BNCHMK_CURR_CD,'') AS STRING), CAST(COALESCE(FX_STL_SALE_STAT_CD,'') AS STRING), CAST(COALESCE(AGEN_DERIV_TXN_SRC_CD,'') AS STRING), CAST(COALESCE(AGEN_DERIV_TXN_CATE_CD,'') AS STRING), CAST(COALESCE(DLVY_MODE_CD,'') AS STRING), CAST(COALESCE(CAST(SPOT_EXCH_RATE AS STRING),'') AS STRING), CAST(COALESCE(CAST(OBANK_PRFT_LOSS_AMT AS STRING),'') AS STRING), CAST(COALESCE(CAST(CONVT_USD_AMT AS STRING),'') AS STRING), CAST(COALESCE(CAST(CONVT_RMB_AMT AS STRING),'') AS STRING), CAST(COALESCE(CAST(BRCH_PRFT_POINT AS STRING),'') AS STRING), CAST(COALESCE(CAST(CUST_PREFR_POINT AS STRING),'') AS STRING), CAST(COALESCE(ORIG_CURR_BRCH_PRFT_LOSS_MNTY_CD,'') AS STRING), CAST(COALESCE(ORIG_TXN_SER_NO,'') AS STRING), CAST(COALESCE(INIT_TXN_SER_NO,'') AS STRING), CAST(COALESCE(FINAL_TXN_STEP_CD,'') AS STRING), CAST(COALESCE(ESPEC_TXN_TYPE_CD,'') AS STRING), CAST(COALESCE(RECORD_TYPE_CD,'') AS STRING), CAST(COALESCE(CLOSE_POS_RSN_CD,'') AS STRING), CAST(COALESCE(CLOSE_POS_RSN_ILUS,'') AS STRING), CAST(COALESCE(FWD_TXN_CD_CD,'') AS STRING), CAST(COALESCE(SUMMARY_CD,'') AS STRING), CAST(COALESCE(REM,'') AS STRING), CAST(COALESCE(CAST(MRG_RATIO AS STRING),'') AS STRING), CAST(COALESCE(CAST(MRG_AMT AS STRING),'') AS STRING), CAST(COALESCE(ACTL_RECV_MRG_CURR_CD,'') AS STRING), CAST(COALESCE(CAST(CRDT_AMT AS STRING),'') AS STRING), CAST(COALESCE(GUAR_SUM_CURR_CD,'') AS STRING), CAST(COALESCE(CAST(GUAR_SUM_AMT AS STRING),'') AS STRING), CAST(COALESCE(FREE_CHARGE_MRG_FLAG,'') AS STRING), CAST(COALESCE(GUAR_COMB_MODE_CD,'') AS STRING), CAST(COALESCE(VAL_DT,'') AS STRING), CAST(COALESCE(MATU_DT,'') AS STRING), CAST(COALESCE(CHS_TERM_BEGIN_DT,'') AS STRING), CAST(COALESCE(TXN_TM_STAMP,'') AS STRING), CAST(COALESCE(TXN_TERM_MODE_CD,'') AS STRING), CAST(COALESCE(GRACE_MATU_DT,'') AS STRING), CAST(COALESCE(TXN_HAPP_TM_CD,'') AS STRING)
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_03
SELECT
       '对公代客外汇远期交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('FX_DERIV_TXN_SER_NO, FX_DERIV_SUB_TXN_SER_NO, TXN_DT, CUST_IN_CD, CUST_NAME, DOCTYP_CD, DOC_NO, TXN_ORG_NO, LP_ORG_NO, TXN_TELR_NO, TXN_AUTH_TELR_NO_1, TXN_AUTH_TELR_NO_2, INTER_STL_CUST_MGR_NO, BUY_AMT, BUY_QUOT, QUOT_TYPE_CD, BUY_CURR_CD, BUY_CASH_RMT_CD, BUY_ACCT_NO_TYPE_CD, BUY_ACCT_NO, BUY_RBMRK_CD, SELL_AMT, SELL_CURR_CD, SELL_QUOT, SELL_CASH_RMT_CD, SELL_ACCT_NO_TYPE_CD, SELL_ACCT_NO, SELL_RBMRK_CD, EXCH_RATE_BNCHMK_CURR_CD, FX_STL_SALE_STAT_CD, AGEN_DERIV_TXN_SRC_CD, AGEN_DERIV_TXN_CATE_CD, DLVY_MODE_CD, SPOT_EXCH_RATE, OBANK_PRFT_LOSS_AMT, CONVT_USD_AMT, CONVT_RMB_AMT, BRCH_PRFT_POINT, CUST_PREFR_POINT, ORIG_CURR_BRCH_PRFT_LOSS_MNTY_CD, ORIG_TXN_SER_NO, INIT_TXN_SER_NO, FINAL_TXN_STEP_CD, ESPEC_TXN_TYPE_CD, RECORD_TYPE_CD, CLOSE_POS_RSN_CD, CLOSE_POS_RSN_ILUS, FWD_TXN_CD_CD, SUMMARY_CD, REM, MRG_RATIO, MRG_AMT, ACTL_RECV_MRG_CURR_CD, CRDT_AMT, GUAR_SUM_CURR_CD, GUAR_SUM_AMT, FREE_CHARGE_MRG_FLAG, GUAR_COMB_MODE_CD, VAL_DT, MATU_DT, CHS_TERM_BEGIN_DT, TXN_TM_STAMP, TXN_TERM_MODE_CD, GRACE_MATU_DT, TXN_HAPP_TM_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
 
      CAST(COALESCE(FX_DERIV_TXN_SER_NO,'') AS STRING), CAST(COALESCE(FX_DERIV_SUB_TXN_SER_NO,'') AS STRING), CAST(COALESCE(TXN_DT,'') AS STRING), CAST(COALESCE(CUST_IN_CD,'') AS STRING), CAST(COALESCE(CUST_NAME,'') AS STRING), CAST(COALESCE(DOCTYP_CD,'') AS STRING), CAST(COALESCE(DOC_NO,'') AS STRING), CAST(COALESCE(TXN_ORG_NO,'') AS STRING), CAST(COALESCE(LP_ORG_NO,'') AS STRING), CAST(COALESCE(TXN_TELR_NO,'') AS STRING), CAST(COALESCE(TXN_AUTH_TELR_NO_1,'') AS STRING), CAST(COALESCE(TXN_AUTH_TELR_NO_2,'') AS STRING), CAST(COALESCE(INTER_STL_CUST_MGR_NO,'') AS STRING), CAST(COALESCE(CAST(BUY_AMT AS STRING),'') AS STRING), CAST(COALESCE(CAST(BUY_QUOT AS STRING),'') AS STRING), CAST(COALESCE(QUOT_TYPE_CD,'') AS STRING), CAST(COALESCE(BUY_CURR_CD,'') AS STRING), CAST(COALESCE(BUY_CASH_RMT_CD,'') AS STRING), CAST(COALESCE(BUY_ACCT_NO_TYPE_CD,'') AS STRING), CAST(COALESCE(BUY_ACCT_NO,'') AS STRING), CAST(COALESCE(BUY_RBMRK_CD,'') AS STRING), CAST(COALESCE(CAST(SELL_AMT AS STRING),'') AS STRING), CAST(COALESCE(SELL_CURR_CD,'') AS STRING), CAST(COALESCE(CAST(SELL_QUOT AS STRING),'') AS STRING), CAST(COALESCE(SELL_CASH_RMT_CD,'') AS STRING), CAST(COALESCE(SELL_ACCT_NO_TYPE_CD,'') AS STRING), CAST(COALESCE(SELL_ACCT_NO,'') AS STRING), CAST(COALESCE(SELL_RBMRK_CD,'') AS STRING), CAST(COALESCE(EXCH_RATE_BNCHMK_CURR_CD,'') AS STRING), CAST(COALESCE(FX_STL_SALE_STAT_CD,'') AS STRING), CAST(COALESCE(AGEN_DERIV_TXN_SRC_CD,'') AS STRING), CAST(COALESCE(AGEN_DERIV_TXN_CATE_CD,'') AS STRING), CAST(COALESCE(DLVY_MODE_CD,'') AS STRING), CAST(COALESCE(CAST(SPOT_EXCH_RATE AS STRING),'') AS STRING), CAST(COALESCE(CAST(OBANK_PRFT_LOSS_AMT AS STRING),'') AS STRING), CAST(COALESCE(CAST(CONVT_USD_AMT AS STRING),'') AS STRING), CAST(COALESCE(CAST(CONVT_RMB_AMT AS STRING),'') AS STRING), CAST(COALESCE(CAST(BRCH_PRFT_POINT AS STRING),'') AS STRING), CAST(COALESCE(CAST(CUST_PREFR_POINT AS STRING),'') AS STRING), CAST(COALESCE(ORIG_CURR_BRCH_PRFT_LOSS_MNTY_CD,'') AS STRING), CAST(COALESCE(ORIG_TXN_SER_NO,'') AS STRING), CAST(COALESCE(INIT_TXN_SER_NO,'') AS STRING), CAST(COALESCE(FINAL_TXN_STEP_CD,'') AS STRING), CAST(COALESCE(ESPEC_TXN_TYPE_CD,'') AS STRING), CAST(COALESCE(RECORD_TYPE_CD,'') AS STRING), CAST(COALESCE(CLOSE_POS_RSN_CD,'') AS STRING), CAST(COALESCE(CLOSE_POS_RSN_ILUS,'') AS STRING), CAST(COALESCE(FWD_TXN_CD_CD,'') AS STRING), CAST(COALESCE(SUMMARY_CD,'') AS STRING), CAST(COALESCE(REM,'') AS STRING), CAST(COALESCE(CAST(MRG_RATIO AS STRING),'') AS STRING), CAST(COALESCE(CAST(MRG_AMT AS STRING),'') AS STRING), CAST(COALESCE(ACTL_RECV_MRG_CURR_CD,'') AS STRING), CAST(COALESCE(CAST(CRDT_AMT AS STRING),'') AS STRING), CAST(COALESCE(GUAR_SUM_CURR_CD,'') AS STRING), CAST(COALESCE(CAST(GUAR_SUM_AMT AS STRING),'') AS STRING), CAST(COALESCE(FREE_CHARGE_MRG_FLAG,'') AS STRING), CAST(COALESCE(GUAR_COMB_MODE_CD,'') AS STRING), CAST(COALESCE(VAL_DT,'') AS STRING), CAST(COALESCE(MATU_DT,'') AS STRING), CAST(COALESCE(CHS_TERM_BEGIN_DT,'') AS STRING), CAST(COALESCE(TXN_TM_STAMP,'') AS STRING), CAST(COALESCE(TXN_TERM_MODE_CD,'') AS STRING), CAST(COALESCE(GRACE_MATU_DT,'') AS STRING), CAST(COALESCE(TXN_HAPP_TM_CD,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_04
SELECT
       '对公代客外汇远期交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,'对公代客外汇远期交易明细聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('FX_DERIV_TXN_SER_NO, FX_DERIV_SUB_TXN_SER_NO, TXN_DT, CUST_IN_CD, CUST_NAME, DOCTYP_CD, DOC_NO, TXN_ORG_NO, LP_ORG_NO, TXN_TELR_NO, TXN_AUTH_TELR_NO_1, TXN_AUTH_TELR_NO_2, INTER_STL_CUST_MGR_NO, BUY_AMT, BUY_QUOT, QUOT_TYPE_CD, BUY_CURR_CD, BUY_CASH_RMT_CD, BUY_ACCT_NO_TYPE_CD, BUY_ACCT_NO, BUY_RBMRK_CD, SELL_AMT, SELL_CURR_CD, SELL_QUOT, SELL_CASH_RMT_CD, SELL_ACCT_NO_TYPE_CD, SELL_ACCT_NO, SELL_RBMRK_CD, EXCH_RATE_BNCHMK_CURR_CD, FX_STL_SALE_STAT_CD, AGEN_DERIV_TXN_SRC_CD, AGEN_DERIV_TXN_CATE_CD, DLVY_MODE_CD, SPOT_EXCH_RATE, OBANK_PRFT_LOSS_AMT, CONVT_USD_AMT, CONVT_RMB_AMT, BRCH_PRFT_POINT, CUST_PREFR_POINT, ORIG_CURR_BRCH_PRFT_LOSS_MNTY_CD, ORIG_TXN_SER_NO, INIT_TXN_SER_NO, FINAL_TXN_STEP_CD, ESPEC_TXN_TYPE_CD, RECORD_TYPE_CD, CLOSE_POS_RSN_CD, CLOSE_POS_RSN_ILUS, FWD_TXN_CD_CD, SUMMARY_CD, REM, MRG_RATIO, MRG_AMT, ACTL_RECV_MRG_CURR_CD, CRDT_AMT, GUAR_SUM_CURR_CD, GUAR_SUM_AMT, FREE_CHARGE_MRG_FLAG, GUAR_COMB_MODE_CD, VAL_DT, MATU_DT, CHS_TERM_BEGIN_DT, TXN_TM_STAMP, TXN_TERM_MODE_CD, GRACE_MATU_DT, TXN_HAPP_TM_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_CORP_AGEN_FX_FWD_TXN_DTL_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-个人客户行社版信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_INDV_CUST_MCH_VER_INFO_TF
--     表中文名：个人客户行社版信息聚合
--     创建日期：2023-12-04 00:00:00
--     主键字段：CUST_IN_CD, BELONG_ORG_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：None
--     描述信息：存储个人客户行社版信息聚合
--     更新记录：
--         2023-12-04 00:00:00 沈健 新增
--         2024-01-30 00:00:00 沈健 更名S01_ORG_INDV_CUST_BASE_INFO更改AGL_S07_ORG_INDV_CUST_BASE_INFO_TF
--         2024-02-23 00:00:00 沈健 修改归属机构编号取值逻辑
--         2024-03-28 00:00:00 喻前程 1、、表中文名和英文名修改： 个人客户基本信息法人机构聚合表 --> 个人客户行社版信息聚合  AGL_S07_ORG_INDV_CUST_BASE_INFO_TF -->AGL_ S07_INDV_CUST_MCH_VER_INFO_AGGREGT_TF2、字段名调整： 最新非空电话建立机构编号 - LATST_NOT_EMPTY_TEL_SETUP_ORG_NO最新非空电话最后修改机构编号 -LATST_NOT_EMPTY_TEL_RECNT_MODIF_ORG_NO
--         2024-04-07 00:00:00 沈健 修改归属机构编号取值逻辑，剔除归属机构为空的情况
--         2024-04-09 00:00:00 沈健 加上DEL_F=0
--         2024-04-10 00:00:00 沈健 修改主手机号码取值逻辑
--         2024-04-19 00:00:00 王穆军 涉税标志，数据集筛选



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_01
SELECT
       '个人客户行社版信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CUST_MCH_VER_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_INDV_CUST_MCH_VER_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_02
SELECT
       '个人客户行社版信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CUST_MCH_VER_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD, BELONG_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_INDV_CUST_MCH_VER_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 CUST_IN_CD, BELONG_ORG_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_03
SELECT
       '个人客户行社版信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CUST_MCH_VER_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD, BELONG_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_INDV_CUST_MCH_VER_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(CUST_IN_CD,'') AS STRING), CAST(COALESCE(BELONG_ORG_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_04
SELECT
       '个人客户行社版信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CUST_MCH_VER_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_INDV_CUST_MCH_VER_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_CUST_MCH_VER_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_CUST_MCH_VER_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_INDV_CUST_MCH_VER_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_INDV_CUST_MCH_VER_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_INDV_CUST_MCH_VER_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'个人客户行社版信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('CUST_IN_CD, BELONG_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_MCH_VER_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-个人客户信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_INDV_CUST_INFO_TF
--     表中文名：个人客户信息聚合
--     创建日期：2023-12-04 00:00:00
--     主键字段：CUST_IN_CD
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：None
--     描述信息：存储个人客户基本信息聚合
--     更新记录：
--         2023-12-04 00:00:00 沈健 新增
--         2024-01-27 00:00:00 沈健 调通语法测试
--         2024-01-30 00:00:00 沈健 更名AGL_INDV_CUST_BASE_INFO_TF更改AGL_INDV_CUST_INFO_TF
--         2024-03-13 00:00:00 沈健 修改客户地址信息表取值逻辑
--         2024-03-28 00:00:00 喻前程 1、表名修改  ： 个人客户基本信息聚合  -- >个人客户信息聚合2、字段修改
--         2024-04-02 00:00:00 沈健 修改最新工作单位地址取值逻辑
--         2024-04-07 00:00:00 沈健 修改联系电话号码，最新非空电话国际长途区号等取值逻辑
--         2024-04-09 00:00:00 沈健 加上DEL_F=0
--         2024-04-10 00:00:00 沈健 修改主手机号码取值逻辑



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_01
SELECT
       '个人客户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CUST_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_INDV_CUST_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_02
SELECT
       '个人客户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CUST_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_INDV_CUST_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 CUST_IN_CD
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_03
SELECT
       '个人客户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CUST_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('CUST_IN_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_INDV_CUST_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(CUST_IN_CD,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_04
SELECT
       '个人客户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_INDV_CUST_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_INDV_CUST_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_CUST_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_INDV_CUST_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_INDV_CUST_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_INDV_CUST_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_INDV_CUST_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'个人客户信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('CUST_IN_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_INDV_CUST_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-对公贷款展期延期信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_COLOAN_LOAN_EXT_EXT_INFO_TF
--     表中文名：对公贷款展期延期信息聚合
--     创建日期：2024-05-27 00:00:00
--     主键字段：LOAN_CONT_NO, DUBIL_SER_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：None
--     描述信息：提供贷款展期和延期后的新合同以及原合同的基础信息用于监管报送。
--     更新记录：
--         2024-05-27 00:00:00 WMJ NEW



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_01
SELECT
       '对公贷款展期延期信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_LOAN_EXT_EXT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_COLOAN_LOAN_EXT_EXT_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_02
SELECT
       '对公贷款展期延期信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_LOAN_EXT_EXT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('LOAN_CONT_NO, DUBIL_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_COLOAN_LOAN_EXT_EXT_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 LOAN_CONT_NO, DUBIL_SER_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_03
SELECT
       '对公贷款展期延期信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_LOAN_EXT_EXT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('LOAN_CONT_NO, DUBIL_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_COLOAN_LOAN_EXT_EXT_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(LOAN_CONT_NO,'') AS STRING), CAST(COALESCE(DUBIL_SER_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_04
SELECT
       '对公贷款展期延期信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_LOAN_EXT_EXT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_COLOAN_LOAN_EXT_EXT_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_COLOAN_LOAN_EXT_EXT_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_COLOAN_LOAN_EXT_EXT_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_COLOAN_LOAN_EXT_EXT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'对公贷款展期延期信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('LOAN_CONT_NO, DUBIL_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_LOAN_EXT_EXT_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-对公贷款联保协议小组成员信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF
--     表中文名：对公贷款联保协议小组成员信息聚合
--     创建日期：2024-05-18 00:00:00
--     主键字段：UNPR_AGT_GRP_NO, CUST_IN_CD, LP_ORG_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：None
--     描述信息：联保小组相关的协议、组员、授信信息
--     更新记录：
--         2024-05-18 00:00:00 王穆军 新建
--         2024-07-26 00:00:00 王穆军 添加技术字段，修改表名



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_01
SELECT
       '对公贷款联保协议小组成员信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_02
SELECT
       '对公贷款联保协议小组成员信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('UNPR_AGT_GRP_NO, CUST_IN_CD, LP_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 UNPR_AGT_GRP_NO, CUST_IN_CD, LP_ORG_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_03
SELECT
       '对公贷款联保协议小组成员信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('UNPR_AGT_GRP_NO, CUST_IN_CD, LP_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(UNPR_AGT_GRP_NO,'') AS STRING), CAST(COALESCE(CUST_IN_CD,'') AS STRING), CAST(COALESCE(LP_ORG_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_04
SELECT
       '对公贷款联保协议小组成员信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'对公贷款联保协议小组成员信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('UNPR_AGT_GRP_NO, CUST_IN_CD, LP_ORG_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_UNPR_AGT_GRP_MEM_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-对公贷款资产风险分类信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_COLOAN_AST_RISK_CLS_INFO_TF
--     表中文名：对公贷款资产风险分类信息聚合
--     创建日期：2024-05-18 00:00:00
--     主键字段：RISK_REPORT_NO, LOAN_CONT_NO, DUBIL_SER_NO, LNACCT_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：None
--     描述信息：提供客户资产风险分类息，供信贷部门、风险分析部门查询使用
--     更新记录：
--         2024-05-18 00:00:00 王穆军 新建



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_01
SELECT
       '对公贷款资产风险分类信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_AST_RISK_CLS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_COLOAN_AST_RISK_CLS_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_02
SELECT
       '对公贷款资产风险分类信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_AST_RISK_CLS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('RISK_REPORT_NO, LOAN_CONT_NO, DUBIL_SER_NO, LNACCT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_COLOAN_AST_RISK_CLS_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 RISK_REPORT_NO, LOAN_CONT_NO, DUBIL_SER_NO, LNACCT_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_03
SELECT
       '对公贷款资产风险分类信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_AST_RISK_CLS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('RISK_REPORT_NO, LOAN_CONT_NO, DUBIL_SER_NO, LNACCT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_COLOAN_AST_RISK_CLS_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(RISK_REPORT_NO,'') AS STRING), CAST(COALESCE(LOAN_CONT_NO,'') AS STRING), CAST(COALESCE(DUBIL_SER_NO,'') AS STRING), CAST(COALESCE(LNACCT_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_04
SELECT
       '对公贷款资产风险分类信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_COLOAN_AST_RISK_CLS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_COLOAN_AST_RISK_CLS_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_COLOAN_AST_RISK_CLS_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_COLOAN_AST_RISK_CLS_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_COLOAN_AST_RISK_CLS_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_COLOAN_AST_RISK_CLS_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_COLOAN_AST_RISK_CLS_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'对公贷款资产风险分类信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('RISK_REPORT_NO, LOAN_CONT_NO, DUBIL_SER_NO, LNACCT_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_COLOAN_AST_RISK_CLS_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-全市场债券发行主体信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF
--     表中文名：全市场债券发行主体信息聚合
--     创建日期：2024-07-30 00:00:00
--     主键字段：WIND_BOND_CD, BOND_MAIN_CORP_NO, BOND_LATST_DEBT_MAIN_FLAG, WIND_BOND_MAIN_RELA_TYPE_CD
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：None
--     描述信息：记录债券发行主体的相关信息
--     更新记录：
--         2024-07-30 00:00:00 王穆军 new



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_01
SELECT
       '全市场债券发行主体信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_02
SELECT
       '全市场债券发行主体信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('WIND_BOND_CD, BOND_MAIN_CORP_NO, BOND_LATST_DEBT_MAIN_FLAG, WIND_BOND_MAIN_RELA_TYPE_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 WIND_BOND_CD, BOND_MAIN_CORP_NO, BOND_LATST_DEBT_MAIN_FLAG, WIND_BOND_MAIN_RELA_TYPE_CD
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_03
SELECT
       '全市场债券发行主体信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('WIND_BOND_CD, BOND_MAIN_CORP_NO, BOND_LATST_DEBT_MAIN_FLAG, WIND_BOND_MAIN_RELA_TYPE_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(WIND_BOND_CD,'') AS STRING), CAST(COALESCE(BOND_MAIN_CORP_NO,'') AS STRING), CAST(COALESCE(BOND_LATST_DEBT_MAIN_FLAG,'') AS STRING), CAST(COALESCE(WIND_BOND_MAIN_RELA_TYPE_CD,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_04
SELECT
       '全市场债券发行主体信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'全市场债券发行主体信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('WIND_BOND_CD, BOND_MAIN_CORP_NO, BOND_LATST_DEBT_MAIN_FLAG, WIND_BOND_MAIN_RELA_TYPE_CD' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_ALL_MKT_BOND_ISSUE_MAIN_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-债券评级信息聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_BOND_RAT_INFO_TF
--     表中文名：债券评级信息聚合
--     创建日期：2024-08-01 00:00:00
--     主键字段：BOND_RAT_SER_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含债券评级信息，债券基本信息，债券发行的信息
--     更新记录：
--         2024-08-01 00:00:00 王穆军 新建第三批脚本



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_01
SELECT
       '债券评级信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_BOND_RAT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_BOND_RAT_INFO_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_02
SELECT
       '债券评级信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_BOND_RAT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('BOND_RAT_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_BOND_RAT_INFO_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 BOND_RAT_SER_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_03
SELECT
       '债券评级信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_BOND_RAT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('BOND_RAT_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_BOND_RAT_INFO_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(BOND_RAT_SER_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_04
SELECT
       '债券评级信息聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_BOND_RAT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_BOND_RAT_INFO_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_BOND_RAT_INFO_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_BOND_RAT_INFO_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_BOND_RAT_INFO_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_BOND_RAT_INFO_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_BOND_RAT_INFO_TF' AS TAB_EN_NAME -- 表英文名
      ,'债券评级信息聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('BOND_RAT_SER_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BOND_RAT_INFO_TF_DQC_04; -- 码值异常



-- 模板名称: 聚合层-DQC质量检查脚本。此脚本由生成引擎自动生成。
-- 层次-表名: AGL-PKA-票据质押明细聚合
-- 模板作者: Zjj,GSP
-- 使用方法: DataOPS平台
-- 模板最后修改日期: 2024-01-09 08:54:00
-- 脚本类型: DML
-- 修改日志:
--     表英文名：AGL_BIL_PLEDGE_DTL_TF
--     表中文名：票据质押明细聚合
--     创建日期：2024-08-02 00:00:00
--     主键字段：BIL_NUM, BIL_PKG_SUB_START_RANGE, BIL_PKG_SUB_END_RANGE, PLEDGE_APP_BATCH_NO
--     归属层次：AGL-PKA
--     归属主题：一级领域
--     库/模式：AGL_COMM
--     分析人员：王穆军
--     时间粒度：None
--     保留周期：None
--     描述信息：涵盖了银行承兑汇票和商业承兑汇票的交易信息、票面信息、合同信息、客户信息
--     更新记录：
--         2024-08-02 00:00:00 王穆军 new



--  0.1 set parameter 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrick;
set spark.sql.orc.compression.codec=zstd;

/* 1.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_04; -- 码值异常


/* 1.2 create check result temp tables  */
CREATE TABLE AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_01 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,REC_CNT DECIMAL(28,0) COMMENT '记录数'
)
USING ORC
COMMENT '检查结果表-记录数'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_02 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_DUP_CNT DECIMAL(28,0) COMMENT '主键重复记录数'
)
USING ORC
COMMENT '检查结果表-是否主键重复'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_03 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,PK STRING COMMENT '主键'
    ,PK_NULL_CNT DECIMAL(28,0) COMMENT '主键为空记录数'
)
USING ORC
COMMENT '检查结果表-主键是否有空'
OPTIONS (compression 'zstd')
;

CREATE TABLE AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_04 (
     TAB_CN_NAME STRING COMMENT '表中文名'
    ,TAB_EN_NAME STRING COMMENT '表英文名'
    ,FIELD_CN_NAME STRING COMMENT '字段中文名'
    ,FIELD_EN_NAME STRING COMMENT '字段英文名'
    ,EXCEP_CD_VAL_CNT DECIMAL(28,0) COMMENT '异常码值记录数'
)
USING ORC
COMMENT '检查结果表-码值异常'
OPTIONS (compression 'zstd')
;

/* 2.1 put data into target table DQC_01 */
INSERT INTO AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_01
SELECT
       '票据质押明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_BIL_PLEDGE_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS REC_CNT -- 记录数
  FROM AGL_COMM.AGL_BIL_PLEDGE_DTL_TF
 WHERE PT_DT = '${process_date}';

/* 2.2 put data into target table DQC_02 */
INSERT INTO AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_02
SELECT
       '票据质押明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_BIL_PLEDGE_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('BIL_NUM, BIL_PKG_SUB_START_RANGE, BIL_PKG_SUB_END_RANGE, PLEDGE_APP_BATCH_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_DUP_CNT -- 主键重复记录数
  FROM AGL_COMM.AGL_BIL_PLEDGE_DTL_TF
 WHERE PT_DT = '${process_date}'
 GROUP BY 
 
 BIL_NUM, BIL_PKG_SUB_START_RANGE, BIL_PKG_SUB_END_RANGE, PLEDGE_APP_BATCH_NO
 HAVING COUNT(1) >1;

/* 2.3 put data into target table DQC_03 */
INSERT INTO AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_03
SELECT
       '票据质押明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_BIL_PLEDGE_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('BIL_NUM, BIL_PKG_SUB_START_RANGE, BIL_PKG_SUB_END_RANGE, PLEDGE_APP_BATCH_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS PK_NULL_CNT -- 主键为空记录数
  FROM AGL_COMM.AGL_BIL_PLEDGE_DTL_TF
 WHERE PT_DT = '${process_date}'
   AND CONCAT(
      CAST(COALESCE(BIL_NUM,'') AS STRING), CAST(COALESCE(BIL_PKG_SUB_START_RANGE,'') AS STRING), CAST(COALESCE(BIL_PKG_SUB_END_RANGE,'') AS STRING), CAST(COALESCE(PLEDGE_APP_BATCH_NO,'') AS STRING)
) = '';

/* 2.4 put data into target table DQC_04 */
/* 码值异常 请按以下 模板 按码值转换字段，每个字段填充一段，依次填充后 落地多组码值异常检查 SQL */ 
INSERT INTO AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_04
SELECT
       '票据质押明细聚合' AS TAB_CN_NAME -- 表中文名
      ,'AGL_BIL_PLEDGE_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,CAST('码值字段1中文注释' AS STRING) AS FIELD_CN_NAME -- 字段中文名
      ,CAST('码值字段1英文字段名' AS STRING) AS FIELD_EN_NAME -- 字段英文名
      ,CAST(COUNT(1) AS DECIMAL(28,0)) AS EXCEP_CD_VAL_CNT -- 异常码值记录数
  FROM AGL_COMM.AGL_BIL_PLEDGE_DTL_TF 
 WHERE PT_DT = '${process_date}'
   AND 码值字段1 like '@%';
   ;

/* 2.5 put all data into final target table T99_CHECK_RESULT_TF */
ALTER TABLE AGL_COMM.T99_CHECK_RESULT_TF DROP IF EXISTS PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_BIL_PLEDGE_DTL_TF');

INSERT INTO AGL_COMM.T99_CHECK_RESULT_TF PARTITION(PT_DT = '${process_date}', JOB_CD = 'AGL_BIL_PLEDGE_DTL_TF')
SELECT
       'AGL' AS LVL -- 归属层次
      ,'AGL_COMM' AS SUBJ -- 归属主题
      ,'AGL_BIL_PLEDGE_DTL_TF_PC' AS TASK_NAME -- 作业名
      ,'AGL_BIL_PLEDGE_DTL_TF_PC' AS TASK_ALIAS -- 作业别名
      ,'AGL_COMM' AS DB_NAME -- 库名
      ,'AGL_BIL_PLEDGE_DTL_TF' AS TAB_EN_NAME -- 表英文名
      ,'票据质押明细聚合' AS TAB_CN_NAME -- 表中文名
      ,P1.REC_CNT AS REC_CNT -- 记录数
      ,CAST('BIL_NUM, BIL_PKG_SUB_START_RANGE, BIL_PKG_SUB_END_RANGE, PLEDGE_APP_BATCH_NO' AS STRING) AS PK -- 主键(如果主键为空则为全字段)
      ,CASE WHEN P2.DUP_CNT = 0 THEN 'N'
            ELSE 'Y' END AS IS_PK_DUP -- 是否主键重复
      ,CASE WHEN P3.PK_NULL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS PK_HAVE_NULL -- 主键是否有空
      ,CASE WHEN P4.EXCEP_CD_VAL_CNT > 1 THEN 'Y'
            ELSE 'N' END AS CD_VAL_EXCEP -- 码值异常
      ,'日' AS REC_CNT -- 时间粒度
      ,'${process_date}' AS 创建日期 -- 记录数
  FROM AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_01 P1
  LEFT JOIN (SELECT COUNT(*) AS DUP_CNT FROM AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_02) P2
         ON 1 =1
  LEFT JOIN AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_03 P3
         ON P1.TAB_EN_NAME = P3.TAB_EN_NAME
  LEFT JOIN (SELECT TAB_EN_NAME,SUM(EXCEP_CD_VAL_CNT) AS EXCEP_CD_VAL_CNT FROM AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_04 GROUP BY TAB_EN_NAME) P4
         ON P1.TAB_EN_NAME = P4.TAB_EN_NAME
;

/* 3.1 drop check result temp tables if exists */
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_01; -- 记录数
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_02; -- 是否主键重复
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_03; -- 主键是否有空
DROP TABLE IF EXISTS AGL_COMM.TMP_AGL_BIL_PLEDGE_DTL_TF_DQC_04; -- 码值异常