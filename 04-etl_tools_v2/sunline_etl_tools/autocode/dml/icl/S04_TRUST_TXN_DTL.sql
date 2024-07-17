/*
*********************************************************************** 
Purpose:       主题聚合层-加工快照表脚本
Author:        Sunline
Usage:         python $ETL_HOME/script/main.py yyyymmdd ${icl_schema}_S04_TRUST_TXN_DTL
CreateDate:    2023/12/19
FileType:      DML
logs:
       表英文名：S04_TRUST_TXN_DTL
       表中文名：代销信托交易明细表
       创建日期：2023/12/19
       主键字段：申请单流水号,信托业务确认代码,TA流水号
       归属层次：聚合层
       归属主题：资管业务
       主要应用：信托交易明细
       分析人员：王穆军
       时间粒度：日
       保留周期：13M
       描述信息：包含信托代销客户购买、赎回、分红、到期兑付等的交易信息
包含信托代销个人和机构客户，对于所有类型产品的所有交易的交易流水信息
*************************************************************************/ 

\timing 
/*创建当日分区*/
   call ${itl_schema}.partition_add('${icl_schema}.S04_TRUST_TXN_DTL','pt_${batch_date}','${batch_date}'); 

/*删除当前批次历史数据*/
   call ${itl_schema}.partition_drop('${icl_schema}.S04_TRUST_TXN_DTL','pt_${batch_date}'); 



/*===================第1组====================*/

DROP TABLE IF EXISTS ${icl_schema}.TMP_S04_TRUST_TXN_DTL_01;
CREATE GLOBAL TEMPORARY TABLE ${icl_schema}.TMP_S04_TRUST_TXN_DTL_01 (
 APP_FORM_SER_NO  CHAR(30) -- 申请单流水号
  ,业务术语待申请  CHAR(3) -- 信托业务确认代码
  ,TA_SER_NO  VARCHAR(32) -- TA流水号
  ,LP_ORG_NO  VARCHAR(6) -- 法人机构编号
  ,业务术语待申请  CHAR(6) -- 交易发生机构编号
  ,业务术语待申请  CHAR(2) -- 信托业务类别代码
  ,APP_SHARE  DECIMAL(16,2) -- 申请份额
  ,APP_AMT  DECIMAL(16,2) -- 申请金额
  ,CFM_SHARE  DECIMAL(16,2) -- 确认份额
  ,CFM_AMT  DECIMAL(16,2) -- 确认金额
  ,业务术语待申请  CHAR(8) -- 交易发生日期
  ,业务术语待申请  CHAR(8) -- 交易发生时间
  ,业务术语待申请  CHAR(8) -- 下单发生日期
  ,业务术语待申请  CHAR(8) -- 下单发生时间
  ,CFM_DT  CHAR(8) -- 确认日期
  ,STL_CURR_CD  CHAR(3) -- 结算币种代码
  ,ORIG_APP_FORM_NO  VARCHAR(32) -- 原申请单编号
  ,业务术语待申请  VARCHAR(32) -- 银行账号
  ,TXN_HAPP_SELLER_NO  VARCHAR(16) -- 交易发生销售商编号
  ,业务术语待申请  CHAR(2) -- TA编号
  ,TA_ACCT_NO  CHAR(12) -- TA账户编号
  ,业务术语待申请  CHAR(17) -- 交易账号
  ,CUST_IN_CD  VARCHAR(16) -- 客户内码
  ,业务术语待申请  CHAR(1) -- 客户类型代码
  ,MAIN_DOCTYP_CD  CHAR(3) -- 主证件类型代码
  ,MAIN_DOC_NO  VARCHAR(20) -- 主证件号码
  ,OPERR_DOCTYP_CD  CHAR(3) -- 经办人证件类型代码
  ,OPERR_DOC_NO  VARCHAR(32) -- 经办人证件号码
  ,OPERR_NAME  VARCHAR(64) -- 经办人名称
  ,SIGN_BNKOUTLS_NO  CHAR(6) -- 签约网点编号
  ,OPEN_CARD_BNKOUTLS_NO  CHAR(6) -- 开卡网点编号
  ,业务术语待申请  CHAR(3) -- 信托产品代码
  ,业务术语待申请  VARCHAR(64) -- 信托产品名称
  ,业务术语待申请  CHAR(1) -- 信托产品类别代码
  ,业务术语待申请  CHAR(2) -- 信托产品风险等级代码
  ,业务术语待申请  DECIMAL(7,4) -- 信托产品净值
  ,业务术语待申请  DECIMAL(16,2) -- 收益率业绩比较基准上限
  ,业务术语待申请  DECIMAL(16,2) -- 收益率业绩比较基准下限
  ,业务术语待申请  CHAR(2) -- 信托交易申请状态代码
  ,ACPTD_MODE_CD  CHAR(1) -- 受理方式代码
  ,DIVDND_MODE_CD  CHAR(1) -- 分红方式代码
  ,业务术语待申请  CHAR(1) -- 信托收费方式代码
  ,FRZ_RSN_CD  CHAR(1) -- 冻结原因代码
  ,业务术语待申请  CHAR(8) -- 冻结截止日期
  ,业务术语待申请  CHAR(1) -- 巨额赎回处理代码
  ,业务术语待申请  CHAR(1) -- 异常交易代码
  ,业务术语待申请  CHAR(1) -- 风险等级匹配标志
  ,业务术语待申请  CHAR(1) -- 交易撤单标识代码
  ,业务术语待申请  CHAR(12) -- 对方TA账号
  ,业务术语待申请  CHAR(17) -- 对方交易账号
  ,业务术语待申请  VARCHAR(16) -- 对方网点编号
  ,业务术语待申请  CHAR(4) -- 对方地区编号
  ,业务术语待申请  VARCHAR(16) -- 对方销售商编号
  ,业务术语待申请  CHAR(6) -- 对方目标产品代码
  ,业务术语待申请  CHAR(1) -- 对方目标收费方式代码
  ,业务术语待申请  CHAR(6) -- 营销人员所属网点编号
  ,EACCT_MKTING_CUST_MGR_NO  CHAR(7) -- 电子账户营销客户经理编号
  ,OPER_TELR_NO  VARCHAR(7) -- 操作柜员编号
  ,TXN_AUTH_TELR_1_NO  CHAR(7) -- 交易授权柜员1编号
  ,TXN_AUTH_TELR_2_NO  CHAR(7) -- 交易授权柜员2编号
  ,业务术语待申请  VARCHAR(128) -- 交易处理返回错误信息
  ,业务术语待申请  CHAR(4) -- 交易处理后返回码
  ,业务术语待申请  CHAR(1) -- 业务过程完全结束代码
  ,业务术语待申请  VARCHAR(128) -- 摘要说明
)
compress(5,5)
DISTRIBUTED BY ( APP_FORM_SER_NO );

INSERT INTO ${icl_schema}.TMP_S04_TRUST_TXN_DTL_01(
  APP_FORM_SER_NO -- 申请单流水号
  ,业务术语待申请 -- 信托业务确认代码
  ,TA_SER_NO -- TA流水号
  ,LP_ORG_NO -- 法人机构编号
  ,业务术语待申请 -- 交易发生机构编号
  ,业务术语待申请 -- 信托业务类别代码
  ,APP_SHARE -- 申请份额
  ,APP_AMT -- 申请金额
  ,CFM_SHARE -- 确认份额
  ,CFM_AMT -- 确认金额
  ,业务术语待申请 -- 交易发生日期
  ,业务术语待申请 -- 交易发生时间
  ,业务术语待申请 -- 下单发生日期
  ,业务术语待申请 -- 下单发生时间
  ,CFM_DT -- 确认日期
  ,STL_CURR_CD -- 结算币种代码
  ,ORIG_APP_FORM_NO -- 原申请单编号
  ,业务术语待申请 -- 银行账号
  ,TXN_HAPP_SELLER_NO -- 交易发生销售商编号
  ,业务术语待申请 -- TA编号
  ,TA_ACCT_NO -- TA账户编号
  ,业务术语待申请 -- 交易账号
  ,CUST_IN_CD -- 客户内码
  ,业务术语待申请 -- 客户类型代码
  ,MAIN_DOCTYP_CD -- 主证件类型代码
  ,MAIN_DOC_NO -- 主证件号码
  ,OPERR_DOCTYP_CD -- 经办人证件类型代码
  ,OPERR_DOC_NO -- 经办人证件号码
  ,OPERR_NAME -- 经办人名称
  ,SIGN_BNKOUTLS_NO -- 签约网点编号
  ,OPEN_CARD_BNKOUTLS_NO -- 开卡网点编号
  ,业务术语待申请 -- 信托产品代码
  ,业务术语待申请 -- 信托产品名称
  ,业务术语待申请 -- 信托产品类别代码
  ,业务术语待申请 -- 信托产品风险等级代码
  ,业务术语待申请 -- 信托产品净值
  ,业务术语待申请 -- 收益率业绩比较基准上限
  ,业务术语待申请 -- 收益率业绩比较基准下限
  ,业务术语待申请 -- 信托交易申请状态代码
  ,ACPTD_MODE_CD -- 受理方式代码
  ,DIVDND_MODE_CD -- 分红方式代码
  ,业务术语待申请 -- 信托收费方式代码
  ,FRZ_RSN_CD -- 冻结原因代码
  ,业务术语待申请 -- 冻结截止日期
  ,业务术语待申请 -- 巨额赎回处理代码
  ,业务术语待申请 -- 异常交易代码
  ,业务术语待申请 -- 风险等级匹配标志
  ,业务术语待申请 -- 交易撤单标识代码
  ,业务术语待申请 -- 对方TA账号
  ,业务术语待申请 -- 对方交易账号
  ,业务术语待申请 -- 对方网点编号
  ,业务术语待申请 -- 对方地区编号
  ,业务术语待申请 -- 对方销售商编号
  ,业务术语待申请 -- 对方目标产品代码
  ,业务术语待申请 -- 对方目标收费方式代码
  ,业务术语待申请 -- 营销人员所属网点编号
  ,EACCT_MKTING_CUST_MGR_NO -- 电子账户营销客户经理编号
  ,OPER_TELR_NO -- 操作柜员编号
  ,TXN_AUTH_TELR_1_NO -- 交易授权柜员1编号
  ,TXN_AUTH_TELR_2_NO -- 交易授权柜员2编号
  ,业务术语待申请 -- 交易处理返回错误信息
  ,业务术语待申请 -- 交易处理后返回码
  ,业务术语待申请 -- 业务过程完全结束代码
  ,业务术语待申请 -- 摘要说明
)
SELECT
  P1.APPSHEETSERIALNO AS APPSHEETSERIALNO -- 申请单编号
  ,P1.BUSINESSCODE_APP AS BUSINESSCODE_APP -- 业务代码(申请)
  ,'' AS None -- None
  ,P1.UNIONCODE AS UNIONCODE -- 总行代码
  ,P1.OPERORG AS OPERORG -- 交易发生网点
  ,P1.BUSINESSCODE AS BUSINESSCODE -- 业务代码
  ,P1.APPLICATIONVOL AS APPLICATIONVOL -- 申请份额
  ,P1.APPLICATIONAMOUNT AS APPLICATIONAMOUNT -- 申请金额
  ,'0' AS None -- None
  ,'0' AS None -- None
  ,P1.TRANSACTIONDATE AS TRANSACTIONDATE -- 交易发生日期格式为：YYYYMMDD
  ,P1.TRANSACTIONTIME AS TRANSACTIONTIME -- 交易发生时间格式为：HHMMSS
  ,P1.OPERDATE AS OPERDATE -- 下单发生日期格式为：YYYYMMDD
  ,P1.OPERTIME AS OPERTIME -- 下单发生时间格式为：HHMMSS
  ,P1.TRANSACTIONCFMDATE AS TRANSACTIONCFMDATE -- 交易确认日期格式为：YYYYMMDD
  ,P1.CURRENCYTYPE AS CURRENCYTYPE -- 结算币种
  ,P1.ORIGINALAPPSHEETNO AS ORIGINALAPPSHEETNO -- 原申请单编号027业务使用，表示转出时的申请单编号
  ,P1.DEPOSITACCT AS DEPOSITACCT -- 投资人在销售人处用于交易的资金帐号
  ,P1.DISTRIBUTORCODE AS DISTRIBUTORCODE -- 销售商代码
  ,P1.TANO AS TANO -- TA代码
  ,P1.TAACCOUNTID AS TAACCOUNTID -- 基金帐号
  ,P1.TRANSACTIONACCOUNTID AS TRANSACTIONACCOUNTID -- 交易帐号
  ,P3.INNERCODE AS INNERCODE -- 客户内码
  ,CASE WHEN P3.INVTP IS NULL OR TRIM(P3.INVTP)='' 
     THEN '' 
ELSE COALESCE(TRIM(S1.TARGET_CD_VAL), '@'||TRIM(P3.INVTP),'') END  AS INVTP -- 投资者类型
  ,nvl(p3.INVTP,"")||'_'||nvl(p3.CERTIFICATETYPE,"") AS INVTP/CERTIFICATETYPE -- 投资者类型/证件类型
  ,P3.CERTIFICATENO AS CERTIFICATENO -- 证件号码
  ,CASE WHEN P1.TRANSACTORCERTTYPE IS NULL OR TRIM(P1.TRANSACTORCERTTYPE)='' 
     THEN '' 
ELSE COALESCE(TRIM(S3.TARGET_CD_VAL), '@'||TRIM(P1.TRANSACTORCERTTYPE),'') END 
 AS TRANSACTORCERTTYPE -- 经办人证件类型
  ,P1.TRANSACTORCERTNO AS TRANSACTORCERTNO -- 经办人证件号码
  ,P1.TRANSACTORNAME AS TRANSACTORNAME -- 经办人姓名
  ,P1.NETNO AS NETNO -- 签约网点
  ,P1.BANKNO AS BANKNO -- 开卡网点
  ,P1.FUNDCODE AS FUNDCODE -- 基金代码
  ,P2.FUNDNAME AS FUNDNAME -- 基金简称
  ,P2.FUNDTYPE AS FUNDTYPE -- 产品类型
  ,P2.RISKLEVEL AS RISKLEVEL -- 风险等级
  ,'' AS None -- None
  ,P2.INCOMEVALUEMIN AS INCOMEVALUEMIN -- 业绩比较基准下限
  ,P2.INCOMEVALUEMAX AS INCOMEVALUEMAX -- 业绩比较基准上限
  ,P1.STATUS AS STATUS -- 申请状态
  ,P1.ACCEPTMETHOD AS ACCEPTMETHOD -- 受理方式
  ,P1.DEFDIVIDENDMETHOD AS DEFDIVIDENDMETHOD -- 默认分红方式
  ,P1.SHARETYPE AS SHARETYPE -- 收费方式
  ,P1.FROZENCAUSE AS FROZENCAUSE -- 冻结原因
  ,P1.FREEZINGDEADLINE AS FREEZINGDEADLINE -- 冻结截止日期格式为：YYYYMMDD
  ,P1.LARGEREDEMPTIONFLAG AS LARGEREDEMPTIONFLAG -- 巨额赎回处理标志
  ,P1.EXCEPTIONALFLAG AS EXCEPTIONALFLAG -- 异常交易标志
  ,P1.RISKMATCHING AS RISKMATCHING -- 风险等级是否匹配
  ,P1.CANCELFLAG AS CANCELFLAG -- 撤单标志
  ,P1.TARGETTAACCOUNTID AS TARGETTAACCOUNTID -- 对方基金账号
  ,P1.TARGETTRANSACTIONACCOUNTID AS TARGETTRANSACTIONACCOUNTID -- 对方交易帐号在销售商是一次完成时，为必须项
  ,P1.TARGETBRANCHCODE AS TARGETBRANCHCODE -- 对方网点号转销售商、非交易过户时使用
  ,P1.TARGETREGIONCODE AS TARGETREGIONCODE -- 对方地区编号具体编码依GB13497-92
  ,P1.TARGETDISTRIBUTORCODE AS TARGETDISTRIBUTORCODE -- 对方销售商代码
  ,P1.TARGETFUNDCODE AS TARGETFUNDCODE -- 对方目标基金代码
  ,P1.TARGETSHARETYPE AS TARGETSHARETYPE -- 对方目标收费方式
  ,P4.BANKNODE AS BANKNODE -- 银行网点号码
  ,P1.CUSTMANAGERID AS CUSTMANAGERID -- 客户经理代码
  ,P1.OPERID AS OPERID -- 操作柜员
  ,P1.CONFIRMOPID1 AS CONFIRMOPID1 -- 复核柜员1
  ,P1.CONFIRMOPID2 AS CONFIRMOPID2 -- 复核柜员2
  ,P1.RETURNMSG AS RETURNMSG -- 交易处理返回错误信息
  ,P1.RETURNCODE AS RETURNCODE -- 交易处理后返回码
  ,'' AS None -- None
  ,P1.SPECIFICATION AS SPECIFICATION -- 摘要说明
 FROM ${ods_tss_schema}.ods_tss_trust_h_app_trans  P1 -- 交易申请历史表 
LEFT JOIN ${ods_tss_schema}.ods_tss_trust_cfg_fund  P2 -- 基金信息 
 ON P1.UNIONCODE = P2.UNIONCODE 
AND P1.TANO=P2.TANO 
AND P1.FUNDCODE = P2.FUNDCODE
AND P2.PT_DT='${batch_date}' 
AND P2.DELETED='0'
LEFT JOIN ${ods_tss_schema}.ods_tss_trust_acct_cust  P3 -- 客户信息表 
 ON p3.UNIONCODE = p1.UNIONCODE   
and p3.CUSTNO = p1.CUSTNO
and P3.PT_DT='${batch_date}' 
AND P3.DELETED='0'
LEFT JOIN ${ods_tss_schema}.ods_tss_trust_acct_custmanager  P4 -- 客户经理信息 
 ON AND P4.UNIONCODE = P1.UNIONCODE  
AND P4.CUSTMANAGERID = P1.CUSTMANAGERID
AND P4.PT_DT='${batch_date}' 
AND P4.DELETED='0'
LEFT JOIN 代码表待命名  S1 -- 代码转换 
 ON P3.CURRENCYTYPE = S1.SRC_CD_VAL 
AND S1.SRC_SYS_CD = 'TSS'
AND S1.SRC_COL_NAME ='INVTP'
AND S1.SRC_TAB_NAME = 'ODS_TSS_TRUST_ACCT_CUST'
LEFT JOIN 代码表待命名  S2 -- 代码转换 
 ON P3.CERTIFICATETYPE = S2.SRC_CD_VAL 
AND S2.SRC_SYS_CD = 'TSS'
AND S2.SRC_COL_NAME ='CERTIFICATETYPE'
AND S2.SRC_TAB_NAME = 'ODS_TSS_TRUST_ACCT_CUST'
LEFT JOIN 代码表待命名  S3 -- 代码转换 
 ON P1.TRANSACTORCERTTYPE = S3.SRC_CD_VAL 
AND S3.SRC_SYS_CD = 'TSS'
AND S3.SRC_COL_NAME ='TRANSACTORCERTTYPE'
AND S3.SRC_TAB_NAME = 'ODS_TSS_TRUST_H_APP_TRANS'
 WHERE P1.PT_DT='${batch_date}' 
AND P1.DELETED='0'
 
;


/*添加表分析*/ 
ANALYZE TABLE ${icl_schema}.TMP_S04_TRUST_TXN_DTL_01;


/*===================第2组====================*/

DROP TABLE IF EXISTS ${icl_schema}.TMP_S04_TRUST_TXN_DTL_02;
CREATE GLOBAL TEMPORARY TABLE ${icl_schema}.TMP_S04_TRUST_TXN_DTL_02 (
 APP_FORM_SER_NO  CHAR(30) -- 申请单流水号
  ,业务术语待申请  CHAR(3) -- 信托业务确认代码
  ,TA_SER_NO  VARCHAR(32) -- TA流水号
  ,LP_ORG_NO  VARCHAR(6) -- 法人机构编号
  ,业务术语待申请  CHAR(6) -- 交易发生机构编号
  ,业务术语待申请  CHAR(2) -- 信托业务类别代码
  ,APP_SHARE  DECIMAL(16,2) -- 申请份额
  ,APP_AMT  DECIMAL(16,2) -- 申请金额
  ,CFM_SHARE  DECIMAL(16,2) -- 确认份额
  ,CFM_AMT  DECIMAL(16,2) -- 确认金额
  ,业务术语待申请  CHAR(8) -- 交易发生日期
  ,业务术语待申请  CHAR(8) -- 交易发生时间
  ,业务术语待申请  CHAR(8) -- 下单发生日期
  ,业务术语待申请  CHAR(8) -- 下单发生时间
  ,CFM_DT  CHAR(8) -- 确认日期
  ,STL_CURR_CD  CHAR(3) -- 结算币种代码
  ,ORIG_APP_FORM_NO  VARCHAR(32) -- 原申请单编号
  ,业务术语待申请  VARCHAR(32) -- 银行账号
  ,TXN_HAPP_SELLER_NO  VARCHAR(16) -- 交易发生销售商编号
  ,业务术语待申请  CHAR(2) -- TA编号
  ,TA_ACCT_NO  CHAR(12) -- TA账户编号
  ,业务术语待申请  CHAR(17) -- 交易账号
  ,CUST_IN_CD  VARCHAR(16) -- 客户内码
  ,业务术语待申请  CHAR(1) -- 客户类型代码
  ,MAIN_DOCTYP_CD  CHAR(3) -- 主证件类型代码
  ,MAIN_DOC_NO  VARCHAR(20) -- 主证件号码
  ,OPERR_DOCTYP_CD  CHAR(3) -- 经办人证件类型代码
  ,OPERR_DOC_NO  VARCHAR(32) -- 经办人证件号码
  ,OPERR_NAME  VARCHAR(64) -- 经办人名称
  ,SIGN_BNKOUTLS_NO  CHAR(6) -- 签约网点编号
  ,OPEN_CARD_BNKOUTLS_NO  CHAR(6) -- 开卡网点编号
  ,业务术语待申请  CHAR(3) -- 信托产品代码
  ,业务术语待申请  VARCHAR(64) -- 信托产品名称
  ,业务术语待申请  CHAR(1) -- 信托产品类别代码
  ,业务术语待申请  CHAR(2) -- 信托产品风险等级代码
  ,业务术语待申请  DECIMAL(7,4) -- 信托产品净值
  ,业务术语待申请  DECIMAL(16,2) -- 收益率业绩比较基准上限
  ,业务术语待申请  DECIMAL(16,2) -- 收益率业绩比较基准下限
  ,业务术语待申请  CHAR(2) -- 信托交易申请状态代码
  ,ACPTD_MODE_CD  CHAR(1) -- 受理方式代码
  ,DIVDND_MODE_CD  CHAR(1) -- 分红方式代码
  ,业务术语待申请  CHAR(1) -- 信托收费方式代码
  ,FRZ_RSN_CD  CHAR(1) -- 冻结原因代码
  ,业务术语待申请  CHAR(8) -- 冻结截止日期
  ,业务术语待申请  CHAR(1) -- 巨额赎回处理代码
  ,业务术语待申请  CHAR(1) -- 异常交易代码
  ,业务术语待申请  CHAR(1) -- 风险等级匹配标志
  ,业务术语待申请  CHAR(1) -- 交易撤单标识代码
  ,业务术语待申请  CHAR(12) -- 对方TA账号
  ,业务术语待申请  CHAR(17) -- 对方交易账号
  ,业务术语待申请  VARCHAR(16) -- 对方网点编号
  ,业务术语待申请  CHAR(4) -- 对方地区编号
  ,业务术语待申请  VARCHAR(16) -- 对方销售商编号
  ,业务术语待申请  CHAR(6) -- 对方目标产品代码
  ,业务术语待申请  CHAR(1) -- 对方目标收费方式代码
  ,业务术语待申请  CHAR(6) -- 营销人员所属网点编号
  ,EACCT_MKTING_CUST_MGR_NO  CHAR(7) -- 电子账户营销客户经理编号
  ,OPER_TELR_NO  VARCHAR(7) -- 操作柜员编号
  ,TXN_AUTH_TELR_1_NO  CHAR(7) -- 交易授权柜员1编号
  ,TXN_AUTH_TELR_2_NO  CHAR(7) -- 交易授权柜员2编号
  ,业务术语待申请  VARCHAR(128) -- 交易处理返回错误信息
  ,业务术语待申请  CHAR(4) -- 交易处理后返回码
  ,业务术语待申请  CHAR(1) -- 业务过程完全结束代码
  ,业务术语待申请  VARCHAR(128) -- 摘要说明
)
compress(5,5)
DISTRIBUTED BY ( APP_FORM_SER_NO );

INSERT INTO ${icl_schema}.TMP_S04_TRUST_TXN_DTL_02(
  APP_FORM_SER_NO -- 申请单流水号
  ,业务术语待申请 -- 信托业务确认代码
  ,TA_SER_NO -- TA流水号
  ,LP_ORG_NO -- 法人机构编号
  ,业务术语待申请 -- 交易发生机构编号
  ,业务术语待申请 -- 信托业务类别代码
  ,APP_SHARE -- 申请份额
  ,APP_AMT -- 申请金额
  ,CFM_SHARE -- 确认份额
  ,CFM_AMT -- 确认金额
  ,业务术语待申请 -- 交易发生日期
  ,业务术语待申请 -- 交易发生时间
  ,业务术语待申请 -- 下单发生日期
  ,业务术语待申请 -- 下单发生时间
  ,CFM_DT -- 确认日期
  ,STL_CURR_CD -- 结算币种代码
  ,ORIG_APP_FORM_NO -- 原申请单编号
  ,业务术语待申请 -- 银行账号
  ,TXN_HAPP_SELLER_NO -- 交易发生销售商编号
  ,业务术语待申请 -- TA编号
  ,TA_ACCT_NO -- TA账户编号
  ,业务术语待申请 -- 交易账号
  ,CUST_IN_CD -- 客户内码
  ,业务术语待申请 -- 客户类型代码
  ,MAIN_DOCTYP_CD -- 主证件类型代码
  ,MAIN_DOC_NO -- 主证件号码
  ,OPERR_DOCTYP_CD -- 经办人证件类型代码
  ,OPERR_DOC_NO -- 经办人证件号码
  ,OPERR_NAME -- 经办人名称
  ,SIGN_BNKOUTLS_NO -- 签约网点编号
  ,OPEN_CARD_BNKOUTLS_NO -- 开卡网点编号
  ,业务术语待申请 -- 信托产品代码
  ,业务术语待申请 -- 信托产品名称
  ,业务术语待申请 -- 信托产品类别代码
  ,业务术语待申请 -- 信托产品风险等级代码
  ,业务术语待申请 -- 信托产品净值
  ,业务术语待申请 -- 收益率业绩比较基准上限
  ,业务术语待申请 -- 收益率业绩比较基准下限
  ,业务术语待申请 -- 信托交易申请状态代码
  ,ACPTD_MODE_CD -- 受理方式代码
  ,DIVDND_MODE_CD -- 分红方式代码
  ,业务术语待申请 -- 信托收费方式代码
  ,FRZ_RSN_CD -- 冻结原因代码
  ,业务术语待申请 -- 冻结截止日期
  ,业务术语待申请 -- 巨额赎回处理代码
  ,业务术语待申请 -- 异常交易代码
  ,业务术语待申请 -- 风险等级匹配标志
  ,业务术语待申请 -- 交易撤单标识代码
  ,业务术语待申请 -- 对方TA账号
  ,业务术语待申请 -- 对方交易账号
  ,业务术语待申请 -- 对方网点编号
  ,业务术语待申请 -- 对方地区编号
  ,业务术语待申请 -- 对方销售商编号
  ,业务术语待申请 -- 对方目标产品代码
  ,业务术语待申请 -- 对方目标收费方式代码
  ,业务术语待申请 -- 营销人员所属网点编号
  ,EACCT_MKTING_CUST_MGR_NO -- 电子账户营销客户经理编号
  ,OPER_TELR_NO -- 操作柜员编号
  ,TXN_AUTH_TELR_1_NO -- 交易授权柜员1编号
  ,TXN_AUTH_TELR_2_NO -- 交易授权柜员2编号
  ,业务术语待申请 -- 交易处理返回错误信息
  ,业务术语待申请 -- 交易处理后返回码
  ,业务术语待申请 -- 业务过程完全结束代码
  ,业务术语待申请 -- 摘要说明
)
SELECT
  P1.APPSHEETSERIALNO AS APPSHEETSERIALNO -- 申请单编号
  ,P1.BUSINESSCODE_APP AS BUSINESSCODE_ACK -- 业务代码(确认)
  ,P1.TASERIALNO AS TASERIALNO -- TA确认流水号
  ,P1.UNIONCODE AS UNIONCODE -- 总行代码
  ,P1.OPERORG AS OPERORG -- 交易发生网点
  ,P1.BUSINESSCODE AS BUSINESSCODE -- 业务代码(确认)
  ,P1.APPLICATIONVOL AS APPLICATIONVOL -- 申请基金份数初始申报
  ,P1.APPLICATIONAMOUNT AS APPLICATIONAMOUNT -- 申请金额初始申报
  ,P1.CONFIRMEDVOL AS CONFIRMEDVOL -- 交易确认份数管理人最终确认[巨额赎回处理的最终结果]
  ,P1.CONFIRMEDAMOUNT AS CONFIRMEDAMOUNT -- 交易确认金额金额为全额管理人最终确认[比例配售的最终结果]
  ,P1.TRANSACTIONDATE AS TRANSACTIONDATE -- 交易发生日期格式为：YYYYMMDD
  ,P1.TRANSACTIONTIME AS TRANSACTIONTIME -- 交易发生时间格式为：HHMMSS
  ,P1.OPERDATE AS OPERDATE -- 下单发生日期格式为：YYYYMMDD
  ,P1.OPERTIME AS OPERTIME -- 下单发生时间格式为：HHMMSS
  ,P1.TRANSACTIONCFMDATE AS TRANSACTIONCFMDATE -- 交易确认日期格式为：YYYYMMDDY申请倒入后就计算好
  ,CASE WHEN P1.CURRENCYTYPE IS NULL OR TRIM(P1.CURRENCYTYPE)='' 
     THEN '' 
ELSE COALESCE(TRIM(S4.TARGET_CD_VAL), '@'||TRIM(P1.CURRENCYTYPE),'') END  AS CURRENCYTYPE -- 结算币种
  ,P1.ORIGINALAPPSHEETNO AS ORIGINALAPPSHEETNO -- 原申请单编号027业务使用，表示转出时的申请单编号
  ,P1.DEPOSITACCT AS DEPOSITACCT -- 投资人在销售人处用于交易的资金帐号
  ,P1.DISTRIBUTORCODE AS DISTRIBUTORCODE -- 销售商代码
  ,P1.TANO AS TANO -- TA代码
  ,P1.TAACCOUNTID AS TAACCOUNTID -- 基金帐号
  ,P1.TRANSACTIONACCOUNTID AS TRANSACTIONACCOUNTID -- 交易帐号
  ,P3.INNERCODE AS CUST_ISN -- 客户内码
  ,CASE WHEN P1.INVTP IS NULL OR TRIM(P1.INVTP)='' 
     THEN '' 
ELSE COALESCE(TRIM(S1.TARGET_CD_VAL), '@'||TRIM(P1.INVTP),'') END  AS INVTP -- 投资者类型
  ,nvl(P3.INVTP,"")||'_'||nvl(P3.CERTIFICATETYPE,"") AS INVTP/CERTIFICATETYPE -- 投资者类型/证件类型
  ,P3.CERTIFICATENO AS CERTIFICATENO -- 证件号码
  ,CASE WHEN P5.TRANSACTORCERTTYPE IS NULL OR TRIM(P5.TRANSACTORCERTTYPE)='' 
     THEN '' 
ELSE COALESCE(TRIM(S3.TARGET_CD_VAL), '@'||TRIM(P5.TRANSACTORCERTTYPE),'') END 
 AS TRANSACTORCERTTYPE -- 经办人证件类型
  ,P5.TRANSACTORCERTNO AS TRANSACTORCERTNO -- 经办人证件号码
  ,P5.TRANSACTORNAME AS TRANSACTORNAME -- 经办人姓名
  ,P3.NETNO AS NETNO -- 签约网点
  ,P1.BANKNO AS BANKNO -- 开卡网点
  ,P1.FUNDCODE AS FUNDCODE -- 基金代码
  ,COALESCE(TRIM(P2.fundname),'') AS FUNDNAME -- 基金简称
  ,P2.FUNDTYPE AS FUNDTYPE -- 产品类型
  ,COALESCE(TRIM(P2.RISKLEVEL),'') AS RISKLEVEL -- 风险等级
  ,P1.NAV AS NAV -- 基金单位净值
  ,P2.INCOMEVALUEMIN AS INCOMEVALUEMIN -- 业绩比较基准下限
  ,P2.INCOMEVALUEMAX AS INCOMEVALUEMAX -- 业绩比较基准上限
  ,'07' AS None -- None
  ,P1.ACCEPTMETHOD AS ACCEPTMETHOD -- 受理方式
  ,COALESCE(TRIM(P5.DEFDIVIDENDMETHOD),TRIM(P1.DEFDIVIDENDMETHOD)) AS DEFDIVIDENDMETHOD -- 默认分红方式
  ,P1.SHARETYPE AS SHARETYPE -- 收费方式
  ,P1.FROZENCAUSE AS FROZENCAUSE -- 冻结原因
  ,P1.FREEZINGDEADLINE AS FREEZINGDEADLINE -- 冻结截止日期格式为：YYYYMMDD
  ,P1.LARGEREDEMPTIONFLAG AS LARGEREDEMPTIONFLAG -- 巨额赎回处理标志
  ,P1.EXCEPTIONALFLAG AS EXCEPTIONALFLAG -- 异常交易标志
  ,P1.RISKMATCHING AS RISKMATCHING -- 风险等级是否匹配
  ,'F' AS None -- None
  ,P1.TARGETTAACCOUNTID AS TARGETTAACCOUNTID -- 对方基金账号
  ,P1.TARGETTRANSACTIONACCOUNTID AS TARGETTRANSACTIONACCOUNTID -- 对方交易帐号在销售商是一次完成时，为必须项
  ,P1.TARGETBRANCHCODE AS TARGETBRANCHCODE -- 对方网点号转销售商、非交易过户时使用
  ,P1.TARGETREGIONCODE AS TARGETREGIONCODE -- 对方地区编号具体编码依GB13497-92
  ,P1.TARGETDISTRIBUTORCODE AS TARGETDISTRIBUTORCODE -- 对方销售商代码
  ,P1.TARGETFUNDCODE AS TARGETFUNDCODE -- 对方目标基金代码
  ,P1.TARGETSHARETYPE AS TARGETSHARETYPE -- 对方目标收费方式
  ,P4.BANKNODE AS BANKNODE -- 银行网点号码
  ,P1.CUSTMANAGERID AS CUSTMANAGERID -- 客户经理代码
  ,P1.OPERID AS OPERID -- 操作柜员
  ,P5.CONFIRMOPID1 AS CONFIRMOPID1 -- 复核柜员1
  ,P5.CONFIRMOPID2 AS CONFIRMOPID2 -- 复核柜员2
  ,P1.RETURNMSG AS RETURNMSG -- 交易处理返回代码系统处理后返回错误信息
  ,P1.RETURNCODE AS RETURNCODE -- 交易处理返回代码系统处理后返回码
  ,P1.BUSINESSFINISHFLAG AS BUSINESSFINISHFLAG -- 业务过程完全结束标识
  ,P1.SPECIFICATION AS SPECIFICATION -- 摘要说明
 FROM ${ods_tss_schema}.ods_tss_trust_h_ack_trans  P1 -- 交易确认表 
LEFT JOIN ${ods_tss_schema}.ods_tss_trust_cfg_fund  P2 -- 基金信息 
 ON P1.UNIONCODE = P2.UNIONCODE 
AND P1.TANO=P2.TANO 
AND P1.FUNDCODE = P2.FUNDCODE
AND P2.PT_DT='${batch_date}' 
AND P2.DELETED='0'
LEFT JOIN ${ods_tss_schema}.ods_tss_trust_acct_cust  P3 -- 客户信息表 
 ON p3.UNIONCODE = p1.UNIONCODE   
and p3.CUSTNO = p1.CUSTNO
and P3.PT_DT='${batch_date}' 
AND P3.DELETED='0'
LEFT JOIN ${ods_tss_schema}.ods_tss_trust_acct_custmanager  P4 -- 客户经理信息 
 ON AND P4.UNIONCODE = P1.UNIONCODE  
AND P4.CUSTMANAGERID = P1.CUSTMANAGERID
AND P4.PT_DT='${batch_date}' 
AND P4.DELETED='0'
LEFT JOIN ${ods_tss_schema}.ods_tss_trust_h_app_trans  P5 -- 交易申请历史表 
 ON P5.APPSHEETSERIALNO = P1.APPSHEETSERIALNO
LEFT JOIN 代码表待命名  S1 -- 代码转换 
 ON P1.INVTP = S1.SRC_CD_VAL 
AND S1.SRC_SYS_CD = 'TSS'
AND S1.SRC_COL_NAME ='INVTP'
AND S1.SRC_TAB_NAME = 'ODS_TSS_TRUST_H_ACK_TRANS'
LEFT JOIN 代码表待命名  S2 -- 代码转换 
 ON P3.CERTIFICATETYPE = S2.SRC_CD_VAL 
AND S2.SRC_SYS_CD = 'TSS'
AND S2.SRC_COL_NAME ='CERTIFICATETYPE'
AND S2.SRC_TAB_NAME = 'ODS_TSS_TRUST_ACCT_CUST'
LEFT JOIN 代码表待命名  S3 -- 代码转换 
 ON P5.TRANSACTORCERTTYPE = S3.SRC_CD_VAL 
AND S3.SRC_SYS_CD = 'TSS'
AND S3.SRC_COL_NAME ='TRANSACTORCERTTYPE'
AND S3.SRC_TAB_NAME = 'ODS_TSS_TRUST_H_APP_TRANS'
LEFT JOIN 代码表待命名  S4 -- 代码转换 
 ON P1.CURRENCYTYPE = S4.SRC_CD_VAL 
AND S4.SRC_SYS_CD = 'TSS'
AND S4.SRC_COL_NAME ='CURRENCYTYPE'
AND S4.SRC_TAB_NAME = 'ODS_TSS_TRUST_H_ACK_TRANS'
 WHERE P1.PT_DT='${batch_date}' 
AND P1.DELETED='0'
 
;


/*添加表分析*/ 
ANALYZE TABLE ${icl_schema}.TMP_S04_TRUST_TXN_DTL_02;


/*===================第3组====================*/

DROP TABLE IF EXISTS ${icl_schema}.TMP_S04_TRUST_TXN_DTL_03;
CREATE GLOBAL TEMPORARY TABLE ${icl_schema}.TMP_S04_TRUST_TXN_DTL_03 (
 APP_FORM_SER_NO  CHAR(30) -- 申请单流水号
  ,业务术语待申请  CHAR(3) -- 信托业务确认代码
  ,TA_SER_NO  VARCHAR(32) -- TA流水号
  ,LP_ORG_NO  VARCHAR(6) -- 法人机构编号
  ,业务术语待申请  CHAR(6) -- 交易发生机构编号
  ,业务术语待申请  CHAR(2) -- 信托业务类别代码
  ,APP_SHARE  DECIMAL(16,2) -- 申请份额
  ,APP_AMT  DECIMAL(16,2) -- 申请金额
  ,CFM_SHARE  DECIMAL(16,2) -- 确认份额
  ,CFM_AMT  DECIMAL(16,2) -- 确认金额
  ,业务术语待申请  CHAR(8) -- 交易发生日期
  ,业务术语待申请  CHAR(8) -- 交易发生时间
  ,业务术语待申请  CHAR(8) -- 下单发生日期
  ,业务术语待申请  CHAR(8) -- 下单发生时间
  ,CFM_DT  CHAR(8) -- 确认日期
  ,STL_CURR_CD  CHAR(3) -- 结算币种代码
  ,ORIG_APP_FORM_NO  VARCHAR(32) -- 原申请单编号
  ,业务术语待申请  VARCHAR(32) -- 银行账号
  ,TXN_HAPP_SELLER_NO  VARCHAR(16) -- 交易发生销售商编号
  ,业务术语待申请  CHAR(2) -- TA编号
  ,TA_ACCT_NO  CHAR(12) -- TA账户编号
  ,业务术语待申请  CHAR(17) -- 交易账号
  ,CUST_IN_CD  VARCHAR(16) -- 客户内码
  ,业务术语待申请  CHAR(1) -- 客户类型代码
  ,MAIN_DOCTYP_CD  CHAR(3) -- 主证件类型代码
  ,MAIN_DOC_NO  VARCHAR(20) -- 主证件号码
  ,OPERR_DOCTYP_CD  CHAR(3) -- 经办人证件类型代码
  ,OPERR_DOC_NO  VARCHAR(32) -- 经办人证件号码
  ,OPERR_NAME  VARCHAR(64) -- 经办人名称
  ,SIGN_BNKOUTLS_NO  CHAR(6) -- 签约网点编号
  ,OPEN_CARD_BNKOUTLS_NO  CHAR(6) -- 开卡网点编号
  ,业务术语待申请  CHAR(3) -- 信托产品代码
  ,业务术语待申请  VARCHAR(64) -- 信托产品名称
  ,业务术语待申请  CHAR(1) -- 信托产品类别代码
  ,业务术语待申请  CHAR(2) -- 信托产品风险等级代码
  ,业务术语待申请  DECIMAL(7,4) -- 信托产品净值
  ,业务术语待申请  DECIMAL(16,2) -- 收益率业绩比较基准上限
  ,业务术语待申请  DECIMAL(16,2) -- 收益率业绩比较基准下限
  ,业务术语待申请  CHAR(2) -- 信托交易申请状态代码
  ,ACPTD_MODE_CD  CHAR(1) -- 受理方式代码
  ,DIVDND_MODE_CD  CHAR(1) -- 分红方式代码
  ,业务术语待申请  CHAR(1) -- 信托收费方式代码
  ,FRZ_RSN_CD  CHAR(1) -- 冻结原因代码
  ,业务术语待申请  CHAR(8) -- 冻结截止日期
  ,业务术语待申请  CHAR(1) -- 巨额赎回处理代码
  ,业务术语待申请  CHAR(1) -- 异常交易代码
  ,业务术语待申请  CHAR(1) -- 风险等级匹配标志
  ,业务术语待申请  CHAR(1) -- 交易撤单标识代码
  ,业务术语待申请  CHAR(12) -- 对方TA账号
  ,业务术语待申请  CHAR(17) -- 对方交易账号
  ,业务术语待申请  VARCHAR(16) -- 对方网点编号
  ,业务术语待申请  CHAR(4) -- 对方地区编号
  ,业务术语待申请  VARCHAR(16) -- 对方销售商编号
  ,业务术语待申请  CHAR(6) -- 对方目标产品代码
  ,业务术语待申请  CHAR(1) -- 对方目标收费方式代码
  ,业务术语待申请  CHAR(6) -- 营销人员所属网点编号
  ,EACCT_MKTING_CUST_MGR_NO  CHAR(7) -- 电子账户营销客户经理编号
  ,OPER_TELR_NO  VARCHAR(7) -- 操作柜员编号
  ,TXN_AUTH_TELR_1_NO  CHAR(7) -- 交易授权柜员1编号
  ,TXN_AUTH_TELR_2_NO  CHAR(7) -- 交易授权柜员2编号
  ,业务术语待申请  VARCHAR(128) -- 交易处理返回错误信息
  ,业务术语待申请  CHAR(4) -- 交易处理后返回码
  ,业务术语待申请  CHAR(1) -- 业务过程完全结束代码
  ,业务术语待申请  VARCHAR(128) -- 摘要说明
)
compress(5,5)
DISTRIBUTED BY ( APP_FORM_SER_NO );

INSERT INTO ${icl_schema}.TMP_S04_TRUST_TXN_DTL_03(
  APP_FORM_SER_NO -- 申请单流水号
  ,业务术语待申请 -- 信托业务确认代码
  ,TA_SER_NO -- TA流水号
  ,LP_ORG_NO -- 法人机构编号
  ,业务术语待申请 -- 交易发生机构编号
  ,业务术语待申请 -- 信托业务类别代码
  ,APP_SHARE -- 申请份额
  ,APP_AMT -- 申请金额
  ,CFM_SHARE -- 确认份额
  ,CFM_AMT -- 确认金额
  ,业务术语待申请 -- 交易发生日期
  ,业务术语待申请 -- 交易发生时间
  ,业务术语待申请 -- 下单发生日期
  ,业务术语待申请 -- 下单发生时间
  ,CFM_DT -- 确认日期
  ,STL_CURR_CD -- 结算币种代码
  ,ORIG_APP_FORM_NO -- 原申请单编号
  ,业务术语待申请 -- 银行账号
  ,TXN_HAPP_SELLER_NO -- 交易发生销售商编号
  ,业务术语待申请 -- TA编号
  ,TA_ACCT_NO -- TA账户编号
  ,业务术语待申请 -- 交易账号
  ,CUST_IN_CD -- 客户内码
  ,业务术语待申请 -- 客户类型代码
  ,MAIN_DOCTYP_CD -- 主证件类型代码
  ,MAIN_DOC_NO -- 主证件号码
  ,OPERR_DOCTYP_CD -- 经办人证件类型代码
  ,OPERR_DOC_NO -- 经办人证件号码
  ,OPERR_NAME -- 经办人名称
  ,SIGN_BNKOUTLS_NO -- 签约网点编号
  ,OPEN_CARD_BNKOUTLS_NO -- 开卡网点编号
  ,业务术语待申请 -- 信托产品代码
  ,业务术语待申请 -- 信托产品名称
  ,业务术语待申请 -- 信托产品类别代码
  ,业务术语待申请 -- 信托产品风险等级代码
  ,业务术语待申请 -- 信托产品净值
  ,业务术语待申请 -- 收益率业绩比较基准上限
  ,业务术语待申请 -- 收益率业绩比较基准下限
  ,业务术语待申请 -- 信托交易申请状态代码
  ,ACPTD_MODE_CD -- 受理方式代码
  ,DIVDND_MODE_CD -- 分红方式代码
  ,业务术语待申请 -- 信托收费方式代码
  ,FRZ_RSN_CD -- 冻结原因代码
  ,业务术语待申请 -- 冻结截止日期
  ,业务术语待申请 -- 巨额赎回处理代码
  ,业务术语待申请 -- 异常交易代码
  ,业务术语待申请 -- 风险等级匹配标志
  ,业务术语待申请 -- 交易撤单标识代码
  ,业务术语待申请 -- 对方TA账号
  ,业务术语待申请 -- 对方交易账号
  ,业务术语待申请 -- 对方网点编号
  ,业务术语待申请 -- 对方地区编号
  ,业务术语待申请 -- 对方销售商编号
  ,业务术语待申请 -- 对方目标产品代码
  ,业务术语待申请 -- 对方目标收费方式代码
  ,业务术语待申请 -- 营销人员所属网点编号
  ,EACCT_MKTING_CUST_MGR_NO -- 电子账户营销客户经理编号
  ,OPER_TELR_NO -- 操作柜员编号
  ,TXN_AUTH_TELR_1_NO -- 交易授权柜员1编号
  ,TXN_AUTH_TELR_2_NO -- 交易授权柜员2编号
  ,业务术语待申请 -- 交易处理返回错误信息
  ,业务术语待申请 -- 交易处理后返回码
  ,业务术语待申请 -- 业务过程完全结束代码
  ,业务术语待申请 -- 摘要说明
)
SELECT
  P1.APPSHEETSERIALNO AS APPSHEETSERIALNO -- 申请单编号
  ,P1.BUSINESSCODE_APP AS BUSINESSCODE_APP -- 业务代码(申请)
  ,'' AS None -- None
  ,P1.UNIONCODE AS UNIONCODE -- 总行代码
  ,P1.OPERORG AS OPERORG -- 交易发生网点
  ,P1.BUSINESSCODE AS BUSINESSCODE -- 业务代码
  ,P1.APPLICATIONVOL AS APPLICATIONVOL -- 申请份额
  ,P1.APPLICATIONAMOUNT AS APPLICATIONAMOUNT -- 申请金额
  ,'0' AS None -- None
  ,'0' AS None -- None
  ,P1.TRANSACTIONDATE AS TRANSACTIONDATE -- 交易发生日期格式为：YYYYMMDD
  ,P1.TRANSACTIONTIME AS TRANSACTIONTIME -- 交易发生时间格式为：HHMMSS
  ,P1.OPERDATE AS OPERDATE -- 下单发生日期格式为：YYYYMMDD
  ,P1.OPERTIME AS OPERTIME -- 下单发生时间格式为：HHMMSS
  ,P1.TRANSACTIONCFMDATE AS TRANSACTIONCFMDATE -- 交易确认日期格式为：YYYYMMDD
  ,CASE WHEN P1.CURRENCYTYPE IS NULL OR TRIM(P1.CURRENCYTYPE)='' 
     THEN '' 
ELSE COALESCE(TRIM(S4.TARGET_CD_VAL), '@'||TRIM(P1.CURRENCYTYPE),'') END  AS CURRENCYTYPE -- 结算币种
  ,P1.ORIGINALAPPSHEETNO AS ORIGINALAPPSHEETNO -- 原申请单编号027业务使用，表示转出时的申请单编号
  ,P1.DEPOSITACCT AS DEPOSITACCT -- 投资人在销售人处用于交易的资金帐号
  ,P1.DISTRIBUTORCODE AS DISTRIBUTORCODE -- 销售商代码
  ,P1.TANO AS TANO -- TA代码
  ,P1.TAACCOUNTID AS TAACCOUNTID -- 基金帐号
  ,P1.TRANSACTIONACCOUNTID AS TRANSACTIONACCOUNTID -- 交易帐号
  ,P3.INNERCODE AS INNERCODE -- 客户内码
  ,CASE WHEN P3.INVTP IS NULL OR TRIM(P3.INVTP)='' 
     THEN '' 
ELSE COALESCE(TRIM(S1.TARGET_CD_VAL), '@'||TRIM(P3.INVTP),'') END  AS INVTP -- 投资者类型
  ,nvl(P3.INVTP,"")||'_'||nvl(P3.CERTIFICATETYPE,"") AS INVTP/CERTIFICATETYPE -- 投资者类型/证件类型
  ,P3.CERTIFICATENO AS CERTIFICATENO -- 证件号码
  ,P1.TRANSACTORCERTTYPE AS TRANSACTORCERTTYPE -- 经办人证件类型
  ,P1.TRANSACTORCERTNO AS TRANSACTORCERTNO -- 经办人证件号码
  ,P1.TRANSACTORNAME AS TRANSACTORNAME -- 经办人姓名
  ,P1.NETNO AS NETNO -- 签约网点
  ,P1.BANKNO AS BANKNO -- 开卡网点
  ,P1.FUNDCODE AS FUNDCODE -- 基金代码
  ,P2.FUNDNAME AS FUNDNAME -- 基金简称
  ,P2.FUNDTYPE AS FUNDTYPE -- 产品类型
  ,P2.RISKLEVEL AS RISKLEVEL -- 风险等级
  ,None AS None -- None
  ,P2.INCOMEVALUEMIN AS INCOMEVALUEMIN -- 业绩比较基准下限
  ,P2.INCOMEVALUEMAX AS INCOMEVALUEMAX -- 业绩比较基准上限
  ,P1.STATUS AS STATUS -- 申请状态
  ,P1.ACCEPTMETHOD AS ACCEPTMETHOD -- 受理方式
  ,P1.DEFDIVIDENDMETHOD AS DEFDIVIDENDMETHOD -- 默认分红方式
  ,P1.SHARETYPE AS SHARETYPE -- 收费方式
  ,P1.FROZENCAUSE AS FROZENCAUSE -- 冻结原因
  ,P1.FREEZINGDEADLINE AS FREEZINGDEADLINE -- 冻结截止日期格式为：YYYYMMDD
  ,P1.LARGEREDEMPTIONFLAG AS LARGEREDEMPTIONFLAG -- 巨额赎回处理标志
  ,P1.EXCEPTIONALFLAG AS EXCEPTIONALFLAG -- 异常交易标志
  ,P1.RISKMATCHING AS RISKMATCHING -- 风险等级是否匹配
  ,P1.CANCELFLAG AS CANCELFLAG -- 撤单标志
  ,P1.TARGETTAACCOUNTID AS TARGETTAACCOUNTID -- 对方基金账号
  ,P1.TARGETTRANSACTIONACCOUNTID AS TARGETTRANSACTIONACCOUNTID -- 对方交易帐号在销售商是一次完成时，为必须项
  ,P1.TARGETBRANCHCODE AS TARGETBRANCHCODE -- 对方网点号转销售商、非交易过户时使用
  ,P1.TARGETREGIONCODE AS TARGETREGIONCODE -- 对方地区编号具体编码依GB13497-92
  ,P1.TARGETDISTRIBUTORCODE AS TARGETDISTRIBUTORCODE -- 对方销售商代码
  ,P1.TARGETFUNDCODE AS TARGETFUNDCODE -- 对方目标基金代码
  ,P1.TARGETSHARETYPE AS TARGETSHARETYPE -- 对方目标收费方式
  ,P4.BANKNODE AS BANKNODE -- 银行网点号码
  ,P1.CUSTMANAGERID AS CUSTMANAGERID -- 客户经理代码
  ,P1.OPERID AS OPERID -- 操作柜员
  ,P1.CONFIRMOPID1 AS CONFIRMOPID1 -- 复核柜员1
  ,P1.CONFIRMOPID2 AS CONFIRMOPID2 -- 复核柜员2
  ,P1.RETURNMSG AS RETURNMSG -- 交易处理返回错误信息
  ,P1.RETURNCODE AS RETURNCODE -- 交易处理后返回码
  ,'' AS None -- None
  ,P1.SPECIFICATION AS SPECIFICATION -- 摘要说明
 FROM ${ods_tss_schema}.ods_tss_trust_h_app_trans  P1 -- 交易申请历史表 
LEFT JOIN ${ods_tss_schema}.ods_tss_trust_cfg_fund  P2 -- 基金信息 
 ON P1.UNIONCODE = P2.UNIONCODE 
AND P1.TANO=P2.TANO 
AND P1.FUNDCODE = P2.FUNDCODE
AND P2.PT_DT='${batch_date}' 
AND P2.DELETED='0'
LEFT JOIN ${ods_tss_schema}.ods_tss_trust_acct_cust  P3 -- 客户信息表 
 ON p3.UNIONCODE = p1.UNIONCODE   
and p3.CUSTNO = p1.CUSTNO
and P3.PT_DT='${batch_date}' 
AND P3.DELETED='0'
LEFT JOIN ${ods_tss_schema}.ods_tss_trust_acct_custmanager  P4 -- 客户经理信息 
 ON AND P4.UNIONCODE = P1.UNIONCODE  
AND P4.CUSTMANAGERID = P1.CUSTMANAGERID
AND P4.PT_DT='${batch_date}' 
AND P4.DELETED='0'
LEFT JOIN 代码表待命名  S1 -- 代码转换 
 ON P3.INVTP = S1.SRC_CD_VAL 
AND S1.SRC_SYS_CD = 'TSS'
AND S1.SRC_COL_NAME ='INVTP'
AND S1.SRC_TAB_NAME = 'ODS_TSS_TRUST_ACCT_CUST'
LEFT JOIN 代码表待命名  S2 -- 代码转换 
 ON P3.CERTIFICATETYPE = S2.SRC_CD_VAL 
AND S2.SRC_SYS_CD = 'TSS'
AND S2.SRC_COL_NAME ='CERTIFICATETYPE'
AND S2.SRC_TAB_NAME = 'ODS_TSS_TRUST_ACCT_CUST'
LEFT JOIN 代码表待命名  S4 -- 代码转换 
 ON P1.CURRENCYTYPE = S4.SRC_CD_VAL 
AND S4.SRC_SYS_CD = 'TSS'
AND S4.SRC_COL_NAME ='CURRENCYTYPE'
AND S4.SRC_TAB_NAME = 'ODS_TSS_TRUST_H_ACK_TRANS'
 WHERE P1.PT_DT='${batch_date}' 
AND P1.DELETED='0'
 
;


/*添加表分析*/ 
ANALYZE TABLE ${icl_schema}.TMP_S04_TRUST_TXN_DTL_03;


/*===================第4组====================*/

INSERT INTO ${icl_schema}.S04_TRUST_TXN_DTL(
  APP_FORM_SER_NO -- 申请单流水号
  ,业务术语待申请 -- 信托业务确认代码
  ,TA_SER_NO -- TA流水号
  ,LP_ORG_NO -- 法人机构编号
  ,业务术语待申请 -- 交易发生机构编号
  ,业务术语待申请 -- 信托业务类别代码
  ,APP_SHARE -- 申请份额
  ,APP_AMT -- 申请金额
  ,CFM_SHARE -- 确认份额
  ,CFM_AMT -- 确认金额
  ,业务术语待申请 -- 交易发生日期
  ,业务术语待申请 -- 交易发生时间
  ,业务术语待申请 -- 下单发生日期
  ,业务术语待申请 -- 下单发生时间
  ,CFM_DT -- 确认日期
  ,STL_CURR_CD -- 结算币种代码
  ,ORIG_APP_FORM_NO -- 原申请单编号
  ,业务术语待申请 -- 银行账号
  ,TXN_HAPP_SELLER_NO -- 交易发生销售商编号
  ,业务术语待申请 -- TA编号
  ,TA_ACCT_NO -- TA账户编号
  ,业务术语待申请 -- 交易账号
  ,CUST_IN_CD -- 客户内码
  ,业务术语待申请 -- 客户类型代码
  ,MAIN_DOCTYP_CD -- 主证件类型代码
  ,MAIN_DOC_NO -- 主证件号码
  ,OPERR_DOCTYP_CD -- 经办人证件类型代码
  ,OPERR_DOC_NO -- 经办人证件号码
  ,OPERR_NAME -- 经办人名称
  ,SIGN_BNKOUTLS_NO -- 签约网点编号
  ,OPEN_CARD_BNKOUTLS_NO -- 开卡网点编号
  ,业务术语待申请 -- 信托产品代码
  ,业务术语待申请 -- 信托产品名称
  ,业务术语待申请 -- 信托产品类别代码
  ,业务术语待申请 -- 信托产品风险等级代码
  ,业务术语待申请 -- 信托产品净值
  ,业务术语待申请 -- 收益率业绩比较基准上限
  ,业务术语待申请 -- 收益率业绩比较基准下限
  ,业务术语待申请 -- 信托交易申请状态代码
  ,ACPTD_MODE_CD -- 受理方式代码
  ,DIVDND_MODE_CD -- 分红方式代码
  ,业务术语待申请 -- 信托收费方式代码
  ,FRZ_RSN_CD -- 冻结原因代码
  ,业务术语待申请 -- 冻结截止日期
  ,业务术语待申请 -- 巨额赎回处理代码
  ,业务术语待申请 -- 异常交易代码
  ,业务术语待申请 -- 风险等级匹配标志
  ,业务术语待申请 -- 交易撤单标识代码
  ,业务术语待申请 -- 对方TA账号
  ,业务术语待申请 -- 对方交易账号
  ,业务术语待申请 -- 对方网点编号
  ,业务术语待申请 -- 对方地区编号
  ,业务术语待申请 -- 对方销售商编号
  ,业务术语待申请 -- 对方目标产品代码
  ,业务术语待申请 -- 对方目标收费方式代码
  ,业务术语待申请 -- 营销人员所属网点编号
  ,EACCT_MKTING_CUST_MGR_NO -- 电子账户营销客户经理编号
  ,OPER_TELR_NO -- 操作柜员编号
  ,TXN_AUTH_TELR_1_NO -- 交易授权柜员1编号
  ,TXN_AUTH_TELR_2_NO -- 交易授权柜员2编号
  ,业务术语待申请 -- 交易处理返回错误信息
  ,业务术语待申请 -- 交易处理后返回码
  ,业务术语待申请 -- 业务过程完全结束代码
  ,业务术语待申请 -- 摘要说明
)
SELECT 
  APP_FORM_SER_NO AS APP_FORM_SER_NO -- 申请单流水号
  ,业务术语待申请 AS 业务术语待申请 -- 信托业务确认代码
  ,TA_SER_NO AS TA_SER_NO -- TA流水号
  ,LP_ORG_NO AS LP_ORG_NO -- 法人机构编号
  ,业务术语待申请 AS 业务术语待申请 -- 交易发生机构编号
  ,业务术语待申请 AS 业务术语待申请 -- 信托业务类别代码
  ,APP_SHARE AS APP_SHARE -- 申请份额
  ,APP_AMT AS APP_AMT -- 申请金额
  ,CFM_SHARE AS CFM_SHARE -- 确认份额
  ,CFM_AMT AS CFM_AMT -- 确认金额
  ,业务术语待申请 AS 业务术语待申请 -- 交易发生日期
  ,业务术语待申请 AS 业务术语待申请 -- 交易发生时间
  ,业务术语待申请 AS 业务术语待申请 -- 下单发生日期
  ,业务术语待申请 AS 业务术语待申请 -- 下单发生时间
  ,CFM_DT AS CFM_DT -- 确认日期
  ,STL_CURR_CD AS STL_CURR_CD -- 结算币种代码
  ,ORIG_APP_FORM_NO AS ORIG_APP_FORM_NO -- 原申请单编号
  ,业务术语待申请 AS 业务术语待申请 -- 银行账号
  ,TXN_HAPP_SELLER_NO AS TXN_HAPP_SELLER_NO -- 交易发生销售商编号
  ,业务术语待申请 AS 业务术语待申请 -- TA编号
  ,TA_ACCT_NO AS TA_ACCT_NO -- TA账户编号
  ,业务术语待申请 AS 业务术语待申请 -- 交易账号
  ,CUST_IN_CD AS CUST_IN_CD -- 客户内码
  ,业务术语待申请 AS 业务术语待申请 -- 客户类型代码
  ,MAIN_DOCTYP_CD AS MAIN_DOCTYP_CD -- 主证件类型代码
  ,MAIN_DOC_NO AS MAIN_DOC_NO -- 主证件号码
  ,OPERR_DOCTYP_CD AS OPERR_DOCTYP_CD -- 经办人证件类型代码
  ,OPERR_DOC_NO AS OPERR_DOC_NO -- 经办人证件号码
  ,OPERR_NAME AS OPERR_NAME -- 经办人名称
  ,SIGN_BNKOUTLS_NO AS SIGN_BNKOUTLS_NO -- 签约网点编号
  ,OPEN_CARD_BNKOUTLS_NO AS OPEN_CARD_BNKOUTLS_NO -- 开卡网点编号
  ,业务术语待申请 AS 业务术语待申请 -- 信托产品代码
  ,业务术语待申请 AS 业务术语待申请 -- 信托产品名称
  ,业务术语待申请 AS 业务术语待申请 -- 信托产品类别代码
  ,业务术语待申请 AS 业务术语待申请 -- 信托产品风险等级代码
  ,业务术语待申请 AS 业务术语待申请 -- 信托产品净值
  ,业务术语待申请 AS 业务术语待申请 -- 收益率业绩比较基准上限
  ,业务术语待申请 AS 业务术语待申请 -- 收益率业绩比较基准下限
  ,业务术语待申请 AS 业务术语待申请 -- 信托交易申请状态代码
  ,ACPTD_MODE_CD AS ACPTD_MODE_CD -- 受理方式代码
  ,DIVDND_MODE_CD AS DIVDND_MODE_CD -- 分红方式代码
  ,业务术语待申请 AS 业务术语待申请 -- 信托收费方式代码
  ,FRZ_RSN_CD AS FRZ_RSN_CD -- 冻结原因代码
  ,业务术语待申请 AS 业务术语待申请 -- 冻结截止日期
  ,业务术语待申请 AS 业务术语待申请 -- 巨额赎回处理代码
  ,业务术语待申请 AS 业务术语待申请 -- 异常交易代码
  ,业务术语待申请 AS 业务术语待申请 -- 风险等级匹配标志
  ,业务术语待申请 AS 业务术语待申请 -- 交易撤单标识代码
  ,业务术语待申请 AS 业务术语待申请 -- 对方TA账号
  ,业务术语待申请 AS 业务术语待申请 -- 对方交易账号
  ,业务术语待申请 AS 业务术语待申请 -- 对方网点编号
  ,业务术语待申请 AS 业务术语待申请 -- 对方地区编号
  ,业务术语待申请 AS 业务术语待申请 -- 对方销售商编号
  ,业务术语待申请 AS 业务术语待申请 -- 对方目标产品代码
  ,业务术语待申请 AS 业务术语待申请 -- 对方目标收费方式代码
  ,业务术语待申请 AS 业务术语待申请 -- 营销人员所属网点编号
  ,EACCT_MKTING_CUST_MGR_NO AS EACCT_MKTING_CUST_MGR_NO -- 电子账户营销客户经理编号
  ,OPER_TELR_NO AS OPER_TELR_NO -- 操作柜员编号
  ,TXN_AUTH_TELR_1_NO AS TXN_AUTH_TELR_1_NO -- 交易授权柜员1编号
  ,TXN_AUTH_TELR_2_NO AS TXN_AUTH_TELR_2_NO -- 交易授权柜员2编号
  ,业务术语待申请 AS 业务术语待申请 -- 交易处理返回错误信息
  ,业务术语待申请 AS 业务术语待申请 -- 交易处理后返回码
  ,业务术语待申请 AS 业务术语待申请 -- 业务过程完全结束代码
  ,业务术语待申请 AS 业务术语待申请 -- 摘要说明 
 FROM (
SELECT P1.* FROM 
TMP_S04_TRUST_TXN_DTL_01 P1 
UNION ALL 
SELECT P2.* FROM 
TMP_S04_TRUST_TXN_DTL_02 P2
SELECT P3.* FROM 
TMP_S04_TRUST_TXN_DTL_03 P3 )  T1 -- 临时表聚合信息 
   
 
;



/*添加目标表分析*/ 
\echo "4.analyze table" 
ANALYZE TABLE ${icl_schema}.S04_TRUST_TXN_DTL;
