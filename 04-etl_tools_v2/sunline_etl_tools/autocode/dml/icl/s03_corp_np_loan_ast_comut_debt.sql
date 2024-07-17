/*
*********************************************************************** 
Purpose:       主题聚合层-加工快照表脚本
Author:        Sunline
Usage:         python $ETL_HOME/script/main.py yyyymmdd ${icl_schema}_S03_CORP_NP_LOAN_AST_COMUT_DEBT
CreateDate:    2023-12-21 00:00:00
FileType:      DML
logs:
       表英文名：S03_CORP_NP_LOAN_AST_COMUT_DEBT
       表中文名：不良贷款资产抵债信息聚合表
       创建日期：2023-12-21 00:00:00
       主键字段：基金定投协议编号
       归属层次：聚合层
       归属主题：信贷
       主要应用：不良贷款、资产抵债
       分析人员：陈丙寰
       时间粒度：日
       保留周期：13M
       描述信息：主要包含抵债资产基本及明细信息、资产抵债时明细以及资产处置时明细。
*************************************************************************/ 

\timing 
/*创建当日分区*/
   call ${itl_schema}.partition_add('${icl_schema}.S03_CORP_NP_LOAN_AST_COMUT_DEBT','pt_${batch_date}','${batch_date}'); 

/*删除当前批次历史数据*/
   call ${itl_schema}.partition_drop('${icl_schema}.S03_CORP_NP_LOAN_AST_COMUT_DEBT','pt_${batch_date}'); 



/*===================第1组====================*/

INSERT INTO ${icl_schema}.S03_CORP_NP_LOAN_AST_COMUT_DEBT(
  COMUT_DEBT_AST_NO -- 抵债资产编号
  ,COMUT_DEBT_AST_DISP_BATCH_NO -- 抵债资产处置批次号
  ,COMUT_DEBT_AST_COMUT_DEBT_SER_NO -- 抵债资产抵债序号
  ,BELONG_ORG_NO -- 归属机构编号
  ,COMUT_DEBT_AST_ESTIM_BOOK_NO -- 抵债资产评估书编号
  ,COMUT_DEBT_AST_ESTIM_VAL -- 抵债资产评估价值
  ,FAIR_VAL_CAN_CAN_GET_FLAG -- 公允价值能否可靠取得标志
  ,APPRV_BOOK_NO -- 审批书编号
  ,COMUT_DEBT_NOTICE_NO -- 抵债通知书编号
  ,COMUT_DEBT_AST_NAME -- 抵债资产名称
  ,ORIG_PLEDGE_SINGLE_NO -- 原抵质押单编号
  ,CURR_CD -- 币种代码
  ,ORIG_PLEDGE_ESTIM_VAL -- 原抵质押物评估价值
  ,COMUT_DEBT_AST_IN_VAL -- 抵债资产入帐价值
  ,COMUT_DEBT_AST_GET_DT -- 抵债资产取得日期
  ,COMUT_DEBT_AST_GET_CNT -- 抵债资产取得数量
  ,MEASURE_CORP_CD -- 计量单位代码
  ,COMUT_DEBT_AST_GET_MODE_CD -- 抵债资产取得方式代码
  ,COMUT_DEBT_BAL -- 抵债余额
  ,CASH_SURPLUS_CNT -- 结余数量
  ,CRSPD_LOAN_PRIN_TOTAL_AMT -- 对应贷款本金总额
  ,WARRANT_CMPLT_FLAG -- 权证齐全标志
  ,WARRANT_TRAN_FLAG -- 权证过户标志
  ,COMUT_DEBT_AST_STORE_LOC -- 抵债资产存放地点
  ,COMUT_DEBT_AST_STATUS_CD -- 抵债资产状态代码
  ,COMUT_DEBT_AST_TYPE_CD -- 抵债资产类别代码
  ,EQTY_INVEST_TYPE -- 股权投资类型
  ,ORIG_OWNER_CUST_NO -- 原所有者客户号
  ,ORIG_OWNER_NAME -- 原所有者名称
  ,BE_ARRIVE_PTY_CUST_IN_CD -- 被抵方客户内码
  ,BE_ARRIVE_PTY_CUST_NO -- 被抵方客户号
  ,WAIT_DEAL_COMUT_DEBT_PRIN_AMT -- 待处理抵债本金金额
  ,AL_COMUT_DEBT_AST_INT -- 已抵债资产利息
  ,WAIT_DEAL_COMUT_DEBT_ON_BS_INT -- 待处理抵债表内息
  ,COMUT_DEBT_WAIT_REALIZE_INT -- 抵债待变现利息
  ,AL_DISP_COMUT_DEBT_AST -- 已处置抵债资产
  ,AL_DISP_AST_PRIN_AMT -- 已处置资产本金金额
  ,AL_DISP_AST_INT -- 已处置资产利息
  ,AL_DISP_AST_INCOME -- 已处置资产收入
  ,COMUT_DEBT_AST_DEPRE_RESER_BAL -- 抵债资产减值准备余额
  ,COMUT_DEBT_AST_DISP_TYPE_CD -- 抵债资产处置类型代码
  ,COMUT_DEBT_AST_DISP_CNT -- 抵债资产处置数量
  ,COMUT_DEBT_AST_DISP_RATE -- 抵债资产处置率
  ,TH_TM_COMUT_DEBT_AST_DISP_AMT -- 本次抵债资产处置金额
  ,COMUT_DEBT_AST_DISP_PRIN_AMT -- 抵债资产处置本金金额
  ,COMUT_DEBT_AST_DISP_INT -- 抵债资产处置利息
  ,TH_TM_DISP_BEF_FAIR_VAL -- 本次处置前公允价值
  ,COMUT_DEBT_AST_TH_TM_DISP_INCOME -- 抵债资产本次处置收入
  ,DISP_TAX_AMT -- 处置税费金额
  ,AST_DISP_NON_BIZ_TXN_AMT -- 资产处置营业外交易金额
  ,DEPRE_RESER_AMORT_AMT -- 减值准备摊销额
  ,COMUT_DEBT_TXN_AMT -- 抵债交易金额
  ,LOAN_ON_BS_INT -- 贷款表内利息
  ,REAL_MTG_ON_BS_INT -- 实抵表内利息
  ,LOAN_ON_BS_CINT -- 贷款表内复利
  ,REAL_MTG_ON_BS_CINT -- 实抵表内复利
  ,LOAN_OFBS_INT -- 贷款表外利息
  ,REAL_MTG_OFBS_INT -- 实抵表外利息
  ,LOAN_OFBS_CINT -- 贷款表外复利
  ,REAL_MTG_OFBS_CINT -- 实抵表外复利
  ,LOAN_CURR_PERIOD_INT -- 贷款本期利息
  ,REAL_MTG_CURR_PERIOD_INT -- 实抵本期利息
  ,REAL_MTG_CLAIM_PRIN_AMT -- 实抵债权本金金额
  ,COMUT_DEBT_TAX_AMT -- 抵债税费金额
  ,COMUT_DEBT_ON_ACCT_AMT -- 抵债挂账金额
  ,IN_NON_BIZ_INCMAMT -- 入营业外收入金额
  ,DATA_DT -- 数据日期
)
SELECT 
  P1.ASAAHY12 AS ASAAHY12 -- 抵债资产编号
  ,P2.DPAAEXNS AS DPAAEXNS -- 处置批次
  ,P3.PYAHSN03 AS PYAHSN03 -- 抵债序号
  ,P1.ASAABRNO AS ASAABRNO -- 机构号
  ,P1.ASAANO30 AS ASAANO30 -- 评估书编号
  ,P1.ASAAVALU AS ASAAVALU -- 评估价值
  ,P1.ASAXBOOL AS ASAXBOOL -- 公允价值能否可靠取得
  ,P1.ASAANO16 AS ASAANO16 -- 审批书编号
  ,P1.ASADNO18 AS ASADNO18 -- 抵债通知书编号
  ,P1.ASAKFLNM AS ASAKFLNM -- 抵债资产名称
  ,P1.ASABHY17 AS ASABHY17 -- 原抵质押单编号
  ,P1.ASAACCYC AS ASAACCYC -- 币种
  ,P1.ASBNAMT AS ASBNAMT -- 原抵质押物评估价值
  ,P1.ASABVALU AS ASABVALU -- 抵债资产入帐价值
  ,P1.ASBLDATE AS ASBLDATE -- 取得日期
  ,P1.ASAAQY12 AS ASAAQY12 -- 取得数量
  ,P1.ASAAUN3B AS ASAAUN3B -- 计量单位
  ,P1.ASAAGTFR AS ASAAGTFR -- 取得方式
  ,P1.ASAABAL AS ASAABAL -- 余额
  ,P1.ASADQY12 AS ASADQY12 -- 结余数量
  ,P1.ASCEAMT AS ASCEAMT -- 对应贷款本金总额
  ,P1.ASAUFLAG AS ASAUFLAG -- 权证是否齐全
  ,P1.ASAVFLAG AS ASAVFLAG -- 权证是否过户
  ,P1.ASALFLNM AS ASALFLNM -- 存放地点
  ,P1.ASAAHYST AS ASAAHYST -- 抵债资产状态
  ,P1.ASAAHYTP AS ASAAHYTP -- 抵债资产类别
  ,P1.ASEAFLAG AS ASEAFLAG -- 股权投资类型
  ,P1.ASAJCSID AS ASAJCSID -- 原所有者客户号
  ,P1.ASBRFLNM AS ASBRFLNM -- 原所有者名称
  ,P1.ASADCSNO AS ASADCSNO -- 被抵方客户内码
  ,P1.ASAGCSID AS ASAGCSID -- 被抵方客户号
  ,P1.ASABPAMT AS ASABPAMT -- 待处理抵债本金
  ,P1.ASAGPAMT AS ASAGPAMT -- 已抵债资产利息
  ,P1.ASCGIAM2 AS ASCGIAM2 -- 待处理抵债表内息
  ,P1.ASCHIAM2 AS ASCHIAM2 -- 抵债待变现利息
  ,P1.ASALPAMT AS ASALPAMT -- 已处置抵债资产
  ,P1.ASAMPAMT AS ASAMPAMT -- 已处置资产本金
  ,P1.ASANPAMT AS ASANPAMT -- 已处置资产利息
  ,P1.ASBMAMT AS ASBMAMT -- 已处置资产收入
  ,P1.ASAMBLNC AS ASAMBLNC -- 减值准备余额
  ,P2.DPECFLAG AS DPECFLAG -- 抵债资产处置类型
  ,P2.DPABQY12 AS DPABQY12 -- 抵债合同处置数量
  ,P2.DPAKRTIO AS DPAKRTIO -- 处置率
  ,P2.DPAOAMT AS DPAOAMT -- 本次处置金额
  ,P2.DPAHPAMT AS DPAHPAMT -- 处置本金
  ,P2.DPAIPAMT AS DPAIPAMT -- 处置利息
  ,P2.DPAOPAMT AS DPAOPAMT -- 本次处置前公允价值
  ,P2.DPAPAMT AS DPAPAMT -- 抵债资产本次处置收入
  ,P2.DPADPAMT AS DPADPAMT -- 税费金额
  ,P2.DPAJPAMT AS DPAJPAMT -- 营业外交易金额
  ,P2.DPAKPAMT AS DPAKPAMT -- 减值准备摊销额
  ,P3.PYARAMT AS PYARAMT -- 交易金额
  ,P3.PYARIAM2 AS PYARIAM2 -- 表内利息
  ,P3.PYAWIAM2 AS PYAWIAM2 -- 实抵表内利息
  ,P3.PYASIAM2 AS PYASIAM2 -- 表内复利
  ,P3.PYAXIAM2 AS PYAXIAM2 -- 实抵表内复利
  ,P3.PYATIAM2 AS PYATIAM2 -- 表外利息
  ,P3.PYAYIAM2 AS PYAYIAM2 -- 实抵表外利息
  ,P3.PYAUIAM2 AS PYAUIAM2 -- 表外复利
  ,P3.PYAZIAM2 AS PYAZIAM2 -- 实抵表外复利
  ,P3.PYAQIAM2 AS PYAQIAM2 -- 本期利息
  ,P3.PYAVIAM2 AS PYAVIAM2 -- 实抵本期利息
  ,P3.PYACPRN AS PYACPRN -- 实抵债权本金
  ,P3.PYADPAMT AS PYADPAMT -- 税费金额
  ,P3.PYAEPAMT AS PYAEPAMT -- 挂账金额
  ,P3.PYAFPAMT AS PYAFPAMT -- 入营业外收入金额
  ,DATE'${batch_date}' AS DATA_DT -- None 
 FROM ${ods_core_schema}.ODS_CORE_BLFMPDAS  P1 -- 抵债资产主文件 
LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMPDPY  P2 -- 资产抵债明细文件 
 ON P2.PYAAHY12=P1.ASAAHY12
AND P2.PT_DT='${batch_date}' 
AND P2.DELETED='0'
LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMPDDP  P3 -- 抵债资产处置明细文件 
 ON P3.DPAAHY12=P1.ASAAHY12 
AND P3.PT_DT='${batch_date}' 
AND P3.DELETED='0'
 WHERE P1.PT_DT='${batch_date}' 
AND P1.DELETED='0' 
 
;



/*添加目标表分析*/ 
\echo "4.analyze table" 
ANALYZE TABLE ${icl_schema}.S03_CORP_NP_LOAN_AST_COMUT_DEBT;
