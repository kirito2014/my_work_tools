-- 层次表名: 聚合层-不良贷款资产抵债信息聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s03_corp_np_loan_ast_comut_debt
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S03_CORP_NP_LOAN_AST_COMUT_DEBT
--     表中文名：不良贷款资产抵债信息聚合表
--     创建日期：2023-12-21 00:00:00
--     主键字段：基金定投协议编号
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：主要包含抵债资产基本及明细信息、资产抵债时明细以及资产处置时明细。
--     更新记录：
--         2023-12-19 00:00:00 王穆军 新增映射文件信息
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S03_CORP_NP_LOAN_AST_COMUT_DEBT drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 不良贷款资产抵债信息聚合表

insert into table ${session}.S03_CORP_NP_LOAN_AST_COMUT_DEBT(
      COMUT_DEBT_AST_NO -- 抵债资产编号
    , COMUT_DEBT_AST_DISP_BATCH_NO -- 抵债资产处置批次号
    , COMUT_DEBT_AST_COMUT_DEBT_SER_NO -- 抵债资产抵债序号
    , BELONG_ORG_NO -- 归属机构编号
    , COMUT_DEBT_AST_ESTIM_BOOK_NO -- 抵债资产评估书编号
    , COMUT_DEBT_AST_ESTIM_VAL -- 抵债资产评估价值
    , FAIR_VAL_CAN_CAN_GET_FLAG -- 公允价值能否可靠取得标志
    , APPRV_BOOK_NO -- 审批书编号
    , COMUT_DEBT_NOTICE_NO -- 抵债通知书编号
    , COMUT_DEBT_AST_NAME -- 抵债资产名称
    , ORIG_PLEDGE_SINGLE_NO -- 原抵质押单编号
    , CURR_CD -- 币种代码
    , ORIG_PLEDGE_ESTIM_VAL -- 原抵质押物评估价值
    , COMUT_DEBT_AST_IN_VAL -- 抵债资产入帐价值
    , COMUT_DEBT_AST_GET_DT -- 抵债资产取得日期
    , COMUT_DEBT_AST_GET_CNT -- 抵债资产取得数量
    , MEASURE_CORP_CD -- 计量单位代码
    , COMUT_DEBT_AST_GET_MODE_CD -- 抵债资产取得方式代码
    , COMUT_DEBT_BAL -- 抵债余额
    , CASH_SURPLUS_CNT -- 结余数量
    , CRSPD_LOAN_PRIN_TOTAL_AMT -- 对应贷款本金总额
    , WARRANT_CMPLT_FLAG -- 权证齐全标志
    , WARRANT_TRAN_FLAG -- 权证过户标志
    , COMUT_DEBT_AST_STORE_LOC -- 抵债资产存放地点
    , COMUT_DEBT_AST_STATUS_CD -- 抵债资产状态代码
    , COMUT_DEBT_AST_TYPE_CD -- 抵债资产类别代码
    , EQTY_INVEST_TYPE -- 股权投资类型
    , ORIG_OWNER_CUST_NO -- 原所有者客户号
    , ORIG_OWNER_NAME -- 原所有者名称
    , BE_ARRIVE_PTY_CUST_IN_CD -- 被抵方客户内码
    , BE_ARRIVE_PTY_CUST_NO -- 被抵方客户号
    , WAIT_DEAL_COMUT_DEBT_PRIN_AMT -- 待处理抵债本金金额
    , AL_COMUT_DEBT_AST_INT -- 已抵债资产利息
    , WAIT_DEAL_COMUT_DEBT_ON_BS_INT -- 待处理抵债表内息
    , COMUT_DEBT_WAIT_REALIZE_INT -- 抵债待变现利息
    , AL_DISP_COMUT_DEBT_AST -- 已处置抵债资产
    , AL_DISP_AST_PRIN_AMT -- 已处置资产本金金额
    , AL_DISP_AST_INT -- 已处置资产利息
    , AL_DISP_AST_INCOME -- 已处置资产收入
    , COMUT_DEBT_AST_DEPRE_RESER_BAL -- 抵债资产减值准备余额
    , COMUT_DEBT_AST_DISP_TYPE_CD -- 抵债资产处置类型代码
    , COMUT_DEBT_AST_DISP_CNT -- 抵债资产处置数量
    , COMUT_DEBT_AST_DISP_RATE -- 抵债资产处置率
    , TH_TM_COMUT_DEBT_AST_DISP_AMT -- 本次抵债资产处置金额
    , COMUT_DEBT_AST_DISP_PRIN_AMT -- 抵债资产处置本金金额
    , COMUT_DEBT_AST_DISP_INT -- 抵债资产处置利息
    , TH_TM_DISP_BEF_FAIR_VAL -- 本次处置前公允价值
    , COMUT_DEBT_AST_TH_TM_DISP_INCOME -- 抵债资产本次处置收入
    , DISP_TAX_AMT -- 处置税费金额
    , AST_DISP_NON_BIZ_TXN_AMT -- 资产处置营业外交易金额
    , DEPRE_RESER_AMORT_AMT -- 减值准备摊销额
    , COMUT_DEBT_TXN_AMT -- 抵债交易金额
    , LOAN_ON_BS_INT -- 贷款表内利息
    , REAL_MTG_ON_BS_INT -- 实抵表内利息
    , LOAN_ON_BS_CINT -- 贷款表内复利
    , REAL_MTG_ON_BS_CINT -- 实抵表内复利
    , LOAN_OFBS_INT -- 贷款表外利息
    , REAL_MTG_OFBS_INT -- 实抵表外利息
    , LOAN_OFBS_CINT -- 贷款表外复利
    , REAL_MTG_OFBS_CINT -- 实抵表外复利
    , LOAN_CURR_PERIOD_INT -- 贷款本期利息
    , REAL_MTG_CURR_PERIOD_INT -- 实抵本期利息
    , REAL_MTG_CLAIM_PRIN_AMT -- 实抵债权本金金额
    , COMUT_DEBT_TAX_AMT -- 抵债税费金额
    , COMUT_DEBT_ON_ACCT_AMT -- 抵债挂账金额
    , IN_NON_BIZ_INCMAMT -- 入营业外收入金额
    , DATA_DT -- 数据日期
)
select
      P1.ASAAHY12 as COMUT_DEBT_AST_NO -- 抵债资产编号
    , P2.DPAAEXNS as COMUT_DEBT_AST_DISP_BATCH_NO -- 处置批次
    , P3.PYAHSN03 as COMUT_DEBT_AST_COMUT_DEBT_SER_NO -- 抵债序号
    , P1.ASAABRNO as BELONG_ORG_NO -- 机构号
    , P1.ASAANO30 as COMUT_DEBT_AST_ESTIM_BOOK_NO -- 评估书编号
    , P1.ASAAVALU as COMUT_DEBT_AST_ESTIM_VAL -- 评估价值
    , P1.ASAXBOOL as FAIR_VAL_CAN_CAN_GET_FLAG -- 公允价值能否可靠取得
    , P1.ASAANO16 as APPRV_BOOK_NO -- 审批书编号
    , P1.ASADNO18 as COMUT_DEBT_NOTICE_NO -- 抵债通知书编号
    , P1.ASAKFLNM as COMUT_DEBT_AST_NAME -- 抵债资产名称
    , P1.ASABHY17 as ORIG_PLEDGE_SINGLE_NO -- 原抵质押单编号
    , P1.ASAACCYC as CURR_CD -- 币种
    , P1.ASBNAMT as ORIG_PLEDGE_ESTIM_VAL -- 原抵质押物评估价值
    , P1.ASABVALU as COMUT_DEBT_AST_IN_VAL -- 抵债资产入帐价值
    , P1.ASBLDATE as COMUT_DEBT_AST_GET_DT -- 取得日期
    , P1.ASAAQY12 as COMUT_DEBT_AST_GET_CNT -- 取得数量
    , P1.ASAAUN3B as MEASURE_CORP_CD -- 计量单位
    , P1.ASAAGTFR as COMUT_DEBT_AST_GET_MODE_CD -- 取得方式
    , P1.ASAABAL as COMUT_DEBT_BAL -- 余额
    , P1.ASADQY12 as CASH_SURPLUS_CNT -- 结余数量
    , P1.ASCEAMT as CRSPD_LOAN_PRIN_TOTAL_AMT -- 对应贷款本金总额
    , P1.ASAUFLAG as WARRANT_CMPLT_FLAG -- 权证是否齐全
    , P1.ASAVFLAG as WARRANT_TRAN_FLAG -- 权证是否过户
    , P1.ASALFLNM as COMUT_DEBT_AST_STORE_LOC -- 存放地点
    , P1.ASAAHYST as COMUT_DEBT_AST_STATUS_CD -- 抵债资产状态
    , P1.ASAAHYTP as COMUT_DEBT_AST_TYPE_CD -- 抵债资产类别
    , P1.ASEAFLAG as EQTY_INVEST_TYPE -- 股权投资类型
    , P1.ASAJCSID as ORIG_OWNER_CUST_NO -- 原所有者客户号
    , P1.ASBRFLNM as ORIG_OWNER_NAME -- 原所有者名称
    , P1.ASADCSNO as BE_ARRIVE_PTY_CUST_IN_CD -- 被抵方客户内码
    , P1.ASAGCSID as BE_ARRIVE_PTY_CUST_NO -- 被抵方客户号
    , P1.ASABPAMT as WAIT_DEAL_COMUT_DEBT_PRIN_AMT -- 待处理抵债本金
    , P1.ASAGPAMT as AL_COMUT_DEBT_AST_INT -- 已抵债资产利息
    , P1.ASCGIAM2 as WAIT_DEAL_COMUT_DEBT_ON_BS_INT -- 待处理抵债表内息
    , P1.ASCHIAM2 as COMUT_DEBT_WAIT_REALIZE_INT -- 抵债待变现利息
    , P1.ASALPAMT as AL_DISP_COMUT_DEBT_AST -- 已处置抵债资产
    , P1.ASAMPAMT as AL_DISP_AST_PRIN_AMT -- 已处置资产本金
    , P1.ASANPAMT as AL_DISP_AST_INT -- 已处置资产利息
    , P1.ASBMAMT as AL_DISP_AST_INCOME -- 已处置资产收入
    , P1.ASAMBLNC as COMUT_DEBT_AST_DEPRE_RESER_BAL -- 减值准备余额
    , P2.DPECFLAG as COMUT_DEBT_AST_DISP_TYPE_CD -- 抵债资产处置类型
    , P2.DPABQY12 as COMUT_DEBT_AST_DISP_CNT -- 抵债合同处置数量
    , P2.DPAKRTIO as COMUT_DEBT_AST_DISP_RATE -- 处置率
    , P2.DPAOAMT as TH_TM_COMUT_DEBT_AST_DISP_AMT -- 本次处置金额
    , P2.DPAHPAMT as COMUT_DEBT_AST_DISP_PRIN_AMT -- 处置本金
    , P2.DPAIPAMT as COMUT_DEBT_AST_DISP_INT -- 处置利息
    , P2.DPAOPAMT as TH_TM_DISP_BEF_FAIR_VAL -- 本次处置前公允价值
    , P2.DPAPAMT as COMUT_DEBT_AST_TH_TM_DISP_INCOME -- 抵债资产本次处置收入
    , P2.DPADPAMT as DISP_TAX_AMT -- 税费金额
    , P2.DPAJPAMT as AST_DISP_NON_BIZ_TXN_AMT -- 营业外交易金额
    , P2.DPAKPAMT as DEPRE_RESER_AMORT_AMT -- 减值准备摊销额
    , P3.PYARAMT as COMUT_DEBT_TXN_AMT -- 交易金额
    , P3.PYARIAM2 as LOAN_ON_BS_INT -- 表内利息
    , P3.PYAWIAM2 as REAL_MTG_ON_BS_INT -- 实抵表内利息
    , P3.PYASIAM2 as LOAN_ON_BS_CINT -- 表内复利
    , P3.PYAXIAM2 as REAL_MTG_ON_BS_CINT -- 实抵表内复利
    , P3.PYATIAM2 as LOAN_OFBS_INT -- 表外利息
    , P3.PYAYIAM2 as REAL_MTG_OFBS_INT -- 实抵表外利息
    , P3.PYAUIAM2 as LOAN_OFBS_CINT -- 表外复利
    , P3.PYAZIAM2 as REAL_MTG_OFBS_CINT -- 实抵表外复利
    , P3.PYAQIAM2 as LOAN_CURR_PERIOD_INT -- 本期利息
    , P3.PYAVIAM2 as REAL_MTG_CURR_PERIOD_INT -- 实抵本期利息
    , P3.PYACPRN as REAL_MTG_CLAIM_PRIN_AMT -- 实抵债权本金
    , P3.PYADPAMT as COMUT_DEBT_TAX_AMT -- 税费金额
    , P3.PYAEPAMT as COMUT_DEBT_ON_ACCT_AMT -- 挂账金额
    , P3.PYAFPAMT as IN_NON_BIZ_INCMAMT -- 入营业外收入金额
    , DATE'${process_date}' as DATA_DT -- None
from
    ${ods_core_schema}.ODS_CORE_BLFMPDAS as P1 -- 抵债资产主文件
    LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMPDPY as P2 -- 资产抵债明细文件
    on P2.PYAAHY12=P1.ASAAHY12
AND P2.PT_DT='${process_date}' 
AND P2.DELETED='0' 
    LEFT JOIN ${ods_core_schema}.ODS_CORE_BLFMPDDP as P3 -- 抵债资产处置明细文件
    on P3.DPAAHY12=P1.ASAAHY12 
AND P3.PT_DT='${process_date}' 
AND P3.DELETED='0' 
where P1.PT_DT='${process_date}' 
AND P1.DELETED='0'
;

-- 删除所有临时表