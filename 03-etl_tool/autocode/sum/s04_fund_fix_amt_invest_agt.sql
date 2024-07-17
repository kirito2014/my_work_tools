-- 层次表名: 聚合层-基金定投协议聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s04_fund_fix_amt_invest_agt
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S04_FUND_FIX_AMT_INVEST_AGT
--     表中文名：基金定投协议聚合表
--     创建日期：2023-12-19 00:00:00
--     主键字段：基金定投协议编号
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：通过基金定投协议表获取定投的相关信息，通过定投协议与基金账户、交易账号信息及基金基本信息、TA信息及包含客户内码的相关表进行关联获取基金开销户日期、基金账号、基金代码名称、客户内码等具体信息
--     更新记录：
--         2023-12-19 00:00:00 王穆军 新增
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S04_FUND_FIX_AMT_INVEST_AGT drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 基金定投协议聚合表

insert into table ${session}.S04_FUND_FIX_AMT_INVEST_AGT(
      业务术语待申请 -- 基金定投协议编号
    , LP_ORG_NO -- 法人机构编号
    , 业务术语待申请 -- 销售商编号
    , TA_NO -- TA编号
    , 业务术语待申请 -- TA名称
    , 业务术语待申请 -- 基金账号
    , 业务术语待申请 -- 定投产品代码
    , 业务术语待申请 -- 定投产品名称
    , 业务术语待申请 -- 定投投资周期代码
    , 业务术语待申请 -- 定投投资周期值
    , 业务术语待申请 -- 定投投资日期
    , 业务术语待申请 -- 定投投资方式代码
    , 业务术语待申请 -- 定投投资值
    , 业务术语待申请 -- 定投终止条件代码
    , 业务术语待申请 -- 定投终止条件值
    , 业务术语待申请 -- 定投协议状态代码
    , 业务术语待申请 -- 定投执行次数
    , 业务术语待申请 -- 定投总成功次数
    , 业务术语待申请 -- 定投累计成功投资金额
    , 业务术语待申请 -- 定投连续扣款失败次数
    , 业务术语待申请 -- 定投总扣款失败次数
    , 业务术语待申请 -- 定投上期应执行日期
    , 业务术语待申请 -- 定投上期实际执行日期
    , 业务术语待申请 -- 定投下期应执行日期
    , 业务术语待申请 -- 交易账号
    , CUST_NO -- 客户号
    , CUST_IN_CD -- 客户内码
    , 业务术语待申请 -- 客户卡号
    , 业务术语待申请 -- 客户风险等级匹配标志
    , 业务术语待申请 -- 投资者类型代码
    , 业务术语待申请 -- 客户风险承受能力等级代码
    , 业务术语待申请 -- 产品风险等级代码
    , 业务术语待申请 -- 风险揭示书类型代码
    , 业务术语待申请 -- 签署风险揭示书代码标志
    , 业务术语待申请 -- 定投受理方式代码
    , OPER_TELR_NO -- 操作柜员编号
    , CUST_MGR_NO -- 客户经理编号
    , FIN_MGR_NO -- 理财经理编号
    , TXN_HAPP_ORG_NO -- 交易发生机构编号
    , TXN_HAPP_BNKOUTLS_NO -- 交易发生网点编号
    , 业务术语待申请 -- 基金账户开户日期
    , 业务术语待申请 -- 基金账户销户日期
    , 业务术语待申请 -- 定投投资初始日期
    , 业务术语待申请 -- 定投投资终止日期
    , 业务术语待申请 -- 新增日期
    , 业务术语待申请 -- 新增时间
    , 业务术语待申请 -- 更新日期
    , 业务术语待申请 -- 更新时间
)
select
      P1.BUY_PLAN_NO as 业务术语待申请 -- 基金定投协议编号
    , P1.UNIONCODE as LP_ORG_NO -- 总行代码
    , P1.DISTRIBUTOR_CODE as 业务术语待申请 -- 销售商代码
    , P1.TA_NO as TA_NO -- TA代码
    , P2.FULL_NAME as 业务术语待申请 -- 全名
    , P3.TA_ACCT as 业务术语待申请 -- 基金帐号
    , P1.PROD_CODE as 业务术语待申请 -- 产品代码
    , P4.PROD_NAME as 业务术语待申请 -- 基金简称
    , P1.INVEST_CYCLE as 业务术语待申请 -- 投资周期
    , P1.INVEST_CYCLE_VALUE as 业务术语待申请 -- 投资周期值
    , P1.INVEST_DAY as 业务术语待申请 -- 投资日期
    , P1.INVEST_MODE as 业务术语待申请 -- 后续投资方式
    , P1.INVEST_AMT as 业务术语待申请 -- 投资值(金额/比例)
    , P1.INVEST_TIME as 业务术语待申请 -- 终止条件
    , P1.INVEST_TIME_VALUE as 业务术语待申请 -- 终止条件值
    , P1.INVEST_STATUS as 业务术语待申请 -- 定投协议状态
    , P1.APP_COUNT as 业务术语待申请 -- 执行次数
    , P1.TOTAL_SUCC_COUNT as 业务术语待申请 -- 总成功次数
    , P1.TOTAL_SUCC_INVEST_AMT as 业务术语待申请 -- 累计成功投资金额
    , P1.CONTINUE_FAIL_COUNT as 业务术语待申请 -- 连续扣款失败次数
    , P1.TOTAL_FAIL_COUNT as 业务术语待申请 -- 总扣款失败次数
    , P1.LAST_APP_DATE as 业务术语待申请 -- 上期应执行日期
    , P1.LAST_REAL_APP_DATE as 业务术语待申请 -- 上期实际执行日期
    , P1.NEXT_APP_DATE as 业务术语待申请 -- 下期应执行日期
    , P1.TRANS_ACCT as 业务术语待申请 -- 交易账号
    , P1.CUST_NO as CUST_NO -- 客户号
    , P5.CUST_INNER_CODE as CUST_IN_CD -- 客户内码              
    , P6.DEPOSIT_ACCT as 业务术语待申请 -- 银行卡号/行外交易账号
    , P1.RISK_MATCHING as 业务术语待申请 -- 风险等级是否匹配
    , P1.INVEST_CUST_TYPE as 业务术语待申请 -- 投资者类型
    , P1.CUST_RISK as 业务术语待申请 -- 客户风险承担能力等级
    , P1.RISK_LEVEL as 业务术语待申请 -- 风险等级
    , P1.RISK_NOTICE_TYPE as 业务术语待申请 -- 风险揭示书类型
    , P1.SIGN_RISK_NOTICE_FLAG as 业务术语待申请 -- 是否签署风险揭示书
    , P1.ACCEPTMETHOD as 业务术语待申请 -- 受理方式
    , P1.OPER_ID as OPER_TELR_NO -- 操作柜员
    , P1.CUST_MANAGER as CUST_MGR_NO -- 客户经理
    , P1.FM_MANAGER as FIN_MGR_NO -- 理财经理
    , P1.TRANS_BRCH_CODE as TXN_HAPP_ORG_NO -- 交易发生分行
    , P1.TRANS_SUBBRCH_CODE as TXN_HAPP_BNKOUTLS_NO -- 交易发生网点
    , P3.NEW_DATE as 业务术语待申请 -- 新增日期
    , P3.UPT_DATE as 业务术语待申请 -- 更新日期
    , P1.START_INVEST_DATE as 业务术语待申请 -- 投资初始日期
    , P1.END_INVEST_DATE as 业务术语待申请 -- 投资终止日期
    , P1.NEW_DATE as 业务术语待申请 -- 新增日期
    , P1.NEW_TIME as 业务术语待申请 -- 新增时间
    , P1.UPT_DATE as 业务术语待申请 -- 更新日期
    , P1.UPT_TIME as 业务术语待申请 -- 更新时间
from
    ${ods_nfds_schema}.ODS_NFDS_FUND_APP_BUYPLANINFO as P1 -- 客户定投协议表
    LEFT JOIN ${ods_nfds_schema}.ODS_NFDS_FUND_CFG_TA as P2 -- TA信息表
    on P1.UNIONCODE=P2.UNIONCODE 
AND  P1.TA_NO=P2.TA_NO
AND P2.PT_DT='${process_date}' 
AND P2.DELETED='0' 
    LEFT JOIN ${ods_nfds_schema}.ODS_NFDS_FUND_ACCT as P3 -- 客户基金帐户表
    on P1.UNIONCODE=P3.UNIONCODE 
AND P1.CUST_NO=P3.CUST_NO 
AND  P1.TA_NO=P3.TA_NO
AND P3.PT_DT='${process_date}' 
AND P3.DELETED='0' 
    LEFT JOIN ${ods_nfds_schema}.ODS_NFDS_FUND_CFG as P4 -- 基金基本信息
    on P1.UNIONCODE=P4.UNIONCODE 
AND P1.PROD_CODE=P4.PROD_CODE
AND  P4.PT_DT='${process_date}' 
AND P4.DELETED='0' 
    LEFT JOIN ${ods_nfds_schema}.ODS_NFDS_COM_CUST_INFO as P5 -- 公共客户资料表
    on P1.UNIONCODE=P5.UNIONCODE 
AND P1.CUST_NO=P5.CUST_NO
AND P5.PT_DT='${process_date}' 
AND P5.DELETED='0' 
    LEFT JOIN ${ods_nfds_schema}.ODS_NFDS_COM_BANK_ACCT as P6 -- 交易账号信息表
    on P1.UNIONCODE=P6.UNIONCODE 
AND P1.CUST_NO=P6.CUST_NO
AND P6.PT_DT='${process_date}' 
AND P6.DELETED='0' 
where P1.PT_DT='${process_date}' 
AND P1.DELETED='0'
;

-- 删除所有临时表