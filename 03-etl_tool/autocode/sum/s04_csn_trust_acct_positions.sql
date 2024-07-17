-- 层次表名: 聚合层-代销信托账户持仓聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s04_csn_trust_acct_positions
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S04_CSN_TRUST_ACCT_POSITIONS
--     表中文名：代销信托账户持仓聚合表
--     创建日期：2023-12-26 00:00:00
--     主键字段：法人机构编号,信托交易账户编号,信托账户编号,信托产品编号
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：None
--     更新记录：
--         2023-12-26 00:00:00 王穆军 新增映射信息
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S04_CSN_TRUST_ACCT_POSITIONS drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 代销信托账户持仓聚合表

insert into table ${session}.S04_CSN_TRUST_ACCT_POSITIONS(
      LP_ORG_NO -- 法人机构编号
    , 业务术语待申请 -- 销售商编号
    , 业务术语待申请 -- TA代码
    , CUST_IN_CD -- 客户内码
    , 业务术语待申请 -- 客户类型代码
    , CUST_NAME -- 客户名称
    , 业务术语待申请 -- 签约机构编号
    , 业务术语待申请 -- 银行账户编号
    , 业务术语待申请 -- 信托交易账户编号
    , 业务术语待申请 -- 信托账户编号
    , 业务术语待申请 -- 银行卡号
    , 业务术语待申请 -- 信托账户开户日期
    , 业务术语待申请 -- 信托账户状态代码
    , CURR_CD -- 币种代码
    , DIVDND_MODE_CD -- 分红方式代码
    , 业务术语待申请 -- 信托产品编号
    , 业务术语待申请 -- 信托产品大类代码
    , 业务术语待申请 -- 信托产品类型代码
    , 业务术语待申请 -- 信托产品名称
    , 业务术语待申请 -- 信托产品最新净值
    , 业务术语待申请 -- 最新净值日期
    , 业务术语待申请 -- 昨日信托份数余额
    , 业务术语待申请 -- 昨日交易冻结份额
    , 业务术语待申请 -- 昨日异常冻结份额
    , 业务术语待申请 -- 当日份额变动份额
    , 业务术语待申请 -- 当日交易冻结份额
    , 业务术语待申请 -- 当日异常冻结份额
    , 业务术语待申请 -- 销售系统交易当日份额变动份额
    , 业务术语待申请 -- 销售系统交易冻结份额
    , 业务术语待申请 -- 销售系统异常冻结份额
    , 业务术语待申请 -- 销售系统清算期间交易冻结份额
    , 业务术语待申请 -- 申购在途份额
    , 业务术语待申请 -- 已兑付份额
    , 业务术语待申请 -- 快速过户冻结份额
    , 业务术语待申请 -- 清算申购在途份额
    , 业务术语待申请 -- 清算快速过户冻结份额
    , 业务术语待申请 -- 最后更新日期
    , PT_DT  -- 数据日期
)
select
      T1.UNIONCODE as LP_ORG_NO -- 总行代码
    , T1.DISTRIBUTORCODE as 业务术语待申请 -- 销售商代码
    , T1.TANO as 业务术语待申请 -- TA代码
    , T2.INNERCODE as CUST_IN_CD -- 客户内码
    , CASE WHEN T2.INVTP IS NULL OR TRIM(T2.INVTP)='' 
     THEN '' 
ELSE COALESCE(TRIM(S1.TARGET_CD_VAL), '@'||TRIM(T2.INVTP),'') END  as 业务术语待申请 -- 投资者类别
    , T2.INVESTORNAME as CUST_NAME -- 客户姓名全称
    , T2.NETNO as 业务术语待申请 -- 签约网点代码
    , T3.DEPOSITACCOUNT as 业务术语待申请 -- 账户
    , T1.TRANSACTIONACCOUNTID as 业务术语待申请 -- 交易帐号
    , T1.TAACCOUNTID as 业务术语待申请 -- 基金帐号
    , T3.DEPOSITACCT as 业务术语待申请 -- 银行账号
    , T4.OPENDATE as 业务术语待申请 -- 开户日期
    , T4.STATUS as 业务术语待申请 -- 帐户状态
    , CASE WHEN T5.CURRENCYTYPE IS NULL OR TRIM(T5.CURRENCYTYPE)='' 
     THEN '' 
ELSE COALESCE(TRIM(S2.TARGET_CD_VAL), '@'||TRIM(T5.CURRENCYTYPE),'') END 
 as CURR_CD -- 结算币种
    , T5.DIVIDEND as DIVDND_MODE_CD -- 分红方式
    , T1.FUNDCODE as 业务术语待申请 -- 基金代码
    , T5.CATEGORY as 业务术语待申请 -- 产品大类
    , T5.FUNDTYPE as 业务术语待申请 -- 产品类型
    , T5.FUNDENGNAME as 业务术语待申请 -- 基金简称
    , T6.NAV as 业务术语待申请 -- 当日产品净值
    , T6.NAVDATE as 业务术语待申请 -- 净值日期
    , T1.LAST_FUNDVOL as 业务术语待申请 -- 昨日基金份数余额
    , T1.LAST_FROZEN as 业务术语待申请 -- 昨日交易冻结份额
    , T1.LAST_ABNMFROZEN as 业务术语待申请 -- 昨日异常冻结份额
    , T1.FUNDVOL as 业务术语待申请 -- 当日份额变动份额[发生]
    , T1.FROZEN as 业务术语待申请 -- 当日交易冻结份额[发生]
    , T1.ABNMFROZEN as 业务术语待申请 -- 当日异常冻结份额[发生]
    , T1.TRD_FUNDVOL as 业务术语待申请 -- 销售系统交易当日份额变动份额[发生]
    , T1.TRD_FROZEN as 业务术语待申请 -- 销售系统交易冻结份额[发生]
    , T1.TRD_ABNMFROZEN as 业务术语待申请 -- 销售系统异常冻结份额[发生]
    , T1.CLR_FROZEN as 业务术语待申请 -- 销售系统清算期间交易冻结份额[发生]
    , T1.PRE_CONFIRMVOL as 业务术语待申请 -- 申购在途份额（包括未确认与未导出两部分）
    , T1.INVESTSHARE as 业务术语待申请 -- 兑付的份额
    , T1.QUICK_FROZENVOL as 业务术语待申请 -- 快速过户冻结份额
    , T1.TRD_PRE_CONFIRMVOL as 业务术语待申请 -- 清算申购在途份额（包括未确认与未导出两部分）
    , T1.TRD_QUICK_FROZENVOL as 业务术语待申请 -- 清算快速过户冻结份额
    , T1.MODIDATE as 业务术语待申请 -- 最后更新日期
    , '${process_date}' as PT_DT  -- None
from
    ${ODS_TSS_SCHEMA}.ODS_TSS_TRUST_BAL_FUND as T1 -- 份额汇总表
    LEFT JOIN ${ODS_TSS_SCHEMA}.ODS_TSS_TRUST_ACCT_CUST as T2 -- 客户信息表
    on T1.UNIONCODE = T2..UNIONCODE 
AND  T1..CUSTNO =  T2.CUSTNO
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN ${ODS_TSS_SCHEMA}.ODS_TSS_TRUST_ACCT_MONEYACCOUNT as T3 -- 资金帐户表
    on T1.UNIONCODE =T3.UNIONCODE 
AND T1.CUSTNO = T3.CUSTNO
AND T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN ${ODS_TSS_SCHEMA}.ODS_TSS_TRUST_ACCT_FUND as T4 -- 基金账户信息表
    on T1.UNIONCODE =T4.UNIONCODE 
AND T1.TANO = T4.TANO 
AND  T1.TAACCOUNTID=T4.TAACCOUNTID
AND T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN ${ODS_TSS_SCHEMA}.ODS_TSS_TRUST_CFG_FUND  as T5 -- 基金信息表
    on T1.UNIONCODE = T5.UNIONCODE 
AND  T1.TANO =  T5.TANO 
AND T1.FUNDCODE =  T5.FUNDCODE
AND T5.PT_DT='${process_date}' 
AND T5.DELETED='0' 
    LEFT JOIN ${ODS_TSS_SCHEMA}.ODS_TSS_TRUST_CFG_FUNDNAV as T6 -- 每日产品净值表
    on T1.UNIONCODE = T6.UNIONCODE 
AND T1.TANO =  T6.TANO 
AND  T1.FUNDCODE =  T6.FUNDCODE
AND T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
    LEFT JOIN 代码表待命名 as S1 -- 代码转换
    on T2.INVTP = S1.SRC_CD_VAL 
AND S1.SRC_SYS_CD = 'TSS'
AND S1.SRC_COL_NAME ='INVTP'
AND S1.SRC_TAB_NAME = 'ODS_TSS_TRUST_ACCT_CUST' 
    LEFT JOIN 代码表待命名 as S2 -- 代码转换
    on T5.CURRENCYTYPE = S2.SRC_CD_VAL 
AND S2.SRC_SYS_CD = 'TSS'
AND S2.SRC_COL_NAME ='CURRENCYTYPE'
AND S2.SRC_TAB_NAME = 'ODS_TSS_TRUST_CFG_FUND ' 
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;

-- 删除所有临时表