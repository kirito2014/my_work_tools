-- 层次表名: 聚合层-代销理财账户持仓聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s07_csn_fin_acct_positions
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S07_CSN_FIN_ACCT_POSITIONS
--     表中文名：代销理财账户持仓聚合表
--     创建日期：2023-12-19 00:00:00
--     主键字段：交易账号,理财产品代码,交易发生销售商编号,TA编号
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：代销理财，当前账户持仓的信息，包含该账户持有份额、冻结份额等信息
--     更新记录：
--         2023-12-19 00:00:00 王穆军 新增
--         2024-01-03 00:00:00 王穆军 修改
--         None None None
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S07_CSN_FIN_ACCT_POSITIONS drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 代销理财账户持仓聚合表

insert into table ${session}.S07_CSN_FIN_ACCT_POSITIONS(
      业务术语待申请 -- 交易账号
    , 业务术语待申请 -- 理财产品代码
    , TXN_HAPP_SELLER_NO -- 交易发生销售商编号
    , TA_NO -- TA编号
    , 业务术语待申请 -- 销售商名称
    , CUST_IN_CD -- 客户内码
    , 业务术语待申请 -- 签约法人机构编号
    , TA_ACCT -- TA账号
    , 业务术语待申请 -- 外部机构产品代码
    , CURR_CD -- 币种代码
    , 业务术语待申请 -- TA账户状态代码
    , FINAL_UPDATE_DT -- 最后更新日期
    , 业务术语待申请 -- TA账户开户日期
    , SIGN_BNKOUTLS_NO -- 签约网点编号
    , DIVDND_MODE_CD -- 分红方式代码
    , 业务术语待申请 -- 理财产品名称
    , 业务术语待申请 -- 理财产品大类代码
    , 业务术语待申请 -- 理财产品类型代码
    , 业务术语待申请 -- 销售系统清算期间交易冻结份额
    , 业务术语待申请 -- 当日交易冻结份额
    , 业务术语待申请 -- 当日份额变动份额
    , 业务术语待申请 -- 昨日交易冻结份额
    , 业务术语待申请 -- 昨日基金份数份额
    , 业务术语待申请 -- 销售系统交易冻结份额
    , 业务术语待申请 -- 销售系统交易当日份额变动份额
    , 业务术语待申请 -- 最新净值 
    , 业务术语待申请 -- 最新净值日期
)
select
      P1.TRANSACTIONACCOUNID as 业务术语待申请 -- 交易账号
    , P1.FUNDCODE as 业务术语待申请 -- 产品代码
    , P1.DISTRIBUTORCODE as TXN_HAPP_SELLER_NO -- 销售商代码
    , P1.TANO as TA_NO -- TA代码
    , P2.ORGNAME as 业务术语待申请 -- 简称
    , P5.INNERCODE as CUST_IN_CD -- 客户内码              
    , P5.UNIONCODE as 业务术语待申请 -- 签约农商行代码
    , P1.TAACCOUNTID as TA_ACCT -- 账号
    , P3.FUNDCODE_EXT as 业务术语待申请 -- 外部机构产品代码
    , CASE WHEN P3.CURRENCYTYPE IS NULL OR TRIM(P3.CURRENCYTYPE)='' 
     THEN '' 
ELSE COALESCE(TRIM(S1.TARGET_CD_VAL), '@'||TRIM(P3.CURRENCYTYPE),'') END  as CURR_CD -- 结算币种
    , P4.STATUS as 业务术语待申请 -- 账户状态
    , P1.MODIDATE as FINAL_UPDATE_DT -- 最后更新日期
    , P4.OPENDATE as 业务术语待申请 -- 开户日期
    , P5.NETNO as SIGN_BNKOUTLS_NO -- 签约网点代码
    , P3.DIVIDEND as DIVDND_MODE_CD -- 分红方式
    , P3.FUNDNAME as 业务术语待申请 -- 产品简称
    , P3.CATEGORY as 业务术语待申请 -- 产品大类
    , P3.FUNDTYPE as 业务术语待申请 -- 产品类型
    , P1.CLR_FROZEN as 业务术语待申请 -- 销售系统清算期间交易冻结份额(发生)
    , P1.FROZEN as 业务术语待申请 -- 当日交易冻结份额
    , P1.FUNDVOL as 业务术语待申请 -- 当日份额变动份额
    , P1.LAST_FROZEN as 业务术语待申请 -- 昨日交易冻结份额
    , P1.LAST_FUNDVOL as 业务术语待申请 -- 昨日基金份数份额
    , P1.TRD_FROZEN as 业务术语待申请 -- 销售系统交易冻结份额(发生)
    , P1.TRD_FUNDVOL as 业务术语待申请 -- 销售系统交易当日份额变动份额(发生)
    , P6.NAV as 业务术语待申请 -- 净值 
    , P6.NAVDATE as 业务术语待申请 -- 净值日期
from
    ${ods_fds_schema}.ODS_FDS_FWS_BAL_FUND as P1 -- 份额汇总表
    LEFT JOIN ${ods_fds_schema}.ODS_FDS_FWS_CFG_DISTRIBUTOR as P2 -- 销售商信息表
    on P1.DISTRIBUTORCODE = P2.DISTRIBUTORCODE
AND  P2.DELETED = '0' 
AND P2.PT_DT = '${process_date}'  
    LEFT JOIN ${ods_fds_schema}.ODS_FDS_FWS_CFG_FUND as P3 -- 产品信息表
    on P1.TANO = P3.TANO 
AND P1.FUNDCODE = P3.FUNDCODE 
AND  P1.DISTRIBUTORCODE = P3.DISTRIBUTORCODE
AND  P3.DELETED = '0' 
AND P3.PT_DT = '${process_date}'  
    LEFT JOIN ${ods_fds_schema}.ODS_FDS_FWS_ACCT_FUND as P4 -- TA账户信息表
    on P1.TANO = P4.TANO 
AND P1.CUSTNO = P4.CUSTNO 
AND P1.DISTRIBUTORCODE = P4.DISTRIBUTORCODE
AND P4.DELETED = '0' 
AND P4.PT_DT = '${process_date}'  
    LEFT JOIN ${ods_fds_schema}.ODS_FDS_FWS_ACCT_CUST as P5 -- 客户信息表
    on P1.CUSTNO = P5.CUSTNO 
AND P1.DISTRIBUTORCODE = P5.DISTRIBUTORCODE
AND P5.DELETED = '0' 
AND P5.PT_DT = '${process_date}'  
    LEFT JOIN ${ods_fds_schema}.ODS_FDS_FWS_CFG_SHOW_FUNDNAV as P6 -- 理财产品展示净值
    on P1.TANO = P6.TANO
AND P1.DISTRIBUTORCODE = P6.DISTRIBUTORCODE 
AND P1.FUNDCODE = P6.FUNDCODE
AND P6.DELETED = '0' 
AND P6.PT_DT = '${process_date}'  
    LEFT JOIN 代码表待命名 as S1 -- 代码转换
    on P3.CURRENCYTYPE = S1.SRC_CD_VAL 
AND S1.SRC_SYS_CD = 'FDS'
AND S1.SRC_COL_NAME ='CURRENCYTYPE'
AND S1.SRC_TAB_NAME = 'ODS_FDS_FWS_CFG_FUND' 
where P1.PT_DT='${process_date}' 
AND P1.DELETED='0'
;

-- 删除所有临时表