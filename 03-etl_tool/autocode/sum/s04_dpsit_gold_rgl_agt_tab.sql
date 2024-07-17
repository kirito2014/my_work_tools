-- 层次表名: 聚合层-积存金定期协议聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s04_dpsit_gold_rgl_agt_tab
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S04_DPSIT_GOLD_RGL_AGT_TAB
--     表中文名：积存金定期协议聚合表
--     创建日期：2023-01-03 00:00:00
--     主键字段：定期积存协议签订支行机构编号
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含积存金的定期协议的协议等信息
--     更新记录：
--         2023-01-03 00:00:00 王穆军 新增
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S04_DPSIT_GOLD_RGL_AGT_TAB drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 客户内码临时表信息
drop table if exists ${session}.TMP_S04_DPSIT_GOLD_RGL_AGT_TAB_01;

create table ${session}.TMP_S04_DPSIT_GOLD_RGL_AGT_TAB_01 (
      ACCT_NO varchar(30) -- 账号
    , CUST_IN_CD varchar(11) -- 客户内码
    , PT_DT string -- 数据日期
)
comment '客户内码临时表信息'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S04_DPSIT_GOLD_RGL_AGT_TAB_01(
      ACCT_NO -- 账号
    , CUST_IN_CD -- 客户内码
    , PT_DT -- 数据日期
)
select
      P1.ACCT_NO as ACCT_NO -- 客户号
    , P1.CUST_INCOD as CUST_IN_CD -- 客户内码
    , '${process_date}' as PT_DT -- None
from
    (
	SELECT DISTINCT 
		trim(U1.AA01AC15) AS ACCT_NO 	--账号
		,trim(U1.AA03CSNO) AS CUST_INCOD 	--客户内码
		--trim(U1.AA04FLNM) AS CUST_NAME	--客户名称
	FROM 
		${ODS_CORE_SCHEMA}.ODS_CORE_BDFMHQAA U1
	WHERE 
		1=1
		AND U1.AA15ZHZT = '1' --正常状态
		AND U1.PT_DT='${process_date}' 
		AND U1.DELETED='0'
		AND TRIM(U1.RCSTRS1B)<>'9' --活期存款主档
	UNION 
	SELECT DISTINCT 
		 trim(U2.CDNOAC19) AS ACCT_NO --账号
		,trim(U2.CINOCSNO) AS CUST_INCOD --客户内码
		--,trim(U2.CHNMNM40) AS CUST_NAME--客户名称
	FROM 
		${ODS_CORE_SCHEMA}.ODS_CORE_BWFMDCIM U2
	WHERE 
		1=1
		AND U2.BSTPBUST = '101' --借记卡
		AND TRIM(U2.CRDSCRDS) IN('1','5') 	--正常状态未销户
		AND U2.PT_DT='${process_date}' 
		AND U2.DELETED='0'
		AND TRIM(U2.RCSTRS1B)<>'9' --（卡片状态1，记录状态9）
	)  as P1 -- None
;
-- ==============字段映射（第2组）==============
-- 积存金定期协议聚合表

insert into table ${session}.S04_DPSIT_GOLD_RGL_AGT_TAB(
      业务术语待申请 -- 定期积存协议签订支行机构编号
    , 业务术语待申请 -- 定期积存协议签订网点编号
    , 业务术语待申请 -- 定期积存协议编号
    , 业务术语待申请 -- 积存金账号编号
    , 业务术语待申请 -- 积存金合作方编号
    , 业务术语待申请 -- 定期积存金产品编号
    , 业务术语待申请 -- 连续成功次数
    , 业务术语待申请 -- 连续失败次数
    , 业务术语待申请 -- 累计成功次数
    , 业务术语待申请 -- 累计失败次数
    , 业务术语待申请 -- 积存手续费
    , 业务术语待申请 -- 积存手续费率
    , 业务术语待申请 -- 展期标志
    , 业务术语待申请 -- 绑定标识代码
    , TXN_DT -- 交易日期
    , 业务术语待申请 -- 积存结束日期
    , CURR_CD -- 币种代码
    , 业务术语待申请 -- 积存金额
    , 业务术语待申请 -- 积存金定投方式代码
    , 业务术语待申请 -- 每期定投日
    , 业务术语待申请 -- 定投期限
    , 业务术语待申请 -- 营销人员编号
    , LP_ORG_NO -- 法人机构编号
    , 业务术语待申请 -- 协议签订日期
    , 业务术语待申请 -- 协议修改日期
    , 业务术语待申请 -- 下次积存日期
    , 业务术语待申请 -- 银行卡号
    , 业务术语待申请 -- 积存金账户类型代码
    , 业务术语待申请 -- 银行账号编号
    , 业务术语待申请 -- 开户柜员编号
    , OPEN_ACCT_DT -- 积存金账户开户日期
    , 业务术语待申请 -- 积存金账户开户机构编号
    , 业务术语待申请 -- 积存金账户状态代码
    , 业务术语待申请 -- 积存金投资人名称
    , 业务术语待申请 -- 积存金投资人类别代码
    , 业务术语待申请 -- 积存金账户销户日期
    , 业务术语待申请 -- 最近一次成功扣款金额
    , 业务术语待申请 -- 最近一次成功扣款日期
    , CUST_IN_CD -- 客户内码
    , PT_DT -- 数据日期
)
select
      T1.BRCH_CODE as 业务术语待申请 -- 定期积存协议签订支行
    , T1.SUBBRCH_CODE as 业务术语待申请 -- 定期积存协议签订网点
    , T1.BUYPLANNO as 业务术语待申请 -- 定期积存协议号
    , T1.PRODACC as 业务术语待申请 -- 积存金账号
    , T1.DISTRIBUTORCODE as 业务术语待申请 -- 合作行代码
    , T1.PROD_CODE as 业务术语待申请 -- 产品代码
    , T1.CONTINUESUCCTIMES as 业务术语待申请 -- 连续成功次数
    , T1.CONTINUEFAILTIMES as 业务术语待申请 -- 连续失败次数
    , T1.TOTALSUCCTIMES as 业务术语待申请 -- 累计成功次数
    , T1.TOTALFAILTIMES as 业务术语待申请 -- 累计失败次数
    , T1.CASHFEE as 业务术语待申请 -- 积存费用（手续费）
    , T1.CASHFEERATE as 业务术语待申请 -- 积存费率
    , T1.EXTFLAG as 业务术语待申请 -- 展期标志（0-否，1-是）
    , T1.STATUS as 业务术语待申请 -- 绑定标志（0-正常,1-到期，2-违约退出，3-申请退出，4-参加计划失败）
    , T1.TRANSACTIONDATE as TXN_DT -- 交易日期
    , T1.ENDDATE as 业务术语待申请 -- 积存结束日期
    , CASE
         WHEN T1.CURRENCYTYPE IS NULL OR TRIM(T1.CURRENCYTYPE) = '' THEN
          ''
         ELSE
          COALESCE(TRIM(S1.TARGET_CD_VAL),
                   '@' || TRIM(T1.CURRENCYTYPE),
                   '')
       END as CURR_CD -- 币种类型
    , T1.AMOUNT as 业务术语待申请 -- 积存金额
    , T1.PLANTYPE as 业务术语待申请 -- 定投方式（1-日均定投，2-定期定额）
    , T1.DCADATE as 业务术语待申请 -- 每期定投日（定投方式为2时必输入）
    , T1.AGMLMT as 业务术语待申请 -- 期限
    , T1.CUSTMANAGERID as 业务术语待申请 -- 营销人员
    , T1.UNIONCODE as LP_ORG_NO -- 农商行代码 
    , T1.INSERTDATE as 业务术语待申请 -- 新增日期
    , T1.MODIDATE as 业务术语待申请 -- 修改日期
    , T1.NEXTPLANDATE as 业务术语待申请 -- 下次积存日期
    , T2.DEPOSITACCT as 业务术语待申请 -- 银行卡号
    , T2.ACCT_FLAG as 业务术语待申请 -- 账户类型 0-主卡，1-附卡
    , T2.DEPOSITACCOUNT as 业务术语待申请 -- 银行账号
    , T2.CUSTMANAGERID as 业务术语待申请 -- 营销人员
    , T2.INSERTDATE as OPEN_ACCT_DT -- 账户新增日期
    , T2.REG_SUBBRCHCODE as 业务术语待申请 -- 签约网点
    , T2.ACCT_STATUS as 业务术语待申请 -- 积存金账户状态（0-正常，1-冻结中， 2-已冻结， 3- 销户中, 4-已销户）
    , T2.CUST_NAME as 业务术语待申请 -- 积存金投资人名称
    , CASE
         WHEN T2.INVTP IS NULL OR TRIM(T2.INVTP) = '' THEN
          ''
         ELSE
          COALESCE(TRIM(S1.TARGET_CD_VAL),
                   '@' || TRIM(T2.INVTP),
                   '')
       END as 业务术语待申请 -- 投资人类别(0-机构,1-个人)
    , T2.MODIDATE as 业务术语待申请 -- 最后更新日期
    , T3.AMOUNT as 业务术语待申请 -- 协议积存金额
    , T3.RUNDATE as 业务术语待申请 -- 协议积存日期
    , T4.CUST_ISN as CUST_IN_CD -- 客户内码
    , '${process_date}' as PT_DT -- None
from
    ${ODS_FMS_SCHEMA}.GOLD_PLANINFO as T1 -- 定期积存协议表 
    LEFT JOIN ( 
SELECT P2.DEPOSITACCT,    --银行卡号 
       P2.PRODACC,      
       P2.UNIONCODE,
       P2.ACCT_FLAG,    --账户类型 0-主卡，1-附卡
       P2.DEPOSITACCOUNT,   --银行账号
       P2.CUSTMANAGERID,  --营销人员
       P2.INSERTDATE,   --账户新增日期
       P2.REG_SUBBRCHCODE,  --签约网点
       P2.ACCT_STATUS,    --积存金账户状态（0-正常，1-冻结中， 2-已冻结， 3- 销户中, 4-已销户）
       P2.CUST_NAME,    --积存金投资人名称
       P2.INVTP,      --投资人类别(0-机构,1-个人)
       P2.MODIDATE      --最后更新日期
  FROM ${ODS_FMS_SCHEMA}.GOLD_PRODACC P2
 WHERE P2.ACCT_STATUS = '3'
 AND P2.PT_DT='${process_date}' 
 AND P2.DELETED='0'
 
--本字段积存金账户状态 为销户取值
) as T2 -- 积存账户表
    on T1.PRODACC =T2.PRODACC 
AND  T1.UNIONCODE=T2.UNIONCODE
 
    LEFT JOIN (
SELECT BUYPLANNO
       AMOUNT,
       RUNDATE,

  FROM (SELECT BUYPLANNO,
               AMOUNT,
               RUNDATE,
               ROW NUMBER() OVER(PARTITION BY BUYPLANO ORDER BY CHKDATE DESC) RN
          FROM ${ODS_FMS_SCHEMA}.GOLD_INTERFACE_PLANCHKMETHOD
         WHERE PT_DT = '${process_date}'
           AND DELETED = '0')
 WHERE RN = 1
) as T3 -- 积存金定期积存扣款记录表
    on T1.BUYPLANNO=T3.BUYPLANNO
AND T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN TMP_S04_DPSIT_GOLD_RGL_AGT_TAB_01 as T4 -- None
    on T4.ACCT_NO =T1.ACCT_NO 
    LEFT JOIN 代码表 as S1 -- 币种代码转换
    on T1.CURRENCYTYPE = S1.SRC_CD_VAL 
AND S1.SRC_SYS_CD = 'FMS'
AND S1.SRC_COL_NAME ='CURRENCYTYPE'
AND S1.SRC_TAB_NAME = 'ODS_FMS_GOLD_PLANINFO' 
    LEFT JOIN 代码表 as S2 -- 投资人类别转换
    on T2.INVTP = S1.SRC_CD_VAL 
AND S1.SRC_SYS_CD = 'FMS'
AND S1.SRC_COL_NAME ='INVTP'
AND S1.SRC_TAB_NAME = 'ODS_FMS_GOLD_CUSTINFO' 
where T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;

-- 删除所有临时表
drop table ${session}.TMP_S04_DPSIT_GOLD_RGL_AGT_TAB_01;