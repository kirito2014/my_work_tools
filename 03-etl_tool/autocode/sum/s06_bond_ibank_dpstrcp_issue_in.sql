-- 层次表名: 聚合层-债券/同业存单发行信息聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s06_bond_ibank_dpstrcp_issue_info
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S06_BOND_IBANK_DPSTRCP_ISSUE_INFO
--     表中文名：债券/同业存单发行信息聚合表
--     创建日期：2023-01-03 00:00:00
--     主键字段：申请编号
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含债券的基本信息，债券的发行信息，发行机构信息，发行机构评级信息
--     更新记录：
--         2023-01-03 00:00:00 王穆军 新增
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S06_BOND_IBANK_DPSTRCP_ISSUE_INFO drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 债券/同业存单发行信息聚合表

insert into table ${session}.S06_BOND_IBANK_DPSTRCP_ISSUE_INFO_TAB(
      业务术语待申请 -- 申请编号
    , 业务术语待申请 -- 申请日期
    , 业务术语待申请 -- 债券产品类型代码
    , 业务术语待申请 -- 债券计划发行金额
    , 业务术语待申请 -- 债券发行机构名称
    , 业务术语待申请 -- 债券发行类型代码
    , 业务术语待申请 -- 债券发行方式代码
    , INTACR_BASE_CD -- 计息基础代码
    , 业务术语待申请 -- 债券公告日期
    , 业务术语待申请 -- 债券发行日期
    , 业务术语待申请 -- 债券面值
    , 业务术语待申请 -- 债券期限
    , 业务术语待申请 -- 期限单位代码
    , 业务术语待申请 -- 债券起息日期
    , 业务术语待申请 -- 债券到期日期
    , 业务术语待申请 -- 债券缴款日期
    , 业务术语待申请 -- 债券兑付日期
    , 业务术语待申请 -- 债券参考收益率
    , 业务术语待申请 -- 债券发行价格
    , 业务术语待申请 -- 债券息票类型代码
    , 业务术语待申请 -- 债券基准利率
    , 业务术语待申请 -- 债券浮动利差
    , 业务术语待申请 -- 债券行权标志
    , 业务术语待申请 -- 债券行权方式代码
    , 业务术语待申请 -- 债券行权日期
    , 业务术语待申请 -- 债券行权利率
    , 业务术语待申请 -- 收款账户类型代码
    , 业务术语待申请 -- 收款账户名称
    , 业务术语待申请 -- 收款账户编号
    , 业务术语待申请 -- 收款账户开户行名称
    , 业务术语待申请 -- 债券实际发行金额
    , 业务术语待申请 -- 债券实际发行价格
    , 业务术语待申请 -- 债券实际利率
    , 业务术语待申请 -- 债券应缴金额
    , 业务术语待申请 -- 债券实缴金额
    , 业务术语待申请 -- 债券承销商简称
    , 业务术语待申请 -- 债券承销手续费金额
    , 业务术语待申请 -- 申请事由
    , TXN_ORG_NO -- 交易机构编号
    , OPERR_TELR_NO -- 经办人柜员编号
    , OPERR_NAME -- 经办人名称
    , 业务术语待申请 -- 数据有效标识代码
    , 业务术语待申请 -- 发行债券类型代码
    , 业务术语待申请 -- 转账方式代码
    , 业务术语待申请 -- 债券编号
    , 业务术语待申请 -- 债券名称简称
    , 业务术语待申请 -- 债券名称全称
    , 业务术语待申请 -- 债券主承销商全称
    , 业务术语待申请 -- 债券发行机构编号
    , 业务术语待申请 -- 债券发行范围代码
    , 业务术语待申请 -- 债券担保方式代码
    , 业务术语待申请 -- 债券发行机构行业类别
    , 业务术语待申请 -- 托管机构代码
    , 业务术语待申请 -- 债券发行机构企业性质
    , 业务术语待申请 -- 债券上市日期
    , 业务术语待申请 -- 债券发行机构所属行业代码
    , 业务术语待申请 -- 债券发行机构经济成分代码
    , 业务术语待申请 -- 债券发行机构企业规模代码
    , 业务术语待申请 -- 债券发行机构国民经济部门代码
    , 业务术语待申请 -- 债券发行机构统一社会信用编号
    , 业务术语待申请 -- 债券还本方式代码
    , 业务术语待申请 -- 债券还本类型代码
    , 业务术语待申请 -- 债券币种类型代码
    , 业务术语待申请 -- 债券首次付息日期
    , 业务术语待申请 -- 债券主体评级结果代码
    , 业务术语待申请 -- 债券信用评级结果代码
    , 业务术语待申请 -- 债券内部主体评级结果代码
    , 业务术语待申请 -- 债券债项评级结果代码
    , 业务术语待申请 -- 债券第三方评级结果代码
)
select
      T1.REQID as 业务术语待申请 -- 申请编号
    , T1.REQDATE as 业务术语待申请 -- 申请日期
    , T1.PRODUCT as 业务术语待申请 -- 产品类型
    , T1.ISSUEAMT as 业务术语待申请 -- 计划发行量(亿元)
    , T1.ISSUER as 业务术语待申请 -- 发行人
    , T1.ISSUEMODE as 业务术语待申请 -- 发行类型[PUBP：公募]
    , T1.ISSUETYPE as 业务术语待申请 -- 发行方式
    , T1.BASIS as INTACR_BASE_CD -- 计息基础
    , T1.NOTICEDATE as 业务术语待申请 -- 公告日
    , T1.ISSUEDATE as 业务术语待申请 -- 发行日
    , T1.PARVALUE as 业务术语待申请 -- 面值
    , T1.TERMSIZE as 业务术语待申请 -- 期限
    , T1.TERMUNIT as 业务术语待申请 -- 期限单位
    , T1.VDATE as 业务术语待申请 -- 起息日
    , T1.MDATE as 业务术语待申请 -- 到期日
    , T1.PAYDATE as 业务术语待申请 -- 缴款日
    , T1.REPAYDATE as 业务术语待申请 -- 兑付日
    , T1.PRATE as 业务术语待申请 -- 参考收益率（%）
    , T1.ISSUEPRICE as 业务术语待申请 -- 发行价格（元/百元面值）
    , T1.INTRULE as 业务术语待申请 -- 息票类型（固息，浮息，零息）
    , T1.RATECODE as 业务术语待申请 -- 基准利率
    , T1.SPREADRATE as 业务术语待申请 -- 浮动利差
    , T1.EXERCISEFALG as 业务术语待申请 -- 行权标志 [Y：是，N：否]
    , T1.EXECRCISETYPE as 业务术语待申请 -- 行权方式
    , T1.EXECDATE as 业务术语待申请 -- 行权日
    , T1.EXECRATE as 业务术语待申请 -- 行权利率
    , T1.RECACCTYPE as 业务术语待申请 -- 收款账户类型
    , T1.RECACCNAME as 业务术语待申请 -- 账户名称
    , T1.RECACC as 业务术语待申请 -- 账号
    , T1.RECBANK as 业务术语待申请 -- 开户行
    , T1.ACTUALISSUEAMT as 业务术语待申请 -- 实际发行量(亿元)
    , T1.ACTUALISSUEPRICE as 业务术语待申请 -- 实际发行价格(元/百元)
    , T1.ACTUALRATE as 业务术语待申请 -- 实际利率(%)
    , T1.INTAMT as 业务术语待申请 -- 应缴金额合计（元）
    , T1.ACTAMT as 业务术语待申请 -- 实缴金额合计（元）
    , T1.UNDERWRITER as 业务术语待申请 -- 承销商（简称）
    , T1.UNDERWRITERFEE as 业务术语待申请 -- 承销手续费
    , T1.REMARK as 业务术语待申请 -- 备注
    , T1.ORGID as TXN_ORG_NO -- 机构号
    , T1.RTRANUSER as OPERR_TELR_NO -- 业务员柜员号(申请)
    , T1.RTRANNAME as OPERR_NAME -- 业务员姓名(申请)
    , T1.EFFECTFLAG as 业务术语待申请 -- 有效标识 D 删除 A 新建 E 有效
    , T1.SECTYPE as 业务术语待申请 -- 债券类型
    , T1.TFATTYPE as 业务术语待申请 -- 处理类型
    , T1.NCDID as 业务术语待申请 -- 存单代码
    , T1.NCDNAME as 业务术语待申请 -- 存单简称
    , T1.FULLNAME as 业务术语待申请 -- 存单全称
    , T3.UNDERWRITER as 业务术语待申请 -- 主承销商全称
    , T3.ISSUER as 业务术语待申请 -- 发行机构代码
    , T3.ISSUEAREA as 业务术语待申请 -- 发行范围(银行间市场）
    , T3.VOUCHTYPE as 业务术语待申请 -- 担保方式
    , T3.SECTORS as 业务术语待申请 -- 行业类别
    , T3.TGORG as 业务术语待申请 -- 托管机构
    , T3.BUSNATURE as 业务术语待申请 -- 企业性质
    , T3.LAUNCHDATE as 业务术语待申请 -- 上市日期
    , T3.INDUSTRIESALIAS as 业务术语待申请 -- 发行人行业
    , T3.COMP_PROFESSION as 业务术语待申请 -- 发行人经济成分
    , T3.COMP_SCALE as 业务术语待申请 -- 发行人企业规模
    , T3.COMP_TYPE as 业务术语待申请 -- 发行人国民经济部门
    , T3.CREDITCODE as 业务术语待申请 -- 发行机构统一社会信用代码
    , T2.RETURNTYPE as 业务术语待申请 -- 还本方式
    , T2.RETURNSUBTYPE as 业务术语待申请 -- 还本类型
    , T2.SETTCCY as 业务术语待申请 -- 债券币种
    , T2.FIRSTPAYDATE as 业务术语待申请 -- 首次付息日
    , T4.S_RATINGRESULT as 业务术语待申请 -- 评级结果
    , T4.C_RATINGRESULT as 业务术语待申请 -- 评级结果
    , T4.I_RATINGRESULT as 业务术语待申请 -- 评级结果
    , T4.Z_RATINGRESULT as 业务术语待申请 -- 评级结果
    , T4.D_RATINGRESULT as 业务术语待申请 -- 评级结果
from
    ${ods_tbs_schema}.ODS_TBS_ISSUEB_DEAL_N as T1 -- 同业存单/债券发行交易表
    LEFT JOIN ${ods_tbs_schema}.ODS_TBS_TGT_SECINFO as T2 -- 债券基本信息
    on T1.NCDID=T2.SECID 
AND T2.effectflag ='E' --D 删除 A 新建 E 有效
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN ${ods_tbs_schema}.ODS_TBS_TGT_SECISSUE as T3 -- 债券发行信息
    on T2.SEQNO=T3.SEQNO 
AND T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN (select 
       t.secid
       ,max(case when t.ratingtype='S' then t.ratingresult else null end) as s_ratingresult
			 ,max(case when t.ratingtype='C' then t.ratingresult else null end) as c_ratingresult
			 ,max(case when t.ratingtype='I' then t.ratingresult else null end) as i_ratingresult
			 ,max(case when t.ratingtype='Z' then t.ratingresult else null end) as z_ratingresult
			 ,max(case when t.ratingtype='D' then t.ratingresult else null end) as d_ratingresult 
from(
        select 
						row_number() over(partition by secid
																						,ratingtype 
															 order by seqid desc
															 ) as row_number
						,secid
						,ratingtype
						,ratingresult 
        from  ${ods_tbs_schema}.ods_tbs_tgt_secrateinfo 
		WHERE  PT_DT='${process_date}' 
		AND DELETED='0'     --关联表TBS.TGT_SECRATEINFO，取评级结果，取最新一条数据,并按照评级类型拆分成多列
) t  
        WHERE  t.row_number=1
        group by t.secid) as T4 -- 债券评级信息
    on T1.secid=T4.secid  
where T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;

-- 删除所有临时表