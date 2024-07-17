-- 层次表名: 聚合层-标准财务报表利润信息聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s03_corp_fin_std_stat_prft
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S03_CORP_FIN_STD_STAT_PRFT
--     表中文名：标准财务报表利润信息聚合表
--     创建日期：2023-12-28 00:00:00
--     主键字段：CUST_INCD,REPORT_YM,AFLT_ORG,FIN_TABLE_TYPE
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含标准报表、金融报表、人行报表、瑞丰报表企业客户财务报表中利润相关信息、贷前评分应用字段以及贷后财务检查涉及的利润字段。同时包含客户基本信息、归属机构、注册行业币种、信贷员信息等公共字段信息。不包含简易报表企业客户。
--     更新记录：
--         2023-12-28 00:00:00 王穆军 NEW
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S03_CORP_FIN_STD_STAT_PRFT drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 数据组1

insert into table ${session}.S03_CORP_FIN_STD_STAT_PRFT(
      CUST_INCD -- 客户内码
    , REPORT_YM -- 报表年月
    , AFLT_ORG -- 归属机构编号
    , FIN_TABLE_TYPE -- 来源表类型代码
    , IF_AUDIT -- 报表审计标志
    , REPORT_TYPE -- 报表类型代码
    , CUST_NAME -- 客户名称
    , CUST_NO -- 客户号
    , REG_INDUS -- 注册行业代码
    , REG_CURR -- 注册币种代码
    , RV_FM_MOP_CHECK -- 主营业务收入
    , RV_FM_MOP_CYCA -- 主营业务收入 本年累计数
    , NS_FM_MOP_CHECK -- 主营业务收入净额
    , NS_FM_MOP_CYCA -- 主营业务收入净额 本年累计数
    , LS_CS_OF_MO_CHECK -- 主营业务成本
    , LS_CS_OF_MO_CYCA -- 主营业务成本 本年累计数
    , TAAD_OF_MOP_CHECK -- 主营业务税金及附加
    , TAAD_OF_MOP_CYCA -- 主营业务税金及附加 本年累计数
    , ICM_FM_MOP_CHECK -- 主营业务利润
    , ICM_FM_MOP_CYCA -- 主营业务利润 本年累计数
    , NS_FM_MOP -- 营业收入
    , ACCR_UEDTOTAL_CYCA -- 营业收入 本年累计数
    , LS_CS_OF_MO -- 营业成本
    , LS_CS_OF_MO_CYCA -- 营业成本 本年累计数
    , ADM_EXP_CHECK -- 管理费用
    , ADM_EXP_CYCA -- 管理费用 本年累计数
    , FNC_EXP_CHECK -- 财务费用
    , FNC_EXP_CYCA -- 财务费用 本年累计数
    , OPR_EXP -- 销售费用
    , ICD_CS_OF_EPS_CYCA -- 销售费用 本年累计数
    , RAD_EXP -- 研发费用
    , OPT_ICM_CHECK -- 营业利润
    , OPT_ICM_CYCA -- 营业利润 本年累计数
    , ADD_IVTMT_ICM_CHECK -- 投资收益
    , ADD_IVTMT_ICM_CYCA -- 投资收益 本年累计数
    , TTL_PFT_CHECK -- 利润总额
    , TTL_PFT_CYCA -- 利润总额 本年累计数
    , LS_ICM_TAX_EXP_CHECK -- 所得税
    , LS_ICM_TAX_EXP_CYCA -- 所得税 本年累计数
    , NT_PFT_CHECK -- 净利润
    , NT_PFT_CYCA -- 净利润 本年累计数
    , TAAD_OF_MOP -- 营业税金及附加
    , TAAD_OF_MOP_CYCA -- 营业税金及附加 本年累计数
    , ICD_EP_SL_CHECK -- 出口产品销售收入 
    , ICD_EP_SL_CYCA -- 出口产品销售收入 本年累计数
    , ICD_IMP_SL_CHECK -- 进口产品销售收入 
    , ICD_IMP_SL_CYCA -- 进口产品销售收入 本年累计数
    , LS_DST_AWS_CHECK -- 折扣与折让 
    , LS_DST_AWS_CYCA -- 折扣与折让 本年累计数
    , ICD_CS_OF_EPS_CHECK -- 出口产品销售成本  
    , ICD_CS_OF_EPS_CYCA -- 出口产品销售成本 本年累计数
    , OPR_EXP_CHECK -- 经营费用  
    , OPR_EXP_CYCA -- 经营费用 本年累计数
    , OTH_EXP_CHECK -- 其他费用  
    , OTH_EXP_CYCA -- 其他费用 本年累计数
    , ADD_DFD_ICM_CHECK -- 递延收益  
    , ADD_DFD_ICM_CYCA -- 递延收益 本年累计数
    , RVN_FM_AGAT_CHECK -- 代购代销收入  
    , RVN_FM_AGAT_CYCA -- 代购代销收入 本年累计数
    , OTH_ICM_CHECK -- 其他收入  
    , OTH_ICM_CYCA -- 其他收入 本年累计数
    , ADD_ICM_FM_OTOPT_CHECK -- 其他业务利润  
    , ADD_ICM_FM_OTOPT_CYCA -- 其他业务利润 本年累计数
    , LS_OPT_EXP_CHECK -- 营业费用  
    , LS_OPT_EXP_CYCA -- 营业费用 本年累计数
    , OTH_EXP2_CHECK -- 其他费用 
    , OTH_EXP2_CYCA -- 其他费用本年累计数
    , FTS_ICM_CHECK -- 期货收益  
    , FTS_ICM_CYCA -- 期货收益 本年累计数
    , SBS_ICM_CHECK -- 补贴收入  
    , SBS_ICM_CYCA -- 补贴收入 本年累计数
    , ICD_SFE_IN_DBS_CHECK -- 补贴前亏损的企业补贴收入  
    , ICD_SFE_IN_DBS_CYCA -- 补贴前亏损的企业补贴收入 本年累计数
    , NOPT_ICM_CHECK -- 营业外收入  
    , NOPT_ICM_CYCA -- 营业外收入 本年累计数
    , ICD_NIOND_OF_FXAST_CHECK -- 处置固定资产净收益  
    , ICD_NIOND_OF_FXAST_CYCA -- 处置固定资产净收益 本年累计数
    , NCS_DL_ICM_CHECK -- 非货币性交易收益  
    , NCS_DL_ICM_CYCA -- 非货币性交易收益 本年累计数
    , ICM_ON_SL_IAST_CHECK -- 出售无形资产收益  
    , ICM_ON_SL_IAST_CYCA -- 出售无形资产收益 本年累计数
    , NET_AMT_ICM_CHECK -- 罚款净收入  
    , NET_AMT_ICM_CYCA -- 罚款净收入 本年累计数
    , OTH_ICM2_CHECK -- 其他收入 
    , OTH_ICM2_CYCA -- 其他收入本年累计数
    , UTCSB_TO_RPIPY_CHECK -- 用以前年度含量工资节余弥补利润  
    , UTCSB_TO_RPIPY_CYCA -- 用以前年度含量工资节余弥补利润 本年累计数
    , LESS_NB_EXP_CHECK -- 营业外支出  
    , LESS_NB_EXP_CYCA -- 营业外支出 本年累计数
    , NI_ON_DOFA_CHECK -- 处置固定资产净损失  
    , NI_ON_DOFA_CYCA -- 处置固定资产净损失 本年累计数
    , LS_ON_AGMT_CHECK -- 债务重组损失  
    , LS_ON_AGMT_CYCA -- 债务重组损失 本年累计数
    , FINE_PAY_CHECK -- 罚款支出  
    , FINE_PAY_CYCA -- 罚款支出 本年累计数
    , DNT_PAY_CHECK -- 捐赠支出  
    , DNT_PAY_CYCA -- 捐赠支出 本年累计数
    , OTH_PAY_CHECK -- 其他支出  
    , OTH_PAY_CYCA -- 其他支出 本年累计数
    , ICD_TFC_WCS_CHECK -- 结转的含量工资包干节余  
    , ICD_TFC_WCS_CYCA -- 结转的含量工资包干节余 本年累计数
    , ADD_AJP_LPY_CHECK -- 以前年度损益调整  
    , ADD_AJP_LPY_CYCA -- 以前年度损益调整 本年累计数
    , MRT_ITR_CHECK -- 少数股东损益  
    , MRT_ITR_CYCA -- 少数股东损益 本年累计数
    , ADD_UFD_IVMT_LS_CHECK -- 未确认的投资损失  
    , ADD_UFD_IVMT_LS_CYCA -- 未确认的投资损失 本年累计数
    , ADD_RAB_OF_TY_CHECK -- 年初未分配利润  
    , ADD_RAB_OF_TY_CYCA -- 年初未分配利润 本年累计数
    , RDL_WTH_PFS_CHECK -- 盈余公积补亏  
    , RDL_WTH_PFS_CYCA -- 盈余公积补亏 本年累计数
    , OTH_ADJ_FTS_CHECK -- 其他调整因素  
    , OTH_ADJ_FTS_CYCA -- 其他调整因素 本年累计数
    , DST_PFT_CHECK -- 可供分配的利润  
    , DST_PFT_CYCA -- 可供分配的利润 本年累计数
    , LS_SRP_CHECK -- 单项留用的利润  
    , LS_SRP_CYCA -- 单项留用的利润 本年累计数
    , SPT_CRT_CPT_CHECK -- 补充流动资本  
    , SPT_CRT_CPT_CYCA -- 补充流动资本 本年累计数
    , DRS_SP_RSV_CHECK -- 提取法定盈余公积  
    , DRS_SP_RSV_CYCA -- 提取法定盈余公积 本年累计数
    , PBL_WLF_FD_CHECK -- 提取法定公益金  
    , PBL_WLF_FD_CYCA -- 提取法定公益金 本年累计数
    , WDS_AND_WKB_WFD_CHECK -- 提取职工奖励及福利基金  
    , WDS_AND_WKB_WFD_CYCA -- 提取职工奖励及福利基金 本年累计数
    , WDL_RSV_FD_CHECK -- 提取储备基金  
    , WDL_RSV_FD_CYCA -- 提取储备基金 本年累计数
    , WDL_RSV_BSN_EXP_CHECK -- 提取企业发展基金  
    , WDL_RSV_BSN_EXP_CYCA -- 提取企业发展基金 本年累计数
    , PFT_CPT_RT_RMT_CHECK -- 利润归还投资  
    , PFT_CPT_RT_RMT_CYCA -- 利润归还投资 本年累计数
    , PFT_AVB_OWN_DST_CHECK -- 可供投资者分配的利润  
    , PFT_AVB_OWN_DST_CYCA -- 可供投资者分配的利润 本年累计数
    , LS_APR_PRF_SDVD_CHECK -- 应付优先股股利  
    , LS_APR_PRF_SDVD_CYCA -- 应付优先股股利 本年累计数
    , APR_DSR_SPL_RSV_CHECK -- 提取任意盈余公积  
    , APR_DSR_SPL_RSV_CYCA -- 提取任意盈余公积 本年累计数
    , APR_ODN_SDVD_CHECK -- 应付普通股股利  
    , APR_ODN_SDVD_CYCA -- 应付普通股股利 本年累计数
    , TSF_ODN_SDVD_TPC_CHECK -- 转作资本的普通股股利  
    , TSF_ODN_SDVD_TPC_CYCA -- 转作资本的普通股股利 本年累计数
    , RTD_PFT_APR_CHECK -- 未分配利润  
    , RTD_PFT_APR_CYCA -- 未分配利润 本年累计数
    , ICD_DR_PBT_NY_CHECK -- 应由以后年度税前利润弥补的亏损  
    , ICD_DR_PBT_NY_CYCA -- 应由以后年度税前利润弥补的亏损 本年累计数
    , CREATE_TM -- 创建时间
    , CREATE_LOAN_OFFICER_NO -- 创建客户经理编号
    , CREATE_LOAN_OFFICER_NAME -- 创建客户经理姓名
    , CREATE_ORG -- 创建机构编号
    , FINAL_MATN_LOAN_OFFICER_NO -- 最后维护客户经理编号
    , FINAL_MATN_LOAN_OFFICER_NAME -- 最后维护客户经理姓名
    , FINAL_MATN_TM -- 最后维护时间
    , FINAL_MATN_ORG -- 最后维护机构编号
    , PT_DT -- 数据日期
)
select
      T1.CUST_ISN as CUST_INCD -- 客户内码
    , T1.RPT_DT as REPORT_YM -- 报表年月
    , T1.BEL_TO_ORG as AFLT_ORG -- 归属机构
    , '1' as FIN_TABLE_TYPE -- None
    , T2.RPT_AUDIT_IND as IF_AUDIT -- 报表是否审计、客户内码、报表年月、归属机构
    , T1.RPT_DT_TYP as REPORT_TYPE -- 报表类型,年报，季报，月报
    , T3.CUST_NAM as CUST_NAME -- 客户名称、客户内码
    , T1.CUST_ID as CUST_NO -- 客户号
    , T4.BEL_TO_IDY as REG_INDUS -- 注册行业、客户内码、归属机构
    , T5.REG_CCY_ID as REG_CURR -- 注册登记币种、客户内码、归属机构
    , T1.RV_FM_MOP_CHECK as RV_FM_MOP_CHECK -- 主营业务收入 核查数
    , T1.RV_FM_MOP_CYCA as RV_FM_MOP_CYCA -- 主营业务收入 本年累计数
    , T1.NS_FM_MOP_CHECK as NS_FM_MOP_CHECK -- 主营业务收入净额 核查数
    , T1.NS_FM_MOP_CYCA as NS_FM_MOP_CYCA -- 主营业务收入净额 本年累计数
    , T1.LS_CS_OF_MO_CHECK as LS_CS_OF_MO_CHECK -- 主营业务成本 核查数
    , T1.LS_CS_OF_MO_CYCA as LS_CS_OF_MO_CYCA -- 主营业务成本 本年累计数
    , T1.TAAD_OF_MOP_CHECK as TAAD_OF_MOP_CHECK -- 主营业务税金及附加 核查数
    , T1.TAAD_OF_MOP_CYCA as TAAD_OF_MOP_CYCA -- 主营业务税金及附加 本年累计数
    , T1.ICM_FM_MOP_CHECK as ICM_FM_MOP_CHECK -- 主营业务利润 核查数
    , T1.ICM_FM_MOP_CYCA as ICM_FM_MOP_CYCA -- 主营业务利润 本年累计数
    , '' as NS_FM_MOP -- None
    , '' as ACCR_UEDTOTAL_CYCA -- None
    , '' as LS_CS_OF_MO -- None
    , '' as LS_CS_OF_MO_CYCA -- None
    , T1.ADM_EXP_CHECK as ADM_EXP_CHECK -- 管理费用 核查数
    , T1.ADM_EXP_CYCA as ADM_EXP_CYCA -- 管理费用 本年累计数
    , T1.FNC_EXP_CHECK as FNC_EXP_CHECK -- 财务费用 核查数
    , T1.FNC_EXP_CYCA as FNC_EXP_CYCA -- 财务费用 本年累计数
    , '' as OPR_EXP -- None
    , '' as ICD_CS_OF_EPS_CYCA -- None
    , '' as RAD_EXP -- None
    , T1.OPT_ICM_CHECK as OPT_ICM_CHECK -- 营业利润 核查数
    , T1.OPT_ICM_CYCA as OPT_ICM_CYCA -- 营业利润 本年累计数
    , T1.ADD_IVTMT_ICM_CHECK as ADD_IVTMT_ICM_CHECK -- 投资收益 核查数
    , T1.ADD_IVTMT_ICM_CYCA as ADD_IVTMT_ICM_CYCA -- 投资收益 本年累计数
    , T1.TTL_PFT_CHECK as TTL_PFT_CHECK -- 利润总额 核查数
    , T1.TTL_PFT_CYCA as TTL_PFT_CYCA -- 利润总额 本年累计数
    , T1.LS_ICM_TAX_EXP_CHECK as LS_ICM_TAX_EXP_CHECK -- 所得税 核查数
    , T1.LS_ICM_TAX_EXP_CYCA as LS_ICM_TAX_EXP_CYCA -- 所得税 本年累计数
    , T1.NT_PFT_CHECK as NT_PFT_CHECK -- 净利润 核查数
    , T1.NT_PFT_CYCA as NT_PFT_CYCA -- 净利润 本年累计数
    , '' as TAAD_OF_MOP -- None
    , '' as TAAD_OF_MOP_CYCA -- None
    , T1.ICD_EP_SL_CHECK as ICD_EP_SL_CHECK -- 其中：出口产品销售收入 核查数
    , T1.ICD_EP_SL_CYCA as ICD_EP_SL_CYCA -- 其中：出口产品销售收入 本年累计数
    , T1.ICD_IMP_SL_CHECK as ICD_IMP_SL_CHECK -- 进口产品销售收入 核查数
    , T1.ICD_IMP_SL_CYCA as ICD_IMP_SL_CYCA -- 进口产品销售收入 本年累计数
    , T1.LS_DST_AWS_CHECK as LS_DST_AWS_CHECK -- 减：折扣与折让 核查数
    , T1.LS_DST_AWS_CYCA as LS_DST_AWS_CYCA -- 减：折扣与折让 本年累计数
    , T1.ICD_CS_OF_EPS_CHECK as ICD_CS_OF_EPS_CHECK -- 其中：出口产品销售成本 核查数
    , T1.ICD_CS_OF_EPS_CYCA as ICD_CS_OF_EPS_CYCA -- 其中：出口产品销售成本 本年累计数
    , T1.OPR_EXP_CHECK as OPR_EXP_CHECK -- 经营费用 核查数
    , T1.OPR_EXP_CYCA as OPR_EXP_CYCA -- 经营费用 本年累计数
    , T1.OTH_EXP_CHECK as OTH_EXP_CHECK -- 其他费用 核查数
    , T1.OTH_EXP_CYCA as OTH_EXP_CYCA -- 其他费用 本年累计数
    , T1.ADD_DFD_ICM_CHECK as ADD_DFD_ICM_CHECK -- 加：递延收益 核查数
    , T1.ADD_DFD_ICM_CYCA as ADD_DFD_ICM_CYCA -- 加：递延收益 本年累计数
    , T1.RVN_FM_AGAT_CHECK as RVN_FM_AGAT_CHECK -- 代购代销收入 核查数
    , T1.RVN_FM_AGAT_CYCA as RVN_FM_AGAT_CYCA -- 代购代销收入 本年累计数
    , T1.OTH_ICM_CHECK as OTH_ICM_CHECK -- 其他收入 核查数
    , T1.OTH_ICM_CYCA as OTH_ICM_CYCA -- 其他收入 本年累计数
    , T1.ADD_ICM_FM_OTOPT_CHECK as ADD_ICM_FM_OTOPT_CHECK -- 加：其他业务利润 核查数
    , T1.ADD_ICM_FM_OTOPT_CYCA as ADD_ICM_FM_OTOPT_CYCA -- 加：其他业务利润 本年累计数
    , T1.LS_OPT_EXP_CHECK as LS_OPT_EXP_CHECK -- 减：营业费用 核查数
    , T1.LS_OPT_EXP_CYCA as LS_OPT_EXP_CYCA -- 减：营业费用 本年累计数
    , T1.OTH_EXP2_CHECK as OTH_EXP2_CHECK -- 其他费用核查数
    , T1.OTH_EXP2_CYCA as OTH_EXP2_CYCA -- 其他费用本年累计数
    , T1.FTS_ICM_CHECK as FTS_ICM_CHECK -- 期货收益 核查数
    , T1.FTS_ICM_CYCA as FTS_ICM_CYCA -- 期货收益 本年累计数
    , T1.SBS_ICM_CHECK as SBS_ICM_CHECK -- 补贴收入 核查数
    , T1.SBS_ICM_CYCA as SBS_ICM_CYCA -- 补贴收入 本年累计数
    , T1.ICD_SFE_IN_DBS_CHECK as ICD_SFE_IN_DBS_CHECK -- 其中：补贴前亏损的企业补贴收入 核查数
    , T1.ICD_SFE_IN_DBS_CYCA as ICD_SFE_IN_DBS_CYCA -- 其中：补贴前亏损的企业补贴收入 本年累计数
    , T1.NOPT_ICM_CHECK as NOPT_ICM_CHECK -- 营业外收入 核查数
    , T1.NOPT_ICM_CYCA as NOPT_ICM_CYCA -- 营业外收入 本年累计数
    , T1.ICD_NIOND_OF_FXAST_CHECK as ICD_NIOND_OF_FXAST_CHECK -- 处置固定资产净收益 核查数
    , T1.ICD_NIOND_OF_FXAST_CYCA as ICD_NIOND_OF_FXAST_CYCA -- 处置固定资产净收益 本年累计数
    , T1.NCS_DL_ICM_CHECK as NCS_DL_ICM_CHECK -- 非货币性交易收益 核查数
    , T1.NCS_DL_ICM_CYCA as NCS_DL_ICM_CYCA -- 非货币性交易收益 本年累计数
    , T1.ICM_ON_SL_IAST_CHECK as ICM_ON_SL_IAST_CHECK -- 出售无形资产收益 核查数
    , T1.ICM_ON_SL_IAST_CYCA as ICM_ON_SL_IAST_CYCA -- 出售无形资产收益 本年累计数
    , T1.NET_AMT_ICM_CHECK as NET_AMT_ICM_CHECK -- 罚款净收入 核查数
    , T1.NET_AMT_ICM_CYCA as NET_AMT_ICM_CYCA -- 罚款净收入 本年累计数
    , T1.OTH_ICM2_CHECK as OTH_ICM2_CHECK -- 其他收入核查数
    , T1.OTH_ICM2_CYCA as OTH_ICM2_CYCA -- 其他收入本年累计数
    , T1.UTCSB_TO_RPIPY_CHECK as UTCSB_TO_RPIPY_CHECK -- 用以前年度含量工资节余弥补利润 核查数
    , T1.UTCSB_TO_RPIPY_CYCA as UTCSB_TO_RPIPY_CYCA -- 用以前年度含量工资节余弥补利润 本年累计数
    , T1.LESS_NB_EXP_CHECK as LESS_NB_EXP_CHECK -- 减：营业外支出 核查数
    , T1.LESS_NB_EXP_CYCA as LESS_NB_EXP_CYCA -- 减：营业外支出 本年累计数
    , T1.NI_ON_DOFA_CHECK as NI_ON_DOFA_CHECK -- 其中：处置固定资产净损失 核查数
    , T1.NI_ON_DOFA_CYCA as NI_ON_DOFA_CYCA -- 其中：处置固定资产净损失 本年累计数
    , T1.LS_ON_AGMT_CHECK as LS_ON_AGMT_CHECK -- 债务重组损失 核查数
    , T1.LS_ON_AGMT_CYCA as LS_ON_AGMT_CYCA -- 债务重组损失 本年累计数
    , T1.FINE_PAY_CHECK as FINE_PAY_CHECK -- 罚款支出 核查数
    , T1.FINE_PAY_CYCA as FINE_PAY_CYCA -- 罚款支出 本年累计数
    , T1.DNT_PAY_CHECK as DNT_PAY_CHECK -- 捐赠支出 核查数
    , T1.DNT_PAY_CYCA as DNT_PAY_CYCA -- 捐赠支出 本年累计数
    , T1.OTH_PAY_CHECK as OTH_PAY_CHECK -- 其他支出 核查数
    , T1.OTH_PAY_CYCA as OTH_PAY_CYCA -- 其他支出 本年累计数
    , T1.ICD_TFC_WCS_CHECK as ICD_TFC_WCS_CHECK -- 结转的含量工资包干节余 核查数
    , T1.ICD_TFC_WCS_CYCA as ICD_TFC_WCS_CYCA -- 结转的含量工资包干节余 本年累计数
    , T1.ADD_AJP_LPY_CHECK as ADD_AJP_LPY_CHECK -- 加：以前年度损益调整 核查数
    , T1.ADD_AJP_LPY_CYCA as ADD_AJP_LPY_CYCA -- 加：以前年度损益调整 本年累计数
    , T1.MRT_ITR_CHECK as MRT_ITR_CHECK -- 少数股东损益 核查数
    , T1.MRT_ITR_CYCA as MRT_ITR_CYCA -- 少数股东损益 本年累计数
    , T1.ADD_UFD_IVMT_LS_CHECK as ADD_UFD_IVMT_LS_CHECK -- 加：未确认的投资损失 核查数
    , T1.ADD_UFD_IVMT_LS_CYCA as ADD_UFD_IVMT_LS_CYCA -- 加：未确认的投资损失 本年累计数
    , T1.ADD_RAB_OF_TY_CHECK as ADD_RAB_OF_TY_CHECK -- 加：年初未分配利润 核查数
    , T1.ADD_RAB_OF_TY_CYCA as ADD_RAB_OF_TY_CYCA -- 加：年初未分配利润 本年累计数
    , T1.RDL_WTH_PFS_CHECK as RDL_WTH_PFS_CHECK -- 盈余公积补亏 核查数
    , T1.RDL_WTH_PFS_CYCA as RDL_WTH_PFS_CYCA -- 盈余公积补亏 本年累计数
    , T1.OTH_ADJ_FTS_CHECK as OTH_ADJ_FTS_CHECK -- 其他调整因素 核查数
    , T1.OTH_ADJ_FTS_CYCA as OTH_ADJ_FTS_CYCA -- 其他调整因素 本年累计数
    , T1.DST_PFT_CHECK as DST_PFT_CHECK -- 可供分配的利润 核查数
    , T1.DST_PFT_CYCA as DST_PFT_CYCA -- 可供分配的利润 本年累计数
    , T1.LS_SRP_CHECK as LS_SRP_CHECK -- 减：单项留用的利润 核查数
    , T1.LS_SRP_CYCA as LS_SRP_CYCA -- 减：单项留用的利润 本年累计数
    , T1.SPT_CRT_CPT_CHECK as SPT_CRT_CPT_CHECK -- 补充流动资本 核查数
    , T1.SPT_CRT_CPT_CYCA as SPT_CRT_CPT_CYCA -- 补充流动资本 本年累计数
    , T1.DRS_SP_RSV_CHECK as DRS_SP_RSV_CHECK -- 提取法定盈余公积 核查数
    , T1.DRS_SP_RSV_CYCA as DRS_SP_RSV_CYCA -- 提取法定盈余公积 本年累计数
    , T1.PBL_WLF_FD_CHECK as PBL_WLF_FD_CHECK -- 提取法定公益金 核查数
    , T1.PBL_WLF_FD_CYCA as PBL_WLF_FD_CYCA -- 提取法定公益金 本年累计数
    , T1.WDS_AND_WKB_WFD_CHECK as WDS_AND_WKB_WFD_CHECK -- 提取职工奖励及福利基金 核查数
    , T1.WDS_AND_WKB_WFD_CYCA as WDS_AND_WKB_WFD_CYCA -- 提取职工奖励及福利基金 本年累计数
    , T1.WDL_RSV_FD_CHECK as WDL_RSV_FD_CHECK -- 提取储备基金 核查数
    , T1.WDL_RSV_FD_CYCA as WDL_RSV_FD_CYCA -- 提取储备基金 本年累计数
    , T1.WDL_RSV_BSN_EXP_CHECK as WDL_RSV_BSN_EXP_CHECK -- 提取企业发展基金 核查数
    , T1.WDL_RSV_BSN_EXP_CYCA as WDL_RSV_BSN_EXP_CYCA -- 提取企业发展基金 本年累计数
    , T1.PFT_CPT_RT_RMT_CHECK as PFT_CPT_RT_RMT_CHECK -- 利润归还投资 核查数
    , T1.PFT_CPT_RT_RMT_CYCA as PFT_CPT_RT_RMT_CYCA -- 利润归还投资 本年累计数
    , T1.PFT_AVB_OWN_DST_CHECK as PFT_AVB_OWN_DST_CHECK -- 可供投资者分配的利润 核查数
    , T1.PFT_AVB_OWN_DST_CYCA as PFT_AVB_OWN_DST_CYCA -- 可供投资者分配的利润 本年累计数
    , T1.LS_APR_PRF_SDVD_CHECK as LS_APR_PRF_SDVD_CHECK -- 减：应付优先股股利 核查数
    , T1.LS_APR_PRF_SDVD_CYCA as LS_APR_PRF_SDVD_CYCA -- 减：应付优先股股利 本年累计数
    , T1.APR_DSR_SPL_RSV_CHECK as APR_DSR_SPL_RSV_CHECK -- 提取任意盈余公积 核查数
    , T1.APR_DSR_SPL_RSV_CYCA as APR_DSR_SPL_RSV_CYCA -- 提取任意盈余公积 本年累计数
    , T1.APR_ODN_SDVD_CHECK as APR_ODN_SDVD_CHECK -- 应付普通股股利 核查数
    , T1.APR_ODN_SDVD_CYCA as APR_ODN_SDVD_CYCA -- 应付普通股股利 本年累计数
    , T1.TSF_ODN_SDVD_TPC_CHECK as TSF_ODN_SDVD_TPC_CHECK -- 转作资本的普通股股利 核查数
    , T1.TSF_ODN_SDVD_TPC_CYCA as TSF_ODN_SDVD_TPC_CYCA -- 转作资本的普通股股利 本年累计数
    , T1.RTD_PFT_APR_CHECK as RTD_PFT_APR_CHECK -- 未分配利润 核查数
    , T1.RTD_PFT_APR_CYCA as RTD_PFT_APR_CYCA -- 未分配利润 本年累计数
    , T1.ICD_DR_PBT_NY_CHECK as ICD_DR_PBT_NY_CHECK -- 其中：应由以后年度税前利润弥补的亏损 核查数
    , T1.ICD_DR_PBT_NY_CYCA as ICD_DR_PBT_NY_CYCA -- 其中：应由以后年度税前利润弥补的亏损 本年累计数
    , T1.CRT_TM as CREATE_TM -- 创建时间
    , T1.CRT_LOAN_OFF as CREATE_LOAN_OFFICER_NO -- 创建客户经理
    , T6.NAME as CREATE_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.CRT_ORGAN as CREATE_ORG -- 创建机构
    , T1.LAST_LOAN_OFF as FINAL_MATN_LOAN_OFFICER_NO -- 最后维护客户经理
    , T7.NAME as FINAL_MATN_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.LAST_TM as FINAL_MATN_TM -- 最后维护时间
    , T1.LAST_ORGAN as FINAL_MATN_ORG -- 最后维护机构
    , '${PROCESS_DATE}' as PT_DT -- None
from
    ${ODS_XDZX_SCHEMA}.EN_LG_ICM_STMT as T1 -- 大中型企业损益表
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_FNC_DOC_INFO as T2 -- 财务资料公共信息表
    on T1.CUST_ISN=T2.CUST_ISN
AND T1.RPT_DT=T2.RPT_DT
AND T1.BEL_TO_ORG=T2.BEL_TO_ORG
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.CREDIT_CUST_BASE_INFO as T3 -- 信贷客户基础信息表
    on T1.CUST_ISN=T3.CUST_ISN
AND   T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_CUST_BASE_INFO as T4 -- 客户公共基础信息表
    on T1.CUST_ISN=T4.CUST_ISN
AND T1.BEL_TO_ORG=T4.BEL_TO_ORG
AND   T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.EN_CUST_EXPD_ECIF as T5 -- 对公客户扩展信息表
    on T1.CUST_ISN=T5.CUST_ISN
AND T1.BEL_TO_ORG=T5.BEL_TO_ORG
AND   T5.PT_DT='${process_date}' 
AND T5.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.USERS as T6 -- 用户表
    on T1.CRT_LOAN_OFF = T6.STAFFER_NO
AND   T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.USERS as T7 -- 用户表
    on T1.LAST_LOAN_OFF = T7.STAFFER_NO
AND   T7.PT_DT='${process_date}' 
AND T7.DELETED='0' 
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第2组）==============
-- 数据组2

insert into table ${session}.S03_CORP_FIN_STD_STAT_PRFT(
      CUST_INCD -- 客户内码
    , REPORT_YM -- 报表年月
    , AFLT_ORG -- 归属机构编号
    , FIN_TABLE_TYPE -- 来源表类型代码
    , IF_AUDIT -- 报表审计标志
    , REPORT_TYPE -- 报表类型代码
    , CUST_NAME -- 客户名称
    , CUST_NO -- 客户号
    , REG_INDUS -- 注册行业代码
    , REG_CURR -- 注册币种代码
    , RV_FM_MOP_CHECK -- 主营业务收入
    , RV_FM_MOP_CYCA -- 主营业务收入 本年累计数
    , NS_FM_MOP_CHECK -- 主营业务收入净额
    , NS_FM_MOP_CYCA -- 主营业务收入净额 本年累计数
    , LS_CS_OF_MO_CHECK -- 主营业务成本
    , LS_CS_OF_MO_CYCA -- 主营业务成本 本年累计数
    , TAAD_OF_MOP_CHECK -- 主营业务税金及附加
    , TAAD_OF_MOP_CYCA -- 主营业务税金及附加 本年累计数
    , ICM_FM_MOP_CHECK -- 主营业务利润
    , ICM_FM_MOP_CYCA -- 主营业务利润 本年累计数
    , NS_FM_MOP -- 营业收入
    , ACCR_UEDTOTAL_CYCA -- 营业收入 本年累计数
    , LS_CS_OF_MO -- 营业成本
    , LS_CS_OF_MO_CYCA -- 营业成本 本年累计数
    , ADM_EXP_CHECK -- 管理费用
    , ADM_EXP_CYCA -- 管理费用 本年累计数
    , FNC_EXP_CHECK -- 财务费用
    , FNC_EXP_CYCA -- 财务费用 本年累计数
    , OPR_EXP -- 销售费用
    , ICD_CS_OF_EPS_CYCA -- 销售费用 本年累计数
    , RAD_EXP -- 研发费用
    , OPT_ICM_CHECK -- 营业利润
    , OPT_ICM_CYCA -- 营业利润 本年累计数
    , ADD_IVTMT_ICM_CHECK -- 投资收益
    , ADD_IVTMT_ICM_CYCA -- 投资收益 本年累计数
    , TTL_PFT_CHECK -- 利润总额
    , TTL_PFT_CYCA -- 利润总额 本年累计数
    , LS_ICM_TAX_EXP_CHECK -- 所得税
    , LS_ICM_TAX_EXP_CYCA -- 所得税 本年累计数
    , NT_PFT_CHECK -- 净利润
    , NT_PFT_CYCA -- 净利润 本年累计数
    , TAAD_OF_MOP -- 营业税金及附加
    , TAAD_OF_MOP_CYCA -- 营业税金及附加 本年累计数
    , ICD_EP_SL_CHECK -- 出口产品销售收入 
    , ICD_EP_SL_CYCA -- 出口产品销售收入 本年累计数
    , ICD_IMP_SL_CHECK -- 进口产品销售收入 
    , ICD_IMP_SL_CYCA -- 进口产品销售收入 本年累计数
    , LS_DST_AWS_CHECK -- 折扣与折让 
    , LS_DST_AWS_CYCA -- 折扣与折让 本年累计数
    , ICD_CS_OF_EPS_CHECK -- 出口产品销售成本  
    , ICD_CS_OF_EPS_CYCA -- 出口产品销售成本 本年累计数
    , OPR_EXP_CHECK -- 经营费用  
    , OPR_EXP_CYCA -- 经营费用 本年累计数
    , OTH_EXP_CHECK -- 其他费用  
    , OTH_EXP_CYCA -- 其他费用 本年累计数
    , ADD_DFD_ICM_CHECK -- 递延收益  
    , ADD_DFD_ICM_CYCA -- 递延收益 本年累计数
    , RVN_FM_AGAT_CHECK -- 代购代销收入  
    , RVN_FM_AGAT_CYCA -- 代购代销收入 本年累计数
    , OTH_ICM_CHECK -- 其他收入  
    , OTH_ICM_CYCA -- 其他收入 本年累计数
    , ADD_ICM_FM_OTOPT_CHECK -- 其他业务利润  
    , ADD_ICM_FM_OTOPT_CYCA -- 其他业务利润 本年累计数
    , LS_OPT_EXP_CHECK -- 营业费用  
    , LS_OPT_EXP_CYCA -- 营业费用 本年累计数
    , OTH_EXP2_CHECK -- 其他费用 
    , OTH_EXP2_CYCA -- 其他费用本年累计数
    , FTS_ICM_CHECK -- 期货收益  
    , FTS_ICM_CYCA -- 期货收益 本年累计数
    , SBS_ICM_CHECK -- 补贴收入  
    , SBS_ICM_CYCA -- 补贴收入 本年累计数
    , ICD_SFE_IN_DBS_CHECK -- 补贴前亏损的企业补贴收入  
    , ICD_SFE_IN_DBS_CYCA -- 补贴前亏损的企业补贴收入 本年累计数
    , NOPT_ICM_CHECK -- 营业外收入  
    , NOPT_ICM_CYCA -- 营业外收入 本年累计数
    , ICD_NIOND_OF_FXAST_CHECK -- 处置固定资产净收益  
    , ICD_NIOND_OF_FXAST_CYCA -- 处置固定资产净收益 本年累计数
    , NCS_DL_ICM_CHECK -- 非货币性交易收益  
    , NCS_DL_ICM_CYCA -- 非货币性交易收益 本年累计数
    , ICM_ON_SL_IAST_CHECK -- 出售无形资产收益  
    , ICM_ON_SL_IAST_CYCA -- 出售无形资产收益 本年累计数
    , NET_AMT_ICM_CHECK -- 罚款净收入  
    , NET_AMT_ICM_CYCA -- 罚款净收入 本年累计数
    , OTH_ICM2_CHECK -- 其他收入 
    , OTH_ICM2_CYCA -- 其他收入本年累计数
    , UTCSB_TO_RPIPY_CHECK -- 用以前年度含量工资节余弥补利润  
    , UTCSB_TO_RPIPY_CYCA -- 用以前年度含量工资节余弥补利润 本年累计数
    , LESS_NB_EXP_CHECK -- 营业外支出  
    , LESS_NB_EXP_CYCA -- 营业外支出 本年累计数
    , NI_ON_DOFA_CHECK -- 处置固定资产净损失  
    , NI_ON_DOFA_CYCA -- 处置固定资产净损失 本年累计数
    , LS_ON_AGMT_CHECK -- 债务重组损失  
    , LS_ON_AGMT_CYCA -- 债务重组损失 本年累计数
    , FINE_PAY_CHECK -- 罚款支出  
    , FINE_PAY_CYCA -- 罚款支出 本年累计数
    , DNT_PAY_CHECK -- 捐赠支出  
    , DNT_PAY_CYCA -- 捐赠支出 本年累计数
    , OTH_PAY_CHECK -- 其他支出  
    , OTH_PAY_CYCA -- 其他支出 本年累计数
    , ICD_TFC_WCS_CHECK -- 结转的含量工资包干节余  
    , ICD_TFC_WCS_CYCA -- 结转的含量工资包干节余 本年累计数
    , ADD_AJP_LPY_CHECK -- 以前年度损益调整  
    , ADD_AJP_LPY_CYCA -- 以前年度损益调整 本年累计数
    , MRT_ITR_CHECK -- 少数股东损益  
    , MRT_ITR_CYCA -- 少数股东损益 本年累计数
    , ADD_UFD_IVMT_LS_CHECK -- 未确认的投资损失  
    , ADD_UFD_IVMT_LS_CYCA -- 未确认的投资损失 本年累计数
    , ADD_RAB_OF_TY_CHECK -- 年初未分配利润  
    , ADD_RAB_OF_TY_CYCA -- 年初未分配利润 本年累计数
    , RDL_WTH_PFS_CHECK -- 盈余公积补亏  
    , RDL_WTH_PFS_CYCA -- 盈余公积补亏 本年累计数
    , OTH_ADJ_FTS_CHECK -- 其他调整因素  
    , OTH_ADJ_FTS_CYCA -- 其他调整因素 本年累计数
    , DST_PFT_CHECK -- 可供分配的利润  
    , DST_PFT_CYCA -- 可供分配的利润 本年累计数
    , LS_SRP_CHECK -- 单项留用的利润  
    , LS_SRP_CYCA -- 单项留用的利润 本年累计数
    , SPT_CRT_CPT_CHECK -- 补充流动资本  
    , SPT_CRT_CPT_CYCA -- 补充流动资本 本年累计数
    , DRS_SP_RSV_CHECK -- 提取法定盈余公积  
    , DRS_SP_RSV_CYCA -- 提取法定盈余公积 本年累计数
    , PBL_WLF_FD_CHECK -- 提取法定公益金  
    , PBL_WLF_FD_CYCA -- 提取法定公益金 本年累计数
    , WDS_AND_WKB_WFD_CHECK -- 提取职工奖励及福利基金  
    , WDS_AND_WKB_WFD_CYCA -- 提取职工奖励及福利基金 本年累计数
    , WDL_RSV_FD_CHECK -- 提取储备基金  
    , WDL_RSV_FD_CYCA -- 提取储备基金 本年累计数
    , WDL_RSV_BSN_EXP_CHECK -- 提取企业发展基金  
    , WDL_RSV_BSN_EXP_CYCA -- 提取企业发展基金 本年累计数
    , PFT_CPT_RT_RMT_CHECK -- 利润归还投资  
    , PFT_CPT_RT_RMT_CYCA -- 利润归还投资 本年累计数
    , PFT_AVB_OWN_DST_CHECK -- 可供投资者分配的利润  
    , PFT_AVB_OWN_DST_CYCA -- 可供投资者分配的利润 本年累计数
    , LS_APR_PRF_SDVD_CHECK -- 应付优先股股利  
    , LS_APR_PRF_SDVD_CYCA -- 应付优先股股利 本年累计数
    , APR_DSR_SPL_RSV_CHECK -- 提取任意盈余公积  
    , APR_DSR_SPL_RSV_CYCA -- 提取任意盈余公积 本年累计数
    , APR_ODN_SDVD_CHECK -- 应付普通股股利  
    , APR_ODN_SDVD_CYCA -- 应付普通股股利 本年累计数
    , TSF_ODN_SDVD_TPC_CHECK -- 转作资本的普通股股利  
    , TSF_ODN_SDVD_TPC_CYCA -- 转作资本的普通股股利 本年累计数
    , RTD_PFT_APR_CHECK -- 未分配利润  
    , RTD_PFT_APR_CYCA -- 未分配利润 本年累计数
    , ICD_DR_PBT_NY_CHECK -- 应由以后年度税前利润弥补的亏损  
    , ICD_DR_PBT_NY_CYCA -- 应由以后年度税前利润弥补的亏损 本年累计数
    , CREATE_TM -- 创建时间
    , CREATE_LOAN_OFFICER_NO -- 创建客户经理编号
    , CREATE_LOAN_OFFICER_NAME -- 创建客户经理姓名
    , CREATE_ORG -- 创建机构编号
    , FINAL_MATN_LOAN_OFFICER_NO -- 最后维护客户经理编号
    , FINAL_MATN_LOAN_OFFICER_NAME -- 最后维护客户经理姓名
    , FINAL_MATN_TM -- 最后维护时间
    , FINAL_MATN_ORG -- 最后维护机构编号
    , PT_DT -- 数据日期
)
select
      T1.CUST_ISN as CUST_INCD -- 客户内码
    , T1.RPT_DT as REPORT_YM -- 报表年月
    , T1.BEL_TO_ORG as AFLT_ORG -- 归属机构
    , '2' as FIN_TABLE_TYPE -- None
    , T2.RPT_AUDIT_IND as IF_AUDIT -- 报表是否审计、客户内码、报表年月、归属机构
    , T1.RPT_DT_TYP as REPORT_TYPE -- 报表类型,年报，季报，月报
    , T3.CUST_NAM as CUST_NAME -- 客户名称、客户内码
    , T1.CUST_ID as CUST_NO -- 客户号
    , T4.BEL_TO_IDY as REG_INDUS -- 注册行业、客户内码、归属机构
    , T5.REG_CCY_ID as REG_CURR -- 注册登记币种、客户内码、归属机构
    , '' as RV_FM_MOP_CHECK -- None
    , '' as RV_FM_MOP_CYCA -- None
    , '' as NS_FM_MOP_CHECK -- None
    , '' as NS_FM_MOP_CYCA -- None
    , '' as LS_CS_OF_MO_CHECK -- None
    , '' as LS_CS_OF_MO_CYCA -- None
    , '' as TAAD_OF_MOP_CHECK -- None
    , '' as TAAD_OF_MOP_CYCA -- None
    , '' as ICM_FM_MOP_CHECK -- None
    , '' as ICM_FM_MOP_CYCA -- None
    , T1.NS_FM_MOP as NS_FM_MOP -- 营业收入
    , '' as ACCR_UEDTOTAL_CYCA -- None
    , T1.LS_CS_OF_MO as LS_CS_OF_MO -- 营业成本
    , '' as LS_CS_OF_MO_CYCA -- None
    , T1.ADM_EXP_CHECK as ADM_EXP_CHECK -- 管理费用
    , T1.FNC_EXP as FNC_EXP_CHECK -- 财务费用
    , '' as FNC_EXP_CYCA -- None
    , T1.OPR_EXP as OPR_EXP -- 销售费用
    , '' as ICD_CS_OF_EPS_CYCA -- None
    , T1.RAD_EXP as RAD_EXP -- 研发费用
    , T1.OPR_PROFIT as OPT_ICM_CHECK -- 营业利润
    , '' as OPT_ICM_CYCA -- None
    , T1.ADD_IVTMT_ICM as ADD_IVTMT_ICM_CHECK -- 投资收益
    , '' as ADD_IVTMT_ICM_CYCA -- None
    , T1.TTL_PFT as TTL_PFT_CHECK -- 利润总额
    , '' as TTL_PFT_CYCA -- None
    , T1.LS_ICM_TAX_EXP as LS_ICM_TAX_EXP_CHECK -- 减：所得税费用
    , '' as LS_ICM_TAX_EXP_CYCA -- None
    , T1.NT_PFT as NT_PFT_CHECK -- 净利润
    , '' as NT_PFT_CYCA -- None
    , T1.TAAD_OF_MOP as TAAD_OF_MOP -- 税金及附加
    , '' as TAAD_OF_MOP_CYCA -- None
    , '' as ICD_EP_SL_CHECK -- None
    , '' as ICD_EP_SL_CYCA -- None
    , '' as ICD_IMP_SL_CHECK -- None
    , '' as ICD_IMP_SL_CYCA -- None
    , '' as LS_DST_AWS_CHECK -- None
    , '' as LS_DST_AWS_CYCA -- None
    , '' as ICD_CS_OF_EPS_CHECK -- None
    , '' as ICD_CS_OF_EPS_CYCA -- None
    , '' as OPR_EXP_CHECK -- None
    , '' as OPR_EXP_CYCA -- None
    , '' as OTH_EXP_CHECK -- None
    , '' as OTH_EXP_CYCA -- None
    , '' as ADD_DFD_ICM_CHECK -- None
    , '' as ADD_DFD_ICM_CYCA -- None
    , '' as RVN_FM_AGAT_CHECK -- None
    , '' as RVN_FM_AGAT_CYCA -- None
    , '' as OTH_ICM_CHECK -- None
    , '' as OTH_ICM_CYCA -- None
    , '' as ADD_ICM_FM_OTOPT_CHECK -- None
    , '' as ADD_ICM_FM_OTOPT_CYCA -- None
    , '' as LS_OPT_EXP_CHECK -- None
    , '' as LS_OPT_EXP_CYCA -- None
    , '' as OTH_EXP2_CHECK -- None
    , '' as OTH_EXP2_CYCA -- None
    , '' as FTS_ICM_CHECK -- None
    , '' as FTS_ICM_CYCA -- None
    , '' as SBS_ICM_CHECK -- None
    , '' as SBS_ICM_CYCA -- None
    , '' as ICD_SFE_IN_DBS_CHECK -- None
    , '' as ICD_SFE_IN_DBS_CYCA -- None
    , '' as NOPT_ICM_CHECK -- None
    , '' as NOPT_ICM_CYCA -- None
    , '' as ICD_NIOND_OF_FXAST_CHECK -- None
    , '' as ICD_NIOND_OF_FXAST_CYCA -- None
    , '' as NCS_DL_ICM_CHECK -- None
    , '' as NCS_DL_ICM_CYCA -- None
    , '' as ICM_ON_SL_IAST_CHECK -- None
    , '' as ICM_ON_SL_IAST_CYCA -- None
    , '' as NET_AMT_ICM_CHECK -- None
    , '' as NET_AMT_ICM_CYCA -- None
    , '' as OTH_ICM2_CHECK -- None
    , '' as OTH_ICM2_CYCA -- None
    , '' as UTCSB_TO_RPIPY_CHECK -- None
    , '' as UTCSB_TO_RPIPY_CYCA -- None
    , '' as LESS_NB_EXP_CHECK -- None
    , '' as LESS_NB_EXP_CYCA -- None
    , '' as NI_ON_DOFA_CHECK -- None
    , '' as NI_ON_DOFA_CYCA -- None
    , '' as LS_ON_AGMT_CHECK -- None
    , '' as LS_ON_AGMT_CYCA -- None
    , '' as FINE_PAY_CHECK -- None
    , '' as FINE_PAY_CYCA -- None
    , '' as DNT_PAY_CHECK -- None
    , '' as DNT_PAY_CYCA -- None
    , '' as OTH_PAY_CHECK -- None
    , '' as OTH_PAY_CYCA -- None
    , '' as ICD_TFC_WCS_CHECK -- None
    , '' as ICD_TFC_WCS_CYCA -- None
    , '' as ADD_AJP_LPY_CHECK -- None
    , '' as ADD_AJP_LPY_CYCA -- None
    , '' as MRT_ITR_CHECK -- None
    , '' as MRT_ITR_CYCA -- None
    , '' as ADD_UFD_IVMT_LS_CHECK -- None
    , '' as ADD_UFD_IVMT_LS_CYCA -- None
    , '' as ADD_RAB_OF_TY_CHECK -- None
    , '' as ADD_RAB_OF_TY_CYCA -- None
    , '' as RDL_WTH_PFS_CHECK -- None
    , '' as RDL_WTH_PFS_CYCA -- None
    , '' as OTH_ADJ_FTS_CHECK -- None
    , '' as OTH_ADJ_FTS_CYCA -- None
    , '' as DST_PFT_CHECK -- None
    , '' as DST_PFT_CYCA -- None
    , '' as LS_SRP_CHECK -- None
    , '' as LS_SRP_CYCA -- None
    , '' as SPT_CRT_CPT_CHECK -- None
    , '' as SPT_CRT_CPT_CYCA -- None
    , '' as DRS_SP_RSV_CHECK -- None
    , '' as DRS_SP_RSV_CYCA -- None
    , '' as PBL_WLF_FD_CHECK -- None
    , '' as PBL_WLF_FD_CYCA -- None
    , '' as WDS_AND_WKB_WFD_CHECK -- None
    , '' as WDS_AND_WKB_WFD_CYCA -- None
    , '' as WDL_RSV_FD_CHECK -- None
    , '' as WDL_RSV_FD_CYCA -- None
    , '' as WDL_RSV_BSN_EXP_CHECK -- None
    , '' as WDL_RSV_BSN_EXP_CYCA -- None
    , '' as PFT_CPT_RT_RMT_CHECK -- None
    , '' as PFT_CPT_RT_RMT_CYCA -- None
    , '' as PFT_AVB_OWN_DST_CHECK -- None
    , '' as PFT_AVB_OWN_DST_CYCA -- None
    , '' as LS_APR_PRF_SDVD_CHECK -- None
    , '' as LS_APR_PRF_SDVD_CYCA -- None
    , '' as APR_DSR_SPL_RSV_CHECK -- None
    , '' as APR_DSR_SPL_RSV_CYCA -- None
    , '' as APR_ODN_SDVD_CHECK -- None
    , '' as APR_ODN_SDVD_CYCA -- None
    , '' as TSF_ODN_SDVD_TPC_CHECK -- None
    , '' as TSF_ODN_SDVD_TPC_CYCA -- None
    , '' as RTD_PFT_APR_CHECK -- None
    , '' as RTD_PFT_APR_CYCA -- None
    , '' as ICD_DR_PBT_NY_CHECK -- None
    , '' as ICD_DR_PBT_NY_CYCA -- None
    , T1.CRT_TM as CREATE_TM -- 创建时间
    , T1.CRT_LOAN_OFF as CREATE_LOAN_OFFICER_NO -- 创建客户经理
    , T6.NAME as CREATE_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.CRT_ORGAN as CREATE_ORG -- 创建机构
    , T1.LAST_LOAN_OFF as FINAL_MATN_LOAN_OFFICER_NO -- 最后维护客户经理
    , T7.NAME as FINAL_MATN_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.LAST_TM as FINAL_MATN_TM -- 最后维护时间
    , T1.LAST_ORGAN as FINAL_MATN_ORG -- 最后维护机构
    , '${PROCESS_DATE}' as PT_DT -- None
from
    ${ODS_XDZX_SCHEMA}.CB_EN_LG_ICM_STMT  as T1 -- 人行大中型企业利润表  
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_FNC_DOC_INFO as T2 -- 财务资料公共信息表
    on T1.CUST_ISN=T2.CUST_ISN
AND T1.RPT_DT=T2.RPT_DT
AND T1.BEL_TO_ORG=T2.BEL_TO_ORG
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.CREDIT_CUST_BASE_INFO as T3 -- 信贷客户基础信息表
    on T1.CUST_ISN=T3.CUST_ISN
AND   T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_CUST_BASE_INFO as T4 -- 客户公共基础信息表
    on T1.CUST_ISN=T4.CUST_ISN
AND T1.BEL_TO_ORG=T4.BEL_TO_ORG
AND   T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.EN_CUST_EXPD_ECIF as T5 -- 对公客户扩展信息表
    on T1.CUST_ISN=T5.CUST_ISN
AND T1.BEL_TO_ORG=T5.BEL_TO_ORG
AND   T5.PT_DT='${process_date}' 
AND T5.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.USERS as T6 -- 用户表
    on T1.CRT_LOAN_OFF = T6.STAFFER_NO
AND   T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.USERS as T7 -- 用户表
    on T1.LAST_LOAN_OFF = T7.STAFFER_NO
AND   T7.PT_DT='${process_date}' 
AND T7.DELETED='0' 
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第3组）==============
-- 数据组3

insert into table ${session}.S03_CORP_FIN_STD_STAT_PRFT(
      CUST_INCD -- 客户内码
    , REPORT_YM -- 报表年月
    , AFLT_ORG -- 归属机构编号
    , FIN_TABLE_TYPE -- 来源表类型代码
    , IF_AUDIT -- 报表审计标志
    , REPORT_TYPE -- 报表类型代码
    , CUST_NAME -- 客户名称
    , CUST_NO -- 客户号
    , REG_INDUS -- 注册行业代码
    , REG_CURR -- 注册币种代码
    , RV_FM_MOP_CHECK -- 主营业务收入
    , RV_FM_MOP_CYCA -- 主营业务收入 本年累计数
    , NS_FM_MOP_CHECK -- 主营业务收入净额
    , NS_FM_MOP_CYCA -- 主营业务收入净额 本年累计数
    , LS_CS_OF_MO_CHECK -- 主营业务成本
    , LS_CS_OF_MO_CYCA -- 主营业务成本 本年累计数
    , TAAD_OF_MOP_CHECK -- 主营业务税金及附加
    , TAAD_OF_MOP_CYCA -- 主营业务税金及附加 本年累计数
    , ICM_FM_MOP_CHECK -- 主营业务利润
    , ICM_FM_MOP_CYCA -- 主营业务利润 本年累计数
    , NS_FM_MOP -- 营业收入
    , ACCR_UEDTOTAL_CYCA -- 营业收入 本年累计数
    , LS_CS_OF_MO -- 营业成本
    , LS_CS_OF_MO_CYCA -- 营业成本 本年累计数
    , ADM_EXP_CHECK -- 管理费用
    , ADM_EXP_CYCA -- 管理费用 本年累计数
    , FNC_EXP_CHECK -- 财务费用
    , FNC_EXP_CYCA -- 财务费用 本年累计数
    , OPR_EXP -- 销售费用
    , ICD_CS_OF_EPS_CYCA -- 销售费用 本年累计数
    , RAD_EXP -- 研发费用
    , OPT_ICM_CHECK -- 营业利润
    , OPT_ICM_CYCA -- 营业利润 本年累计数
    , ADD_IVTMT_ICM_CHECK -- 投资收益
    , ADD_IVTMT_ICM_CYCA -- 投资收益 本年累计数
    , TTL_PFT_CHECK -- 利润总额
    , TTL_PFT_CYCA -- 利润总额 本年累计数
    , LS_ICM_TAX_EXP_CHECK -- 所得税
    , LS_ICM_TAX_EXP_CYCA -- 所得税 本年累计数
    , NT_PFT_CHECK -- 净利润
    , NT_PFT_CYCA -- 净利润 本年累计数
    , TAAD_OF_MOP -- 营业税金及附加
    , TAAD_OF_MOP_CYCA -- 营业税金及附加 本年累计数
    , ICD_EP_SL_CHECK -- 出口产品销售收入 
    , ICD_EP_SL_CYCA -- 出口产品销售收入 本年累计数
    , ICD_IMP_SL_CHECK -- 进口产品销售收入 
    , ICD_IMP_SL_CYCA -- 进口产品销售收入 本年累计数
    , LS_DST_AWS_CHECK -- 折扣与折让 
    , LS_DST_AWS_CYCA -- 折扣与折让 本年累计数
    , ICD_CS_OF_EPS_CHECK -- 出口产品销售成本  
    , ICD_CS_OF_EPS_CYCA -- 出口产品销售成本 本年累计数
    , OPR_EXP_CHECK -- 经营费用  
    , OPR_EXP_CYCA -- 经营费用 本年累计数
    , OTH_EXP_CHECK -- 其他费用  
    , OTH_EXP_CYCA -- 其他费用 本年累计数
    , ADD_DFD_ICM_CHECK -- 递延收益  
    , ADD_DFD_ICM_CYCA -- 递延收益 本年累计数
    , RVN_FM_AGAT_CHECK -- 代购代销收入  
    , RVN_FM_AGAT_CYCA -- 代购代销收入 本年累计数
    , OTH_ICM_CHECK -- 其他收入  
    , OTH_ICM_CYCA -- 其他收入 本年累计数
    , ADD_ICM_FM_OTOPT_CHECK -- 其他业务利润  
    , ADD_ICM_FM_OTOPT_CYCA -- 其他业务利润 本年累计数
    , LS_OPT_EXP_CHECK -- 营业费用  
    , LS_OPT_EXP_CYCA -- 营业费用 本年累计数
    , OTH_EXP2_CHECK -- 其他费用 
    , OTH_EXP2_CYCA -- 其他费用本年累计数
    , FTS_ICM_CHECK -- 期货收益  
    , FTS_ICM_CYCA -- 期货收益 本年累计数
    , SBS_ICM_CHECK -- 补贴收入  
    , SBS_ICM_CYCA -- 补贴收入 本年累计数
    , ICD_SFE_IN_DBS_CHECK -- 补贴前亏损的企业补贴收入  
    , ICD_SFE_IN_DBS_CYCA -- 补贴前亏损的企业补贴收入 本年累计数
    , NOPT_ICM_CHECK -- 营业外收入  
    , NOPT_ICM_CYCA -- 营业外收入 本年累计数
    , ICD_NIOND_OF_FXAST_CHECK -- 处置固定资产净收益  
    , ICD_NIOND_OF_FXAST_CYCA -- 处置固定资产净收益 本年累计数
    , NCS_DL_ICM_CHECK -- 非货币性交易收益  
    , NCS_DL_ICM_CYCA -- 非货币性交易收益 本年累计数
    , ICM_ON_SL_IAST_CHECK -- 出售无形资产收益  
    , ICM_ON_SL_IAST_CYCA -- 出售无形资产收益 本年累计数
    , NET_AMT_ICM_CHECK -- 罚款净收入  
    , NET_AMT_ICM_CYCA -- 罚款净收入 本年累计数
    , OTH_ICM2_CHECK -- 其他收入 
    , OTH_ICM2_CYCA -- 其他收入本年累计数
    , UTCSB_TO_RPIPY_CHECK -- 用以前年度含量工资节余弥补利润  
    , UTCSB_TO_RPIPY_CYCA -- 用以前年度含量工资节余弥补利润 本年累计数
    , LESS_NB_EXP_CHECK -- 营业外支出  
    , LESS_NB_EXP_CYCA -- 营业外支出 本年累计数
    , NI_ON_DOFA_CHECK -- 处置固定资产净损失  
    , NI_ON_DOFA_CYCA -- 处置固定资产净损失 本年累计数
    , LS_ON_AGMT_CHECK -- 债务重组损失  
    , LS_ON_AGMT_CYCA -- 债务重组损失 本年累计数
    , FINE_PAY_CHECK -- 罚款支出  
    , FINE_PAY_CYCA -- 罚款支出 本年累计数
    , DNT_PAY_CHECK -- 捐赠支出  
    , DNT_PAY_CYCA -- 捐赠支出 本年累计数
    , OTH_PAY_CHECK -- 其他支出  
    , OTH_PAY_CYCA -- 其他支出 本年累计数
    , ICD_TFC_WCS_CHECK -- 结转的含量工资包干节余  
    , ICD_TFC_WCS_CYCA -- 结转的含量工资包干节余 本年累计数
    , ADD_AJP_LPY_CHECK -- 以前年度损益调整  
    , ADD_AJP_LPY_CYCA -- 以前年度损益调整 本年累计数
    , MRT_ITR_CHECK -- 少数股东损益  
    , MRT_ITR_CYCA -- 少数股东损益 本年累计数
    , ADD_UFD_IVMT_LS_CHECK -- 未确认的投资损失  
    , ADD_UFD_IVMT_LS_CYCA -- 未确认的投资损失 本年累计数
    , ADD_RAB_OF_TY_CHECK -- 年初未分配利润  
    , ADD_RAB_OF_TY_CYCA -- 年初未分配利润 本年累计数
    , RDL_WTH_PFS_CHECK -- 盈余公积补亏  
    , RDL_WTH_PFS_CYCA -- 盈余公积补亏 本年累计数
    , OTH_ADJ_FTS_CHECK -- 其他调整因素  
    , OTH_ADJ_FTS_CYCA -- 其他调整因素 本年累计数
    , DST_PFT_CHECK -- 可供分配的利润  
    , DST_PFT_CYCA -- 可供分配的利润 本年累计数
    , LS_SRP_CHECK -- 单项留用的利润  
    , LS_SRP_CYCA -- 单项留用的利润 本年累计数
    , SPT_CRT_CPT_CHECK -- 补充流动资本  
    , SPT_CRT_CPT_CYCA -- 补充流动资本 本年累计数
    , DRS_SP_RSV_CHECK -- 提取法定盈余公积  
    , DRS_SP_RSV_CYCA -- 提取法定盈余公积 本年累计数
    , PBL_WLF_FD_CHECK -- 提取法定公益金  
    , PBL_WLF_FD_CYCA -- 提取法定公益金 本年累计数
    , WDS_AND_WKB_WFD_CHECK -- 提取职工奖励及福利基金  
    , WDS_AND_WKB_WFD_CYCA -- 提取职工奖励及福利基金 本年累计数
    , WDL_RSV_FD_CHECK -- 提取储备基金  
    , WDL_RSV_FD_CYCA -- 提取储备基金 本年累计数
    , WDL_RSV_BSN_EXP_CHECK -- 提取企业发展基金  
    , WDL_RSV_BSN_EXP_CYCA -- 提取企业发展基金 本年累计数
    , PFT_CPT_RT_RMT_CHECK -- 利润归还投资  
    , PFT_CPT_RT_RMT_CYCA -- 利润归还投资 本年累计数
    , PFT_AVB_OWN_DST_CHECK -- 可供投资者分配的利润  
    , PFT_AVB_OWN_DST_CYCA -- 可供投资者分配的利润 本年累计数
    , LS_APR_PRF_SDVD_CHECK -- 应付优先股股利  
    , LS_APR_PRF_SDVD_CYCA -- 应付优先股股利 本年累计数
    , APR_DSR_SPL_RSV_CHECK -- 提取任意盈余公积  
    , APR_DSR_SPL_RSV_CYCA -- 提取任意盈余公积 本年累计数
    , APR_ODN_SDVD_CHECK -- 应付普通股股利  
    , APR_ODN_SDVD_CYCA -- 应付普通股股利 本年累计数
    , TSF_ODN_SDVD_TPC_CHECK -- 转作资本的普通股股利  
    , TSF_ODN_SDVD_TPC_CYCA -- 转作资本的普通股股利 本年累计数
    , RTD_PFT_APR_CHECK -- 未分配利润  
    , RTD_PFT_APR_CYCA -- 未分配利润 本年累计数
    , ICD_DR_PBT_NY_CHECK -- 应由以后年度税前利润弥补的亏损  
    , ICD_DR_PBT_NY_CYCA -- 应由以后年度税前利润弥补的亏损 本年累计数
    , CREATE_TM -- 创建时间
    , CREATE_LOAN_OFFICER_NO -- 创建客户经理编号
    , CREATE_LOAN_OFFICER_NAME -- 创建客户经理姓名
    , CREATE_ORG -- 创建机构编号
    , FINAL_MATN_LOAN_OFFICER_NO -- 最后维护客户经理编号
    , FINAL_MATN_LOAN_OFFICER_NAME -- 最后维护客户经理姓名
    , FINAL_MATN_TM -- 最后维护时间
    , FINAL_MATN_ORG -- 最后维护机构编号
    , PT_DT -- 数据日期
)
select
      T1.CUST_ISN as CUST_INCD -- 客户内码
    , T1.RPT_DT as REPORT_YM -- 报表年月
    , T1.BEL_TO_ORG as AFLT_ORG -- 归属机构
    , '3' as FIN_TABLE_TYPE -- None
    , T2.RPT_AUDIT_IND as IF_AUDIT -- 报表是否审计、客户内码、报表年月、归属机构
    , T1.RPT_DT_TYP as REPORT_TYPE -- 报表类型,年报，季报，月报
    , T3.CUST_NAM as CUST_NAME -- 客户名称、客户内码
    , T1.CUST_ID as CUST_NO -- 客户号
    , T4.BEL_TO_IDY as REG_INDUS -- 注册行业、客户内码、归属机构
    , T5.REG_CCY_ID as REG_CURR -- 注册登记币种、客户内码、归属机构
    , '' as RV_FM_MOP_CHECK -- None
    , '' as RV_FM_MOP_CYCA -- None
    , '' as NS_FM_MOP_CHECK -- None
    , '' as NS_FM_MOP_CYCA -- None
    , '' as LS_CS_OF_MO_CHECK -- None
    , '' as LS_CS_OF_MO_CYCA -- None
    , '' as TAAD_OF_MOP_CHECK -- None
    , '' as TAAD_OF_MOP_CYCA -- None
    , '' as ICM_FM_MOP_CHECK -- None
    , '' as ICM_FM_MOP_CYCA -- None
    , T1.YYSRHCS as NS_FM_MOP -- 营业收入 核查数
    , '' as ACCR_UEDTOTAL_CYCA -- None
    , '' as LS_CS_OF_MO -- None
    , '' as LS_CS_OF_MO_CYCA -- None
    , '' as ADM_EXP_CHECK -- None
    , '' as ADM_EXP_CYCA -- None
    , '' as FNC_EXP_CHECK -- None
    , '' as FNC_EXP_CYCA -- None
    , '' as OPR_EXP -- None
    , '' as ICD_CS_OF_EPS_CYCA -- None
    , '' as RAD_EXP -- None
    , T1.YYLR as OPT_ICM_CHECK -- 营业利润
    , '' as OPT_ICM_CYCA -- None
    , T1.TZSY as ADD_IVTMT_ICM_CHECK -- 投资收益
    , '' as ADD_IVTMT_ICM_CYCA -- None
    , T1.LRZE as TTL_PFT_CHECK -- 利润总额
    , '' as TTL_PFT_CYCA -- None
    , T1.SDSFY as LS_ICM_TAX_EXP_CHECK -- 所得税费用
    , '' as LS_ICM_TAX_EXP_CYCA -- None
    , T1.JLR as NT_PFT_CHECK -- 净利润
    , '' as NT_PFT_CYCA -- None
    , T1.YYSJ as TAAD_OF_MOP -- 营业税金及附加
    , '' as TAAD_OF_MOP_CYCA -- None
    , '' as ICD_EP_SL_CHECK -- None
    , '' as ICD_EP_SL_CYCA -- None
    , '' as ICD_IMP_SL_CHECK -- None
    , '' as ICD_IMP_SL_CYCA -- None
    , '' as LS_DST_AWS_CHECK -- None
    , '' as LS_DST_AWS_CYCA -- None
    , '' as ICD_CS_OF_EPS_CHECK -- None
    , '' as ICD_CS_OF_EPS_CYCA -- None
    , '' as OPR_EXP_CHECK -- None
    , '' as OPR_EXP_CYCA -- None
    , '' as OTH_EXP_CHECK -- None
    , '' as OTH_EXP_CYCA -- None
    , '' as ADD_DFD_ICM_CHECK -- None
    , '' as ADD_DFD_ICM_CYCA -- None
    , '' as RVN_FM_AGAT_CHECK -- None
    , '' as RVN_FM_AGAT_CYCA -- None
    , '' as OTH_ICM_CHECK -- None
    , '' as OTH_ICM_CYCA -- None
    , '' as ADD_ICM_FM_OTOPT_CHECK -- None
    , '' as ADD_ICM_FM_OTOPT_CYCA -- None
    , '' as LS_OPT_EXP_CHECK -- None
    , '' as LS_OPT_EXP_CYCA -- None
    , '' as OTH_EXP2_CHECK -- None
    , '' as OTH_EXP2_CYCA -- None
    , '' as FTS_ICM_CHECK -- None
    , '' as FTS_ICM_CYCA -- None
    , '' as SBS_ICM_CHECK -- None
    , '' as SBS_ICM_CYCA -- None
    , '' as ICD_SFE_IN_DBS_CHECK -- None
    , '' as ICD_SFE_IN_DBS_CYCA -- None
    , '' as NOPT_ICM_CHECK -- None
    , '' as NOPT_ICM_CYCA -- None
    , '' as ICD_NIOND_OF_FXAST_CHECK -- None
    , '' as ICD_NIOND_OF_FXAST_CYCA -- None
    , '' as NCS_DL_ICM_CHECK -- None
    , '' as NCS_DL_ICM_CYCA -- None
    , '' as ICM_ON_SL_IAST_CHECK -- None
    , '' as ICM_ON_SL_IAST_CYCA -- None
    , '' as NET_AMT_ICM_CHECK -- None
    , '' as NET_AMT_ICM_CYCA -- None
    , '' as OTH_ICM2_CHECK -- None
    , '' as OTH_ICM2_CYCA -- None
    , '' as UTCSB_TO_RPIPY_CHECK -- None
    , '' as UTCSB_TO_RPIPY_CYCA -- None
    , '' as LESS_NB_EXP_CHECK -- None
    , '' as LESS_NB_EXP_CYCA -- None
    , '' as NI_ON_DOFA_CHECK -- None
    , '' as NI_ON_DOFA_CYCA -- None
    , '' as LS_ON_AGMT_CHECK -- None
    , '' as LS_ON_AGMT_CYCA -- None
    , '' as FINE_PAY_CHECK -- None
    , '' as FINE_PAY_CYCA -- None
    , '' as DNT_PAY_CHECK -- None
    , '' as DNT_PAY_CYCA -- None
    , '' as OTH_PAY_CHECK -- None
    , '' as OTH_PAY_CYCA -- None
    , '' as ICD_TFC_WCS_CHECK -- None
    , '' as ICD_TFC_WCS_CYCA -- None
    , '' as ADD_AJP_LPY_CHECK -- None
    , '' as ADD_AJP_LPY_CYCA -- None
    , '' as MRT_ITR_CHECK -- None
    , '' as MRT_ITR_CYCA -- None
    , '' as ADD_UFD_IVMT_LS_CHECK -- None
    , '' as ADD_UFD_IVMT_LS_CYCA -- None
    , '' as ADD_RAB_OF_TY_CHECK -- None
    , '' as ADD_RAB_OF_TY_CYCA -- None
    , '' as RDL_WTH_PFS_CHECK -- None
    , '' as RDL_WTH_PFS_CYCA -- None
    , '' as OTH_ADJ_FTS_CHECK -- None
    , '' as OTH_ADJ_FTS_CYCA -- None
    , '' as DST_PFT_CHECK -- None
    , '' as DST_PFT_CYCA -- None
    , '' as LS_SRP_CHECK -- None
    , '' as LS_SRP_CYCA -- None
    , '' as SPT_CRT_CPT_CHECK -- None
    , '' as SPT_CRT_CPT_CYCA -- None
    , '' as DRS_SP_RSV_CHECK -- None
    , '' as DRS_SP_RSV_CYCA -- None
    , '' as PBL_WLF_FD_CHECK -- None
    , '' as PBL_WLF_FD_CYCA -- None
    , '' as WDS_AND_WKB_WFD_CHECK -- None
    , '' as WDS_AND_WKB_WFD_CYCA -- None
    , '' as WDL_RSV_FD_CHECK -- None
    , '' as WDL_RSV_FD_CYCA -- None
    , '' as WDL_RSV_BSN_EXP_CHECK -- None
    , '' as WDL_RSV_BSN_EXP_CYCA -- None
    , '' as PFT_CPT_RT_RMT_CHECK -- None
    , '' as PFT_CPT_RT_RMT_CYCA -- None
    , '' as PFT_AVB_OWN_DST_CHECK -- None
    , '' as PFT_AVB_OWN_DST_CYCA -- None
    , '' as LS_APR_PRF_SDVD_CHECK -- None
    , '' as LS_APR_PRF_SDVD_CYCA -- None
    , '' as APR_DSR_SPL_RSV_CHECK -- None
    , '' as APR_DSR_SPL_RSV_CYCA -- None
    , '' as APR_ODN_SDVD_CHECK -- None
    , '' as APR_ODN_SDVD_CYCA -- None
    , '' as TSF_ODN_SDVD_TPC_CHECK -- None
    , '' as TSF_ODN_SDVD_TPC_CYCA -- None
    , '' as RTD_PFT_APR_CHECK -- None
    , '' as RTD_PFT_APR_CYCA -- None
    , '' as ICD_DR_PBT_NY_CHECK -- None
    , '' as ICD_DR_PBT_NY_CYCA -- None
    , T1.CRT_TM as CREATE_TM -- 创建时间
    , T1.CRT_LOAN_OFF as CREATE_LOAN_OFFICER_NO -- 创建客户经理
    , T6.NAME as CREATE_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.CRT_ORGAN as CREATE_ORG -- 创建机构
    , T1.LAST_LOAN_OFF as FINAL_MATN_LOAN_OFFICER_NO -- 最后维护客户经理
    , T7.NAME as FINAL_MATN_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.LAST_TM as FINAL_MATN_TM -- 最后维护时间
    , T1.LAST_ORGAN as FINAL_MATN_ORG -- 最后维护机构
    , '${PROCESS_DATE}' as PT_DT -- None
from
    ${ODS_XDZX_SCHEMA}.EN_FNC_ICM_STMT as T1 -- 人行大中型企业利润表  
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_FNC_DOC_INFO as T2 -- 财务资料公共信息表
    on T1.CUST_ISN=T2.CUST_ISN
AND T1.RPT_DT=T2.RPT_DT
AND T1.BEL_TO_ORG=T2.BEL_TO_ORG
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.CREDIT_CUST_BASE_INFO as T3 -- 信贷客户基础信息表
    on T1.CUST_ISN=T3.CUST_ISN
AND   T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_CUST_BASE_INFO as T4 -- 客户公共基础信息表
    on T1.CUST_ISN=T4.CUST_ISN
AND T1.BEL_TO_ORG=T4.BEL_TO_ORG
AND   T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.EN_CUST_EXPD_ECIF as T5 -- 对公客户扩展信息表
    on T1.CUST_ISN=T5.CUST_ISN
AND T1.BEL_TO_ORG=T5.BEL_TO_ORG
AND   T5.PT_DT='${process_date}' 
AND T5.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.USERS as T6 -- 用户表
    on T1.CRT_LOAN_OFF = T6.STAFFER_NO
AND   T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.USERS as T7 -- 用户表
    on T1.LAST_LOAN_OFF = T7.STAFFER_NO
AND   T7.PT_DT='${process_date}' 
AND T7.DELETED='0' 
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第4组）==============
-- 数据组4

insert into table ${session}.S03_CORP_FIN_STD_STAT_PRFT(
      CUST_INCD -- 客户内码
    , REPORT_YM -- 报表年月
    , AFLT_ORG -- 归属机构编号
    , FIN_TABLE_TYPE -- 来源表类型代码
    , IF_AUDIT -- 报表审计标志
    , REPORT_TYPE -- 报表类型代码
    , CUST_NAME -- 客户名称
    , CUST_NO -- 客户号
    , REG_INDUS -- 注册行业代码
    , REG_CURR -- 注册币种代码
    , RV_FM_MOP_CHECK -- 主营业务收入
    , RV_FM_MOP_CYCA -- 主营业务收入 本年累计数
    , NS_FM_MOP_CHECK -- 主营业务收入净额
    , NS_FM_MOP_CYCA -- 主营业务收入净额 本年累计数
    , LS_CS_OF_MO_CHECK -- 主营业务成本
    , LS_CS_OF_MO_CYCA -- 主营业务成本 本年累计数
    , TAAD_OF_MOP_CHECK -- 主营业务税金及附加
    , TAAD_OF_MOP_CYCA -- 主营业务税金及附加 本年累计数
    , ICM_FM_MOP_CHECK -- 主营业务利润
    , ICM_FM_MOP_CYCA -- 主营业务利润 本年累计数
    , NS_FM_MOP -- 营业收入
    , ACCR_UEDTOTAL_CYCA -- 营业收入 本年累计数
    , LS_CS_OF_MO -- 营业成本
    , LS_CS_OF_MO_CYCA -- 营业成本 本年累计数
    , ADM_EXP_CHECK -- 管理费用
    , ADM_EXP_CYCA -- 管理费用 本年累计数
    , FNC_EXP_CHECK -- 财务费用
    , FNC_EXP_CYCA -- 财务费用 本年累计数
    , OPR_EXP -- 销售费用
    , ICD_CS_OF_EPS_CYCA -- 销售费用 本年累计数
    , RAD_EXP -- 研发费用
    , OPT_ICM_CHECK -- 营业利润
    , OPT_ICM_CYCA -- 营业利润 本年累计数
    , ADD_IVTMT_ICM_CHECK -- 投资收益
    , ADD_IVTMT_ICM_CYCA -- 投资收益 本年累计数
    , TTL_PFT_CHECK -- 利润总额
    , TTL_PFT_CYCA -- 利润总额 本年累计数
    , LS_ICM_TAX_EXP_CHECK -- 所得税
    , LS_ICM_TAX_EXP_CYCA -- 所得税 本年累计数
    , NT_PFT_CHECK -- 净利润
    , NT_PFT_CYCA -- 净利润 本年累计数
    , TAAD_OF_MOP -- 营业税金及附加
    , TAAD_OF_MOP_CYCA -- 营业税金及附加 本年累计数
    , ICD_EP_SL_CHECK -- 出口产品销售收入 
    , ICD_EP_SL_CYCA -- 出口产品销售收入 本年累计数
    , ICD_IMP_SL_CHECK -- 进口产品销售收入 
    , ICD_IMP_SL_CYCA -- 进口产品销售收入 本年累计数
    , LS_DST_AWS_CHECK -- 折扣与折让 
    , LS_DST_AWS_CYCA -- 折扣与折让 本年累计数
    , ICD_CS_OF_EPS_CHECK -- 出口产品销售成本  
    , ICD_CS_OF_EPS_CYCA -- 出口产品销售成本 本年累计数
    , OPR_EXP_CHECK -- 经营费用  
    , OPR_EXP_CYCA -- 经营费用 本年累计数
    , OTH_EXP_CHECK -- 其他费用  
    , OTH_EXP_CYCA -- 其他费用 本年累计数
    , ADD_DFD_ICM_CHECK -- 递延收益  
    , ADD_DFD_ICM_CYCA -- 递延收益 本年累计数
    , RVN_FM_AGAT_CHECK -- 代购代销收入  
    , RVN_FM_AGAT_CYCA -- 代购代销收入 本年累计数
    , OTH_ICM_CHECK -- 其他收入  
    , OTH_ICM_CYCA -- 其他收入 本年累计数
    , ADD_ICM_FM_OTOPT_CHECK -- 其他业务利润  
    , ADD_ICM_FM_OTOPT_CYCA -- 其他业务利润 本年累计数
    , LS_OPT_EXP_CHECK -- 营业费用  
    , LS_OPT_EXP_CYCA -- 营业费用 本年累计数
    , OTH_EXP2_CHECK -- 其他费用 
    , OTH_EXP2_CYCA -- 其他费用本年累计数
    , FTS_ICM_CHECK -- 期货收益  
    , FTS_ICM_CYCA -- 期货收益 本年累计数
    , SBS_ICM_CHECK -- 补贴收入  
    , SBS_ICM_CYCA -- 补贴收入 本年累计数
    , ICD_SFE_IN_DBS_CHECK -- 补贴前亏损的企业补贴收入  
    , ICD_SFE_IN_DBS_CYCA -- 补贴前亏损的企业补贴收入 本年累计数
    , NOPT_ICM_CHECK -- 营业外收入  
    , NOPT_ICM_CYCA -- 营业外收入 本年累计数
    , ICD_NIOND_OF_FXAST_CHECK -- 处置固定资产净收益  
    , ICD_NIOND_OF_FXAST_CYCA -- 处置固定资产净收益 本年累计数
    , NCS_DL_ICM_CHECK -- 非货币性交易收益  
    , NCS_DL_ICM_CYCA -- 非货币性交易收益 本年累计数
    , ICM_ON_SL_IAST_CHECK -- 出售无形资产收益  
    , ICM_ON_SL_IAST_CYCA -- 出售无形资产收益 本年累计数
    , NET_AMT_ICM_CHECK -- 罚款净收入  
    , NET_AMT_ICM_CYCA -- 罚款净收入 本年累计数
    , OTH_ICM2_CHECK -- 其他收入 
    , OTH_ICM2_CYCA -- 其他收入本年累计数
    , UTCSB_TO_RPIPY_CHECK -- 用以前年度含量工资节余弥补利润  
    , UTCSB_TO_RPIPY_CYCA -- 用以前年度含量工资节余弥补利润 本年累计数
    , LESS_NB_EXP_CHECK -- 营业外支出  
    , LESS_NB_EXP_CYCA -- 营业外支出 本年累计数
    , NI_ON_DOFA_CHECK -- 处置固定资产净损失  
    , NI_ON_DOFA_CYCA -- 处置固定资产净损失 本年累计数
    , LS_ON_AGMT_CHECK -- 债务重组损失  
    , LS_ON_AGMT_CYCA -- 债务重组损失 本年累计数
    , FINE_PAY_CHECK -- 罚款支出  
    , FINE_PAY_CYCA -- 罚款支出 本年累计数
    , DNT_PAY_CHECK -- 捐赠支出  
    , DNT_PAY_CYCA -- 捐赠支出 本年累计数
    , OTH_PAY_CHECK -- 其他支出  
    , OTH_PAY_CYCA -- 其他支出 本年累计数
    , ICD_TFC_WCS_CHECK -- 结转的含量工资包干节余  
    , ICD_TFC_WCS_CYCA -- 结转的含量工资包干节余 本年累计数
    , ADD_AJP_LPY_CHECK -- 以前年度损益调整  
    , ADD_AJP_LPY_CYCA -- 以前年度损益调整 本年累计数
    , MRT_ITR_CHECK -- 少数股东损益  
    , MRT_ITR_CYCA -- 少数股东损益 本年累计数
    , ADD_UFD_IVMT_LS_CHECK -- 未确认的投资损失  
    , ADD_UFD_IVMT_LS_CYCA -- 未确认的投资损失 本年累计数
    , ADD_RAB_OF_TY_CHECK -- 年初未分配利润  
    , ADD_RAB_OF_TY_CYCA -- 年初未分配利润 本年累计数
    , RDL_WTH_PFS_CHECK -- 盈余公积补亏  
    , RDL_WTH_PFS_CYCA -- 盈余公积补亏 本年累计数
    , OTH_ADJ_FTS_CHECK -- 其他调整因素  
    , OTH_ADJ_FTS_CYCA -- 其他调整因素 本年累计数
    , DST_PFT_CHECK -- 可供分配的利润  
    , DST_PFT_CYCA -- 可供分配的利润 本年累计数
    , LS_SRP_CHECK -- 单项留用的利润  
    , LS_SRP_CYCA -- 单项留用的利润 本年累计数
    , SPT_CRT_CPT_CHECK -- 补充流动资本  
    , SPT_CRT_CPT_CYCA -- 补充流动资本 本年累计数
    , DRS_SP_RSV_CHECK -- 提取法定盈余公积  
    , DRS_SP_RSV_CYCA -- 提取法定盈余公积 本年累计数
    , PBL_WLF_FD_CHECK -- 提取法定公益金  
    , PBL_WLF_FD_CYCA -- 提取法定公益金 本年累计数
    , WDS_AND_WKB_WFD_CHECK -- 提取职工奖励及福利基金  
    , WDS_AND_WKB_WFD_CYCA -- 提取职工奖励及福利基金 本年累计数
    , WDL_RSV_FD_CHECK -- 提取储备基金  
    , WDL_RSV_FD_CYCA -- 提取储备基金 本年累计数
    , WDL_RSV_BSN_EXP_CHECK -- 提取企业发展基金  
    , WDL_RSV_BSN_EXP_CYCA -- 提取企业发展基金 本年累计数
    , PFT_CPT_RT_RMT_CHECK -- 利润归还投资  
    , PFT_CPT_RT_RMT_CYCA -- 利润归还投资 本年累计数
    , PFT_AVB_OWN_DST_CHECK -- 可供投资者分配的利润  
    , PFT_AVB_OWN_DST_CYCA -- 可供投资者分配的利润 本年累计数
    , LS_APR_PRF_SDVD_CHECK -- 应付优先股股利  
    , LS_APR_PRF_SDVD_CYCA -- 应付优先股股利 本年累计数
    , APR_DSR_SPL_RSV_CHECK -- 提取任意盈余公积  
    , APR_DSR_SPL_RSV_CYCA -- 提取任意盈余公积 本年累计数
    , APR_ODN_SDVD_CHECK -- 应付普通股股利  
    , APR_ODN_SDVD_CYCA -- 应付普通股股利 本年累计数
    , TSF_ODN_SDVD_TPC_CHECK -- 转作资本的普通股股利  
    , TSF_ODN_SDVD_TPC_CYCA -- 转作资本的普通股股利 本年累计数
    , RTD_PFT_APR_CHECK -- 未分配利润  
    , RTD_PFT_APR_CYCA -- 未分配利润 本年累计数
    , ICD_DR_PBT_NY_CHECK -- 应由以后年度税前利润弥补的亏损  
    , ICD_DR_PBT_NY_CYCA -- 应由以后年度税前利润弥补的亏损 本年累计数
    , CREATE_TM -- 创建时间
    , CREATE_LOAN_OFFICER_NO -- 创建客户经理编号
    , CREATE_LOAN_OFFICER_NAME -- 创建客户经理姓名
    , CREATE_ORG -- 创建机构编号
    , FINAL_MATN_LOAN_OFFICER_NO -- 最后维护客户经理编号
    , FINAL_MATN_LOAN_OFFICER_NAME -- 最后维护客户经理姓名
    , FINAL_MATN_TM -- 最后维护时间
    , FINAL_MATN_ORG -- 最后维护机构编号
    , PT_DT -- 数据日期
)
select
      T1.CUST_ISN as CUST_INCD -- 客户内码
    , T1.RPT_DT as REPORT_YM -- 报表年月
    , T1.BEL_TO_ORG as AFLT_ORG -- 归属机构
    , '4' as FIN_TABLE_TYPE -- None
    , T2.RPT_AUDIT_IND as IF_AUDIT -- 报表是否审计、客户内码、报表年月、归属机构
    , T1.RPT_DT_TYP as REPORT_TYPE -- 报表类型,年报，季报，月报
    , T3.CUST_NAM as CUST_NAME -- 客户名称、客户内码
    , T1.CUST_ID as CUST_NO -- 客户号
    , T4.BEL_TO_IDY as REG_INDUS -- 注册行业、客户内码、归属机构
    , T5.REG_CCY_ID as REG_CURR -- 注册登记币种、客户内码、归属机构
    , '' as RV_FM_MOP_CHECK -- None
    , '' as RV_FM_MOP_CYCA -- None
    , '' as NS_FM_MOP_CHECK -- None
    , '' as NS_FM_MOP_CYCA -- None
    , '' as LS_CS_OF_MO_CHECK -- None
    , '' as LS_CS_OF_MO_CYCA -- None
    , '' as TAAD_OF_MOP_CHECK -- None
    , '' as TAAD_OF_MOP_CYCA -- None
    , '' as ICM_FM_MOP_CHECK -- None
    , '' as ICM_FM_MOP_CYCA -- None
    , T1.ACCR_UEDTOTAL_CHECK as NS_FM_MOP -- 营业总收入 本月值
    , T1.ACCR_UEDTOTAL_CYCA as ACCR_UEDTOTAL_CYCA -- 营业总收入 本年累计数
    , T1.LS_CS_OF_MO_CHECK as LS_CS_OF_MO -- 营业成本 本月值
    , T1.LS_CS_OF_MO_CYCA as LS_CS_OF_MO_CYCA -- 营业成本 本年累计数
    , T1.ADM_EXP_CHECK as ADM_EXP_CHECK -- 管理费用 本月值
    , T1.ADM_EXP_CYCA as ADM_EXP_CYCA -- 管理费用 本年累计数
    , T1.FNC_EXP_CHECK as FNC_EXP_CHECK -- 财务费用 本月值
    , T1.FNC_EXP_CYCA as FNC_EXP_CYCA -- 财务费用 本年累计数
    , T1.ICD_CS_OF_EPS_CHECK as OPR_EXP -- 销售费用 本月值
    , T1.ICD_CS_OF_EPS_CYCA as ICD_CS_OF_EPS_CYCA -- 销售费用 本年累计数
    , '' as RAD_EXP -- None
    , T1.ICM_FM_MOP_CHECK as OPT_ICM_CHECK -- 营业利润 本月值
    , T1.ICM_FM_MOP_CYCA as OPT_ICM_CYCA -- 营业利润 本年累计数
    , T1.ADD_IVTMT_ICM_CHECK as ADD_IVTMT_ICM_CHECK -- 投资收益 本月值
    , T1.ADD_IVTMT_ICM_CYCA as ADD_IVTMT_ICM_CYCA -- 投资收益 本年累计数
    , T1.TTL_PFT_CHECK as TTL_PFT_CHECK -- 利润总额 本月值
    , T1.TTL_PFT_CYCA as TTL_PFT_CYCA -- 利润总额 本年累计数
    , T1.LS_ICM_TAX_EXP_CHECK as LS_ICM_TAX_EXP_CHECK -- 所得税费用 本月值
    , T1.LS_ICM_TAX_EXP_CYCA as LS_ICM_TAX_EXP_CYCA -- 所得税费用 本年累计数
    , T1.NT_PFT_CHECK as NT_PFT_CHECK -- 净利润 本月值
    , T1.NT_PFT_CYCA as NT_PFT_CYCA -- 净利润 本年累计数
    , T1.TAAD_OF_MOP_CHECK as TAAD_OF_MOP -- 营业税金及附加 本月值
    , T1.TAAD_OF_MOP_CYCA as TAAD_OF_MOP_CYCA -- 营业税金及附加 本年累计数
    , '' as ICD_EP_SL_CHECK -- None
    , '' as ICD_EP_SL_CYCA -- None
    , '' as ICD_IMP_SL_CHECK -- None
    , '' as ICD_IMP_SL_CYCA -- None
    , '' as LS_DST_AWS_CHECK -- None
    , '' as LS_DST_AWS_CYCA -- None
    , '' as ICD_CS_OF_EPS_CHECK -- None
    , '' as ICD_CS_OF_EPS_CYCA -- None
    , '' as OPR_EXP_CHECK -- None
    , '' as OPR_EXP_CYCA -- None
    , '' as OTH_EXP_CHECK -- None
    , '' as OTH_EXP_CYCA -- None
    , '' as ADD_DFD_ICM_CHECK -- None
    , '' as ADD_DFD_ICM_CYCA -- None
    , '' as RVN_FM_AGAT_CHECK -- None
    , '' as RVN_FM_AGAT_CYCA -- None
    , '' as OTH_ICM_CHECK -- None
    , '' as OTH_ICM_CYCA -- None
    , '' as ADD_ICM_FM_OTOPT_CHECK -- None
    , '' as ADD_ICM_FM_OTOPT_CYCA -- None
    , '' as LS_OPT_EXP_CHECK -- None
    , '' as LS_OPT_EXP_CYCA -- None
    , '' as OTH_EXP2_CHECK -- None
    , '' as OTH_EXP2_CYCA -- None
    , '' as FTS_ICM_CHECK -- None
    , '' as FTS_ICM_CYCA -- None
    , '' as SBS_ICM_CHECK -- None
    , '' as SBS_ICM_CYCA -- None
    , '' as ICD_SFE_IN_DBS_CHECK -- None
    , '' as ICD_SFE_IN_DBS_CYCA -- None
    , '' as NOPT_ICM_CHECK -- None
    , '' as NOPT_ICM_CYCA -- None
    , '' as ICD_NIOND_OF_FXAST_CHECK -- None
    , '' as ICD_NIOND_OF_FXAST_CYCA -- None
    , '' as NCS_DL_ICM_CHECK -- None
    , '' as NCS_DL_ICM_CYCA -- None
    , '' as ICM_ON_SL_IAST_CHECK -- None
    , '' as ICM_ON_SL_IAST_CYCA -- None
    , '' as NET_AMT_ICM_CHECK -- None
    , '' as NET_AMT_ICM_CYCA -- None
    , '' as OTH_ICM2_CHECK -- None
    , '' as OTH_ICM2_CYCA -- None
    , '' as UTCSB_TO_RPIPY_CHECK -- None
    , '' as UTCSB_TO_RPIPY_CYCA -- None
    , '' as LESS_NB_EXP_CHECK -- None
    , '' as LESS_NB_EXP_CYCA -- None
    , '' as NI_ON_DOFA_CHECK -- None
    , '' as NI_ON_DOFA_CYCA -- None
    , '' as LS_ON_AGMT_CHECK -- None
    , '' as LS_ON_AGMT_CYCA -- None
    , '' as FINE_PAY_CHECK -- None
    , '' as FINE_PAY_CYCA -- None
    , '' as DNT_PAY_CHECK -- None
    , '' as DNT_PAY_CYCA -- None
    , '' as OTH_PAY_CHECK -- None
    , '' as OTH_PAY_CYCA -- None
    , '' as ICD_TFC_WCS_CHECK -- None
    , '' as ICD_TFC_WCS_CYCA -- None
    , '' as ADD_AJP_LPY_CHECK -- None
    , '' as ADD_AJP_LPY_CYCA -- None
    , '' as MRT_ITR_CHECK -- None
    , '' as MRT_ITR_CYCA -- None
    , '' as ADD_UFD_IVMT_LS_CHECK -- None
    , '' as ADD_UFD_IVMT_LS_CYCA -- None
    , '' as ADD_RAB_OF_TY_CHECK -- None
    , '' as ADD_RAB_OF_TY_CYCA -- None
    , '' as RDL_WTH_PFS_CHECK -- None
    , '' as RDL_WTH_PFS_CYCA -- None
    , '' as OTH_ADJ_FTS_CHECK -- None
    , '' as OTH_ADJ_FTS_CYCA -- None
    , '' as DST_PFT_CHECK -- None
    , '' as DST_PFT_CYCA -- None
    , '' as LS_SRP_CHECK -- None
    , '' as LS_SRP_CYCA -- None
    , '' as SPT_CRT_CPT_CHECK -- None
    , '' as SPT_CRT_CPT_CYCA -- None
    , '' as DRS_SP_RSV_CHECK -- None
    , '' as DRS_SP_RSV_CYCA -- None
    , '' as PBL_WLF_FD_CHECK -- None
    , '' as PBL_WLF_FD_CYCA -- None
    , '' as WDS_AND_WKB_WFD_CHECK -- None
    , '' as WDS_AND_WKB_WFD_CYCA -- None
    , '' as WDL_RSV_FD_CHECK -- None
    , '' as WDL_RSV_FD_CYCA -- None
    , '' as WDL_RSV_BSN_EXP_CHECK -- None
    , '' as WDL_RSV_BSN_EXP_CYCA -- None
    , '' as PFT_CPT_RT_RMT_CHECK -- None
    , '' as PFT_CPT_RT_RMT_CYCA -- None
    , '' as PFT_AVB_OWN_DST_CHECK -- None
    , '' as PFT_AVB_OWN_DST_CYCA -- None
    , '' as LS_APR_PRF_SDVD_CHECK -- None
    , '' as LS_APR_PRF_SDVD_CYCA -- None
    , '' as APR_DSR_SPL_RSV_CHECK -- None
    , '' as APR_DSR_SPL_RSV_CYCA -- None
    , '' as APR_ODN_SDVD_CHECK -- None
    , '' as APR_ODN_SDVD_CYCA -- None
    , '' as TSF_ODN_SDVD_TPC_CHECK -- None
    , '' as TSF_ODN_SDVD_TPC_CYCA -- None
    , '' as RTD_PFT_APR_CHECK -- None
    , '' as RTD_PFT_APR_CYCA -- None
    , '' as ICD_DR_PBT_NY_CHECK -- None
    , '' as ICD_DR_PBT_NY_CYCA -- None
    , T1.CRT_TM as CREATE_TM -- 创建时间
    , T1.CRT_LOAN_OFF as CREATE_LOAN_OFFICER_NO -- 创建客户经理
    , T6.NAME as CREATE_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.CRT_ORGAN as CREATE_ORG -- 创建机构
    , T1.LAST_LOAN_OFF as FINAL_MATN_LOAN_OFFICER_NO -- 最后维护客户经理
    , T7.NAME as FINAL_MATN_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.LAST_TM as FINAL_MATN_TM -- 最后维护时间
    , T1.LAST_ORGAN as FINAL_MATN_ORG -- 最后维护机构
    , '${PROCESS_DATE}' as PT_DT -- None
from
    ${ODS_XDZX_SCHEMA}.RF_EN_LG_ICM_STMT as T1 -- 对公金融客户利润表
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_FNC_DOC_INFO as T2 -- 财务资料公共信息表
    on T1.CUST_ISN=T2.CUST_ISN
AND T1.RPT_DT=T2.RPT_DT
AND T1.BEL_TO_ORG=T2.BEL_TO_ORG
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.CREDIT_CUST_BASE_INFO as T3 -- 信贷客户基础信息表
    on T1.CUST_ISN=T3.CUST_ISN
AND   T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_CUST_BASE_INFO as T4 -- 客户公共基础信息表
    on T1.CUST_ISN=T4.CUST_ISN
AND T1.BEL_TO_ORG=T4.BEL_TO_ORG
AND   T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.EN_CUST_EXPD_ECIF as T5 -- 对公客户扩展信息表
    on T1.CUST_ISN=T5.CUST_ISN
AND T1.BEL_TO_ORG=T5.BEL_TO_ORG
AND   T5.PT_DT='${process_date}' 
AND T5.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.USERS as T6 -- 用户表
    on T1.CRT_LOAN_OFF = T6.STAFFER_NO
AND   T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.USERS as T7 -- 用户表
    on T1.LAST_LOAN_OFF = T7.STAFFER_NO
AND   T7.PT_DT='${process_date}' 
AND T7.DELETED='0' 
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;

-- 删除所有临时表