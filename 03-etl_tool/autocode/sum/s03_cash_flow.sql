-- 层次表名: 聚合层-财务报表现金流量聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s03_cash_flow
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S03_CASH_FLOW
--     表中文名：财务报表现金流量聚合表
--     创建日期：2023-12-28 00:00:00
--     主键字段：CUST_INCD,REPORT_YM,AFLT_ORG,SRC_TAB_TYPE_CD
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包含金融类、大中型企业的经营、投资、筹资活动产生的现金流量，以及贷前评分、贷后财务健康检查、风险预警涉及的现金流量字段。同时包含客户基本信息、注册行业、币种、归属信息、表维护时间等公共字段信息。
--     更新记录：
--         2023-12-28 00:00:00 王穆军 new
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S03_CASH_FLOW drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 数据组1

insert into table ${session}.S03_CASH_FLOW(
      CUST_IN_CD -- 客户内码
    , REPORT_YM -- 报表年月
    , AFLT_ORG_NO -- 所属机构编号
    , SRC_TAB_TYPE_CD -- 来源表类型代码
    , CUST_NO -- 客户号
    , CUST_NAME -- 客户名称
    , REG_CURR_CD -- 注册登记币种代码
    , REPORT_TYPE_CD -- 报表类型代码
    , CORP_REPORT_TEMPLET_CD -- 企业报表模板代码
    , FIN_STAT_TYPE_CD -- 财务报表类型代码
    , AUDIT_FLAG -- 审计标志
    , INDS_CATE_CD -- 所属行业种类代码
    , SALES_MERCHD_PROVI_LABOR_SERV_RECV_CASH -- 销售商品提供劳务收到的现金
    , RECV_TAX_RETURN -- 收到的税费返还
    , RECV_OTHER_AND_OPER_ACT_RELAT_CASH -- 收到的其他与经营活动有关的现金
    , OPER_ACT_CASH_INFLOW_SUBTOTAL -- 经营活动现金流入小计
    , BUY_MERCHD_ACPT_LABOR_SERV_PAY_CASH -- 购买商品接受劳务支付的现金
    , PAY_GIVE_EMPLY_AND_BY_EMPLY_PAY_CASH -- 支付给职工以及为职工支付的现金
    , PAY_EACH_TAX -- 支付的各项税费
    , PAY_OTHER_AND_OPER_ACT_RELAT_CASH -- 支付的其他与经营活动有关的现金
    , OPER_ACT_CASH_OUTFLOW_SUBTOTAL -- 经营活动现金流出小计
    , OPER_ACT_PROD_CASHFLOW_NET_AMT -- 经营活动产生的现金流量净额
    , RETRA_INVEST_RECV_CASH -- 收回投资所收到的现金
    , GET_INVEST_PRFT_RECV_CASH -- 取得投资收益所收到的现金
    , DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_RETRA_CASH_NET_AMT -- 处置固定资产无形资产和其他长期资产所收回的现金净额
    , RECV_OTHER_AND_INVEST_ACT_RELAT_CASH -- 收到的其他与投资活动有关的现金
    , INVEST_ACT_CASH_INFLOW_SUBTOTAL -- 投资活动现金流入小计
    , _FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_PAY_CASH -- 购建固定资产无形资产和其他长期资产所支付的现金
    , INVEST_PAY_CASH -- 投资所支付的现金
    , PAY_OTHER_AND_INVEST_ACT_RELAT_CASH -- 支付的其他与投资活动有关的现金
    , INVEST_ACT_CASH_OUTFLOW_SUBTOTAL -- 投资活动现金流出小计
    , INVEST_ACT_PROD_CASHFLOW_NET_AMT -- 投资活动产生的现金流量净额
    , ABSB_INVEST_RECV_CASH -- 吸收投资所收到的现金
    , LOAN_RECV_CASH -- 借款所收到的现金
    , RECV_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 收到的其他与筹资活动有关的现金
    , FUNDRS_ACT_CASH_INFLOW_SUBTOTAL -- 筹资活动现金流入小计
    , REPAY_DEBT_PAY_CASH -- 偿还债务所支付的现金
    , DISTRI_DIVDND_PRFT_OR_REPAY_INT_PAY_CASH -- 分配股利利润或偿付利息所支付的现金
    , PAY_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 支付的其他与筹资活动有关的现金
    , FUNDRS_ACT_CASH_OUTFLOW_SUBTOTAL -- 筹资活动现金流出小计
    , FUNDRS_ACT_PROD_CASHFLOW_NET_AMT -- 筹资活动产生的现金流量净额
    , EXCH_RATE_CHG_CORR_CASH_INFLN -- 汇率变动对现金的影响
    , CASH_AND_CASH_EQUIV_NET_INCRMT -- 现金及现金等价物净增加额
    , NET_PRFT -- 净利润
    , ADD_ACRU_AST_DEPRE_RESER -- 加： 计提的资产减值准备
    , FIX_AST_DEPRE -- 固定资产折旧
    , IMMATRL_AST_AMORT -- 无形资产摊销
    , LONG_TERM_PEND_FEE_AMORT -- 长期待摊费用摊销
    , PEND_FEE_DECRS -- 待摊费用减少（减：增加）
    , ACCR_FEE_INCRS -- 预提费用增加（减：减少）
    , DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_LOSS_SUB_PRFT -- 处置固定资产、无形资产和其他长期资产的损失（减：收益）
    , FIX_AST_SCRAP_LOSS -- 固定资产报废损失
    , FIN_FEE -- 财务费用
    , INVEST_LOSS -- 投资损失（减：收益）
    , DEFER_TAX_LOAN_ITEM -- 递延税款贷项（减：借项）
    , INVTRY_DECRS -- 存货的减少（减：增加）
    , OPERL_RECVBL_PROJ_DECRS -- 经营性应收项目的减少（减：增加）
    , OPERL_PAYBL_PROJ_INCRS -- 经营性应付项目的增加（减：减少）
    , OTHER_NET_PRFT_ADJ_BY_OPER_ACT_CASHFLOW -- 其他将净利润调节为经营活动的现金流量
    , DEBT_TURN_BY_CAP -- 债务转为资本
    , Y1_IN_MATU_CONVTBL_CORP_BOND -- 一年内到期的可转换公司债券
    , FIN_IN_FIX_AST -- 融资租入固定资产
    , OTHER_NOT_INVOL_CASH_BAL_PAY_INVEST_AND_FUNDRS_ACT -- 其他不涉及现金收支的投资和筹资活动
    , CASH_FINAL_BAL -- 现金的期末余额
    , SUB_CASH_BEGIN_BAL -- 减：现金的期初余额
    , ADD_CASH_EQUIV_FINAL_BAL -- 加：现金等价物的期末余额
    , SUB_CASH_EQUIV_BEGIN_BAL -- 减：现金等价物的期初余额
    , CUST_DPSIT_AND_IBANK_DEP_MNYITM_NET_INCRMT -- 客户存款和同业存放款项净增加额
    , _CENTER_BANK_LOAN_NET_INCRMT -- 向中央银行借款净增加额
    , _OTHER_FIN_ORG_BORROW_CAP_NET_INCRMT -- 向其他金融机构拆入资金净增加额
    , COL_INT_COMM_FEE_AND_COMM_CASH -- 收取利息、手续费及佣金的现金
    , CUST_LOAN_AND_ADV_MNY_NET_INCRMT -- 客户贷款及垫款净增加额
    , STORE_CENTER_BANK_AND_IBANK_MNYITM_NET_INCRMT -- 存放中央银行和同业款项净增加额
    , PAY_COMM_FEE_AND_COMM_CASH -- 支付手续费及佣金的现金
    , ISSUE_BOND_RECV_CASH -- 发行债券收到的现金
    , EXCH_RATE_CHG_CORR_CASH_AND_CASH_EQUIV_INFLN -- 汇率变动对现金及现金等价物的影响
    , GET_SUB_CORP_AND_OTHER_OPEN_CORP_PAY_CASH_NET_AMT -- 取得子公司及其他营业单位支付的现金净额
    , DISP_SUB_CORP_AND_OTHER_OPEN_CORP_RECV_CASH_NET_AMT -- 处置子公司及其他营业单位收到的现金净额
    , CREATE_TM -- 创建时间
    , CREATE_LOAN_OFFICER_NO -- 创建信贷员编号
    , CREATE_LOAN_OFFICER_NAME -- 创建信贷员姓名
    , RECNT_MATN_LOAN_OFFICER_NO -- 最近维护信贷员编号
    , RECNT_MATN_LOAN_OFFICER_NAME -- 最近维护信贷员姓名
    , RECNT_MATN_TM -- 最近维护时间
    , RECNT_MATN_ORG_NO -- 最近维护机构编号
    , DATA_DT -- 数据日期
)
select
      CUST_ISN as CUST_IN_CD -- 客户内码
    , RPT_DT as REPORT_YM -- 报表年月
    , BEL_TO_ORG as AFLT_ORG_NO -- 所属机构
    , '1' as SRC_TAB_TYPE_CD -- None
    , T1.CUST_ID as CUST_NO -- 客户号
    , T2.CUST_NAM as CUST_NAME -- 客户名称
    , T3.REG_CCY_ID as REG_CURR_CD -- 注册登记币种
    , T1.RPT_DT_TYP as REPORT_TYPE_CD -- 报表类型年报，季报，月报
    , T4.EN_SZ as CORP_REPORT_TEMPLET_CD -- 企业报表模板
    , T4.FA_RPT_TYP as FIN_STAT_TYPE_CD -- 财务报表类型
    , T4.RPT_AUDIT_IND as AUDIT_FLAG -- 报表是否审计
    , T5.BEL_TO_IDY as INDS_CATE_CD -- 注册行业
    , T1.SC_OR_OL_CASH_CHECK as SALES_MERCHD_PROVI_LABOR_SERV_RECV_CASH -- 销售商品提供劳务收到的现金 核查数
    , T1.RF_OF_TAFR_CHECK as RECV_TAX_RETURN -- 收到的税费返还 核查数
    , T1.OCRR_TO_OA_CHECK as RECV_OTHER_AND_OPER_ACT_RELAT_CASH -- 收到的其他与经营活动有关的现金 核查数
    , T1.CASH_OP_IS_CHECK as OPER_ACT_CASH_INFLOW_SUBTOTAL -- 现金流入小计（经营） 核查数
    , T1.CP_FOR_COL_CHECK as BUY_MERCHD_ACPT_LABOR_SERV_PAY_CASH -- 购买商品接受劳务支付的现金 核查数
    , T1.CP_TO_AND_FE_CHECK as PAY_GIVE_EMPLY_AND_BY_EMPLY_PAY_CASH -- 支付给职工以及为职工支付的现金 核查数
    , T1.TX_AND_FP_CHECK as PAY_EACH_TAX -- 支付的各项税费 核查数
    , T1.OCPR_TO_OA_CHECK as PAY_OTHER_AND_OPER_ACT_RELAT_CASH -- 支付的其他与经营活动有关的现金 核查数
    , T1.CASH_OP_OS_CHECK as OPER_ACT_CASH_OUTFLOW_SUBTOTAL -- 现金流出小计（经营） 核查数
    , T1.CFG_FROM_OAA_CHECK as OPER_ACT_PROD_CASHFLOW_NET_AMT -- 经营活动产生的现金流量净额 核查数
    , T1.CASH_FROM_IW_CHECK as RETRA_INVEST_RECV_CASH -- 收回投资所收到的现金 核查数
    , T1.CASH_FM_ITI_CHECK as GET_INVEST_PRFT_RECV_CASH -- 取得投资收益所收到的现金 核查数
    , T1.NC_FM_DFA_IAAOLA_CHECK as DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_RETRA_CASH_NET_AMT -- 处置固定资产无形资产和其他长期资产所收回的现金净额 核查数
    , T1.OCRR_TO_IVT_CHECK as RECV_OTHER_AND_INVEST_ACT_RELAT_CASH -- 收到的其他与投资活动有关的现金 核查数
    , T1.CASH_IT_IS_CHECK as INVEST_ACT_CASH_INFLOW_SUBTOTAL -- 现金流入小计（投资）核查数
    , T1.CP_FOR_BFA_IAAOLI_CHECK as _FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_PAY_CASH -- 购建固定资产无形资产和其他长期资产所支付的现金 核查数
    , T1.CP_FOR_IM_CHECK as INVEST_PAY_CASH -- 投资所支付的现金 核查数
    , T1.OCPR_TO_IA_CHECK as PAY_OTHER_AND_INVEST_ACT_RELAT_CASH -- 支付的其他与投资活动有关的现金 核查数
    , T1.CASH_IT_OS_CHECK as INVEST_ACT_CASH_OUTFLOW_SUBTOTAL -- 现金流出小计（投资） 核查数
    , T1.CFG_FM_IAA_CHECK as INVEST_ACT_PROD_CASHFLOW_NET_AMT -- 投资活动产生的现金流量净额 核查数
    , T1.CASH_FM_AI_CHECK as ABSB_INVEST_RECV_CASH -- 吸收投资所收到的现金 核查数
    , T1.CASH_RCV_BRW_CHECK as LOAN_RECV_CASH -- 借款所收到的现金 核查数
    , T1.OCRR_TO_FA_CHECK as RECV_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 收到的其他与筹资活动有关的现金 核查数
    , T1.CASH_FN_IS_CHECK as FUNDRS_ACT_CASH_INFLOW_SUBTOTAL -- 现金流入小计（筹集） 核查数
    , T1.CP_FOR_DB_CHECK as REPAY_DEBT_PAY_CASH -- 偿还债务所支付的现金 核查数
    , T1.CP_FOR_DV_PI_CHECK as DISTRI_DIVDND_PRFT_OR_REPAY_INT_PAY_CASH -- 分配股利利润或偿付利息所支付的现金 核查数
    , T1.OCPR_TO_FA_CHECK as PAY_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 支付的其他与筹资活动有关的现金 核查数
    , T1.CASH_FN_OS_CHECK as FUNDRS_ACT_CASH_OUTFLOW_SUBTOTAL -- 现金流出小计（筹集） 核查数
    , T1.CF_FM_FAA_CHECK as FUNDRS_ACT_PROD_CASHFLOW_NET_AMT -- 筹资活动产生的现金流量净额 核查数
    , T1.EFE_RATE_CHG_CASH_CHECK as EXCH_RATE_CHG_CORR_CASH_INFLN -- 汇率变动对现金的影响 核查数
    , T1.NI_OF_CA_CE_CHECK as CASH_AND_CASH_EQUIV_NET_INCRMT -- 现金及现金等价物净增加额 核查数
    , T6.NET_PROFIT as NET_PRFT -- 净利润
    , T6.ADD_PNOFAS_IPTOFAS as ADD_ACRU_AST_DEPRE_RESER -- 加： 计提的资产减值准备
    , T6.DPN_OF_FDAS as FIX_AST_DEPRE -- 固定资产折旧
    , T6.ITB_AS_WD as IMMATRL_AST_AMORT -- 无形资产摊销
    , T6.LT_PDES_RVN as LONG_TERM_PEND_FEE_AMORT -- 长期待摊费用摊销
    , T6.DCE_PD_ES as PEND_FEE_DECRS -- 待摊费用减少（减：增加）
    , T6.ICE_AS_ES as ACCR_FEE_INCRS -- 预提费用增加（减：减少）
    , T6.HD_FAS_IAS_LAS_LS as DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_LOSS_SUB_PRFT -- 处置固定资产、无形资产和其他长期资产的损失（减：收益）
    , T6.FALOS_FOR_RT as FIX_AST_SCRAP_LOSS -- 固定资产报废损失
    , T6.FNL_EPS as FIN_FEE -- 财务费用
    , T6.IVT_LOS as INVEST_LOSS -- 投资损失（减：收益）
    , T6.DFE_TAX_CD as DEFER_TAX_LOAN_ITEM -- 递延税款贷项（减：借项）
    , T6.DEC_OF_SK as INVTRY_DECRS -- 存货的减少（减：增加）
    , T6.DEC_OF_BS_RS as OPERL_RECVBL_PROJ_DECRS -- 经营性应收项目的减少（减：增加）
    , T6.ICE_OF_BS_RS as OPERL_PAYBL_PROJ_INCRS -- 经营性应付项目的增加（减：减少）
    , T6.OTHER as OTHER_NET_PRFT_ADJ_BY_OPER_ACT_CASHFLOW -- 其他
    , T6.DT_ITO_CL as DEBT_TURN_BY_CAP -- 债务转为资本
    , T6.MWAY_CCB as Y1_IN_MATU_CONVTBL_CORP_BOND -- 一年内到期的可转换公司债券
    , T6.FG_FOR_FFAS as FIN_IN_FIX_AST -- 融资租入固定资产
    , T6.OTHER_TWO as OTHER_NOT_INVOL_CASH_BAL_PAY_INVEST_AND_FUNDRS_ACT -- 其他2
    , T6.CASH_FOR_EDBE as CASH_FINAL_BAL -- 现金的期末余额
    , T6.LS_CASH_FOR_EYBE as SUB_CASH_BEGIN_BAL -- 减：现金的期初余额
    , T6.ADD_CASH_EQT_EDBE as ADD_CASH_EQUIV_FINAL_BAL -- 加：现金等价物的期末余额
    , T6.LS_CASH_EQT_EYBE as SUB_CASH_EQUIV_BEGIN_BAL -- 减：现金等价物的期初余额
    , '' as CUST_DPSIT_AND_IBANK_DEP_MNYITM_NET_INCRMT -- None
    , '' as _CENTER_BANK_LOAN_NET_INCRMT -- None
    , '' as _OTHER_FIN_ORG_BORROW_CAP_NET_INCRMT -- None
    , '' as COL_INT_COMM_FEE_AND_COMM_CASH -- None
    , '' as CUST_LOAN_AND_ADV_MNY_NET_INCRMT -- None
    , '' as STORE_CENTER_BANK_AND_IBANK_MNYITM_NET_INCRMT -- None
    , '' as PAY_COMM_FEE_AND_COMM_CASH -- None
    , '' as ISSUE_BOND_RECV_CASH -- None
    , '' as EXCH_RATE_CHG_CORR_CASH_AND_CASH_EQUIV_INFLN -- None
    , '' as GET_SUB_CORP_AND_OTHER_OPEN_CORP_PAY_CASH_NET_AMT -- None
    , '' as DISP_SUB_CORP_AND_OTHER_OPEN_CORP_RECV_CASH_NET_AMT -- None
    , T1.CRT_TM as CREATE_TM -- 创建时间
    , T1.CRT_LOAN_OFF as CREATE_LOAN_OFFICER_NO -- 创建信贷员
    , T7.NAME as CREATE_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.LAST_LOAN_OFF as RECNT_MATN_LOAN_OFFICER_NO -- 最后维护信贷员
    , T8.NAME as RECNT_MATN_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.LAST_TM as RECNT_MATN_TM -- 最后维护时间
    , T1.LAST_ORGAN as RECNT_MATN_ORG_NO -- 最后维护机构
    , '${PROCESS_DATE}' as DATA_DT -- None
from
    ${ODS_XDZX_SCHEMA}.EN_LG_CASH_FLOW as T1 -- 大中型企业现金流量表
    LEFT JOIN ${ODS_XDZX_SCHEMA}.CREDIT_CUST_BASE_INFO as T2 -- 信贷客户基础信息表
    on T1.CUST_ISN = T2.CUST_ISN 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.EN_CUST_EXPD_ECIF as T3 -- 对公客户扩展信息表
    on T1.CUST_ISN =T3.CUST_ISN 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_FNC_DOC_INFO as T4 -- 财务资料公共信息表
    on T4.CUST_ISN = T1.CUST_ISN and
T4.RPT_DT = T1.RPT_DT and 
T4.BEL_TO_ORG = T1.BEL_TO_ORG  
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_CUST_BASE_INFO as T5 -- 客户公共基础信息表
    on T5.CUST_ISN = T5.CUST_ISN 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.EN_LG_CASH_FLOW_SCHEDULE as T6 -- 大中型企业现金流量表附表
    on T6.CUST_ISN = T1.CUST_ISN and
T6.RPT_DT = T1.RPT_DT and 
T6.BEL_TO_ORG = T1.BEL_TO_ORG  
    LEFT JOIN ${ODS_XDZX_SCHEMA}.Users as T7 -- 用户表
    on T7.STAFFER_NO =T1.CRT_LOAN_OFF 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.Users as T8 -- 用户表
    on T8.STAFFER_NO = T1.LAST_LOAN_OFF 
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第2组）==============
-- 数据组2

insert into table ${session}.S03_CASH_FLOW(
      CUST_IN_CD -- 客户内码
    , REPORT_YM -- 报表年月
    , AFLT_ORG_NO -- 所属机构编号
    , SRC_TAB_TYPE_CD -- 来源表类型代码
    , CUST_NO -- 客户号
    , CUST_NAME -- 客户名称
    , REG_CURR_CD -- 注册登记币种代码
    , REPORT_TYPE_CD -- 报表类型代码
    , CORP_REPORT_TEMPLET_CD -- 企业报表模板代码
    , FIN_STAT_TYPE_CD -- 财务报表类型代码
    , AUDIT_FLAG -- 审计标志
    , INDS_CATE_CD -- 所属行业种类代码
    , SALES_MERCHD_PROVI_LABOR_SERV_RECV_CASH -- 销售商品提供劳务收到的现金
    , RECV_TAX_RETURN -- 收到的税费返还
    , RECV_OTHER_AND_OPER_ACT_RELAT_CASH -- 收到的其他与经营活动有关的现金
    , OPER_ACT_CASH_INFLOW_SUBTOTAL -- 经营活动现金流入小计
    , BUY_MERCHD_ACPT_LABOR_SERV_PAY_CASH -- 购买商品接受劳务支付的现金
    , PAY_GIVE_EMPLY_AND_BY_EMPLY_PAY_CASH -- 支付给职工以及为职工支付的现金
    , PAY_EACH_TAX -- 支付的各项税费
    , PAY_OTHER_AND_OPER_ACT_RELAT_CASH -- 支付的其他与经营活动有关的现金
    , OPER_ACT_CASH_OUTFLOW_SUBTOTAL -- 经营活动现金流出小计
    , OPER_ACT_PROD_CASHFLOW_NET_AMT -- 经营活动产生的现金流量净额
    , RETRA_INVEST_RECV_CASH -- 收回投资所收到的现金
    , GET_INVEST_PRFT_RECV_CASH -- 取得投资收益所收到的现金
    , DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_RETRA_CASH_NET_AMT -- 处置固定资产无形资产和其他长期资产所收回的现金净额
    , RECV_OTHER_AND_INVEST_ACT_RELAT_CASH -- 收到的其他与投资活动有关的现金
    , INVEST_ACT_CASH_INFLOW_SUBTOTAL -- 投资活动现金流入小计
    , _FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_PAY_CASH -- 购建固定资产无形资产和其他长期资产所支付的现金
    , INVEST_PAY_CASH -- 投资所支付的现金
    , PAY_OTHER_AND_INVEST_ACT_RELAT_CASH -- 支付的其他与投资活动有关的现金
    , INVEST_ACT_CASH_OUTFLOW_SUBTOTAL -- 投资活动现金流出小计
    , INVEST_ACT_PROD_CASHFLOW_NET_AMT -- 投资活动产生的现金流量净额
    , ABSB_INVEST_RECV_CASH -- 吸收投资所收到的现金
    , LOAN_RECV_CASH -- 借款所收到的现金
    , RECV_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 收到的其他与筹资活动有关的现金
    , FUNDRS_ACT_CASH_INFLOW_SUBTOTAL -- 筹资活动现金流入小计
    , REPAY_DEBT_PAY_CASH -- 偿还债务所支付的现金
    , DISTRI_DIVDND_PRFT_OR_REPAY_INT_PAY_CASH -- 分配股利利润或偿付利息所支付的现金
    , PAY_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 支付的其他与筹资活动有关的现金
    , FUNDRS_ACT_CASH_OUTFLOW_SUBTOTAL -- 筹资活动现金流出小计
    , FUNDRS_ACT_PROD_CASHFLOW_NET_AMT -- 筹资活动产生的现金流量净额
    , EXCH_RATE_CHG_CORR_CASH_INFLN -- 汇率变动对现金的影响
    , CASH_AND_CASH_EQUIV_NET_INCRMT -- 现金及现金等价物净增加额
    , NET_PRFT -- 净利润
    , ADD_ACRU_AST_DEPRE_RESER -- 加： 计提的资产减值准备
    , FIX_AST_DEPRE -- 固定资产折旧
    , IMMATRL_AST_AMORT -- 无形资产摊销
    , LONG_TERM_PEND_FEE_AMORT -- 长期待摊费用摊销
    , PEND_FEE_DECRS -- 待摊费用减少（减：增加）
    , ACCR_FEE_INCRS -- 预提费用增加（减：减少）
    , DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_LOSS_SUB_PRFT -- 处置固定资产、无形资产和其他长期资产的损失（减：收益）
    , FIX_AST_SCRAP_LOSS -- 固定资产报废损失
    , FIN_FEE -- 财务费用
    , INVEST_LOSS -- 投资损失（减：收益）
    , DEFER_TAX_LOAN_ITEM -- 递延税款贷项（减：借项）
    , INVTRY_DECRS -- 存货的减少（减：增加）
    , OPERL_RECVBL_PROJ_DECRS -- 经营性应收项目的减少（减：增加）
    , OPERL_PAYBL_PROJ_INCRS -- 经营性应付项目的增加（减：减少）
    , OTHER_NET_PRFT_ADJ_BY_OPER_ACT_CASHFLOW -- 其他将净利润调节为经营活动的现金流量
    , DEBT_TURN_BY_CAP -- 债务转为资本
    , Y1_IN_MATU_CONVTBL_CORP_BOND -- 一年内到期的可转换公司债券
    , FIN_IN_FIX_AST -- 融资租入固定资产
    , OTHER_NOT_INVOL_CASH_BAL_PAY_INVEST_AND_FUNDRS_ACT -- 其他不涉及现金收支的投资和筹资活动
    , CASH_FINAL_BAL -- 现金的期末余额
    , SUB_CASH_BEGIN_BAL -- 减：现金的期初余额
    , ADD_CASH_EQUIV_FINAL_BAL -- 加：现金等价物的期末余额
    , SUB_CASH_EQUIV_BEGIN_BAL -- 减：现金等价物的期初余额
    , CUST_DPSIT_AND_IBANK_DEP_MNYITM_NET_INCRMT -- 客户存款和同业存放款项净增加额
    , _CENTER_BANK_LOAN_NET_INCRMT -- 向中央银行借款净增加额
    , _OTHER_FIN_ORG_BORROW_CAP_NET_INCRMT -- 向其他金融机构拆入资金净增加额
    , COL_INT_COMM_FEE_AND_COMM_CASH -- 收取利息、手续费及佣金的现金
    , CUST_LOAN_AND_ADV_MNY_NET_INCRMT -- 客户贷款及垫款净增加额
    , STORE_CENTER_BANK_AND_IBANK_MNYITM_NET_INCRMT -- 存放中央银行和同业款项净增加额
    , PAY_COMM_FEE_AND_COMM_CASH -- 支付手续费及佣金的现金
    , ISSUE_BOND_RECV_CASH -- 发行债券收到的现金
    , EXCH_RATE_CHG_CORR_CASH_AND_CASH_EQUIV_INFLN -- 汇率变动对现金及现金等价物的影响
    , GET_SUB_CORP_AND_OTHER_OPEN_CORP_PAY_CASH_NET_AMT -- 取得子公司及其他营业单位支付的现金净额
    , DISP_SUB_CORP_AND_OTHER_OPEN_CORP_RECV_CASH_NET_AMT -- 处置子公司及其他营业单位收到的现金净额
    , CREATE_TM -- 创建时间
    , CREATE_LOAN_OFFICER_NO -- 创建信贷员编号
    , CREATE_LOAN_OFFICER_NAME -- 创建信贷员姓名
    , RECNT_MATN_LOAN_OFFICER_NO -- 最近维护信贷员编号
    , RECNT_MATN_LOAN_OFFICER_NAME -- 最近维护信贷员姓名
    , RECNT_MATN_TM -- 最近维护时间
    , RECNT_MATN_ORG_NO -- 最近维护机构编号
    , DATA_DT -- 数据日期
)
select
      CUST_ISN as CUST_IN_CD -- 客户内码
    , RPT_DT as REPORT_YM -- 报表年月
    , BEL_TO_ORG as AFLT_ORG_NO -- 所属机构
    , '2' as SRC_TAB_TYPE_CD -- None
    , T1.CUST_ID as CUST_NO -- 客户号
    , T2.CUST_NAM as CUST_NAME -- 客户名称
    , T3.REG_CCY_ID as REG_CURR_CD -- 注册登记币种
    , T1.RPT_DT_TYP as REPORT_TYPE_CD -- 报表年份
    , T4.EN_SZ as CORP_REPORT_TEMPLET_CD -- 企业报表模板
    , T4.FA_RPT_TYP as FIN_STAT_TYPE_CD -- 财务报表类型
    , T4.RPT_AUDIT_IND as AUDIT_FLAG -- 报表是否审计
    , T5.BEL_TO_IDY as INDS_CATE_CD -- 注册行业
    , T1.SC_OR_OL_CASH as SALES_MERCHD_PROVI_LABOR_SERV_RECV_CASH -- 销售商品提供劳务收到的现金
    , T1.RF_OF_TAFR as RECV_TAX_RETURN -- 收到的税费返还
    , T1.OCRR_TO_OA as RECV_OTHER_AND_OPER_ACT_RELAT_CASH -- 收到的其他与经营活动有关的现金
    , T1.CASH_OP_IS as OPER_ACT_CASH_INFLOW_SUBTOTAL -- 经营活动现金流入小计
    , T1.CP_FOR_COL as BUY_MERCHD_ACPT_LABOR_SERV_PAY_CASH -- 购买商品、接受劳务支付的现金
    , T1.CP_TO_AND_FE as PAY_GIVE_EMPLY_AND_BY_EMPLY_PAY_CASH -- 支付给职工以及为职工支付的现金
    , T1.TX_AND_FP as PAY_EACH_TAX -- 支付的各项税费
    , T1.OCPR_TO_OA as PAY_OTHER_AND_OPER_ACT_RELAT_CASH -- 支付的其它与经营活动有关的现金
    , T1.CASH_OP_OS as OPER_ACT_CASH_OUTFLOW_SUBTOTAL -- 经营活动现金流出小计
    , T1.CFG_FROM_OAA as OPER_ACT_PROD_CASHFLOW_NET_AMT -- 经营活动产生的现金流量净额
    , T1.CASH_FROM_IW as RETRA_INVEST_RECV_CASH -- 收回投资所收到的现金
    , T1.CASH_FM_ITI as GET_INVEST_PRFT_RECV_CASH -- 取得投资收益所收到的现金
    , T1.NC_FM_DFA_IAAOLA as DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_RETRA_CASH_NET_AMT -- 处置固定资产、无形资产和其他长期资产所收回的现金净额
    , T1.OCRR_TO_IVT as RECV_OTHER_AND_INVEST_ACT_RELAT_CASH -- 收到的其他与投资活动有关的现金
    , T1.CASH_IT_IS as INVEST_ACT_CASH_INFLOW_SUBTOTAL -- 投资活动现金流入小计
    , T1.CP_FOR_BFA_IAAOLI as _FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_PAY_CASH -- 购建固定资产、无形资产和其他长期资产所支付的现金
    , T1.CP_FOR_IM as INVEST_PAY_CASH -- 投资所支付的现金
    , T1.OCPR_TO_IA as PAY_OTHER_AND_INVEST_ACT_RELAT_CASH -- 支付的其他与投资活动有关的现金
    , T1.CASH_IT_OS as INVEST_ACT_CASH_OUTFLOW_SUBTOTAL -- 现金流出小计（投资）
    , T1.CFG_FM_IAA as INVEST_ACT_PROD_CASHFLOW_NET_AMT -- 投资活动产生的现金流量净额
    , T1.CASH_FM_AI as ABSB_INVEST_RECV_CASH -- 吸收投资收到的现金
    , T1.CASH_RCV_BRW as LOAN_RECV_CASH -- 取得借款所收到的现金
    , T1.OCRR_TO_FA as RECV_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 收到其他与筹资活动有关的现金
    , T1.CASH_FN_IS as FUNDRS_ACT_CASH_INFLOW_SUBTOTAL -- 筹资活动现金流入小计
    , T1.CP_FOR_DB as REPAY_DEBT_PAY_CASH -- 偿还债务所支付的现金
    , T1.CP_FOR_DV_PI as DISTRI_DIVDND_PRFT_OR_REPAY_INT_PAY_CASH -- 来源表类型代码：分配股利、利润或偿付利息所支付的现金
    , T1.OCPR_TO_FA as PAY_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 支付的其他与筹资活动有关的现金
    , T1.CASH_FN_OS as FUNDRS_ACT_CASH_OUTFLOW_SUBTOTAL -- 筹资活动现金流出小计
    , T1.CF_FM_FAA as FUNDRS_ACT_PROD_CASHFLOW_NET_AMT -- 筹资活动产生的现金流量净额
    , T1.EFE_RATE_CHG_CASH as EXCH_RATE_CHG_CORR_CASH_INFLN -- 汇率变动对现金及现金等价物的影响
    , T1.NI_OF_CA_CE as CASH_AND_CASH_EQUIV_NET_INCRMT -- 现金及现金等价物净增加额
    , T6.NET_PROFIT as NET_PRFT -- 净利润
    , T6.ADD_PNOFAS_IPTOFAS as ADD_ACRU_AST_DEPRE_RESER -- 加： 计提的资产减值准备
    , T6.DPN_OF_FDAS as FIX_AST_DEPRE -- 固定资产折旧
    , T6.ITB_AS_WD as IMMATRL_AST_AMORT -- 无形资产摊销
    , T6.LT_PDES_RVN as LONG_TERM_PEND_FEE_AMORT -- 长期待摊费用摊销
    , T6.DCE_PD_ES as PEND_FEE_DECRS -- 待摊费用减少（减：增加）
    , T6.ICE_AS_ES as ACCR_FEE_INCRS -- 预提费用增加（减：减少）
    , T6.HD_FAS_IAS_LAS_LS as DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_LOSS_SUB_PRFT -- 处置固定资产、无形资产和其他长期资产的损失（减：收益）
    , T6.FALOS_FOR_RT as FIX_AST_SCRAP_LOSS -- 固定资产报废损失
    , T6.FNL_EPS as FIN_FEE -- 财务费用
    , T6.IVT_LOS as INVEST_LOSS -- 投资损失（减：收益）
    , T6.DFE_TAX_CD as DEFER_TAX_LOAN_ITEM -- 递延税款贷项（减：借项）
    , T6.DEC_OF_SK as INVTRY_DECRS -- 存货的减少（减：增加）
    , T6.DEC_OF_BS_RS as OPERL_RECVBL_PROJ_DECRS -- 经营性应收项目的减少（减：增加）
    , T6.ICE_OF_BS_RS as OPERL_PAYBL_PROJ_INCRS -- 经营性应付项目的增加（减：减少）
    , T6.OTHER as OTHER_NET_PRFT_ADJ_BY_OPER_ACT_CASHFLOW -- 其他
    , T6.DT_ITO_CL as DEBT_TURN_BY_CAP -- 债务转为资本
    , T6.MWAY_CCB as Y1_IN_MATU_CONVTBL_CORP_BOND -- 一年内到期的可转换公司债券
    , T6.FG_FOR_FFAS as FIN_IN_FIX_AST -- 融资租入固定资产
    , T6.OTHER_TWO as OTHER_NOT_INVOL_CASH_BAL_PAY_INVEST_AND_FUNDRS_ACT -- 其他2
    , T6.CASH_FOR_EDBE as CASH_FINAL_BAL -- 现金的期末余额
    , T6.LS_CASH_FOR_EYBE as SUB_CASH_BEGIN_BAL -- 减：现金的期初余额
    , T6.ADD_CASH_EQT_EDBE as ADD_CASH_EQUIV_FINAL_BAL -- 加：现金等价物的期末余额
    , T6.LS_CASH_EQT_EYBE as SUB_CASH_EQUIV_BEGIN_BAL -- 减：现金等价物的期初余额
    , '' as CUST_DPSIT_AND_IBANK_DEP_MNYITM_NET_INCRMT -- None
    , '' as _CENTER_BANK_LOAN_NET_INCRMT -- None
    , '' as _OTHER_FIN_ORG_BORROW_CAP_NET_INCRMT -- None
    , '' as COL_INT_COMM_FEE_AND_COMM_CASH -- None
    , '' as CUST_LOAN_AND_ADV_MNY_NET_INCRMT -- None
    , '' as STORE_CENTER_BANK_AND_IBANK_MNYITM_NET_INCRMT -- None
    , '' as PAY_COMM_FEE_AND_COMM_CASH -- None
    , '' as ISSUE_BOND_RECV_CASH -- None
    , '' as EXCH_RATE_CHG_CORR_CASH_AND_CASH_EQUIV_INFLN -- None
    , T1.CP_FOR_SUBSIDIARIES as GET_SUB_CORP_AND_OTHER_OPEN_CORP_PAY_CASH_NET_AMT -- 取得子公司及其他营业单位支付的现金净额
    , T1.NC_FM_SUBSIDIARIES as DISP_SUB_CORP_AND_OTHER_OPEN_CORP_RECV_CASH_NET_AMT -- 处置子公司及其他营业单位收到的现金净额
    , T1.CRT_TM as CREATE_TM -- 创建时间
    , T1.CRT_LOAN_OFF as CREATE_LOAN_OFFICER_NO -- 创建信贷员
    , T7.NAME as CREATE_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.LAST_LOAN_OFF as RECNT_MATN_LOAN_OFFICER_NO -- 最后维护信贷员
    , T8.NAME as RECNT_MATN_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.LAST_TM as RECNT_MATN_TM -- 最后维护时间
    , T1.LAST_ORGAN as RECNT_MATN_ORG_NO -- 最后维护机构
    , '${PROCESS_DATE}' as DATA_DT -- None
from
    ${ODS_XDZX_SCHEMA}.CB_EN_LG_CASH_FLOW as T1 -- 央行财报现金流量表
    LEFT JOIN ${ODS_XDZX_SCHEMA}.CREDIT_CUST_BASE_INFO as T2 -- 信贷客户基础信息表
    on T1.CUST_ISN = T2.CUST_ISN 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.EN_CUST_EXPD_ECIF as T3 -- 对公客户扩展信息表
    on T1.CUST_ISN =T3.CUST_ISN 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_FNC_DOC_INFO as T4 -- 财务资料公共信息表
    on T4.CUST_ISN = T1.CUST_ISN and
T4.RPT_DT = T1.RPT_DT and 
T4.BEL_TO_ORG = T1.BEL_TO_ORG  
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_CUST_BASE_INFO as T5 -- 客户公共基础信息表
    on T5.CUST_ISN = T5.CUST_ISN 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.EN_LG_CASH_FLOW_SCHEDULE as T6 -- 大中型企业现金流量表附表
    on T6.CUST_ISN = T1.CUST_ISN and
T6.RPT_DT = T1.RPT_DT and 
T6.BEL_TO_ORG = T1.BEL_TO_ORG  
    LEFT JOIN ${ODS_XDZX_SCHEMA}.Users as T7 -- 用户表
    on T7.STAFFER_NO =T1.CRT_LOAN_OFF 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.Users as T8 -- 用户表
    on T8.STAFFER_NO = T1.LAST_LOAN_OFF 
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第3组）==============
-- 数据组3

insert into table ${session}.S03_CASH_FLOW(
      CUST_IN_CD -- 客户内码
    , REPORT_YM -- 报表年月
    , AFLT_ORG_NO -- 所属机构编号
    , SRC_TAB_TYPE_CD -- 来源表类型代码
    , CUST_NO -- 客户号
    , CUST_NAME -- 客户名称
    , REG_CURR_CD -- 注册登记币种代码
    , REPORT_TYPE_CD -- 报表类型代码
    , CORP_REPORT_TEMPLET_CD -- 企业报表模板代码
    , FIN_STAT_TYPE_CD -- 财务报表类型代码
    , AUDIT_FLAG -- 审计标志
    , INDS_CATE_CD -- 所属行业种类代码
    , SALES_MERCHD_PROVI_LABOR_SERV_RECV_CASH -- 销售商品提供劳务收到的现金
    , RECV_TAX_RETURN -- 收到的税费返还
    , RECV_OTHER_AND_OPER_ACT_RELAT_CASH -- 收到的其他与经营活动有关的现金
    , OPER_ACT_CASH_INFLOW_SUBTOTAL -- 经营活动现金流入小计
    , BUY_MERCHD_ACPT_LABOR_SERV_PAY_CASH -- 购买商品接受劳务支付的现金
    , PAY_GIVE_EMPLY_AND_BY_EMPLY_PAY_CASH -- 支付给职工以及为职工支付的现金
    , PAY_EACH_TAX -- 支付的各项税费
    , PAY_OTHER_AND_OPER_ACT_RELAT_CASH -- 支付的其他与经营活动有关的现金
    , OPER_ACT_CASH_OUTFLOW_SUBTOTAL -- 经营活动现金流出小计
    , OPER_ACT_PROD_CASHFLOW_NET_AMT -- 经营活动产生的现金流量净额
    , RETRA_INVEST_RECV_CASH -- 收回投资所收到的现金
    , GET_INVEST_PRFT_RECV_CASH -- 取得投资收益所收到的现金
    , DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_RETRA_CASH_NET_AMT -- 处置固定资产无形资产和其他长期资产所收回的现金净额
    , RECV_OTHER_AND_INVEST_ACT_RELAT_CASH -- 收到的其他与投资活动有关的现金
    , INVEST_ACT_CASH_INFLOW_SUBTOTAL -- 投资活动现金流入小计
    , _FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_PAY_CASH -- 购建固定资产无形资产和其他长期资产所支付的现金
    , INVEST_PAY_CASH -- 投资所支付的现金
    , PAY_OTHER_AND_INVEST_ACT_RELAT_CASH -- 支付的其他与投资活动有关的现金
    , INVEST_ACT_CASH_OUTFLOW_SUBTOTAL -- 投资活动现金流出小计
    , INVEST_ACT_PROD_CASHFLOW_NET_AMT -- 投资活动产生的现金流量净额
    , ABSB_INVEST_RECV_CASH -- 吸收投资所收到的现金
    , LOAN_RECV_CASH -- 借款所收到的现金
    , RECV_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 收到的其他与筹资活动有关的现金
    , FUNDRS_ACT_CASH_INFLOW_SUBTOTAL -- 筹资活动现金流入小计
    , REPAY_DEBT_PAY_CASH -- 偿还债务所支付的现金
    , DISTRI_DIVDND_PRFT_OR_REPAY_INT_PAY_CASH -- 分配股利利润或偿付利息所支付的现金
    , PAY_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 支付的其他与筹资活动有关的现金
    , FUNDRS_ACT_CASH_OUTFLOW_SUBTOTAL -- 筹资活动现金流出小计
    , FUNDRS_ACT_PROD_CASHFLOW_NET_AMT -- 筹资活动产生的现金流量净额
    , EXCH_RATE_CHG_CORR_CASH_INFLN -- 汇率变动对现金的影响
    , CASH_AND_CASH_EQUIV_NET_INCRMT -- 现金及现金等价物净增加额
    , NET_PRFT -- 净利润
    , ADD_ACRU_AST_DEPRE_RESER -- 加： 计提的资产减值准备
    , FIX_AST_DEPRE -- 固定资产折旧
    , IMMATRL_AST_AMORT -- 无形资产摊销
    , LONG_TERM_PEND_FEE_AMORT -- 长期待摊费用摊销
    , PEND_FEE_DECRS -- 待摊费用减少（减：增加）
    , ACCR_FEE_INCRS -- 预提费用增加（减：减少）
    , DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_LOSS_SUB_PRFT -- 处置固定资产、无形资产和其他长期资产的损失（减：收益）
    , FIX_AST_SCRAP_LOSS -- 固定资产报废损失
    , FIN_FEE -- 财务费用
    , INVEST_LOSS -- 投资损失（减：收益）
    , DEFER_TAX_LOAN_ITEM -- 递延税款贷项（减：借项）
    , INVTRY_DECRS -- 存货的减少（减：增加）
    , OPERL_RECVBL_PROJ_DECRS -- 经营性应收项目的减少（减：增加）
    , OPERL_PAYBL_PROJ_INCRS -- 经营性应付项目的增加（减：减少）
    , OTHER_NET_PRFT_ADJ_BY_OPER_ACT_CASHFLOW -- 其他将净利润调节为经营活动的现金流量
    , DEBT_TURN_BY_CAP -- 债务转为资本
    , Y1_IN_MATU_CONVTBL_CORP_BOND -- 一年内到期的可转换公司债券
    , FIN_IN_FIX_AST -- 融资租入固定资产
    , OTHER_NOT_INVOL_CASH_BAL_PAY_INVEST_AND_FUNDRS_ACT -- 其他不涉及现金收支的投资和筹资活动
    , CASH_FINAL_BAL -- 现金的期末余额
    , SUB_CASH_BEGIN_BAL -- 减：现金的期初余额
    , ADD_CASH_EQUIV_FINAL_BAL -- 加：现金等价物的期末余额
    , SUB_CASH_EQUIV_BEGIN_BAL -- 减：现金等价物的期初余额
    , CUST_DPSIT_AND_IBANK_DEP_MNYITM_NET_INCRMT -- 客户存款和同业存放款项净增加额
    , _CENTER_BANK_LOAN_NET_INCRMT -- 向中央银行借款净增加额
    , _OTHER_FIN_ORG_BORROW_CAP_NET_INCRMT -- 向其他金融机构拆入资金净增加额
    , COL_INT_COMM_FEE_AND_COMM_CASH -- 收取利息、手续费及佣金的现金
    , CUST_LOAN_AND_ADV_MNY_NET_INCRMT -- 客户贷款及垫款净增加额
    , STORE_CENTER_BANK_AND_IBANK_MNYITM_NET_INCRMT -- 存放中央银行和同业款项净增加额
    , PAY_COMM_FEE_AND_COMM_CASH -- 支付手续费及佣金的现金
    , ISSUE_BOND_RECV_CASH -- 发行债券收到的现金
    , EXCH_RATE_CHG_CORR_CASH_AND_CASH_EQUIV_INFLN -- 汇率变动对现金及现金等价物的影响
    , GET_SUB_CORP_AND_OTHER_OPEN_CORP_PAY_CASH_NET_AMT -- 取得子公司及其他营业单位支付的现金净额
    , DISP_SUB_CORP_AND_OTHER_OPEN_CORP_RECV_CASH_NET_AMT -- 处置子公司及其他营业单位收到的现金净额
    , CREATE_TM -- 创建时间
    , CREATE_LOAN_OFFICER_NO -- 创建信贷员编号
    , CREATE_LOAN_OFFICER_NAME -- 创建信贷员姓名
    , RECNT_MATN_LOAN_OFFICER_NO -- 最近维护信贷员编号
    , RECNT_MATN_LOAN_OFFICER_NAME -- 最近维护信贷员姓名
    , RECNT_MATN_TM -- 最近维护时间
    , RECNT_MATN_ORG_NO -- 最近维护机构编号
    , DATA_DT -- 数据日期
)
select
      CUST_ISN as CUST_IN_CD -- 客户内码
    , RPT_DT as REPORT_YM -- 报表年月
    , BEL_TO_ORG as AFLT_ORG_NO -- 所属机构
    , '3' as SRC_TAB_TYPE_CD -- None
    , T1.CUST_ID as CUST_NO -- 客户号
    , T2.CUST_NAM as CUST_NAME -- 客户名称
    , T3.REG_CCY_ID as REG_CURR_CD -- 注册登记币种
    , T1.RPT_DT_TYP as REPORT_TYPE_CD -- 报表类型年报，季报，月报
    , T4.EN_SZ as CORP_REPORT_TEMPLET_CD -- 企业报表模板
    , T4.FA_RPT_TYP as FIN_STAT_TYPE_CD -- 财务报表类型
    , T4.RPT_AUDIT_IND as AUDIT_FLAG -- 报表是否审计
    , T5.BEL_TO_IDY as INDS_CATE_CD -- 注册行业
    , '' as SALES_MERCHD_PROVI_LABOR_SERV_RECV_CASH -- None
    , '' as RECV_TAX_RETURN -- None
    , t1.QTJYHDXJHCS as RECV_OTHER_AND_OPER_ACT_RELAT_CASH -- 收到其他与经营活动有关的现金核查数
    , T1.JYHDXJLRXJHCS as OPER_ACT_CASH_INFLOW_SUBTOTAL -- 经营活动现金流入小计
    , '' as BUY_MERCHD_ACPT_LABOR_SERV_PAY_CASH -- None
    , T1.WZGZFXJHCS as PAY_GIVE_EMPLY_AND_BY_EMPLY_PAY_CASH -- 支付给职工以及为职工支付的现金 核查数
    , T1.ZFGXSFHCS as PAY_EACH_TAX -- 支付的各项税费 核查数
    , T1.ZFQTYJYHDXJHCS as PAY_OTHER_AND_OPER_ACT_RELAT_CASH -- 支付其他与经营活动有关的现金
    , T1.JYHDXJLCXJHCS as OPER_ACT_CASH_OUTFLOW_SUBTOTAL -- 经营活动现金流出小计核查数
    , T1.JYHDCSXJLLJEHCS as OPER_ACT_PROD_CASHFLOW_NET_AMT -- 经营活动产生的现金流量净额 核查数
    , T1.SHTZSDDXJHCS as RETRA_INVEST_RECV_CASH -- 收回投资收到的现金核查数
    , T1.QDTZSYSDDXJHCS as GET_INVEST_PRFT_RECV_CASH -- 取得投资收益所收到的现金 核查数
    , '' as DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_RETRA_CASH_NET_AMT -- None
    , T1.SDQTYGXJHCS as RECV_OTHER_AND_INVEST_ACT_RELAT_CASH -- 收到其他与投资活动有关的现金核查数
    , T1.TZHDXJLRXJHCS as INVEST_ACT_CASH_INFLOW_SUBTOTAL -- 投资活动现金流入小计核查数
    , T1.GJGWQZFDXJHCS as _FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_PAY_CASH -- 购建固定资产、无形资产和其他长期资产支付的现金核查数
    , T1.TZZFDXJHCS as INVEST_PAY_CASH -- 投资支付的现金核查数
    , T1.ZFQTYTYGDXJHCS as PAY_OTHER_AND_INVEST_ACT_RELAT_CASH -- 支付其他与投资活动有关的现金核查数
    , T1.TZHDXJLCXJHCS as INVEST_ACT_CASH_OUTFLOW_SUBTOTAL -- 投资活动现金流出小计核查数
    , T1.TZHDCXLLJEHCS as INVEST_ACT_PROD_CASHFLOW_NET_AMT -- 投资活动产生的现金流量净额核查数
    , T1.XSTZSDXJHCS as ABSB_INVEST_RECV_CASH -- 吸收投资收到的现金核查数
    , '' as LOAN_RECV_CASH -- None
    , T1.SDQYCYGXJHCS as RECV_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 收到其他与筹资活动有关的现金核查数
    , T1.CZHDXJLRXJHCS as FUNDRS_ACT_CASH_INFLOW_SUBTOTAL -- 筹资活动现金流入小计核查数
    , T1.CHZWZFDXJHCS as REPAY_DEBT_PAY_CASH -- 偿还债务支付的现金核查数
    , T1.FPGLLCZFXJHCS as DISTRI_DIVDND_PRFT_OR_REPAY_INT_PAY_CASH -- 分配股利、利润或偿付利息支付的现金核查数
    , T1.ZFQYCYGXJHCS as PAY_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 支付其他与筹资活动有关的现金核查数
    , T1.CZHDXJLCXJHCS as FUNDRS_ACT_CASH_OUTFLOW_SUBTOTAL -- 筹资活动现金流出小计核查数
    , T1.CZHDCSXJLLJEHCS as FUNDRS_ACT_PROD_CASHFLOW_NET_AMT -- 筹资活动产生的现金流量净额核查数
    , T1.HBDXJXDJWYXHCS as EXCH_RATE_CHG_CORR_CASH_INFLN -- 汇率变动对现金及现金等价物的影响核查数
    , T1.XJJXJDJWJZJEHCS as CASH_AND_CASH_EQUIV_NET_INCRMT -- 现金及现金等价物净增加额核查数
    , T6.NET_PROFIT as NET_PRFT -- 净利润
    , T6.ADD_PNOFAS_IPTOFAS as ADD_ACRU_AST_DEPRE_RESER -- 加： 计提的资产减值准备
    , T6.DPN_OF_FDAS as FIX_AST_DEPRE -- 固定资产折旧
    , T6.ITB_AS_WD as IMMATRL_AST_AMORT -- 无形资产摊销
    , T6.LT_PDES_RVN as LONG_TERM_PEND_FEE_AMORT -- 长期待摊费用摊销
    , T6.DCE_PD_ES as PEND_FEE_DECRS -- 待摊费用减少（减：增加）
    , T6.ICE_AS_ES as ACCR_FEE_INCRS -- 预提费用增加（减：减少）
    , T6.HD_FAS_IAS_LAS_LS as DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_LOSS_SUB_PRFT -- 处置固定资产、无形资产和其他长期资产的损失（减：收益）
    , T6.FALOS_FOR_RT as FIX_AST_SCRAP_LOSS -- 固定资产报废损失
    , T6.FNL_EPS as FIN_FEE -- 财务费用
    , T6.IVT_LOS as INVEST_LOSS -- 投资损失（减：收益）
    , T6.DFE_TAX_CD as DEFER_TAX_LOAN_ITEM -- 递延税款贷项（减：借项）
    , T6.DEC_OF_SK as INVTRY_DECRS -- 存货的减少（减：增加）
    , T6.DEC_OF_BS_RS as OPERL_RECVBL_PROJ_DECRS -- 经营性应收项目的减少（减：增加）
    , T6.ICE_OF_BS_RS as OPERL_PAYBL_PROJ_INCRS -- 经营性应付项目的增加（减：减少）
    , T6.OTHER as OTHER_NET_PRFT_ADJ_BY_OPER_ACT_CASHFLOW -- 其他
    , T6.DT_ITO_CL as DEBT_TURN_BY_CAP -- 债务转为资本
    , T6.MWAY_CCB as Y1_IN_MATU_CONVTBL_CORP_BOND -- 一年内到期的可转换公司债券
    , T6.FG_FOR_FFAS as FIN_IN_FIX_AST -- 融资租入固定资产
    , T6.OTHER_TWO as OTHER_NOT_INVOL_CASH_BAL_PAY_INVEST_AND_FUNDRS_ACT -- 其他2
    , T6.CASH_FOR_EDBE as CASH_FINAL_BAL -- 现金的期末余额
    , T6.LS_CASH_FOR_EYBE as SUB_CASH_BEGIN_BAL -- 减：现金的期初余额
    , T6.ADD_CASH_EQT_EDBE as ADD_CASH_EQUIV_FINAL_BAL -- 加：现金等价物的期末余额
    , T6.LS_CASH_EQT_EYBE as SUB_CASH_EQUIV_BEGIN_BAL -- 减：现金等价物的期初余额
    , T1.CKCFKXJZJHCS as CUST_DPSIT_AND_IBANK_DEP_MNYITM_NET_INCRMT -- 客户存款和同业存放款项净增加额核查数
    , T1.ZYYHJKJZJHCS as _CENTER_BANK_LOAN_NET_INCRMT -- 向中央银行借款净增加额核查数
    , T1.QTJRJGCRZJJZJHCS as _OTHER_FIN_ORG_BORROW_CAP_NET_INCRMT -- 向其他金融机构拆入资金净增加额核查数
    , T1.LXSXFYJHCS as COL_INT_COMM_FEE_AND_COMM_CASH -- 收取利息、手续费及佣金的现金核查数
    , T1.KHDKDKJZJHCS as CUST_LOAN_AND_ADV_MNY_NET_INCRMT -- 客户贷款及垫款净增加额核查数
    , T1.ZYYHTYKXJZJHCS as STORE_CENTER_BANK_AND_IBANK_MNYITM_NET_INCRMT -- 存放中央银行和同业款项净增加额核查数
    , T1.SXFYJXJHCS as PAY_COMM_FEE_AND_COMM_CASH -- 支付手续费及佣金的现金核查数
    , T1.FXZQSDXJHCS as ISSUE_BOND_RECV_CASH -- 发行债券收到的现金核查数
    , T1.HBDXJXDJWYXHCS as EXCH_RATE_CHG_CORR_CASH_AND_CASH_EQUIV_INFLN -- 汇率变动对现金及现金等价物的影响核查数
    , '' as GET_SUB_CORP_AND_OTHER_OPEN_CORP_PAY_CASH_NET_AMT -- None
    , '' as DISP_SUB_CORP_AND_OTHER_OPEN_CORP_RECV_CASH_NET_AMT -- None
    , T1.CRT_TM as CREATE_TM -- 创建时间
    , T1.CRT_LOAN_OFF as CREATE_LOAN_OFFICER_NO -- 创建信贷员
    , T6.NAME as CREATE_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.LAST_LOAN_OFF as RECNT_MATN_LOAN_OFFICER_NO -- 最后维护信贷员
    , T7.NAME as RECNT_MATN_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.LAST_TM as RECNT_MATN_TM -- 最后维护时间
    , T1.LAST_ORGAN as RECNT_MATN_ORG_NO -- 最后维护机构
    , '${PROCESS_DATE}' as DATA_DT -- None
from
    ${ODS_XDZX_SCHEMA}.EN_FNC_CASH_FLOW as T1 -- 对公金融客户现金流量表
    LEFT JOIN ${ODS_XDZX_SCHEMA}.CREDIT_CUST_BASE_INFO as T2 -- 信贷客户基础信息表
    on T1.CUST_ISN = T2.CUST_ISN 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.EN_CUST_EXPD_ECIF as T3 -- 对公客户扩展信息表
    on T1.CUST_ISN =T3.CUST_ISN 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_FNC_DOC_INFO as T4 -- 财务资料公共信息表
    on T4.CUST_ISN = T1.CUST_ISN and
T4.RPT_DT = T1.RPT_DT and 
T4.BEL_TO_ORG = T1.BEL_TO_ORG  
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_CUST_BASE_INFO as T5 -- 客户公共基础信息表
    on T5.CUST_ISN = T5.CUST_ISN 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.Users as T6 -- 用户表
    on T6.STAFFER_NO =T1.CRT_LOAN_OFF 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.Users as T7 -- 用户表
    on T7.STAFFER_NO = T1.LAST_LOAN_OFF 
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第4组）==============
-- 数据组4

insert into table ${session}.S03_CASH_FLOW(
      CUST_IN_CD -- 客户内码
    , REPORT_YM -- 报表年月
    , AFLT_ORG_NO -- 所属机构编号
    , SRC_TAB_TYPE_CD -- 来源表类型代码
    , CUST_NO -- 客户号
    , CUST_NAME -- 客户名称
    , REG_CURR_CD -- 注册登记币种代码
    , REPORT_TYPE_CD -- 报表类型代码
    , CORP_REPORT_TEMPLET_CD -- 企业报表模板代码
    , FIN_STAT_TYPE_CD -- 财务报表类型代码
    , AUDIT_FLAG -- 审计标志
    , INDS_CATE_CD -- 所属行业种类代码
    , SALES_MERCHD_PROVI_LABOR_SERV_RECV_CASH -- 销售商品提供劳务收到的现金
    , RECV_TAX_RETURN -- 收到的税费返还
    , RECV_OTHER_AND_OPER_ACT_RELAT_CASH -- 收到的其他与经营活动有关的现金
    , OPER_ACT_CASH_INFLOW_SUBTOTAL -- 经营活动现金流入小计
    , BUY_MERCHD_ACPT_LABOR_SERV_PAY_CASH -- 购买商品接受劳务支付的现金
    , PAY_GIVE_EMPLY_AND_BY_EMPLY_PAY_CASH -- 支付给职工以及为职工支付的现金
    , PAY_EACH_TAX -- 支付的各项税费
    , PAY_OTHER_AND_OPER_ACT_RELAT_CASH -- 支付的其他与经营活动有关的现金
    , OPER_ACT_CASH_OUTFLOW_SUBTOTAL -- 经营活动现金流出小计
    , OPER_ACT_PROD_CASHFLOW_NET_AMT -- 经营活动产生的现金流量净额
    , RETRA_INVEST_RECV_CASH -- 收回投资所收到的现金
    , GET_INVEST_PRFT_RECV_CASH -- 取得投资收益所收到的现金
    , DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_RETRA_CASH_NET_AMT -- 处置固定资产无形资产和其他长期资产所收回的现金净额
    , RECV_OTHER_AND_INVEST_ACT_RELAT_CASH -- 收到的其他与投资活动有关的现金
    , INVEST_ACT_CASH_INFLOW_SUBTOTAL -- 投资活动现金流入小计
    , _FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_PAY_CASH -- 购建固定资产无形资产和其他长期资产所支付的现金
    , INVEST_PAY_CASH -- 投资所支付的现金
    , PAY_OTHER_AND_INVEST_ACT_RELAT_CASH -- 支付的其他与投资活动有关的现金
    , INVEST_ACT_CASH_OUTFLOW_SUBTOTAL -- 投资活动现金流出小计
    , INVEST_ACT_PROD_CASHFLOW_NET_AMT -- 投资活动产生的现金流量净额
    , ABSB_INVEST_RECV_CASH -- 吸收投资所收到的现金
    , LOAN_RECV_CASH -- 借款所收到的现金
    , RECV_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 收到的其他与筹资活动有关的现金
    , FUNDRS_ACT_CASH_INFLOW_SUBTOTAL -- 筹资活动现金流入小计
    , REPAY_DEBT_PAY_CASH -- 偿还债务所支付的现金
    , DISTRI_DIVDND_PRFT_OR_REPAY_INT_PAY_CASH -- 分配股利利润或偿付利息所支付的现金
    , PAY_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 支付的其他与筹资活动有关的现金
    , FUNDRS_ACT_CASH_OUTFLOW_SUBTOTAL -- 筹资活动现金流出小计
    , FUNDRS_ACT_PROD_CASHFLOW_NET_AMT -- 筹资活动产生的现金流量净额
    , EXCH_RATE_CHG_CORR_CASH_INFLN -- 汇率变动对现金的影响
    , CASH_AND_CASH_EQUIV_NET_INCRMT -- 现金及现金等价物净增加额
    , NET_PRFT -- 净利润
    , ADD_ACRU_AST_DEPRE_RESER -- 加： 计提的资产减值准备
    , FIX_AST_DEPRE -- 固定资产折旧
    , IMMATRL_AST_AMORT -- 无形资产摊销
    , LONG_TERM_PEND_FEE_AMORT -- 长期待摊费用摊销
    , PEND_FEE_DECRS -- 待摊费用减少（减：增加）
    , ACCR_FEE_INCRS -- 预提费用增加（减：减少）
    , DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_LOSS_SUB_PRFT -- 处置固定资产、无形资产和其他长期资产的损失（减：收益）
    , FIX_AST_SCRAP_LOSS -- 固定资产报废损失
    , FIN_FEE -- 财务费用
    , INVEST_LOSS -- 投资损失（减：收益）
    , DEFER_TAX_LOAN_ITEM -- 递延税款贷项（减：借项）
    , INVTRY_DECRS -- 存货的减少（减：增加）
    , OPERL_RECVBL_PROJ_DECRS -- 经营性应收项目的减少（减：增加）
    , OPERL_PAYBL_PROJ_INCRS -- 经营性应付项目的增加（减：减少）
    , OTHER_NET_PRFT_ADJ_BY_OPER_ACT_CASHFLOW -- 其他将净利润调节为经营活动的现金流量
    , DEBT_TURN_BY_CAP -- 债务转为资本
    , Y1_IN_MATU_CONVTBL_CORP_BOND -- 一年内到期的可转换公司债券
    , FIN_IN_FIX_AST -- 融资租入固定资产
    , OTHER_NOT_INVOL_CASH_BAL_PAY_INVEST_AND_FUNDRS_ACT -- 其他不涉及现金收支的投资和筹资活动
    , CASH_FINAL_BAL -- 现金的期末余额
    , SUB_CASH_BEGIN_BAL -- 减：现金的期初余额
    , ADD_CASH_EQUIV_FINAL_BAL -- 加：现金等价物的期末余额
    , SUB_CASH_EQUIV_BEGIN_BAL -- 减：现金等价物的期初余额
    , CUST_DPSIT_AND_IBANK_DEP_MNYITM_NET_INCRMT -- 客户存款和同业存放款项净增加额
    , _CENTER_BANK_LOAN_NET_INCRMT -- 向中央银行借款净增加额
    , _OTHER_FIN_ORG_BORROW_CAP_NET_INCRMT -- 向其他金融机构拆入资金净增加额
    , COL_INT_COMM_FEE_AND_COMM_CASH -- 收取利息、手续费及佣金的现金
    , CUST_LOAN_AND_ADV_MNY_NET_INCRMT -- 客户贷款及垫款净增加额
    , STORE_CENTER_BANK_AND_IBANK_MNYITM_NET_INCRMT -- 存放中央银行和同业款项净增加额
    , PAY_COMM_FEE_AND_COMM_CASH -- 支付手续费及佣金的现金
    , ISSUE_BOND_RECV_CASH -- 发行债券收到的现金
    , EXCH_RATE_CHG_CORR_CASH_AND_CASH_EQUIV_INFLN -- 汇率变动对现金及现金等价物的影响
    , GET_SUB_CORP_AND_OTHER_OPEN_CORP_PAY_CASH_NET_AMT -- 取得子公司及其他营业单位支付的现金净额
    , DISP_SUB_CORP_AND_OTHER_OPEN_CORP_RECV_CASH_NET_AMT -- 处置子公司及其他营业单位收到的现金净额
    , CREATE_TM -- 创建时间
    , CREATE_LOAN_OFFICER_NO -- 创建信贷员编号
    , CREATE_LOAN_OFFICER_NAME -- 创建信贷员姓名
    , RECNT_MATN_LOAN_OFFICER_NO -- 最近维护信贷员编号
    , RECNT_MATN_LOAN_OFFICER_NAME -- 最近维护信贷员姓名
    , RECNT_MATN_TM -- 最近维护时间
    , RECNT_MATN_ORG_NO -- 最近维护机构编号
    , DATA_DT -- 数据日期
)
select
      CUST_ISN as CUST_IN_CD -- 客户内码
    , RPT_DT as REPORT_YM -- 报表期数
    , BEL_TO_ORG as AFLT_ORG_NO -- 所属机构
    , '4' as SRC_TAB_TYPE_CD -- None
    , T1.CUST_ID as CUST_NO -- 客户号
    , T2.CUST_NAM as CUST_NAME -- 客户名称
    , T3.REG_CCY_ID as REG_CURR_CD -- 注册登记币种
    , T1.RPT_DT_TYP as REPORT_TYPE_CD -- 报表类型年报，季报，月报
    , T4.EN_SZ as CORP_REPORT_TEMPLET_CD -- 企业报表模板
    , T4.FA_RPT_TYP as FIN_STAT_TYPE_CD -- 财务报表类型
    , T4.RPT_AUDIT_IND as AUDIT_FLAG -- 报表是否审计
    , T5.BEL_TO_IDY as INDS_CATE_CD -- 注册行业
    , T1.SC_OR_OL_CASH as SALES_MERCHD_PROVI_LABOR_SERV_RECV_CASH -- 销售商品提供劳务收到的现金
    , T1.RF_OF_TAFR as RECV_TAX_RETURN -- 收到的税费返还
    , T1.OCRR_TO_OA as RECV_OTHER_AND_OPER_ACT_RELAT_CASH -- 收到其他与经营活动有关的现金
    , T1.CASH_OP_IS as OPER_ACT_CASH_INFLOW_SUBTOTAL -- 经营活动现金流入小计
    , T1.CP_FOR_COL as BUY_MERCHD_ACPT_LABOR_SERV_PAY_CASH -- 购买商品、接受劳务支付的现金
    , T1.CP_TO_AND_FE as PAY_GIVE_EMPLY_AND_BY_EMPLY_PAY_CASH -- 支付给职工以及为职工支付的现金
    , T1.TX_AND_FP as PAY_EACH_TAX -- 支付的各项税费
    , T1.OCPR_TO_OA as PAY_OTHER_AND_OPER_ACT_RELAT_CASH -- 来源表类型代码2：支付的其它与经营活动有关的现金
    , T1.CASH_OP_OS as OPER_ACT_CASH_OUTFLOW_SUBTOTAL -- 经营活动现金流出小计
    , T1.CFG_FROM_OAA as OPER_ACT_PROD_CASHFLOW_NET_AMT -- 经营活动产生的现金流量净额
    , T1.CASH_FROM_IW as RETRA_INVEST_RECV_CASH -- 收回投资收到的现金
    , T1.CASH_FM_ITI as GET_INVEST_PRFT_RECV_CASH -- 取得投资收益所收到的现金
    , T1.NC_FM_DFA_IAAOLA as DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_RETRA_CASH_NET_AMT -- 处置固定资产、无形资产和其他长期资产收回的现金净额
    , T1.OCRR_TO_IVT as RECV_OTHER_AND_INVEST_ACT_RELAT_CASH -- 收到其他与投资活动有关的现金
    , T1.CASH_IT_IS as INVEST_ACT_CASH_INFLOW_SUBTOTAL -- 投资活动现金流入小计
    , T1.CP_FOR_BFA_IAAOLI as _FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_PAY_CASH -- 构建固定资产、无形资产和其他长期资产支付的现金
    , T1.CP_FOR_IM as INVEST_PAY_CASH -- 投资支付的现金
    , T1.OCPR_TO_IA as PAY_OTHER_AND_INVEST_ACT_RELAT_CASH -- 支付的其他与投资活动有关的现金
    , T1.CASH_IT_OS as INVEST_ACT_CASH_OUTFLOW_SUBTOTAL -- 投资活动现金流出小计
    , T1.CFG_FM_IAA as INVEST_ACT_PROD_CASHFLOW_NET_AMT -- 投资活动产生的现金流量净额
    , T1.CASH_FM_AI as ABSB_INVEST_RECV_CASH -- 吸收投资收到的现金
    , T1.CASH_RCV_BRW as LOAN_RECV_CASH -- 取得借款收到的现金
    , T1.OCRR_TO_FA as RECV_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 收到其他与筹资活动有关的现金
    , T1.CASH_FN_IS as FUNDRS_ACT_CASH_INFLOW_SUBTOTAL -- 筹资活动现金流入小计
    , T1.CP_FOR_DB as REPAY_DEBT_PAY_CASH -- 偿还债务支付的现金
    , T1.CP_FOR_DV_PI as DISTRI_DIVDND_PRFT_OR_REPAY_INT_PAY_CASH -- 分配股利、利润或偿付利息支付的现金
    , T1.OCPR_TO_FA as PAY_OTHER_AND_FUNDRS_ACT_RELAT_CASH -- 支付的其他与筹资活动有关的现金
    , T1.CASH_FN_OS as FUNDRS_ACT_CASH_OUTFLOW_SUBTOTAL -- 筹资活动现金流出小计
    , T1.CF_FM_FAA as FUNDRS_ACT_PROD_CASHFLOW_NET_AMT -- 筹资活动产生的现金流量净额
    , T1.EFE_RATE_CHG_CASH as EXCH_RATE_CHG_CORR_CASH_INFLN -- 汇率变动对现金及现金等价物的影响
    , T1.NI_OF_CA_CE as CASH_AND_CASH_EQUIV_NET_INCRMT -- 现金及现金等价物净增加额
    , T6.NET_PROFIT as NET_PRFT -- 净利润
    , T6.ADD_PNOFAS_IPTOFAS as ADD_ACRU_AST_DEPRE_RESER -- 加： 计提的资产减值准备
    , T6.DPN_OF_FDAS as FIX_AST_DEPRE -- 固定资产折旧
    , T6.ITB_AS_WD as IMMATRL_AST_AMORT -- 无形资产摊销
    , T6.LT_PDES_RVN as LONG_TERM_PEND_FEE_AMORT -- 长期待摊费用摊销
    , T6.DCE_PD_ES as PEND_FEE_DECRS -- 待摊费用减少（减：增加）
    , T6.ICE_AS_ES as ACCR_FEE_INCRS -- 预提费用增加（减：减少）
    , T6.HD_FAS_IAS_LAS_LS as DISP_FIX_AST_IMMATRL_AST_AND_OTHER_LONG_TERM_AST_LOSS_SUB_PRFT -- 处置固定资产、无形资产和其他长期资产的损失（减：收益）
    , T6.FALOS_FOR_RT as FIX_AST_SCRAP_LOSS -- 固定资产报废损失
    , T6.FNL_EPS as FIN_FEE -- 财务费用
    , T6.IVT_LOS as INVEST_LOSS -- 投资损失（减：收益）
    , T6.DFE_TAX_CD as DEFER_TAX_LOAN_ITEM -- 递延税款贷项（减：借项）
    , T6.DEC_OF_SK as INVTRY_DECRS -- 存货的减少（减：增加）
    , T6.DEC_OF_BS_RS as OPERL_RECVBL_PROJ_DECRS -- 经营性应收项目的减少（减：增加）
    , T6.ICE_OF_BS_RS as OPERL_PAYBL_PROJ_INCRS -- 经营性应付项目的增加（减：减少）
    , T6.OTHER as OTHER_NET_PRFT_ADJ_BY_OPER_ACT_CASHFLOW -- 其他
    , T6.DT_ITO_CL as DEBT_TURN_BY_CAP -- 债务转为资本
    , T6.MWAY_CCB as Y1_IN_MATU_CONVTBL_CORP_BOND -- 一年内到期的可转换公司债券
    , T6.FG_FOR_FFAS as FIN_IN_FIX_AST -- 融资租入固定资产
    , T6.OTHER_TWO as OTHER_NOT_INVOL_CASH_BAL_PAY_INVEST_AND_FUNDRS_ACT -- 其他2
    , T6.CASH_FOR_EDBE as CASH_FINAL_BAL -- 现金的期末余额
    , T6.LS_CASH_FOR_EYBE as SUB_CASH_BEGIN_BAL -- 减：现金的期初余额
    , T6.ADD_CASH_EQT_EDBE as ADD_CASH_EQUIV_FINAL_BAL -- 加：现金等价物的期末余额
    , T6.LS_CASH_EQT_EYBE as SUB_CASH_EQUIV_BEGIN_BAL -- 减：现金等价物的期初余额
    , '' as CUST_DPSIT_AND_IBANK_DEP_MNYITM_NET_INCRMT -- None
    , '' as _CENTER_BANK_LOAN_NET_INCRMT -- None
    , '' as _OTHER_FIN_ORG_BORROW_CAP_NET_INCRMT -- None
    , '' as COL_INT_COMM_FEE_AND_COMM_CASH -- None
    , '' as CUST_LOAN_AND_ADV_MNY_NET_INCRMT -- None
    , '' as STORE_CENTER_BANK_AND_IBANK_MNYITM_NET_INCRMT -- None
    , '' as PAY_COMM_FEE_AND_COMM_CASH -- None
    , '' as ISSUE_BOND_RECV_CASH -- None
    , '' as EXCH_RATE_CHG_CORR_CASH_AND_CASH_EQUIV_INFLN -- None
    , '' as GET_SUB_CORP_AND_OTHER_OPEN_CORP_PAY_CASH_NET_AMT -- None
    , '' as DISP_SUB_CORP_AND_OTHER_OPEN_CORP_RECV_CASH_NET_AMT -- None
    , T1.CRT_TM as CREATE_TM -- 创建时间
    , T1.CRT_LOAN_OFF as CREATE_LOAN_OFFICER_NO -- 创建信贷员
    , T7.NAME as CREATE_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.LAST_LOAN_OFF as RECNT_MATN_LOAN_OFFICER_NO -- 最后维护信贷员
    , T8.NAME as RECNT_MATN_LOAN_OFFICER_NAME -- 信贷人员姓名
    , T1.LAST_TM as RECNT_MATN_TM -- 最后维护时间
    , T1.LAST_ORGAN as RECNT_MATN_ORG_NO -- 最后维护机构
    , '${PROCESS_DATE}' as DATA_DT -- None
from
    ${ODS_XDZX_SCHEMA}.RF_EN_LG_CASH_FLOW as T1 -- 瑞丰新会计准则现金流量表
    LEFT JOIN ${ODS_XDZX_SCHEMA}.CREDIT_CUST_BASE_INFO as T2 -- 信贷客户基础信息表
    on T1.CUST_ISN = T2.CUST_ISN 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.EN_CUST_EXPD_ECIF as T3 -- 对公客户扩展信息表
    on T1.CUST_ISN =T3.CUST_ISN 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_FNC_DOC_INFO as T4 -- 财务资料公共信息表
    on T4.CUST_ISN = T1.CUST_ISN and
T4.RPT_DT = T1.RPT_DT and 
T4.BEL_TO_ORG = T1.BEL_TO_ORG  
    LEFT JOIN ${ODS_XDZX_SCHEMA}.PUB_CUST_BASE_INFO as T5 -- 客户公共基础信息表
    on T5.CUST_ISN = T5.CUST_ISN 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.EN_LG_CASH_FLOW_SCHEDULE as T6 -- 大中型企业现金流量表附表
    on T6.CUST_ISN = T1.CUST_ISN and
T6.RPT_DT = T1.RPT_DT and 
T6.BEL_TO_ORG = T1.BEL_TO_ORG  
    LEFT JOIN ${ODS_XDZX_SCHEMA}.Users as T7 -- 用户表
    on T7.STAFFER_NO =T1.CRT_LOAN_OFF 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.Users as T8 -- 用户表
    on T8.STAFFER_NO = T1.LAST_LOAN_OFF 
where 1=1 
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;

-- 删除所有临时表