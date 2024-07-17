-- 层次表名: 聚合层-法人贷款房地产项目聚合表（瑞丰）
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s03_real_estate_proj_loan_tab
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S03_REAL_ESTATE_PROJ_LOAN_TAB
--     表中文名：法人贷款房地产项目聚合表（瑞丰）
--     创建日期：2023-12-28 00:00:00
--     主键字段：CUST_IN_CD,PROJ_TYPE,PROJ_ID
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：包括了土地储备项目贷款、开发园区项目贷款、房地产项目贷款的信息（只包含瑞丰的数据）
--     更新记录：
--         2023-12-28 00:00:00 王穆军 新增映射文档
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S03_REAL_ESTATE_PROJ_LOAN_TAB drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 法人贷款房地产项目聚合_合作方信息
drop table if exists ${session}.TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_01;

create table ${session}.TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_01 (
      CUST_ISN varchar(100) -- 客户内码
    , AGGRE_INVEST decimal(18,2) -- 项目投资总额
    , SHAREHOLD_DEV_QUALI varchar(255) -- 股东开发资质
    , CAPITAL_FUND decimal(18,2) -- 楼盘资本金
    , OTHER_FINANCING decimal(18,2) -- 楼盘其他融资金额
    , MORT_SITU_OF_OTH_BANK varchar(1024) -- 楼盘他行按揭情况(按揭银行/额度)
    , PROJECT_NAME varchar(255) -- 合作项目名称
    , CUST_NAME varchar(255) -- 合作方名称
    , LEGAL_PERSON varchar(255) -- 法定代表人名称
    , ACTUAL_CTRL_PERSON varchar(255) -- 实际控制人名称
    , REG_CAPITAL decimal(20,2) -- 注册资本金额
    , REG_ADDR varchar(255) -- 注册地
    , DEV_QUALI varchar(255) -- 开发资质
    , PRE_SALE_FUND_BANK varchar(255) -- 预售资金专用存款账户开户行名称
    , MORGAGE_BUILDING_NAME varchar(255) -- 按揭楼盘名称
    , MORGAGE_COOP_AREA varchar(255) -- 本次按揭楼盘合作范围
    , ORIGIN_COOP_AMOUNT decimal(20,2) -- 原合作额度
    , GUARANT_SITUATION varchar(255) -- 原合作方项目额度担保情况
    , ISSUED_GUARANT_SITUATION varchar(255) -- 新合作方项目额度担保情况
    , ISSUED_AMOUNT decimal(20,2) -- 合作方项目已发放额度
    , FOUR_CERTS_AUDIT_FLAG varchar(20) -- 合作方四证审批情况
    , BUILDING_NAME varchar(255) -- 楼盘名称
    , BUILDING_ADDR varchar(255) -- 楼盘坐落位置
    , DEV_HOUSE_TYPE varchar(64) -- 合作楼盘开发房产类型
    , OPEN_DATE date -- 楼盘开盘日期
    , LAND_TYPE varchar(64) -- 合作楼盘用途
    , LAND_EFFECTIVE_DATE date -- 合作楼盘土地生效日期
    , BUY_PRICE decimal(20,2) -- 合作楼盘购入价
    , CURR_ASSET decimal(20,2) -- 合作方当期资产
    , CURR_NET_ASSET decimal(20,2) -- 合作方当期净资产
    , CURR_SALE decimal(20,2) -- 合作方当期销售
    , LAST_YEAR_SALE decimal(20,2) -- 合作方上年销售
    , DEBT_RATE decimal(10,2) -- 合作方负债率
    , CURR_PROFIT decimal(20,2) -- 合作方当期利润
    , LAST_YEAR_PROFIT decimal(20,2) -- 合作方上年利润
    , BANK_LOAN decimal(20,2) -- 合作方银行贷款
    , OUT_GUARANT decimal(20,2) -- 合作方对外担保
    , REPORT_CODE varchar(64) -- 尽职调查报告编号
    , OTHER_COST decimal(18,2) -- 楼盘其他费用
    , AMOUNT_INVESTED decimal(18,2) -- 楼盘已投入金额
    , CAPITAL_RATIO decimal(5,2) -- 资本金比例
    , BANK_FINANCING decimal(18,2) -- 银行融资
    , HOUSE_TYPE varchar(20) -- 房屋类型代码
    , NUM_OF_PRESALE_CER integer(10) -- 领取预售证套数
    , AREA_OF_PRESALE_CER decimal(18,2) -- 领取预售证面积
    , EST_AVER_SALES_PRICE decimal(18,2) -- 预计销售均价
    , NUM_OF_UNITS_SOLD integer(10) -- 楼盘已销售套数
    , AREA_SOLD decimal(18,2) -- 楼盘已销售面积
)
comment '法人贷款房地产项目聚合_合作方信息'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_01(
      CUST_ISN -- 客户内码
    , AGGRE_INVEST -- 项目投资总额
    , SHAREHOLD_DEV_QUALI -- 股东开发资质
    , CAPITAL_FUND -- 楼盘资本金
    , OTHER_FINANCING -- 楼盘其他融资金额
    , MORT_SITU_OF_OTH_BANK -- 楼盘他行按揭情况(按揭银行/额度)
    , PROJECT_NAME -- 合作项目名称
    , CUST_NAME -- 合作方名称
    , LEGAL_PERSON -- 法定代表人名称
    , ACTUAL_CTRL_PERSON -- 实际控制人名称
    , REG_CAPITAL -- 注册资本金额
    , REG_ADDR -- 注册地
    , DEV_QUALI -- 开发资质
    , PRE_SALE_FUND_BANK -- 预售资金专用存款账户开户行名称
    , MORGAGE_BUILDING_NAME -- 按揭楼盘名称
    , MORGAGE_COOP_AREA -- 本次按揭楼盘合作范围
    , ORIGIN_COOP_AMOUNT -- 原合作额度
    , GUARANT_SITUATION -- 原合作方项目额度担保情况
    , ISSUED_GUARANT_SITUATION -- 新合作方项目额度担保情况
    , ISSUED_AMOUNT -- 合作方项目已发放额度
    , FOUR_CERTS_AUDIT_FLAG -- 合作方四证审批情况
    , BUILDING_NAME -- 楼盘名称
    , BUILDING_ADDR -- 楼盘坐落位置
    , DEV_HOUSE_TYPE -- 合作楼盘开发房产类型
    , OPEN_DATE -- 楼盘开盘日期
    , LAND_TYPE -- 合作楼盘用途
    , LAND_EFFECTIVE_DATE -- 合作楼盘土地生效日期
    , BUY_PRICE -- 合作楼盘购入价
    , CURR_ASSET -- 合作方当期资产
    , CURR_NET_ASSET -- 合作方当期净资产
    , CURR_SALE -- 合作方当期销售
    , LAST_YEAR_SALE -- 合作方上年销售
    , DEBT_RATE -- 合作方负债率
    , CURR_PROFIT -- 合作方当期利润
    , LAST_YEAR_PROFIT -- 合作方上年利润
    , BANK_LOAN -- 合作方银行贷款
    , OUT_GUARANT -- 合作方对外担保
    , REPORT_CODE -- 尽职调查报告编号
    , OTHER_COST -- 楼盘其他费用
    , AMOUNT_INVESTED -- 楼盘已投入金额
    , CAPITAL_RATIO -- 资本金比例
    , BANK_FINANCING -- 银行融资
    , HOUSE_TYPE -- 房屋类型代码
    , NUM_OF_PRESALE_CER -- 领取预售证套数
    , AREA_OF_PRESALE_CER -- 领取预售证面积
    , EST_AVER_SALES_PRICE -- 预计销售均价
    , NUM_OF_UNITS_SOLD -- 楼盘已销售套数
    , AREA_SOLD -- 楼盘已销售面积
)
select
      T1.CUST_ISN as CUST_ISN -- 客户内码
    , T3.AGGRE_INVEST as AGGRE_INVEST -- 项目投资总额
    , T2.SHAREHOLD_DEV_QUALI as SHAREHOLD_DEV_QUALI -- 股东开发资质
    , T3.CAPITAL_FUND as CAPITAL_FUND -- 资本金
    , T3.OTHER_FINANCING as OTHER_FINANCING -- 其他融资
    , T3.MORT_SITU_OF_OTH_BANK as MORT_SITU_OF_OTH_BANK -- 他行按揭情况(按揭银行/额度)
    , T1.PROJECT_NAME as PROJECT_NAME -- 合作项目名称
    , T1.CUST_NAME as CUST_NAME -- 合作方名称
    , T2.LEGAL_PERSON as LEGAL_PERSON -- 法定代表人
    , T2.ACTUAL_CTRL_PERSON as ACTUAL_CTRL_PERSON -- 实际控制人
    , T2.REG_CAPITAL as REG_CAPITAL -- 注册资本
    , T2.REG_ADDR as REG_ADDR -- 注册地
    , T2.DEV_QUALI as DEV_QUALI -- 开发资质
    , T2.PRE_SALE_FUND_BANK as PRE_SALE_FUND_BANK -- 预售资金专用存款账户开户行
    , T2.MORGAGE_BUILDING_NAME as MORGAGE_BUILDING_NAME -- 按揭楼盘名称
    , T2.MORGAGE_COOP_AREA as MORGAGE_COOP_AREA -- 本次按揭楼盘合作范围
    , T2.ORIGIN_COOP_AMOUNT as ORIGIN_COOP_AMOUNT -- 原合作额度
    , T2.GUARANT_SITUATION as GUARANT_SITUATION -- 担保情况
    , T2.ISSUED_GUARANT_SITUATION as ISSUED_GUARANT_SITUATION -- 担保情况
    , T2.ISSUED_AMOUNT as ISSUED_AMOUNT -- 已发放余额
    , T2.FOUR_CERTS_AUDIT_FLAG as FOUR_CERTS_AUDIT_FLAG -- 四证审批情况
    , T4.BUILDING_NAME as BUILDING_NAME -- 楼盘名称
    , T4.BUILDING_ADDR as BUILDING_ADDR -- 坐落位置
    , T4.DEV_HOUSE_TYPE as DEV_HOUSE_TYPE -- 开发房产类型
    , T4.OPEN_DATE as OPEN_DATE -- 开盘日期
    , T4.LAND_TYPE as LAND_TYPE -- 地类(用途)
    , T4.LAND_EFFECTIVE_DATE as LAND_EFFECTIVE_DATE -- 土地生效日期
    , T4.BUY_PRICE as BUY_PRICE -- 购入价
    , T5.CURR_ASSET as CURR_ASSET -- 当期资产
    , T5.CURR_NET_ASSET as CURR_NET_ASSET -- 当期净资产
    , T5.CURR_SALE as CURR_SALE -- 当期销售
    , T5.LAST_YEAR_SALE as LAST_YEAR_SALE -- 上年销售
    , T5.DEBT_RATE as DEBT_RATE -- 负债率
    , T5.CURR_PROFIT as CURR_PROFIT -- 当期利润
    , T5.LAST_YEAR_PROFIT as LAST_YEAR_PROFIT -- 上年利润
    , T5.BANK_LOAN as BANK_LOAN -- 银行贷款
    , T5.OUT_GUARANT as OUT_GUARANT -- 对外担保
    , T3.REPORT_CODE as REPORT_CODE -- 报告编号
    , T3.OTHER_COST as OTHER_COST -- 其他费用
    , T3.AMOUNT_INVESTED as AMOUNT_INVESTED -- 已投入金额
    , T3.CAPITAL_RATIO as CAPITAL_RATIO -- 资本金比例
    , T3.BANK_FINANCING as BANK_FINANCING -- 银行融资
    , T6.HOUSE_TYPE as HOUSE_TYPE -- 房屋类型
    , T6.NUM_OF_PRESALE_CER as NUM_OF_PRESALE_CER -- 领取预售证套数
    , T6.AREA_OF_PRESALE_CER as AREA_OF_PRESALE_CER -- 领取预售证面积
    , T6.EST_AVER_SALES_PRICE as EST_AVER_SALES_PRICE -- 预计销售均价
    , T6.NUM_OF_UNITS_SOLD as NUM_OF_UNITS_SOLD -- 已销售套数
    , T6.AREA_SOLD as AREA_SOLD -- 已销售面积
from
    (SELECT 
	P1.CUST_ISN
	,P1.PROJECT_NAME
	,P1.CUST_NAME
	,ROW_NUMBER()OVER(PARTITION BY P1.CUST_ISN 
			ORDER BY P1.LAST_TM DESC )RN 
FROM ${ODS_XDZX_SCHEMA}.RF_PROJECT_PARTER_BASE P1
WHERE 1=1 
	AND P1.PT_DT='${process_date}' 
	AND P1.DELETED='0') as T1 -- None
    LEFT JOIN ${ODS_XDZX_SCHEMA}.RF_COOP_DUTE_REPORT as T2 -- 瑞丰-合作方尽职调查报告
    on T1. CUST_ISN  = T2.CUST_ISN 
AND T1.PROJECT_PRO_NUMBER = T2.PROJECT_PRO_NUMBER
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.RF_DEVE_PROC_AND_FINANCING as T3 -- 瑞丰-楼盘进度和资金筹集
    on T2.CUST_ISN = T3.CUST_ISN  
AND T2.REPORT_CODE = T3.REPORT_CODE
AND T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.RF_COOP_CURR_BUILDING as T4 -- 瑞丰-尽职调查报告-本次合作楼盘情况
    on T2.REPORT_CODE =T4.REPORT_CODE
AND T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.RF_COOP_HOUSE_DEBT as T5 -- 瑞丰-尽职调查报告-房地产企业资产负债情况
    on T2.CUST_ISN = T5.CUST_ISN 
AND T2.REPORT_CODE = T5.REPORT_CODE
AND T5.PT_DT='${process_date}' 
AND T5.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.RF_ESTATE_SALES as T6 -- 瑞丰-楼盘销售情况
    on T2.CUST_ISN = T6.CUST_ISN 
AND T2.REPORT_CODE = T6.REPORT_CODE
AND T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
where T1.RN = 1
;
-- ==============字段映射（第2组）==============
-- 法人贷款房地产项目聚合_合作方信息
drop table if exists ${session}.TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_02;

create table ${session}.TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_02 (
      CUST_ISN cust_isn -- 客户内码
    , PROJECT_TYPE project_type -- 项目类别
    , PROJECT_ID project_id -- 项目序号
    , CUST_NAM cust_nam -- 客户名称
    , BEL_ORG_NO bel_org_no -- 所属机构
    , PROJECT_LOAN_NO project_loan_no -- 项目贷款编号
    , PROJECT_NAME project_name -- 项目名称
    , PROJECT_ADDR project_addr -- 项目地址
    , COVER_AREA cover_area -- 项目占地面积
    , CURRENCY currency -- 项目币种
    , OVERALL_FLOORAGE overall_floorage -- 房地产项目总建筑面积
    , OTHER_AREA other_area -- 房地产项目其他建筑面积
    , PROJECT_PLOT_RATIO project_plot_ratio -- 房地产项目容积率
    , START_DATE start_date -- 开工日期
    , PLAN_END_DATE plan_end_date -- 计划竣工日期
    , CONSTR_LAND_USE_PERMIT constr_land_use_permit -- 建设用地规划许可证件
    , LAND_USE_CERTIFICATE land_use_certificate -- 土地使用权证书
    , CONSTR_PLAN_PERMIT constr_plan_permit -- 建筑工程规划许可证
    , CONSTR_BUILD_PLAN_PERMIT constr_build_plan_permit -- 建筑工程施工许可证
    , PRE_SALE_PERMIT pre_sale_permit -- 商品房预售许可证
    , SUPERVISION_NAME_QUA_LEVEL supervision_name_qua_level -- 房地产项目监理单位名称及资质等级
    , STORE_AREA store_area -- 商铺面积
    , OFFICES_AREA offices_area -- 写字楼面积
    , LOAN_INOUR_BANK loan_inour_bank -- 土地开发项目我行贷款
    , PROJECT_PROGRESS project_progress -- 项目当前进度
    , TOTAL_CONSTR_PERIOD total_constr_period -- 房地产项目总建设期数
    , GREEN_LOAN_SIGN green_loan_sign -- 绿色贷款标志
    , PROJECT_SUB_TYPE project_sub_type -- 房地产项目子类型代码
    , PROJECT_LEVEL project_level -- 项目级别
    , PROJECT_CONSTR_CONTENT project_constr_content -- 项目建设内容
    , TOTAL_PLAN_INVESTMENT total_plan_investment -- 房地产项目计划总投资
    , CONTRACTOR_NAME_QUA contractor_name_qua -- 承建单位名称及资质
    , CAPITAL_SOURCE_NAME capital_source_name -- 资金来源机构名称/融资银行名称/出资人名称
    , INVESTMENT_AMOUNT investment_amount -- 项目投资金额/融资金额
    , AMOUNT_INVESTED amount_invested -- 项目当前已投资金额
    , LAND_COST land_cost -- 项目投资构成中土地费用
    , PROJECT_COST project_cost -- 项目投资构成中工程费用/前期工程费
    , EQUIPMENT_COST equipment_cost -- 项目投资构成中设备费用
    , CONT_INTEREST cont_interest -- 项目投资构成中建设期利息
    , WORKING_FUND working_fund -- 项目投资构成中流动资金
    , OTHER_FEES other_fees -- 项目投资构成中其他费用
    , COMPENSATION_FEE compensation_fee -- 拆迁补偿费
    , CRE_QUOTA cre_quota -- 客户时点授信额度
    , CREDIT_TOTAL credit_total -- 客户时点综合授信额度
    , OPEN_TOTAL open_total -- 客户时点敞口授信额度
    , LOAN_PURPOSE_CODE loan_purpose_code -- 行业投向代码
    , LOAN_PURPOSE_NAME loan_purpose_name -- 行业投向名称
    , CUST_TYP cust_typ -- 合作方类型
    , CRT_ORGAN crt_organ -- 创建机构编号
    , CRT_LOAN_OFF crt_loan_off -- 创建柜员编号
    , CRT_TM crt_tm -- 创建时间
    , LAST_TM last_tm -- 修改时间
)
comment '法人贷款房地产项目聚合_合作方信息'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_02(
      CUST_ISN -- 客户内码
    , PROJECT_TYPE -- 项目类别
    , PROJECT_ID -- 项目序号
    , CUST_NAM -- 客户名称
    , BEL_ORG_NO -- 所属机构
    , PROJECT_LOAN_NO -- 项目贷款编号
    , PROJECT_NAME -- 项目名称
    , PROJECT_ADDR -- 项目地址
    , COVER_AREA -- 项目占地面积
    , CURRENCY -- 项目币种
    , OVERALL_FLOORAGE -- 房地产项目总建筑面积
    , OTHER_AREA -- 房地产项目其他建筑面积
    , PROJECT_PLOT_RATIO -- 房地产项目容积率
    , START_DATE -- 开工日期
    , PLAN_END_DATE -- 计划竣工日期
    , CONSTR_LAND_USE_PERMIT -- 建设用地规划许可证件
    , LAND_USE_CERTIFICATE -- 土地使用权证书
    , CONSTR_PLAN_PERMIT -- 建筑工程规划许可证
    , CONSTR_BUILD_PLAN_PERMIT -- 建筑工程施工许可证
    , PRE_SALE_PERMIT -- 商品房预售许可证
    , SUPERVISION_NAME_QUA_LEVEL -- 房地产项目监理单位名称及资质等级
    , STORE_AREA -- 商铺面积
    , OFFICES_AREA -- 写字楼面积
    , LOAN_INOUR_BANK -- 土地开发项目我行贷款
    , PROJECT_PROGRESS -- 项目当前进度
    , TOTAL_CONSTR_PERIOD -- 房地产项目总建设期数
    , GREEN_LOAN_SIGN -- 绿色贷款标志
    , PROJECT_SUB_TYPE -- 房地产项目子类型代码
    , PROJECT_LEVEL -- 项目级别
    , PROJECT_CONSTR_CONTENT -- 项目建设内容
    , TOTAL_PLAN_INVESTMENT -- 房地产项目计划总投资
    , CONTRACTOR_NAME_QUA -- 承建单位名称及资质
    , CAPITAL_SOURCE_NAME -- 资金来源机构名称/融资银行名称/出资人名称
    , INVESTMENT_AMOUNT -- 项目投资金额/融资金额
    , AMOUNT_INVESTED -- 项目当前已投资金额
    , LAND_COST -- 项目投资构成中土地费用
    , PROJECT_COST -- 项目投资构成中工程费用/前期工程费
    , EQUIPMENT_COST -- 项目投资构成中设备费用
    , CONT_INTEREST -- 项目投资构成中建设期利息
    , WORKING_FUND -- 项目投资构成中流动资金
    , OTHER_FEES -- 项目投资构成中其他费用
    , COMPENSATION_FEE -- 拆迁补偿费
    , CRE_QUOTA -- 客户时点授信额度
    , CREDIT_TOTAL -- 客户时点综合授信额度
    , OPEN_TOTAL -- 客户时点敞口授信额度
    , LOAN_PURPOSE_CODE -- 行业投向代码
    , LOAN_PURPOSE_NAME -- 行业投向名称
    , CUST_TYP -- 合作方类型
    , CRT_ORGAN -- 创建机构编号
    , CRT_LOAN_OFF -- 创建柜员编号
    , CRT_TM -- 创建时间
    , LAST_TM -- 修改时间
)
select
      T1.CUST_ISN as CUST_ISN -- 客户内码
    , T1.PROJECT_TYPE as PROJECT_TYPE -- 项目类别
    , T1.PROJECT_ID as PROJECT_ID -- 项目ID
    , T2.CUST_NAM as CUST_NAM -- 客户名称
    , T4.BEL_ORG_NO as BEL_ORG_NO -- 所属机构
    , T4.PROJECT_LOAN_NO as PROJECT_LOAN_NO -- 项目贷款编号
    , T5.PROJECT_NAME as PROJECT_NAME -- 项目名字
    , T5.PROJECT_ADDR as PROJECT_ADDR -- 项目地址
    , T5.COVER_AREA as COVER_AREA -- 占地面积
    , T1.CURRENCY as CURRENCY -- 币种
    , T5.OVERALL_FLOORAGE as OVERALL_FLOORAGE -- 总建筑面积
    , T5.OTHER_AREA as OTHER_AREA -- 其他面积
    , T5.PROJECT_PLOT_RATIO as PROJECT_PLOT_RATIO -- 项目容积率
    , T5.START_DATE as START_DATE -- 开工日期
    , T5.PLAN_END_DATE as PLAN_END_DATE -- 计划竣工日期
    , T5.CONSTR_LAND_USE_PERMIT as CONSTR_LAND_USE_PERMIT -- 建设用地规划许可证件
    , T5.LAND_USE_CERTIFICATE as LAND_USE_CERTIFICATE -- 土地使用权证书
    , T5.CONSTR_PLAN_PERMIT as CONSTR_PLAN_PERMIT -- 建筑工程规划许可证
    , T5.CONSTR_BUILD_PLAN_PERMIT as CONSTR_BUILD_PLAN_PERMIT -- 建筑工程施工许可证
    , T5.PRE_SALE_PERMIT as PRE_SALE_PERMIT -- 商品房预售许可证
    , T5.SUPERVISION_NAME_QUA_LEVEL as SUPERVISION_NAME_QUA_LEVEL -- 监理单位名称及资质等级
    , T5.STORE_AREA as STORE_AREA -- 商铺面积
    , T5.OFFICES_AREA as OFFICES_AREA -- 写字楼面积
    , T6.LOAN_INOUR_BANK as LOAN_INOUR_BANK -- 土地开发项目我行贷款
    , T7.PROJECT_PROGRESS as PROJECT_PROGRESS -- 项目当前进度
    , T5.TOTAL_CONSTR_PERIOD as TOTAL_CONSTR_PERIOD -- 总建设期数
    , T3.GREEN_LOAN_SIGN as GREEN_LOAN_SIGN -- 绿色贷款标志
    , T5.PROJECT_SUB_TYPE as PROJECT_SUB_TYPE -- 项目子类型
    , T5.PROJECT_LEVEL as PROJECT_LEVEL -- 项目级别
    , T5.PROJECT_CONSTR_CONTENT as PROJECT_CONSTR_CONTENT -- 项目建设内容
    , T5.TOTAL_PLAN_INVESTMENT as TOTAL_PLAN_INVESTMENT -- 计划总投资
    , T5.CONTRACTOR_NAME_QUA as CONTRACTOR_NAME_QUA -- 承建单位名称及资质
    , T1.CAPITAL_SOURCE_NAME as CAPITAL_SOURCE_NAME -- 资金来源机构名称/融资银行名称/出资人名称
    , T1.INVESTMENT_AMOUNT as INVESTMENT_AMOUNT -- 投资金额/融资金额
    , T7.AMOUNT_INVESTED as AMOUNT_INVESTED -- 已投资金额
    , T8.LAND_COST as LAND_COST -- 土地费用
    , T8.PROJECT_COST as PROJECT_COST -- 工程费用/前期工程费
    , T8.EQUIPMENT_COST as EQUIPMENT_COST -- 设备费用
    , T8.CONT_INTEREST as CONT_INTEREST -- 建设期利息
    , T8.WORKING_FUND as WORKING_FUND -- 流动资金
    , T8.OTHER_FEES as OTHER_FEES -- 其他费用
    , T6.COMPENSATION_FEE as COMPENSATION_FEE -- 拆迁、补偿费
    , T3.CRE_QUOTA as CRE_QUOTA -- 授信额度
    , T4.CREDIT_TOTAL as CREDIT_TOTAL -- 综合授信额度
    , T4.OPEN_TOTAL as OPEN_TOTAL -- 敞口授信额度
    , T4.LOAN_PURPOSE_CODE as LOAN_PURPOSE_CODE -- 行业投向代码
    , T4.LOAN_PURPOSE_NAME as LOAN_PURPOSE_NAME -- 行业投向名称
    , T9.CUST_TYP as CUST_TYP -- 合作方类型
    , T1.CRT_ORGAN as CRT_ORGAN -- 登记人
    , T1.CRT_LOAN_OFF as CRT_LOAN_OFF -- 创建柜员
    , T1.CRT_TM as CRT_TM -- 登记时间
    , T1.LAST_TM as LAST_TM -- 更新时间
from
    ${ODS_XDZX_SCHEMA}.RF_PROJECT_CAPITAL_SOURCE as T1 -- 项目资金来源表
    ${ODS_XDZX_SCHEMA}.RF_QUA_AND_CERT_HOUSE as T2 -- 瑞丰-客户资质与认证-房地产资质
    on T1.CUST_ISN = T2.CUST_ISN
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    (
SELECT 
	P1.CUST_ISN
	,P1.GREEN_LOAN_SIGN
	,P1.CRE_QUOTA
	,P1.AGATE_CODE

	,ROW_NUMBER()OVER(PARTITION BY P1.CUST_ISN 
			ORDER BY P1.UPDATE_TIME DESC )RN 
FROM ${ODS_XDZX_SCHEMA}.RF_ORG_CREDIT_BEFORE_APPROVAL_MODIFY P1
WHERE 1=1 
	AND P1.PT_DT='${process_date}' 
	AND P1.DELETED='0'
) as T3 -- 对公客户授信前置信息审批调整表
    on T1.CUST_ISN = T3.CUST_ISN
AND T3.PT_DT='${process_date}' 
AND T3.DELETED='0'
AND T3.RN = 1 
    ${ODS_XDZX_SCHEMA}.RF_ORG_CREDIT_BEFORE_DETAILS as T4 -- 对公客户授信前置细项表
    on T4.CUST_ISN = T3.CUST_ISN 
ANDD  T3.AGATE_CODE =T4.AGATE_CODE
AND T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    ${ODS_XDZX_SCHEMA}.RF_PROJECT_REALTY as T5 -- 房地产项目信息表
    on T1.PROJECT_ID= T5.ID
AND  T1.CUST_ISN = T5.CUST_ISN 
AND T1.PROJECT_TYPE = T5.PROJECT_TYPE
AND T5.PT_DT='${process_date}' 
AND T5.DELETED='0' 
    ${ODS_XDZX_SCHEMA}.RF_PROJECT_LAND_DEVELOP_BANK as T6 -- 土地储备/开发区项目信息表
    on T1.PROJECT_ID= T6.PROJECT_ID 
AND T1.CUST_ISN = T6.CUST_ISN
AND  T1.PROJECT_TYPE = T6.PROJECT_TYPE
AND T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
    ${ODS_XDZX_SCHEMA}.RF_PROJECT_PROGRESS_INFO as T7 -- 项目进展表
    on T1.PROJECT_ID=T7.PROJECT_ID 
AND   T1.CUST_ISN = T7.CUST_ISN 
AND   T1.PROJECT_TYPE = T7.PROJECT_TYPE
AND T7.PT_DT='${process_date}' 
AND T7.DELETED='0' 
    ${ODS_XDZX_SCHEMA}.RF_PROJECT_INPUT_OUTPUT_MEASURE as T8 -- 项目投入产出测算表
    on T1.PROJECT_ID= T8.PROJECT_ID 
AND  T1.CUST_ISN = T8.CUST_ISN 
AND T1.PROJECT_TYPE = T8.PROJECT_TYPE
AND T8.PT_DT='${process_date}' 
AND T8.DELETED='0' 
    ${ODS_XDZX_SCHEMA}.RF_PARTER as T9 -- 合作方信息表
    on T1.CUST_ISN =T9. CUST_ISN
AND T9.PT_DT='${process_date}' 
AND T9.DELETED='0' 
where 1=1 
AND T1.PROJECT_TYPE = '2'
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第3组）==============
-- 数据汇总插入

insert into table ${session}.S03_REAL_ESTATE_PROJ_LOAN_TAB(
      CUST_IN_CD -- 客户内码
    , PROJ_TYPE -- 项目类别
    , PROJ_ID -- 项目序号
    , CUST_NAME -- 客户名称
    , AFLT_ORG -- 所属机构
    , PROJ_LOAN_NO -- 项目贷款编号
    , PROJ_NAME -- 项目名称
    , PROJ_ADDR -- 项目地址
    , PROJ_TOTAL_INVEST -- 项目投资总额
    , SHAREHD_DVLP_QLF -- 股东开发资质
    , PROJ_OCPY_LAND_AREA -- 项目占地面积
    , PROJ_CURR -- 项目币种
    , ESTATE_PROJ_TOTAL_BUILD_AREA -- 房地产项目总建筑面积
    , ESTATE_PROJ_OTHER_BUILD_AREA -- 房地产项目其他建筑面积
    , ESTATE_PROJ_PLOT_RATIO -- 房地产项目容积率
    , COMMEN_DT -- 开工日期
    , PLAN_CMPLT_DT -- 计划竣工日期
    , CNSTR_LAND_PLAN_LIC_DOC -- 建设用地规划许可证件
    , LAND_USE_RIGHT_CERT_BOOK -- 土地使用权证书
    , BUILD_PLAN_LIC -- 建筑工程规划许可证
    , BUILD_CNSTR_LIC -- 建筑工程施工许可证
    , COMD_HOS_PRESELL_LIC -- 商品房预售许可证
    , ESTATE_PROJ_SUPVSR_CORP_NAME_AND_QLF_LVL -- 房地产项目监理单位名称及资质等级
    , PRMS_CAP -- 楼盘资本金
    , PRMS_OTHER_FIN_AMT -- 楼盘其他融资金额
    , MALL_AREA -- 商铺面积
    , OFFICES_AREA -- 写字楼面积
    , LAND_DVLP_PROJ_OBANK_LOAN -- 土地开发项目我行贷款
    , PROJ_CURR_SCHED -- 项目当前进度
    , PRMS_OTHER_BANK_MTG_SITU -- 楼盘他行按揭情况(按揭银行/额度)
    , ESTATE_PROJ_TOTAL_CNSTR_TMS -- 房地产项目总建设期数
    , GREEN_LOAN_FLAG -- 绿色贷款标志
    , ESTATE_PROJ_SUB_TYPE_CD -- 房地产项目子类型代码
    , CO_PROJ_NAME -- 合作项目名称
    , PROJ_LVL -- 项目级别
    , PROJ_CNSTR_CONTENT -- 项目建设内容
    , ESTATE_PROJ_PLAN_TOTAL_INVEST -- 房地产项目计划总投资
    , CNSTR_CORP_NAME_AND_QLF -- 承建单位名称及资质
    , CAP_SRC_ORG_NAME_FIN_BANK_NAME_INVESTOR_NAME -- 资金来源机构名称/融资银行名称/出资人名称
    , PROJ_INVEST_AMT_FIN -- 项目投资金额/融资金额
    , PROJ_CURR_AL_INVEST_AMT -- 项目当前已投资金额
    , PROJ_INVEST_CONST_MDL_LAND_FEE -- 项目投资构成中土地费用
    , PROJ_INVEST_CONST_MDL_PROJ_FEE_EARLY_DAYS_PROJ_FEE -- 项目投资构成中工程费用/前期工程费
    , PROJ_INVEST_CONST_MDL_EQUIP_FEE -- 项目投资构成中设备费用
    , PROJ_INVEST_CONST_MDL_BUILD_PERIOD_INT -- 项目投资构成中建设期利息
    , PROJ_INVEST_CONST_MDL_WORK_CAP -- 项目投资构成中流动资金
    , PROJ_INVEST_CONST_MDL_OTHER_FEE -- 项目投资构成中其他费用
    , REMOVE_COMP -- 拆迁补偿费
    , CUST_TM_POINT_CRDT_LINE -- 客户时点授信额度
    , CUST_TM_POINT_SYN_CRDT_LINE -- 客户时点综合授信额度
    , CUST_TM_POINT_OPEN_CRDT_LINE -- 客户时点敞口授信额度
    , INDUS_INVEST_CD -- 行业投向代码
    , INDUS_INVEST_NAME -- 行业投向名称
    , COOR_TYPE -- 合作方类型
    , COOR_NAME -- 合作方名称
    , LEGAL_REP_NAME -- 法定代表人名称
    , ACTL_CTRLER_NAME -- 实际控制人名称
    , REG_CAP_AMT -- 注册资本金额
    , REG_ADDR -- 注册地
    , DVLP_QLF -- 开发资质
    , PRESELL_CAP_SPEC_DPSIT_ACCT_OPBANK_NAME -- 预售资金专用存款账户开户行名称
    , MTG_PRMS_NAME -- 按揭楼盘名称
    , TH_TM_MTG_PRMS_CO_SCOPE -- 本次按揭楼盘合作范围
    , ORIG_CO_LMT -- 原合作额度
    , ORIG_COOR_PROJ_LMT_GUAR_SITU -- 原合作方项目额度担保情况
    , NEW_COOR_PROJ_LMT_GUAR_SITU -- 新合作方项目额度担保情况
    , COOR_PROJ_ISSUED_LMT -- 合作方项目已发放额度
    , COOR_FRTH_PRV_APPRV_SITU -- 合作方四证审批情况
    , PRMS_NAME -- 楼盘名称
    , PRMS_LOCAT_POSITION -- 楼盘坐落位置
    , CO_PRMS_DVLP_ESTATE_TYPE -- 合作楼盘开发房产类型
    , PRMS_OPEN_QUOT_DT -- 楼盘开盘日期
    , CO_PRMS_USAGE -- 合作楼盘用途
    , CO_PRMS_LAND_EFFECT_DT -- 合作楼盘土地生效日期
    , CO_PRMS_BUY_PRC -- 合作楼盘购入价
    , COOR_CURR_PERIOD_AST -- 合作方当期资产
    , COOR_CURR_PERIOD_NET_AST -- 合作方当期净资产
    , COOR_CURR_PERIOD_SALES -- 合作方当期销售
    , COOR_LAST_YEAR_SALES -- 合作方上年销售
    , COOR_LIAB_RATE -- 合作方负债率
    , COOR_CURR_PERIOD_PRFT -- 合作方当期利润
    , COOR_LAST_YEAR_PRFT -- 合作方上年利润
    , COOR_BANK_LOAN -- 合作方银行贷款
    , COOR_FORGN_GUAR -- 合作方对外担保
    , DUE_DIL_INVSTG_REPORT_NO -- 尽职调查报告编号
    , PRMS_OTHER_FEE -- 楼盘其他费用
    , PRMS_AL_PUT_IN_AMT -- 楼盘已投入金额
    , CAP_RATIO -- 资本金比例
    , BANK_FIN -- 银行融资
    , HOS_TYPE_CD -- 房屋类型代码
    , DRAW_PRESAL_CERT_CNT -- 领取预售证套数
    , DRAW_PRESAL_CERT_AREA -- 领取预售证面积
    , ANTCPTD_SALES_AVG_PRC -- 预计销售均价
    , PRMS_AL_SALES_CNT -- 楼盘已销售套数
    , PRMS_AL_SALES_AREA -- 楼盘已销售面积
    , CREATE_ORG_NO -- 创建机构编号
    , CREATE_TELR_NO -- 创建柜员编号
    , CREATE_TM -- 创建时间
    , MODIF_TM -- 修改时间
    , PT_DT -- 数据日期
)
select
      T1.CUST_ISN as CUST_IN_CD -- 客户内码
    , T1.PROJECT_TYPE as PROJ_TYPE -- 项目类别
    , T1.PROJECT_ID as PROJ_ID -- 项目序号
    , T1.CUST_NAM as CUST_NAME -- 客户名称
    , T1.BEL_ORG_NO as AFLT_ORG -- 所属机构
    , T1.PROJECT_LOAN_NO as PROJ_LOAN_NO -- 项目贷款编号
    , T1.PROJECT_NAME as PROJ_NAME -- 项目名称
    , T1.PROJECT_ADDR as PROJ_ADDR -- 项目地址
    , T2.AGGRE_INVEST as PROJ_TOTAL_INVEST -- 项目投资总额
    , T2.SHAREHOLD_DEV_QUALI as SHAREHD_DVLP_QLF -- 股东开发资质
    , T1.COVER_AREA as PROJ_OCPY_LAND_AREA -- 项目占地面积
    , T1.CURRENCY as PROJ_CURR -- 项目币种
    , T1.OVERALL_FLOORAGE as ESTATE_PROJ_TOTAL_BUILD_AREA -- 房地产项目总建筑面积
    , T1.OTHER_AREA as ESTATE_PROJ_OTHER_BUILD_AREA -- 房地产项目其他建筑面积
    , T1.PROJECT_PLOT_RATIO as ESTATE_PROJ_PLOT_RATIO -- 房地产项目容积率
    , T1.START_DATE as COMMEN_DT -- 开工日期
    , T1.PLAN_END_DATE as PLAN_CMPLT_DT -- 计划竣工日期
    , T1.CONSTR_LAND_USE_PERMIT as CNSTR_LAND_PLAN_LIC_DOC -- 建设用地规划许可证件
    , T1.LAND_USE_CERTIFICATE as LAND_USE_RIGHT_CERT_BOOK -- 土地使用权证书
    , T1.CONSTR_PLAN_PERMIT as BUILD_PLAN_LIC -- 建筑工程规划许可证
    , T1.CONSTR_BUILD_PLAN_PERMIT as BUILD_CNSTR_LIC -- 建筑工程施工许可证
    , T1.PRE_SALE_PERMIT as COMD_HOS_PRESELL_LIC -- 商品房预售许可证
    , T1.SUPERVISION_NAME_QUA_LEVEL as ESTATE_PROJ_SUPVSR_CORP_NAME_AND_QLF_LVL -- 房地产项目监理单位名称及资质等级
    , T2.CAPITAL_FUND as PRMS_CAP -- 楼盘资本金
    , T2.OTHER_FINANCING as PRMS_OTHER_FIN_AMT -- 楼盘其他融资金额
    , T1.STORE_AREA as MALL_AREA -- 商铺面积
    , T1.OFFICES_AREA as OFFICES_AREA -- 写字楼面积
    , T1.LOAN_INOUR_BANK as LAND_DVLP_PROJ_OBANK_LOAN -- 土地开发项目我行贷款
    , T1.PROJECT_PROGRESS as PROJ_CURR_SCHED -- 项目当前进度
    , T2.MORT_SITU_OF_OTH_BANK as PRMS_OTHER_BANK_MTG_SITU -- 楼盘他行按揭情况(按揭银行/额度)
    , T1.TOTAL_CONSTR_PERIOD as ESTATE_PROJ_TOTAL_CNSTR_TMS -- 房地产项目总建设期数
    , T1.GREEN_LOAN_SIGN as GREEN_LOAN_FLAG -- 绿色贷款标志
    , T1.PROJECT_SUB_TYPE as ESTATE_PROJ_SUB_TYPE_CD -- 房地产项目子类型代码
    , T1.PROJECT_NAME as CO_PROJ_NAME -- 合作项目名称
    , T1.PROJECT_LEVEL as PROJ_LVL -- 项目级别
    , T1.PROJECT_CONSTR_CONTENT as PROJ_CNSTR_CONTENT -- 项目建设内容
    , T1.TOTAL_PLAN_INVESTMENT as ESTATE_PROJ_PLAN_TOTAL_INVEST -- 房地产项目计划总投资
    , T1.CONTRACTOR_NAME_QUA as CNSTR_CORP_NAME_AND_QLF -- 承建单位名称及资质
    , T1.CAPITAL_SOURCE_NAME as CAP_SRC_ORG_NAME_FIN_BANK_NAME_INVESTOR_NAME -- 资金来源机构名称/融资银行名称/出资人名称
    , T1.INVESTMENT_AMOUNT as PROJ_INVEST_AMT_FIN -- 项目投资金额/融资金额
    , T1.AMOUNT_INVESTED as PROJ_CURR_AL_INVEST_AMT -- 项目当前已投资金额
    , T1.LAND_COST as PROJ_INVEST_CONST_MDL_LAND_FEE -- 项目投资构成中土地费用
    , T1.PROJECT_COST as PROJ_INVEST_CONST_MDL_PROJ_FEE_EARLY_DAYS_PROJ_FEE -- 项目投资构成中工程费用/前期工程费
    , T1.EQUIPMENT_COST as PROJ_INVEST_CONST_MDL_EQUIP_FEE -- 项目投资构成中设备费用
    , T1.CONT_INTEREST as PROJ_INVEST_CONST_MDL_BUILD_PERIOD_INT -- 项目投资构成中建设期利息
    , T1.WORKING_FUND as PROJ_INVEST_CONST_MDL_WORK_CAP -- 项目投资构成中流动资金
    , T1.OTHER_FEES as PROJ_INVEST_CONST_MDL_OTHER_FEE -- 项目投资构成中其他费用
    , T1.COMPENSATION_FEE as REMOVE_COMP -- 拆迁补偿费
    , T1.CRE_QUOTA as CUST_TM_POINT_CRDT_LINE -- 客户时点授信额度
    , T1.CREDIT_TOTAL as CUST_TM_POINT_SYN_CRDT_LINE -- 客户时点综合授信额度
    , T1.OPEN_TOTAL as CUST_TM_POINT_OPEN_CRDT_LINE -- 客户时点敞口授信额度
    , T1.LOAN_PURPOSE_CODE as INDUS_INVEST_CD -- 行业投向代码
    , T1.LOAN_PURPOSE_NAME as INDUS_INVEST_NAME -- 行业投向名称
    , T1.CUST_TYP as COOR_TYPE -- 合作方类型
    , T2.CUST_NAME as COOR_NAME -- 合作方名称
    , T2.LEGAL_PERSON as LEGAL_REP_NAME -- 法定代表人名称
    , T2.ACTUAL_CTRL_PERSON as ACTL_CTRLER_NAME -- 实际控制人名称
    , T2.REG_CAPITAL as REG_CAP_AMT -- 注册资本金额
    , T2.REG_ADDR as REG_ADDR -- 注册地
    , T2.DEV_QUALI as DVLP_QLF -- 开发资质
    , T2.PRE_SALE_FUND_BANK as PRESELL_CAP_SPEC_DPSIT_ACCT_OPBANK_NAME -- 预售资金专用存款账户开户行名称
    , T2.MORGAGE_BUILDING_NAME as MTG_PRMS_NAME -- 按揭楼盘名称
    , T2.MORGAGE_COOP_AREA as TH_TM_MTG_PRMS_CO_SCOPE -- 本次按揭楼盘合作范围
    , T2.ORIGIN_COOP_AMOUNT as ORIG_CO_LMT -- 原合作额度
    , T2.GUARANT_SITUATION as ORIG_COOR_PROJ_LMT_GUAR_SITU -- 原合作方项目额度担保情况
    , T2.ISSUED_GUARANT_SITUATION as NEW_COOR_PROJ_LMT_GUAR_SITU -- 新合作方项目额度担保情况
    , T2.ISSUED_AMOUNT as COOR_PROJ_ISSUED_LMT -- 合作方项目已发放额度
    , T2.FOUR_CERTS_AUDIT_FLAG as COOR_FRTH_PRV_APPRV_SITU -- 合作方四证审批情况
    , T2.BUILDING_NAME as PRMS_NAME -- 楼盘名称
    , T2.BUILDING_ADDR as PRMS_LOCAT_POSITION -- 楼盘坐落位置
    , T2.DEV_HOUSE_TYPE as CO_PRMS_DVLP_ESTATE_TYPE -- 合作楼盘开发房产类型
    , T2.OPEN_DATE as PRMS_OPEN_QUOT_DT -- 楼盘开盘日期
    , T2.LAND_TYPE as CO_PRMS_USAGE -- 合作楼盘用途
    , T2.LAND_EFFECTIVE_DATE as CO_PRMS_LAND_EFFECT_DT -- 合作楼盘土地生效日期
    , T2.BUY_PRICE as CO_PRMS_BUY_PRC -- 合作楼盘购入价
    , T2.CURR_ASSET as COOR_CURR_PERIOD_AST -- 合作方当期资产
    , T2.CURR_NET_ASSET as COOR_CURR_PERIOD_NET_AST -- 合作方当期净资产
    , T2.CURR_SALE as COOR_CURR_PERIOD_SALES -- 合作方当期销售
    , T2.LAST_YEAR_SALE as COOR_LAST_YEAR_SALES -- 合作方上年销售
    , T2.DEBT_RATE as COOR_LIAB_RATE -- 合作方负债率
    , T2.CURR_PROFIT as COOR_CURR_PERIOD_PRFT -- 合作方当期利润
    , T2.LAST_YEAR_PROFIT as COOR_LAST_YEAR_PRFT -- 合作方上年利润
    , T2.BANK_LOAN as COOR_BANK_LOAN -- 合作方银行贷款
    , T2.OUT_GUARANT as COOR_FORGN_GUAR -- 合作方对外担保
    , T2.REPORT_CODE as DUE_DIL_INVSTG_REPORT_NO -- 尽职调查报告编号
    , T2.OTHER_COST as PRMS_OTHER_FEE -- 楼盘其他费用
    , T1.AMOUNT_INVESTED as PRMS_AL_PUT_IN_AMT -- 楼盘已投入金额
    , T2.CAPITAL_RATIO as CAP_RATIO -- 资本金比例
    , T2.BANK_FINANCING as BANK_FIN -- 银行融资
    , T2.HOUSE_TYPE as HOS_TYPE_CD -- 房屋类型代码
    , T2.NUM_OF_PRESALE_CER as DRAW_PRESAL_CERT_CNT -- 领取预售证套数
    , T2.AREA_OF_PRESALE_CER as DRAW_PRESAL_CERT_AREA -- 领取预售证面积
    , T2.EST_AVER_SALES_PRICE as ANTCPTD_SALES_AVG_PRC -- 预计销售均价
    , T2.NUM_OF_UNITS_SOLD as PRMS_AL_SALES_CNT -- 楼盘已销售套数
    , T2.AREA_SOLD as PRMS_AL_SALES_AREA -- 楼盘已销售面积
    , T1.CRT_ORGAN as CREATE_ORG_NO -- 创建机构编号
    , T1.CRT_LOAN_OFF as CREATE_TELR_NO -- 创建柜员编号
    , T1.CRT_TM as CREATE_TM -- 创建时间
    , T1.LAST_TM as MODIF_TM -- 修改时间
    , ${PROCESS_DATE}' as PT_DT -- 数据日期
from
    TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_02 as T1 -- 合作方信息
    LEFT JOIN TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_01 as T2 -- 基础信息
    on T1.CUST_ISN=T2.CUST_ISN 
;
-- ==============字段映射（第4组）==============
-- 法人贷款房地产项目聚合_合作方信息
drop table if exists ${session}.TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_03;

create table ${session}.TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_03 (
      CUST_ISN cust_isn -- 客户内码
    , PROJECT_TYPE project_type -- 项目类别
    , PROJECT_ID project_id -- 项目序号
    , CUST_NAM cust_nam -- 客户名称
    , BEL_ORG_NO bel_org_no -- 所属机构
    , PROJECT_LOAN_NO project_loan_no -- 项目贷款编号
    , PROJECT_NAME project_name -- 项目名称
    , PROJECT_ADDR project_addr -- 项目地址
    , COVER_AREA cover_area -- 项目占地面积
    , CURRENCY currency -- 项目币种
    , OVERALL_FLOORAGE overall_floorage -- 房地产项目总建筑面积
    , OTHER_AREA other_area -- 房地产项目其他建筑面积
    , PROJECT_PLOT_RATIO project_plot_ratio -- 房地产项目容积率
    , START_DATE start_date -- 开工日期
    , PLAN_END_DATE plan_end_date -- 计划竣工日期
    , CONSTR_LAND_USE_PERMIT constr_land_use_permit -- 建设用地规划许可证件
    , LAND_USE_CERTIFICATE land_use_certificate -- 土地使用权证书
    , CONSTR_PLAN_PERMIT constr_plan_permit -- 建筑工程规划许可证
    , CONSTR_BUILD_PLAN_PERMIT constr_build_plan_permit -- 建筑工程施工许可证
    , PRE_SALE_PERMIT pre_sale_permit -- 商品房预售许可证
    , SUPERVISION_NAME_QUA_LEVEL supervision_name_qua_level -- 房地产项目监理单位名称及资质等级
    , STORE_AREA store_area -- 商铺面积
    , OFFICES_AREA offices_area -- 写字楼面积
    , LOAN_INOUR_BANK loan_inour_bank -- 土地开发项目我行贷款
    , PROJECT_PROGRESS project_progress -- 项目当前进度
    , TOTAL_CONSTR_PERIOD total_constr_period -- 房地产项目总建设期数
    , GREEN_LOAN_SIGN green_loan_sign -- 绿色贷款标志
    , PROJECT_SUB_TYPE project_sub_type -- 房地产项目子类型代码
    , PROJECT_LEVEL project_level -- 项目级别
    , PROJECT_CONSTR_CONTENT project_constr_content -- 项目建设内容
    , TOTAL_PLAN_INVESTMENT total_plan_investment -- 房地产项目计划总投资
    , CONTRACTOR_NAME_QUA contractor_name_qua -- 承建单位名称及资质
    , CAPITAL_SOURCE_NAME capital_source_name -- 资金来源机构名称/融资银行名称/出资人名称
    , INVESTMENT_AMOUNT investment_amount -- 项目投资金额/融资金额
    , AMOUNT_INVESTED amount_invested -- 项目当前已投资金额
    , LAND_COST land_cost -- 项目投资构成中土地费用
    , PROJECT_COST project_cost -- 项目投资构成中工程费用/前期工程费
    , EQUIPMENT_COST equipment_cost -- 项目投资构成中设备费用
    , CONT_INTEREST cont_interest -- 项目投资构成中建设期利息
    , WORKING_FUND working_fund -- 项目投资构成中流动资金
    , OTHER_FEES other_fees -- 项目投资构成中其他费用
    , COMPENSATION_FEE compensation_fee -- 拆迁补偿费
    , CRE_QUOTA cre_quota -- 客户时点授信额度
    , CREDIT_TOTAL credit_total -- 客户时点综合授信额度
    , OPEN_TOTAL open_total -- 客户时点敞口授信额度
    , LOAN_PURPOSE_CODE loan_purpose_code -- 行业投向代码
    , LOAN_PURPOSE_NAME loan_purpose_name -- 行业投向名称
    , CUST_TYP cust_typ -- 合作方类型
    , CRT_ORGAN crt_organ -- 创建机构编号
    , CRT_LOAN_OFF crt_loan_off -- 创建柜员编号
    , CRT_TM crt_tm -- 创建时间
    , LAST_TM last_tm -- 修改时间
)
comment '法人贷款房地产项目聚合_合作方信息'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;

insert into table ${session}.TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_03(
      CUST_ISN -- 客户内码
    , PROJECT_TYPE -- 项目类别
    , PROJECT_ID -- 项目序号
    , CUST_NAM -- 客户名称
    , BEL_ORG_NO -- 所属机构
    , PROJECT_LOAN_NO -- 项目贷款编号
    , PROJECT_NAME -- 项目名称
    , PROJECT_ADDR -- 项目地址
    , COVER_AREA -- 项目占地面积
    , CURRENCY -- 项目币种
    , OVERALL_FLOORAGE -- 房地产项目总建筑面积
    , OTHER_AREA -- 房地产项目其他建筑面积
    , PROJECT_PLOT_RATIO -- 房地产项目容积率
    , START_DATE -- 开工日期
    , PLAN_END_DATE -- 计划竣工日期
    , CONSTR_LAND_USE_PERMIT -- 建设用地规划许可证件
    , LAND_USE_CERTIFICATE -- 土地使用权证书
    , CONSTR_PLAN_PERMIT -- 建筑工程规划许可证
    , CONSTR_BUILD_PLAN_PERMIT -- 建筑工程施工许可证
    , PRE_SALE_PERMIT -- 商品房预售许可证
    , SUPERVISION_NAME_QUA_LEVEL -- 房地产项目监理单位名称及资质等级
    , STORE_AREA -- 商铺面积
    , OFFICES_AREA -- 写字楼面积
    , LOAN_INOUR_BANK -- 土地开发项目我行贷款
    , PROJECT_PROGRESS -- 项目当前进度
    , TOTAL_CONSTR_PERIOD -- 房地产项目总建设期数
    , GREEN_LOAN_SIGN -- 绿色贷款标志
    , PROJECT_SUB_TYPE -- 房地产项目子类型代码
    , PROJECT_LEVEL -- 项目级别
    , PROJECT_CONSTR_CONTENT -- 项目建设内容
    , TOTAL_PLAN_INVESTMENT -- 房地产项目计划总投资
    , CONTRACTOR_NAME_QUA -- 承建单位名称及资质
    , CAPITAL_SOURCE_NAME -- 资金来源机构名称/融资银行名称/出资人名称
    , INVESTMENT_AMOUNT -- 项目投资金额/融资金额
    , AMOUNT_INVESTED -- 项目当前已投资金额
    , LAND_COST -- 项目投资构成中土地费用
    , PROJECT_COST -- 项目投资构成中工程费用/前期工程费
    , EQUIPMENT_COST -- 项目投资构成中设备费用
    , CONT_INTEREST -- 项目投资构成中建设期利息
    , WORKING_FUND -- 项目投资构成中流动资金
    , OTHER_FEES -- 项目投资构成中其他费用
    , COMPENSATION_FEE -- 拆迁补偿费
    , CRE_QUOTA -- 客户时点授信额度
    , CREDIT_TOTAL -- 客户时点综合授信额度
    , OPEN_TOTAL -- 客户时点敞口授信额度
    , LOAN_PURPOSE_CODE -- 行业投向代码
    , LOAN_PURPOSE_NAME -- 行业投向名称
    , CUST_TYP -- 合作方类型
    , CRT_ORGAN -- 创建机构编号
    , CRT_LOAN_OFF -- 创建柜员编号
    , CRT_TM -- 创建时间
    , LAST_TM -- 修改时间
)
select
      T1.CUST_ISN as CUST_ISN -- 客户内码
    , T1.PROJECT_TYPE as PROJECT_TYPE -- 项目类别
    , T1.PROJECT_ID as PROJECT_ID -- 项目ID
    , T2.CUST_NAM as CUST_NAM -- 客户名称
    , T4.BEL_ORG_NO as BEL_ORG_NO -- 所属机构
    , T4.PROJECT_LOAN_NO as PROJECT_LOAN_NO -- 项目贷款编号
    , T6.PROJECT_NAME as PROJECT_NAME -- 项目名字
    , T6.PROJECT_ADDR as PROJECT_ADDR -- 项目地址
    , T6.COVER_AREA as COVER_AREA -- 占地面积
    , T1.CURRENCY as CURRENCY -- 币种
    , T5.OVERALL_FLOORAGE as OVERALL_FLOORAGE -- 总建筑面积
    , T5.OTHER_AREA as OTHER_AREA -- 其他面积
    , T5.PROJECT_PLOT_RATIO as PROJECT_PLOT_RATIO -- 项目容积率
    , T5.START_DATE as START_DATE -- 开工日期
    , T5.PLAN_END_DATE as PLAN_END_DATE -- 计划竣工日期
    , T5.CONSTR_LAND_USE_PERMIT as CONSTR_LAND_USE_PERMIT -- 建设用地规划许可证件
    , T5.LAND_USE_CERTIFICATE as LAND_USE_CERTIFICATE -- 土地使用权证书
    , T5.CONSTR_PLAN_PERMIT as CONSTR_PLAN_PERMIT -- 建筑工程规划许可证
    , T5.CONSTR_BUILD_PLAN_PERMIT as CONSTR_BUILD_PLAN_PERMIT -- 建筑工程施工许可证
    , T5.PRE_SALE_PERMIT as PRE_SALE_PERMIT -- 商品房预售许可证
    , T5.SUPERVISION_NAME_QUA_LEVEL as SUPERVISION_NAME_QUA_LEVEL -- 监理单位名称及资质等级
    , T5.STORE_AREA as STORE_AREA -- 商铺面积
    , T5.OFFICES_AREA as OFFICES_AREA -- 写字楼面积
    , T6.LOAN_INOUR_BANK as LOAN_INOUR_BANK -- 土地开发项目我行贷款
    , T7.PROJECT_PROGRESS as PROJECT_PROGRESS -- 项目当前进度
    , T5.TOTAL_CONSTR_PERIOD as TOTAL_CONSTR_PERIOD -- 总建设期数
    , T3.GREEN_LOAN_SIGN as GREEN_LOAN_SIGN -- 绿色贷款标志
    , T5.PROJECT_SUB_TYPE as PROJECT_SUB_TYPE -- 项目子类型
    , T6.PROJECT_LEVEL as PROJECT_LEVEL -- 项目级别
    , T6.PROJECT_CONSTR_CONTENT as PROJECT_CONSTR_CONTENT -- 项目建设内容
    , T5.TOTAL_PLAN_INVESTMENT as TOTAL_PLAN_INVESTMENT -- 计划总投资
    , T5.CONTRACTOR_NAME_QUA as CONTRACTOR_NAME_QUA -- 承建单位名称及资质
    , T1.CAPITAL_SOURCE_NAME as CAPITAL_SOURCE_NAME -- 资金来源机构名称/融资银行名称/出资人名称
    , T1.INVESTMENT_AMOUNT as INVESTMENT_AMOUNT -- 投资金额/融资金额
    , T7.AMOUNT_INVESTED as AMOUNT_INVESTED -- 已投资金额
    , T8.LAND_COST as LAND_COST -- 土地费用
    , T8.PROJECT_COST as PROJECT_COST -- 工程费用/前期工程费
    , T8.EQUIPMENT_COST as EQUIPMENT_COST -- 设备费用
    , T8.CONT_INTEREST as CONT_INTEREST -- 建设期利息
    , T8.WORKING_FUND as WORKING_FUND -- 流动资金
    , T8.OTHER_FEES as OTHER_FEES -- 其他费用
    , T6.COMPENSATION_FEE as COMPENSATION_FEE -- 拆迁、补偿费
    , T3.CRE_QUOTA as CRE_QUOTA -- 授信额度
    , T4.CREDIT_TOTAL as CREDIT_TOTAL -- 综合授信额度
    , T4.OPEN_TOTAL as OPEN_TOTAL -- 敞口授信额度
    , T4.LOAN_PURPOSE_CODE as LOAN_PURPOSE_CODE -- 行业投向代码
    , T4.LOAN_PURPOSE_NAME as LOAN_PURPOSE_NAME -- 行业投向名称
    , T9.CUST_TYP as CUST_TYP -- 合作方类型
    , T1.CRT_ORGAN as CRT_ORGAN -- 登记人
    , T1.CRT_LOAN_OFF as CRT_LOAN_OFF -- 创建柜员
    , T1.CRT_TM as CRT_TM -- 登记时间
    , T1.LAST_TM as LAST_TM -- 更新时间
from
    ${ODS_XDZX_SCHEMA}.RF_PROJECT_CAPITAL_SOURCE as T1 -- 项目资金来源表
    ${ODS_XDZX_SCHEMA}.RF_QUA_AND_CERT_HOUSE as T2 -- 瑞丰-客户资质与认证-房地产资质
    on T1.CUST_ISN = T2.CUST_ISN
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    (
SELECT 
	P1.CUST_ISN
	,P1.GREEN_LOAN_SIGN
	,P1.CRE_QUOTA
	,P1.AGATE_CODE

	,ROW_NUMBER()OVER(PARTITION BY P1.CUST_ISN 
			ORDER BY P1.UPDATE_TIME DESC )RN 
FROM ${ODS_XDZX_SCHEMA}.RF_ORG_CREDIT_BEFORE_APPROVAL_MODIFY P1
WHERE 1=1 
	AND P1.PT_DT='${process_date}' 
	AND P1.DELETED='0'
) as T3 -- 对公客户授信前置信息审批调整表
    on T1.CUST_ISN = T3.CUST_ISN
AND T3.PT_DT='${process_date}' 
AND T3.DELETED='0'
AND T3.RN = 1 
    ${ODS_XDZX_SCHEMA}.RF_ORG_CREDIT_BEFORE_DETAILS as T4 -- 对公客户授信前置细项表
    on T4.CUST_ISN = T3.CUST_ISN 
ANDD  T3.AGATE_CODE =T4.AGATE_CODE
AND T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    ${ODS_XDZX_SCHEMA}.RF_PROJECT_REALTY as T5 -- 房地产项目信息表
    on T1.PROJECT_ID= T5.ID
AND  T1.CUST_ISN = T5.CUST_ISN 
AND T1.PROJECT_TYPE = T5.PROJECT_TYPE
AND T5.PT_DT='${process_date}' 
AND T5.DELETED='0' 
    ${ODS_XDZX_SCHEMA}.RF_PROJECT_LAND_DEVELOP_BANK as T6 -- 土地储备/开发区项目信息表
    on T1.PROJECT_ID= T6.PROJECT_ID 
AND T1.CUST_ISN = T6.CUST_ISN
AND  T1.PROJECT_TYPE = T6.PROJECT_TYPE
AND T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
    ${ODS_XDZX_SCHEMA}.RF_PROJECT_PROGRESS_INFO as T7 -- 项目进展表
    on T1.PROJECT_ID=T7.PROJECT_ID 
AND   T1.CUST_ISN = T7.CUST_ISN 
AND   T1.PROJECT_TYPE = T7.PROJECT_TYPE
AND T7.PT_DT='${process_date}' 
AND T7.DELETED='0' 
    ${ODS_XDZX_SCHEMA}.RF_PROJECT_INPUT_OUTPUT_MEASURE as T8 -- 项目投入产出测算表
    on T1.PROJECT_ID= T8.PROJECT_ID 
AND  T1.CUST_ISN = T8.CUST_ISN 
AND T1.PROJECT_TYPE = T8.PROJECT_TYPE
AND T8.PT_DT='${process_date}' 
AND T8.DELETED='0' 
    ${ODS_XDZX_SCHEMA}.RF_PARTER as T9 -- 合作方信息表
    on T1.CUST_ISN =T9. CUST_ISN
AND T9.PT_DT='${process_date}' 
AND T9.DELETED='0' 
where 1=1 
AND T1.PROJECT_TYPE in ('3','4')
AND T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第5组）==============
-- 数据汇总插入

insert into table ${session}.S03_REAL_ESTATE_PROJ_LOAN_TAB(
      CUST_IN_CD -- 客户内码
    , PROJ_TYPE -- 项目类别
    , PROJ_ID -- 项目序号
    , CUST_NAME -- 客户名称
    , AFLT_ORG -- 所属机构
    , PROJ_LOAN_NO -- 项目贷款编号
    , PROJ_NAME -- 项目名称
    , PROJ_ADDR -- 项目地址
    , PROJ_TOTAL_INVEST -- 项目投资总额
    , SHAREHD_DVLP_QLF -- 股东开发资质
    , PROJ_OCPY_LAND_AREA -- 项目占地面积
    , PROJ_CURR -- 项目币种
    , ESTATE_PROJ_TOTAL_BUILD_AREA -- 房地产项目总建筑面积
    , ESTATE_PROJ_OTHER_BUILD_AREA -- 房地产项目其他建筑面积
    , ESTATE_PROJ_PLOT_RATIO -- 房地产项目容积率
    , COMMEN_DT -- 开工日期
    , PLAN_CMPLT_DT -- 计划竣工日期
    , CNSTR_LAND_PLAN_LIC_DOC -- 建设用地规划许可证件
    , LAND_USE_RIGHT_CERT_BOOK -- 土地使用权证书
    , BUILD_PLAN_LIC -- 建筑工程规划许可证
    , BUILD_CNSTR_LIC -- 建筑工程施工许可证
    , COMD_HOS_PRESELL_LIC -- 商品房预售许可证
    , ESTATE_PROJ_SUPVSR_CORP_NAME_AND_QLF_LVL -- 房地产项目监理单位名称及资质等级
    , PRMS_CAP -- 楼盘资本金
    , PRMS_OTHER_FIN_AMT -- 楼盘其他融资金额
    , MALL_AREA -- 商铺面积
    , OFFICES_AREA -- 写字楼面积
    , LAND_DVLP_PROJ_OBANK_LOAN -- 土地开发项目我行贷款
    , PROJ_CURR_SCHED -- 项目当前进度
    , PRMS_OTHER_BANK_MTG_SITU -- 楼盘他行按揭情况(按揭银行/额度)
    , ESTATE_PROJ_TOTAL_CNSTR_TMS -- 房地产项目总建设期数
    , GREEN_LOAN_FLAG -- 绿色贷款标志
    , ESTATE_PROJ_SUB_TYPE_CD -- 房地产项目子类型代码
    , CO_PROJ_NAME -- 合作项目名称
    , PROJ_LVL -- 项目级别
    , PROJ_CNSTR_CONTENT -- 项目建设内容
    , ESTATE_PROJ_PLAN_TOTAL_INVEST -- 房地产项目计划总投资
    , CNSTR_CORP_NAME_AND_QLF -- 承建单位名称及资质
    , CAP_SRC_ORG_NAME_FIN_BANK_NAME_INVESTOR_NAME -- 资金来源机构名称/融资银行名称/出资人名称
    , PROJ_INVEST_AMT_FIN -- 项目投资金额/融资金额
    , PROJ_CURR_AL_INVEST_AMT -- 项目当前已投资金额
    , PROJ_INVEST_CONST_MDL_LAND_FEE -- 项目投资构成中土地费用
    , PROJ_INVEST_CONST_MDL_PROJ_FEE_EARLY_DAYS_PROJ_FEE -- 项目投资构成中工程费用/前期工程费
    , PROJ_INVEST_CONST_MDL_EQUIP_FEE -- 项目投资构成中设备费用
    , PROJ_INVEST_CONST_MDL_BUILD_PERIOD_INT -- 项目投资构成中建设期利息
    , PROJ_INVEST_CONST_MDL_WORK_CAP -- 项目投资构成中流动资金
    , PROJ_INVEST_CONST_MDL_OTHER_FEE -- 项目投资构成中其他费用
    , REMOVE_COMP -- 拆迁补偿费
    , CUST_TM_POINT_CRDT_LINE -- 客户时点授信额度
    , CUST_TM_POINT_SYN_CRDT_LINE -- 客户时点综合授信额度
    , CUST_TM_POINT_OPEN_CRDT_LINE -- 客户时点敞口授信额度
    , INDUS_INVEST_CD -- 行业投向代码
    , INDUS_INVEST_NAME -- 行业投向名称
    , COOR_TYPE -- 合作方类型
    , COOR_NAME -- 合作方名称
    , LEGAL_REP_NAME -- 法定代表人名称
    , ACTL_CTRLER_NAME -- 实际控制人名称
    , REG_CAP_AMT -- 注册资本金额
    , REG_ADDR -- 注册地
    , DVLP_QLF -- 开发资质
    , PRESELL_CAP_SPEC_DPSIT_ACCT_OPBANK_NAME -- 预售资金专用存款账户开户行名称
    , MTG_PRMS_NAME -- 按揭楼盘名称
    , TH_TM_MTG_PRMS_CO_SCOPE -- 本次按揭楼盘合作范围
    , ORIG_CO_LMT -- 原合作额度
    , ORIG_COOR_PROJ_LMT_GUAR_SITU -- 原合作方项目额度担保情况
    , NEW_COOR_PROJ_LMT_GUAR_SITU -- 新合作方项目额度担保情况
    , COOR_PROJ_ISSUED_LMT -- 合作方项目已发放额度
    , COOR_FRTH_PRV_APPRV_SITU -- 合作方四证审批情况
    , PRMS_NAME -- 楼盘名称
    , PRMS_LOCAT_POSITION -- 楼盘坐落位置
    , CO_PRMS_DVLP_ESTATE_TYPE -- 合作楼盘开发房产类型
    , PRMS_OPEN_QUOT_DT -- 楼盘开盘日期
    , CO_PRMS_USAGE -- 合作楼盘用途
    , CO_PRMS_LAND_EFFECT_DT -- 合作楼盘土地生效日期
    , CO_PRMS_BUY_PRC -- 合作楼盘购入价
    , COOR_CURR_PERIOD_AST -- 合作方当期资产
    , COOR_CURR_PERIOD_NET_AST -- 合作方当期净资产
    , COOR_CURR_PERIOD_SALES -- 合作方当期销售
    , COOR_LAST_YEAR_SALES -- 合作方上年销售
    , COOR_LIAB_RATE -- 合作方负债率
    , COOR_CURR_PERIOD_PRFT -- 合作方当期利润
    , COOR_LAST_YEAR_PRFT -- 合作方上年利润
    , COOR_BANK_LOAN -- 合作方银行贷款
    , COOR_FORGN_GUAR -- 合作方对外担保
    , DUE_DIL_INVSTG_REPORT_NO -- 尽职调查报告编号
    , PRMS_OTHER_FEE -- 楼盘其他费用
    , PRMS_AL_PUT_IN_AMT -- 楼盘已投入金额
    , CAP_RATIO -- 资本金比例
    , BANK_FIN -- 银行融资
    , HOS_TYPE_CD -- 房屋类型代码
    , DRAW_PRESAL_CERT_CNT -- 领取预售证套数
    , DRAW_PRESAL_CERT_AREA -- 领取预售证面积
    , ANTCPTD_SALES_AVG_PRC -- 预计销售均价
    , PRMS_AL_SALES_CNT -- 楼盘已销售套数
    , PRMS_AL_SALES_AREA -- 楼盘已销售面积
    , CREATE_ORG_NO -- 创建机构编号
    , CREATE_TELR_NO -- 创建柜员编号
    , CREATE_TM -- 创建时间
    , MODIF_TM -- 修改时间
    , PT_DT -- 数据日期
)
select
      T1.CUST_ISN as CUST_IN_CD -- 客户内码
    , T1.PROJECT_TYPE as PROJ_TYPE -- 项目类别
    , T1.PROJECT_ID as PROJ_ID -- 项目序号
    , T1.CUST_NAM as CUST_NAME -- 客户名称
    , T1.BEL_ORG_NO as AFLT_ORG -- 所属机构
    , T1.PROJECT_LOAN_NO as PROJ_LOAN_NO -- 项目贷款编号
    , T1.PROJECT_NAME as PROJ_NAME -- 项目名称
    , T1.PROJECT_ADDR as PROJ_ADDR -- 项目地址
    , T2.AGGRE_INVEST as PROJ_TOTAL_INVEST -- 项目投资总额
    , T2.SHAREHOLD_DEV_QUALI as SHAREHD_DVLP_QLF -- 股东开发资质
    , T1.COVER_AREA as PROJ_OCPY_LAND_AREA -- 项目占地面积
    , T1.CURRENCY as PROJ_CURR -- 项目币种
    , T1.OVERALL_FLOORAGE as ESTATE_PROJ_TOTAL_BUILD_AREA -- 房地产项目总建筑面积
    , T1.OTHER_AREA as ESTATE_PROJ_OTHER_BUILD_AREA -- 房地产项目其他建筑面积
    , T1.PROJECT_PLOT_RATIO as ESTATE_PROJ_PLOT_RATIO -- 房地产项目容积率
    , T1.START_DATE as COMMEN_DT -- 开工日期
    , T1.PLAN_END_DATE as PLAN_CMPLT_DT -- 计划竣工日期
    , T1.CONSTR_LAND_USE_PERMIT as CNSTR_LAND_PLAN_LIC_DOC -- 建设用地规划许可证件
    , T1.LAND_USE_CERTIFICATE as LAND_USE_RIGHT_CERT_BOOK -- 土地使用权证书
    , T1.CONSTR_PLAN_PERMIT as BUILD_PLAN_LIC -- 建筑工程规划许可证
    , T1.CONSTR_BUILD_PLAN_PERMIT as BUILD_CNSTR_LIC -- 建筑工程施工许可证
    , T1.PRE_SALE_PERMIT as COMD_HOS_PRESELL_LIC -- 商品房预售许可证
    , T1.SUPERVISION_NAME_QUA_LEVEL as ESTATE_PROJ_SUPVSR_CORP_NAME_AND_QLF_LVL -- 房地产项目监理单位名称及资质等级
    , T2.CAPITAL_FUND as PRMS_CAP -- 楼盘资本金
    , T2.OTHER_FINANCING as PRMS_OTHER_FIN_AMT -- 楼盘其他融资金额
    , T1.STORE_AREA as MALL_AREA -- 商铺面积
    , T1.OFFICES_AREA as OFFICES_AREA -- 写字楼面积
    , T1.LOAN_INOUR_BANK as LAND_DVLP_PROJ_OBANK_LOAN -- 土地开发项目我行贷款
    , T1.PROJECT_PROGRESS as PROJ_CURR_SCHED -- 项目当前进度
    , T2.MORT_SITU_OF_OTH_BANK as PRMS_OTHER_BANK_MTG_SITU -- 楼盘他行按揭情况(按揭银行/额度)
    , T1.TOTAL_CONSTR_PERIOD as ESTATE_PROJ_TOTAL_CNSTR_TMS -- 房地产项目总建设期数
    , T1.GREEN_LOAN_SIGN as GREEN_LOAN_FLAG -- 绿色贷款标志
    , T1.PROJECT_SUB_TYPE as ESTATE_PROJ_SUB_TYPE_CD -- 房地产项目子类型代码
    , T1.PROJECT_NAME as CO_PROJ_NAME -- 合作项目名称
    , T1.PROJECT_LEVEL as PROJ_LVL -- 项目级别
    , T1.PROJECT_CONSTR_CONTENT as PROJ_CNSTR_CONTENT -- 项目建设内容
    , T1.TOTAL_PLAN_INVESTMENT as ESTATE_PROJ_PLAN_TOTAL_INVEST -- 房地产项目计划总投资
    , T1.CONTRACTOR_NAME_QUA as CNSTR_CORP_NAME_AND_QLF -- 承建单位名称及资质
    , T1.CAPITAL_SOURCE_NAME as CAP_SRC_ORG_NAME_FIN_BANK_NAME_INVESTOR_NAME -- 资金来源机构名称/融资银行名称/出资人名称
    , T1.INVESTMENT_AMOUNT as PROJ_INVEST_AMT_FIN -- 项目投资金额/融资金额
    , T1.AMOUNT_INVESTED as PROJ_CURR_AL_INVEST_AMT -- 项目当前已投资金额
    , T1.LAND_COST as PROJ_INVEST_CONST_MDL_LAND_FEE -- 项目投资构成中土地费用
    , T1.PROJECT_COST as PROJ_INVEST_CONST_MDL_PROJ_FEE_EARLY_DAYS_PROJ_FEE -- 项目投资构成中工程费用/前期工程费
    , T1.EQUIPMENT_COST as PROJ_INVEST_CONST_MDL_EQUIP_FEE -- 项目投资构成中设备费用
    , T1.CONT_INTEREST as PROJ_INVEST_CONST_MDL_BUILD_PERIOD_INT -- 项目投资构成中建设期利息
    , T1.WORKING_FUND as PROJ_INVEST_CONST_MDL_WORK_CAP -- 项目投资构成中流动资金
    , T1.OTHER_FEES as PROJ_INVEST_CONST_MDL_OTHER_FEE -- 项目投资构成中其他费用
    , T1.COMPENSATION_FEE as REMOVE_COMP -- 拆迁补偿费
    , T1.CRE_QUOTA as CUST_TM_POINT_CRDT_LINE -- 客户时点授信额度
    , T1.CREDIT_TOTAL as CUST_TM_POINT_SYN_CRDT_LINE -- 客户时点综合授信额度
    , T1.OPEN_TOTAL as CUST_TM_POINT_OPEN_CRDT_LINE -- 客户时点敞口授信额度
    , T1.LOAN_PURPOSE_CODE as INDUS_INVEST_CD -- 行业投向代码
    , T1.LOAN_PURPOSE_NAME as INDUS_INVEST_NAME -- 行业投向名称
    , T1.CUST_TYP as COOR_TYPE -- 合作方类型
    , T2.CUST_NAME as COOR_NAME -- 合作方名称
    , T2.LEGAL_PERSON as LEGAL_REP_NAME -- 法定代表人名称
    , T2.ACTUAL_CTRL_PERSON as ACTL_CTRLER_NAME -- 实际控制人名称
    , T2.REG_CAPITAL as REG_CAP_AMT -- 注册资本金额
    , T2.REG_ADDR as REG_ADDR -- 注册地
    , T2.DEV_QUALI as DVLP_QLF -- 开发资质
    , T2.PRE_SALE_FUND_BANK as PRESELL_CAP_SPEC_DPSIT_ACCT_OPBANK_NAME -- 预售资金专用存款账户开户行名称
    , T2.MORGAGE_BUILDING_NAME as MTG_PRMS_NAME -- 按揭楼盘名称
    , T2.MORGAGE_COOP_AREA as TH_TM_MTG_PRMS_CO_SCOPE -- 本次按揭楼盘合作范围
    , T2.ORIGIN_COOP_AMOUNT as ORIG_CO_LMT -- 原合作额度
    , T2.GUARANT_SITUATION as ORIG_COOR_PROJ_LMT_GUAR_SITU -- 原合作方项目额度担保情况
    , T2.ISSUED_GUARANT_SITUATION as NEW_COOR_PROJ_LMT_GUAR_SITU -- 新合作方项目额度担保情况
    , T2.ISSUED_AMOUNT as COOR_PROJ_ISSUED_LMT -- 合作方项目已发放额度
    , T2.FOUR_CERTS_AUDIT_FLAG as COOR_FRTH_PRV_APPRV_SITU -- 合作方四证审批情况
    , T2.BUILDING_NAME as PRMS_NAME -- 楼盘名称
    , T2.BUILDING_ADDR as PRMS_LOCAT_POSITION -- 楼盘坐落位置
    , T2.DEV_HOUSE_TYPE as CO_PRMS_DVLP_ESTATE_TYPE -- 合作楼盘开发房产类型
    , T2.OPEN_DATE as PRMS_OPEN_QUOT_DT -- 楼盘开盘日期
    , T2.LAND_TYPE as CO_PRMS_USAGE -- 合作楼盘用途
    , T2.LAND_EFFECTIVE_DATE as CO_PRMS_LAND_EFFECT_DT -- 合作楼盘土地生效日期
    , T2.BUY_PRICE as CO_PRMS_BUY_PRC -- 合作楼盘购入价
    , T2.CURR_ASSET as COOR_CURR_PERIOD_AST -- 合作方当期资产
    , T2.CURR_NET_ASSET as COOR_CURR_PERIOD_NET_AST -- 合作方当期净资产
    , T2.CURR_SALE as COOR_CURR_PERIOD_SALES -- 合作方当期销售
    , T2.LAST_YEAR_SALE as COOR_LAST_YEAR_SALES -- 合作方上年销售
    , T2.DEBT_RATE as COOR_LIAB_RATE -- 合作方负债率
    , T2.CURR_PROFIT as COOR_CURR_PERIOD_PRFT -- 合作方当期利润
    , T2.LAST_YEAR_PROFIT as COOR_LAST_YEAR_PRFT -- 合作方上年利润
    , T2.BANK_LOAN as COOR_BANK_LOAN -- 合作方银行贷款
    , T2.OUT_GUARANT as COOR_FORGN_GUAR -- 合作方对外担保
    , T2.REPORT_CODE as DUE_DIL_INVSTG_REPORT_NO -- 尽职调查报告编号
    , T2.OTHER_COST as PRMS_OTHER_FEE -- 楼盘其他费用
    , T1.AMOUNT_INVESTED as PRMS_AL_PUT_IN_AMT -- 楼盘已投入金额
    , T2.CAPITAL_RATIO as CAP_RATIO -- 资本金比例
    , T2.BANK_FINANCING as BANK_FIN -- 银行融资
    , T2.HOUSE_TYPE as HOS_TYPE_CD -- 房屋类型代码
    , T2.NUM_OF_PRESALE_CER as DRAW_PRESAL_CERT_CNT -- 领取预售证套数
    , T2.AREA_OF_PRESALE_CER as DRAW_PRESAL_CERT_AREA -- 领取预售证面积
    , T2.EST_AVER_SALES_PRICE as ANTCPTD_SALES_AVG_PRC -- 预计销售均价
    , T2.NUM_OF_UNITS_SOLD as PRMS_AL_SALES_CNT -- 楼盘已销售套数
    , T2.AREA_SOLD as PRMS_AL_SALES_AREA -- 楼盘已销售面积
    , T1.CRT_ORGAN as CREATE_ORG_NO -- 创建机构编号
    , T1.CRT_LOAN_OFF as CREATE_TELR_NO -- 创建柜员编号
    , T1.CRT_TM as CREATE_TM -- 创建时间
    , T1.LAST_TM as MODIF_TM -- 修改时间
    , '${PROCESS_DATE}' as PT_DT -- 数据日期
from
    TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_03 as T1 -- 合作方信息
    LEFT JOIN TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_01 as T2 -- 基础信息
    on T1.CUST_ISN=T2.CUST_ISN 
;

-- 删除所有临时表
drop table ${session}.TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_01;
drop table ${session}.TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_02;
drop table ${session}.TMP_S03_REAL_ESTATE_PROJ_LOAN_TAB_03;