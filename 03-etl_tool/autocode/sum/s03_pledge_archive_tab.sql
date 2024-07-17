-- 层次表名: 聚合层-押品档案聚合表
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd ${session}_s03_pledge_archive_tab
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：S03_PLEDGE_ARCHIVE_TAB
--     表中文名：押品档案聚合表
--     创建日期：2023-01-03 00:00:00
--     主键字段：PLEDGE_NO,MTG_SER_NO
--     归属层次：聚合层
--     归属主题：一级领域
--     库/模式：${session}
--     分析人员：王穆军
--     时间粒度：日
--     保留周期：13M
--     描述信息：记录全部个人贷款，法人贷款中抵质押品档案的各分类的详细信息，如房产，车辆，土地，机器设备，船舶，票据，存单等抵质押物的详细信息取抵质押物功能层面应用记录的详细信息
--     更新记录：
--         2023-01-03 00:00:00 王穆军 new
--         None None None

-- 1.清除目标表当天分区数据
alter table ${session}.S03_PLEDGE_ARCHIVE_TAB drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
-- ==============字段映射（第1组）==============
-- 客户担保物品

insert into table ${session}.S03_PLEDGE_ARCHIVE_TAB(
      PLEDGE_NO -- 抵质押物编号
    , MTG_SER_NO -- 抵押物序号
    , BORR_CUST_NAME -- 借款人客户名称
    , BORR_CUST_NO -- 借款人客户号
    , LNACCT_NO -- 贷款账户编号
    , BORR_CUST_IN_CD -- 借款人客户内码
    , PLEDGE_COMN_PRPTR_NAME -- 抵质押物共有人名称
    , PLEDGE_EVERYONE_NAME -- 抵质押物所有人名称
    , PLEDGE_CSTD_NAME -- 抵质押物保管人名称
    , MTG_EVERYONE_CUST_IN_CD -- 抵押物所有人客户内码
    , PLEDGE_MCLS_CD -- 抵质押物大类代码
    , GUAR_ARTI_CATE_CD -- 担保物品种类代码
    , PLEDGE_SINGLE_STATUS_CD -- 抵质押单状态代码
    , CRDT_PLEDGE_STATUS_CD -- 大信贷抵质押物状态代码
    , PLEDGE_STORE_TYPE_CD -- 抵质押物出入库类型代码
    , PLEDGE_PROC_STATUS_CD -- 抵质押物流程状态代码
    , CORE_PLEDGE_STATUS_CD -- 核心抵质押状态代码
    , WEB_CORE_PLEDGE_STATUS_CD -- 网络核心抵质押状态代码
    , PLEDGE_TYPE_CD -- 抵质押类型代码
    , PLEDGE_NAME -- 抵质押物名称
    , PLEDGE_REG_NO -- 抵质押物登记编号
    , PLEDGE_APPRS_COMPNY_NAME -- 抵质押物评估机构名称
    , PLEDGE_AMT -- 抵质押金额
    , ESTIM_VAL_TOTAL_AMT -- 评估价值总额
    , PLEDGE_MAX_GUAR_AMT -- 抵质押物最高担保金额
    , PLEDGE_AL_GUAR_AMT -- 抵质押物已担保金额
    , PLEDGE_GUAR_TMS -- 抵质押物担保次数
    , PLEDGE_CNT -- 抵质押物数量
    , PLEDGE_DEPRE_RATE -- 抵质押物折旧率
    , PLEDGE_WARRANT_NAME -- 抵质押物权证名称
    , MTG_LOC -- 抵押物所在地
    , MTG_LOC_DIST_CD -- 抵押物所在地行政区划编码
    , PLEDGE_REG_IDNT_CD -- 抵质押物登记标识代码
    , PLEDGE_SPEC -- 抵质押物规格
    , PLEDGE_BOOK_VAL_AMT -- 抵质押物账面价值金额
    , RELA_PLEDGE_DOC_NO -- 关联抵质押物资料编号
    , FIRST_BIT_FLAG -- 第一顺位标志
    , PLEDGE_REG_ORG_NAME -- 抵质押物登记机关名称
    , PLEDGE_INVOICE_NO -- 抵质押物发票编号
    , PLEDGE_HANDOV_MODE_CD -- 抵质押物移交方式代码
    , EXT_TYPE_CD -- 展期类型代码
    , STOP_PAY_NO -- 止付编号
    , CURR_CD -- 币种代码
    , AGAIN_MTG_ORIG_MTG_BIL_NO -- 再抵押原抵押单号
    , AGAIN_MTG_ORIG_MTG_SER_NO -- 再抵押原抵押序号
    , COLLATERAL_NO -- 质押物编号
    , COLLATERAL_ACCT_NO -- 质押物账户编号
    , OTHER_BANK_REG_FLAG -- 他行登记标志
    , OTHER_BANK_REG_AMT -- 他行登记金额
    , OTHER_BANK_REG_CERT_NO -- 他行登记证号码
    , PAYEE_OPEN_ACCT_BANK_NAME -- 收款人开户银行名称
    , APPRV_BOOK_NO -- 审批书编号
    , PICK_TO_READ_NOTICE_NO -- 调阅通知单编号
    , PICK_TO_READ_STATUS_CD -- 调阅状态代码
    , APPRV_PSN_NAME -- 审批人名称
    , PICK_TO_READ_PSN_NAME -- 调阅人名称
    , COGNZT_NAME -- 认定人名称
    , CHG_AGT_NO -- 变更协议编号
    , FORWD_ROOM_CURR_HOS_IDNT_CD -- 期房现房标识代码
    , PLEDGE_IN_WHS_ILUS -- 抵质押物入库说明
    , PLEDGE_EX_WHS_ILUS -- 抵质押物出库说明
    , PLEDGE_STORE_LOC -- 抵质押物存放地点
    , MAX_PLEDGE_RATE -- 最高抵质押率
    , PRIOR_COMP_WT_AMT -- 优先受偿权数额
    , OWNSHIP_SITU_ILUS -- 权属状况说明
    , STOP_PAY_CFM_FLAG -- 止付确认标志
    , AGAIN_MTG_FLAG -- 再抵押标志
    , ELEC_PLEDGE_FLAG -- 电子质押标志
    , COLLATERAL_BIL_ACCT_NO -- 质押票证账户编号
    , SHIP_CLS_REG_NO -- 船舶类登记号码
    , LARGE_AMT_DPSTRCP_ACCT_NO -- 大额存单账户编号
    , HOLD_BOD_AGREE_GUAR_OPIN_BOOK_FLAG -- 持有董事会同意担保意见书标志
    , HOLD_MTG_LSE_CONT_FLAG -- 持有抵押物租赁合同标志
    , DISPLC_OUT_GUAR_AMT -- 置换出担保金额
    , DISPLC_OUT_ESTATE_WARRANT_NO -- 置换出房产权证编号
    , LARGE_AMT_DPSTRCP_PROD_CD -- 大额存单产品编码
    , MRGACCT_NO -- 保证金账户编号
    , ROOM_PROP_CERT_NO -- 房产权证编号
    , BIL_NO -- 票据编号
    , VEHIC_LIC_PLATE_NO -- 车辆牌照号码
    , EQTY_CERT_NO -- 权益证书编号
    , PLEDGE_POLICY_NO -- 抵质押物保险单编号
    , HOLD_SHAREHD_AGREE_GUAR_OPIN_BOOK_FLAG -- 持有股东会同意担保意见书标志
    , HOLD_SUPER_DIROR_DEPT_AGREE_BOOK_FLAG -- 持有上级主管部门同意书标志
    , HOLD_HOS_RENT_SUPP_AGT_FLAG -- 持有房屋出租补充协议标志
    , LAND_CLS_LAND_USE_RIGHT_CERT_NO -- 土地类土地使用权证编号
    , CNSTRING_PROJ_CLS_LAND_USE_RIGHT_CERT_NO -- 在建工程类土地使用权证编号
    , MACH_EQUIP_CLS_REG_NO -- 机器设备类登记编号
    , MOV_PRO_CLS_REG_NO -- 动产类登记编号
    , PLEDGE_INFO_AST_CLS -- 质押信息资产分类
    , DISPLC_IN_ESTATE_WARRANT_NO -- 置换入不动产权证编号
    , DISPLC_IN_GUAR_NO -- 置换入担保编号
    , DISPLC_OUT_GUAR_NO -- 置换出担保编号
    , DISPLC_IN_GUAR_AMT -- 置换入担保金额
    , DISPLC_NO -- 置换编号
    , DISPLC_MODE_CD -- 置换方式代码
    , DISPLC_AMT -- 置换金额
    , PLEDGE_DISPLC_CD -- 抵质押物置换代码
    , NEW_GUAR_MODE_CD -- 新担保方式代码
    , NEW_GUAR_TYPE_CD -- 新担保类型代码
    , ORIG_SPNSR_CD -- 原担保方式代码
    , ORIG_GUAR_TYPE_CD -- 原担保类型代码
    , BE_GUAR_NO -- 被担保编号
    , ORIG_GUAR_RELA_TYPE_CD -- 原担保关系类型代码
    , NEW_GUAR_RELA_TYPE_CD -- 新担保关系类型代码
    , NEW_GUAR_START_TM -- 新担保起始时间
    , NEW_GUAR_END_TM -- 新担保结束时间
    , INSURE_CORP_NAME -- 保险公司名称
    , INSURE_FIRST_BNFC_NAME -- 保险第一受益人名称
    , INSURE_AMT -- 保险金额
    , POLICY_BEGIN_DT -- 保险单起始日期
    , POLICY_MATU_DT -- 保险单到期日期
    , MRGACCT_NAME -- 保证金账户名称
    , LARGE_AMT_DPSTRCP_SUBSCR_AMT -- 大额存单认购金额
    , LARGE_AMT_DPSTRCP_SUBSCR_CHNL_CD -- 大额存单认购渠道代码
    , LARGE_AMT_DPSTRCP_SUBSCR_DT -- 大额存单认购日期
    , LARGE_AMT_DPSTRCP_PAY_INT_MODE_CD -- 大额存单付息方式代码
    , LARGE_AMT_DPSTRCP_NEXT_PAY_INT_DT -- 大额存单下一付息日期
    , LARGE_AMT_DPSTRCP_PRE_PAY_INT_DT -- 大额存单上一付息日期
    , LARGE_AMT_DPSTRCP_ACRDG_PERIOD_PAY_INT_CUM_AMT -- 大额存单按期付息累计金额
    , LARGE_AMT_DPSTRCP_ACRDG_PERIOD_PAY_INT_TMS_CUM -- 大额存单按期付息期数累计
    , DPSTRCP_STATUS_CD -- 存单状态代码
    , LARGE_AMT_DPSTRCP_MATU_DT -- 大额存单到期日期
    , SHIP_NAME -- 船名
    , SHIP_REG_PORT -- 船籍港
    , SHIP_CALL_SIGN -- 船舶呼号
    , SHIP_IMO_NO -- 船舶IMO编号
    , SHIP_CATE -- 船舶种类
    , SHIP_MATRL -- 船体材料
    , SHIP_VAL -- 船舶价值
    , SHIP_SCALE -- 船舶尺度
    , SHIP_WIDTH -- 船舶型宽
    , SHIP_DEPTH -- 船舶型深
    , SHIP_TOTAL_TONNAGE -- 船舶总吨位
    , SHIP_NET_HEAVY_TONNAGE -- 船舶净重吨位
    , SHIP_EVERYONE_NAME -- 船舶所有人名称
    , SHIP_EVERYONE_ADDR -- 船舶所有人地址
    , SHIP_PROD_DT -- 船舶生产日期
    , MOV_PRO_CLS_SPEC -- 动产类规格
    , CSTD_CUST_NO -- 动产保管人客户编号
    , PROD_MANUFAC_NAME -- 生产厂家名称
    , ESTATE_LAND_BOOK_VAL -- 房产土地账面价值
    , STRG_PLEDGE_CSTD_AGT_NO -- 仓储物质押保管协议编号
    , MOV_PRO_PROD_DT -- 动产生产日期
    , MDL_ROOM_INQUIRY_RGN_NO -- 中房查询地区编号
    , BUY_HOS_CONT_NO -- 购房合同编号
    , INQUIRY_RGN -- 国土查询地区
    , MTG_PROC_SCHED_CD -- 抵押物办理进度代码
    , MTG_PROC_SCHED_DEAL_TM -- 抵押物办理进度处理时间
    , MTG_OBLG_USCD -- 抵押权利人统一社会信用代码
    , INQUIRY_MODE_CD -- 查询方式代码
    , MACH_EQUIP_PROD_DT -- 机器设备生产日期
    , MACH_EQUIP_BUY_PRC -- 机器设备购买价格
    , MACH_EQUIP_AFLT_CORP_NAME -- 机器设备所属公司名称
    , MACH_MODEL -- 机器型号
    , MACH_EQUIP_MFR_MANUFAC -- 机器设备制造厂家
    , MACH_EQUIP_CURR_BUY_PRC -- 机器设备现购买价格
    , MACH_EQUIP_GOODS_PAY_PAYOFF_RATIO -- 机器设备货款付清比例
    , MACH_EQUIP_QLTY_AND_SITU -- 机器设备质量及状况
    , BIL_TYPE_CD -- 票据类型代码
    , PLEDGOR_ACCT_NO -- 出质人账户编号
    , PLEDGOR_OPBANK_NO -- 出质人开户行行号
    , PLEDGOR_USCD -- 出质人统一社会信用代码
    , PLEDGEE_BANK_NO -- 质权人行号
    , ACCPTOR_OPBANK_NO -- 承兑人开户行行号
    , BIL_APP_REM -- 票据申请备注
    , SUB_START_RANGE -- 子票开始区间
    , SUB_END_RANGE -- 子票结束区间
    , BIL_SRC -- 票据来源
    , VEHIC_BRAND -- 车辆品牌
    , VEHIC_MODEL -- 车辆型号
    , ENGINE_NO -- 发动机编号
    , CAR_FRAME_NO -- 车架编号
    , VEHIC_LEAVE_FACTORY_YEAR -- 车辆出厂年份
    , VEHIC_SCRAP_YEAR -- 车辆报废年份
    , MOTOR_VEHIC_REG_CERT_NO -- 机动车登记证书编号
    , MOTOR_VEHIC_REG_DT -- 机动车登记日期
    , MOTOR_VEHIC_REG_NO -- 机动车登记编号
    , VEHIC_BELONGER_CD -- 车辆归属人代码
    , VEHIC_TYPE -- 车辆类型
    , VEHIC_OPER_FLAG -- 车辆营运标志
    , VEHIC_INVOICE_DT -- 车辆发票日期
    , VEHIC_TAX_SUM_AMT -- 车辆计税合计金额
    , VEHIC_NO_TAX_PRC -- 车辆不含税价
    , VEHIC_INVOICE_CD -- 车辆发票代码
    , ESTATE_TYPE_CD -- 房产类型代码
    , ESTATE_LOCAL_ADDR -- 房产所在地址
    , ESTATE_BUILD_AREA -- 房产建筑面积
    , ESTATE_STRU -- 房产结构
    , ESTATE_BLD_YEAR -- 房产建造年份
    , ESTATE_EVALU_TOTAL_AMT -- 房产评估价总额
    , ESTATE_BOOK_PRC_TOTAL_AMT -- 房产账面价总额
    , ESTATE_REG_PROOF_NO -- 房产登记证明编号
    , PROV_GOLD_ESTATE_DIST_NO -- 省金综房产行政区划编号
    , LAND_CLS_LAND_BOOK_PRC -- 土地类土地账面价格
    , ESTATE_LAND_EVALU -- 房产土地评估价
    , LAND_CLS_LAND_ESTIM_PRC -- 土地类土地评估价格
    , ESTATE_OCPY_LAND_AREA -- 房产占地面积
    , LAND_CLS_LAND_USE_RIGHT_AREA -- 土地类土地使用权面积
    , CNSTRING_PROJ_CLS_LAND_USE_RIGHT_AREA -- 在建工程类土地使用权面积
    , ESTATE_LAND_USE_RIGHT_TERMI_DT -- 房产土地使用权终止日期
    , LAND_CLS_LAND_USE_RIGHT_TERMI_DT -- 土地类土地使用权终止日期
    , CNSTRING_PROJ_CLS_LAND_USE_RIGHT_TERMI_DT -- 在建工程类土地使用权终止日期
    , ESTATE_LAND_LOCAL_ADDR -- 房产土地所在地址
    , ESTATE_MTG_OBLG_FULLNAME -- 房产抵押权利人全称
    , LAND_CLS_LAND_LOCAL_ADDR -- 土地类土地所在地址
    , CNSTRING_PROJ_LAND_LOC_ -- 在建工程土地所在地址
    , ESTATE_LAND_NO -- 房产土地地号
    , LAND_CLS_LAND_NO -- 土地类土地地号
    , CNSTRING_PROJ_LAND_NO -- 在建工程土地地号
    , ESTATE_LAND_USE_RIGHT_BEGIN_DT -- 房产土地使用权起始日期
    , LAND_CLS_LAND_USE_RIGHT_BEGIN_DT -- 土地类土地使用权起始日期
    , CNSTRING_PROJ_LAND_USE_RIGHT_BEGIN_DT -- 在建工程土地使用权起始日期
    , ESTATE_LAND_USAGE_CD -- 房产土地用途代码
    , LAND_CLS_LAND_USAGE_CD -- 土地类土地用途代码
    , CNSTRING_PROJ_LAND_USAGE_CD -- 在建工程土地用途代码
    , ESTATE_LAND_USE_RIGHT_CERT_NO -- 房产土地使用权证书编号
    , CNSTR_LAND_PLAN_LIC_NO -- 建设用地规划许可证编号
    , CNSTR_PROJ_PLAN_LIC_CERT_NO -- 建设工程规划许可证书编号
    , CNSTR_LIC_BOOK_NO -- 施工许可证书编号
    , PROJ_CNSTR_AL_PUT_IN_CAP_OCUP_TOTAL_CAST_CAP_PCT -- 工程建设已投入资金占总投资额百分比
    , ESTATE_LAND_USE_RIGHT_TYPE_CD -- 房产土地使用权类型代码
    , LAND_CLS_LAND_USE_RIGHT_TYPE_CD -- 土地类土地使用权类型代码
    , IMMATRL_AST_EQTY_CERT_ISSUE_CORP_NAME -- 无形资产权益证书签发单位名称
    , IMMATRL_AST_EQTY_VALID -- 无形资产权益有效期限
    , IMMATRL_AST_EQTY_STABLE_FLAG -- 无形资产权益稳定标志
    , IMMATRL_AST_MAJOR_ORG_ESTIM_CERT_NO -- 无形资产专业机构评估证书编号
    , IMMATRL_AST_NEED_PAY_FEE_FLAG -- 无形资产需缴纳费用标志
    , IMMATRL_AST_FEE_PAY_VOCH_NO -- 无形资产费用缴纳凭证号码
    , IMMATRL_AST_PLEDGE_HANDOV_DT -- 无形资产质押物移交日期
    , COLLATERAL_CURR_CD -- 质押物币种代码
    , COLLATERAL_MATU_DT -- 质押物到期日期
    , COLLATERAL_ISSUE_CORP_NAME -- 质押物签发单位名称
    , COLLATERAL_ISSUE_CORP_ORG_NO -- 质押物签发单位机构编号
    , COLLATERAL_BEGIN_DT -- 质押物起始日期
    , COLLATERAL_DNMNT -- 质押物面额
    , COLLATERAL_BIL_NO -- 质押票证编号
    , COLLATERAL_BIL_TYPE_CD -- 质押票证类型代码
    , COLLATERAL_HANDOV_DT -- 质押物移交日期
    , CNSTRING_PROJ_LAND_USE_RIGHT_CD -- 在建工程土地使用权代码
    , ESTATE_BELONGER_IDENTITY_CARD_NO -- 不动产归属人身份证编号
    , COLLATERAL_REG_ORG_NO -- 质押登记机构编号
    , PLEDGE_REG_CUST_NO -- 质押登记客户编号
    , GUAR_PROD -- 担保产品
    , REG_PROOF_NO -- 登记证明编号
    , COLLATERAL_REG_MODIF_NO -- 质押登记修改编号
    , COLLATERAL_REG_STATUS_CD -- 质押登记状态代码
    , MOV_PRO_SITU_ILUS -- 动产状况说明
    , CSDC_NET_DEREGIS_REG_NO -- 中登网注销登记编号
    , CSDC_NET_CHG_REG_NO -- 中登网变更登记编号
    , PLEDGE_CSTD_TELR_NO -- 抵质押物保管人柜员编号
    , SETUP_TELR_NO -- 建立柜员编号
    , FINAL_MODIF_TELR_NO -- 最后修改柜员编号
    , OBANK_REG_EFFECT_BEGIN_DT -- 本行登记有效起始日期
    , OBANK_REG_EFFECT_TERMI_DT -- 本行登记有效终止日期
    , CREATE_DT -- 创建日期
    , PLEDGE_GUAR_DT -- 抵质押物担保日期
    , PLEDGE_EX_WHS_DT -- 抵质押物出库日期
    , PLEDGE_IN_WHS_DT -- 抵质押物入库日期
    , PLEDGE_OFBS_REG_DT -- 抵质押物表外登记日期
    , PLEDGE_VALUT_PERIOD -- 抵质押物估值周期
    , PLEDGE_FINAL_ESTIM_DT -- 抵质押物最后评估日期
    , PLEDGE_MATU_DT -- 抵质押物到期日期
    , HANDOV_DT -- 移交日期
    , PICK_TO_READ_DT -- 调阅日期
    , RTN_DT -- 归还日期
    , PLEDGE_DEREGIS_DT -- 抵质押物注销日期
    , PLEDGE_REG_DT -- 抵质押物登记日期
    , EXT_DT -- 展期日期
    , SETUP_DT -- 建立日期
    , SETUP_TM_STAMP -- 建立时间戳
    , FINAL_MODIF_DT -- 最后修改日期
    , FINAL_MODIF_TM_STAMP -- 最后修改时间戳
    , SETUP_ORG_NO -- 建立机构编号
    , OPEN_ORG_NO -- 营业机构编号
    , TXN_ORG_NO -- 交易机构编号
    , ACCT_ORG_NO -- 账务机构编号
    , COLLATERAL_BIL_ISSUE_ORG_NO -- 质押票证签发机构编号
    , OFBS_GL_ORG_NO -- 表外总账机构编号
    , BELONG_ORG_NO -- 归属机构编号
    , RECNT_MODIF_ORG_NO -- 最后修改机构编号
    , APPRS_COMPNY_NO -- 评估机构编号
    , DATA_DT -- 数据日期
)
select
      T1.AGBHU1AB as PLEDGE_NO -- 担保物品编号
    , T1.AGPNPTSN as MTG_SER_NO -- 担保物品序号
    , T2.REQ_CUSTOMER_NAME as BORR_CUST_NAME -- 客户名称
    , T2.REQ_CUSTOMER_NO as BORR_CUST_NO -- 客户号
    , T3.COUNT_ACCOUNT as LNACCT_NO -- 借款人客户账号
    , T1.CINOCSNO as BORR_CUST_IN_CD -- 客户内码
    , T1.AGGPNM5B as PLEDGE_COMN_PRPTR_NAME -- 担保物品共有人
    , T1.AGXPNM5B as PLEDGE_EVERYONE_NAME -- 担保物品所有人
    , T1.AGJPNM5B as PLEDGE_CSTD_NAME -- 担保物品保管人
    , T3.T2._NO as MTG_EVERYONE_CUST_IN_CD -- 抵押人客户内码
    , T3.ASSET_FATHER_TYPE as PLEDGE_MCLS_CD -- 物品大类
    , T1.AGTYPDCD as GUAR_ARTI_CATE_CD -- 担保物品种类代码
    , T2.BILL_STATUS as PLEDGE_SINGLE_STATUS_CD -- 抵质押单状态
    , T2.STATUS as CRDT_PLEDGE_STATUS_CD -- 抵质押物状态
    , '' as PLEDGE_STORE_TYPE_CD -- None
    , '' as PLEDGE_PROC_STATUS_CD -- None
    , T1.PCSTU1ST as CORE_PLEDGE_STATUS_CD -- 抵质押状态
    , '1' as WEB_CORE_PLEDGE_STATUS_CD -- None
    , DECODE(T1.PCTYU1TP,'3','9','1','1','2','2',T1.PCTYU1TP) as PLEDGE_TYPE_CD -- 抵质押类型
    , T1.AGNMNM5B as PLEDGE_NAME -- 物品名称
    , T1.PXCDU1DJ as PLEDGE_REG_NO -- 登记号码
    , T1.EVBMNM5B as PLEDGE_APPRS_COMPNY_NAME -- 评估部门
    , T1.PCAMAMT as PLEDGE_AMT -- 抵质押金额
    , T1.EVAMAMT as ESTIM_VAL_TOTAL_AMT -- 评估价值
    , T1.MHAAAMT as PLEDGE_MAX_GUAR_AMT -- 最高担保金额
    , T1.USAAAMT as PLEDGE_AL_GUAR_AMT -- 已担保金额
    , T1.USTMQTY5 as PLEDGE_GUAR_TMS -- 担保次数
    , T3.ASSET_AMOUNT as PLEDGE_CNT -- 数量
    , T3.RATE_DEPRECIATION as PLEDGE_DEPRE_RATE -- 折旧率
    , T3.WARRANT_NAME as PLEDGE_WARRANT_NAME -- 权证名称
    , T3.ADDRESS as MTG_LOC -- 抵押物所在地
    , T3.MORTGAGE_ADDR as MTG_LOC_DIST_CD -- 抵押物所在地行政区划
    , T3.MORTGAGE_FLAG as PLEDGE_REG_IDNT_CD -- 抵押物登记标识
    , T3.STANDARDS as PLEDGE_SPEC -- 规格
    , T3.BOOK_VALUE as PLEDGE_BOOK_VAL_AMT -- 账面价值
    , T3.PLEDGE_NO as RELA_PLEDGE_DOC_NO -- 关联抵质押物资料编号
    , T3.FIRST_ORDER as FIRST_BIT_FLAG -- 是否第一顺位
    , T1.DJJGNM80 as PLEDGE_REG_ORG_NAME -- 登记机关
    , T3.INVOICE_NO as PLEDGE_INVOICE_NO -- 发票号码
    , T1.DYYJTY1T as PLEDGE_HANDOV_MODE_CD -- 移交方式
    , T1.ZQBZBOOL as EXT_TYPE_CD -- 展期标志
    , T1.ZFBHFRZN as STOP_PAY_NO -- 止付编号
    , T1.AGCYCCYC as CURR_CD -- 币种
    , T3.ORIGINAL_BILL_NO as AGAIN_MTG_ORIG_MTG_BIL_NO -- 再抵押的原抵押单号
    , T3.ORIGINAL_SN as AGAIN_MTG_ORIG_MTG_SER_NO -- 再抵押的原抵押序号
    , T4.IMPAWN_ASSET_NO as COLLATERAL_NO -- 质押物号码
    , T4.IMPAWN_ACCOUNT_NO as COLLATERAL_ACCT_NO -- 质押物帐号
    , T3.IS_REGISTED as OTHER_BANK_REG_FLAG -- 是否在他行登记
    , T3.OTHER_REGISTER_MONEY as OTHER_BANK_REG_AMT -- 他行登记金额
    , T3.OTHER_REGISTER_NO as OTHER_BANK_REG_CERT_NO -- 他行登记证号码
    , T3.PAYEE_BANK_NAME as PAYEE_OPEN_ACCT_BANK_NAME -- 收款人开户银行
    , T2.REQUISITION_ID as APPRV_BOOK_NO -- 审批书号
    , T1.DYTZNO18 as PICK_TO_READ_NOTICE_NO -- 调阅通知单编号
    , T1.DYBZTY1S as PICK_TO_READ_STATUS_CD -- 调阅状态
    , T1.QSPRNM5B as APPRV_PSN_NAME -- 审批人
    , T1.QDYRNM5B as PICK_TO_READ_PSN_NAME -- 调阅人
    , T1.QRDRNM5B as COGNZT_NAME -- 认定人
    , T1.BHTYTY1H as CHG_AGT_NO -- 变更协议编号
    , T3.MORTGAGE_HOUSE as FORWD_ROOM_CURR_HOS_IDNT_CD -- 期/现房标识
    , '' as PLEDGE_IN_WHS_ILUS -- None
    , '' as PLEDGE_EX_WHS_ILUS -- None
    , T1.QCFDFLNM as PLEDGE_STORE_LOC -- 存放地点
    , T3.MAX_MORTGAGE_RATE as MAX_PLEDGE_RATE -- 最高抵押率
    , T3.FIRST_PRIORITY as PRIOR_COMP_WT_AMT -- 优先受偿权数额
    , T3.WARRANT_CONDITION as OWNSHIP_SITU_ILUS -- 权属状况
    , T1.ABPYBOOL as STOP_PAY_CFM_FLAG -- 是否止付确认
    , T3.MORTGAGE_AGAIN_SIGN as AGAIN_MTG_FLAG -- 再抵押标志
    , T3.IS_MORTGAGER_FLAG as ELEC_PLEDGE_FLAG -- 电子质押标志
    , T1.PCACAC32 as COLLATERAL_BIL_ACCT_NO -- 质押票证帐号
    , T5.SHIP_REGISTER_CODE as SHIP_CLS_REG_NO -- 登记号码
    , T6.CL01AC15 as LARGE_AMT_DPSTRCP_ACCT_NO -- 账号
    , T3.DIRECTORATEAGREEMENT as HOLD_BOD_AGREE_GUAR_OPIN_BOOK_FLAG -- 有无董事会同意担保意见书
    , T3.IS_THIRD_PARTY_HIRE_AGREEMENT as HOLD_MTG_LSE_CONT_FLAG -- 有无抵押物租赁合同、出租三方协议
    , T17.ASSURESUM_OLD as DISPLC_OUT_GUAR_AMT -- 置换出担保金额
    , T8.OLD_BUILDING_CARD_NO as DISPLC_OUT_ESTATE_WARRANT_NO -- 房产权证号码
    , T6.CL02NO12 as LARGE_AMT_DPSTRCP_PROD_CD -- 大额存单产品编码
    , T1.ZHNOAC32 as MRGACCT_NO -- 保证金帐号
    , T9.BUILDING_CARD_NO as ROOM_PROP_CERT_NO -- 房产权证号码
    , T10.DRAFT_NO as BIL_NO -- 票据编号
    , T11.CAR_LICENSE_NO as VEHIC_LIC_PLATE_NO -- 车辆牌照号码
    , T12.CREDENTIALS_NO as EQTY_CERT_NO -- 权益证书编号
    , T13.INSURANCE_NO as PLEDGE_POLICY_NO -- 保险单号
    , T3.STOCKAGREEMENT as HOLD_SHAREHD_AGREE_GUAR_OPIN_BOOK_FLAG -- 有无股东会同意担保意见书
    , T3.IS_HIGHER_BODY_AGREEMENT as HOLD_SUPER_DIROR_DEPT_AGREE_BOOK_FLAG -- 有无上级主管部门/村民代表大会同意书
    , T3.IS_HIRE_SUPPLY as HOLD_HOS_RENT_SUPP_AGT_FLAG -- 有无房屋出租补充协议
    , T14.LAND_USUFRUCT_NO as LAND_CLS_LAND_USE_RIGHT_CERT_NO -- 土地使用权证号
    , T15.LAND_USUFRUCT_NO as CNSTRING_PROJ_CLS_LAND_USE_RIGHT_CERT_NO -- 土地使用权证号码
    , T16.REGISTER_CODE as MACH_EQUIP_CLS_REG_NO -- 登记号码
    , T17.REGISTER_CODE as MOV_PRO_CLS_REG_NO -- 登记号码
    , T18.ASSET_TYPE as PLEDGE_INFO_AST_CLS -- 资产分类
    , T8.BUILDING_CARD_NO as DISPLC_IN_ESTATE_WARRANT_NO -- 不动产权证号
    , T17.ASSURENO_NEW as DISPLC_IN_GUAR_NO -- 置换入担保编号
    , T17.ASSURENO_OLD as DISPLC_OUT_GUAR_NO -- 置换出的担保编号
    , T17.ASSURESUM_NEW as DISPLC_IN_GUAR_AMT -- 置换入担保金额
    , T17.INTERCHANGE_NO as DISPLC_NO -- 置换编号
    , T1.ZHFGZHFG as DISPLC_MODE_CD -- 置换方式
    , T1.ZZJEAMT as DISPLC_AMT -- 置换金额
    , T1.YNZHBOOL as PLEDGE_DISPLC_CD -- 置换标志
    , T19.ASSURETYPE_NEW as NEW_GUAR_MODE_CD -- 新担保方式
    , T17.ASSURETYPE_NEW as NEW_GUAR_TYPE_CD -- 新担保类型
    , T19.ASSURETYPE_OLD as ORIG_SPNSR_CD -- 原担保方式
    , T17.ASSURETYPE_OLD as ORIG_GUAR_TYPE_CD -- 原担保类型
    , T17.CONTRACTNO as BE_GUAR_NO -- 被担保编号（借款合同号）
    , T17.CONTRACTTYPE_OLD as ORIG_GUAR_RELA_TYPE_CD -- 原担保关系类型（合同类型）
    , T17.CONTRACTTYPE_NEW as NEW_GUAR_RELA_TYPE_CD -- 新担保关系类型（合同类型）
    , T17.STARTDATE as NEW_GUAR_START_TM -- 新担保的起始时间
    , T17.ENDDATE as NEW_GUAR_END_TM -- 新担保的结束时间
    , T13.INSURER_NAME as INSURE_CORP_NAME -- 保险公司名称
    , T13.INSURER_BENEFICIARY as INSURE_FIRST_BNFC_NAME -- 保险第一受益人
    , T13.INSURER_AMOUNT as INSURE_AMT -- 保险金额
    , T13.INSURANCE_START_DATE as POLICY_BEGIN_DT -- 保险单起始日
    , T13.INSURANCE_END_DATE as POLICY_MATU_DT -- 保险单到期日
    , T20.CASH_DEPOSIT_OWNER as MRGACCT_NAME -- 保证金户名
    , T6.CL03AMT as LARGE_AMT_DPSTRCP_SUBSCR_AMT -- 认购金额
    , T6.CL04CNCD as LARGE_AMT_DPSTRCP_SUBSCR_CHNL_CD -- 认购渠道
    , T6.CL06DATE as LARGE_AMT_DPSTRCP_SUBSCR_DT -- 认购日期
    , T6.CL08ICTP as LARGE_AMT_DPSTRCP_PAY_INT_MODE_CD -- 付息方式
    , T6.CL09DATE as LARGE_AMT_DPSTRCP_NEXT_PAY_INT_DT -- 下一付息日
    , T6.CL10DATE as LARGE_AMT_DPSTRCP_PRE_PAY_INT_DT -- 上一付息日
    , T6.CL11IBAL as LARGE_AMT_DPSTRCP_ACRDG_PERIOD_PAY_INT_CUM_AMT -- 按期付息累计
    , T6.CL12SN04 as LARGE_AMT_DPSTRCP_ACRDG_PERIOD_PAY_INT_TMS_CUM -- 按期付息期数累计
    , T6.CL15ACST as DPSTRCP_STATUS_CD -- 存单状态
    , T6.CL07DATE as LARGE_AMT_DPSTRCP_MATU_DT -- 到期日
    , T5.SHIP_NAME as SHIP_NAME -- 船名
    , T5.SHIP_REGISTER_PORT as SHIP_REG_PORT -- 船籍港
    , T5.SHIP_CALL_LETTERS as SHIP_CALL_SIGN -- 船舶呼号
    , T5.SHIP_IMO_NO as SHIP_IMO_NO -- IMO编号
    , T5.SHIP_TYPE as SHIP_CATE -- 船舶种类
    , T5.HULL_MATERIAL as SHIP_MATRL -- 船体材料
    , T5.SHIP_VALUE as SHIP_VAL -- 船舶价值
    , T5.SHIP_SIZE as SHIP_SCALE -- 尺度(总长，米)
    , T5.SHIP_WIDTH as SHIP_WIDTH -- 型宽(米)
    , T5.SHIP_DEPTH as SHIP_DEPTH -- 型深(米)
    , T5.SHIP_SUM_TONNAGE as SHIP_TOTAL_TONNAGE -- 吨位(总吨)
    , T5.SHIP_SUTTLE_TONNAGE as SHIP_NET_HEAVY_TONNAGE -- 吨位(净重)
    , T5.SHIP_OWNER as SHIP_EVERYONE_NAME -- 船舶所有人(抵押人)名称
    , T5.SHIP_OWNER_ADDR as SHIP_EVERYONE_ADDR -- 船舶所有人(抵押人)地址
    , T5.SHIP_PRODUCE_DATE as SHIP_PROD_DT -- 生产日期
    , T17.SPEC as MOV_PRO_CLS_SPEC -- 规格
    , T17.KEEPER_CUSTOMER_NO as CSTD_CUST_NO -- 保(监)管人客户号
    , T17.MANUFACTURER as PROD_MANUFAC_NAME -- 生产厂家
    , T9.LAND_ACCOUNT as ESTATE_LAND_BOOK_VAL -- 土地账面价
    , T17.STORAGE_IMPAWN_KEEP_PROT_ID as STRG_PLEDGE_CSTD_AGT_NO -- 仓储物质押保管(监管)协议编号
    , T17.MADE_DATE as MOV_PRO_PROD_DT -- 生产日期
    , T9.BUILD_DIVISION_TYPE as MDL_ROOM_INQUIRY_RGN_NO -- 中房查询地区
    , T9.BUY_HCONTRACT as BUY_HOS_CONT_NO -- 购房合同号
    , T9.GT_BUILD_DIVISION_KEY as INQUIRY_RGN -- 国土查询地区
    , T9.PROCESS_PROGRESS as MTG_PROC_SCHED_CD -- 抵押物办理进度
    , T9.PROCESS_TIME as MTG_PROC_SCHED_DEAL_TM -- 抵押物办理进度处理时间
    , T9.QLRSHXYDM as MTG_OBLG_USCD -- 抵押权利人统一社会信用代码
    , T9.QUERY_TYPE as INQUIRY_MODE_CD -- 查询方式
    , T16.PRODUCE_DATE as MACH_EQUIP_PROD_DT -- 生产日期
    , T16.BUY_PRICE as MACH_EQUIP_BUY_PRC -- 购买价格
    , T16.COMPANY as MACH_EQUIP_AFLT_CORP_NAME -- 单位
    , T16.MACHINE_TYPE as MACH_MODEL -- 机器型号
    , T16.MADE_FACTORY as MACH_EQUIP_MFR_MANUFAC -- 制造厂家
    , T16.NOW_PRICE as MACH_EQUIP_CURR_BUY_PRC -- 现购买价
    , T16.PAY_SCALE as MACH_EQUIP_GOODS_PAY_PAYOFF_RATIO -- 货款付清比例
    , T16.QUALITY_CONDITION as MACH_EQUIP_QLTY_AND_SITU -- 质量及状况
    , T10.DRAFT_TYPE as BIL_TYPE_CD -- 票据类型
    , T10.PLEDGOR_ACCT as PLEDGOR_ACCT_NO -- 出质人账号
    , T10.PLEDGOR_BANKNO as PLEDGOR_OPBANK_NO -- 出质人开户行行号
    , T10.PLEDGOR_SOCCODE as PLEDGOR_USCD -- 出质人社会统一信用代码
    , T10.PAWNEE_BANKNO as PLEDGEE_BANK_NO -- 质权人行号
    , T10.ACCEPTOR_BANKNO as ACCPTOR_OPBANK_NO -- 承兑人开户行行号
    , T10.REQ_REMARK as BIL_APP_REM -- 申请备注
    , T3.BILL_RANGE_START as SUB_START_RANGE -- 子票开始区间
    , T3.BILL_RANGE_END as SUB_END_RANGE -- 子票结束区间
    , T3.BILL_ORIGIN as BIL_SRC -- 票据来源
    , T11.CAR_BRAND_NAME as VEHIC_BRAND -- 车辆品牌
    , T11.CAR_MODEL as VEHIC_MODEL -- 车辆型号
    , T11.ENGINE_NO as ENGINE_NO -- 发动机号码
    , T11.CAR_RACK_NO as CAR_FRAME_NO -- 车架号码
    , T11.CAR_LEAVE_FACTORY_YEAR as VEHIC_LEAVE_FACTORY_YEAR -- 车辆出厂年份
    , T11.CAR_DISCARD_YEAR as VEHIC_SCRAP_YEAR -- 车辆报废年份
    , '' as MOTOR_VEHIC_REG_CERT_NO -- None
    , '' as MOTOR_VEHIC_REG_DT -- None
    , '' as MOTOR_VEHIC_REG_NO -- None
    , '' as VEHIC_BELONGER_CD -- None
    , '' as VEHIC_TYPE -- None
    , '' as VEHIC_OPER_FLAG -- None
    , '' as VEHIC_INVOICE_DT -- None
    , '' as VEHIC_TAX_SUM_AMT -- None
    , '' as VEHIC_NO_TAX_PRC -- None
    , '' as VEHIC_INVOICE_CD -- None
    , T9.BUILDING_TYPE as ESTATE_TYPE_CD -- 房产类型
    , T9.BUILDING_LOCATION as ESTATE_LOCAL_ADDR -- 房产住所
    , T9.BUILDING_AREA as ESTATE_BUILD_AREA -- 房产建筑面积
    , T9.BUILDING_STRUCT as ESTATE_STRU -- 房产结构
    , T9.BUILD_YEAR as ESTATE_BLD_YEAR -- 房产建造年份
    , T9.BUILDING_APPRAISE as ESTATE_EVALU_TOTAL_AMT -- 房产评估价
    , T9.BUILDING_ACCOUNT as ESTATE_BOOK_PRC_TOTAL_AMT -- 房产账面价
    , T9.REG_CERT_NUMBER as ESTATE_REG_PROOF_NO -- 登记证明号
    , T9.XZQH_CODE as PROV_GOLD_ESTATE_DIST_NO -- sjz行政区划编号
    , T14.LAND_ACCOUNT as LAND_CLS_LAND_BOOK_PRC -- 土地账面价
    , T9.LAND_APPRAISE as ESTATE_LAND_EVALU -- 土地评估价
    , T14.LAND_APPRAISE as LAND_CLS_LAND_ESTIM_PRC -- 土地评估价
    , T9.LAND_AREA as ESTATE_OCPY_LAND_AREA -- 占地面积
    , T14.LAND_AREA as LAND_CLS_LAND_USE_RIGHT_AREA -- 土地使用权面积
    , T15.LAND_AREA as CNSTRING_PROJ_CLS_LAND_USE_RIGHT_AREA -- 土地使用权面积
    , T9.LAND_END_DATE as ESTATE_LAND_USE_RIGHT_TERMI_DT -- 土地使用权终止日
    , T14.LAND_END_DATE as LAND_CLS_LAND_USE_RIGHT_TERMI_DT -- 土地使用权终止日
    , T15.LAND_END_DATE as CNSTRING_PROJ_CLS_LAND_USE_RIGHT_TERMI_DT -- 土地使用权终止日
    , T9.LAND_LOCATION as ESTATE_LAND_LOCAL_ADDR -- 土地处所
    , T9.DYQLRQC as ESTATE_MTG_OBLG_FULLNAME -- 抵押权利人全称
    , T14.LAND_LOCATION as LAND_CLS_LAND_LOCAL_ADDR -- 土地处所
    , T15.LAND_LOCATION as CNSTRING_PROJ_LAND_LOC_ -- 土地处所
    , T9.LAND_NO as ESTATE_LAND_NO -- 土地地号
    , T14.LAND_NO as LAND_CLS_LAND_NO -- 土地地号
    , T15.LAND_NO as CNSTRING_PROJ_LAND_NO -- 土地地号
    , T9.LAND_START_DATE as ESTATE_LAND_USE_RIGHT_BEGIN_DT -- 土地使用权起始日
    , T14.LAND_START_DATE as LAND_CLS_LAND_USE_RIGHT_BEGIN_DT -- 土地使用权起始日
    , T15.LAND_START_DATE as CNSTRING_PROJ_LAND_USE_RIGHT_BEGIN_DT -- 土地使用权起始日
    , T9.LAND_USE as ESTATE_LAND_USAGE_CD -- 土地用途
    , T14.LAND_USE as LAND_CLS_LAND_USAGE_CD -- 土地用途
    , T15.LAND_USE as CNSTRING_PROJ_LAND_USAGE_CD -- 土地用途
    , T9.LAND_USUFRUCT_NO as ESTATE_LAND_USE_RIGHT_CERT_NO -- 土地使用权证号
    , T15.LAND_PERMIT_NO as CNSTR_LAND_PLAN_LIC_NO -- 建设用地规划许可证号
    , T15.PLAN_PERMIT_NO as CNSTR_PROJ_PLAN_LIC_CERT_NO -- 建设工程规划许可证
    , T15.CONSTRUCT_NO as CNSTR_LIC_BOOK_NO -- 施工许可证号
    , T15.INPUT_SCALE as PROJ_CNSTR_AL_PUT_IN_CAP_OCUP_TOTAL_CAST_CAP_PCT -- 工程建设已投入资金占总投资额的百分比
    , T9.LAND_USUFRUCT_TYPE as ESTATE_LAND_USE_RIGHT_TYPE_CD -- 土地使用权类型
    , T14.LAND_USUFRUCT_TYPE as LAND_CLS_LAND_USE_RIGHT_TYPE_CD -- 土地使用权类型
    , T12.COMPANY_ISSUE as IMMATRL_AST_EQTY_CERT_ISSUE_CORP_NAME -- 权益证书签发单位
    , T12.DEADLINE as IMMATRL_AST_EQTY_VALID -- 权益有效期限
    , T12.IS_STEADY as IMMATRL_AST_EQTY_STABLE_FLAG -- 权益是否稳定
    , T12.ASSESS_NO as IMMATRL_AST_MAJOR_ORG_ESTIM_CERT_NO -- 专业机构评估书编号
    , T12.IS_PAYFOR as IMMATRL_AST_NEED_PAY_FEE_FLAG -- 是否需要缴纳费用
    , T12.EVIDENCES_NO as IMMATRL_AST_FEE_PAY_VOCH_NO -- 费用缴纳凭证号
    , T12.TRANSFER_DATE as IMMATRL_AST_PLEDGE_HANDOV_DT -- 质押物移交日
    , T4.IMPAWN_CURRENCY_TYPE as COLLATERAL_CURR_CD -- 质押物币种
    , T4.IMPAWN_END_DATE as COLLATERAL_MATU_DT -- 质押物到期日
    , T4.IMPAWN_ISSUE_ORGAN_NAME as COLLATERAL_ISSUE_CORP_NAME -- 质押物签发单位名称
    , T4.IMPAWN_ISSUE_ORGAN_NO as COLLATERAL_ISSUE_CORP_ORG_NO -- 质押物签发单位机构码
    , T4.IMPAWN_START_DATE as COLLATERAL_BEGIN_DT -- 质押物起始日
    , T4.IMPAWN_SURFACE_AMOUNT as COLLATERAL_DNMNT -- 质押物面额
    , T1.PCPHU1BC as COLLATERAL_BIL_NO -- 质押票证号码
    , T1.PCPTU1BT as COLLATERAL_BIL_TYPE_CD -- 质押票证类型
    , T4.IMPAWN_HANDOVER_DATE as COLLATERAL_HANDOV_DT -- 质押物移交日
    , T15.LAND_USUFRUCT_TYPE as CNSTRING_PROJ_LAND_USE_RIGHT_CD -- 土地使用权类型
    , T8.CERTID as ESTATE_BELONGER_IDENTITY_CARD_NO -- 身份证号
    , T18.ORG_NO as COLLATERAL_REG_ORG_NO -- 机构号
    , T18.CUST_ID as PLEDGE_REG_CUST_NO -- 客户号
    , T18.GUARANTEE_PRODUCT as GUAR_PROD -- 担保产品
    , T18.REGISTER_NO as REG_PROOF_NO -- 登记证明编号
    , T18.MODIFY_CODE as COLLATERAL_REG_MODIF_NO -- 修改码
    , T18.REGISTER_STATE as COLLATERAL_REG_STATUS_CD -- 状态
    , T17.STATUS as MOV_PRO_SITU_ILUS -- 状况
    , T18.CANCEL_NO as CSDC_NET_DEREGIS_REG_NO -- 注销登记编号
    , T18.REGISTER_NO_CHANGE as CSDC_NET_CHG_REG_NO -- 变更登记编号
    , T1.QBGRNM5B as PLEDGE_CSTD_TELR_NO -- 保(监)管人
    , T1.BUOPSTAF as SETUP_TELR_NO -- 建立柜员
    , T1.UPOPSTAF as FINAL_MODIF_TELR_NO -- 修改柜员
    , T3.REGISTER_BEGIN_DATE as OBANK_REG_EFFECT_BEGIN_DT -- 登记有效起始日期
    , T3.REGISTER_END_DATE as OBANK_REG_EFFECT_TERMI_DT -- 登记有效终止日期
    , T2.CREATE_DATE as CREATE_DT -- 创建日期
    , T1.AGDTDATE as PLEDGE_GUAR_DT -- 担保日期
    , T1.OUDTDATE as PLEDGE_EX_WHS_DT -- 出库日期
    , T1.IUDTDATE as PLEDGE_IN_WHS_DT -- 入库日期
    , T1.TODTDATE as PLEDGE_OFBS_REG_DT -- 表外登记日期
    , T3.ACCESS_CYCLE as PLEDGE_VALUT_PERIOD -- 估值周期
    , T1.LEDTDATE as PLEDGE_FINAL_ESTIM_DT -- 最后评估日期
    , T1.ENDTDATE as PLEDGE_MATU_DT -- 到期日期
    , T1.DEDTDATE as HANDOV_DT -- 移交日期
    , T1.DYRQDATE as PICK_TO_READ_DT -- 调阅日期
    , T1.GHRQDATE as RTN_DT -- 归还日期
    , T1.LGOTDATE as PLEDGE_DEREGIS_DT -- 注销日期
    , T1.DEJIDATE as PLEDGE_REG_DT -- 登记日期
    , T1.ZQDTDATE as EXT_DT -- 展期日期
    , T1.BUDIDATE as SETUP_DT -- 建立日期
    , T1.BUTSSTAM as SETUP_TM_STAMP -- 建立时间戳
    , T1.UPDTDATE as FINAL_MODIF_DT -- 修改日期
    , T1.UPTSSTAM as FINAL_MODIF_TM_STAMP -- 修改时间戳
    , T1.BOIDBRNO as SETUP_ORG_NO -- 建立机构号
    , T1.PCBRFLNM as COLLATERAL_BIL_ISSUE_ORG_NO -- 质押票证签发机构
    , T1.BWZZBRNO as OFBS_GL_ORG_NO -- 表外总帐机构
    , T1.GSJGBRNO as BELONG_ORG_NO -- 归属机构号
    , T1.UOIDBRNO as RECNT_MODIF_ORG_NO -- 修改机构号
    , '' as APPRS_COMPNY_NO -- None
    , ${process_date} as DATA_DT -- None
from
    ${ODS_CORE_SCHEMA}.ODS_CORE_BUFMIIDI as T1 -- 客户担保物品信息表
    LEFT JOIN ${ODS_CORE_SCHEMA}.ODS_CORE_BFFMDQCL as T6 -- 大额存单产品信息登记簿
    on T1.PCACAC3 =T6.CL01AC15
AND T6.PT_DT='${process_date}' 
AND T6.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_MORTGAGE as T2 -- 抵质押物
    on T1.AGBHU1AB = T2.BILL_NO 
AND T1.AGPNPTSN = T1.SERIAL_NO
AND T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_MORTGAGE_GENERAL as T3 -- 抵押物品--通用抵质押物 
    on T1.AGBHU1AB = T3.BILL_NO 
AND T1.AGPNPTSN = T3..SERIAL_NO
AND T3.PT_DT='${process_date}' 
AND T3.DELETED='0'
 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_MORTGAGE_IMPAWN as T4 -- 质押押物品-普通质押
    on T1.AGBHU1AB =T4.BILL_NO 
AND T1.AGPNPTSN = T4.SERIAL_NO
AND T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_MORTGAGE_SHIP as T5 -- 抵押物品-船舶 
    on T1.AGBHU1AB = T5.BILL_NO 
AND T1.AGPNPTSN = T5..SERIAL_NO
AND T5.PT_DT='${process_date}' 
AND T5.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_MORTGAGE_CHANGE_CERT as T8 -- 不动产抵押登记换证
    on T1.AGBHU1AB = T8.BILL_NO 
AND T1.AGPNPTSN = T8.SERIAL_NO
AND T8.PT_DT='${process_date}' 
AND T8.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_MORTGAGE_BUILDING as T9 -- 抵押物品--房产
    on T1.AGBHU1AB = T9.BILL_NO 
AND T1.AGPNPTSN = T9.SERIAL_NO 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_MORTGAGE_DRAFT as T10 -- 抵押物品-票据 
    on T1.AGBHU1AB = T10.BILL_NO 
AND T1.AGPNPTSN = T10.SERIAL_NO 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_MORTGAGE_CAR as T11 -- 抵押物品--汽车 
    on T1.AGBHU1AB = T11.BILL_NO 
AND T1.AGPNPTSN = T11.SERIAL_NO 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_MORTGAGE_INVISIBLE as T12 -- 质押物品-无形资产
    on T1.AGBHU1AB = T12.BILL_NO 
AND T1.AGPNPTSN = T12.SERIAL_NO 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_MORTGAGE_INSURANCE as T13 -- 保险信息
    on T1.AGBHU1AB = T13.BILL_NO 
AND T1.AGPNPTSN =T13.SERIAL_NO 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_MORTGAGE_LAND as T14 -- 抵押物品-土地 
    on T1.AGBHU1AB =T14.BILL_NO 
AND T1.AGPNPTSN =T14.SERIAL_NO 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_MORTGAGE_ENGINEERING as T15 -- 抵押物品--在建工程
    on T1.AGBHU1AB = T15.BILL_NO 
AND T1.AGPNPTSN = T15.SERIAL_NO 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_MORTGAGE_MACHINE as T16 -- 抵押物品-机器设备
    on T1.AGBHU1AB = T16.BILL_NO 
AND T1.AGPNPTSN = T16.SERIAL_NO 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_MORTGAGE_PERSONALTY as T17 -- 抵押物品-动产 
    on T1.AGBHU1AB = T17.BILL_NO 
AND T1.AGPNPTSN = T17.SERIAL_NO 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_INTERCHANGE_ASSURE_INFO as T7 -- 担保置换
    on T1.AGBHU1AB =T7.ASSURENO_NEW
AND T7.PT_DT='${process_date}' 
AND T7.DELETED='0' 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_PLEDGE_REGISTER_INFORMATION as T18 -- 质押登记信息表
    on T1.AGBHU1AB = T18.BILL_NO 
AND T1.AGPNPTSN = T18.SERIAL_NO 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_CHANGE_ASSURE_INFO as T19 -- 担保置换
    on T1.AGBHU1AB = T19.ASSURENO_NEW 
    LEFT JOIN ${ODS_XDZX_SCHEMA}.ODS_XDZX_CASH_DEPOSIT as T20 -- 保证金
    on T1.AGBHU1AB = T20.BILL_NO 
AND T1.AGPNPTSN = T20.SERIAL_NO 
where T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;
-- ==============字段映射（第2组）==============
-- 互联网核心信息

insert into table ${session}.S03_PLEDGE_ARCHIVE_TAB(
      PLEDGE_NO -- 抵质押物编号
    , MTG_SER_NO -- 抵押物序号
    , BORR_CUST_NAME -- 借款人客户名称
    , BORR_CUST_NO -- 借款人客户号
    , LNACCT_NO -- 贷款账户编号
    , BORR_CUST_IN_CD -- 借款人客户内码
    , PLEDGE_COMN_PRPTR_NAME -- 抵质押物共有人名称
    , PLEDGE_EVERYONE_NAME -- 抵质押物所有人名称
    , PLEDGE_CSTD_NAME -- 抵质押物保管人名称
    , MTG_EVERYONE_CUST_IN_CD -- 抵押物所有人客户内码
    , PLEDGE_MCLS_CD -- 抵质押物大类代码
    , GUAR_ARTI_CATE_CD -- 担保物品种类代码
    , PLEDGE_SINGLE_STATUS_CD -- 抵质押单状态代码
    , CRDT_PLEDGE_STATUS_CD -- 大信贷抵质押物状态代码
    , PLEDGE_STORE_TYPE_CD -- 抵质押物出入库类型代码
    , PLEDGE_PROC_STATUS_CD -- 抵质押物流程状态代码
    , CORE_PLEDGE_STATUS_CD -- 核心抵质押状态代码
    , WEB_CORE_PLEDGE_STATUS_CD -- 网络核心抵质押状态代码
    , PLEDGE_TYPE_CD -- 抵质押类型代码
    , PLEDGE_NAME -- 抵质押物名称
    , PLEDGE_REG_NO -- 抵质押物登记编号
    , PLEDGE_APPRS_COMPNY_NAME -- 抵质押物评估机构名称
    , PLEDGE_AMT -- 抵质押金额
    , ESTIM_VAL_TOTAL_AMT -- 评估价值总额
    , PLEDGE_MAX_GUAR_AMT -- 抵质押物最高担保金额
    , PLEDGE_AL_GUAR_AMT -- 抵质押物已担保金额
    , PLEDGE_GUAR_TMS -- 抵质押物担保次数
    , PLEDGE_CNT -- 抵质押物数量
    , PLEDGE_DEPRE_RATE -- 抵质押物折旧率
    , PLEDGE_WARRANT_NAME -- 抵质押物权证名称
    , MTG_LOC -- 抵押物所在地
    , MTG_LOC_DIST_CD -- 抵押物所在地行政区划编码
    , PLEDGE_REG_IDNT_CD -- 抵质押物登记标识代码
    , PLEDGE_SPEC -- 抵质押物规格
    , PLEDGE_BOOK_VAL_AMT -- 抵质押物账面价值金额
    , RELA_PLEDGE_DOC_NO -- 关联抵质押物资料编号
    , FIRST_BIT_FLAG -- 第一顺位标志
    , PLEDGE_REG_ORG_NAME -- 抵质押物登记机关名称
    , PLEDGE_INVOICE_NO -- 抵质押物发票编号
    , PLEDGE_HANDOV_MODE_CD -- 抵质押物移交方式代码
    , EXT_TYPE_CD -- 展期类型代码
    , STOP_PAY_NO -- 止付编号
    , CURR_CD -- 币种代码
    , AGAIN_MTG_ORIG_MTG_BIL_NO -- 再抵押原抵押单号
    , AGAIN_MTG_ORIG_MTG_SER_NO -- 再抵押原抵押序号
    , COLLATERAL_NO -- 质押物编号
    , COLLATERAL_ACCT_NO -- 质押物账户编号
    , OTHER_BANK_REG_FLAG -- 他行登记标志
    , OTHER_BANK_REG_AMT -- 他行登记金额
    , OTHER_BANK_REG_CERT_NO -- 他行登记证号码
    , PAYEE_OPEN_ACCT_BANK_NAME -- 收款人开户银行名称
    , APPRV_BOOK_NO -- 审批书编号
    , PICK_TO_READ_NOTICE_NO -- 调阅通知单编号
    , PICK_TO_READ_STATUS_CD -- 调阅状态代码
    , APPRV_PSN_NAME -- 审批人名称
    , PICK_TO_READ_PSN_NAME -- 调阅人名称
    , COGNZT_NAME -- 认定人名称
    , CHG_AGT_NO -- 变更协议编号
    , FORWD_ROOM_CURR_HOS_IDNT_CD -- 期房现房标识代码
    , PLEDGE_IN_WHS_ILUS -- 抵质押物入库说明
    , PLEDGE_EX_WHS_ILUS -- 抵质押物出库说明
    , PLEDGE_STORE_LOC -- 抵质押物存放地点
    , MAX_PLEDGE_RATE -- 最高抵质押率
    , PRIOR_COMP_WT_AMT -- 优先受偿权数额
    , OWNSHIP_SITU_ILUS -- 权属状况说明
    , STOP_PAY_CFM_FLAG -- 止付确认标志
    , AGAIN_MTG_FLAG -- 再抵押标志
    , ELEC_PLEDGE_FLAG -- 电子质押标志
    , COLLATERAL_BIL_ACCT_NO -- 质押票证账户编号
    , SHIP_CLS_REG_NO -- 船舶类登记号码
    , LARGE_AMT_DPSTRCP_ACCT_NO -- 大额存单账户编号
    , HOLD_BOD_AGREE_GUAR_OPIN_BOOK_FLAG -- 持有董事会同意担保意见书标志
    , HOLD_MTG_LSE_CONT_FLAG -- 持有抵押物租赁合同标志
    , DISPLC_OUT_GUAR_AMT -- 置换出担保金额
    , DISPLC_OUT_ESTATE_WARRANT_NO -- 置换出房产权证编号
    , LARGE_AMT_DPSTRCP_PROD_CD -- 大额存单产品编码
    , MRGACCT_NO -- 保证金账户编号
    , ROOM_PROP_CERT_NO -- 房产权证编号
    , BIL_NO -- 票据编号
    , VEHIC_LIC_PLATE_NO -- 车辆牌照号码
    , EQTY_CERT_NO -- 权益证书编号
    , PLEDGE_POLICY_NO -- 抵质押物保险单编号
    , HOLD_SHAREHD_AGREE_GUAR_OPIN_BOOK_FLAG -- 持有股东会同意担保意见书标志
    , HOLD_SUPER_DIROR_DEPT_AGREE_BOOK_FLAG -- 持有上级主管部门同意书标志
    , HOLD_HOS_RENT_SUPP_AGT_FLAG -- 持有房屋出租补充协议标志
    , LAND_CLS_LAND_USE_RIGHT_CERT_NO -- 土地类土地使用权证编号
    , CNSTRING_PROJ_CLS_LAND_USE_RIGHT_CERT_NO -- 在建工程类土地使用权证编号
    , MACH_EQUIP_CLS_REG_NO -- 机器设备类登记编号
    , MOV_PRO_CLS_REG_NO -- 动产类登记编号
    , PLEDGE_INFO_AST_CLS -- 质押信息资产分类
    , DISPLC_IN_ESTATE_WARRANT_NO -- 置换入不动产权证编号
    , DISPLC_IN_GUAR_NO -- 置换入担保编号
    , DISPLC_OUT_GUAR_NO -- 置换出担保编号
    , DISPLC_IN_GUAR_AMT -- 置换入担保金额
    , DISPLC_NO -- 置换编号
    , DISPLC_MODE_CD -- 置换方式代码
    , DISPLC_AMT -- 置换金额
    , PLEDGE_DISPLC_CD -- 抵质押物置换代码
    , NEW_GUAR_MODE_CD -- 新担保方式代码
    , NEW_GUAR_TYPE_CD -- 新担保类型代码
    , ORIG_SPNSR_CD -- 原担保方式代码
    , ORIG_GUAR_TYPE_CD -- 原担保类型代码
    , BE_GUAR_NO -- 被担保编号
    , ORIG_GUAR_RELA_TYPE_CD -- 原担保关系类型代码
    , NEW_GUAR_RELA_TYPE_CD -- 新担保关系类型代码
    , NEW_GUAR_START_TM -- 新担保起始时间
    , NEW_GUAR_END_TM -- 新担保结束时间
    , INSURE_CORP_NAME -- 保险公司名称
    , INSURE_FIRST_BNFC_NAME -- 保险第一受益人名称
    , INSURE_AMT -- 保险金额
    , POLICY_BEGIN_DT -- 保险单起始日期
    , POLICY_MATU_DT -- 保险单到期日期
    , MRGACCT_NAME -- 保证金账户名称
    , LARGE_AMT_DPSTRCP_SUBSCR_AMT -- 大额存单认购金额
    , LARGE_AMT_DPSTRCP_SUBSCR_CHNL_CD -- 大额存单认购渠道代码
    , LARGE_AMT_DPSTRCP_SUBSCR_DT -- 大额存单认购日期
    , LARGE_AMT_DPSTRCP_PAY_INT_MODE_CD -- 大额存单付息方式代码
    , LARGE_AMT_DPSTRCP_NEXT_PAY_INT_DT -- 大额存单下一付息日期
    , LARGE_AMT_DPSTRCP_PRE_PAY_INT_DT -- 大额存单上一付息日期
    , LARGE_AMT_DPSTRCP_ACRDG_PERIOD_PAY_INT_CUM_AMT -- 大额存单按期付息累计金额
    , LARGE_AMT_DPSTRCP_ACRDG_PERIOD_PAY_INT_TMS_CUM -- 大额存单按期付息期数累计
    , DPSTRCP_STATUS_CD -- 存单状态代码
    , LARGE_AMT_DPSTRCP_MATU_DT -- 大额存单到期日期
    , SHIP_NAME -- 船名
    , SHIP_REG_PORT -- 船籍港
    , SHIP_CALL_SIGN -- 船舶呼号
    , SHIP_IMO_NO -- 船舶IMO编号
    , SHIP_CATE -- 船舶种类
    , SHIP_MATRL -- 船体材料
    , SHIP_VAL -- 船舶价值
    , SHIP_SCALE -- 船舶尺度
    , SHIP_WIDTH -- 船舶型宽
    , SHIP_DEPTH -- 船舶型深
    , SHIP_TOTAL_TONNAGE -- 船舶总吨位
    , SHIP_NET_HEAVY_TONNAGE -- 船舶净重吨位
    , SHIP_EVERYONE_NAME -- 船舶所有人名称
    , SHIP_EVERYONE_ADDR -- 船舶所有人地址
    , SHIP_PROD_DT -- 船舶生产日期
    , MOV_PRO_CLS_SPEC -- 动产类规格
    , CSTD_CUST_NO -- 动产保管人客户编号
    , PROD_MANUFAC_NAME -- 生产厂家名称
    , ESTATE_LAND_BOOK_VAL -- 房产土地账面价值
    , STRG_PLEDGE_CSTD_AGT_NO -- 仓储物质押保管协议编号
    , MOV_PRO_PROD_DT -- 动产生产日期
    , MDL_ROOM_INQUIRY_RGN_NO -- 中房查询地区编号
    , BUY_HOS_CONT_NO -- 购房合同编号
    , INQUIRY_RGN -- 国土查询地区
    , MTG_PROC_SCHED_CD -- 抵押物办理进度代码
    , MTG_PROC_SCHED_DEAL_TM -- 抵押物办理进度处理时间
    , MTG_OBLG_USCD -- 抵押权利人统一社会信用代码
    , INQUIRY_MODE_CD -- 查询方式代码
    , MACH_EQUIP_PROD_DT -- 机器设备生产日期
    , MACH_EQUIP_BUY_PRC -- 机器设备购买价格
    , MACH_EQUIP_AFLT_CORP_NAME -- 机器设备所属公司名称
    , MACH_MODEL -- 机器型号
    , MACH_EQUIP_MFR_MANUFAC -- 机器设备制造厂家
    , MACH_EQUIP_CURR_BUY_PRC -- 机器设备现购买价格
    , MACH_EQUIP_GOODS_PAY_PAYOFF_RATIO -- 机器设备货款付清比例
    , MACH_EQUIP_QLTY_AND_SITU -- 机器设备质量及状况
    , BIL_TYPE_CD -- 票据类型代码
    , PLEDGOR_ACCT_NO -- 出质人账户编号
    , PLEDGOR_OPBANK_NO -- 出质人开户行行号
    , PLEDGOR_USCD -- 出质人统一社会信用代码
    , PLEDGEE_BANK_NO -- 质权人行号
    , ACCPTOR_OPBANK_NO -- 承兑人开户行行号
    , BIL_APP_REM -- 票据申请备注
    , SUB_START_RANGE -- 子票开始区间
    , SUB_END_RANGE -- 子票结束区间
    , BIL_SRC -- 票据来源
    , VEHIC_BRAND -- 车辆品牌
    , VEHIC_MODEL -- 车辆型号
    , ENGINE_NO -- 发动机编号
    , CAR_FRAME_NO -- 车架编号
    , VEHIC_LEAVE_FACTORY_YEAR -- 车辆出厂年份
    , VEHIC_SCRAP_YEAR -- 车辆报废年份
    , MOTOR_VEHIC_REG_CERT_NO -- 机动车登记证书编号
    , MOTOR_VEHIC_REG_DT -- 机动车登记日期
    , MOTOR_VEHIC_REG_NO -- 机动车登记编号
    , VEHIC_BELONGER_CD -- 车辆归属人代码
    , VEHIC_TYPE -- 车辆类型
    , VEHIC_OPER_FLAG -- 车辆营运标志
    , VEHIC_INVOICE_DT -- 车辆发票日期
    , VEHIC_TAX_SUM_AMT -- 车辆计税合计金额
    , VEHIC_NO_TAX_PRC -- 车辆不含税价
    , VEHIC_INVOICE_CD -- 车辆发票代码
    , ESTATE_TYPE_CD -- 房产类型代码
    , ESTATE_LOCAL_ADDR -- 房产所在地址
    , ESTATE_BUILD_AREA -- 房产建筑面积
    , ESTATE_STRU -- 房产结构
    , ESTATE_BLD_YEAR -- 房产建造年份
    , ESTATE_EVALU_TOTAL_AMT -- 房产评估价总额
    , ESTATE_BOOK_PRC_TOTAL_AMT -- 房产账面价总额
    , ESTATE_REG_PROOF_NO -- 房产登记证明编号
    , PROV_GOLD_ESTATE_DIST_NO -- 省金综房产行政区划编号
    , LAND_CLS_LAND_BOOK_PRC -- 土地类土地账面价格
    , ESTATE_LAND_EVALU -- 房产土地评估价
    , LAND_CLS_LAND_ESTIM_PRC -- 土地类土地评估价格
    , ESTATE_OCPY_LAND_AREA -- 房产占地面积
    , LAND_CLS_LAND_USE_RIGHT_AREA -- 土地类土地使用权面积
    , CNSTRING_PROJ_CLS_LAND_USE_RIGHT_AREA -- 在建工程类土地使用权面积
    , ESTATE_LAND_USE_RIGHT_TERMI_DT -- 房产土地使用权终止日期
    , LAND_CLS_LAND_USE_RIGHT_TERMI_DT -- 土地类土地使用权终止日期
    , CNSTRING_PROJ_CLS_LAND_USE_RIGHT_TERMI_DT -- 在建工程类土地使用权终止日期
    , ESTATE_LAND_LOCAL_ADDR -- 房产土地所在地址
    , ESTATE_MTG_OBLG_FULLNAME -- 房产抵押权利人全称
    , LAND_CLS_LAND_LOCAL_ADDR -- 土地类土地所在地址
    , CNSTRING_PROJ_LAND_LOC_ -- 在建工程土地所在地址
    , ESTATE_LAND_NO -- 房产土地地号
    , LAND_CLS_LAND_NO -- 土地类土地地号
    , CNSTRING_PROJ_LAND_NO -- 在建工程土地地号
    , ESTATE_LAND_USE_RIGHT_BEGIN_DT -- 房产土地使用权起始日期
    , LAND_CLS_LAND_USE_RIGHT_BEGIN_DT -- 土地类土地使用权起始日期
    , CNSTRING_PROJ_LAND_USE_RIGHT_BEGIN_DT -- 在建工程土地使用权起始日期
    , ESTATE_LAND_USAGE_CD -- 房产土地用途代码
    , LAND_CLS_LAND_USAGE_CD -- 土地类土地用途代码
    , CNSTRING_PROJ_LAND_USAGE_CD -- 在建工程土地用途代码
    , ESTATE_LAND_USE_RIGHT_CERT_NO -- 房产土地使用权证书编号
    , CNSTR_LAND_PLAN_LIC_NO -- 建设用地规划许可证编号
    , CNSTR_PROJ_PLAN_LIC_CERT_NO -- 建设工程规划许可证书编号
    , CNSTR_LIC_BOOK_NO -- 施工许可证书编号
    , PROJ_CNSTR_AL_PUT_IN_CAP_OCUP_TOTAL_CAST_CAP_PCT -- 工程建设已投入资金占总投资额百分比
    , ESTATE_LAND_USE_RIGHT_TYPE_CD -- 房产土地使用权类型代码
    , LAND_CLS_LAND_USE_RIGHT_TYPE_CD -- 土地类土地使用权类型代码
    , IMMATRL_AST_EQTY_CERT_ISSUE_CORP_NAME -- 无形资产权益证书签发单位名称
    , IMMATRL_AST_EQTY_VALID -- 无形资产权益有效期限
    , IMMATRL_AST_EQTY_STABLE_FLAG -- 无形资产权益稳定标志
    , IMMATRL_AST_MAJOR_ORG_ESTIM_CERT_NO -- 无形资产专业机构评估证书编号
    , IMMATRL_AST_NEED_PAY_FEE_FLAG -- 无形资产需缴纳费用标志
    , IMMATRL_AST_FEE_PAY_VOCH_NO -- 无形资产费用缴纳凭证号码
    , IMMATRL_AST_PLEDGE_HANDOV_DT -- 无形资产质押物移交日期
    , COLLATERAL_CURR_CD -- 质押物币种代码
    , COLLATERAL_MATU_DT -- 质押物到期日期
    , COLLATERAL_ISSUE_CORP_NAME -- 质押物签发单位名称
    , COLLATERAL_ISSUE_CORP_ORG_NO -- 质押物签发单位机构编号
    , COLLATERAL_BEGIN_DT -- 质押物起始日期
    , COLLATERAL_DNMNT -- 质押物面额
    , COLLATERAL_BIL_NO -- 质押票证编号
    , COLLATERAL_BIL_TYPE_CD -- 质押票证类型代码
    , COLLATERAL_HANDOV_DT -- 质押物移交日期
    , CNSTRING_PROJ_LAND_USE_RIGHT_CD -- 在建工程土地使用权代码
    , ESTATE_BELONGER_IDENTITY_CARD_NO -- 不动产归属人身份证编号
    , COLLATERAL_REG_ORG_NO -- 质押登记机构编号
    , PLEDGE_REG_CUST_NO -- 质押登记客户编号
    , GUAR_PROD -- 担保产品
    , REG_PROOF_NO -- 登记证明编号
    , COLLATERAL_REG_MODIF_NO -- 质押登记修改编号
    , COLLATERAL_REG_STATUS_CD -- 质押登记状态代码
    , MOV_PRO_SITU_ILUS -- 动产状况说明
    , CSDC_NET_DEREGIS_REG_NO -- 中登网注销登记编号
    , CSDC_NET_CHG_REG_NO -- 中登网变更登记编号
    , PLEDGE_CSTD_TELR_NO -- 抵质押物保管人柜员编号
    , SETUP_TELR_NO -- 建立柜员编号
    , FINAL_MODIF_TELR_NO -- 最后修改柜员编号
    , OBANK_REG_EFFECT_BEGIN_DT -- 本行登记有效起始日期
    , OBANK_REG_EFFECT_TERMI_DT -- 本行登记有效终止日期
    , CREATE_DT -- 创建日期
    , PLEDGE_GUAR_DT -- 抵质押物担保日期
    , PLEDGE_EX_WHS_DT -- 抵质押物出库日期
    , PLEDGE_IN_WHS_DT -- 抵质押物入库日期
    , PLEDGE_OFBS_REG_DT -- 抵质押物表外登记日期
    , PLEDGE_VALUT_PERIOD -- 抵质押物估值周期
    , PLEDGE_FINAL_ESTIM_DT -- 抵质押物最后评估日期
    , PLEDGE_MATU_DT -- 抵质押物到期日期
    , HANDOV_DT -- 移交日期
    , PICK_TO_READ_DT -- 调阅日期
    , RTN_DT -- 归还日期
    , PLEDGE_DEREGIS_DT -- 抵质押物注销日期
    , PLEDGE_REG_DT -- 抵质押物登记日期
    , EXT_DT -- 展期日期
    , SETUP_DT -- 建立日期
    , SETUP_TM_STAMP -- 建立时间戳
    , FINAL_MODIF_DT -- 最后修改日期
    , FINAL_MODIF_TM_STAMP -- 最后修改时间戳
    , SETUP_ORG_NO -- 建立机构编号
    , OPEN_ORG_NO -- 营业机构编号
    , TXN_ORG_NO -- 交易机构编号
    , ACCT_ORG_NO -- 账务机构编号
    , COLLATERAL_BIL_ISSUE_ORG_NO -- 质押票证签发机构编号
    , OFBS_GL_ORG_NO -- 表外总账机构编号
    , BELONG_ORG_NO -- 归属机构编号
    , RECNT_MODIF_ORG_NO -- 最后修改机构编号
    , APPRS_COMPNY_NO -- 评估机构编号
    , DATA_DT -- 数据日期
)
select
      T1.COLTNO as PLEDGE_NO -- 抵质押物编号
    , '1' as MTG_SER_NO -- None
    , T1.ACCTNA as BORR_CUST_NAME -- 客户名称
    , CONCAT(TRIM(T2.IDTFTP, TRIM(T2.IDTFNO)) as BORR_CUST_NO -- 证件类型+证件号码
    , '' as LNACCT_NO -- None
    , T1.CUSTNO as BORR_CUST_IN_CD -- 客户号
    , T2.SHARNA as PLEDGE_COMN_PRPTR_NAME -- 抵质押物共有人姓名
    , T2.OWNNAM as PLEDGE_EVERYONE_NAME -- 所有人姓名
    , '' as PLEDGE_CSTD_NAME -- None
    , T1.CUSTNO as MTG_EVERYONE_CUST_IN_CD -- 客户号
    , CASE T3.PLEGNA WHEN '0' THEN '23' ELSE NULL END as PLEDGE_MCLS_CD -- 押品种类
    , CASE T3.PLEGNA WHEN '0' THEN '232' ELSE NULL END as GUAR_ARTI_CATE_CD -- 押品种类
    , '' as PLEDGE_SINGLE_STATUS_CD -- None
    , '' as CRDT_PLEDGE_STATUS_CD -- None
    , T4.CLOTFG as PLEDGE_STORE_TYPE_CD -- 出入库类型
    , T2.PSTATU as PLEDGE_PROC_STATUS_CD -- 押品流程状态
    , DECODE(T2.PLESTA,'0','1','1','2','2','3',T2.PLESTA) as CORE_PLEDGE_STATUS_CD -- 押品状态
    , T1.COLTST as WEB_CORE_PLEDGE_STATUS_CD -- 抵质押状态
    , T4.COLTTP as PLEDGE_TYPE_CD -- 抵质押物方式
    , T1.COLTMA as PLEDGE_NAME -- 抵质押物名称
    , '' as PLEDGE_REG_NO -- None
    , '' as PLEDGE_APPRS_COMPNY_NAME -- None
    , T1.COLTAM as PLEDGE_AMT -- 抵质押金额
    , T4.TRANAM as ESTIM_VAL_TOTAL_AMT -- 交易金额
    , '' as PLEDGE_MAX_GUAR_AMT -- None
    , '' as PLEDGE_AL_GUAR_AMT -- None
    , '' as PLEDGE_GUAR_TMS -- None
    , '' as PLEDGE_CNT -- None
    , '' as PLEDGE_DEPRE_RATE -- None
    , '' as PLEDGE_WARRANT_NAME -- None
    , '' as MTG_LOC -- None
    , '' as MTG_LOC_DIST_CD -- None
    , '' as PLEDGE_REG_IDNT_CD -- None
    , '' as PLEDGE_SPEC -- None
    , '' as PLEDGE_BOOK_VAL_AMT -- None
    , '' as RELA_PLEDGE_DOC_NO -- None
    , '' as FIRST_BIT_FLAG -- None
    , T2.REGDEP as PLEDGE_REG_ORG_NAME -- 登记机关
    , T2.BILNUM as PLEDGE_INVOICE_NO -- 发票号码
    , '' as PLEDGE_HANDOV_MODE_CD -- None
    , '' as EXT_TYPE_CD -- None
    , '' as STOP_PAY_NO -- None
    , T1.CRCYCD as CURR_CD -- 币种
    , '' as AGAIN_MTG_ORIG_MTG_BIL_NO -- None
    , '' as AGAIN_MTG_ORIG_MTG_SER_NO -- None
    , '' as COLLATERAL_NO -- None
    , '' as COLLATERAL_ACCT_NO -- None
    , '' as OTHER_BANK_REG_FLAG -- None
    , '' as OTHER_BANK_REG_AMT -- None
    , '' as OTHER_BANK_REG_CERT_NO -- None
    , '' as PAYEE_OPEN_ACCT_BANK_NAME -- None
    , '' as APPRV_BOOK_NO -- None
    , '' as PICK_TO_READ_NOTICE_NO -- None
    , '' as PICK_TO_READ_STATUS_CD -- None
    , '' as APPRV_PSN_NAME -- None
    , '' as PICK_TO_READ_PSN_NAME -- None
    , '' as COGNZT_NAME -- None
    , '' as CHG_AGT_NO -- None
    , '' as FORWD_ROOM_CURR_HOS_IDNT_CD -- None
    , T5.INSHOW as PLEDGE_IN_WHS_ILUS -- 入库说明
    , T5.OUTSHW as PLEDGE_EX_WHS_ILUS -- 出库说明
    , '' as PLEDGE_STORE_LOC -- None
    , T3.EVTAXT as MAX_PLEDGE_RATE -- 抵质押率
    , '' as PRIOR_COMP_WT_AMT -- None
    , '' as OWNSHIP_SITU_ILUS -- None
    , '' as STOP_PAY_CFM_FLAG -- None
    , '' as AGAIN_MTG_FLAG -- None
    , '' as ELEC_PLEDGE_FLAG -- None
    , '' as COLLATERAL_BIL_ACCT_NO -- None
    , '' as SHIP_CLS_REG_NO -- None
    , '' as LARGE_AMT_DPSTRCP_ACCT_NO -- None
    , '' as HOLD_BOD_AGREE_GUAR_OPIN_BOOK_FLAG -- None
    , '' as HOLD_MTG_LSE_CONT_FLAG -- None
    , '' as DISPLC_OUT_GUAR_AMT -- None
    , '' as DISPLC_OUT_ESTATE_WARRANT_NO -- None
    , '' as LARGE_AMT_DPSTRCP_PROD_CD -- None
    , '' as MRGACCT_NO -- None
    , '' as ROOM_PROP_CERT_NO -- None
    , '' as BIL_NO -- None
    , '' as VEHIC_LIC_PLATE_NO -- None
    , '' as EQTY_CERT_NO -- None
    , '' as PLEDGE_POLICY_NO -- None
    , '' as HOLD_SHAREHD_AGREE_GUAR_OPIN_BOOK_FLAG -- None
    , '' as HOLD_SUPER_DIROR_DEPT_AGREE_BOOK_FLAG -- None
    , '' as HOLD_HOS_RENT_SUPP_AGT_FLAG -- None
    , '' as LAND_CLS_LAND_USE_RIGHT_CERT_NO -- None
    , '' as CNSTRING_PROJ_CLS_LAND_USE_RIGHT_CERT_NO -- None
    , '' as MACH_EQUIP_CLS_REG_NO -- None
    , '' as MOV_PRO_CLS_REG_NO -- None
    , '' as PLEDGE_INFO_AST_CLS -- None
    , '' as DISPLC_IN_ESTATE_WARRANT_NO -- None
    , '' as DISPLC_IN_GUAR_NO -- None
    , '' as DISPLC_OUT_GUAR_NO -- None
    , '' as DISPLC_IN_GUAR_AMT -- None
    , '' as DISPLC_NO -- None
    , '' as DISPLC_MODE_CD -- None
    , '' as DISPLC_AMT -- None
    , '' as PLEDGE_DISPLC_CD -- None
    , '' as NEW_GUAR_MODE_CD -- None
    , '' as NEW_GUAR_TYPE_CD -- None
    , '' as ORIG_SPNSR_CD -- None
    , '' as ORIG_GUAR_TYPE_CD -- None
    , '' as BE_GUAR_NO -- None
    , '' as ORIG_GUAR_RELA_TYPE_CD -- None
    , '' as NEW_GUAR_RELA_TYPE_CD -- None
    , '' as NEW_GUAR_START_TM -- None
    , '' as NEW_GUAR_END_TM -- None
    , '' as INSURE_CORP_NAME -- None
    , '' as INSURE_FIRST_BNFC_NAME -- None
    , '' as INSURE_AMT -- None
    , '' as POLICY_BEGIN_DT -- None
    , '' as POLICY_MATU_DT -- None
    , '' as MRGACCT_NAME -- None
    , '' as LARGE_AMT_DPSTRCP_SUBSCR_AMT -- None
    , '' as LARGE_AMT_DPSTRCP_SUBSCR_CHNL_CD -- None
    , '' as LARGE_AMT_DPSTRCP_SUBSCR_DT -- None
    , '' as LARGE_AMT_DPSTRCP_PAY_INT_MODE_CD -- None
    , '' as LARGE_AMT_DPSTRCP_NEXT_PAY_INT_DT -- None
    , '' as LARGE_AMT_DPSTRCP_PRE_PAY_INT_DT -- None
    , '' as LARGE_AMT_DPSTRCP_ACRDG_PERIOD_PAY_INT_CUM_AMT -- None
    , '' as LARGE_AMT_DPSTRCP_ACRDG_PERIOD_PAY_INT_TMS_CUM -- None
    , '' as DPSTRCP_STATUS_CD -- None
    , '' as LARGE_AMT_DPSTRCP_MATU_DT -- None
    , '' as SHIP_NAME -- None
    , '' as SHIP_REG_PORT -- None
    , '' as SHIP_CALL_SIGN -- None
    , '' as SHIP_IMO_NO -- None
    , '' as SHIP_CATE -- None
    , '' as SHIP_MATRL -- None
    , '' as SHIP_VAL -- None
    , '' as SHIP_SCALE -- None
    , '' as SHIP_WIDTH -- None
    , '' as SHIP_DEPTH -- None
    , '' as SHIP_TOTAL_TONNAGE -- None
    , '' as SHIP_NET_HEAVY_TONNAGE -- None
    , '' as SHIP_EVERYONE_NAME -- None
    , '' as SHIP_EVERYONE_ADDR -- None
    , '' as SHIP_PROD_DT -- None
    , '' as MOV_PRO_CLS_SPEC -- None
    , '' as CSTD_CUST_NO -- None
    , '' as PROD_MANUFAC_NAME -- None
    , '' as ESTATE_LAND_BOOK_VAL -- None
    , '' as STRG_PLEDGE_CSTD_AGT_NO -- None
    , '' as MOV_PRO_PROD_DT -- None
    , '' as MDL_ROOM_INQUIRY_RGN_NO -- None
    , '' as BUY_HOS_CONT_NO -- None
    , '' as INQUIRY_RGN -- None
    , '' as MTG_PROC_SCHED_CD -- None
    , '' as MTG_PROC_SCHED_DEAL_TM -- None
    , '' as MTG_OBLG_USCD -- None
    , '' as INQUIRY_MODE_CD -- None
    , '' as MACH_EQUIP_PROD_DT -- None
    , '' as MACH_EQUIP_BUY_PRC -- None
    , '' as MACH_EQUIP_AFLT_CORP_NAME -- None
    , '' as MACH_MODEL -- None
    , '' as MACH_EQUIP_MFR_MANUFAC -- None
    , '' as MACH_EQUIP_CURR_BUY_PRC -- None
    , '' as MACH_EQUIP_GOODS_PAY_PAYOFF_RATIO -- None
    , '' as MACH_EQUIP_QLTY_AND_SITU -- None
    , '' as BIL_TYPE_CD -- None
    , '' as PLEDGOR_ACCT_NO -- None
    , '' as PLEDGOR_OPBANK_NO -- None
    , '' as PLEDGOR_USCD -- None
    , '' as PLEDGEE_BANK_NO -- None
    , '' as ACCPTOR_OPBANK_NO -- None
    , '' as BIL_APP_REM -- None
    , '' as SUB_START_RANGE -- None
    , '' as SUB_END_RANGE -- None
    , '' as BIL_SRC -- None
    , T2.CARBRA as VEHIC_BRAND -- 车辆品牌
    , T2.CARMOD as VEHIC_MODEL -- 车辆型号
    , T2.ENGINO as ENGINE_NO -- 发动机号
    , T2.CARVIN as CAR_FRAME_NO -- 车辆识别代号/车架号
    , '' as VEHIC_LEAVE_FACTORY_YEAR -- None
    , '' as VEHIC_SCRAP_YEAR -- None
    , T2.MOTOCE as MOTOR_VEHIC_REG_CERT_NO -- 机动车登记证书编号
    , T2.REDATE as MOTOR_VEHIC_REG_DT -- 登记日期
    , T2.MOTONU as MOTOR_VEHIC_REG_NO -- 机动车登记编号
    , T2.OWNCAR as VEHIC_BELONGER_CD -- 车辆归属
    , T2.CARTYP as VEHIC_TYPE -- 车辆类型
    , T2.USETYP as VEHIC_OPER_FLAG -- 使用性质
    , T2.BILDAT as VEHIC_INVOICE_DT -- 发票日期
    , T2.TALTAX as VEHIC_TAX_SUM_AMT -- 计税合计
    , T2.NOTAXT as VEHIC_NO_TAX_PRC -- 不含税价
    , T2.BILLNO as VEHIC_INVOICE_CD -- 发票代码
    , '' as ESTATE_TYPE_CD -- None
    , '' as ESTATE_LOCAL_ADDR -- None
    , '' as ESTATE_BUILD_AREA -- None
    , '' as ESTATE_STRU -- None
    , '' as ESTATE_BLD_YEAR -- None
    , '' as ESTATE_EVALU_TOTAL_AMT -- None
    , '' as ESTATE_BOOK_PRC_TOTAL_AMT -- None
    , '' as ESTATE_REG_PROOF_NO -- None
    , '' as PROV_GOLD_ESTATE_DIST_NO -- None
    , '' as LAND_CLS_LAND_BOOK_PRC -- None
    , '' as ESTATE_LAND_EVALU -- None
    , '' as LAND_CLS_LAND_ESTIM_PRC -- None
    , '' as ESTATE_OCPY_LAND_AREA -- None
    , '' as LAND_CLS_LAND_USE_RIGHT_AREA -- None
    , '' as CNSTRING_PROJ_CLS_LAND_USE_RIGHT_AREA -- None
    , '' as ESTATE_LAND_USE_RIGHT_TERMI_DT -- None
    , '' as LAND_CLS_LAND_USE_RIGHT_TERMI_DT -- None
    , '' as CNSTRING_PROJ_CLS_LAND_USE_RIGHT_TERMI_DT -- None
    , '' as ESTATE_LAND_LOCAL_ADDR -- None
    , '' as ESTATE_MTG_OBLG_FULLNAME -- None
    , '' as LAND_CLS_LAND_LOCAL_ADDR -- None
    , '' as CNSTRING_PROJ_LAND_LOC_ -- None
    , '' as ESTATE_LAND_NO -- None
    , '' as LAND_CLS_LAND_NO -- None
    , '' as CNSTRING_PROJ_LAND_NO -- None
    , '' as ESTATE_LAND_USE_RIGHT_BEGIN_DT -- None
    , '' as LAND_CLS_LAND_USE_RIGHT_BEGIN_DT -- None
    , '' as CNSTRING_PROJ_LAND_USE_RIGHT_BEGIN_DT -- None
    , '' as ESTATE_LAND_USAGE_CD -- None
    , '' as LAND_CLS_LAND_USAGE_CD -- None
    , '' as CNSTRING_PROJ_LAND_USAGE_CD -- None
    , '' as ESTATE_LAND_USE_RIGHT_CERT_NO -- None
    , '' as CNSTR_LAND_PLAN_LIC_NO -- None
    , '' as CNSTR_PROJ_PLAN_LIC_CERT_NO -- None
    , '' as CNSTR_LIC_BOOK_NO -- None
    , '' as PROJ_CNSTR_AL_PUT_IN_CAP_OCUP_TOTAL_CAST_CAP_PCT -- None
    , '' as ESTATE_LAND_USE_RIGHT_TYPE_CD -- None
    , '' as LAND_CLS_LAND_USE_RIGHT_TYPE_CD -- None
    , '' as IMMATRL_AST_EQTY_CERT_ISSUE_CORP_NAME -- None
    , '' as IMMATRL_AST_EQTY_VALID -- None
    , '' as IMMATRL_AST_EQTY_STABLE_FLAG -- None
    , '' as IMMATRL_AST_MAJOR_ORG_ESTIM_CERT_NO -- None
    , '' as IMMATRL_AST_NEED_PAY_FEE_FLAG -- None
    , '' as IMMATRL_AST_FEE_PAY_VOCH_NO -- None
    , '' as IMMATRL_AST_PLEDGE_HANDOV_DT -- None
    , '' as COLLATERAL_CURR_CD -- None
    , '' as COLLATERAL_MATU_DT -- None
    , '' as COLLATERAL_ISSUE_CORP_NAME -- None
    , '' as COLLATERAL_ISSUE_CORP_ORG_NO -- None
    , '' as COLLATERAL_BEGIN_DT -- None
    , '' as COLLATERAL_DNMNT -- None
    , '' as COLLATERAL_BIL_NO -- None
    , '' as COLLATERAL_BIL_TYPE_CD -- None
    , '' as COLLATERAL_HANDOV_DT -- None
    , '' as CNSTRING_PROJ_LAND_USE_RIGHT_CD -- None
    , '' as ESTATE_BELONGER_IDENTITY_CARD_NO -- None
    , '' as COLLATERAL_REG_ORG_NO -- None
    , '' as PLEDGE_REG_CUST_NO -- None
    , '' as GUAR_PROD -- None
    , '' as REG_PROOF_NO -- None
    , '' as COLLATERAL_REG_MODIF_NO -- None
    , '' as COLLATERAL_REG_STATUS_CD -- None
    , '' as MOV_PRO_SITU_ILUS -- None
    , '' as CSDC_NET_DEREGIS_REG_NO -- None
    , '' as CSDC_NET_CHG_REG_NO -- None
    , '' as PLEDGE_CSTD_TELR_NO -- None
    , T3.TRANUS as SETUP_TELR_NO -- 录入人
    , T3.UPDUSR as FINAL_MODIF_TELR_NO -- 最后修改人
    , '' as OBANK_REG_EFFECT_BEGIN_DT -- None
    , '' as OBANK_REG_EFFECT_TERMI_DT -- None
    , '' as CREATE_DT -- None
    , T3.PLDATE as PLEDGE_GUAR_DT -- 抵质押日期
    , T5.OUTDAT as PLEDGE_EX_WHS_DT -- 出库日期
    , T5.INDATE as PLEDGE_IN_WHS_DT -- 入库日期
    , T5.REGDAT as PLEDGE_OFBS_REG_DT -- 表外登记日期
    , '' as PLEDGE_VALUT_PERIOD -- None
    , T3.EVDATE as PLEDGE_FINAL_ESTIM_DT -- 评估日期
    , T3.ENDATE as PLEDGE_MATU_DT -- 到期日期
    , '' as HANDOV_DT -- None
    , '' as PICK_TO_READ_DT -- None
    , '' as RTN_DT -- None
    , '' as PLEDGE_DEREGIS_DT -- None
    , '' as PLEDGE_REG_DT -- None
    , '' as EXT_DT -- None
    , T3.CRDATE as SETUP_DT -- 创建日期
    , T3.GMT_CREATE as SETUP_TM_STAMP -- 创建时间
    , T3.UPDDAT as FINAL_MODIF_DT -- 修改日期
    , '' as FINAL_MODIF_TM_STAMP -- None
    , T3.BRCHNO as SETUP_ORG_NO -- 录入机构
    , T1.BUSIBR as OPEN_ORG_NO -- 营业机构
    , T1.TRANBR as TXN_ORG_NO -- 交易机构
    , T1.BRCHNO as ACCT_ORG_NO -- 账务机构
    , '' as COLLATERAL_BIL_ISSUE_ORG_NO -- None
    , '' as OFBS_GL_ORG_NO -- None
    , T1.CORPNO as BELONG_ORG_NO -- 法人代码
    , '' as RECNT_MODIF_ORG_NO -- None
    , T3.EVORGD as APPRS_COMPNY_NO -- 评估机构
    , ${process_date} as DATA_DT -- None
from
    ${ODS_NFCP_SCHEMA}.ODS_NFCP_COA_ACCT as T1 -- 抵质押物主表
    LEFT JOIN ${ODS_NFCP_SCHEMA}.ODS_NFCP_KWAW_PLEDGE_REGIST as T2 -- 押品信息登记表
    on T1.COLTNO = T2.PLEGNO
T2.PT_DT='${process_date}' 
AND T2.DELETED='0' 
    LEFT JOIN ${ODS_NFCP_SCHEMA}.ODS_NFCP_KWAW_PLEDGE as T3 -- 押品信息表
    on T1.COLTNO = T3.PLEGNO
T3.PT_DT='${process_date}' 
AND T3.DELETED='0' 
    LEFT JOIN ${ODS_NFCP_SCHEMA}.ODS_NFCP_COS_TRAN as T4 -- 抵质押物明细表
    on T1.COLTNO = T4.COLTNO
T4.PT_DT='${process_date}' 
AND T4.DELETED='0' 
    LEFT JOIN ${ODS_NFCP_SCHEMA}.ODS_NFCP_KWAW_PLEDGE_INOUT as T5 -- 押品出入库表
    on T1.COLTNO = T5.PLEGNO
T5.PT_DT='${process_date}' 
AND T5.DELETED='0' 
where T1.PT_DT='${process_date}' 
AND T1.DELETED='0'
;

-- 删除所有临时表