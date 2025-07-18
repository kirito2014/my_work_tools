drop table ods_sunline.ods_sunline_psn_binfo;

CREATE TABLE ods_sunline.ods_sunline_psn_binfo (
    ID INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    EMPLY_NAME VARCHAR(255) COMMENT '员工姓名',
    BLG_DEPT VARCHAR(255) COMMENT '归属部门',
    CUST_STL_IDNT VARCHAR(255) COMMENT '客户结算标识',
    NOT_STL_RSN_CLS VARCHAR(255) COMMENT '不结算原因分类',
    CORP_LVL VARCHAR(255) COMMENT '公司级别',
    ETRC_DT VARCHAR(255) COMMENT '入场日期',
    WITHDRAWAL_DT VARCHAR(255) COMMENT '撤场日期',
    BLG_PROJ VARCHAR(255) COMMENT '归属项目',
    CUST_FUNC VARCHAR(255) COMMENT '客户职能',
    CUST_PRINC VARCHAR(255) COMMENT '客户负责人',
    CUST_TM_LVL VARCHAR(255) COMMENT '客户定级',
    CUST_UNPRC DECIMAL(28,2) COMMENT '客户单价',
    IMPL_FLAG VARCHAR(255) COMMENT '在场标志',
    
    CREATE_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    UPDATE_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    INDEX idx_emply_name (EMPLY_NAME),
    INDEX idx_blg_dept (BLG_DEPT),
    INDEX idx_etrc_dt (ETRC_DT),
    INDEX idx_blg_proj (BLG_PROJ)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='长亮人员基本信息表';
use ods_sunline

select
	*
from
	ods_sunline.ods_sunline_psn_binfo
where
	IMPL_FLAG = 'Y';

LOAD DATA INFILE '/var/lib/mysql-files/ods_sunline_psn_binfo.csv'
INTO TABLE ods_sunline.ods_sunline_psn_binfo
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(EMPLY_NAME, BLG_DEPT, CUST_STL_IDNT, NOT_STL_RSN_CLS, 
 CORP_LVL, ETRC_DT, WITHDRAWAL_DT, BLG_PROJ, 
 CUST_FUNC, CUST_PRINC, CUST_TM_LVL, CUST_UNPRC,IMPL_FLAG);

 

drop table ods_sunline.pub_cd_map;
CREATE TABLE ods_sunline.pub_cd_map (
    id INT(10) AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    cd_val_en_name VARCHAR(100) NOT NULL COMMENT '码值英文名称',
    cd_val_cn_name VARCHAR(100) NOT NULL COMMENT '码值中文名称',
    cd_val VARCHAR(50) NOT NULL COMMENT '码值',
    cd_val_mean VARCHAR(255) COMMENT '码值含义',
    enabled_flag VARCHAR(50) COMMENT '启用标志',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建日期',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新日期',
    
    UNIQUE KEY uk_code_value (cd_val_en_name,cd_val),
    INDEX idx_cd_val_en_name (cd_val_en_name),
    INDEX idx_cd_val_cn_name (cd_val_cn_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='码值信息映射表';


-- 将文件放在MySQL可访问目录，如/var/lib/mysql-files/
LOAD DATA INFILE '/var/lib/mysql-files/pub_cd_map.csv'
INTO TABLE ods_sunline.pub_cd_map
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(cd_val_en_name, cd_val_cn_name, cd_val, cd_val_mean, enabled_flag);


select * from ods_sunline.pub_cd_map 
;

-- INSERT INTO ods_sunline.pub_cd_map 
-- (cd_val_en_name, cd_val_cn_name, cd_val, cd_val_mean, enabled_flag)
-- VALUES
-- ('GENDER_CD', '性别代码', '1', '男性', 'Y'),
-- ('GENDER_CD', '性别代码', '2', '女性', 'Y'),
-- ('GENDER_CD', '性别代码', '0', '其他', 'Y'),
-- ('STATUS_CD', '状态代码', '0', '禁用', 'Y'),
-- ('STATUS_CD', '状态代码', '1', '启用', 'Y');


CREATE TABLE ods_sunline.in_bank_psn_atten_dtl_in_bank_exp (
    SER_NO INT(10) AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    ATTEN_DT VARCHAR(20) COMMENT '考勤日期',
    USER_NAME VARCHAR(50) COMMENT '用户名',
    EMPLY_NAME VARCHAR(50) COMMENT '员工姓名',
    AFLT_SUPR VARCHAR(100) COMMENT '所属供应商',
    AFLT_DEPT VARCHAR(100) COMMENT '所属部门',
    PLAT_SIGNIN_TM VARCHAR(20) COMMENT '平台签到时间',
    PLAT_SIGNOUT_TM VARCHAR(20) COMMENT '平台签退时间',
    EARLIEST_SINGIN_TM VARCHAR(20) COMMENT '最早签到时间',
    LATST_SIGNOUT_TM VARCHAR(20) COMMENT '最晚签退时间',
    STD_DURAN VARCHAR(20) COMMENT '标准时长',
    STD_ATTEN_DT VARCHAR(20) COMMENT '标准考勤日期',
    
    CREATE_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    UPDATE_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    INDEX idx_atten_dt (ATTEN_DT),
    INDEX idx_emply_name (EMPLY_NAME),
    INDEX idx_user_name (USER_NAME),
    INDEX idx_aflt_dept (AFLT_DEPT)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='行内人员考勤明细导出表';

 
LOAD DATA INFILE '/var/lib/mysql-files/in_bank_psn_atten_dtl_in_bank_exp.csv'
INTO TABLE ods_sunline.in_bank_psn_atten_dtl_in_bank_exp
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(ATTEN_DT, USER_NAME, EMPLY_NAME, AFLT_SUPR, AFLT_DEPT, 
 PLAT_SIGNIN_TM, PLAT_SIGNOUT_TM, EARLIEST_SINGIN_TM, 
 LATST_SIGNOUT_TM, STD_DURAN, STD_ATTEN_DT);