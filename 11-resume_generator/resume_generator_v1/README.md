# 简历生成器项目说明

## 项目概述
这是一个用于生成和管理员工简历的工具，支持从Excel文件导入员工信息并生成标准格式的JSON文件，用于简历生成。

## 核心功能
1. 从Excel导入员工信息并转换为JSON格式
2. 支持更新特定人员的简历和信息JSON文件
3. 生成标准化的人员信息JSON文件

## 键名中英文对照表

| 中文名称 | 英文键名 | 说明 |
|---------|---------|------|
| 员工编号 | EmpNo | 员工工号，格式化为5位数 |
| 工作名 | Name | 员工姓名，不包含数字 |
| 管理关系一级部门 | DepartmentLevel1 | 员工所属一级部门 |
| 管理关系二级部门 | DepartmentLevel2 | 员工所属二级部门 |
| 岗位 | Position | 员工岗位名称 |
| 专业级别 | ProfessionalLevel | 员工专业技术级别 |
| 公司邮箱 | CompanyEmail | 员工公司电子邮箱 |
| 入职日期 | EntryDate | 员工入职日期 |
| 初次入职日期 | FirstEntryDate | 员工首次入职公司的日期 |
| 司龄 | CompanyYears | 员工在公司的工作年限 |
| 工龄 | WorkYears | 员工总工作年限 |
| base地 | BaseLocation | 员工工作地点 |
| 性别 | Gender | 员工性别 |
| 出生日期 | BirthDate | 员工出生日期 |
| 年龄 | Age | 员工年龄 |
| 政治面貌 | PoliticalStatus | 员工政治面貌 |
| 证件号码 | IDNumber | 员工身份证号或其他证件号码 |
| 手机号码 | PhoneNumber | 员工手机号码 |
| 毕业日期 | GraduationTime | 员工毕业日期 |
| 毕业院校 | GraduationSchool | 员工毕业院校名称 |
| 学历 | HighestEducation | 员工最高学历 |
| 专业 | Major | 员工所学专业 |
| 合同签订法人 | ContractLegalPerson | 员工劳动合同签订法人 |

## 主要脚本说明

### excel_2_info_json.py
- 功能：将Excel员工信息转换为JSON格式
- 使用方法：`python package\functions\excel_2_info_json.py [Excel文件路径]`
- 输出：在output/info_json目录下生成工号_姓名_人员信息.json文件

### update_specific_jsons.py
- 功能：更新特定人员的JSON文件
- 使用方法：`python package\functions\update_specific_jsons.py 更新选项 员工工号 [--excel Excel文件路径]`
- 参数说明：
  - 更新选项：1-只更新简历json，2-只更新信息json，3-两者都更新
  - 员工工号：可以是ALL（全部），或列表格式如['07003','02794']，或逗号分隔如07003,02794

## 目录结构
- input：存放输入文件，如Excel员工名单和原始简历文件
- output：存放输出文件
  - info_json：存放人员信息JSON文件
  - modify_json：存放可修改的简历JSON文件
  - output_resumes：存放生成的简历文件
- package：核心功能包
  - functions：各种功能脚本
  - utils：工具类和辅助函数
- template：模板文件目录

## 使用示例

### 1. 生成人员信息JSON
```bash
python package\functions\excel_2_info_json.py input\技术人员名单.xlsx
```

### 2. 更新特定人员的JSON文件
```bash
# 更新特定员工的简历JSON
python package\functions\update_specific_jsons.py 1 ['07003','02794']

# 更新所有员工的信息JSON
python package\functions\update_specific_jsons.py 2 ALL

# 更新特定员工的所有JSON文件
python package\functions\update_specific_jsons.py 3 07003,02794
```

## 注意事项
1. 确保Excel文件格式正确，表头与对照表中的中文名称一致
2. 工号会自动格式化为5位数，不足5位的前面补零
3. 姓名中的数字会被自动移除，确保与简历信息保持一致