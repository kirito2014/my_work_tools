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

### 1. 入口脚本

#### 1.1 resume_gui.py
- **功能**: 简历生成器图形用户界面
- **使用方法**: 直接运行 `python resume_gui.py`
- **核心功能**:
  - 简历文件解析入库: 选择Word文档解析为JSON格式
  - 简历生成: 全量或按名单生成指定银行的简历文档
  - 包含文件选择、解析进度显示、人员名单管理、银行选择和日志输出功能

#### 1.2 gen_resume.py
- **功能**: 命令行简历生成入口
- **使用方法**: `python gen_resume.py <resume_file>`
- **功能说明**: 接收简历文件路径，调用resume_generator模块生成简历

#### 1.3 render_resumes.py
- **功能**: 批量渲染简历
- **使用方法**: 可作为模块调用
- **核心功能**:
  - 读取配置(包括JSON文件、模板文件、输出目录等)
  - 设置渲染参数并处理简历数据
  - 支持渲染指定人员或全部人员简历

### 2. 核心功能脚本

#### 2.1 doc_2_json.py
- **功能**: 简历文档解析脚本
- **使用方法**: 可作为模块调用或通过GUI使用
- **核心功能**:
  - 通过关键词定位表格模块提取简历信息(基本情况、工作经历、项目经历、能力与资质)
  - 支持姓名提取、工号解析、模板格式转换
  - 支持.doc和.docx格式文件
  - 将解析结果保存为JSON格式到output/original_json和output/modify_json目录

#### 2.2 resume_generator.py
- **功能**: 简历生成核心脚本
- **使用方法**: 作为模块被其他脚本调用
- **核心功能**:
  - 导入pandas、numpy和docxtpl库
  - 使用DocxTemplate渲染包含个人信息的简历模板
  - 保存为Word文档

#### 2.3 excel_2_info_json.py
- **功能**: 将Excel员工信息转换为JSON格式
- **使用方法**: `python package\functions\excel_2_info_json.py [Excel文件路径]`
- **输出**: 在output/info_json目录下生成工号_姓名_人员信息.json文件

#### 2.4 update_specific_jsons.py
- **功能**: 更新特定人员的JSON文件
- **使用方法**: `python package\functions\update_specific_jsons.py 更新选项 员工工号 [--excel Excel文件路径]`
- **参数说明**:
  - 更新选项：1-只更新简历json，2-只更新信息json，3-两者都更新
  - 员工工号：可以是ALL（全部），或列表格式如['07003','02794']，或逗号分隔如07003,02794

#### 2.5 check_resume_valid.py
- **功能**: 简历数据有效性校验工具
- **使用方法**: 
  - `python check_resume_valid.py --json <JSON文件路径>`
  - `python check_resume_valid.py --excel <Excel文件路径>`
- **核心校验功能**:
  - 工作经历校验: 日期格式、时间顺序、时间段重叠检测
  - 项目经历校验: 日期冲突、跨公司项目检测
  - 学历信息校验: 各学历层级的完整性检查
  - 工作年限校验: 毕业时间与工作时间的逻辑一致性
- **输出**: 校验结果保存至output/checkExcel/check_result.xlsx

#### 2.6 doc_converter.py
- **功能**: 文档格式转换工具
- **使用方法**: `python doc_converter.py <doc文件路径> [输出目录]`
- **核心功能**: 将.doc格式文件转换为.docx格式
- **依赖**: 需要安装comtypes库和Microsoft Word

### 3. 工具脚本

#### 3.1 file_helper.py
- **功能**: 文件操作辅助工具
- **核心功能**:
  - create_dirs: 创建目录，支持权限设置
  - read_file: 读取文件内容，支持自定义编码
  - write_file: 写入文件内容，支持自动创建目录

#### 3.2 excel_helper.py (工具模块)
- **功能**: Excel操作辅助工具
- **说明**: 提供Excel文件读写相关的辅助功能

## 主要功能流程

### 简历解析流程
1. 通过resume_gui.py或直接调用doc_2_json.py选择简历文档
2. doc_converter.py将.doc文件转换为.docx格式(如需要)
3. doc_2_json.py解析文档提取关键信息
4. 解析结果保存为JSON格式到相应目录

### 简历生成流程
1. 准备简历JSON数据(通过解析或Excel导入)
2. 通过resume_gui.py或gen_resume.py调用resume_generator.py
3. 选择目标银行模板
4. 渲染生成Word格式简历
5. 保存到指定输出目录

### 数据校验流程
1. 通过check_resume_valid.py提供JSON文件或Excel文件
2. 执行各项校验(工作经历、项目经历、学历、工作年限等)
3. 生成校验报告Excel文件

### 数据更新流程
1. 使用update_specific_jsons.py选择更新选项和目标员工
2. 根据选项更新简历JSON、信息JSON或两者同时更新
3. 生成更新日志报告

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