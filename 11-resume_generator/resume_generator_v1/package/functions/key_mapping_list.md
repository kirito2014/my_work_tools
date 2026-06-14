# 简历JSON数据键值映射清单

本文档提供了简历JSON数据中长键值与短键值的对应关系，用于在渲染简历模板时进行键值映射转换。

## 顶级结构键映射

| 长键值 | 短键值 | 说明 |
|-------|-------|------|
| BasicInfo | bi | 基本信息 |
| WorkExperience | we | 工作经历 |
| ProjectExperience | pe | 项目经验 |
| WorkAbility | wa | 工作能力 |
| AdditionInfo | ai | 附加信息 |
| SpecialInfo | si | 特殊信息 |

## 1. 基本信息 (BasicInfo -> bi) 内部字段

| 长键值 | 短键值 | 说明 |
|-------|-------|------|
| EmpNo | emp | 员工编号 |
| Name | name | 姓名 |
| WorkYears | work | 工作年限 |
| GraduationTime | grad | 毕业时间 |
| GraduationSchool | school | 毕业学校 |
| Major | major | 专业 |
| HighestEducation | edu | 最高学历 |
| Department | dept | 部门 |
| Title | title | 职称/职位 |
| PersonalProfile | profile | 个人简介 |

## 2. 工作经历 (WorkExperience -> we) 内部字段

| 长键值 | 短键值 | 说明 |
|-------|-------|------|
| StartTime | start | 开始时间 |
| EndTime | end | 结束时间 |
| CompanyName | comp | 公司名称 |
| Position | pos | 职位 |
| JobDescription | desc | 工作描述 |

## 3. 项目经验 (ProjectExperience -> pe) 内部字段

| 长键值 | 短键值 | 说明 |
|-------|-------|------|
| StartTime | start | 开始时间 |
| EndTime | end | 结束时间 |
| ProjectName | proj | 项目名称 |
| ProjectRole | role | 项目角色 |
| JobDescription | desc | 工作描述 |

## 4. 工作能力 (WorkAbility -> wa) 内部字段

| 长键值 | 短键值 | 说明 |
|-------|-------|------|
| BusinessAbility | skill | 业务能力 |
| Certification | cert | 证书 |
| Training | train | 培训经历 |
| SkillTag | tags | 技能标签 |

## 5. 附加信息 (AdditionInfo -> ai) 内部字段

| 长键值 | 短键值 | 说明 |
|-------|-------|------|
| EmpNo | emp | 员工编号 |
| Name | name | 姓名 |
| DepartmentLevel1 | dept1 | 一级部门 |
| DepartmentLevel2 | dept2 | 二级部门 |
| Position | pos | 职位 |
| JobCategory | job | 岗位类别 |
| ProfessionalLevel | level | 专业等级 |
| EmploymentStatus | status | 在职状态 |
| EntryDate | entry | 入职日期 |
| FirstEntryDate | first | 首次入职日期 |
| CompanyYears | years | 司龄 |
| WorkYears | work | 工作年限 |
| PaymentCompany | pay | 薪资发放公司 |
| BaseLocation | loc | 工作地点 |
| Gender | gender | 性别 |
| BirthDate | birth | 出生日期 |
| Age | age | 年龄 |
| PoliticalStatus | pol | 政治面貌 |
| IDNumber | id | 身份证号 |
| PhoneNumber | phone | 电话号码 |
| NativePlace | plc | 籍贯 |
| GraduationTime | grad | 毕业时间 |
| GraduationSchool | school | 毕业学校 |
| HighestEducation | edu | 最高学历 |
| Major | major | 专业 |
| ContractLegalPerson | legal | 合同法人 |

## 6. 特殊信息 (SpecialInfo -> si) 内部字段

| 长键值 | 短键值 | 说明 |
|-------|-------|------|
| Degree | degree | 学位 |
| StartWorkDate | workdt | 参加工作日期 |

## 使用说明

在渲染简历模板时，可以使用 `key_map_convert.py` 中的函数将原始简历数据转换为使用短键值的数据。这样做的好处是：

1. 减少数据传输量
2. 简化模板代码中的键名引用
3. 保持数据结构的一致性
4. 所有短键值长度均小于5个字符，更简洁高效

## 示例

```python
# 导入转换函数
from key_map_convert import convert_resume_data

# 原始简历数据（使用长键值）
original_data = {
    "张三": {
        "BasicInfo": {
            "EmpNo": "12345",
            "Name": "张三",
            "WorkYears": "5年"
        },
        "WorkExperience": [
            {
                "StartTime": "2020/01",
                "EndTime": "至今",
                "CompanyName": "示例公司"
            }
        ]
    }
}

# 转换后的数据（使用短键值）
converted_data = convert_resume_data(original_data)

# 转换后的结构示例
# {
#     "张三": {
#         "bi": {
#             "emp": "12345",
#             "name": "张三",
#             "work": "5年"
#         },
#         "we": [
#             {
#                 "start": "2020/01",
#                 "end": "至今",
#                 "comp": "示例公司"
#             }
#         ]
#     }
# }
```

## 注意事项

1. 所有短键值均严格控制在5个字符以内，确保最大简洁性
2. 顶级结构键（如BasicInfo）被统一缩写为2-3个字符的短键
3. 字段键名保留了足够的语义信息，便于理解和使用
4. 转换过程是递归的，会处理所有层级的数据结构
5. 如果遇到映射表中未定义的键，将保持其原始名称不变
6. 此映射表仅包含当前已知的键值对应关系，如有新增字段需要及时更新
