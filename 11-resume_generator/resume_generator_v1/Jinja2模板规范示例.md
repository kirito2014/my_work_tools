# 🧩 Jinja2 模板字段对照表

## 一、基本信息（BasicInfo）

| 字段名 | 说明 | Jinja2 表达式 | 示例/备注 |
|--------|------|---------------|-----------|
| 员工编号 | EmpNo | `{{ BasicInfo.EmpNo }}` | 00001 |
| 姓名 | Name | `{{ BasicInfo.Name }}` | 张三 |
| 工作年限 | WorkYears | `{{ BasicInfo.WorkYears }}` | 12年 |
| 毕业时间 | GraduationTime | `{{ BasicInfo.GraduationTime }}` | 2012年6月 |
| 毕业院校 | GraduationSchool | `{{ BasicInfo.GraduationSchool }}` | 广东雷某大学 |
| 专业 | Major | `{{ BasicInfo.Major }}` | 电子商务 |
| 最高学历 | HighestEducation | `{{ BasicInfo.HighestEducation }}` | 本科 |
| 学历对应学位 | Degree（条件判断） | `{% if BasicInfo.HighestEducation == "本科" %}学士{% elif BasicInfo.HighestEducation == "硕士" %}硕士{% else %}其他{% endif %}` | 学士 |
| 部门 | Department | `{{ BasicInfo.Department }}` | 金融业务六部 |
| 职位 | Title | `{{ BasicInfo.Title }}` | 主任工程师 |
| 个人简介 | PersonalProfile | `{{ BasicInfo.PersonalProfile }}` | 支持多行文本 |

---

## 二、工作经历（WorkExperience）

循环遍历工作经历：

```jinja2
{% for work in WorkExperience %}
- 公司：{{ work.CompanyName }}
  职位：{{ work.Position }}
  时间：{{ work.StartTime }} 至 {{ work.EndTime }}
  描述：{{ work.JobDescription }}
{% endfor %}
```

---

## 三、项目经验（ProjectExperience）

循环遍历项目经验：

```jinja2
{% for project in ProjectExperience %}
- 项目名称：{{ project.ProjectName }}
  角色：{{ project.ProjectRole }}
  时间：{{ project.StartTime }} 至 {{ project.EndTime }}
  描述：{{ project.JobDescription }}
{% endfor %}
```

---

## 四、工作能力（WorkAbility）

| 字段 | 说明 | Jinja2 表达式 |
|------|------|---------------|
| 业务能力 | BusinessAbility | `{{ WorkAbility.BusinessAbility }}` |
| 证书 | Certification | `{{ WorkAbility.Certification }}` |
| 培训经历 | Training | `{{ WorkAbility.Training }}` |
| 技能标签 | SkillTag | `{{ WorkAbility.SkillTag }}` |

---

## 五、附加信息（AdditionInfo）

| 字段 | 说明 | Jinja2 表达式 | 备注 |
|------|------|---------------|-------|
| 一级部门 | DepartmentLevel1 | `{{ AdditionInfo.DepartmentLevel1 }}` | 金融业务六部 |
| 二级部门 | DepartmentLevel2 | `{{ AdditionInfo.DepartmentLevel2 }}` | 金融业务6A部 |
| 岗位类别 | JobCategory | `{{ AdditionInfo.JobCategory }}` | 交付管理类 |
| 专业级别 | ProfessionalLevel | `{{ AdditionInfo.ProfessionalLevel }}` | 四级C |
| 司龄（取整） | CompanyYears | `{{ AdditionInfo.CompanyYears | float | round }}` | 10年 |
| 总工作年限（取整） | WorkYears | `{{ AdditionInfo.WorkYears | float | round }}` | 13年 |
| 性别 | Gender | `{{ AdditionInfo.Gender }}` | 男 |
| 年龄（取整） | Age | `{{ AdditionInfo.Age | float | round }}` | 37 |
| 政治面貌 | PoliticalStatus | `{{ AdditionInfo.PoliticalStatus }}` | 中国共产党党员 |
| 联系电话 | PhoneNumber | `{{ AdditionInfo.PhoneNumber }}` | 13800000000 |

---

## 六、常用 Jinja2 语法示例

### 1. 条件判断（学历 → 学位）

```jinja2
{% if BasicInfo.HighestEducation == "本科" %}
学位：学士
{% elif BasicInfo.HighestEducation == "硕士" %}
学位：硕士
{% else %}
学位：其他
{% endif %}
```

### 2. 字符串拼接

```jinja2
{{ BasicInfo.Name }}（工号：{{ BasicInfo.EmpNo }}）
```

### 3. 数字取整

```jinja2
司龄：{{ AdditionInfo.CompanyYears | float | round }} 年
```

### 4. 循环与序号

```jinja2
{% for project in ProjectExperience %}
{{ loop.index }}. {{ project.ProjectName }}
{% endfor %}
```

### 5.去除空格

```jinja2
{{ BasicInfo.PersonalProfile | trim }}
```

### 6. 截取字符串

```jinja2
{{ BasicInfo.PersonalProfile | truncate(20) }}
```

### 7.去掉字符串倒数1位

```jinja2
{{ BasicInfo.PersonalProfile | rstrip("-") }}
```

### 8.按照特殊字符进行换行

```jinja2
{{ BasicInfo.PersonalProfile | replace("\n", "<br>") }}
```
