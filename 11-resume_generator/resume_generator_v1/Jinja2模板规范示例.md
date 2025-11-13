# 🧩 Jinja2 模板字段对照表

## 一、基本信息（BasicInfo / bi）

| 字段名 | 短键值 | 说明 | Jinja2 表达式（长键） | Jinja2 表达式（短键） | 示例/备注 |
|--------|--------|------|-----------------------|-----------------------|-----------|
| EmpNo | emp | 员工编号 | `{{ BasicInfo.EmpNo }}` | `{{ bi.emp }}` | 00001 |
| Name | name | 姓名 | `{{ BasicInfo.Name }}` | `{{ bi.name }}` | 张三 |
| WorkYears | work | 工作年限 | `{{ BasicInfo.WorkYears }}` | `{{ bi.work }}` | 12年 |
| GraduationTime | grad | 毕业时间 | `{{ BasicInfo.GraduationTime }}` | `{{ bi.grad }}` | 2012年6月 |
| GraduationSchool | school | 毕业院校 | `{{ BasicInfo.GraduationSchool }}` | `{{ bi.school }}` | 广东雷某大学 |
| Major | major | 专业 | `{{ BasicInfo.Major }}` | `{{ bi.major }}` | 电子商务 |
| HighestEducation | edu | 最高学历 | `{{ BasicInfo.HighestEducation }}` | `{{ bi.edu }}` | 本科 |
| Department | dept | 部门 | `{{ BasicInfo.Department }}` | `{{ bi.dept }}` | 金融业务六部 |
| Title | title | 职位 | `{{ BasicInfo.Title }}` | `{{ bi.title }}` | 主任工程师 |
| PersonalProfile | profile | 个人简介 | `{{ BasicInfo.PersonalProfile }}` | `{{ bi.profile }}` | 支持多行文本 |

---

## 二、工作经历（WorkExperience / we）

| 字段名 | 短键值 | 说明 | Jinja2 表达式（长键） | Jinja2 表达式（短键） |
|--------|--------|------|-----------------------|-----------------------|
| StartTime | start | 开始时间 | `{{ work.StartTime }}` | `{{ work.start }}` |
| EndTime | end | 结束时间 | `{{ work.EndTime }}` | `{{ work.end }}` |
| CompanyName | comp | 公司名称 | `{{ work.CompanyName }}` | `{{ work.comp }}` |
| Position | pos | 职位 | `{{ work.Position }}` | `{{ work.pos }}` |
| JobDescription | desc | 工作描述 | `{{ work.JobDescription }}` | `{{ work.desc }}` |

循环遍历工作经历（长键）：

```jinja2
{% for work in WorkExperience %}
- 公司：{{ work.CompanyName }}
  职位：{{ work.Position }}
  时间：{{ work.StartTime }} 至 {{ work.EndTime }}
  描述：{{ work.JobDescription }}
{% endfor %}
```

循环遍历工作经历（短键）：

```jinja2
{% for work in we %}
- 公司：{{ work.comp }}
  职位：{{ work.pos }}
  时间：{{ work.start }} 至 {{ work.end }}
  描述：{{ work.desc }}
{% endfor %}
```

---

## 三、项目经验（ProjectExperience / pe）

| 字段名 | 短键值 | 说明 | Jinja2 表达式（长键） | Jinja2 表达式（短键） |
|--------|--------|------|-----------------------|-----------------------|
| ProjectName | proj | 项目名称 | `{{ project.ProjectName }}` | `{{ project.proj }}` |
| ProjectRole | role | 项目角色 | `{{ project.ProjectRole }}` | `{{ project.role }}` |
| StartTime | start | 开始时间 | `{{ project.StartTime }}` | `{{ project.start }}` |
| EndTime | end | 结束时间 | `{{ project.EndTime }}` | `{{ project.end }}` |
| JobDescription | desc | 工作描述 | `{{ project.JobDescription }}` | `{{ project.desc }}` |

循环遍历项目经验（长键）：

```jinja2
{% for project in ProjectExperience %}
- 项目名称：{{ project.ProjectName }}
  角色：{{ project.ProjectRole }}
  时间：{{ project.StartTime }} 至 {{ project.EndTime }}
  描述：{{ project.JobDescription }}
{% endfor %}
```

循环遍历项目经验（短键）：

```jinja2
{% for prj in pe %}
- 项目名称：{{ prj.proj }}
  角色：{{ prj.role }}
  时间：{{ prj.start }} 至 {{ prj.end }}
  描述：{{ prj.desc }}
{% endfor %}
```

---

## 四、工作能力（WorkAbility / wa）

| 字段名 | 短键值 | 说明 | Jinja2 表达式（长键） | Jinja2 表达式（短键） |
|--------|--------|------|-----------------------|-----------------------|
| BusinessAbility | skill | 业务能力 | `{{ WorkAbility.BusinessAbility }}` | `{{ wa.skill }}` |
| Certification | cert | 证书 | `{{ WorkAbility.Certification }}` | `{{ wa.cert }}` |
| Training | train | 培训经历 | `{{ WorkAbility.Training }}` | `{{ wa.train }}` |
| SkillTag | tags | 技能标签 | `{{ WorkAbility.SkillTag }}` | `{{ wa.tags }}` |

---

## 五、附加信息（AdditionInfo / ai）

| 字段名 | 短键值 | 说明 | Jinja2 表达式（长键） | Jinja2 表达式（短键） | 备注 |
|--------|--------|------|-----------------------|-----------------------|-------|
| DepartmentLevel1 | dept1 | 一级部门 | `{{ AdditionInfo.DepartmentLevel1 }}` | `{{ ai.dept1 }}` | 金融业务六部 |
| DepartmentLevel2 | dept2 | 二级部门 | `{{ AdditionInfo.DepartmentLevel2 }}` | `{{ ai.dept2 }}` | 金融业务6A部 |
| JobCategory | job | 岗位类别 | `{{ AdditionInfo.JobCategory }}` | `{{ ai.job }}` | 交付管理类 |
| ProfessionalLevel | level | 专业级别 | `{{ AdditionInfo.ProfessionalLevel }}` | `{{ ai.level }}` | 四级C |
| CompanyYears | years | 司龄（取整） | `{{ AdditionInfo.CompanyYears | float | round }}` | `{{ ai.years | float | round }}` | 10年 |
| WorkYears | work | 总工作年限（取整） | `{{ AdditionInfo.WorkYears | float | round }}` | `{{ ai.work | float | round }}` | 13年 |
| Gender | gender | 性别 | `{{ AdditionInfo.Gender }}` | `{{ ai.gender }}` | 男 |
| Age | age | 年龄（取整） | `{{ AdditionInfo.Age | float | round }}` | `{{ ai.age | float | round }}` | 37 |
| PoliticalStatus | pol | 政治面貌 | `{{ AdditionInfo.PoliticalStatus }}` | `{{ ai.pol }}` | 中国共产党党员 |
| PhoneNumber | phone | 联系电话 | `{{ AdditionInfo.PhoneNumber }}` | `{{ ai.phone }}` | 13800000000 |
| BirthDate | birth | 出生日期 | `{{ AdditionInfo.BirthDate }}` | `{{ ai.birth }}` | 1987-01-01 |
| IDNumber | id | 身份证号 | `{{ AdditionInfo.IDNumber }}` | `{{ ai.id }}` | - |
| PaymentCompany | pay | 薪资发放公司 | `{{ AdditionInfo.PaymentCompany }}` | `{{ ai.pay }}` | - |
| BaseLocation | loc | 工作地点 | `{{ AdditionInfo.BaseLocation }}` | `{{ ai.loc }}` | - |
| ContractLegalPerson | legal | 合同法人 | `{{ AdditionInfo.ContractLegalPerson }}` | `{{ ai.legal }}` | - |
| GraduationTime | grad | 毕业时间 | `{{ AdditionInfo.GraduationTime }}` | `{{ ai.grad }}` | 2012年6月 |
| GraduationSchool | school | 毕业院校 | `{{ AdditionInfo.GraduationSchool }}` | `{{ ai.school }}` | 广东雷某大学 |
| HighestEducation | edu | 最高学历 | `{{ AdditionInfo.HighestEducation }}` | `{{ ai.edu }}` | 本科 |
| Major | major | 专业 | `{{ AdditionInfo.Major }}` | `{{ ai.major }}` | 电子商务 |
| EntryDate | entry | 入职日期 | `{{ AdditionInfo.EntryDate }}` | `{{ ai.entry }}` | - |
| FirstEntryDate | first | 首次入职日期 | `{{ AdditionInfo.FirstEntryDate }}` | `{{ ai.first }}` | - |
| EmploymentStatus | status | 在职状态 | `{{ AdditionInfo.EmploymentStatus }}` | `{{ ai.status }}` | - |
| Position | pos | 职位 | `{{ AdditionInfo.Position }}` | `{{ ai.pos }}` | - |

---

## 六、特殊信息（SpecialInfo / si）

| 字段名 | 短键值 | 说明 | Jinja2 表达式（长键） | Jinja2 表达式（短键） | 示例/备注 |
|--------|--------|------|-----------------------|-----------------------|-----------|
| Degree | degree | 学位 | `{{ SpecialInfo.Degree }}` | `{{ si.degree }}` | 学士 |
| StartWorkDate | workdt | 参加工作日期 | `{{ SpecialInfo.StartWorkDate }}` | `{{ si.workdt }}` | 2012-07-01 |

---

## 七、常用 Jinja2 语法示例

### 1. 条件判断（学历 → 学位）

使用长键：
```jinja2
{% if BasicInfo.HighestEducation == "本科" %}
学位：学士
{% elif BasicInfo.HighestEducation == "硕士" %}
学位：硕士
{% else %}
学位：其他
{% endif %}
```

使用短键：
```jinja2
{% if bi.edu == "本科" %}
学位：学士
{% elif bi.edu == "硕士" %}
学位：硕士
{% else %}
学位：其他
{% endif %}
```

### 1.1 直接使用学位字段

使用长键：
```jinja2
学位：{{ SpecialInfo.Degree }}
```

使用短键：
```jinja2
学位：{{ si.degree }}
```

### 2. 字符串拼接

使用长键：
```jinja2
{{ BasicInfo.Name }}（工号：{{ BasicInfo.EmpNo }}）
```

使用短键：
```jinja2
{{ bi.name }}（工号：{{ bi.emp }}）
```

### 3. 数字取整

使用长键：
```jinja2
司龄：{{ AdditionInfo.CompanyYears | float | round }} 年
```

使用短键：
```jinja2
司龄：{{ ai.years | float | round }} 年
```

### 4. 循环与序号

使用长键：
```jinja2
{% for prj in ProjectExperience %}
{{ loop.index }}. {{ prj.proj }}
{% endfor %}
```

使用短键：
```jinja2
{% for prj in pe %}
{{ loop.index }}. {{ prj.proj }}
{% endfor %}
```

### 5.去除空格

使用长键：
```jinja2
{{ BasicInfo.PersonalProfile | trim }}
```

使用短键：
```jinja2
{{ bi.profile | trim }}
```

### 6. 截取字符串

使用长键：
```jinja2
{{ BasicInfo.PersonalProfile | truncate(20) }}
```

使用短键：
```jinja2
{{ bi.profile | truncate(20) }}
```

### 7.去掉字符串倒数1位

使用长键：
```jinja2
{{ BasicInfo.PersonalProfile | rstrip("-") }}
```

使用短键：
```jinja2
{{ bi.profile | rstrip("-") }}
```

### 8.按照特殊字符进行换行

使用长键：
```jinja2
{{ BasicInfo.PersonalProfile | replace("\n", "<br>") }}
```

使用短键：
```jinja2
{{ bi.profile | replace("\n", "<br>") }}
```

- 示例一：根据空格替换为换行（长键）
```jinja2
{{ BasicInfo.PersonalProfile | replace(" ", "<br>") }}
```

- 示例一：根据空格替换为换行（短键）
```jinja2
{{ bi.profile | replace(" ", "<br>") }}
### 9.单表格中填写循环项目经历

使用长键：
```jinja2
{% for exp in WorkExperience %}{{ exp.StartTime|default('') }}, {{ exp.EndTime|default('') }}, {{ exp.CompanyName|default('') }}{% if not loop.last %}; 
{% endif %}{% endfor %}
```

使用短键：
```jinja2
{% for exp in we %}{{ exp.start|default('') }}, {{ exp.end|default('') }}, {{ exp.comp|default('') }}{% if not loop.last %}; 
{% endif %}{% endfor %}
```