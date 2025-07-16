# 考勤系统API接口文档

## 接口概览
本文档列出了考勤系统当前可用的API接口，包括接口定义、参数说明和响应格式。

## 1. 获取概览统计数据

### 接口信息
- **接口名称**：获取概览统计数据
- **URL**：`/api/overview/<time_range>`
- **请求方法**：GET
- **返回格式**：JSON

### 路径参数
| 参数名 | 类型 | 必须 | 描述 | 可选值 |
|--------|------|------|------|--------|
| time_range | string | 是 | 统计时间范围 | today, week, month, quarter |

### 响应示例
```json
{
  "current": {
    "checkin": 86,
    "late": 5,
    "total_employees": 100,
    "late_rate": 5.81
  },
  "compare": {
    "checkin": {
      "trend": "up",
      "percent": 2.38
    },
    "late": {
      "trend": "down",
      "percent": 16.67
    },
    "total_employees": {
      "trend": "flat",
      "percent": 0.0
    },
    "late_rate": {
      "trend": "down",
      "percent": 15.44
    }
  },
  "prev_period": "yesterday"
}
```

### 响应字段说明
| 字段 | 类型 | 描述 |
|------|------|------|
| current | object | 当前周期统计数据 |
| current.checkin | integer | 签到数 |
| current.late | integer | 迟到数 |
| current.total_employees | integer | 签到人员总数 |
| current.late_rate | float | 迟到率(%) |
| compare | object | 与上一周期比较数据 |
| compare.*.trend | string | 趋势(up:上升, down:下降, flat:持平) |
| compare.*.percent | float | 变化百分比(%) |
| prev_period | string | 比较基准周期(yesterday, last_week, last_month, last_quarter) |

## 2. 其他接口（待补充）
以下接口存在于前端调用中，具体实现细节需进一步确认：

- **获取趋势数据**：`/api/trend` (GET)
- **获取分布数据**：`/api/distribution` (GET)
- **获取批次分布数据**：`/api/batch` (GET)
- **获取部门加班数据**：`/api/overtime` (GET)

*注：上述接口的详细定义将在后续补充完善。*