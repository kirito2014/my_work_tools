# 导入 pandas 以便后续数据处理
import pandas as pd
import excel_helper
# 假设你已经成功获取了所有数据并存储在 excel.data 中
# excel.data 是一个字典，包含每个 sheet 的数据
#sdm_excel = excel_helper.Xlsx(file_path)
# 假设 "数据来源" 是我们感兴趣的 sheet
#
#print(sheet_data)
# 将数据转换为 DataFrame，以便于操作
def main():
    file_path = r"D:\github\11-resume_generator\template\人员简历汇总_20241103.xlsx"
    sdm_excel = excel_helper.Xlsx(file_path)
    sheet_data = sdm_excel.data["数据来源"]
    df = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])  # 忽略表头行
    processed_data = process_data(df)
    print(df)
    # 创建一个字典用于存储结果
    for name, info in processed_data.items():
        print(f"姓名: {name}")
        print("基本情况:", info['基本情况'])
        print("工作经历:", info['工作经历'])
        print("项目经历:", info['项目经历'])
        print("-" * 20)

def process_data(data):
    # 创建一个空的字典来存储每个人的信息
    result = {}
    
    for index, row in data.iterrows():
        name = row['基本情况-姓名']
        
        if name not in result:
            result[name] = {
                '基本情况': {
                    '工作年限': row['基本情况-工作年限'],
                    '最高学历': row['基本情况-最高学历'],
                    '毕业日期': row['基本情况-最高学历-毕业日期'],
                    '毕业学校': row['基本情况-最高学历-毕业学校'],
                    '专业': row['基本情况-最高学历-专业'],
                    '职称': row['基本情况-职称'],
                    '个人简介': row['基本情况-个人简介'],
                },
                '工作经历': [],
                '项目经历': [],
            }
        
        # 提取工作经历
        work_experience = {
            '开始时间': row['工作经历-开始时间'],
            '结束时间': row['工作经历-结束时间'],
            '公司名称': row['工作经历-公司名称'],
            '担任职务': row['工作经历-担任职务'],
            '工作职责说明': row['工作经历-工作职责说明'],
        }
        
        # 添加工作经历到对应的列表
        result[name]['工作经历'].append(work_experience)

        # 提取项目经历
        for i in range(1, 5):  # 假设每个项目经历有4组
            if pd.notna(row[f'项目经历-开始时间-{i}']):
                project_experience = {
                    '开始时间': row[f'项目经历-开始时间-{i}'],
                    '结束时间': row[f'项目经历-结束时间-{i}'],
                    '项目名称': row[f'项目经历-项目名称-{i}'],
                    '项目角色': row[f'项目经历-项目角色-{i}'],
                    '项目职责说明': row[f'项目经历-项目职责说明-{i}'],
                }
                result[name]['项目经历'].append(project_experience)

    return result



if __name__ == "__main__":
    main()