
import pandas as pd
import excel_helper
import logging
#sdm_excel = excel_helper.Xlsx(file_path)
# 假设 "数据来源" 是我们感兴趣的 sheet
#
#print(sheet_data)
# 将数据转换为 DataFrame，以便于操作


def data_to_frame():
# 定义列标题列表
    headers = [
        "填写人", "所在部门", "填写时间", "用户类型", 
        "基本情况-姓名", "基本情况-工作年限", "基本情况-最高学历", 
        "基本情况-最高学历-毕业日期", "基本情况-最高学历-毕业学校", 
        "基本情况-最高学历-专业", "基本情况-第一学历-毕业日期", 
        "基本情况-第一学历-毕业学校", "基本情况-第一学历-专业", 
        "基本情况-部门", "基本情况-职称", "基本情况-个人简介", 
        "工作经历-开始时间", "工作经历-结束时间", 
        "工作经历-公司名称", "工作经历-担任职务", 
        "工作经历-工作职责说明", "项目经历-开始时间", 
        "项目经历-结束时间", "项目经历-项目名称", 
        "项目经历-项目角色", "项目经历-项目职责说明", 
        "项目经历-开始时间", "项目经历-结束时间", 
        "项目经历-项目名称", "项目经历-项目角色", 
        "项目经历-项目职责说明", "项目经历-开始时间", 
        "项目经历-结束时间", "项目经历-项目名称", 
        "项目经历-项目角色", "项目经历-项目职责说明", 
        "项目经历-开始时间", "项目经历-结束时间", 
        "项目经历-项目名称", "项目经历-项目角色", 
        "项目经历-项目职责说明", "能力与资质-业务与技术能力详述", 
        "能力与资质-资质认证", "能力与资质-参与培训", "能力与资质-技能标签", 
        "工作经历-开始时间", "工作经历-结束时间", 
        "工作经历-公司名称", "工作经历-担任职务", 
        "工作经历-工作职责说明", "基本情况-第2学历-毕业日期", 
        "基本情况-第2学历-毕业学校", "基本情况-第2学历-专业", 
        "基本情况-第3学历-毕业日期", "基本情况-第3学历-毕业学校", 
        "基本情况-第3学历-专业"
    ]
    headers = [header.replace(" ", "") for header in headers]
    file_path = r"D:\github\11-resume_generator\template\人员简历汇总_20241103.xlsx"
    sdm_excel = excel_helper.Xlsx(file_path)
    df = pd.DataFrame(sdm_excel.data["数据来源"]).iloc[1:]
    df.columns = headers[:len(df.columns)]
    return df

def data_keys():
    try:
        resume_sections = [
            {"基本情况": "basic_info"},
            {"工作经历": "work_experience"},
            {"项目经历": "project_experience"},
            {"能力与资质": "ability_qualification"}
        ]
    except Exception as e:
        print(f"发生错误: {e}")
        return []
    
    return resume_sections

#根据data_keys 将带有相同前缀的列按照相同的后缀合并到一起，最终按照resume_sections的顺序输出，并且以填写人为key，同时保留数据种的前缀，去除填写人为None的数据
def merge_data_by_prefix(df, resume_sections):
    try:
        merged_data = {}
        for section in resume_sections:
            section_name, section_key = list(section.items())[0]
            merged_data[section_key] = {}

            def process_row(row):
                key = row["填写人"]
                if key is None:
                    return
                if key not in merged_data[section_key]:
                    merged_data[section_key][key] = {}
                for column in df.columns:
                    if column.startswith(section_name):
                        suffix = column[len(section_name):]
                        merged_data[section_key][key][suffix] = row[column]

            df.apply(process_row, axis=1)
        return merged_data
    except KeyError as ke:
        logging.error(f"键错误: {ke}，检查输入数据是否包含'填写人'列")
        return {}
    except TypeError as te:
        logging.error(f"类型错误: {te}，检查输入数据类型")
        return {}
    except Exception as e:
        logging.error(f"发生未知错误: {e}")
        return {}



#根据分割字段合并内容

if __name__ == "__main__":
    #将数据转换为 DataFrame
    df = data_to_frame()
    #name_list= df[df.columns[0]].dropna().unique().tolist()
    #print(' '.join(str(name) for name in name_list))
    #print(df[df.columns[0]].dropna().unique().tolist())
    data_keys = data_keys()
    #print(data_keys[0])
    merge_data = merge_data_by_prefix(df, data_keys)
    print(merge_data["basic_info"]['于彦波'])
    print(merge_data["work_experience"]['于彦波'])
    print(merge_data["project_experience"]['于彦波'])