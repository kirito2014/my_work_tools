
import pandas as pd
import excel_helper

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
    #sheet_data = sdm_excel.data["数据来源"]
    df = pd.DataFrame(sdm_excel.data["数据来源"]).iloc[1:]
    df.columns = headers[:len(df.columns)]
    #print(df.columns)
    #print(df)
    return df

def data_keys():
    resume_key = [
        ["基本情况":"basic_info"]
        ,["工作经历":"work_experience"]
        ,["项目经历":"project_experience"]
        ,["能力与资质":"ability_qualification"]
    ]
    return resume_key

#根据分割字段合并内容

if __name__ == "__main__":
    #将数据转换为 DataFrame
    data_to_frame()