import json
import pandas as pd
import os
import numpy as np

# Load the JSON data

# file = f"D:/github/11-resume_generator/template/template.json"
# with open(file, "r") as f:  
#     data = json.loads(f)    
    
    
    
data = json.loads("""
{
    "PersonName": {
        "BasicInfo": {
            "Name": "1",
            "WorkYears": "2",
            "GraduationTime": "3",
            "GraduationSchool": "4",
            "Major": "5",
            "HighestEducation": "6",
            "Department": "7",
            "Title": "8",
            "PersonalProfile": "9"
        },
        "WorkExperience": [
            {
                "StartTime": "2023/01/01",
                "EndTime": "至今",
                "CompanyName": "xxx1公司",
                "Position": "xxxx职务",
                "JobDescription": "xxxxxxxx"
            },
            {
                "StartTime": "2022/01/01",
                "EndTime": "2022/12/31",
                "CompanyName": "xxx2公司",
                "Position": "xxxx职务",
                "JobDescription": "xxxxxxxx"
            },
            {
                "StartTime": "2021/01/01",
                "EndTime": "2021/12/31",
                "CompanyName": "xxx3公司",
                "Position": "xxxx职务",
                "JobDescription": "xxxxxxxx"
            }
        ],
        "ProjectExperience": [
            {
                "StartTime": "2023/01/01",
                "EndTime": "至今",
                "ProjectName": "xxx1项目",
                "ProjectRole": "xxxx职务",
                "JobDescription": "xxxxxxxx"
            },
            {
                "StartTime": "2022/01/01",
                "EndTime": "2022/12/31",
                "ProjectName": "xxx2项目",
                "ProjectRole": "xxxx职务",
                "JobDescription": "xxxxxxxx"
            }
        ],
        "WorkAbility": {
            "BusinessAbility": "wwww",
            "Certification": "qqqqq",
            "Training": "eeee"
            "SkillTag":"ttttt"
        }
    }
}

""")

# Adjust the format
#data转为字典
print(data['PersonName']['WorkExperience'][0])
print(data['PersonName']['BasicInfo']['Name'])
print(data['PersonName']['WorkAbility']['BusinessAbility'])
#print(data)
