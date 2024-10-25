import os,re
import time
import datetime
import pandas as pd
import numpy as np
from docxtpl import DocxTemplate
from package.function import render

def resume_generator():

    person_name = '测试人员'
    gen_date = '2024-10-10'
    template_file = f'人员简历_模板.docx'
    target_file = f'人员简历_{person_name}_{gen_date}.docx'
    print(target_file)
    tpl = DocxTemplate(template_file)
    context = {
        'person_name': '测试人员',
        'work_years': '5',
        'business_ability':'11111',
        'qulity_certification':'222',
        'participate_training':'22222'
    }
    tpl.render(context)
    tpl.save(target_file)