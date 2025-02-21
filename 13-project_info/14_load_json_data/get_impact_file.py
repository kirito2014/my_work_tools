#*--coding utf-8--*
import requests
import json
import re,os,time
from login import login_manager
import openpyxl
from openpyxl import Workbook

#根据清单项目编号 自动排序 切换对应的项目
# {id: 133, name: "AGLCOMM"}
# {id: 134, name: "AGLS01"}
# {id: 139, name: "AGLS02"}
# {id: 137, name: "AGLS03"}
# {id: 135, name: "AGLS04"}
# {id: 191, name: "AGLS05"}
#读取配置文件
#{"appName":"AGLS04","dispatchId":2,"envName":"Dispatch_PERF","etlSystem":"AGLS04_340045","etlJob":"AGL_IBANK_PLEDGE_TXN_DTL_TA_PC","layer":1,"skipVirtualJob":1}
#AGLS04_340045|AGL_IBANK_PLEDGE_TXN_DTL_TA|7
#排序清单
#根据应用名，判断是否需要切换，如上一个时AGLS01 下一个仍然是AGLS01 就不需要切换

class GetImpactFile:
    def __init__(self,app,table_list,headers):
        self.app = app
        self.table_list = table_list
        self.headers = headers
        self.authorization = None

    #读取配置文件
    def read_file(self,table_list):
        with open(table_list,'r',encoding='utf-8') as file:
            lines = file.readlines()
        lines = [line.strip() for line in lines if line.strip()]
        unique_lines = list(set(lines))
        unique_lines.sort(key=lambda x:x.split('|')[1].split('_')[0])
        self.table_job_list = []
        for line in unique_lines:
            if '|' in line:
                parts = line.split('|')
                if len(parts) == 3:
                    self.table_job_list.append((parts[0].strip(),parts[1].strip(),parts[2].strip()))
                else:
                    print(f"配置读取错误:{line}格式错误")
            else:
                print(f"配置读取错误:{line} 中没有'|' 分隔符")
        return self.table_job_list

    def get_current_project(self,table_job_list):
        #初始化应用名
        prv_job_app = None
        for idx,(table_name,job_number,impact_layer) in enumerate(self.table_job_list):
            time.sleep(10)
            job_app = job_number.split('_')[0].upper()

            if job_app != prv_job_app:
                #切换对应项目
                #print(job_app)
                self.change_project(job_app)
                #获取当前项目的初始ID和状态,执行后获取最新的ID，如初始ID和最新ID一致则推出，若不一致 判断状态是否为2
                #一般提交顺序都是递增顺序
                begin_first_records,begin_first_status=self.get_first_id()
                self.submit_impact(job_app,job_number,table_name,impact_layer)
                after_first_records,after_first_status=self.get_first_id()
                #print(begin_first_records)
                #print(after_first_records)
                while True:
                    
                    if begin_first_records == after_first_records:
                        print(f'{table_name}刷新中....')
                        time.sleep(2)
                        after_first_records,after_first_status=self.get_first_id()
                    #处理中
                    elif begin_first_records != after_first_records and after_first_status != 2 :
                        print(f'{table_name}还是在刷新中....')
                        time.sleep(2)
                        after_first_records,after_first_status=self.get_first_id()
                    elif begin_first_records != after_first_records and after_first_status == 2 :
                        print(f'{table_name}找到啦，开整！！！！')
                        break
                first_records =after_first_records
                #取第一条的详细信息

                self.get_job_json(first_records,table_name,job_number)

            elif job_app == prv_job_app:
                begin_first_records,begin_first_status=self.get_first_id()
                self.submit_impact(job_app,job_number,table_name,impact_layer)
                after_first_records,after_first_status=self.get_first_id()
                #print(begin_first_records)
                #print(after_first_records)
                while True:
                    if begin_first_records == after_first_records:
                        print(f'{table_name}刷新中....')
                        time.sleep(2)
                        after_first_records,after_first_status=self.get_first_id()
                        print(after_first_records)
                    #处理中
                    elif begin_first_records != after_first_records and after_first_status != 2 :
                        print(f'{table_name}还是在刷新中....')
                        time.sleep(2)
                        after_first_records,after_first_status=self.get_first_id()
                    elif begin_first_records != after_first_records and after_first_status == 2 :
                        print(f'{table_name}找到啦，开整！！！！')
                        break
                first_records =after_first_records
                
                #取第一条的详细信息
                self.get_job_json(first_records,table_name,job_number)
            #print(first_records)

            prv_job_app = job_app

    def change_project(self,app_name):
        #获取app对应的id
        app_id = app.get(f"{app_name}") 
        #print(app_id)
        try:
            payload_1 = {r"appId":app_id}
            payload = json.dumps(payload_1,indent=None,separators=(',',':'))
            change_prj_url = "http://158.219.232.42:7777/api/bdsp/common/change/project"
            response = requests.request("POST", change_prj_url, headers=self.headers, data=payload).json()
            #print(response)
            head = response.get("head",{})
            records = response.get("bizContent",{})
            if not records and head.get("returnCode") != "00000": 
                print("切换项目失败,")
                return False
            elif records:
                print(f"项目切换成功，当前项目:{app_name}")
                return True
        except Exception as e:
            print(f"切换项目时遇到错误：{e}")

    def submit_impact(self,job_app,job_number,table_name,impact_layer):
        try:
            payload_submit = {"appName":f"{job_app}","dispatchId":2,"envName":"Dispatch_PERF","etlSystem":f"{job_number}","etlJob":f"{table_name}_PC","layer":str(impact_layer),"skipVirtualJob":1}
            payload = json.dumps(payload_submit,indent=None,separators=(',',':'))
            submit_impact_url =  "http://158.219.232.42:7777/api/operation/impact/submit"
            response = requests.request("POST", submit_impact_url, headers=self.headers, data=payload).json()
            head = response.get("head",{})
            if  head.get("returnCode") != "00000" :
                print("分析任务提交成功.")
        except Exception as e:
            print(f"提交下游影响影响分析时遇到错误：{e}.")

    #获取当前项目的第一个id 和status 如status <> 2则为处理中 
    #若提交后的id和提交前的id一致则提交失败

    def get_first_id(self):
        try:
            get_impact_list_url = "http://158.219.232.42:7777/api/operation/impact/history/list"
            payload = ""
            response = requests.request("GET", get_impact_list_url, headers=self.headers, data=payload).json()
            head = response.get("head",{})
            records = response.get("bizContent",{}).get("records",[])
            if not records: 
                #print("record 为空")
                return None
            else:
                first_records = records[0].get("id",None)
                first_status = records[0].get("status",None)
                #print(f"获取id和status :{first_records} , {first_status}")
            return first_records,first_status
        except Exception as e:
            print(f"获取分析列表出现错误：{e}.")



    def get_job_json(self,first_records,job_name,job_number):
        #获取提交后的第一个id

        try:
            get_detail_url = f"http://158.219.232.42:7777/api/operation/impact/detail/list?id={first_records}&current=1&size=500"
            payload = ""
            response_detail = requests.request("GET", get_detail_url, headers=self.headers, data=payload).json()
            records = response_detail.get("bizContent",{}).get("records",[])
            if not records:
                print("JSON 中无可用记录.")
            self.write_to_excel(records,job_name,job_number)
        except Exception as e:
            print(f"发生错误：{e}.")

    @staticmethod
    def write_to_excel(records,job_name,job_number):
        output_file = "output.xlsx"
        try:
            workbook = openpyxl.load_workbook(output_file)
            print(f"{output_file} 文件已存在，加载中....")
        except FileNotFoundError:
            print(f"{output_file} 文件不存在，创建中...")
            workbook = Workbook()
        
        #获取sheet1
        if "output" in workbook.sheetnames:
            sheet = workbook["output"]
        else:
            sheet = workbook.active
            sheet.title = "output"
        #写入表头

        if sheet.max_row == 1 and sheet.max_column == 1:
            headers = [
                "英文表名",
                "作业编号",
                "下游作业编号",
                "下游作业名称",
                "依赖上游作业编号",
                "依赖上游作业名称",
                "影响层级",
                "作业类型",
                "是否虚拟作业",
                "运维支持组",
                "运维联系人",
                "下游依赖类型",
                "分析时间",
                "是否确认"

            ]
            sheet.append(headers)

        #清除A2后的数据，只在首次执行
        # if  sheet.max_row >1:
        #     for row in sheet.iter_rows(min_row=2,max_row=sheet.max_row,max_col=sheet.max_column):
        #         for cell in row:
        #             cell.value = None 
        
        #插入数据
        current_row = sheet.max_row
        #print(len(records))
        if len(records) == 0 :
            row = [
                job_name,
                "无下游影响"
            ]
            sheet.append(row)
        else:
            for record in records:
                downstream_type = "直接下游" if record.get("layer") == 1 else "间接下游"
                isVirtualJob = "是" if record.get("isVirtualJob") == 1 else "否"
                row = [
                    job_name,
                    job_number,
                    record.get("etlSystem"),
                    record.get("etlJob"),
                    record.get("dependencyEtlSystem"),
                    record.get("dependencyEtlJob"),
                    record.get("layer"),
                    record.get("jobTypeDesc"),
                    isVirtualJob,
                    record.get("operationSupportGroup"),
                    record.get("contacts"), 
                    downstream_type,
                    record.get("createTime")
                    
                ]
                sheet.append(row)

        #保存文件
        workbook.save(output_file)
        print(f"数据已写入：{output_file}")
        
    def main(self):
        #登录网站 获取认证状态及认证串
        self.authorization = login_manager.get_authorization()
        print(self.authorization)
        self.headers["Authorization"] = self.authorization
        #分析清单
        table_job_list = self.read_file(self.table_list)
        self.get_current_project(table_job_list)
        #根据清单切换项目
        #完成后退出登录
        #login_manager.logout()



app = {"AGLCOMM":133,"AGLS01":134,"AGLS02":139,"AGLS03":137,"AGLS04":135,"AGLS05":191}
table_list="shangxianqingdan.txt"
headers = {
'Content-Type': 'application/json;charset=UTF-8'
}
get_impact_file = GetImpactFile(app,table_list,headers)
get_impact_file.main()
