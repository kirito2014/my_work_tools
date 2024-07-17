#!/user/bin/env python3
# -*- coding:utf-8 -*-

"""
作者: zjj
日期: 20231125
功能: 读取CEA模板的数据，先定位了出每个区域的内容，再对区域内容进行格式化，最终得到CEA模板类，包含原始数据及格式化后数据。
日志:
    20231125 zjj v1.0
"""
from collections import OrderedDict
from package.utils import excelhelper


class CeaSDM:
    sdm_key = [
        ["基本信息", "basic_info"]
        , ["更新记录", "update_record"]
        , ["字段映射（第1组）", "column_mapping"]
        # , ["加载过程描述", "loader_describe"]
    ]

    mapping_key = [
        ["字段映射", "column_mapping"]
    ]

    def __init__(self, martix):
        self.martix = martix

        self.axis = self.find_key_axis(self.martix, self.sdm_key)
        self.data = self.cut_key_data(self.martix, self.axis)

        self.sdm = self.format_sdm()

    def format_sdm(self):
        basic_info = self.format_basic_info()
        update_record = self.format_update_record()
        # loader_describe = self.format_loader_describe()

        #
        sub_axis = self.axis[self.sdm_key[2][1]]
        mapping_martix = self.submartix(self.martix, sub_axis[0][0], 0, sub_axis[1][0], sub_axis[1][1])
        mapping_axis = self.find_key_axis(mapping_martix, self.mapping_key)
        mapping_data = self.cut_key_data(mapping_martix, mapping_axis)

        column_mapping = []
        for key, value in mapping_data.items():
            mapping = self.format_column_mapping(value)
            column_mapping.append(mapping)

        sdm_formatted = {
            "basic_info": basic_info
            , "update_record": update_record
            , "column_mapping": column_mapping
            # , "loader_describe": loader_describe
        }

        return sdm_formatted

    def format_basic_info(self):
        key = self.sdm_key[0][1]
        data = self.data[key]

        basic_info = OrderedDict({
            "table_cn_name": data[1][1]
            , "table_pk_column": data[1][3].split("、")
            , "analyse_user": data[1][6]
            , "model_level": data[1][8]
            , "data_interval": data[1][10]
            , "table_name": data[2][1]
            , "table_schema": data[2][3]
            , "create_date": data[2][6]
            , "model_theme": data[2][7]
            , "date_retention": data[2][10]
            , "table_comment": data[3][1]
        })

        return basic_info

    def format_update_record(self):
        key = self.sdm_key[1][1]
        data = self.data[key]

        update_record = []
        for index, row in enumerate(data):
            if index in (0, 1):
                pass
            else:
                record = OrderedDict({
                    "update_date": row[0]
                    , "update_user": row[1]
                    , "update_comment": row[2]
                })

                update_record.append(record)

        return update_record

    def format_loader_describe(self):
        key = self.sdm_key[3][1]
        data = self.data[key]

        loader_describe = OrderedDict({
            "init_setting": data[1][1]
            , "init_load": data[2][1]
            , "daily_load": data[3][1]
        })

        return loader_describe

    def format_column_mapping(self, value):
        keyword_axis = self.submartix(value, 0, 0, len(value), 1)

        # 查找分布键的位置
        key_index = {}
        for index, keyword in enumerate(keyword_axis):
            if keyword[0] is None:
                continue
            elif "字段中文名" in keyword[0]:
                key_index["column_key"] = index
            elif "分布键" in keyword[0]:
                key_index["distribut_key"] = index
            elif "表关联信息" in  keyword[0]:
                key_index["table_join"] = index
            elif "过滤条件" in  keyword[0]:
                key_index["where"] = index
            elif "分组条件" in keyword[0]:
                key_index["group_by"] = index
            elif "排序条件" in  keyword[0]:
                key_index["order_by"] = index
            else:
                pass

        # 1.获取映射信息
        column_mapping = OrderedDict({
            "id": value[0][0]
            , "table_cn_name": value[1][1]
            , "table_name": value[1][3]
            , "table_comment": value[1][6]
            , "template_table_type": value[1][10]
            , "distributed_key": value[key_index["distribut_key"]][1]
            , "where": value[key_index["where"]][1]
            , "group_by": value[key_index["group_by"]][1]
            , "order_by": value[key_index["order_by"]][1]
        })

        # 2.获取字段映射数据
        mapping_martix = self.submartix(value, key_index["column_key"]+1, 0, key_index["distribut_key"], len(value))
        mapping = []
        for line in mapping_martix:
            data = OrderedDict({
                "tgt_column_cn_name": line[0]
                , "tgt_column_name": line[1]
                , "tgt_column_type": line[2]
                , "src_schema": line[3]
                , "src_table_cn_name": line[4]
                , "src_table_name": line[5]
                , "src_column_cn_name": line[6]
                , "src_column_name": line[7]
                , "src_column_type": line[8]
                , "mapping_rule": line[9]
                , "comment": line[10]
            })
            mapping.append(data)

        # 3.获取表关联数据
        table_join_martix = self.submartix(value, key_index["table_join"]+2, 0, key_index["where"], len(value))
        table_join = []
        for line in table_join_martix:
            if line[2] is None:
                continue

            data = OrderedDict({
                "src_schema": line[0]
                , "src_table_cn_name": line[1]
                , "src_table_name": line[2]
                , "src_table_alias": line[3]
                , "join_type": line[4]
                , "join_condition": line[5]
                , "etl_job_column": line[8]
                , "etl_job_name": line[9]
            })
            table_join.append(data)

        # column_mapping = OrderedDict({
        #     "info": info
        #     , "mapping": mapping
        #     , "table_join": table_join
        # })
        column_mapping.update({
            "mapping": mapping
            , "table_join": table_join
        })
        return column_mapping

    @staticmethod
    def find_key_axis(data_martix, keyword_martix):
        sub_matix = CeaSDM.submartix(data_martix, 0, 0, None, 2)
        # 定义SDM各部分关键字、下标的数据结构
        key_axis = OrderedDict()
        for keyword in keyword_martix:
            key_axis[keyword[1]] = []

        # 遍历整个Excel表格内容，找到每一个关键字的下标
        # 模糊匹配时的后缀
        fuzzy_match = {}
        for line_key, line in enumerate(sub_matix):
            for row_key, element in enumerate(line):
                if element is None:
                    continue
                else:
                    element = str(element)
                    a = dict()

                # 将Excel的值与所有关键字进行匹配
                for cn_keyword, en_keyword in keyword_martix:
                    # 值与关键字相等，记录其下标
                    if element == cn_keyword:
                        key_axis[en_keyword].append([line_key, row_key])
                    # 值包含关键字，记录其多个下标
                    elif cn_keyword in element:
                        if en_keyword in fuzzy_match:
                            fuzzy_match[en_keyword] += 1
                        else:
                            fuzzy_match[en_keyword] = 1
                        key_axis["%s_%s" % (en_keyword, fuzzy_match[en_keyword])] = [[line_key, row_key]]
                    else:
                        pass

        # 当使用了模糊匹配时，把匹配的关键字从字典中去除。
        for key, value in fuzzy_match.items():
            del key_axis[key]

        key_list = list(key_axis.keys())
        for index, keyword in enumerate(key_list):
            if index != len(key_axis) - 1:
                next_en_keyword = key_list[index + 1]
                key_axis[keyword].append([key_axis[next_en_keyword][0][0], len(data_martix[0])])
            else:
                key_axis[keyword].append([len(data_martix), len(data_martix[0])])

        return key_axis

    @staticmethod
    def cut_key_data(data_martix, key_axis):
        """
        main_key_axis = {
            'basic_info': [[2, 1], [6, 13]]
            , 'update_record': [[6, 1], [10, 13]]
            , 'column_mapping': [
                  [[10, 1], [45, 13]]
                , [[45, 1], [91, 13]]
                , [[91, 1], [157, 13]]
                , [[157, 1], [228, 13]]
                , [[228, 1], [297, 13]]
                , [[297, 1], [360, 13]]]
            , 'loader_describe': [[360, 1], [367, 13]]}
        """
        key_data = OrderedDict()
        for key, axis in key_axis.items():
            start_line, start_row = axis[0]
            end_line, end_row = axis[1]
            sub_matrix = CeaSDM.submartix(data_martix, start_line, start_row, end_line, end_row)
            key_data[key] = sub_matrix

        return key_data

    # 根据开始及结束位置取子矩阵。
    @staticmethod
    def submartix(matrix, start_line_num, start_row_num, end_line_num=None, end_row_num=None):
        """
        a = [ [1, 2, 3]
            , [4, 5, 6]
            , [7, 8, 9]]
        b = submartix(a, 1, 1)
        b = [ [5, 6]
            , [8, 9]]
        """
        return [line[start_row_num:end_row_num] for line in matrix[start_line_num: end_line_num]]


def main():
    file_path = "C:/PortableAPP/sunline_etl_tool/templage/cea/sdm_for_etlscript.xlsm"
    sdm_excel = excelhelper.Xlsx(file_path)
    sdm_data_dl2 = sdm_excel.data

    table_sdm_l2 = sdm_data_dl2["S01_INDV_CUST_BASE_INFO"]
    cea_sdm = CeaSDM(table_sdm_l2)
    #print(table_sdm_l2)

    return True


if __name__ == '__main__':
    main()
