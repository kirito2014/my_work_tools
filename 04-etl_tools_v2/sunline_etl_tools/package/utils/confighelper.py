# -*- coding:utf-8 -*-
import os
import sys;
import configparser,collections
os.environ["ETL_HOME"] = 'D:\\vscode\\sunline_etl_tools'
etl_home = os.environ["ETL_HOME"] 
print(etl_home)


def get_config(path):
    cp = configparser.ConfigParser(dict_type=collections.OrderedDict,allow_no_value=True)
    cp.read(path)

    dict = {}
    for section in cp.sections():
        dict[section] = {}
        for key, val in cp.items(section):
            dict[section][key] = val
    return dict
def get_etl_home():
    return etl_home

def main():
    src_config = os.path.join(etl_home, 'etc', 'src_config.conf')
    #print(src_config)
    src_dict = get_config(src_config)
    print(src_dict)
    print(src_dict["core"]["src_ip"])

if __name__ == "__main__":
    main()
