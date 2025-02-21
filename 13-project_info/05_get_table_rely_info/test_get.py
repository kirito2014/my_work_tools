
file_path = 'D:/sunline_etl_tool/autocode/agl/dml/agl_recv_bil_agt_info_tf_pc.hql'

with open(file_path, 'r' ,encoding='utf-8') as file:
    lines = file.readlines()
    if len(lines) >= 19:
        line_9 = lines[8]
        if '：' in line_9:
            #print(line_9.split('：',1)[1].strip())
            tab_cn_name = line_9.split('：',1)[1].strip()
            print(tab_cn_name)
            #get_file_info.append(tab_cn_name)
        line_15 = lines[14]
        #print(line_15)
        if '：' in line_15:
            #print(line_15.split('：',1)[1].strip())
            dev_ops = line_15.split('：',1)[1].strip()
            print(dev_ops)