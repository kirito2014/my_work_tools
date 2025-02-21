import pandas as pd

#读取Excel文件，返回数据集合
def read_excel_with_header(file_path):
    return pd.read_excel(file_path,sheet_name='rem-代码映射',header=1,index_col=None)

def compare_files(file_old,file_new):
    df_old = read_excel_with_header(file_old)
    df_new = read_excel_with_header(file_new)

    key_columns = df_old.columns.tolist()
    df_old['Key'] = df_old[key_columns].astype(str).agg('|'.join,axis=1)
    df_new['Key'] = df_new[key_columns].astype(str).agg('|'.join,axis=1)

    deleted = df_old[~df_old['Key'].isin(df_new['Key'])].copy()
    deleted['Status'] = '删除'

    added = df_new[~df_new['Key'].isin(df_new['Key'])].copy()
    added['Status'] = '新增'

    merged = pd.merge(df_old,df_new,on='Key',suffixes=('_old','_new'))
    changed = merged[
        (merged.drop(columns=['Key']).filter(like='_old').values !=
        merged.drop(columns=['Key']).filter(like='_old').values).any(axis=1)
    ].copy()
    changed['Status'] = '变更'

    changed = df_new[df_new['Key'].isin(changed['Key'])]

    df_old.drop(columns=['Key'],inplace=True)
    df_new.drop(columns=['Key'],inplace=True)

    result = pd.concat([deleted,added,changed],ignore_index=True)

    return result

def save_result_to_excel(df,output_path):
    df.to_excel(output_path,index=False)

if __name__=='__main__':
    file_old=r'D:\git\tools\08_sdm_collect_tool\pub_cd_map_old.xlsx'
    file_new=r'D:\git\tools\08_sdm_collect_tool\pub_cd_map.xlsx'


    output_path = 'output_comparison.xlsx'
    result_df = compare_files(file_old,file_new)
    save_result_to_excel(result_df,output_path)

    print(f"结果已保存到{output_path}")