-- 层次表名: {{basic_info.model_level}}-{{basic_info.table_cn_name}}
-- 模板作者: Zjj
-- 使用方法: python $ETL_HOME/script/main.py yyyymmdd {{basic_info.table_schema}}_{{basic_info.table_name|lower}}
-- 创建日期: 2023-11-27
-- 脚本类型: DML
-- 修改日志:
--     表英文名：{{basic_info.table_name}}
--     表中文名：{{basic_info.table_cn_name}}
--     创建日期：{{basic_info.create_date}}
--     主键字段：
            {%- for pk in basic_info.table_pk_column %}
                {{-pk}}
                {%- if not loop.last %}, {% endif %}
            {%- endfor %}
--     归属层次：{{basic_info.model_level}}
--     归属主题：{{basic_info.model_theme}}
--     库/模式：{{basic_info.table_schema}}
--     分析人员：{{basic_info.analyse_user}}
--     时间粒度：{{basic_info.data_interval}}
--     保留周期：{{basic_info.date_retention}}
--     描述信息：{{basic_info.table_comment}}
--     更新记录：
           {%- for record in update_record %}
--         {{record.update_date}} {{record.update_user}} {{record.update_comment}}
           {%- endfor %}

-- 1.清除目标表当天分区数据
alter table {{basic_info.table_schema}}.{{basic_info.table_name}} drop if exists partition(pt_dt = '${process_date}');

-- 2.处理各组字段映射的处理规则
{#- 循环输出每一组字段映射的处理规则 #}
{%- for cm in column_mapping %}
-- =============={{cm.id}}==============
-- {{cm.table_cn_name}}
{%- if 'Y' in cm.template_table_type %}
drop table if exists {{basic_info.table_schema}}.{{cm.table_name}};

create table {{basic_info.table_schema}}.{{cm.table_name}} (
{% for m in cm.mapping %}
    {%- if m.tgt_column_name is not none %}
        {%- if not loop.first %}, {% else %}      {% endif %}
        {{-m.tgt_column_name}} {{m.tgt_column_type|lower}} -- {{m.tgt_column_cn_name}}
    {% endif %}
{%- endfor %}
)
comment '{{cm.table_cn_name}}'
partitioned by (
    pt_dt string comment '表分区日期'
)
stored as orc
tablproperties ("orc.compress"="SNAPPY")
;
{%- endif %}

insert into table {{basic_info.table_schema}}.{{cm.table_name}}(
{% for m in cm.mapping %}
    {%- if m.tgt_column_name is not none %}
        {%- if not loop.first %}, {% else %}      {% endif %}
        {{-m.tgt_column_name}} -- {{m.tgt_column_cn_name}}
    {% endif %}
{%- endfor %}
)
select
{% for m in cm.mapping %}
    {%- if m.mapping_rule is not none %}
        {%- if not loop.first %}, {% else %}      {% endif %}
            {{-m.mapping_rule}} as {{m.tgt_column_name}} -- {{m.src_column_cn_name}}
    {% endif %}
{%- endfor %}
from
{%- for tj in cm.table_join %}
    {% if tj.join_type is not none %}{{tj.join_type}} {% endif %}
    {%- if tj.src_schema is not none %}{{-tj.src_schema}}.{% endif %}
    {{-tj.src_table_name}} as {{tj.src_table_alias}} -- {{tj.src_table_cn_name}}
    {% if tj.join_condition is not none %}on {{tj.join_condition}} {% endif %}
{%- endfor %}
{#- 以下三项均是有值时才输出 #}
{%- if cm.where is not none %}
where {{cm.where}}
{%- endif %}
{%- if cm.group_by is not none %}
group by {{cm.group_by}}
{%- endif %}
{%- if cm.order_by is not none %}
order by {{cm.order_by}}
{%- endif %}
;
{%- endfor %}

-- 删除所有临时表
{%- for cm in column_mapping %}
{%- if 'Y' in cm.template_table_type %}
drop table {{basic_info.table_schema}}.{{cm.table_name}};
{%- endif %}
{%- endfor %}