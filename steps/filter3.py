
from snowflake.snowpark.functions import col
from snowflake.snowpark.files import SnowflakeFile

import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import pandas as pd
import snowflake.snowpark.functions as F
import yaml
import sys
import json

#where_clause = '""type""=''configuration/entityTypes/HCP'''


v_json_prev_field_list=[]  # Global variable 
v_table_name='' # global variable

v_audit_columns_list = ['BATCH_RUN_ID','REC_INSERT_TMSP','REC_CREATED_BY']
v_audit_columns_values_list = ["(select batch_id from load_master where open_status='Open' and process_name='HCP') as BATCH_RUN_ID",'CURRENT_TIMESTAMP() AS REC_INSERT_TMPS','CURRENT_USER() AS REC_CREATED_BY']

def cust_address(session,config_file,interface_name):
    pass


def build_flatten_class(session,Objects) -> str:
    str1='--'
    #return "LATERAL FLATTEN(input => ""attributes"":" + Objects[0] + ", outer => true) {}".format(Objects[0])
    v_json_field_str=list(Objects.keys())[0]
    v_json_value_str=list(Objects.values())[0][0]
    v_json_dataType_str=list(Objects.values())[0][1]
    v_json_alias_str =list(Objects.values())[0][2]
    v_json_path_str =list(Objects.values())[0][3]

    global v_json_prev_field_list

    if v_json_field_str.strip() not in v_json_prev_field_list:
        v_json_prev_field_list.append(v_json_field_str.strip())  # list to keep tracking of flatten inline Query , to avoid the dupicate inline query 

        #if v_table_name != 'NA': #'MDM_CUSTOMER_SALESTEAM':
        print(str1)
        if v_json_path_str.strip() != 'NA':
           str1= "LATERAL FLATTEN(input => {}.value:value ,path => '{}', outer => true) {}".format(v_json_path_str,v_json_field_str.replace('_1',''),v_json_field_str)
        else: 
           str1= "LATERAL FLATTEN(input => src:attributes:" + v_json_field_str + ", outer => true) {}".format(v_json_field_str)
    return str1
def build_filter_class(session,Objects):
    v_json_field_str=list(Objects.keys())[0]
    v_json_value_str=list(Objects.values())[0][0]
    v_json_dataType_str=list(Objects.values())[0][1]
    v_json_alias_str =list(Objects.values())[0][3]

    return "NVL({}.value:ov::string,'true')='true'".format(v_json_field_str)

#def save_data(df_save,to_table_name) --> str:
#   df_save.write.mode("append").save_as_table(to_table_name,column_order='name') 

def get_sql(session,interface_name: str):
    global v_table_name 
    v_table_name = interface_name
    scope_url = session.sql("select BUILD_SCOPED_FILE_URL(@STGS3, 'project_config3.yml') as sc_url")
    scope_url= scope_url.select("sc_url").collect()
    sc_url= scope_url[0][0]
    with SnowflakeFile.open(sc_url) as f:
        config_read=yaml.safe_load(f)
        column_list = config_read["CANONICAL"][interface_name]
        database_name = config_read["CANONICAL"]["DATABASE_NAME"]
        schema_name = config_read["CANONICAL"]["SCHEMA_NAME"]
        where_clause = " WHERE type='configuration/entityTypes/HCP'"
        
    #prepare default column list 

        v_default_columns_select_list = [my_dic for my_dic in column_list if list(my_dic.keys())[0] == 'Default']
        v_default_columns_select_list=(v_default_columns_select_list[0])['Default']
        v_default_columns_select_list = [ "'{}' as {}".format(x[0],x[1]) for x in v_default_columns_select_list ]
        v_default_columns_select_list = ','.join(v_default_columns_select_list)
        
    #default columns for insert sql (col1,col2...)
        v_dafault_to_save_columns_list = [x[1] for x in v_default_columns_select_list ]
        
    #prepare insert sql columns parameters using the both main select columns list + default columns + audit columms (col1,col2....)
        
        v_to_save_columns_list = [list(my_dic.values())[0][2] for my_dic in column_list  \
                if (list(my_dic.values())[0])[0] != 'NA' and list(my_dic.keys())[0] != 'Default'] +  v_dafault_to_save_columns_list + v_audit_columns_list

        v_to_save_columns_str = ','.join(v_to_save_columns_list)
        v_to_save_columns_str ="INSERT INTO STG_{}({})".format(interface_name,v_to_save_columns_str)

    #confg_yaml =config_yaml_data
        #column_list= config_read["CANONICAL"][interface_name]
    #select section of unpack sql, all columns in select unpack sql
        v_select_colummns_unpack_1_str=[list(my_dic.keys())[0] + ".value:" + (list(my_dic.values())[0])[0]\
                + "::" + (list(my_dic.values())[0])[1] + " as {}".format(list(my_dic.values())[0][2]) for my_dic in column_list if (list(my_dic.values())[0])[0] != 'NA' and list(my_dic.keys())[0] != 'Default' ] + v_default_columns_select_list + v_audit_columns_values_list
        v_select_colummns_unpack_1_str = "\n,".join(v_select_colummns_unpack_1_str)
    #lateral flatten section of unpack sql
        v_flatten_unpack_2_str = [build_flatten_class(session,my_dec) for my_dic in column_list if list(my_dic.keys())[0] not in v_json_prev_field_list and ( (list(my_dic.values())[0])[0] != 'NA' and list(my_dic.keys())[0] != 'Default') ]
        #remove NONE values : 
        v_flatten_unpack_2_str=[i for i in v_flatten_unpack_2_str if i is not None]
        v_flatten_unpack_2_str="\n,".join(v_flatten_unpack_2_str)
        v_flatten_unpack_2_str.replace(',--','')         
    #fitler section of unpack sql
        v_filter_unpack_3_str=''
        if interface_name == 'MDM_CUSTOMER_MASTER':  # currently filter is used only for customer_master fo other tables add into this list 
            v_filter_unpack_3_str = [build_filter_class(session,my_dec) for my_dec in column_list]
            v_filter_unpack_3_str = "\n AND ".join(v_filter_unpack_3_str)
            v_filter_unpack_3_str = "WHERE " + v_filter_unpack_3_str
    #build from class of unpack sql
        #v_from_clause_str = "FROM {}.{}.{}_VW_STREAMS P".format(database_name,schema_name,interface_name)
        v_from_clause_str = "FROM {}.{}.{}_VW_STREAMS P".format('db_naushad','schema_naushad','car_sales1')

    #build full unpack sql
        #unpack_sql = "CREATE OR REPLACE TABLE STG_{} AS SELECT \n {} \n {} ,\n {}  \n  {} \n {}".format(interface_name,v_select_colummns_unpack_1_str, v_from_clause_str, v_flatten_unpack_2_str,where_clause,v_filter_unpack_3_str)
        #unpack_sql = "CREATE OR REPLACE TABLE STG_{} AS SELECT \n {} \n {} ,\n {}  \n {}".format(interface_name,v_select_colummns_unpack_1_str, v_from_clause_str, v_flatten_unpack_2_str,v_filter_unpack_3_str)
        unpack_sql = "{} \n SELECT \n {} \n {} ,\n {}  \n {}".format(v_to_save_columns_str,v_select_colummns_unpack_1_str, v_from_clause_str, v_flatten_unpack_2_str,v_filter_unpack_3_str)
     #create stg tables 
        #session.sql("create or replace table abac_test(empid number)").collect()
        session.sql(unpack_sql).collect()
    return unpack_sql

#if __name__ == "__main__":
#    #print(get_sql('MDM_CUSTOMER_MASTER'))
#     print(get_sql('MDM_CUSTOMER_MASTER'))
