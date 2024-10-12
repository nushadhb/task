
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

# Open and read the JSON file
# load environment variables
#f_file_json=r'C:\naushad\AbbVie\jsonfiles\HCO_L2_JSON.json'
#config_file="poject_config.yml"

#scope_url = session.sql("select BUILD_SCOPED_FILE_URL(@STGS3, 'project_config.yml') as sc_url")
#scope_url= scope_url.select("sc_url").collect()
#sc_url= scope_url[0][0]


#f= open("project_config.yml", "r")
#f= open(sc_url,"r")
#config_yaml_data = yaml.safe_load(f)
#f.close
#database_name = config_yaml_data["CANONICAL"]["DATABASE_NAME"]
#schema_name = config_yaml_data["CANONICAL"]["SCHEMA_NAME"]
#where_clause = '""type""=''configuration/entityTypes/HCP'''

Address_list=['AddressLine1','AddressLine2','AddressLoadDate','AddressScore','AddressStatus','AddressType','City',\
              'Country','DEA','GeoLocation']
Dea_list=['ExpirationDate','Status1']
Geo_list=['GeoAccuracy','Latitude','Longitude','HCOAddressScore','PreferredPhysicalAddressFlag','Premise','StateProvince',\
          'Street','SubAdministrativeArea','VerificationStatus','VerificationStatusDetails',\
         'Zip', ]
zip_list=['Zip4','Zip5']
Education_list=['SchoolName','Type','Degree','YearsInProgram','GraduationYear','StartYear','EndYear','FieldofStudy','ConfirmationFlag','SchoolCode','State','EducationEndDate']

##MD_Customer_salesteam sub-level
Team_assignment_address=['AddressID','AddressLine1','AddressLine2','City','StateProvince','Zip4','Zip5'] # Address
Team_assinment_saleteam=['SalesTeam','Address','CalledOnStatus','Email','AMDMID'] #TeamAssignment
Team_assignment_eamil=['Email']
Team_assignment_phone=['Phone']
team_assignment_nested_levels ={'Address':"['AddressID','AddressLine1','AddressLine2','City','StateProvince','Zip4','Zip5']"\
                             ,'TeamAssignment':"['SalesTeam','Address','CalledOnStatus','Email','AMDMID']"
                             ,'Email':"['Phone']"}

v_json_prev_field=[]  # Global variable 
v_table_name='' # global variable
def cust_address(session,config_file,interface_name):
    pass

def get_nested_obect(v_json_field: str) ->str:
    for key,val in team_assignment_nested_levels.items():
        if v_json_field in val:
            return "LATERAL FLATTEN(input => {}.value:value ,path => {}, outer => true) {}".format(key,v_json_field,v_json_field)
        else: 
            return "LATERAL FLATTEN(input => src:attributes:" + v_json_field + ", outer => true) {}".format(v_json_field)


def build_flatten_class(session,Objects) -> str:
    str1='--'
    #return "LATERAL FLATTEN(input => ""attributes"":" + Objects[0] + ", outer => true) {}".format(Objects[0])
    v_json_field=list(Objects.keys())[0]
    v_json_value=list(Objects.values())[0][0]
    v_json_dataType=list(Objects.values())[0][1]
    v_json_alias =list(Objects.values())[0][2]
    v_json_path =list(Objects.values())[0][3]

    global v_json_prev_field

    if v_json_field.strip() not in v_json_prev_field:
        v_json_prev_field.append(v_json_field.strip())

        #if v_table_name != 'NA': #'MDM_CUSTOMER_SALESTEAM':
            print(str1)
        if v_json_path.strip() != 'NA':
           str1= "LATERAL FLATTEN(input => {}.value:value ,path => '{}', outer => true) {}".format(v_json_path,v_json_field.replace('_1',''),v_json_field)
        else: 
           str1= "LATERAL FLATTEN(input => src:attributes:" + v_json_field + ", outer => true) {}".format(v_json_field)
    return str1
def build_filter_class(session,Objects):
    v_json_field=list(Objects.keys())[0]
    v_json_value=list(Objects.values())[0][0]
    v_json_dataType=list(Objects.values())[0][1]
    v_json_alias =list(Objects.values())[0][3]

    return "NVL({}.value:ov::string,'true')='true'".format(v_json_field)

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

        #confg_yaml =config_yaml_data
        column_list= config_read["CANONICAL"][interface_name]
    # for my_dec in column_list:
    #     c=list(my_dec.keys())[0]
    #     print(c)
    #     a=(list(my_dec.values())[0])[1]
    #     print(a)

        # print(list(my_dec.items())[0])
        # print((list(my_dec.items())[0])[0])
        # print(((list(my_dec.items())[0])[1])[1])
        #exit(0)
    #select section of unpack sql
        column_unpack_1=[list(my_dec.keys())[0] + ".value:" + (list(my_dec.values())[0])[0]\
                + "::" + (list(my_dec.values())[0])[1] + " as {}".format(list(my_dec.values())[0][2]) for my_dec in column_list if (list(my_dec.values())[0])[0] != 'NA' ]
        column_unpack_1 = "\n,".join(column_unpack_1)
    #lateral flatten section of unpack sql
        flatten_unpack_2 = [build_flatten_class(session,my_dec) for my_dec in column_list if list(my_dec.keys())[0] not in v_json_prev_field ]
        #remove NONE values : 
        flatten_unpack_2=[i for i in flatten_unpack_2 if i is not None]
        flatten_unpack_2="\n,".join(flatten_unpack_2)
        flatten_unpack_2.replace(',--','')         
    #fitler section of unpack sql
        filter_unpack_3=''
        if interface_name == 'MDM_CUSTOMER_MASTER':
            filter_unpack_3 = [build_filter_class(session,my_dec) for my_dec in column_list]
            filter_unpack_3 = "\n AND ".join(filter_unpack_3)
            filter_unpack_3 = "WHERE " + filter_unpack_3
    #build from class of unpack sql
        #from_clause = "FROM {}.{}.{}_VW_STREAMS P".format(database_name,schema_name,interface_name)
        from_clause = "FROM {}.{}.{}_VW_STREAMS P".format('db_naushad','schema_naushad','car_sales1')

    #build full unpack sql
        #unpack_sql = "CREATE OR REPLACE TABLE STG_{} AS SELECT \n {} \n {} ,\n {}  \n  {} \n {}".format(interface_name,column_unpack_1, from_clause, flatten_unpack_2,where_clause,filter_unpack_3)
        unpack_sql = "CREATE OR REPLACE TABLE STG_{} AS SELECT \n {} \n {} ,\n {}  \n {}".format(interface_name,column_unpack_1, from_clause, flatten_unpack_2,filter_unpack_3)
     #create stg tables 
        #session.sql("create or replace table abac_test(empid number)").collect()
        #session.sql(unpack_sql).collect()
    return unpack_sql

#if __name__ == "__main__":
#    #print(get_sql('MDM_CUSTOMER_MASTER'))
#     print(get_sql('MDM_CUSTOMER_MASTER'))
