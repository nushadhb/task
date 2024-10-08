
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
Dea_list=['ExpirationDate','Status']
Geo_list=['GeoAccuracy','Latitude','Longitude','HCOAddressScore','PreferredPhysicalAddressFlag','Premise','StateProvince',\
          'Street','SubAdministrativeArea','VerificationStatus','VerificationStatusDetails',\
         'Zip', ]
zip_list=['Zip4','Zip5']

def cust_address(session,config_file,interface_name):
    pass
# def build_customer_master(Objects):
#
#     if Objects[0] in address_list:
#         str = "LATERAL FLATTEN(input => Adddress.value:value ,path => {}, outer => true) {}".format(Objects[0],Objects[0])
#         print(str)
#         exit(0)
#     if Objects[0] in Dea_list:
#         str = "LATERAL FLATTEN(input => DEA.value:value ,path => {}, outer => true) {}".format(Objects[0],Objects[0])
#     if Objects[0] in Geo_list:
#         str = "LATERAL FLATTEN(input => GeoLocation.value:value ,path => {}, outer => true) {}".format(Objects[0], Objects[0])
#     return str

def build_flatten_class(session,Objects) -> str:
    str1=''
    #return "LATERAL FLATTEN(input => ""attributes"":" + Objects[0] + ", outer => true) {}".format(Objects[0])
    if Objects[0].strip() in Address_list:

        str1 = "LATERAL FLATTEN(input => Address.value:value ,path => '{}', outer => true) {}".format(Objects[0], Objects[0])
        print(str1)
        # exit(0)
    elif Objects[0] in Dea_list:
        print(str1)
        str1 = "LATERAL FLATTEN(input => DEA.value:value ,path => '{}', outer => true) {}".format(Objects[0], Objects[0])
    elif Objects[0] in Geo_list:
        print(str1)
        str1 = "LATERAL FLATTEN(input => GeoLocation.value:value ,path => '{}', outer => true) {}".format(Objects[0], Objects[0])
    elif Objects[0] in zip_list:
        print(str1)
        str1 = "LATERAL FLATTEN(input => Zip.value:value ,path => '{}', outer => true) {}".format(Objects[0], Objects[0])
    else:
        str1 ="LATERAL FLATTEN(input => ""attributes"":" + Objects[0] + ", outer => true) {}".format(Objects[0])
    return str1
def build_filter_class(session,Objects):
    return "NVL({}.value:ov::string,'true')='true'".format(Objects[0])

def get_sql(session,interface_name: str):
    scope_url = session.sql("select BUILD_SCOPED_FILE_URL(@STGS3, 'project_config.yml') as sc_url")
    scope_url= scope_url.select("sc_url").collect()
    sc_url= scope_url[0][0]
    with SnowflakeFile.open(sc_url) as f:
        config_read=yaml.safe_load(f)
        column_list = config_read["CANONICAL"][interface_name]
        database_name = config_read["CANONICAL"]["DATABASE_NAME"]
        schema_name = config_read["CANONICAL"]["SCHEMA_NAME"]
        where_clause = ' WHERE "type"=''configuration/entityTypes/HCP'''

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
        unpack_1=[list(my_dec.keys())[0] + ".value:" + (list(my_dec.values())[0])[0]\
                + "::" + (list(my_dec.values())[0])[1] + " as {}".format(list(my_dec.keys())[0]) for my_dec in column_list ]
        unpack_1 = ",".join(unpack_1)
    #lateral flatten section of unpack sql
        unpack_2 = [build_flatten_class(session,list(my_dec.keys())) for my_dec in column_list]
        unpack_2=",".join(unpack_2)
                 
    #fitler section of unpack sql
        unpack_3=None
        if interface_name == 'MDM_CUSTOMER_MASTER':
            unpack_3 = [build_filter_class(session,list(my_dec.items())[0]) for my_dec in column_list]
            unpack_3 = " AND ".join(unpack_3)

    #build from class of unpack sql
        from_clause = "FROM {}.{}.{}_VW_STREAMS P".format(database_name,schema_name,interface_name)

    #build full unpack sql
        unpack_sql = "CREATE OR REPLACE TABLE STG_{} AS SELECT \n {} \n {} ,\n {}  \n  {} \n {}".format(interface_name,unpack_1, from_clause, unpack_2,where_clause,unpack_3)
     #create stg tables 
     sesssion.sql(unpack_sql).collect 
    return unpack_sql

#if __name__ == "__main__":
#    #print(get_sql('MDM_CUSTOMER_MASTER'))
#     print(get_sql('MDM_CUSTOMER_MASTER'))
