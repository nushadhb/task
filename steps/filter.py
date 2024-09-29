from snowflake.snowpark.functions import col
from snowflake.snowpark.files import SnowflakeFile

import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import pandas as pd
import snowflake.snowpark.functions as F
import yaml
import common_class as comm_c

def filter_by_role(session, table_name, role):
  df = session.table(table_name)
  return df.filter(col("role") == role)

def filter_by_territory(session,table_name,territory):
  df = session.table(table_name)
  return df.filter(col("territory") == territory)

def get_unpack_sql(session,project_name,interface_name):
    #config_yaml=project_config.yml
    #config_yaml = snowflake.snowpark.files.SnowflakeFile.open
    #config_yaml= '@DB_NAUSHAD.SCHEMA_NAUSHAD.SNOWFLAKE_GIT_PYTHONCODE/branches/main/steps/project_config.yml'
    #with open(config_yaml,"r") as f:
    scope_url = session.sql("select BUILD_SCOPED_FILE_URL(@STGS3, 'project_config.yml') as sc_url")
    scope_url= scope_url.select("sc_url").collect()
    sc_url= scope_url[0][0]
    with  SnowflakeFile.open(sc_url) as f: 
        config_read=yaml.safe_load(f)
        column_list = config_read["CANONICAL"][interface_name]
        database_name = config_read["CANONICAL"]["DATABASE_NAME"]
        schema_name = config_read["CANONICAL"]["SCHEMA_NAME"]
        where_clause = ' WHERE "type"=''configuration/entityTypes/HCP'''

        lambda1 = lambda Objects: "LATERAL FLATTEN(input => ""attributes"":" + Objects[0] + ", outer => true) {}".format(Objects[0])
        lambda2 = lambda Objects: "NVL({}.value:ov::string,'true')='true'".format(Objects[0])

        unpack_1 = [(list(my_dec.items())[0])[0] + ".value:" + (list(my_dec.items())[0])[1] \
                    + "::string as {}".format((list(my_dec.items())[0])[0]) for my_dec in column_list]
        unpack_1 = ",".join(unpack_1)

        unpack_2 = [lambda1(list(my_dec.items())[0]) for my_dec in column_list]
        unpack_2 = ",".join(unpack_2)

        unpack_3 = [lambda2(list(my_dec.items())[0]) for my_dec in column_list]
        unpack_3 = " AND ".join(unpack_3)
        from_clause = "FROM {}.{}.{}_VW_STREAMS P".format(database_name, schema_name, interface_name)

        unpack_sql = "SELECT \n {} \n {} \n {}  \n {} \n {};".format(unpack_1, from_clause, unpack_2, where_clause,unpack_3)
        return unpack_sql

