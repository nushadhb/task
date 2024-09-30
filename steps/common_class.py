
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import pandas as pd
import snowflake.snowpark.functions as F
import yaml
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import col

class common_class():
       config_yaml = "project_config.yml"
       interface_name = "ENTITIES_HCP"
       #from snowflake.snowpark import Session
       def __init__(session,self,project: str, entity: str):
           self.project_name = project
           self.interface_name =  entity #"ENTITIES_HCP"

       @staticmethod
       def get_scope_rule_df(json_entity,session):
           if json_entity == "ENTITIES_HCP":
               scope_url_df = session.sql("select BUILD_SCOPED_FILE_URL(@STGS3, 'project_config.yml') as sc_url")
           return scope_url_df
           
       @classmethod   
       def get_scope_url(cls,session,json_entity):
           #select_sql = "select BUILD_SCOPED_FILE_URL({}, {}) as sc_url;".format('@STGS3',config_yaml)
           scope_url_df = cls.get_scope_rule_df(json_entity,session)
           #scope_url = session.sql("select BUILD_SCOPED_FILE_URL(@STGS3, 'project_config.yml') as sc_url")
           scope_url= scope_url_df.select("sc_url").collect()
           sc_url= scope_url[0][0]
           return sc_url
       @classmethod
       def get_unpack_sql(cls,session,json_entity):
           sc_url= cls.get_scope_url(session,json_entity)
           with SnowflakeFile.open(sc_url) as f:
               config_read=yaml.safe_load(f)
               column_list = config_read["CANONICAL"][cls.interface_name]
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
               from_clause = "FROM {}.{}.{}_VW_STREAMS P".format(database_name, schema_name, cls.interface_name)

               unpack_sql = "SELECT \n {} \n {} \n {}  \n {} \n {};".format(unpack_1, from_clause, unpack_2, where_clause,unpack_3)
           return unpack_sql
       
def main(session): 
    # # Your code goes here, inside the "main" handler.
    tableName = 'information_schema.packages'
    dataframe = session.table(tableName).filter(col("language") == 'python')

    # # Print a sample of the dataframe to standard output.
    # dataframe.show()
    calssOjbect = common_class(session,project="CANONICAL",entity="ENTITIES_HCP")
    common_class.interface_name = "ENTITIES_HCP"
    print(calssOjbect.get_unpack_sql(session,'ENTITIES_HCP'))
   
    return dataframe
        
