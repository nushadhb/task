import sys ,io
import yaml

class common_class:
       def __init__(self,project: str, entity: str):
           self.project_name = project
           self.interface_name =  entity #"ENTITIES_HCP"
           self.config_yaml = "config.yaml"

       def get_unpack_sql(self):
           with open(self.config_yaml,"r") as f:
               config_read=yaml.safe_load(f)
               column_list = config_read["CANONICAL"][self.interface_name]
               database_name = config_read["CANONICAL"]["DATABASE_NAME"]
               schema_name = config_read["CANONICAL"]["SCHEMA_NAME"]
               where_clause = ' WHERE "type"=''configuration/entityTypes/HCP'''

               lambda1 = lambda Objects: "LATERAL FLATTEN(input => ""attributes"":" + Objects[
               0] + ", outer => true) {}".format(Objects[0])
               lambda2 = lambda Objects: "NVL({}.value:ov::string,'true')='true'".format(Objects[0])

               unpack_1 = [(list(my_dec.items())[0])[0] + ".value:" + (list(my_dec.items())[0])[1] \
                       + "::string as {}".format((list(my_dec.items())[0])[0]) for my_dec in column_list]
               unpack_1 = ",".join(unpack_1)

               unpack_2 = [lambda1(list(my_dec.items())[0]) for my_dec in column_list]
               unpack_2 = ",".join(unpack_2)

               unpack_3 = [lambda2(list(my_dec.items())[0]) for my_dec in column_list]
               unpack_3 = " AND ".join(unpack_3)
               from_clause = "FROM {}.{}.{}_VW_STREAMS P".format(database_name, schema_name, self.interface_name)

               unpack_sql = "SELECT \n {} \n {} \n {}  \n {} \n {};".format(unpack_1, from_clause, unpack_2, where_clause,unpack_3)
           return unpack_sql

if __name__=="__main__":

    calssOjbect = common_class(project="CANONICAL",entity="ENTITIES_HCP")

    for i in range(20*5):
        print("-",end='')
    print("\n")
    print(calssOjbect.get_unpack_sql())
