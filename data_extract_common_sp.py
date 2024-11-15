
"""

File name:  MDM_CANONICAL_JSON_UNPACK.py

Author: Naushad Baig, Snowflake Solution Architect, Cognizant Technoloyg solutions

Created: Nov,2024

Version: 1.0

Description: common framework to generate the vendor extracts

"""

from snowflake.snowpark.functions import col

from snowflake.snowpark.files import SnowflakeFile

from snowflake.snowpark.types import StringType 

import snowflake.snowpark as snowpark

from snowflake.snowpark import Session

from snowflake.snowpark.functions import col

#import pandas as pd

import snowflake.snowpark.functions as F

import yaml

import sys

import json

from snowflake.snowpark.functions import lit

from io import StringIO

import io

#import modin.pandas as pd

# Import the Snowpark plugin for modin.

#import snowflake.snowpark.modin.plugin

v_extract_to_location_str='@mdm.mdm_canonical_local_stg/align/vendor_extract/code/config/'

v_stage_str=''

def set_context(session) -> str:

    global v_stage_str

    config_file_yml_path = '@mdm.mdm_canonical_local_stg/align/code/config/mdm_vendor_extact_config.yml'

    #config_file_yml_path =r"C:\Abbvie\other\other\mdm_vendor_extact_config.yml"

    #read config file

    fd = session.file.get_stream(config_file_yml_path, decompress=False)

    res =  fd.read()

    fd.close()

    config_read=yaml.safe_load(res)  

    column_list = config_read["CANONICAL-ALIGN"]

    #set context

    session.use_role(column_list['ROLE_NAME'])

    session.use_database(column_list['DATABASE_NAME'])

    session.use_schema(column_list['SCHEMA_NAME'])

    session.use_warehouse((column_list['warehouse'])['XSMALL'])

    v_stage_str=column_list['stage_name']

    config_table=column_list['config_table']

    return v_stage_str +',' + config_table

    # print(column_list)

    # return column_list

def getthe_extract_sequence(session,args) -> str:

    sequence =[val for row in args for val in row]   # [row('filed'=val)]

    return list(set(sequence ))

# def map_copy_format_options(session,row_field_value:StopIteration) -> str:

#     for key, val in  file_formate_option.items():

#         if row_field_value == val:

#             return key

def delete_imt_previous_data(session,*args) -> str:

    #delte the current data from IMT table  if found duplicate for current run

    for unpack in args:

        if unpack[0].upper()=='DELTA':  # do only for deta extract

            extract_id=unpack[1]

            group_name = unpack[2]

            #delete previous data for group

            v_DML_sql_str = "DELETE FROM ALIGN.ALIGN_EXTRACT_IMT where EXTRACT_ID={EXTRACT_ID}  AND BATCH_STATUS = 'CURRENT'".format(EXTRACT_ID=extract_id)

            del_output=session.sql(v_DML_sql_str).collect()

            log_insert=session.sql("call mdm.mdm_insert_to_process_log_sp(batch_id=>0,run_step=>'vendor-extract',rows_affect=>'{results}',\

                        business_name=>'vendor_extract',interface_name=>'{group_nm}',query_txt=>'current data deleted')".format(group_nm=group_name,results=del_output)).collect()

def update_imt_current_data(session,*args) -> str:

    for unpack in args:

        if unpack[0].upper()=='DELTA':  # do only for deta extract

            extract_id=unpack[1]

            group_name = unpack[2]

            #delete previous data from IMT table

            #delete previous data for group

            v_DML_sql_str = "DELETE FROM ALIGN.ALIGN_EXTRACT_IMT where EXTRACT_ID={EXTRACT_ID}  AND BATCH_STATUS = 'PREVIOUS'".format(EXTRACT_ID=extract_id)

            del_output=session.sql(v_DML_sql_str).collect()

            log_insert=session.sql("call mdm.mdm_insert_to_process_log_sp(batch_id=>0,run_step=>'vendor-extract',rows_affect=>'{results}',\

                        business_name=>'vendor_extract',interface_name=>'{group_nm}',query_txt=>'Previous data deleted')".format(group_nm=group_name,results=del_output)).collect()

            #update current data to previous for group

            v_DML_sql_str = "Update ALIGN.ALIGN_EXTRACT_IMT set BATCH_STATUS='PREVIOUS' where EXTRACT_ID={EXTRACT_ID}  AND BATCH_STATUS = 'CURRENT'".format(EXTRACT_ID=extract_id)

            del_output=session.sql(v_DML_sql_str).collect()

            log_insert=session.sql("call mdm.mdm_insert_to_process_log_sp(batch_id=>0,run_step=>'vendor-extract',rows_affect=>'{results}',\

                        business_name=>'vendor_extract',interface_name=>'{group_nm}',query_txt=>'current data updated to previous set')".format(group_nm=group_name,results=del_output)).collect()

 

def build_extract_file_name(session,*args) -> str:    

    v_args=args

def write_control_details(session,controls) -> None:

    v_control_file_formate_option_dic={}

   # v_control_file_formate_option_dic["FIELD_DELIMITER"] =

    v_control_file_formate_option_dic["COMPRESSION"]="NONE"

    v_control_file_formate_option_dic["TYPE"] ='CSV'

   

    df_control=session.create_dataframe(controls)

    df_control=df_control.select(col("control_file_path"),col("details"),col("Author"))

    for row in df_control.to_local_iterator():

        df_write_control=session.create_dataframe([row])

        # return row

        f_path=row["CONTROL_FILE_PATH"]

        #return f_path

        #res=df_write_control.write.copy_into_location(f_path,file_format_type='CSV',header=False,overwrite=True,single=True)

        res=df_write_control.write.copy_into_location(f_path,format_type_options=v_control_file_formate_option_dic,header=False,overwrite=True,single=True)

   

def write_control_file (session,kwargs) -> None:

   

    for key,val in kwargs.items():

        v_copy_path=val[0]['file_path']

        stream_write=str(val[0]['control_result'])

        #data = io.StringIO()

        my_string='This goes into the read buffer.'

        input = my_string.encode(encoding="utf-8")

        text_buffer = io.StringIO('hello')

        read_content=text_buffer.getvalue()

   

        # #io.StringIO(b'jlkfjsklfj')

        #stream_str = io.StringIO("JournalDev Python: hello")

        stg_path='@ABBV_SMDW_DEV.MDM.MDM_CANONICAL_LOCAL_STG/align/vendor_extract/data/abc.control'

        stg_path='@~/abc.txt'

        #stg_path='@"ABBV_SMDW_DEV"."MDM"."MDM_CANONICAL_LOCAL_STG"/align/vendor_extract/data/CGI/Align_Extracts/keystone_file_None.txt'

        res = session.file.put_stream(stage_location=stg_path, input_stream=text_buffer.seek(0), auto_compress =False,overwrite =True)

        #res = session.file.put_stream(input_stream=stream_str.getvalue(), stage_location=v_copy_path, auto_compress =False,overwrite =True)       

def prepare_extract_batch_in_order(session,df) -> str:

    global v_stage_str

    #reset lists and dics

    v_file_formate_option_dic={}

    v_file_header_option_list=[]

    v_file_copy_path_list=[]

    v_copy_format_option_list=[]

    v_file_path_list=[]

    v_control_file_dic={}

    v_group_name_list=[]

    v_control_file_dic={}

    seq =0

    v_control_file_list=[]

    v_stage_file_control_path_list=[]

    v_delete_update_imt_list=()

    v_delete_update_imt_list1=[]

    v_delete_update_imt_list2=[]

    #--->

    #get the copy of  dfs for all extract sql to run  in parallel

    dfs = [session.sql(row["EXTRACT_QUERY"]) for row in df.to_local_iterator()]

    #--->

    #insrto to imt table for delta extract

    # temp=lambda IMT_QUERY: session.sql(IMT_QUERY).collect()

    # dlete the current data from IMT table  in case of rerun

    dfs_imt = [delete_imt_previous_data(session,tuple([row["EXTRACT_TYPE"],row["EXTRACT_ID"],row["GROUP_NAME"]])) for row in df.to_local_iterator() if row["EXTRACT_TYPE"].upper()=='DELTA']    

    # dfs_imt = [temp row["IMT_QUERY"] for row in dfs.to_local_iterator() if row["EXTRACT_TYPE"].upper()=="DELTA" and row["EXTRACT_QUERY"] !=None]

    #insert the current data to imt table

    dfs_imt = [session.sql(row["IMT_QUERY"]).collect() for row in df.to_local_iterator() if row["EXTRACT_TYPE"].upper()=='DELTA']    

    # for row in df.to_local_iterator():

    #     if row["EXTRACT_TYPE"].upper()=='DELTA': #and row["EXTRACT_QUERY"] !='':

    #         imt_sql= row["IMT_QUERY"]  

    #         dfs_imt = session.sql(imt_sql).collect()

    #--->

    #get the file copy options , file format options and stage path to copy the data

    for row in df.to_local_iterator():

        #{ k:v for (k,v) in zip(keys, values)}  

        group_name =row["GROUP_NAME"]

        extract_id=row["EXTRACT_ID"]

        extract_type=row["EXTRACT_TYPE"]

        v_group_name_list.append(group_name)

        file_tgt_pth=row["FILE_TGT_PTH"]

        file_name=row["FILE_NAME"]

        file_extension=row["FILE_EXTN"]

        v_date_format = row["DATE_FORMAT"]

        v_date_format = "select case when '{0}' != ''  then to_char(current_timestamp,'{0}') else 'NA' end".format(v_date_format)

        #v_date_format = "select coalesce('{date_format}','NA')".format(date_format=v_date_format)

        v_date_format =session.sql(v_date_format).collect()[0][0]

        #file_extension1=  file_tgt_pth + "." + file_extension if row["DATE_FORMAT"]  is None else file_tgt_pth +"_"+ row["DATE_FORMAT"]  + "." + file_extension

        file_extension1=  file_tgt_pth + "." + file_extension if v_date_format  is 'NA' else file_tgt_pth +"_"+ v_date_format + "." + file_extension

        file_extension1=file_extension1.replace('_None','')

        #File format options ( when field_optionally_enclosed_by=None then empty_field_as_null should be set to False)

        v_file_formate_option_dic["FIELD_OPTIONALLY_ENCLOSED_BY"] =  None if row["OPTIONAL_FIELD_ENCLOSED"]  !='"' else row["OPTIONAL_FIELD_ENCLOSED"]

        v_file_formate_option_dic["EMPTY_FIELD_AS_NULL"]=False if row["OPTIONAL_FIELD_ENCLOSED"] !='"' else True

        v_file_formate_option_dic["NULL_IF"]=('')

        #v_file_formate_option_dic["EMPTY_FIELD_AS_NULL"]=True

        v_file_formate_option_dic["FIELD_DELIMITER"] = row["FIELD_DELIMITER"]

        v_file_formate_option_dic["COMPRESSION"]="NONE"

        v_file_formate_option_dic["FILE_EXTENSION"] = row["FILE_EXTN"]

        v_file_formate_option_dic["TYPE"] ='CSV'

       

        #build full targret file path in stage

        #-->

        v_stage_str1 ="select concat('{}','{}')".format(v_stage_str,file_extension1)

        v_stage_str1 =session.sql(v_stage_str1).collect()

        v_stage_str1=v_stage_str1[0][0]  

        #-->
        #-->

        #build file control path

        v_stage_file_control_path = v_stage_str + file_tgt_pth

        #-->

        #file copy option

        Header_flg = True if row["FILE_HEADER"]=='Y' and row["FIELD_DELIMITER"] in [',','|','\t']  else False

        #-->

        #add the copy option to lists

        v_file_header_option_list.append(Header_flg)

        v_file_copy_path_list.append(v_stage_str1)  # this variable holds the target file copy path

        v_copy_format_option_list.append(v_file_formate_option_dic)

        v_stage_file_control_path_list.append(v_stage_file_control_path)

        #-->

        #IMT detelete and update prepare

        v_delete_update_imt_list=extract_type,extract_id,group_name

        v_delete_update_imt_list1.append(v_delete_update_imt_list)

    #rasync_jobs=[df.collect_nowait() for df in dfs]

    dfs_asynch_final_list =zip(dfs,v_copy_format_option_list,v_file_copy_path_list,v_file_header_option_list,v_group_name_list,v_stage_file_control_path_list,v_delete_update_imt_list1)

    for  asyncjobs ,format_option,copy_full_path,header_option ,group_name_1,control_path_1,imt_delete_update in dfs_asynch_final_list:

        seq +=1

        #first imt delete for delta extract if found extract type is DELTA:

        #delete IMT data

        #v_delete_update_imt_list2.append(tuple(imt_delete_update))

        #delete_imt_previous_data(session,tuple(imt_delete_update))

        res=asyncjobs.write.copy_into_location(copy_full_path, format_type_options=format_option ,header=header_option,block=False,DETAILED_OUTPUT=True,single=True,overwrite=True,MAX_FILE_SIZE =5368709120).result()

        if res:

            res=res[0]["ROW_COUNT"]  # only row count

            #res=res  # other extract details filen name,rowcunt etc

        v_control_file_dic[group_name_1 +'_'+ str(seq)]=[{"file_path":copy_full_path,"control_result":res,"Author":'Snowflake bulk Download'}]

        row1=snowpark.Row(group_nm=group_name_1,control_file_path=control_path_1 +'.control',details=res,Author='Snowflake bulk download')

        v_control_file_list.append(row1)

        #row=write_control_details(session,v_control_file_list)

        #insert to process log :

        log_insert="call mdm.mdm_insert_to_process_log_sp(batch_id=>0,run_step=>'vendor-extract',rows_affect=>'{results}',\

                    business_name=>'vendor_extract',interface_name=>'{group_name_2}',query_txt=>'STORED PROCEDURE CALL=VENDOR_EXTRACT_SP')".format(group_name_2=group_name_1,results=res)

        log_ins=session.sql(log_insert).collect()

        #delete IMT data

        update_imt_current_data(session,tuple(imt_delete_update))

    row=write_control_details(session,v_control_file_list)

    return dfs_imt #imt_delete_update #v_control_file_list #v_control_file_dic #res #res #res#res[0]['ROW_COUNT'] 

def generate_extract(session,vendor_name: str) -> str:

    #global v_stage_str

    v_stage_str,config_table = set_context(session).split(',')

    exract_status=''

    v_sql_select_str= "select *, concat_ws('/',group_name,TGT_FILE_LOCATION,ifnull(file_name,''),\

                     ifnull(date_format,'')) as file_tgt_pth from ALIGN.{} where GROUP_NAME='{}' and EXTRACT_ACTIVE_FLAG='Y'".format(config_table,vendor_name)

    v_sql_select_str= "select *, concat_ws('/',group_name,TGT_FILE_LOCATION,ifnull(file_name,'')) as file_tgt_pth from ALIGN.{} where GROUP_NAME='{}' and EXTRACT_ACTIVE_FLAG='Y'".format(config_table,vendor_name)

    # df=session.table(config_table).filter( (col("GROUP_NAME")== vendor_name) & (col("EXTRACT_ACTIVE_FLAG")=='Y'))\

    #             .select(col("EXTRACT_ID"),col("GROUP_NAME"),col("FILE_NAME"),col("EXTRACT_TYPE"),col("FILE_EXTN"),col("FILE_HEADER")\

    #             ,col("FIELD_DELIMITER"),col("EXTRACT_QUERY"),col("TGT_FILE_LOCATION"),col("EXTRACT_SEQUENCE"),col("DATE_FORMAT"))

    df=session.sql(v_sql_select_str)

    v_extract_squence_list =df.select(col("EXTRACT_SEQUENCE")).collect()

    v_extract_sque_list = getthe_extract_sequence(session,v_extract_squence_list)   

    #generate extract in asynchronously for each  seqence order

    for  seq in v_extract_sque_list:

        df_generate_ext = df.filter((col("EXTRACT_SEQUENCE")==seq))  # pull the records order sequence asc

        exract_status=prepare_extract_batch_in_order(session,df_generate_ext)

        #for row in df.to_local_iterator():

    return exract_status

# if __name__ == "__main__":

#     with Session.builder.getOrCreate() as session:

#         main(session)
