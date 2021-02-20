import pandas as pd
import numpy as np
import requests
import re
import snowflake.connector as snow
from snowflake.connector.errors import DatabaseError, ProgrammingError
from snowflake.connector.pandas_tools import write_pandas
import datetime as dt
import psycopg2
import psycopg2.extras
from datetime import timedelta
from flask import Flask
app = Flask(__name__)

@app.route('/')
@app.route('/task/aws_loader')
def load_aws():
  env = 'PROD'#
  tables = ['Vet_Data_Emails','Vet_Data_Clients','Invoices','Vet_Support_Usages','Telehealth_Payments','Plan_Changes','Promo_Codes','User_Promotion_Claims','Vet_Data_Patients','Plans','Clinics','Pets','Promotions','User_Checklist_Group_Records','User_Checklist_Item_Records','Pet_Checklist_Group_Records','Pet_Checklist_Item_Records','Claims','Rewards','Withdrawals','User_Promotions','Users','External_User_Identifiers']
  no_ins_tables = ['User_Promotion_Claims']
  sub_col = ['DATETIME','DEACTIVATED_AT','OCCURRED_ON','EXPIRY','START_DATE','END_DATE','DATE_OF_BIRTH','DELETED_AT','SOURCE_CREATED_AT','DATE_OF_DEATH','FIRST_VISIT_DATE','LAST_TRANSACTION_DATE','SOURCE_UPDATED_AT','SOURCE_REMOVED_AT']

  for table in tables:
    columns = []
    conn = psycopg2.connect(
        database="dbj52iqdmgels",
        user="analyst",
        password="p3a48decddee14fcd236b7ca8f61593620ade69f239827f07469e8f37bdf40564",
        host="ec2-34-235-198-233.compute-1.amazonaws.com",
        port='5432'
    )

    def get_field_types(table,column):
      sql='''
      select table_name,UPPER(column_name) AS column_name,ordinal_position,data_type,character_maximum_length,
      CASE
      WHEN DATA_TYPE LIKE 'ARRAY' THEN 'ARRAY'
      WHEN UDT_NAME LIKE 'int%' OR UDT_NAME LIKE '_int%' THEN 'INTEGER'
      WHEN (UDT_NAME LIKE '%text' OR UDT_NAME='varchar' OR UDT_NAME LIKE '%status%') AND CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN CONCAT('VARCHAR (',CHARACTER_MAXIMUM_LENGTH,')')
      WHEN UDT_NAME='bool' THEN 'BOOLEAN'
      WHEN UDT_NAME='timestamp' THEN 'TIMESTAMP'
      WHEN UDT_NAME LIKE 'float%' THEN 'FLOAT'
      ELSE 'VARCHAR (10000)'
      END AS TYPE
      from information_schema.columns
      where table_name = '<<<Table>>>'
      and column_name = '<<<Column>>>'
      '''
      sql_mod = sql.replace('<<<Table>>>',table.lower()).replace('<<<Column>>>',column.lower())
      curs.execute(sql_mod)
      data = curs.fetchall()
      df=pd.DataFrame([i.copy() for i in data])
      return df['type'].values[0]

    def try_write(sqlMethod,temp=False,missing_fields=[]):
      j=0
      temps=''
      if temp:
        temps='TEMP_'
      max_columns = 50
      while j<max_columns:
        try:
          if temp:
            conn_write,df,table_temp = sqlMethod[0],sqlMethod[1],sqlMethod[2]
            write_pandas(conn_write, df,table_temp)
            print('Successfully written to Temp.')
          else:
            tablex,envx,field_columnsx,keysx,fieldsx=sqlMethod[0],sqlMethod[1],sqlMethod[2],sqlMethod[3],sqlMethod[4]
            keysx.extend(missing_fields)
            keysx = list(set(keysx))
            merge_string = f'MERGE INTO {str.upper(tablex)}_{str.upper(envx)} USING {str.upper(tablex)}_TEMP_{str.upper(envx)} ON '+' AND '.join(f'{str.upper(tablex)}_{str.upper(envx)}.{x}={str.upper(tablex)}_TEMP_{str.upper(envx)}.{x}' for x in keysx) + f' WHEN NOT MATCHED THEN INSERT ({field_columnsx}) VALUES ' +  '(' + ','.join(f'{str.upper(tablex)}_TEMP_{str.upper(envx)}.{x}' for x in fieldsx) + ')'
            cur_write.execute(merge_string)
            print('Successfully merged unique rows.')
          j=max_columns
        except DatabaseError as db_ex:
          if db_ex.errno == 904:
            print(db_ex.msg)
            field = re.search('\'(.*?)\'',db_ex.msg).group(0).strip('\'')
            missing_fields.append(field)
            # if field in fields:
            type=get_field_types(table,field)
            sql2=f'ALTER TABLE {str.upper(table)}_{str.upper(temps)}{str.upper(env)} ADD {str.upper(field)} {str.upper(type)}'
            try:
              cur_write.execute(sql2)
              print(f'{str.upper(field)} added to {str.upper(table)}_{str.upper(temps)}{str.upper(env)} as {str.upper(type)}.')
            except DatabaseError as db_ex:
              print(db_ex.msg)
          j+=1
      return missing_fields
    
    curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    conn_write = snow.connect(user="PROD_USER",
    password="F32fb415-478e-4be7-9884-8f59f9adb11b",
    account="cv52121.us-central1.gcp",
    warehouse="VET_DATA_WAREHOUSE",
    database="VET_DB",
    schema="AWS_SCHEMA")

    cur_write = conn_write.cursor()

    sql = "USE ROLE ACCOUNTADMIN"
    cur_write.execute(sql)

    sql = "USE WAREHOUSE VET_DATA_WAREHOUSE"
    cur_write.execute(sql)

    sql = "USE DATABASE VET_DB"
    cur_write.execute(sql)

    sql = "USE SCHEMA AWS_SCHEMA"
    cur_write.execute(sql)

    sql = f'DELETE FROM {str.upper(table)}_TEMP_{str.upper(env)}'
    cur_write.execute(sql)

    top=50000
    i=0

    days = 2
    missing_fields = []
    df = pd.DataFrame()
    result = pd.DataFrame()
    while((i==0) or (len(df)==top)):
      start_time = (dt.datetime.now()-timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
      if table in no_ins_tables:
        sql=f'SELECT * FROM {str.upper(table)} LIMIT {top} OFFSET {i}'	
      else:
        sql=f'SELECT * FROM {str.upper(table)} WHERE INSERTED_AT >= \'{start_time}\' OR UPDATED_AT >= \'{start_time}\' LIMIT {top} OFFSET {i}'	
      curs.execute(sql)
      data = curs.fetchall()
      df=pd.DataFrame([i.copy() for i in data])
      result = result.append(df,ignore_index=True)
      df_to_sf = result
      df_to_sf.columns = df_to_sf.columns.str.upper()
      columns.extend(df_to_sf.columns.tolist())
      if len(result):
        if table not in no_ins_tables:
          print(f'Loading last {days} days from {str.upper(table)} to SF, rows {i} to {top+i}.')
          df_to_sf['UPDATED_AT'],df_to_sf['INSERTED_AT']  = df_to_sf['UPDATED_AT'].astype(str),df_to_sf['INSERTED_AT'].astype(str)
        else:
          print(f'Loading from {str.upper(table)} to SF, rows {i} to {top+i}.')
        for col in list(set(sub_col) & set(df_to_sf.columns)):
          df_to_sf[col] = df_to_sf[col].fillna('sub').astype(str).replace('sub',np.nan)
        missing_fields = try_write([conn_write, df_to_sf, str.upper(table) + f'_TEMP_{str.upper(env)}'],True)
      result = pd.DataFrame()
      i += top

    if table in no_ins_tables:
      keys = ['ID']
    else:
      keys = ['ID','UPDATED_AT']
    keys.extend(missing_fields)
    if columns:
      fields = list(set(columns))
      field_columns = ','.join(fields)
      try_write([table,env,field_columns,keys,fields])
      if 'UPDATED_AT' in keys:
        unique_constraint = f'DELETE FROM {str.upper(table)}_{str.upper(env)} T USING (SELECT ID,UPDATED_AT,ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATED_AT DESC) AS RANK_IN_KEY FROM {str.upper(table)}_{str.upper(env)} T) X WHERE X.RANK_IN_KEY <> 1 AND T.ID = X.ID AND T.UPDATED_AT = X.UPDATED_AT'
        cur_write.execute(unique_constraint)
        print(f'Older ID\'s in {str.upper(table)}_{str.upper(env)} have been cleaned up')
    else:
      print(f'No data to load for Table: {str.upper(table)}')

    sql = f'DELETE FROM {str.upper(table)}_TEMP_{str.upper(env)}'
    cur_write.execute(sql)

    sql = "ALTER WAREHOUSE VET_DATA_WAREHOUSE SUSPEND"
    cur_write.execute(sql)

    cur_write.close()
    conn_write.close()
    
  return 'Success'

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)