import pandas as pd
import requests
import snowflake.connector as snow
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
  tables = ['Users','External_User_Identifiers']

  for table in tables:
    columns = []
    conn = psycopg2.connect(
        database="dbj52iqdmgels",
        user="analyst",
        password="p3a48decddee14fcd236b7ca8f61593620ade69f239827f07469e8f37bdf40564",
        host="ec2-34-235-198-233.compute-1.amazonaws.com",
        port='5432'
    )
    
    curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    conn_write = snow.connect(user="PROD_USER",
    password="F32fb415-478e-4be7-9884-8f59f9adb11b",
    account="cv52121.us-central1.gcp",
    warehouse="VET_DATA_WAREHOUSE",
    database="VET_DB",
    schema="VET_SCHEMA")

    cur_write = conn_write.cursor()

    sql = "USE ROLE ACCOUNTADMIN"
    cur_write.execute(sql)

    sql = "USE WAREHOUSE VET_DATA_WAREHOUSE"
    cur_write.execute(sql)

    sql = "USE DATABASE VET_DB"
    cur_write.execute(sql)

    sql = "USE SCHEMA VET_SCHEMA"
    cur_write.execute(sql)

    sql = f'DELETE FROM {str.upper(table)}_TEMP_{str.upper(env)}'
    cur_write.execute(sql)
    
    top=50000
    i=0
    days = 2

    df = pd.DataFrame()
    result = pd.DataFrame()
    while((i==0) or (len(df)==top)):
      start_time = (dt.datetime.now()-timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
      sql=f'SELECT * FROM {str.upper(table)} WHERE INSERTED_AT > \'{start_time}\' OR UPDATED_AT > \'{start_time}\' LIMIT {top} OFFSET {i}'	
      curs.execute(sql)
      data = curs.fetchall()
      df=pd.DataFrame([i.copy() for i in data])
      mem_df = df.memory_usage(index=True).sum()/1000000
      result = result.append(df,ignore_index=True)
      mem_res = result.memory_usage(index=True).sum()/1000000
      if ((mem_df+mem_res)>16) or (len(df)<top):
          df_to_sf = result
          df_to_sf.columns = df_to_sf.columns.str.upper()
          columns.extend(df_to_sf.columns.tolist())
          if len(result):
            print(f'Loading last {days} days from {str.upper(table)} to SF')
            df_to_sf['UPDATED_AT'],df_to_sf['INSERTED_AT']  = df_to_sf['UPDATED_AT'].astype(str),df_to_sf['INSERTED_AT'].astype(str)
            write_pandas(conn_write, df_to_sf, str.upper(table) + f'_TEMP_{str.upper(env)}')
          result = pd.DataFrame()
      i += top

    keys = ['ID','UPDATED_AT']
    key_columns = ','.join(keys)
    if columns:
      fields = list(set(columns))
      field_columns = ','.join(fields)
      merge_string2 = f'MERGE INTO {str.upper(table)}_{str.upper(env)} USING {str.upper(table)}_TEMP_{str.upper(env)} ON '+' AND '.join(f'{str.upper(table)}_{str.upper(env)}.{x}={str.upper(table)}_TEMP_{str.upper(env)}.{x}' for x in keys) + f' WHEN NOT MATCHED THEN INSERT ({field_columns}) VALUES ' +  '(' + ','.join(f'{str.upper(table)}_TEMP_{str.upper(env)}.{x}' for x in fields) + ')'

      sql = merge_string2
      cur_write.execute(sql)
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