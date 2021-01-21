import pandas as pd
import requests
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
import datetime as dt
from datetime import timedelta
from flask import Flask
import time
import socket

app = Flask(__name__)

@app.route('/_ah/warmup')
def doNothing():
  return 'WarmedUp'

@app.route('/')
@app.route('/task/loader')
def sfLibraries():
  url = r'https://api.vetdata.net/InstallationList'
  user = 'futurepet'
  passw = 'f32fb415-478e-4be7-9884-8f59f9adb11b'
  env = 'PROD'#'STG'
  tables = ['PatientWellnessPlans','WellnessPlans','Invoices','Transactions','Clients','Patients','Codes']

  def get_installations(url,user,passw):
      r = requests.get(url,auth=(user,passw))
      installs = [i['InstallationId'] for i in r.json()]
      return installs

  def make_url(mode,header,table,days,user,passw):
    url = f'https://api.vetdata.net/v2/{table}?$filter=APIRemovedDate eq null'
    instal = header['Installation']
    if mode == "all":
      url2 = url
      print(f'Reloading table: {table}, installation: {instal}')
    if mode == "latest":
      print(f'Getting latest {days} days for Table: {table}, Installation: {instal}')
      dt1 = dt.datetime.now() - timedelta(days=days)
      dt2 = dt1.strftime('%Y-%m-%dT%H:%M:%S.%f')
      url_temp = url + ' and (APILastChangeDate ge datetime\'<<<dt>>>\' or APICreateDate ge datetime\'<<<dt>>>\')'
      url2 = url_temp.replace('<<<dt>>>',str(dt2))
    url_final = url2 + '&$orderby=APICreateDate&$skip=<<<Skip>>>&$top=<<<Top>>>'
    return url_final

  class APIWrapper:
    def poll_api(self,tries,initial_delay,delay,backoff,apifunction):
      #time.sleep(initial_delay)
      for n in range(tries):
        try:
          status = apifunction.status_code
          if status != 200:
            #polling_time = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
            print("Sleeping for {0} seconds.".format(delay))
            time.sleep(delay)
            delay *= backoff
          else:
            return apifunction
        except socket.error as e:
          print("Connection dropped with error code{0}".format(e.errno))
      print('No response.')#raise ExceededRetries("Failed to poll {0} within {1} tries.".format(apifunction, tries))

  a = APIWrapper()
  installations = get_installations(url,user,passw)

  for table in tables:
    columns = []
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

    sql = f"DELETE FROM {str.upper(table)}_TEMP_{str.upper(env)}"
    cur_write.execute(sql)

    top=50000

    for instal in installations:
      header = {
        'Accept': 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Installation' : instal
      }

      i=0
      df = pd.DataFrame()
      result = pd.DataFrame()
      while((i==0) or (len(df)==top)):
        url = make_url('latest',header,table,2,user,passw)
        url_mod = url.replace('<<<Skip>>>',str(i)).replace('<<<Top>>>',str(top))
        r = a.poll_api(7, 60, 1, 2, requests.get(url_mod,auth=(user,passw),headers=header))
        if r is None:
          i += top
          print(f'Table: {table}, Installation: {instal}, will not be loaded.')
          continue
        t = [i for i in r.json().values()]
        df = pd.json_normalize(t[1])
        mem_df = df.memory_usage(index=True).sum()/1000000
        result = result.append(df,ignore_index=True)
        mem_res = result.memory_usage(index=True).sum()/1000000
        if ((mem_df+mem_res)>16) or (len(df)<top):
            if len(df):
              result = result.drop(['odata.etag'],axis=1)
            df_to_sf = result
            df_to_sf.columns = df_to_sf.columns.str.upper()
            columns.extend(df_to_sf.columns.tolist())
            if len(result):
              print('Loading to SF')
              write_pandas(conn_write, df_to_sf, str.upper(table) + f'_TEMP_{str.upper(env)}')
            result = pd.DataFrame()
        i += top

    keys = ['INSTALLATIONID','ID','APILASTCHANGEDATE']
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