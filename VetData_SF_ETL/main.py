import pandas as pd
import requests
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
import datetime as dt
from datetime import timedelta
from flask import Flask

app = Flask(__name__)

@app.route('/')
@app.route('/task/loader')
def sfLibraries():
    url = r'https://api.vetdata.net/InstallationList'
    user = 'futurepet'
    passw = 'f32fb415-478e-4be7-9884-8f59f9adb11b'
    env = 'PROD'#'STG'
    tables = ['Invoices']

    def get_installations(url,user,passw):
        r = requests.get(url,auth=(user,passw))
        installs = [i['InstallationId'] for i in r.json()]
        return installs

    installations = get_installations(url,user,passw)

    for table in tables:

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

      def make_url(mode,header,table,days,user,passw):
        url = f'https://api.vetdata.net/v2/{table}?$filter=APIRemovedDate eq null'
        instal = header['Installation']
        if mode == "all":
          url2 = url
          print(f'Reloading table: {table}, installation: {instal}')
        if mode == "latest":
          print(f'Getting latest {days} days for table: {table}, Installation: {instal}')
          dt1 = dt.datetime.now() - timedelta(days=days)
          dt2 = dt1.strftime('%Y-%m-%dT%H:%M:%S.%f')
          url_temp = url + ' and (APILastChangeDate ge datetime\'<<<dt>>>\' or APICreateDate ge datetime\'<<<dt>>>\')'
          url2 = url_temp.replace('<<<dt>>>',str(dt2))
        url_final = url2 + '&$orderby=APICreateDate&$skip=<<<Skip>>>&$top=<<<Top>>>'
        return url_final

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
          url = make_url('latest',header,'Invoices',2,user,passw)
          url_mod = url.replace('<<<Skip>>>',str(i)).replace('<<<Top>>>',str(top))
          r = requests.get(url_mod,auth=(user,passw),headers=header)
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
              if len(result):
                print('Loading to SF')
                write_pandas(conn_write, df_to_sf, str.upper(table) + f'_TEMP_{str.upper(env)}')
              result = pd.DataFrame()
          i += top

      keys = ['INSTALLATIONID','ID']
      key_columns = ','.join(keys)
      fields = df_to_sf.columns.tolist()
      field_columns = ','.join(fields)
      merge_string2 = f'MERGE INTO {str.upper(table)}_{str.upper(env)} USING {str.upper(table)}_TEMP_{str.upper(env)} ON '+' AND '.join(f'{str.upper(table)}_{str.upper(env)}.{x}={str.upper(table)}_TEMP_{str.upper(env)}.{x}' for x in keys) + f' WHEN NOT MATCHED THEN INSERT ({field_columns}) VALUES ' +  '(' + ','.join(f'{str.upper(table)}_TEMP_{str.upper(env)}.{x}' for x in fields) + ')'

      sql = merge_string2
      cur_write.execute(sql)

      sql = f'DELETE FROM {str.upper(table)}_TEMP_{str.upper(env)}'
      cur_write.execute(sql)

      sql = "ALTER WAREHOUSE VET_DATA_WAREHOUSE SUSPEND"
      cur_write.execute(sql)

      cur_write.close()
      conn_write.close()
    
    return 'Success'

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)