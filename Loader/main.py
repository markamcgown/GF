import pandas as pd
import requests
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
from flask import Flask

app = Flask(__name__)

@app.route('/')
def sfLibraries():
    url = r'https://api.vetdata.net/InstallationList'
    user = 'futurepet'
    passw = 'f32fb415-478e-4be7-9884-8f59f9adb11b'

    def get_installations(url,user,passw):
        r = requests.get(url,auth=(user,passw))
        installs = [i['InstallationId'] for i in r.json()]
        return installs

    installations = get_installations(url,user,passw)

    conn_write = snow.connect(user="PROD_USER",
    password="F32fb415-478e-4be7-9884-8f59f9adb11b",
    account="cv52121.us-central1.gcp",
    warehouse="VET_DATA_WAREHOUSE",
    database="VET_DB",
    schema="VET_SCHEMA")

    table = 'Invoices'
    cur_write = conn_write.cursor()

    sql = "USE ROLE ACCOUNTADMIN"
    cur_write.execute(sql)

    sql = "USE WAREHOUSE VET_DATA_WAREHOUSE"
    cur_write.execute(sql)

    sql = "USE DATABASE VET_DB"
    cur_write.execute(sql)

    sql = "USE SCHEMA VET_SCHEMA"
    cur_write.execute(sql)

    top=50000

    for instal in installations:
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Installation' : instal
        }

        i=0
        df = pd.DataFrame()
        result = pd.DataFrame()
        while((i==0) or (len(df)==top)):
            url2 = 'https://api.vetdata.net/v2/<<<Table>>>?$orderby=APICreateDate&$filter=APIRemovedDate eq null&$skip=<<<Skip>>>&$top=<<<Top>>>'
            url_mod = url2.replace('<<<Table>>>',table).replace('<<<Skip>>>',str(i)).replace('<<<Top>>>',str(top))
            r = requests.get(url_mod,auth=(user,passw),headers=headers)
            t = [i for i in r.json().values()]
            df = pd.json_normalize(t[1])
            mem_df = df.memory_usage(index=True).sum()/1000000
            result = result.append(df,ignore_index=True)
            mem_res = result.memory_usage(index=True).sum()/1000000
            if ((mem_df+mem_res)>16) or (len(df)<top):
                print('Loading to SF')
                df_to_sf = result.drop(['odata.etag'],axis=1)
                df_to_sf.columns = df_to_sf.columns.str.upper()
                write_pandas(conn_write, df_to_sf, str.upper(table) + '_TEMP_PROD')
                result = pd.DataFrame()
            i += top

    keys = ['INSTALLATIONID','ID']
    key_columns = ','.join(keys)
    fields = df_to_sf.columns.tolist()
    field_columns = ','.join(fields)
    merge_string2 = f'MERGE INTO {str.upper(table)}_PROD USING {str.upper(table)}_TEMP_PROD ON '+' AND '.join(f'{str.upper(table)}_PROD.{x}={str.upper(table)}_TEMP_PROD.{x}' for x in keys) + f' WHEN NOT MATCHED THEN INSERT ({field_columns}) VALUES ' +  '(' + ','.join(f'{str.upper(table)}_TEMP_PROD.{x}' for x in fields) + ')'

    sql = merge_string2
    cur_write.execute(sql)

    sql = f'DELETE FROM {str.upper(table)}_TEMP_PROD'
    cur_write.execute(sql)

    sql = "ALTER WAREHOUSE VET_DATA_WAREHOUSE SUSPEND"
    cur_write.execute(sql)

    cur_write.close()
    conn_write.close()
    
    return 'Success'

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)