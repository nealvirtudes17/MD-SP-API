# %run ./keys.ipynb

from sp_api.api import Reports
from sp_api.base import Marketplaces, ReportType, ProcessingStatus, Granularity

import json
from datetime import datetime, timedelta, date
from pprint import pprint
import time
import requests
import csv
import pandas as pd
import gzip
from sqlalchemy import create_engine,text
import pymysql


start_time = time.time()
print(start_time)
pymysql.install_as_MySQLdb()


day = datetime.now().strftime('%A')

if day in ['Monday','Wednesday','Thursday']:
    api_start = 31
    api_end = 1
    del_start = 30

else:
    api_start = 2
    api_end = 1
    del_start = 1

engine = create_engine("")

########################## Date Calculation ###########################

dateas = datetime.now() + timedelta(hours=8)
yesterday_date = datetime.strftime(dateas - timedelta(api_start), '%Y-%m-%d')
current_date = datetime.strftime(datetime.now()-timedelta(api_end), '%Y-%m-%d')
s_date = yesterday_date + 'T22:00:00.000Z'
e_date = current_date + 'T22:00:00.000Z'

########################## SQL Delete Date Calculation #################################


d_now = date.today()

d2 = d_now - timedelta(del_start)

d2_dt = datetime.combine(d2, datetime.min.time())

d2_dt_i = d2_dt + timedelta(hours=6)

d2_dt_F= d2_dt_i.strftime('%Y-%m-%d %H:%M:%S')

d2_dt_F = str(d2_dt_F)


sql_1 = "DELETE FROM All_Orders WHERE purchaseDate > '"
sql_2 = d2_dt_F + "'"

sql_F = sql_1 + sql_2




#change new_date_frmatted to start date and end_date to end date

res = Reports(credentials=CLIENT_CONFIG, marketplace=Marketplaces.DE).create_report(
                reportType=ReportType.GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL,
                dataStartTime=s_date,dataEndTime=e_date,
                )

report = res.payload
print(report)
report_id = report['reportId']

req_report = Reports(credentials=CLIENT_CONFIG, marketplace=Marketplaces.DE)
data = req_report.get_report(report_id)

while data.payload.get('processingStatus') not in [ProcessingStatus.DONE, ProcessingStatus.FATAL,
                                                           ProcessingStatus.CANCELLED]:
        print(data.payload)
        print('Sleeping...')
        time.sleep(5)
        data = req_report.get_report(report_id)

if data.payload.get('processingStatus') in [ProcessingStatus.FATAL, ProcessingStatus.CANCELLED]:
        print("Report failed!")
        report_data = data.payload
else:
    print("Success:")
    print(data.payload)
    report_data = req_report.get_report_document(data.payload['reportDocumentId'])
    print("Document:")
    print(report_data.payload)

report_url = report_data.payload.get('url')
read_url = requests.get(report_url)


try:

    decoded_content = read_url.content.decode('utf-8')
    reader = csv.DictReader(decoded_content.splitlines(), delimiter='\t')

except:

    try:
        # Decompress the data
        decompressed_data = gzip.decompress(read_url.content)

        # Now decode the decompressed data as UTF-8
        decoded_content = decompressed_data.decode('utf-8')
        reader = csv.DictReader(decoded_content.splitlines(), delimiter='\t')
        # Process utf8_text as needed
    except Exception as e:
        print("Error:", e)

df_AO = pd.DataFrame(reader)

# df.rename(columns={'purchase-date': 'purchaseDate'},inplace=True)

# df.drop(['is-iba'], axis=1)



df_AO['amazon-order-id'] = df_AO['amazon-order-id'].str.lstrip("\n")

df_AO['purchase-date'] = pd.to_datetime(df_AO['purchase-date']) + timedelta(hours=8)

df_AO['purchase-date'] = df_AO['purchase-date'].dt.tz_localize(None)

#df_AO['purchaseDate'] = df_AO['purchase-date'].dt.strftime('%Y-%m-%d')

df_AO['purchaseDate'] = df_AO['purchase-date']

# df_AO['purchase-date'] = pd.to_datetime(df_AO['purchase-date']) + timedelta(hours=8)

df_AO = df_AO[:-1]

df_AO.rename(columns = {'is-sold-by-ab ':'is-sold-by-ab'}, inplace = True)

df_AO['is-sold-by-ab'] = df_AO['is-iba']

del df_AO['purchase-date']

del df_AO['is-iba']

# del df_AO['invoice-business-tax-office ']

try:
    df_AO.rename(columns = {'buyer-citizen-id ':'buyer-citizen-id'}, inplace = True)
except:
    print('no column')

with engine.connect() as con:

    rs = con.execute(sql_F)
    con.close()
print(sql_F)


#ADD concat to db and remember to discpose engine

#df_AO.to_sql(name='All_Orders',con=engine,if_exists='append', index=False)

print("--- %s seconds ---" % (time.time() - start_time))
engine.dispose()

print('Finished downloading All orders')

engine.dispose()

# df_sp_api = df_AO
# df_sp_api
