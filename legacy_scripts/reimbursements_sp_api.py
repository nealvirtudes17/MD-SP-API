from sp_api.api import Reports
from sp_api.base import Marketplaces, ReportType, ProcessingStatus, Granularity

import json
from datetime import datetime, timedelta, date
from pprint import pprint
import time
import requests
import csv
import pandas as pd
import pytz

from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()

# #get today with utc
# today = datetime.today().date()
# yesterday = today - timedelta(days=1)

# #show midnight time for yesterday
# yesterday_utc = pytz.utc.localize(datetime.combine(yesterday, datetime.min.time())) - timedelta(hours=2)

# #one day gather
# date_end = yesterday_utc.isoformat()
# date_start = (yesterday_utc - timedelta(days=1)).isoformat()

# #for backtracking
# date_start_monthly = (yesterday_utc - timedelta(days=30)).isoformat()

pymysql.install_as_MySQLdb()
start_time = time.time()
print(start_time)

CLIENT_CONFIG = {
 }

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

sql_1 = "DELETE FROM Reimbursement WHERE `approval-date` > '"
sql_2 = d2_dt_F + "'"

sql_F = sql_1 + sql_2


#change date_start_monthly with date_start for daily
res = Reports(credentials=CLIENT_CONFIG, marketplace=Marketplaces.DE).create_report(
                reportType=ReportType.GET_FBA_REIMBURSEMENTS_DATA,
                dataStartTime=s_date,dateEndTime=e_date,
                )

report = res.payload
print(report)
report_id = report['reportId']

res = Reports(credentials=CLIENT_CONFIG, marketplace=Marketplaces.DE)
data = res.get_report(report_id)

report_data = ''

while data.payload.get('processingStatus') not in [ProcessingStatus.DONE, ProcessingStatus.FATAL,
                                                       ProcessingStatus.CANCELLED]:
    print(data.payload)
    print('Sleeping...')
    time.sleep(5)
    data = res.get_report(report_id)
if data.payload.get('processingStatus') in [ProcessingStatus.FATAL, ProcessingStatus.CANCELLED]:
        print("Report failed!")
        report_data = data.payload
else:
    print("Success:")
    print(data.payload)
    report_data = res.get_report_document(data.payload['reportDocumentId'])
    print("Document:")
    print(report_data.payload)

    report_url = report_data.payload.get('url')
    print(report_url)



    res = requests.get(report_url)
    decoded_content = res.content.decode('utf-8')
    reader = csv.DictReader(decoded_content.splitlines(), delimiter='\t')

    try:
        #datatime is in UTC format
        df_Reim = pd.DataFrame(reader)

        df_Reim['approval-date'] = pd.to_datetime(df_Reim['approval-date']) + timedelta(hours=8)

        df_Reim['approval-date'] = df_Reim['approval-date'].dt.tz_localize(None)

        del df_Reim['original-reimbursement-id']
        del df_Reim['original-reimbursement-type']




        with engine.connect() as con:
            rs = con.execute(sql_F)
            con.close()
        print(sql_F)
        print('DELETED')


        print('Appending Reimbursement Data.....')
        df_Reim.to_sql(name='Reimbursement',con=engine,if_exists='append', index=False)

        engine.dispose()

        print('Finished downloading Reimbursement reports')

        print("--- %s seconds ---" % (time.time() - start_time))


    except Exception as e:

        print("Error:", e)








