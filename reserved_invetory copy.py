
from sp_api.api import Reports
from sp_api.base import Marketplaces, ReportType, ProcessingStatus, Granularity

import json
from datetime import datetime, timedelta, date
from pprint import pprint
import time
import requests
import csv
import pandas as pd

from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()


start_time = time.time()

CLIENT_CONFIG = {
  "lwa_app_id": "",

  "aws_access_key": "",
  "aws_secret_key": "",
  "role_arn": "",
  "refresh_token": ""
}

try:
    engine = create_engine("db credentials")

    existing_dates = pd.read_sql_table('Reserved_Inventory', con=engine)
    existing_dates_list = [str(d) for d in existing_dates['DateStamp'].values]
    #today = date.today().strftime('%Y-%m-%d')

    today = datetime.now().date()

    yesterday = date.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')

    if today in existing_dates['DateStamp'].dt.date.values:

        print(f"Data for {today} already exists in the database. Skipping.")

    else:

        report_type = ReportType.GET_RESERVED_INVENTORY_DATA
        res = Reports(credentials=CLIENT_CONFIG, marketplace=Marketplaces.DE)

        data = res.create_report(reportType=report_type)

        report = data.payload
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
        decoded_content = res.content.decode('cp1252')
        reader = csv.DictReader(decoded_content.splitlines(), delimiter='\t')


        data_list = []
        for row in reader:
            data = {
                'DateStamp': today,
                'SKU': row['sku'],
                'FNSKU': row['fnsku'],
                'ASIN': row['asin'],
                'ProductName': row['product-name'],
                'ReservedQTY': row['reserved_qty'],
                'ReservedCustomerOrders': row['reserved_fc-transfers'],
                'ReservedFCtransfer': row['reserved_fc-transfers'],
                'ReservedFCprocessing': row['reserved_fc-processing'],
            }
            data_list.append(data)

        df = pd.DataFrame(data_list)

        df.to_sql(name='Reserved_Inventory',con=engine,if_exists='append', index=False)

        #filtered_df = df[df['SKU'].isin(skus_manage_fba)]

        #filtered_df.to_sql(name='Manage_FBA_Inventory',con=engine,if_exists='append', index=False)

        print("Gathering and appending of data completed without errors")

except Exception as e:
    print("An error occurred: ", e)

finally:
    print("Running of reserved inventory data is completed after %s seconds" % (time.time() - start_time))

engine.dispose()