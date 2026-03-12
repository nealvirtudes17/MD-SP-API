#Manage FBA Report

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

from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()


start_time = time.time()

CLIENT_CONFIG = {
  }

skus_manage_fba = ['DE-SHADE-FOREST-S&L',
        'DE-SHADE-FOREST',
        'DE-SHADE-NOPRINT-S&L',
        'DE-SHADE-NOPRINT',
        'DE-SHADE-SEALIFE',
        'DE-SHADE-SEALIFE-S&L',
        'DE-SHADE-WILDLIFE',
        'DE-SHADE-WILDLIFE-S&L',
        'SHADE-WILDLIFE-DARK',
        'SHADE-WILDLIFE-DARK-S&L',
        'SHADE-BLACK-DARK',
        'SHADE-BLACK-DARK-S&L',
        'U6-BOU6-LX95',
        '6W-V6GS-CRXQ',
        'BND-KICKMAT1-SHADES',
        'CA-3X7Q-WLRR',
        'DE-SHADE-BLACK-S&L',
        'DE-SHADE-FAIRIES-S&L',
        'DE-SHADE-SPACE-S&L',
        'SEAT-PROTECTOR-GREY',
        'Z0-BQXD-9XGD',
        'TRAVEL-TRAY-BLACK']

try:
    engine = create_engine()

    existing_dates = pd.read_sql_table('Manage_FBA_Inventory', con=engine)
    existing_dates_list = [str(d) for d in existing_dates['DateStamp'].values]
    #today = date.today().strftime('%Y-%m-%d')

    today = datetime.now().date()

    yesterday = date.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')

    if today in existing_dates['DateStamp'].dt.date.values:

        print(f"Data for {today} already exists in the database. Skipping.")

    else:

        report_type = ReportType.GET_FBA_MYI_ALL_INVENTORY_DATA
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

        today = date.today().strftime('%Y-%m-%d')

        data_list = []
        for row in reader:
            data = {
                'DateStamp': today,
                'SKU': row['sku'],
                'FNSKU': row['fnsku'],
                'ASIN': row['asin'],
                'ProductName': row['product-name'],
                'ItemCondition': row['condition'],
                'YourPrice': float(row['your-price']),
                'MFNListingExist': row['mfn-listing-exists'] == 'Yes',
                'mfnfulfillablequantity': row['mfn-fulfillable-quantity'] or None,
                'afnlistingexists': row['afn-listing-exists'] == 'Yes',
                'afnwarehousequantity': row['afn-warehouse-quantity'],
                'afnfulfillablequantity': row['afn-fulfillable-quantity'],
                'afnunsellablequantity': row['afn-unsellable-quantity'],
                'afnreservedquantity': row['afn-reserved-quantity'],
                'afntotalquantity': row['afn-total-quantity'],
                'perunitvolume': float(row['per-unit-volume']) if row['per-unit-volume'] else None,
                'afninboundworkingquantity': row['afn-inbound-working-quantity'],
                'afninboundshippedquantity': row['afn-inbound-shipped-quantity'],
                'afninboundreceivingquantity': row['afn-inbound-receiving-quantity'],
                'afnresearchingquantity': row['afn-researching-quantity'],
                'afnreservedfuturesupply': row['afn-reserved-future-supply'],
                'afnfuturesupplybuyable': row['afn-future-supply-buyable'],

            }
            data_list.append(data)

        df = pd.DataFrame(data_list)

        filtered_df = df[df['SKU'].isin(skus_manage_fba)]

        filtered_df.to_sql(name='Manage_FBA_Inventory',con=engine,if_exists='append', index=False)

        print("Gathering and appending of data completed without errors")

except Exception as e:
    print("An error occurred: ", e)

finally:
    print("Running of manage_fba check is completed after %s seconds" % (time.time() - start_time))

engine.dispose()