#Get SnL registration
# %run ./keys.ipynb

import json
from datetime import datetime, timedelta, date
from pprint import pprint
import time

from sp_api.api import Products, Catalog, Sales, FbaSmallAndLight
from sp_api.base import SellingApiException, Marketplaces, SellingApiForbiddenException, Granularity
from sp_api.api import Orders, Catalog, CatalogItems

import pandas as pd
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()

start_time = time.time()


skus_snl = ['DE-SHADE-FOREST-S&L',
        'DE-SHADE-NOPRINT-S&L',
        'DE-SHADE-SEALIFE-S&L',
        'DE-SHADE-WILDLIFE-S&L',
        'SHADE-WILDLIFE-DARK-S&L',
        'SHADE-BLACK-DARK-S&L',
       ]

CLIENT_CONFIG = {
 }

try:
    engine = create_engine("")

    existing_dates = pd.read_sql('SnL_Enrollment_Status', con=engine)
    existing_dates_list = [str(d) for d in existing_dates['Date'].values]
    today = date.today().strftime('%Y-%m-%d')

    yesterday = date.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')

    if today in existing_dates.values:

        print(f"Data for {today} already exists in the database. Skipping.")

    else:

        res = FbaSmallAndLight(credentials=CLIENT_CONFIG, marketplace=Marketplaces.DE)


        rows = []

        for sku in skus_snl:
            data = res.get_small_and_light_enrollment_by_seller_sku(sku)

            row = {
                    'Date':today,
                    'SKU':sku,
                    'Status':data('status')
                    }
            rows.append(row)

        df = pd.DataFrame(rows)

        df.to_sql(name='SnL_Enrollment_Status',con=engine,if_exists='append', index=False)

        print("Gathering and appending of data completed without errors")


except Exception as e:
    print("An error occurred: ", e)

finally:
    print("Running of SnL registration check is completed after %s seconds" % (time.time() - start_time))