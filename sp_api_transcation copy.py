import pandas as pd
import time
from mws import mws
import requests
import xml.etree.ElementTree as et
import pandas as pd
import numpy as np
from datetime import date, timedelta
from datetime import datetime
import datetime as dt
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import openpyxl
from sp_api.api import Products, Catalog, Sales, FbaSmallAndLight
from sp_api.base import SellingApiException, Marketplaces, SellingApiForbiddenException, Granularity
from sp_api.api import Orders, Catalog, CatalogItems
from datetime import datetime, timedelta, date
from sp_api.api import Finances
import json
from typing import List
from pprint import pprint
import time
import openpyxl

def get_payment_details(x,event, quantity):

    if x == 'Principal':
        Payment_type = 'Product charges'
        Payment_detail = ''
        Quantity = quantity
    elif x == 'Tax':
        Payment_type = 'Other'
        Payment_detail = 'Product Tax'
        Quantity = ''

    elif x == 'FBAPerUnitFulfillmentFee':
        Payment_type = 'Amazon fees'
        Payment_detail = 'FBA fulfilment fee per unit'
        Quantity = ''

    elif x == 'Commission':
        Payment_type = 'Amazon fees'
        Payment_detail = 'Commission'
        Quantity = ''
    elif x == 'RefundCommission':
        Payment_type = 'Amazon fees'
        Payment_detail = 'Refund Commission'
        Quantity = ''

    elif x == 'REVERSAL_REIMBURSEMENT':
        Payment_type = 'FBA Inventory Reimbursement - Customer Return'
        Payment_detail = ''
        Quantity = quantity

    elif x == 'WAREHOUSE_DAMAGE':
        Payment_type = 'FBA Inventory Reimbursement - Damaged:Warehouse'
        Payment_detail = ''
        Quantity = quantity

    elif x == 'WAREHOUSE_LOST_MANUAL':
        Payment_type = 'FBA Inventory Reimbursement - Lost:Warehouse'
        Payment_detail = ''
        Quantity = quantity

    elif x == 'RunLightningDealFee':
        Payment_type = 'Amazon fees'
        Payment_detail = 'Lightning Deal Fee'
        Quantity = quantity

    elif x == 'FBAStorageFee':
        Payment_type = 'Amazon fees'
        Payment_detail = 'FBA storage fee'
        Quantity = quantity

    elif x == 'FBALongTermStorageFee':
        Payment_type = 'Amazon fees'
        Payment_detail = 'FBA Long-Term Storage Fee'
        Quantity = quantity

    elif x == 'Subscription':
        Payment_type = 'Amazon fees'
        Payment_detail = 'Subscription'
        Quantity = quantity

    elif x == 'FBAInboundTransportationFee':
        Payment_type = 'Amazon fees'
        Payment_detail = 'Inbound Transportation Fee'
        Quantity = quantity

    elif x == 'LabelingFee':
        Payment_type = 'Amazon fees'
        Payment_detail = 'Labeling Fee'
        Quantity = quantity

    elif x == 'FBAInboundTransportationProgramFee':
        Payment_type = 'Amazon fees'
        Payment_detail = 'Inbound Transportation Program Fee'
        Quantity = quantity

    elif x == 'CouponRedemptionFee':
        Payment_type = 'Amazon fees'
        Payment_detail = 'Coupon Redemption Fee for ' + z + ' Redemptions'
        Quantity = ''

    else:
        Payment_type = 'Other'
        Payment_detail = x
        Quantity = ''

#     print(Payment_type, Payment_detail, Quantity)

    return {
        'payment_type':Payment_type,
        'payment_detail': Payment_detail,
        'quantity': Quantity
    }


def shipment_event_list(data):
    rows = []
    for v in data:
        transaction_type = "Order Payment"
        amazon_order_id = v['AmazonOrderId']
        amazon_seller_order_id = v['SellerOrderId']
        marketplace_name = v['MarketplaceName']
        posted_date = v['PostedDate']
        shipment_items = v['ShipmentItemList']

        for shipment_item in shipment_items:
            seller_sku = shipment_item['SellerSKU']
            try:
                order_item_id = shipment_item['OrderItemId']
            except:
                order_item_id = ''

            quantity = shipment_item['QuantityShipped']



            try:
                item_charge_list = shipment_item['ItemChargeList']
            except:
                item_charge_list = []

            for k in item_charge_list:
                details = get_payment_details(k['ChargeType'],'charge', quantity)
                new_row = [posted_date,
                           marketplace_name,
                           amazon_order_id,
                           seller_sku,
                           transaction_type,
                           details['payment_type'],
                           details['payment_detail'],
                           k['ChargeAmount']['CurrencyAmount'],
                           details['quantity'],
                           ''
                          ]

                if k['ChargeAmount']['CurrencyAmount'] != 0:
                    rows.append(new_row)


            try:
                item_fee_list = shipment_item['ItemFeeList']
            except:
                item_fee_list = []


            for j in item_fee_list:
                details = get_payment_details(j['FeeType'],'fee', quantity)
                new_row = [posted_date,
                           marketplace_name,
                           amazon_order_id,
                           seller_sku,
                           transaction_type,
                           details['payment_type'],
                           details['payment_detail'],
                           j['FeeAmount']['CurrencyAmount'],
                           details['quantity'],
                           ''
                          ]
                #print(new_row)
                if j['FeeAmount']['CurrencyAmount'] != 0:
                    rows.append(new_row)

            try:
                promotional_list = shipment_item['PromotionList']
            except:
                promotional_list = []

            for p in promotional_list:
                try:
                    promo_id = p['PromotionId']
                except:
                    promo_id = ''

                details = get_payment_details(promo_id,'promotion', quantity)

                new_row = [posted_date,
                           marketplace_name,
                           amazon_order_id,
                           seller_sku,
                           transaction_type,
                           details['payment_type'],
                           details['payment_detail'],
                           p['PromotionAmount']['CurrencyAmount'],
                           details['quantity'],
                           ''
                          ]

                #print(new_row)
                if p['PromotionAmount']['CurrencyAmount'] != 0:
                    rows.append(new_row)

    return rows, posted_date

def refund_event_list(data):
    rows = []
    for v in data:
        transaction_type = "Refund"
        amazon_order_id = v['AmazonOrderId']
        amazon_seller_order_id = v['SellerOrderId']
        marketplace_name = v['MarketplaceName']
        posted_date = v['PostedDate']
        shipment_adjustment_items = v['ShipmentItemAdjustmentList']

        for shipment_item in shipment_adjustment_items:
            seller_sku = shipment_item['SellerSKU']
            try:
                order_item_id = shipment_item['OrderItemId']
            except:
                order_item_id = ''

            quantity = shipment_item['QuantityShipped']



            try:
                item_charge_list = shipment_item['ItemChargeAdjustmentList']
            except:
                item_charge_list = []

            for k in item_charge_list:
                details = get_payment_details(k['ChargeType'],'charge', quantity)
                new_row = [posted_date,
                           marketplace_name,
                           amazon_order_id,
                           seller_sku,
                           transaction_type,
                           details['payment_type'],
                           details['payment_detail'],
                           k['ChargeAmount']['CurrencyAmount'],
                           details['quantity'],
                           ''
                          ]
                # print(new_row)
                if k['ChargeAmount']['CurrencyAmount'] != 0:
                    rows.append(new_row)


            try:
                item_fee_list = shipment_item['ItemFeeAdjustmentList']
            except:
                item_fee_list = []


            for j in item_fee_list:
                details = get_payment_details(j['FeeType'],'fee', quantity)
                new_row = [posted_date,
                           marketplace_name,
                           amazon_order_id,
                           seller_sku,
                           transaction_type,
                           details['payment_type'],
                           details['payment_detail'],
                           j['FeeAmount']['CurrencyAmount'],
                           details['quantity'],
                           ''
                          ]
                # print(new_row)
                if j['FeeAmount']['CurrencyAmount'] != 0:
                    rows.append(new_row)

    return rows


def service_fee_event_list(data, date):
    rows = []
    for v in data:
        for j in v['FeeList']:
            details = get_payment_details(j['FeeType'],'servicefee','')
            new_row = [
                date,
                '',
                '',
                '',
                'Service Fees',
                details['payment_type'],
                details['payment_detail'],
                j['FeeAmount']['CurrencyAmount'],
                details['quantity'],
                ''
            ]
            # print(new_row)
            if j['FeeAmount']['CurrencyAmount'] != 0:
                    rows.append(new_row)
    return rows

def adjustment_event_list(data):
    rows = []
    for v in data:
        transaction_type = "Other"
        adjustment_type = v['AdjustmentType']
        posted_date = v['PostedDate']
        marketplace_name = ''
        amount = v['AdjustmentAmount']['CurrencyAmount']
        seller_sku = ''
        quantity = ''

        details = get_payment_details(adjustment_type,'adjustment', quantity)
        try:
            adjustment_items = v['AdjustmentItemList']
        except:
            adjustment_items = []

        if adjustment_items:
            for adjustment_item in adjustment_items:
                seller_sku = adjustment_item['SellerSKU']
                quantity = adjustment_item['Quantity']
                amount = adjustment_item['TotalAmount']['CurrencyAmount']

        new_row = [
                posted_date,
                marketplace_name,
                '',
                seller_sku,
                transaction_type,
                details['payment_type'],
                details['payment_detail'],
                amount,
                quantity,
                ''
            ]
        # print(new_row)
        rows.append(new_row)

    return rows




CLIENT_CONFIG = {
  }

start_time = time.time()
df_cols = ["Date",
           "Marketplace",
           "Order ID",
           "SKU",
           "Transaction type",
           "Payment Type",
           "Payment Detail",
           "Amount",
           "Quantity",
           "Product Title"
           ]

day = datetime.now().strftime('%A')

if day in ['Monday','Wednesday','Thursday']:
    api_start = 31
    api_end = 1
    del_start = 30

else:
    api_start = 8
    api_end = 1
    del_start = 7

yesterday_date = datetime.strftime(datetime.now() - timedelta(api_start), '%Y-%m-%d')
current_date = datetime.strftime(datetime.now() - timedelta(api_end), '%Y-%m-%d')


s_date = yesterday_date + 'T22:00:00.000Z'
e_date = current_date + 'T22:00:00.000Z'


print(s_date, e_date)


############### DATE CALCULATION FOR BACK TRACKING ####################
d_now_del = date.today()

d2_del = d_now_del - timedelta(del_start)

##################################################

#Delete Data
d2_del_F = d2_del.strftime('%Y-%m-%d')

engine = create_engine("mysql://web83_4:BKRjP7G8m2yqkbMV@s95.goserver.host/web83_db4?charset=utf8")



sql_trans1 = "DELETE FROM Transaction_report WHERE Date >= '"
sql_trans2 = d2_del_F + "'"

sql_trans = sql_trans1 + sql_trans2

print(sql_trans)

##########################################################################



res = Finances(credentials=CLIENT_CONFIG, marketplace=Marketplaces.DE)
data = res.list_financial_events(PostedAfter=s_date,PostedBefore=e_date)
count = 1

items = []
while True:
    if count != 1:
        res = Finances(credentials=CLIENT_CONFIG, marketplace=Marketplaces.DE)
        data = res.list_financial_events(NextToken = next_token)

    events = data.payload

    try:
        next_token = events['NextToken']
    except:
        next_token = None

    financial_events = events['FinancialEvents']

    if next_token:
        for i in financial_events:
            z = financial_events[i]

            if z:
                if i == 'ShipmentEventList':
                    data = shipment_event_list(z)
                    rows = data[0]
                    date_last = data[1]

                    items = items + rows
                elif i == 'RefundEventList':

                    rows = refund_event_list(z)
                    df = pd.DataFrame(rows, columns = df_cols)

                    items = items + rows

                elif i == 'ServiceFeeEventList':
                    rows = service_fee_event_list(z,date_last)
                    items = items + rows

                elif i == 'AdjustmentEventList':
                    rows = adjustment_event_list(z)
                    items = items + rows
                else:
                    print(i)
                    print(z)
    else:
        for i in financial_events:
            z = financial_events[i]

            if z:
                if i == 'ShipmentEventList':
                    data = shipment_event_list(z)
                    rows = data[0]
                    date_last = data[1]

                    items = items + rows
                elif i == 'RefundEventList':

                    rows = refund_event_list(z)
                    df = pd.DataFrame(rows, columns = df_cols)

                    items = items + rows

                elif i == 'ServiceFeeEventList':
                    rows = service_fee_event_list(z,date_last)
                    items = items + rows

                elif i == 'AdjustmentEventList':
                    rows = adjustment_event_list(z)
                    items = items + rows
                else:
                    print(i)
                    print(z)
        break

    count += 1

df = pd.DataFrame(items, columns = df_cols)

filters = ['Amazon.de','']

df = df[df['Marketplace'].isin(filters)]

df = df.replace('&amp;',"&", regex=True)

df['Date'] = pd.to_datetime(df['Date']) + timedelta(hours = 2)

df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')


del df['Marketplace']


with engine.connect() as con:

    rs = con.execute(sql_trans)
    con.close()
print("DELETED")


if len(df) >= 1:
    engine = create_engine("")

    df.to_sql(name='Transaction_report',con=engine,if_exists='append', index=False)
    print('Append Completed')

print("--- %s seconds ---" % (time.time() - start_time))

print('Finished downloading Transactions Report')

engine.dispose()

print('No more pages')



