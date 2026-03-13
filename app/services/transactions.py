import logging
import time
from datetime import datetime, timedelta, date

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sp_api.api import Finances
from sp_api.base import Marketplaces

from app.database import engine
from app.api_client import Config

logger = logging.getLogger(__name__)


CHARGE_TYPE_MAP = {
    'Principal': ('Product charges', '', True),
    'Tax': ('Other', 'Product Tax', False),
    'FBAPerUnitFulfillmentFee': ('Amazon fees', 'FBA fulfilment fee per unit', False),
    'Commission': ('Amazon fees', 'Commission', False),
    'RefundCommission': ('Amazon fees', 'Refund Commission', False),
    'REVERSAL_REIMBURSEMENT': ('FBA Inventory Reimbursement - Customer Return', '', True),
    'WAREHOUSE_DAMAGE': ('FBA Inventory Reimbursement - Damaged:Warehouse', '', True),
    'WAREHOUSE_LOST_MANUAL': ('FBA Inventory Reimbursement - Lost:Warehouse', '', True),
    'RunLightningDealFee': ('Amazon fees', 'Lightning Deal Fee', True),
    'FBAStorageFee': ('Amazon fees', 'FBA storage fee', True),
    'FBALongTermStorageFee': ('Amazon fees', 'FBA Long-Term Storage Fee', True),
    'Subscription': ('Amazon fees', 'Subscription', True),
    'FBAInboundTransportationFee': ('Amazon fees', 'Inbound Transportation Fee', True),
    'LabelingFee': ('Amazon fees', 'Labeling Fee', True),
    'FBAInboundTransportationProgramFee': ('Amazon fees', 'Inbound Transportation Program Fee', True),
    'CouponRedemptionFee': ('Amazon fees', 'Coupon Redemption Fee', False)
}

def get_mapped_details(raw_type: str, quantity: int) -> dict:
    """Uses the mapping dictionary to return standardized payment details."""
    p_type, p_detail, inc_qty = CHARGE_TYPE_MAP.get(raw_type, ('Other', raw_type, False))
    return {
        'PaymentType': p_type,
        'PaymentDetail': p_detail,
        'Quantity': quantity if inc_qty else 0
    }

# ==============================================================================
# EVENT PARSERS 
# PURPOSE: To "flatten" Amazon's deeply nested JSON tree into standard database rows.
# ==============================================================================

def parse_shipment_event(event: dict) -> tuple[list, str]:
    """Flattens ShipmentEventList into database-ready rows."""
    rows = []
    
    # 1. Extract Top-Level Shipment Data
    order_id = event.get('AmazonOrderId', '')
    posted_date = event.get('PostedDate')
    marketplace = event.get('MarketplaceName', '')

    # 2. Extract Item-Level Data
    for item in event.get('ShipmentItemList', []):
        sku = item.get('SellerSKU', '')
        qty = item.get('QuantityShipped', 0)
        
        # We loop dynamically through Charges and Fees
        for list_name, item_type in [('ItemChargeList', 'ChargeType'), ('ItemFeeList', 'FeeType')]:
            for charge in item.get(list_name, []):
                amt = charge.get(item_type.replace('Type', 'Amount'), {}).get('CurrencyAmount', 0)
                if amt == 0:
                    continue
                    
                mapped = get_mapped_details(charge[item_type], qty)
                
                rows.append({
                    'Date': posted_date, 
                    'Marketplace': marketplace, 
                    'Order ID': order_id,           # Fixed space
                    'SKU': sku, 
                    'Transaction type': 'Order Payment', # Fixed space and casing
                    'Payment Type': mapped['PaymentType'], # Fixed space
                    'Payment Detail': mapped['PaymentDetail'], # Fixed space
                    'Amount': amt, 
                    'Quantity': mapped['Quantity'], 
                    'Product Title': ''             # Fixed space
                })
                
        # Parse Promotions
        for promo in item.get('PromotionList', []):
            amt = promo.get('PromotionAmount', {}).get('CurrencyAmount', 0)
            if amt == 0:
                continue
                
            mapped = get_mapped_details(promo.get('PromotionId', ''), qty)
            
            rows.append({
                'Date': posted_date, 
                'Marketplace': marketplace, 
                'Order ID': order_id, 
                'SKU': sku, 
                'Transaction type': 'Order Payment', 
                'Payment Type': mapped['PaymentType'], 
                'Payment Detail': mapped['PaymentDetail'], 
                'Amount': amt, 
                'Quantity': mapped['Quantity'], 
                'Product Title': ''
            })

    return rows, posted_date


def parse_refund_event(event: dict) -> list:
    """Flattens RefundEventList."""
    rows = []
    
    #Extract Top data level with .get()
    order_id = event.get('AmazonOrderId', '')
    posted_date = event.get('PostedDate')
    marketplace = event.get('MarketplaceName', '')

    #loop through the items (defaults to empty list if missing)
    for item in event.get('ShipmentItemAdjustmentList', []):
        sku = item.get('SellerSKU', '')
        qty = item.get('QuantityShipped', 0)

        #Loop for both Charges and Fees
        for list_name, item_type in [('ItemChargeAdjustmentList', 'ChargeType'), ('ItemFeeAdjustmentList', 'FeeType')]:
            for charge in item.get(list_name, []):
                amt = charge.get(item_type.replace('Type', 'Amount'), {}).get('CurrencyAmount', 0)
                if amt == 0:
                    continue
        
                mapped = get_mapped_details(charge[item_type], qty)
    
                rows.append({
                    'Date': posted_date, 
                    'Marketplace': marketplace, 
                    'Order ID': order_id, 
                    'SKU': sku, 
                    'Transaction type': 'Refund', 
                    'Payment Type': mapped['PaymentType'], 
                    'Payment Detail': mapped['PaymentDetail'], 
                    'Amount': amt, 
                    'Quantity': mapped['Quantity'], 
                    'Product Title': ''
                })
    return rows


def parse_service_fee_event(event: dict, fallback_date: str) -> list:
    """Flattens ServiceFeeEventList. Uses fallback_date if none is provided by Amazon."""
    rows = []
    
    # Loop through the fees, default is zero if not found
    for fee in event.get('FeeList', []):
        
        amt = fee.get('FeeAmount', {}).get('CurrencyAmount', 0)
        
        #Added a guard clause to skip zero amounts
        if amt == 0:
            continue
            
        
        mapped = get_mapped_details(fee.get('FeeType', ''), 0)
        
        rows.append({
            'Date': fallback_date, 
            'Marketplace': '', 
            'Order ID': '', 
            'SKU': '', 
            'Transaction type': 'Service Fees', 
            'Payment Type': mapped['PaymentType'], 
            'Payment Detail': mapped['PaymentDetail'], 
            'Amount': amt, 
            'Quantity': mapped['Quantity'], 
            'Product Title': ''
        })
        
    return rows


def parse_adjustment_event(event: dict) -> list:
    """
    Flattens AdjustmentEventList. 
    Handles both Global Adjustments and Item-Level Adjustments.
    """
    rows = []
    posted_date = event.get('PostedDate')
    adj_type = event.get('AdjustmentType', '')
    
    items = event.get('AdjustmentItemList', [])
    
    if items:
        # Handling adjustment evenst that are tied to an SKU
        for item in items:
            sku = item.get('SellerSKU', '')
            qty = item.get('Quantity', 0)
            amt = item.get('TotalAmount', {}).get('CurrencyAmount', 0)
            
            # Translate using the specific quantity for this item
            mapped = get_mapped_details(adj_type, qty)
            
            # SCHEMA FIX: Keys exactly match legacy df_cols
            rows.append({
                'Date': posted_date, 
                'Marketplace': '', 
                'Order ID': '', 
                'SKU': sku, 
                'Transaction type': 'Other', 
                'Payment Type': mapped['PaymentType'], 
                'Payment Detail': mapped['PaymentDetail'], 
                'Amount': amt, 
                'Quantity': mapped['Quantity'], 
                'Product Title': ''
            })
    else:
        # Handling global adjustments
        amt = event.get('AdjustmentAmount', {}).get('CurrencyAmount', 0)
        mapped = get_mapped_details(adj_type, 0)
        
        # SCHEMA FIX: Keys exactly match legacy df_cols
        rows.append({
            'Date': posted_date, 
            'Marketplace': '', 
            'Order ID': '', 
            'SKU': '', 
            'Transaction type': 'Other', 
            'Payment Type': mapped['PaymentType'], 
            'Payment Detail': mapped['PaymentDetail'], 
            'Amount': amt, 
            'Quantity': mapped['Quantity'], 
            'Product Title': ''
        })
        
    return rows


# ==============================================================================
# MAIN ETL and Pagination management of API
# ==============================================================================

def get_sync_parameters() -> tuple[str, str, str]:
    """Calculates strict API boundaries and DB purge cutoffs with UTC+8 sync."""
    day = datetime.now().strftime('%A')
    
    if day in ['Monday', 'Wednesday', 'Thursday']:
        api_start = 31
        api_end = 1
        del_start = 30
    else:
        api_start = 8
        api_end = 1
        del_start = 7

    now = datetime.now()

    #Calculate API Boundaries (UTC)
    yesterday_date = (now - timedelta(days=api_start)).strftime('%Y-%m-%d')
    current_date = (now - timedelta(days=api_end)).strftime('%Y-%m-%d')
    
    s_date = f"{yesterday_date}T22:00:00.000Z"
    e_date = f"{current_date}T22:00:00.000Z"
    
    #Calculate Database Purge Date (Local Time UTC+8)
    #Explicitly added 06:00:00 to perfectly align with 22:00:00 UTC
    d_now_del = date.today()
    d2_del = d_now_del - timedelta(days=del_start)
    cutoff_str = f"{d2_del.strftime('%Y-%m-%d')} 06:00:00"
    
    return s_date, e_date, cutoff_str



def execute_sync():
    logger.info("Starting Financial Transactions synchronization...")
    s_date, e_date, cutoff_str = get_sync_parameters()
    
    try:
        client = Finances(credentials=Config.get_sp_api_credentials(), marketplace=Marketplaces.DE)
        all_events = []
        last_seen_date = (datetime.now() + timedelta(hours=8)).strftime('%Y-%m-%dT00:00:00Z')
        
        res = client.list_financial_events(PostedAfter=s_date, PostedBefore=e_date)
        while True:
            payload = res.payload.get('FinancialEvents', {})
            
            for event in payload.get('ShipmentEventList', []):
                rows, date_last = parse_shipment_event(event)
                all_events.extend(rows)
                if date_last: 
                    last_seen_date = date_last
                    
            for event in payload.get('RefundEventList', []):
                all_events.extend(parse_refund_event(event))
                
            for event in payload.get('ServiceFeeEventList', []):
                all_events.extend(parse_service_fee_event(event, last_seen_date))
                
            for event in payload.get('AdjustmentEventList', []):
                all_events.extend(parse_adjustment_event(event))
            
            next_token = res.pagination.get('NextToken') if hasattr(res, 'pagination') else None
            if not next_token: break
            res = client.list_financial_events(NextToken=next_token)

        df = pd.DataFrame(all_events)
        if df.empty:
            logger.info("No records found.")
            return

        df = df[df['Marketplace'].isin(['Amazon.de', ''])]
        df = df.replace('&amp;',"&", regex=True)
        df['Date'] = pd.to_datetime(df['Date'])
        #Converting to local timezone (UTC+8)
        df['Date'] = df['Date'] + pd.Timedelta(hours=8)
        # 3. Strip timezone info but KEEP THE HOURS/MINUTES/SECONDS
        df['Date'] = df['Date'].dt.tz_localize(None)
        df.drop(columns=['Marketplace'], inplace=True, errors='ignore')

        with engine.begin() as conn:
            logger.info(f"Purging old transactions >= {cutoff_str}...")
            conn.execute(text("DELETE FROM Transaction_report WHERE Date >= :cutoff"), {"cutoff": cutoff_str})
            df.to_sql(name='Transaction_report', con=conn, if_exists='append', index=False)
            
        logger.info("Sync completed.")

    except (SQLAlchemyError, Exception) as e:
        logger.error(f"Sync failed: {str(e)}")
        raise