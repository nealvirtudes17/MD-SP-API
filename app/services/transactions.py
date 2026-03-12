import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sp_api.base.exceptions import SellingApiException

from app.database import engine
from app.api_client import get_financial_events

logger = logging.getLogger(__name__)

# --- 1. Core Mapping Logic ---

def get_payment_details(charge_type: str, quantity: Any) -> Tuple[str, str, Any]:
    """
    Replaces the legacy 16-clause if/elif block with an O(1) hash map lookup.
    Maps Amazon API charge types to internal accounting categorizations.
    """
    mapping = {
        'Principal': ('Product charges', '', quantity),
        'Tax': ('Other', 'Product Tax', ''),
        'FBAPerUnitFulfillmentFee': ('Amazon fees', 'FBA fulfilment fee per unit', ''),
        'Commission': ('Amazon fees', 'Commission', ''),
        'RefundCommission': ('Amazon fees', 'Refund Commission', ''),
        'REVERSAL_REIMBURSEMENT': ('Other', 'Reversal Reimbursement', ''),
        'FBA storage fee': ('Amazon fees', 'FBA storage fee', ''),
        'WAREHOUSE_DAMAGE_INVENTORY': ('Other', 'WAREHOUSE_DAMAGE_INVENTORY', quantity),
        'StorageRenewalBilling': ('Amazon fees', 'StorageRenewalBilling', ''),
        'StorageBilling': ('Amazon fees', 'StorageBilling', ''),
        'GIFTWRAP': ('Other', 'GIFTWRAP', ''),
        'Goodwill': ('Other', 'Goodwill', ''),
        'ShippingCharge': ('Other', 'ShippingCharge', ''),
        'ShippingTax': ('Other', 'ShippingTax', ''),
        'FBA transportation fee': ('Amazon fees', 'FBA transportation fee', ''),
        'ReturnShipping': ('Amazon fees', 'ReturnShipping', '')
    }
    return mapping.get(charge_type, ('Other', charge_type, quantity))

# --- 2. Deep Nested JSON Extraction ---

def parse_shipment_events(events: List[Dict]) -> List[Dict]:
    """Parses Order/Shipment transactions."""
    records = []
    for event in events:
        date_val = event.get('PostedDate')
        marketplace = event.get('MarketplaceName')
        order_id = event.get('AmazonOrderId')
        
        for item in event.get('ShipmentItemList', []):
            sku = item.get('SellerSKU', '')
            qty = item.get('QuantityShipped', 1)
            
            # Extract Charges (e.g., Principal, Tax)
            for charge in item.get('ItemChargeList', []):
                ctype = charge.get('ChargeType', '')
                amount = float(charge.get('ChargeAmount', {}).get('CurrencyAmount', 0.0))
                p_type, p_detail, p_qty = get_payment_details(ctype, qty)
                
                records.append({
                    'Date': date_val, 'Marketplace': marketplace, 'Order ID': order_id,
                    'Type': 'Order', 'Payment Type': p_type, 'Payment Detail': p_detail,
                    'Amount': amount, 'Quantity': p_qty, 'SKU': sku, 'Description': ''
                })
                
            # Extract Fees (e.g., Commission, FBA fees)
            for fee in item.get('ItemFeeList', []):
                ftype = fee.get('FeeType', '')
                amount = float(fee.get('FeeAmount', {}).get('CurrencyAmount', 0.0))
                p_type, p_detail, p_qty = get_payment_details(ftype, qty)
                
                records.append({
                    'Date': date_val, 'Marketplace': marketplace, 'Order ID': order_id,
                    'Type': 'Order', 'Payment Type': p_type, 'Payment Detail': p_detail,
                    'Amount': amount, 'Quantity': p_qty, 'SKU': sku, 'Description': ''
                })
    return records

def parse_refund_events(events: List[Dict]) -> List[Dict]:
    """Parses Refund transactions."""
    records = []
    for event in events:
        date_val = event.get('PostedDate')
        marketplace = event.get('MarketplaceName')
        order_id = event.get('AmazonOrderId')
        
        for item in event.get('ShipmentItemAdjustmentList', []):
            sku = item.get('SellerSKU', '')
            qty = item.get('QuantityShipped', 1)
            
            # Extract Refund Charges
            for charge in item.get('ItemChargeAdjustmentList', []):
                ctype = charge.get('ChargeType', '')
                amount = float(charge.get('ChargeAmount', {}).get('CurrencyAmount', 0.0))
                p_type, p_detail, p_qty = get_payment_details(ctype, qty)
                
                records.append({
                    'Date': date_val, 'Marketplace': marketplace, 'Order ID': order_id,
                    'Type': 'Refund', 'Payment Type': p_type, 'Payment Detail': p_detail,
                    'Amount': amount, 'Quantity': p_qty, 'SKU': sku, 'Description': ''
                })
                
            # Extract Refund Fees
            for fee in item.get('ItemFeeAdjustmentList', []):
                ftype = fee.get('FeeType', '')
                amount = float(fee.get('FeeAmount', {}).get('CurrencyAmount', 0.0))
                p_type, p_detail, p_qty = get_payment_details(ftype, qty)
                
                records.append({
                    'Date': date_val, 'Marketplace': marketplace, 'Order ID': order_id,
                    'Type': 'Refund', 'Payment Type': p_type, 'Payment Detail': p_detail,
                    'Amount': amount, 'Quantity': p_qty, 'SKU': sku, 'Description': ''
                })
                
            # Extract Promotions
            for promo in item.get('PromotionAdjustmentList', []):
                desc = promo.get('PromotionId', '')
                amount = float(promo.get('PromotionAmount', {}).get('CurrencyAmount', 0.0))
                p_type, p_detail, p_qty = get_payment_details('Promotion', qty)
                
                records.append({
                    'Date': date_val, 'Marketplace': marketplace, 'Order ID': order_id,
                    'Type': 'Refund', 'Payment Type': p_type, 'Payment Detail': p_detail,
                    'Amount': amount, 'Quantity': p_qty, 'SKU': sku, 'Description': desc
                })
    return records

def parse_service_fee_events(events: List[Dict]) -> List[Dict]:
    """Parses Account-level Service Fees (e.g., Subscriptions, Storage)."""
    records = []
    for event in events:
        date_val = event.get('CreationDate')
        marketplace = ''
        order_id = ''
        sku = event.get('SellerSKU', '')
        desc = event.get('FeeReason', '')
        
        for fee in event.get('FeeList', []):
            ftype = fee.get('FeeType', '')
            amount = float(fee.get('FeeAmount', {}).get('CurrencyAmount', 0.0))
            p_type, p_detail, p_qty = get_payment_details(ftype, '')
            
            records.append({
                'Date': date_val, 'Marketplace': marketplace, 'Order ID': order_id,
                'Type': ftype, 'Payment Type': p_type, 'Payment Detail': p_detail,
                'Amount': amount, 'Quantity': p_qty, 'SKU': sku, 'Description': desc
            })
    return records

def parse_adjustment_events(events: List[Dict]) -> List[Dict]:
    """Parses FBA Inventory Adjustments (e.g., Lost/Damaged)."""
    records = []
    for event in events:
        date_val = event.get('PostedDate')
        adj_type = event.get('AdjustmentType', '')
        
        for item in event.get('AdjustmentItemList', []):
            sku = item.get('SellerSKU', '')
            qty = item.get('Quantity', '')
            desc = item.get('ProductDescription', '')
            amount = float(item.get('PerUnitAmount', {}).get('CurrencyAmount', 0.0))
            p_type, p_detail, p_qty = get_payment_details(adj_type, qty)
            
            records.append({
                'Date': date_val, 'Marketplace': '', 'Order ID': '',
                'Type': adj_type, 'Payment Type': p_type, 'Payment Detail': p_detail,
                'Amount': amount, 'Quantity': p_qty, 'SKU': sku, 'Description': desc
            })
    return records

# --- 3. Main ETL Pipeline Logic ---

def get_sync_window() -> Tuple[str, str, str]:
    """Calculates synchronization boundaries to ensure seamless overlapping updates."""
    with engine.connect() as conn:
        stmt = text("SELECT MAX(Date) FROM Transaction_report")
        max_date_val = conn.execute(stmt).scalar()

    now = datetime.utcnow()
    posted_before = (now - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    if max_date_val:
        # Support both string and datetime returns from different DB dialects
        if isinstance(max_date_val, str):
            max_date = datetime.strptime(max_date_val[:10], "%Y-%m-%d")
        else:
            max_date = max_date_val
            
        posted_after_dt = max_date - timedelta(days=2)
        posted_after = posted_after_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        cutoff_str = posted_after_dt.strftime("%Y-%m-%d")
    else:
        # Default 30-day backfill if the table is completely empty
        posted_after_dt = now - timedelta(days=30)
        posted_after = posted_after_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        cutoff_str = posted_after_dt.strftime("%Y-%m-%d")

    return posted_after, posted_before, cutoff_str

def transform_data(raw_items: List[Dict]) -> pd.DataFrame:
    """Consolidates and formats the final Pandas DataFrame."""
    if not raw_items:
        return pd.DataFrame()

    df = pd.DataFrame(raw_items)

    # 1. Strict Legacy Filtering: Keep only German Marketplace and Account-level events
    if 'Marketplace' in df.columns:
        df = df[df['Marketplace'].isin(['Amazon.de', ''])]
        df = df.drop(columns=['Marketplace'])

    # 2. Text Cleaning
    df = df.replace({'&amp;': '&'}, regex=True)
    
    # 3. Timezone Adjustment & Formatting (UTC to CET +2 hours)
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce') + timedelta(hours=2)
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

    # 4. Numeric Enforcement
    if 'Quantity' in df.columns:
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')

    return df

def execute_sync():
    """Main Orchestration point for Transactions ETL."""
    logger.info("Starting Transactions sync...")
    
    posted_after, posted_before, cutoff_str = get_sync_window()
    logger.info(f"Fetching events from {posted_after} to {posted_before}")

    all_raw_items = []
    next_token = None
    page_count = 0
    MAX_PAGES = 50 # Fail-safe to prevent infinite API polling loops

    try:
        # --- EXTRACT ---
        while page_count < MAX_PAGES:
            logger.info(f"Fetching Finances API page {page_count + 1}...")
            
            res = get_financial_events(posted_after, posted_before, next_token=next_token)
            payload = res.payload.get('FinancialEvents', {})
            
            # Extract and flatten all nested event arrays
            all_raw_items.extend(parse_shipment_events(payload.get('ShipmentEventList', [])))
            all_raw_items.extend(parse_refund_events(payload.get('RefundEventList', [])))
            all_raw_items.extend(parse_service_fee_events(payload.get('ServiceFeeEventList', [])))
            all_raw_items.extend(parse_adjustment_events(payload.get('AdjustmentEventList', [])))
            
            next_token = res.payload.get('NextToken')
            if not next_token:
                break
                
            page_count += 1

        # --- TRANSFORM ---
        clean_df = transform_data(all_raw_items)

        if clean_df.empty:
            logger.info("No transaction records found in this time window.")
            return

        # --- LOAD ---
        with engine.begin() as conn:
            logger.info(f"Deleting overlapping records where Date >= {cutoff_str}")
            delete_stmt = text("DELETE FROM Transaction_report WHERE Date >= :cutoff")
            conn.execute(delete_stmt, {"cutoff": cutoff_str})
            
            logger.info(f"Appending {len(clean_df)} records to Transaction_report...")
            clean_df.to_sql(name='Transaction_report', con=conn, if_exists='append', index=False)
            
        logger.info("Transactions sync completed successfully.")

    except SellingApiException as e:
        logger.error(f"Amazon SP-API Error: {e}")
        raise
    except SQLAlchemyError as e:
        logger.error(f"Database Integrity Error: {e}")
        raise