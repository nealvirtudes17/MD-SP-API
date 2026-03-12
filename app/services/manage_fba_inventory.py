import csv
import logging
from datetime import datetime
from typing import List

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.database import engine
from app.api_client import fetch_sp_api_report
from sp_api.base import ReportType

logger = logging.getLogger(__name__)

# Portfolio SKU filter list from legacy script
SKUS_MANAGE_FBA = [
    'DE-SHADE-FOREST-S&L', 'DE-SHADE-FOREST', 'DE-SHADE-NOPRINT-S&L',
    'DE-SHADE-NOPRINT', 'DE-SHADE-SEALIFE', 'DE-SHADE-SEALIFE-S&L',
    'DE-SHADE-WILDLIFE', 'DE-SHADE-WILDLIFE-S&L', 'SHADE-WILDLIFE-DARK',
    'SHADE-WILDLIFE-DARK-S&L', 'SHADE-BLACK-DARK', 'SHADE-BLACK-DARK-S&L',
    'U6-BOU6-LX95', '6W-V6GS-CRXQ', 'BND-KICKMAT1-SHADES', 'CA-3X7Q-WLRR',
    'DE-SHADE-BLACK-S&L', 'DE-SHADE-FAIRIES-S&L', 'DE-SHADE-SPACE-S&L',
    'SEAT-PROTECTOR-GREY', 'Z0-BQXD-9XGD', 'TRAVEL-TRAY-BLACK'
]

def is_already_synced(today_date_str: str) -> bool:
    """Checks if data for today already exists using a performant scalar query."""
    with engine.connect() as conn:
        stmt = text("SELECT 1 FROM Manage_FBA_Inventory WHERE DateStamp = :today LIMIT 1")
        result = conn.execute(stmt, {"today": today_date_str}).fetchone()
        return result is not None

def transform_data(decoded_content: str, today_date_obj) -> pd.DataFrame:
    """Transforms raw TSV data and strictly maps FBA Inventory columns to DB Schema."""
    reader = csv.DictReader(decoded_content.splitlines(), delimiter='\t')
    
    data_list = []
    for row in reader:
        data_list.append({
            'DateStamp': today_date_obj, # DB Type: date
            'SKU': row.get('sku'),
            'FNSKU': row.get('fnsku'),
            'ASIN': row.get('asin'),
            'ProductName': row.get('product-name'),
            'ItemCondition': row.get('condition'),
            'YourPrice': row.get('your-price'),
            
            # Extract boolean logic immediately, will cast to tinyint(1) later
            'MFNListingExist': row.get('mfn-listing-exists') == 'Yes', 
            'mfnfulfillablequantity': row.get('mfn-fulfillable-quantity'),
            
            'afnlistingexists': row.get('afn-listing-exists') == 'Yes',
            'afnwarehousequantity': row.get('afn-warehouse-quantity'),
            'afnfulfillablequantity': row.get('afn-fulfillable-quantity'),
            'afnunsellablequantity': row.get('afn-unsellable-quantity'),
            'afnreservedquantity': row.get('afn-reserved-quantity'),
            'afntotalquantity': row.get('afn-total-quantity'),
            'perunitvolume': row.get('per-unit-volume'),
            'afninboundworkingquantity': row.get('afn-inbound-working-quantity'),
            'afninboundshippedquantity': row.get('afn-inbound-shipped-quantity'),
            'afninboundreceivingquantity': row.get('afn-inbound-receiving-quantity'),
            'afnresearchingquantity': row.get('afn-researching-quantity'),
            'afnreservedfuturesupply': row.get('afn-reserved-future-supply'),
            'afnfuturesupplybuyable': row.get('afn-future-supply-buyable'),
        })

    df = pd.DataFrame(data_list)
    if df.empty:
        return df

    # 1. Apply SKU Filtering
    df = df[df['SKU'].isin(SKUS_MANAGE_FBA)]

    # 2. Strict Type Casting to match DB Schema
    
    # TINYINT(1) - Explicitly cast True/False to 1/0
    tinyint_cols = ['MFNListingExist', 'afnlistingexists']
    for col in tinyint_cols:
        df[col] = df[col].astype(int)

    # FLOAT - Convert prices and volumes
    float_cols = ['YourPrice', 'perunitvolume']
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    # INT(11) - Convert all quantities
    int_cols = [
        'mfnfulfillablequantity', 'afnwarehousequantity', 'afnfulfillablequantity',
        'afnunsellablequantity', 'afnreservedquantity', 'afntotalquantity',
        'afninboundworkingquantity', 'afninboundshippedquantity', 
        'afninboundreceivingquantity', 'afnresearchingquantity',
        'afnreservedfuturesupply', 'afnfuturesupplybuyable'
    ]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
    # Convert any remaining NaNs in string columns to None for SQL NULL compatibility
    return df.replace({np.nan: None})

def execute_sync():
    """Main ETL entry point for Manage FBA Inventory."""
    logger.info("Starting Manage FBA Inventory synchronization...")
    
    today_date_obj = datetime.now().date()
    today_date_str = today_date_obj.strftime('%Y-%m-%d')
    
    try:
        if is_already_synced(today_date_str):
            logger.info(f"Data for {today_date_str} already exists. Skipping.")
            return

        content = fetch_sp_api_report(ReportType.GET_FBA_MYI_ALL_INVENTORY_DATA)
        clean_df = transform_data(content, today_date_obj)
        
        if clean_df.empty:
            logger.warning("No records found in the FBA Inventory report after filtering.")
            return

        with engine.begin() as conn:
            logger.info(f"Appending {len(clean_df)} records to Manage_FBA_Inventory...")
            clean_df.to_sql(name='Manage_FBA_Inventory', con=conn, if_exists='append', index=False)
            
        logger.info("Manage FBA Inventory sync completed successfully.")

    except (SQLAlchemyError, Exception) as e:
        logger.error(f"Sync failed for Manage FBA Inventory: {str(e)}")
        raise