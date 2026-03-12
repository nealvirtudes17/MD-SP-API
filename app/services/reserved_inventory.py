import csv
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.database import engine
from app.api_client import fetch_sp_api_report
from sp_api.base import ReportType

logger = logging.getLogger(__name__)

def is_already_synced(today_date: str) -> bool:
    """Uses a highly performant scalar query to check for existing records."""
    # 2.0 Standard: explicit connect() for a read-only query
    with engine.connect() as conn:
        stmt = text("SELECT 1 FROM Reserved_Inventory WHERE DateStamp = :today LIMIT 1")
        result = conn.execute(stmt, {"today": today_date}).fetchone()
        return result is not None

def transform_data(decoded_content: str, today_date: str) -> pd.DataFrame:
    """Transforms raw TSV data and maps columns strictly per legacy requirements."""
    reader = csv.DictReader(decoded_content.splitlines(), delimiter='\t')
    
    data_list = []
    for row in reader:
        data_list.append({
            'DateStamp': today_date,
            'SKU': row.get('sku'),
            'FNSKU': row.get('fnsku'),
            'ASIN': row.get('asin'),
            'ProductName': row.get('product-name'),
            'ReservedQTY': row.get('reserved_qty'),
            'ReservedCustomerOrders': row.get('reserved_customerorders'), 
            'ReservedFCtransfer': row.get('reserved_fc-transfers'),
            'ReservedFCprocessing': row.get('reserved_fc-processing'),
        })

    df = pd.DataFrame(data_list)
    
    if df.empty:
        return df
        
    numeric_cols = ['ReservedQTY', 'ReservedCustomerOrders', 'ReservedFCtransfer', 'ReservedFCprocessing']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
    return df

def execute_sync():
    """Main ETL entry point for Reserved Inventory."""
    logger.info("Starting Reserved Inventory synchronization...")
    
    # Use standard date format to match database constraints
    today_date = datetime.now().date().strftime('%Y-%m-%d')
    
    try:
        # Step 1: Idempotency Check (Don't run if already synced today)
        if is_already_synced(today_date):
            logger.info(f"Data for {today_date} already exists in the database. Skipping.")
            return

        # Step 2: Fetch Report
        # Note: This report type does not require start/end dates
        content = fetch_sp_api_report(ReportType.GET_RESERVED_INVENTORY_DATA)

        # Step 3: Transform
        clean_df = transform_data(content, today_date)
        
        if clean_df.empty:
            logger.warning("No records found in the Reserved Inventory report.")
            return

        # Step 4: Transactional Load
        with engine.begin() as conn:
            logger.info(f"Appending {len(clean_df)} records to Reserved_Inventory...")
            clean_df.to_sql(name='Reserved_Inventory', con=conn, if_exists='append', index=False)
            
        logger.info("Reserved Inventory sync completed successfully.")

    except (SQLAlchemyError, Exception) as e:
        logger.error(f"Sync failed for Reserved Inventory: {str(e)}")
        raise