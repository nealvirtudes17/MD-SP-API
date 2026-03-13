import csv
import logging
from datetime import datetime, timedelta, date

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.database import engine
from app.api_client import fetch_sp_api_report
from sp_api.base import ReportType

logger = logging.getLogger(__name__)

def get_sync_parameters() -> tuple[str, str, str]:
    """Calculates API boundaries and SQL deletion cutoffs."""
    day = datetime.now().strftime('%A')
    
    if day in ['Monday', 'Wednesday', 'Thursday']:
        api_start, api_end, del_start = 31, 1, 30
    else:
        api_start, api_end, del_start = 2, 1, 1

    now_tz = datetime.now() + timedelta(hours=8)
    s_date = (now_tz - timedelta(api_start)).strftime('%Y-%m-%d') + 'T22:00:00.000Z'
    e_date = (datetime.now() - timedelta(api_end)).strftime('%Y-%m-%d') + 'T22:00:00.000Z'
    
    cutoff_date = date.today() - timedelta(del_start)
    cutoff_dt = datetime.combine(cutoff_date, datetime.min.time()) + timedelta(hours=6)
    
    return s_date, e_date, cutoff_dt.strftime('%Y-%m-%d %H:%M:%S')

def transform_data(decoded_content: str) -> pd.DataFrame:
    """Transforms raw TSV data using Pandas (Schema-Agnostic)."""
    reader = csv.DictReader(decoded_content.splitlines(), delimiter='\t')
    df = pd.DataFrame(reader)

    if df.empty:
        return df

    # 1. Clean Core Identifiers
    df['amazon-order-id'] = df['amazon-order-id'].str.lstrip("\n")
    
    # 2. Date Normalization (Matching your legacy script exactly)
    df['purchase-date'] = pd.to_datetime(df['purchase-date'], errors='coerce') + timedelta(hours=8)
    df['purchase-date'] = df['purchase-date'].dt.tz_localize(None)
    df['purchaseDate'] = df['purchase-date']  # Duplicate for the database column match

    df['last-updated-date'] = pd.to_datetime(df['last-updated-date'], errors='coerce') + timedelta(hours=8)
    df['last-updated-date'] = df['last-updated-date'].dt.tz_localize(None)
    df['last-updated-date'] = df['last-updated-datee'] 
    
    # 3. Drop trailing summary row and format specific columns
    df = df[:-1]
    
    if 'is-iba' in df.columns:
        df['is-sold-by-ab'] = df['is-iba']
        df = df.drop(columns=['is-iba'])
        
    df = df.drop(columns=['purchase-date'], errors='ignore')

    if 'buyer-citizen-id ' in df.columns:
        df = df.rename(columns={'buyer-citizen-id ': 'buyer-citizen-id'})

    # 4. Fill NaNs to prevent SQL insertion errors
    return df.replace({np.nan: None})

def execute_sync():
    logger.info("Starting All Orders synchronization...")
    s_date, e_date, cutoff_str = get_sync_parameters()
    
    try:
        # Step 1: Fetch and Decode Report via Shared API Client
        content = fetch_sp_api_report(
            ReportType.GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL,
            s_date, e_date
        )

        # Step 2: Transform
        clean_df = transform_data(content)
        
        if clean_df.empty:
            logger.info("No records found to sync.")
            return

        with engine.begin() as conn:
            logger.info(f"Clearing existing data after {cutoff_str}...")
            delete_stmt = text("DELETE FROM All_Orders WHERE purchaseDate > :cutoff")
            conn.execute(delete_stmt, {"cutoff": cutoff_str})
            
            logger.info(f"Appending {len(clean_df)} records to All_Orders...")
            clean_df.to_sql(name='All_Orders', con=conn, if_exists='append', index=False)
            
        logger.info("Sync completed successfully.")

    except (SQLAlchemyError, Exception) as e:
        logger.error(f"Sync failed: {str(e)}")
        raise