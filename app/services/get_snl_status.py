import logging
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sp_api.base.exceptions import SellingApiException

from app.database import engine
from app.api_client import get_snl_enrollment

logger = logging.getLogger(__name__)

# Target SKUs 
TARGET_SKUS = [
    'DE-SHADE-FOREST-S&L', 'DE-SHADE-NOPRINT-S&L', 'DE-SHADE-SEALIFE-S&L',
    'DE-SHADE-WILDLIFE-S&L', 'SHADE-WILDLIFE-DARK-S&L', 'SHADE-BLACK-DARK-S&L'
]

def is_already_synced(today_date_str: str) -> bool:
    """Idempotency check: prevents duplicate rows on multiple runs."""
    with engine.connect() as conn:
        stmt = text("SELECT 1 FROM SnL_Enrollment_Status WHERE Date = :today LIMIT 1")
        result = conn.execute(stmt, {"today": today_date_str}).fetchone()
        return result is not None

def fetch_and_transform(today_date_obj) -> pd.DataFrame:
    """Iterates through SKUs and extracts SnL status with strict schema checking."""
    rows = []
    
    for sku in TARGET_SKUS:
        try:
            logger.info(f"Fetching SnL Enrollment status for SKU: {sku}")
            res = get_snl_enrollment(sku)
            
            # STRICT ENFORCEMENT & LEGACY BUG FIX
            # Replaced legacy `data('status')` with standard dictionary extraction
            status = res.payload['status']
            
            rows.append({
                'Date': today_date_obj,
                'SKU': sku,
                'Status': status
            })
            
            # THROTTLING PROTECTION: 
            # 0.5s sleep to protect against Amazon QuotaExceeded errors
            time.sleep(0.5)
            
        except SellingApiException as e:
            # Amazon SP-API often returns a 404 if a SKU is simply NOT enrolled
            # We catch it gracefully, log it, and move on.
            logger.warning(f"API Error (or Not Enrolled) for SKU {sku}: {e}. Skipping.")
            continue

    return pd.DataFrame(rows)

def execute_sync():
    """Main ETL entry point for SnL Enrollment Status."""
    logger.info("Starting SnL Enrollment Status synchronization...")
    
    today_date_obj = datetime.now().date()
    today_date_str = today_date_obj.strftime('%Y-%m-%d')
    
    try:
        # Step 1: Idempotency Check
        if is_already_synced(today_date_str):
            logger.info(f"Data for {today_date_str} already exists in SnL_Enrollment_Status. Skipping.")
            return

        # Step 2 & 3: Extract & Transform
        clean_df = fetch_and_transform(today_date_obj)
        
        if clean_df.empty:
            logger.warning("No SnL status records were successfully fetched.")
            return

        # Step 4: Transactional Load
        with engine.begin() as conn:
            logger.info(f"Appending {len(clean_df)} records to SnL_Enrollment_Status...")
            clean_df.to_sql(name='SnL_Enrollment_Status', con=conn, if_exists='append', index=False)
            
        logger.info("SnL Status sync completed successfully.")

    except (SQLAlchemyError, Exception) as e:
        logger.error(f"Sync failed for SnL Status: {str(e)}")
        raise