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
    """Calculates API boundaries and SQL deletion cutoffs based on the day of the week."""
    day = datetime.now().strftime('%A')
    
    # Legacy logic: Deep sync on Mon/Wed/Thu, shallow sync otherwise
    if day in ['Monday', 'Wednesday', 'Thursday']:
        api_start, api_end, del_start = 31, 1, 30
    else:
        api_start, api_end, del_start = 2, 1, 1

    # API Date Calculation
    dateas = datetime.now() + timedelta(hours=8)
    s_date = (dateas - timedelta(api_start)).strftime('%Y-%m-%d') + 'T22:00:00.000Z'
    e_date = (datetime.now() - timedelta(api_end)).strftime('%Y-%m-%d') + 'T22:00:00.000Z'
    
    # SQL Cutoff Date Calculation
    cutoff_date = date.today() - timedelta(del_start)
    cutoff_dt = datetime.combine(cutoff_date, datetime.min.time()) + timedelta(hours=6)
    cutoff_str = cutoff_dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return s_date, e_date, cutoff_str

def transform_data(decoded_content: str) -> pd.DataFrame:
    """Transforms raw TSV data and formats dates/columns strictly per legacy requirements."""
    reader = csv.DictReader(decoded_content.splitlines(), delimiter='\t')
    df = pd.DataFrame(reader)

    if df.empty:
        return df

    # 1. Date Formatting
    if 'approval-date' in df.columns:
        df['approval-date'] = pd.to_datetime(df['approval-date'], errors='coerce') + timedelta(hours=8)
        df['approval-date'] = df['approval-date'].dt.tz_localize(None)

    # 2. Drop unwanted legacy columns safely
    columns_to_drop = ['original-reimbursement-id', 'original-reimbursement-type']
    df = df.drop(columns=columns_to_drop, errors='ignore')

    # 3. Handle missing values
    return df.replace({np.nan: None})

def execute_sync():
    """Main ETL entry point for FBA Reimbursements."""
    logger.info("Starting Reimbursements synchronization...")
    s_date, e_date, cutoff_str = get_sync_parameters()
    
    try:
        # Step 1: Fetch Report (Bug in legacy dateEndTime fixed automatically by api_client)
        content = fetch_sp_api_report(ReportType.GET_FBA_REIMBURSEMENTS_DATA, s_date, e_date)

        # Step 2: Transform
        clean_df = transform_data(content)
        
        if clean_df.empty:
            logger.info("No records found in the Reimbursements report.")
            return

        # Step 3: Transactional Load
        # Uses engine.begin() for atomic delete-and-insert
        with engine.begin() as conn:
            logger.info(f"Clearing existing data where approval-date > {cutoff_str}...")
            
            # Backticks are required around `approval-date` because of the hyphen
            delete_stmt = text("DELETE FROM Reimbursement WHERE `approval-date` > :cutoff")
            conn.execute(delete_stmt, {"cutoff": cutoff_str})
            
            logger.info(f"Appending {len(clean_df)} records to Reimbursement...")
            clean_df.to_sql(name='Reimbursement', con=conn, if_exists='append', index=False)
            
        logger.info("Reimbursements sync completed successfully.")

    except (SQLAlchemyError, Exception) as e:
        logger.error(f"Sync failed for Reimbursements: {str(e)}")
        raise