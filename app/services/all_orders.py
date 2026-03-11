import csv
import gzip
import logging
from datetime import datetime, timedelta, date

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Import from our centralized app modules
from app.database import engine
from app.api_client import fetch_sp_api_report
from sp_api.base import ReportType

logger = logging.getLogger(__name__)

def get_report_dates() -> tuple[str, str, str]:
    """Calculates API boundaries and SQL deletion cutoffs based on the day of the week."""
    day = datetime.now().strftime('%A')
    
    if day in ['Monday', 'Wednesday', 'Thursday']:
        api_start, api_end, del_start = 31, 1, 30
    else:
        api_start, api_end, del_start = 2, 1, 1

    dateas = datetime.now() + timedelta(hours=8)
    s_date = datetime.strftime(dateas - timedelta(api_start), '%Y-%m-%d') + 'T22:00:00.000Z'
    e_date = datetime.strftime(datetime.now() - timedelta(api_end), '%Y-%m-%d') + 'T22:00:00.000Z'
    
    cutoff_date = date.today() - timedelta(del_start)
    cutoff_dt = datetime.combine(cutoff_date, datetime.min.time()) + timedelta(hours=6)
    
    return s_date, e_date, cutoff_dt.strftime('%Y-%m-%d %H:%M:%S')

def transform_data(decoded_content: str) -> pd.DataFrame:
    """Cleans and formats the raw TSV string into a DataFrame."""
    reader = csv.DictReader(decoded_content.splitlines(), delimiter='\t')
    df = pd.DataFrame(reader)

    if df.empty:
        logger.warning("Empty DataFrame received. Skipping transformation.")
        return df

    df['amazon-order-id'] = df['amazon-order-id'].str.lstrip("\n")
    df['purchase-date'] = pd.to_datetime(df['purchase-date'], errors='coerce') + timedelta(hours=8)
    df['purchase-date'] = df['purchase-date'].dt.tz_localize(None)
    df['purchaseDate'] = df['purchase-date']
    
    df = df[:-1] 
    
    if 'is-iba' in df.columns:
        df['is-sold-by-ab'] = df['is-iba']
        df = df.drop(columns=['is-iba'])
        
    df = df.drop(columns=['purchase-date'], errors='ignore')

    if 'buyer-citizen-id ' in df.columns:
        df = df.rename(columns={'buyer-citizen-id ': 'buyer-citizen-id'})

    return df

def load_data(df: pd.DataFrame, cutoff_dt_str: str):
    """Executes the legacy delete constraint and loads new data via explicit transaction."""
    if df.empty:
        logger.info("No data to load for All Orders.")
        return

    try:
        # 2.0 Standard: explicit begin() for transactional safety
        with engine.begin() as conn:
            logger.info(f"Deleting old records where purchaseDate > {cutoff_dt_str}")
            stmt = text("DELETE FROM All_Orders WHERE purchaseDate > :cutoff_date")
            conn.execute(stmt, {"cutoff_date": cutoff_dt_str})
            
            logger.info(f"Appending {len(df)} new records to All_Orders.")
            # Eventually, this .to_sql should be replaced by session.execute(insert()) using the ORM
            df.to_sql(name='All_Orders', con=conn, if_exists='append', index=False)
            
    except SQLAlchemyError as e:
        logger.error(f"Database operation failed for All Orders: {e}")
        raise

def execute_sync():
    """Main execution function called by the orchestrator."""
    logger.info("Starting All Orders sync...")
    s_date, e_date, cutoff_dt_str = get_report_dates()
    
    # 1. Fetch Report URL using our shared API client
    report_url = fetch_sp_api_report(
        report_type=ReportType.GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL,
        start_date=s_date,
        end_date=e_date
    )
    
    # 2. Download & Decode
    res = requests.get(report_url)
    try:
        decoded_content = res.content.decode('utf-8')
    except UnicodeDecodeError:
        decoded_content = gzip.decompress(res.content).decode('utf-8')

    # 3. Transform
    clean_df = transform_data(decoded_content)
    
    # 4. Load
    load_data(clean_df, cutoff_dt_str)
    logger.info("All Orders sync complete.")