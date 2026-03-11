import os
import time
import csv
import gzip
import logging
from datetime import datetime, timedelta, date
from typing import Dict, Any

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from sp_api.api import Reports
from sp_api.base import Marketplaces, ReportType, ProcessingStatus

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load Environment Variables
load_dotenv()

# Engine Configuration (SQLAlchemy 2.0 Standard)
engine = create_engine(
    os.getenv("DB_URL"),
    pool_pre_ping=True,  # Ensures connections are alive
    pool_recycle=3600
)

def get_sp_api_credentials() -> Dict[str, str]:
    return {
        "lwa_app_id": os.getenv("lwa_app_id"),
        "lwa_client_secret": os.getenv("lwa_client_secret"),
        "aws_access_key": os.getenv("aws_access_key"),
        "aws_secret_key": os.getenv("aws_secret_key"),
        "role_arn": os.getenv("role_arn"),
        "refresh_token": os.getenv("refresh_token")
    }

def get_report_dates() -> tuple[str, str, str]:
    """Calculates API boundaries and SQL deletion cutoffs based on the day of the week."""
    day = datetime.now().strftime('%A')
    
    if day in ['Monday', 'Wednesday', 'Thursday']:
        api_start = 31
        api_end = 1
        del_start = 30
    else:
        api_start = 2
        api_end = 1
        del_start = 1

    dateas = datetime.now() + timedelta(hours=8)
    s_date = datetime.strftime(dateas - timedelta(api_start), '%Y-%m-%d') + 'T22:00:00.000Z'
    e_date = datetime.strftime(datetime.now() - timedelta(api_end), '%Y-%m-%d') + 'T22:00:00.000Z'
    
    cutoff_date = date.today() - timedelta(del_start)
    cutoff_dt = datetime.combine(cutoff_date, datetime.min.time()) + timedelta(hours=6)
    
    return s_date, e_date, cutoff_dt.strftime('%Y-%m-%d %H:%M:%S')

def fetch_report_data(s_date: str, e_date: str, credentials: Dict[str, str]) -> pd.DataFrame:
    """Requests and downloads the report from Amazon SP-API."""
    logger.info("Requesting report from SP-API...")
    report_api = Reports(credentials=credentials, marketplace=Marketplaces.DE)
    
    res = report_api.create_report(
        reportType=ReportType.GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL,
        dataStartTime=s_date,
        dataEndTime=e_date,
    )
    
    report_id = res.payload['reportId']
    
    while True:
        data = report_api.get_report(report_id)
        status = data.payload.get('processingStatus')
        if status in [ProcessingStatus.DONE, ProcessingStatus.FATAL, ProcessingStatus.CANCELLED]:
            break
        logger.info("Report processing... sleeping for 5s.")
        time.sleep(5)

    if status != ProcessingStatus.DONE:
        raise RuntimeError(f"Report generation failed with status: {status}")

    report_data = report_api.get_report_document(data.payload['reportDocumentId'])
    read_url = requests.get(report_data.payload.get('url'))

    try:
        decoded_content = read_url.content.decode('utf-8')
    except UnicodeDecodeError:
        logger.info("Failed standard decode, attempting GZIP decompression.")
        decompressed_data = gzip.decompress(read_url.content)
        decoded_content = decompressed_data.decode('utf-8')

    reader = csv.DictReader(decoded_content.splitlines(), delimiter='\t')
    return pd.DataFrame(reader)


logger = logging.getLogger(__name__)

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and formats the raw DataFrame."""
    if df.empty:
        logger.warning("Empty DataFrame received. Skipping transformation.")
        return df

    return (
        df
        .assign(
            # Unpack dictionary to handle hyphenated column names in .assign()
            **{'amazon-order-id': lambda df_: df_['amazon-order-id'].str.lstrip("\n")},
            
            # Vectorized datetime conversions mapped to the new column name
            purchaseDate=lambda df_: (
                pd.to_datetime(df_['purchase-date'], errors='coerce') 
                + pd.Timedelta(hours=8)
            ).dt.tz_localize(None)
        )
        .rename(columns={
            'is-iba': 'is-sold-by-ab',
            'buyer-citizen-id ': 'buyer-citizen-id'
        })
        .drop(columns=['purchase-date'], errors='ignore')
        .iloc[:-1] # Explicitly index over ambiguous slicing
    )

def load_data(df: pd.DataFrame, cutoff_dt_str: str):
    """Executes the legacy delete constraint and loads new data inside a transaction."""
    if df.empty:
        logger.info("No data to load.")
        return

    # Using explicit transactions (engine.begin()) guarantees commit/rollback
    try:
        with engine.begin() as conn:
            logger.info(f"Deleting old records where purchaseDate > {cutoff_dt_str}")
            # REQUIRED 2.0 PATTERN: text() with bound parameters (prevents SQL injection)
            stmt = text("DELETE FROM All_Orders WHERE purchaseDate > :cutoff_date")
            conn.execute(stmt, {"cutoff_date": cutoff_dt_str})
            
            logger.info(f"Appending {len(df)} new records.")
            df.to_sql(name='All_Orders', con=conn, if_exists='append', index=False)
            
        logger.info("Database transaction committed successfully.")
    except SQLAlchemyError as e:
        logger.error(f"Database operation failed: {e}")
        raise

def main():
    start_time = time.time()
    
    try:
        creds = get_sp_api_credentials()
        s_date, e_date, cutoff_dt_str = get_report_dates()
        
        raw_df = fetch_report_data(s_date, e_date, creds)
        clean_df = transform_data(raw_df)
        
        load_data(clean_df, cutoff_dt_str)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
    finally:
        engine.dispose()
        logger.info(f"Pipeline finished in {time.time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()