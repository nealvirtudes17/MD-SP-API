import logging
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sp_api.base.exceptions import SellingApiException

from app.database import engine
from app.api_client import get_catalog_item

logger = logging.getLogger(__name__)

# Target SKUS to get sales rank, we may query in db products table or create a view for ease
TARGET_SKUS = [
    'DE-SHADE-FOREST-S&L', 'DE-SHADE-NOPRINT-S&L', 'DE-SHADE-SEALIFE-S&L',
    'DE-SHADE-WILDLIFE-S&L', 'SHADE-WILDLIFE-DARK-S&L', 'SHADE-BLACK-DARK-S&L',
    'U6-BOU6-LX95', '6W-V6GS-CRXQ', 'BND-KICKMAT1-SHADES', 'CA-3X7Q-WLRR',
    'DE-SHADE-BLACK-S&L', 'DE-SHADE-FAIRIES-S&L', 'DE-SHADE-SPACE-S&L',
    'SEAT-PROTECTOR-GREY', 'Z0-BQXD-9XGD'
]

def is_already_synced(today_date_str: str) -> bool:
    """
    Idempotency Check: Uses a highly performant scalar query to check if data 
    for today already exists. Prevents duplicate rows on multiple pipeline runs.
    """
    with engine.connect() as conn:
        stmt = text("SELECT 1 FROM Sales_Rank WHERE Date = :today LIMIT 1")
        result = conn.execute(stmt, {"today": today_date_str}).fetchone()
        return result is not None

def fetch_and_transform(today_date_obj) -> pd.DataFrame:
    """
    Iterates through SKUs, handles rate limits, and extracts Sales Ranks.
    Uses 'Fail Fast' strict schema enforcement to prevent silent data corruption.
    """
    rows = []
    
    for sku in TARGET_SKUS:
        try:
            logger.info(f"Fetching Catalog data for SKU: {sku}")
            res = get_catalog_item(sku)
            
            # STRICT SCHEMA ENFORCEMENT (Fail Fast)
            # We use bracket notation [] instead of .get(). If Amazon changes their API 
            # and omits 'Items', this triggers a KeyError and immediately stops the pipeline.
            items = res.payload['Items'] 
            
            for item in items:
                # Demanding exact JSON hierarchy for ASIN and Rankings
                asin = item['Identifiers']['MarketplaceASIN']['ASIN']
                rankings = item['SalesRankings']
                
                for i, ranking in enumerate(rankings):
                    # Demanding the Rank exists
                    rank = ranking['Rank']
                    
                    # The first ranking item is always the Main Category, subsequent are Sub Categories
                    category = 'Main Category' if i == 0 else 'Sub Category'
                    
                    rows.append({
                        'Date': today_date_obj,
                        'ASIN': asin,
                        'Rank': rank,
                        'Category': category
                    })
            
            # THROTTLING PROTECTION: 
            # The Amazon Catalog API is synchronous and heavily rate-limited.
            # We sleep for 0.5 seconds between requests to prevent QuotaExceeded errors.
            time.sleep(0.5)
            
        except SellingApiException as e:
            # We specifically catch SellingApiException because network timeouts or 
            # Amazon server errors are expected. We log the warning and try the next SKU.
            logger.warning(f"Amazon API Error for SKU {sku}: {e}. Skipping to next SKU.")
            continue
            
        # Note: We do NOT catch standard Exceptions (like KeyError or ValueError). 
        # If the JSON schema changes, we want the loop to crash so we can fix the code,
        # rather than quietly filling the database with blank data.

    # Convert the extracted rows into a Pandas DataFrame
    df = pd.DataFrame(rows)
    
    if not df.empty:
        # DB SCHEMA ENFORCEMENT
        # errors='raise' guarantees that if Amazon returns a letter instead of a number
        # for a Rank, Pandas will crash rather than inserting a NaN or string into the SQL DB.
        df['Rank'] = pd.to_numeric(df['Rank'], errors='raise').astype(int)
        
    return df

def execute_sync():
    """Main ETL entry point for the Sales Rank service."""
    logger.info("Starting Sales Rank synchronization...")
    
    # 1. Generate both the String (for SQL queries) and the Object (for Pandas/DB Schema)
    today_date_obj = datetime.now().date()
    today_date_str = today_date_obj.strftime('%Y-%m-%d')
    
    try:
        # Step 1: Idempotency Check
        if is_already_synced(today_date_str):
            logger.info(f"Data for {today_date_str} already exists in Sales_Rank. Skipping.")
            return

        # Step 2 & 3: Extract & Transform
        clean_df = fetch_and_transform(today_date_obj)
        
        if clean_df.empty:
            logger.warning("No sales rank records were successfully fetched.")
            return

        # Step 4: Transactional Load (Push to DB)
        # Using engine.begin() opens a database transaction.
        # If the df.to_sql insert fails halfway through, the database automatically rolls back
        # the entire transaction, preventing partial data loads.
        with engine.begin() as conn:
            logger.info(f"Appending {len(clean_df)} records to Sales_Rank table...")
            clean_df.to_sql(name='Sales_Rank', con=conn, if_exists='append', index=False)
            
        logger.info("Sales Rank sync completed successfully.")

    except (SQLAlchemyError, Exception) as e:
        # This catches Database errors OR the KeyErrors from our Fail Fast logic above
        logger.error(f"Sync failed for Sales Rank: {str(e)}")
        raise