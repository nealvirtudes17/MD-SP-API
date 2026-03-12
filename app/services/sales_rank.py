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

# Target SKUs extracted from legacy script
TARGET_SKUS = [
    'DE-SHADE-FOREST-S&L', 'DE-SHADE-NOPRINT-S&L', 'DE-SHADE-SEALIFE-S&L',
    'DE-SHADE-WILDLIFE-S&L', 'SHADE-WILDLIFE-DARK-S&L', 'SHADE-BLACK-DARK-S&L',
    'U6-BOU6-LX95', '6W-V6GS-CRXQ', 'BND-KICKMAT1-SHADES', 'CA-3X7Q-WLRR',
    'DE-SHADE-BLACK-S&L', 'DE-SHADE-FAIRIES-S&L', 'DE-SHADE-SPACE-S&L',
    'SEAT-PROTECTOR-GREY', 'Z0-BQXD-9XGD'
]

def is_already_synced(today_date_str: str) -> bool:
    """Uses a highly performant scalar query to check for existing records."""
    with engine.connect() as conn:
        stmt = text("SELECT 1 FROM Sales_Rank WHERE Date = :today LIMIT 1")
        result = conn.execute(stmt, {"today": today_date_str}).fetchone()
        return result is not None

def fetch_and_transform(today_date_obj) -> pd.DataFrame:
    """Iterates through SKUs, handles rate limits, and extracts Sales Ranks."""
    rows = []
    
    for sku in TARGET_SKUS:
        try:
            logger.info(f"Fetching Catalog data for SKU: {sku}")
            res = get_catalog_item(sku)
            items = res.payload.get('Items', [])
            
            for item in items:
                # Safely navigate nested JSON
                asin = item.get('Identifiers', {}).get('MarketplaceASIN', {}).get('ASIN', '')
                rankings = item.get('SalesRankings', [])
                
                for i, ranking in enumerate(rankings):
                    rank = ranking.get('Rank')
                    category = 'Main Category' if i == 0 else 'Sub Category'
                    
                    rows.append({
                        'Date': today_date_obj,
                        'ASIN': asin,
                        'Rank': rank,
                        'Category': category
                    })
            
            # THROTTLING PROTECTION: Amazon Catalog API is heavily rate-limited
            # Sleep for 0.5 seconds between synchronous requests to prevent QuotaExceeded errors
            time.sleep(0.5)
            
        except SellingApiException as e:
            logger.warning(f"Amazon API Error for SKU {sku}: {e}. Skipping to next SKU.")
            continue
        except Exception as e:
            logger.error(f"Data extraction failed for SKU {sku}: {e}. Skipping.")
            continue

    df = pd.DataFrame(rows)
    
    if not df.empty:
        # Schema Enforcement: Ensure Rank is explicitly cast to an integer
        df['Rank'] = pd.to_numeric(df['Rank'], errors='coerce').fillna(0).astype(int)
        
    return df

def execute_sync():
    """Main ETL entry point for Sales Rank."""
    logger.info("Starting Sales Rank synchronization...")
    
    today_date_obj = datetime.now().date()
    today_date_str = today_date_obj.strftime('%Y-%m-%d')
    
    try:
        # Step 1: Idempotency Check
        if is_already_synced(today_date_str):
            logger.info(f"Data for {today_date_str} already exists in Sales_Rank. Skipping.")
            return

        # Step 2 & 3: Fetch and Transform
        clean_df = fetch_and_transform(today_date_obj)
        
        if clean_df.empty:
            logger.warning("No sales rank records were successfully fetched.")
            return

        # Step 4: Transactional Load
        with engine.begin() as conn:
            logger.info(f"Appending {len(clean_df)} records to Sales_Rank...")
            clean_df.to_sql(name='Sales_Rank', con=conn, if_exists='append', index=False)
            
        logger.info("Sales Rank sync completed successfully.")

    except (SQLAlchemyError, Exception) as e:
        logger.error(f"Sync failed for Sales Rank: {str(e)}")
        raise