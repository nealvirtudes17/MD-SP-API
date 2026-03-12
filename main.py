import logging
from app.database import engine
from app.services import (
    all_orders, 
    reserved_inventory, 
    manage_fba_inventory,
    sales_rank,
    snl_status,
    transactions,
    reimbursements  # <-- Import the final service
)

# Production-grade logging configuration
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def run_pipeline():
    """Executes the complete Amazon SP-API ETL Pipeline."""
    try:
        logger.info("=== Starting Amazon SP-API ETL Pipeline ===")
        
        # 1. Orders & Financials
        all_orders.execute_sync()
        transactions.execute_sync()
        reimbursements.execute_sync() 
        
        # 2. Inventory Management
        reserved_inventory.execute_sync()
        manage_fba_inventory.execute_sync()
        
        # 3. Product Catalog
        sales_rank.execute_sync()
        snl_status.execute_sync()
        
        logger.info("=== Pipeline Completed Successfully ===")
        
    except Exception as e:
        logger.error(f"Pipeline terminated due to critical failure: {e}")
        
    finally:
        # MANDATORY: Release the SQLAlchemy connection pool gracefully
        engine.dispose()
        logger.info("Database connection pool released.")

if __name__ == "__main__":
    run_pipeline()