import logging
from app.database import engine
from app.services import all_orders, reserved_inventory, transactions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_pipeline():
    try:
        logger.info("Starting ETL Pipeline...")
        
        # 1. Run All Orders
        all_orders.execute_sync()
        
        
    except Exception as e:
        logger.error(f"Pipeline failure: {e}")
    finally:
        # Crucial: Release connection pool
        engine.dispose()
        logger.info("Pipeline terminated safely.")

if __name__ == "__main__":
    run_pipeline()