import logging
from sqlalchemy import create_engine
from app.config import Config

logger = logging.getLogger(__name__)


engine = create_engine(
    Config.DB_URL,
    pool_pre_ping=True, 
    pool_recycle=3600    
)