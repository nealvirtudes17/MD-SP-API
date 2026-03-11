import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from app.config import Config

logger = logging.getLogger(__name__)

# 2.0 Standard: Robust pooling for long-running ETLs
engine = create_engine(
    Config.DB_URL,
    pool_pre_ping=True,
    pool_recycle=3600
)

class Base(DeclarativeBase):
    pass