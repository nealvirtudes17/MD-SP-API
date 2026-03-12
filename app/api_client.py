import time
import logging
import requests
import gzip
from typing import Optional

from sp_api.api import Reports, Finances
from sp_api.base import Marketplaces, ProcessingStatus
from app.config import Config

logger = logging.getLogger(__name__)

def fetch_sp_api_report(report_type: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> str:
    """Creates a report, polls for completion, and returns the raw decoded content."""
    client = Reports(credentials=Config.get_sp_api_credentials(), marketplace=Marketplaces.DE)
    
    kwargs = {"reportType": report_type}
    if start_date and end_date:
        kwargs.update({"dataStartTime": start_date, "dataEndTime": end_date})
        
    res = client.create_report(**kwargs)
    report_id = res.payload['reportId']
    
    while True:
        data = client.get_report(report_id)
        status = data.payload.get('processingStatus')
        if status in [ProcessingStatus.DONE, ProcessingStatus.FATAL, ProcessingStatus.CANCELLED]:
            break
        logger.info(f"Polling report {report_id}... Status: {status}")
        time.sleep(5)

    if status != ProcessingStatus.DONE:
        raise RuntimeError(f"Report {report_id} failed with status: {status}")

    # Download and decode the report
    doc_info = client.get_report_document(data.payload['reportDocumentId'])
    report_url = doc_info.payload.get('url')
    
    download_res = requests.get(report_url)
    download_res.raise_for_status()
    
    try:
        return download_res.content.decode('utf-8')
    except UnicodeDecodeError:
        try:
            return gzip.decompress(download_res.content).decode('utf-8')
        except Exception:
            # Fallback for reserved inventory legacy encoding
            return download_res.content.decode('cp1252', errors='ignore')

def get_financial_events(posted_after: str, posted_before: str):
    """Fetches financial events (transactions) from the Finances API."""
    client = Finances(credentials=Config.get_sp_api_credentials(), marketplace=Marketplaces.DE)
    return client.list_financial_events(PostedAfter=posted_after, PostedBefore=posted_before)