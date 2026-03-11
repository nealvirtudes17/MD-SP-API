import time
import logging
from sp_api.api import Reports
from sp_api.base import Marketplaces, ProcessingStatus
from app.config import Config

logger = logging.getLogger(__name__)

def fetch_sp_api_report(report_type: str, start_date=None, end_date=None) -> str:
    """Creates a report, polls for completion, and returns the document ID."""
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

    doc_info = client.get_report_document(data.payload['reportDocumentId'])
    return doc_info.payload.get('url')