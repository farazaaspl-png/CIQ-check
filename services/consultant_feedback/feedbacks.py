from core.db.crud import DatabaseManager
from core.exceptions import InvalidMetadataError, CustomBaseException
from config import Config as cfg
from core.utility import get_custom_logger
from sqlalchemy import text as querytext
import logging

logger = get_custom_logger(__name__)

def remove_keys(pdict: dict, keylist: list = [ 'createdBy', 'updatedBy', 'createdOn', 'updatedOn']):
    # removeKeys = [ 'createdBy', 'updatedBy', 'createdOn', 'updatedOn']
    for key in keylist:    
        if key in pdict: pdict.pop(key, None) 

def record_feedback(header,payload,debug):
    if debug:
        logger.setLevel(logging.DEBUG)
    remove_keys(payload)
    remove_keys(header)
    status = {'ACCEPT_RECOMMENDATION': "ACCEPTED", 'SKIP_RECOMMENDATION': "SKIPPED"}
    try:
        projectid = payload.get('projectId')
        requestid = header.get("requestId")

        logger.info(f"{requestid}-Started Recording feedback")
        db = DatabaseManager(cfg.DEBUG)
        sql = querytext(f"""
                SELECT rec.requestid
                FROM {cfg.DATABASE_SCHEMA}.{cfg.RECOMMENDATION_TABLE} rec
                WHERE rec.projectid = :projectid
                ORDER BY rec.created_date DESC
                LIMIT 1;
            """)
        latestrequestId = db.query_database(sql,{'projectid': projectid})['requestid'][0]
        logger.info(f"{requestid}-Fetched latest requestId: {latestrequestId}")

        db.update_recommendation(where_clause={
            'templateid': payload.get("recommendationId"),
            'requestid': latestrequestId
            },
            update_values={'status': status[header.get("eventSubType")],
            'userquery': payload.get("reason",None)})
    except CustomBaseException as exc:
        raise exc
    # finally:
    #     if 'db' in locals(): del db