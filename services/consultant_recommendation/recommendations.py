import os,uuid,pandas as pd,asyncio
import logging
from pathlib import Path
from sqlalchemy import text as querytext

from core.db.crud import DatabaseManager
from core.dellattachments import DellAttachments
from core.exceptions import DatabaseReadError, DatabaseWriteError, DellAttachmentsApiError, DellAttachmentsDownloadError, DellAttachmentsUploadError, UnExpectedError, UnableToFindAnyRecommendation
from core.utility import get_custom_logger
from config import Config as cfg

logger = get_custom_logger(__name__)

async def generate_recommendations(debug: bool=False,**kwargs):
    requestId = kwargs.get('requestId')
    project_id = kwargs.get('projectId')
    logger.info(f'Looking documents for project_id: {project_id}')
    try:
        # request_id = uuid.UUID(request_id_str)
        # logger.info(f"Converted to UUID: {requestId}")
        db = DatabaseManager(debug)
        with db.engine.connect() as conn:
            query = querytext(f"""SELECT * FROM {cfg.DATABASE_SCHEMA}.{cfg.GENERATE_RECOMMENDATION_FUNCTION}(:request_id)""")
            logger.info(f"{requestId} - Executing database function with request_id: {requestId}")
            recdf = pd.read_sql(query, conn, params={'request_id': requestId})
            conn.commit()
            
        logger.info(f"{requestId} - Retrieved {recdf.shape[0]} recommendations from database function")
        if recdf.shape[0]==0:
            logger.warning(f"{requestId} - No recommendations found for requestId: {requestId}")
            raise UnableToFindAnyRecommendation()
    except UnableToFindAnyRecommendation as e:
        raise e
    except Exception as e:
        logger.error(f"{requestId} - Error reading from database function: {e}", exc_info=True)
        raise DatabaseReadError(error=e)
    # finally:
    #     if 'db' in locals(): del db
    
    uuid_columns = recdf.select_dtypes(include=['object']).columns
    recdf[uuid_columns] = recdf[uuid_columns].apply(lambda x: x.astype(str))

    #remove this if we need to upload and download the rec files
    # recdf.drop(columns=['dafileid'], inplace=True)
    recdf.rename(columns={'projectid': 'FF_ProjectId'}, inplace=True)

    reccommendations = recdf.to_dict(orient='records')
    logger.info(f'{requestId} - Retrieved recommendations')
 
    return reccommendations

def remove_keys(pdict: dict, keylist: list = [ 'createdBy', 'updatedBy', 'createdOn', 'updatedOn']):
    # removeKeys = [ 'createdBy', 'updatedBy', 'createdOn', 'updatedOn']
    for key in keylist:    
        if key in pdict: pdict.pop(key, None) 

async def get_recommendations(header,payload, debug):
    remove_keys(payload)
    remove_keys(header)
    if debug:
            logger.setLevel(logging.DEBUG)
    
    # requestId = uuid.UUID(header.pop('requestId'))
    payload = await generate_recommendations(debug,**payload,**header)
   
    return payload


