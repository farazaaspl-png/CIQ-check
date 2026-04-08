import asyncio, json, os
from pathlib import Path
import logging
from core.exceptions import  CustomBaseException,FileFormatNotSupported,TextExtractionError
from core.dispatcher import Dispatcher
from services.consultant_recommendation.summary import SowSummarizer
from core.db.crud import DatabaseManager
from core.dellattachments import DellAttachments
from config import Configuration
from core.utility import get_custom_logger

# logging.getLogger().setLevel(logging.INFO)
logger = get_custom_logger(__name__)
# logger.propagate = False



def saveJson(jsondata, json_path):
    json_path = Path(json_path)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(jsondata, f, indent=2)
    except CustomBaseException as e:
        # raise FileWriteError(json_path, original_exc=e)
        raise e
            

async def call_summarization(fileid, filepath, debug= False, **kwargs):
    cfg=kwargs.get("config",None)
    # logger.info(f"Executing recommendation for the fileid: {fileid} and file: {filepath}")
    dispatcher = Dispatcher(filepath,fileid,analyze_images=cfg.IMAGE_ANALYZE_SWITCH,debug=cfg.DEBUG)
   
    try:
        extractor =  dispatcher.getSOWExtractor()
        _ = extractor.extract_content()
        filecontent , _ = extractor.get_filecontent()
        extractor.clean_up()
        
        logger.info(f"{fileid}-->File content extracted: {len(filecontent)} characters")
        logger.debug(f"{fileid}-->File content {filecontent}")
        if not filecontent.strip():
            raise TextExtractionError("No text content extracted from file",fileid)
    except CustomBaseException as e:
        logger.error(f"{fileid}-->Error processing file | file: {filepath} - {e}", exc_info=True)
        # raise TextExtractionError(str(e),fileid)
        raise e

    try:
        sow = SowSummarizer(fileid, filecontent,debug=cfg.DEBUG)
        fout = sow.summarize()
    except CustomBaseException as e:
        extractor.clean_up()
        raise e
    # return fout
    fout['requestId'] = kwargs.get('requestId')
    fout['uniqueid'] = kwargs.get('uuid')
    fout['projectid'] = kwargs.get('projectId', 'NA')
    fout['sowfilename'] = kwargs.get('name', 'NA')
    fout['dafileid'] = str(fileid)
    # return fout
    
    try:
        db=DatabaseManager(debug=cfg.DEBUG)
        # db.delete_sow(projectid=fout['projectid'])
        db.insert_sow(fout)
        # if 'db' in locals(): del db
    except CustomBaseException as e:
        logger.error(f"{fileid}-->Error inserting sow | file: {filepath}- {e}", exc_info=True)
        raise e

    return {"FF_ProjectId":  fout['projectid'],
            # "daFileId": str(outFileId),
            # "filename": Path(outfilepath).name,
            "summary": fout['summary'],
            "sowdaFileIds": [str(fileid)],
            "status": "SUCCESS"
            }

def remove_keys(pdict: dict):
    removeKeys = [ 'createdBy', 'updatedBy', 'createdOn', 'updatedOn']
    for key in removeKeys:    
        if key in pdict: pdict.pop(key, None) 

async def main(header,payload,debug=False):
    remove_keys(payload)
    remove_keys(header)

    cfg=Configuration()
    cfg.load_active_config()

    filename = Path(payload["name"]).stem[:20]+Path(payload["name"]).suffix
    fdict = {'id':payload["daFileId"],
             'name':payload["name"],
             'filepath':os.path.join(cfg.INDIR,cfg.CONSULTANT_FLOW_DIR,header["requestId"], filename)}
    if Path(fdict['filepath']).exists():
        logger.info(f'{payload["daFileId"]}-->File {fdict['filepath']} present locally')
    else:
        asyncio.sleep(10)
        try:
            da = DellAttachments(debug=cfg.DEBUG)
            fdict = await da.download(filedict=fdict)
        except CustomBaseException as e:
            logger.error(f"{payload["daFileId"]}-->Error downloading Sow from dell attachments | file: {payload['name']}- {e}", exc_info=True)
            # raise DellAttachmentsDownloadError(f"Unable to download Sow Summarization output from dell attachments: Error -{str(e)}")
            raise e

    fileid = fdict['id']
    filepath = fdict['filepath']
    payload = await call_summarization(fileid, filepath, debug=cfg.DEBUG, **payload,**header,config=cfg)

    return payload