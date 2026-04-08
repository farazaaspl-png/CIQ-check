#main.py
import os, json
from pathlib import Path
import shutil
import time
from zipfile import BadZipFile

# import pandas as pd
import pendulum
# from sqlalchemy import text as querytext
 
from services.gtl_recommendation.classification.classification import Classifier
from core.dispatcher import Dispatcher
from core.db.crud import DatabaseManager
from core.dellattachments import DellAttachments
from core.exceptions import CustomBaseException, DellAttachmentsApiError, FileReadError, FileWriteError, EmptyFileError, NoSensitiveItemFound
from core.utility import get_custom_logger
from config import Config as cfg
from config import Configuration
logger = get_custom_logger(__name__)
# logging.getLogger().setLevel(logging.INFO)
# logger.propagate = False



OUTPUT_DIR = os.path.join(cfg.DATA_DIR,cfg.GTL_FLOW_DIR)
# da = DellAttachments()
output_topic = cfg.OUTPUT_TOPIC
def saveJson(jsondata, fileid, filename: str = ""):
    try:
        json_path = os.path.join(OUTPUT_DIR, str(fileid), f"{filename}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(jsondata, f, indent=2)
        return json_path
    except Exception as e:
        logger.error(f"Saving JSON failed at '{json_path}': {e}")
        raise FileWriteError(json_path, original_exc=e)
    
def clean_outdir(fileid):
    outpath = Path(os.path.join(OUTPUT_DIR, str(fileid)))
    if outpath.exists():
        try:
            shutil.rmtree(outpath)
        except PermissionError:
            time.sleep(10)  # wait for 10 seconds
            try:
                shutil.rmtree(outpath)
            except PermissionError as e:
                logger.error(f"{fileid}-->Failed to delete {outpath}: {e}")

def get_final_output(request_id,requestUuid,fileid,daFileId):
    try:
        db=DatabaseManager()
        df = db.get_vwclassificationout_row(requestid=request_id,daoriginal_fileid=fileid,dafileid=daFileId)
        # uuid_columns = df.select_dtypes(include=['object']).columns
        uuid_columns = ['requestid', 'daoriginal_fileid', 'dafileid', 'dasanitizationoutfileid', 'daclassificationoutfileid']
        # df[uuid_columns] = df[uuid_columns].apply(lambda x: x.astype(str))  
        df[uuid_columns] = df[uuid_columns].fillna('').astype(str)
 
        logger.info(f"{request_id}-{requestUuid}-Queried vwclassificationout view to get complete record for a document")
    except CustomBaseException as e:
        logger.error(f'{request_id}-{requestUuid}- {e}')
        raise e
    # finally:
    #     if 'db' in locals(): del db
   
    return(df.to_dict(orient='records')[0])

async def _classify_and_redact(fileid, filepath, debug, producer, **kwargs):
    # logger.info(f"Executing recommendation for the fileid: {fileid} and file: {filepath}")
    request_id = str(kwargs.get('requestId'))
    requestUuid = str(kwargs.get('uuid'))
    cfg=kwargs.get("config",None)
    dametadata = {"EPS_ProjectId": "consulting_workspace_IP_uploads",
                  "EPSSection": "other",
                  "SubSystem" : "consulting_ip_draft_workspace",
                  "CustomerUploaded": "false",
                  "FileVisibleExternal": "False"}
    da = DellAttachments(debug=cfg.DEBUG)
    def send_status(stage: str, status: str, message: str):
        if producer is None or output_topic is None:
            return  # No-op if not provided
        status_headers = {
            "eventType": "IP_GOLDEN_COPY_REQUEST_RECOMMENDATION_ACK",
            "eventSubType": "STATUS",
            "createdOn": pendulum.now("UTC").format("ddd MMM DD HH:mm:ss [UTC] YYYY"),
            "requestId": request_id,
        }
        status_payload = {
            "requestUuid": requestUuid,
            "stage": stage,
            "status": status,
            "message": message,
        }
        producer.send_message(output_topic, status_headers, status_payload)
        # logger.info(f"Sent status for stage:{stage}, status: {status}, message:{message}")

    db=DatabaseManager()
    logger.info(f"{request_id}-{requestUuid}-Started Classification for the fileid: {fileid} and file: {filepath}")
    dispatcher = Dispatcher(filepath,dafileid=fileid,analyze_images=cfg.IMAGE_ANALYZE_SWITCH,debug=cfg.DEBUG)
    extractor = dispatcher.getExtractor()
    try:
        extractor.extract_content()
    except BadZipFile as e:
        logger.error(f"{request_id}-{requestUuid}-Error processing file | file: {filepath} - {e}", exc_info=True)
        db.update_document(where_clause={'requestid': request_id,'daoriginal_fileid': fileid},update_values={'status': 'Bad Zip File'})
        send_status("CONTENT_EXTRACTION", "COMPLETED", "Bad Zip File")
        raise FileReadError(path=str(fileid), original_exc=e)
    filecontent = extractor.get_filecontent()
    if debug:
        out_text_path = os.path.join(cfg.DATA_DIR,cfg.GTL_FLOW_DIR,"filecontent.txt")
        with open(out_text_path, 'w', encoding='utf-8') as outf:
            outf.write(filecontent)
    
    db.update_document(where_clause={'requestid': request_id,'daoriginal_fileid': fileid},update_values={'status': 'Content Extracted'})
    send_status("CONTENT_EXTRACTION", "COMPLETED", "Content extracted successfully")
    
    # Validate file content is not empty
    if not filecontent or filecontent.strip() == '':
        logger.error(f"{request_id}-{requestUuid}-File is empty, no text content found")
        db.update_document(where_clause={'requestid': request_id,'daoriginal_fileid': fileid},update_values={'status': 'GOT EMPTY FILE'})
        raise EmptyFileError(fileid=fileid)
    
    # MODIFIED: Pass kwargs to Classifier constructor
    cls = Classifier(filecontent, debug=cfg.DEBUG, **kwargs)
    cls.classify()
    send_status("CLASSIFICATION", "COMPLETED", "Classification completed")

    if kwargs.get("eventSubType") == "MANUAL_UPLOAD":
        if hasattr(extractor, 'waspdf'):
            waspdf = extractor.waspdf
        else:
            waspdf = False

        if waspdf:
            daFileId = await da.upload(extractor.filepath, metadata=dametadata)
            logger.info(f"{request_id}-{requestUuid}-Uploaded redacted file to dell attachments {daFileId}")
        else:
            daFileId = fileid

        db=DatabaseManager()
        update_vals = {'status' : 'Manually Uploaded','dafileid': daFileId}
        db.update_document(where_clause={'requestid': request_id,'daoriginal_fileid': fileid},update_values=update_vals)
        clean_outdir(fileid)
        return get_final_output(request_id,requestUuid,fileid,daFileId)
    
    try:
        logger.info(f"{request_id}-{requestUuid}-Started Redaction for the fileid: {fileid} and file: {filepath}")
        daFileId, daSanitizationOutFileId = kwargs.get('daFileId'), None

        textRedactor, imageRedactor = dispatcher.getRedactors()
        filecontent = extractor.get_filecontent(True)
        isTextRedacted = textRedactor.sanitize(fileid,filecontent, **kwargs)
        extractor.clean_up() 
        
        outfilepath, redacteditemsfilepath = textRedactor.save(outdir=os.path.join(OUTPUT_DIR, str(fileid)))
        status = 'Text Redaction Completed' if isTextRedacted else 'No Text Redacted'
        db.update_document(where_clause={'requestid': request_id,'daoriginal_fileid': fileid},update_values={'status': status}) 
        logger.info(f"{request_id}-{requestUuid}-{status}")
        send_status("TEXT_REDACTION", "COMPLETED", status)

        isImageRedacted= False
        if imageRedactor is not None:
            isImageRedacted = imageRedactor.redact(tobe_redacted = textRedactor.tobe_redacted,imgprocessor = extractor.image_processor, **kwargs)
            status = 'Image Redaction Completed' if isTextRedacted else 'No Image Redacted'
            db.update_document(where_clause={'requestid': request_id,'daoriginal_fileid': fileid},update_values={'status': status}) 
            logger.info(f"{request_id}-{requestUuid}-{status}")
            send_status("IMAGE_REDACTION", "COMPLETED", status)

        
        if hasattr(extractor, 'waspdf'):
            waspdf = extractor.waspdf
        else:
            waspdf = False
            
        if any([isTextRedacted, isImageRedacted, waspdf]):
            daFileId = await da.upload(outfilepath, metadata=dametadata)
            logger.info(f"{request_id}-{requestUuid}-Uploaded redacted file to dell attachments {daFileId}")

        if redacteditemsfilepath is not None:
            try:
                daSanitizationOutFileId = await da.upload(redacteditemsfilepath, metadata=dametadata)
                logger.info(f"{request_id}-{requestUuid}-Redacted items file to dell attachments {daSanitizationOutFileId}")
            except DellAttachmentsApiError as e:
                logger.warning(f"{request_id}-{requestUuid}-Error uploading redacted items file to dell attachments: {e}")


        if any([isTextRedacted, isImageRedacted, waspdf]):
            send_status("FILE UPLOAD", "COMPLETED", "Uploaded redacted file and redacted items file to DA")
            update_vals = {'status' : 'File Uploaded to Dell Attachments'}
            update_vals['dafileid'] = daFileId
            if daSanitizationOutFileId is not None:
                update_vals['dasanitizationoutfileid'] = daSanitizationOutFileId
            db.update_document(where_clause={'requestid': request_id,'daoriginal_fileid': fileid},update_values=update_vals)
        else:
            update_vals = {'status' : 'Nothing Redacted From File','dafileid': daFileId}
            db.update_document(where_clause={'requestid': request_id,'daoriginal_fileid': fileid},update_values=update_vals)

    except NoSensitiveItemFound as e:
        logger.warning(f'{request_id}-{requestUuid}- {e}')
        update_vals = {'status' : 'No Sensitive Item Found','dafileid': daFileId}
        db.update_document(where_clause={'requestid': request_id,'daoriginal_fileid': fileid},update_values=update_vals)
    except CustomBaseException as e:
        logger.error(f'{request_id}-{requestUuid}-unable to upload file to dell attachments: {e}')
        raise e
 
    dbrecordJson = get_final_output(request_id,requestUuid,fileid,daFileId)
    # try:
    #     db=DatabaseManager()
    #     df = db.get_vwclassificationout_row(requestid=request_id,daoriginal_fileid=fileid,dafileid=daFileId)
    #     # uuid_columns = df.select_dtypes(include=['object']).columns
    #     uuid_columns = ['requestid', 'daoriginal_fileid', 'dafileid', 'dasanitizationoutfileid', 'daclassificationoutfileid']
    #     # df[uuid_columns] = df[uuid_columns].apply(lambda x: x.astype(str))  
    #     df[uuid_columns] = df[uuid_columns].fillna('').astype(str)
 
    #     logger.info(f"{request_id}-{requestUuid}-Queried vwclassificationout view to get complete record for a document")
    # except CustomBaseException as e:
    #     logger.error(f'{request_id}-{requestUuid}- {e}')
    #     raise e
    # # finally:
    # #     if 'db' in locals(): del db
   
    # dbrecordJson = df.to_dict(orient='records')[0]
    # jsonfilepath = saveJson(dbrecordJson, fileid, filepath.stem + '_metadata')
   
    # try:
    #     da = DellAttachments(debug=cfg.DEBUG)
    #     dametadata = {"EPS_ProjectId": "consulting_workspace_IP_uploads",
    #                   "EPSSection": "other",
    #                   "SubSystem" : "consulting_ip_draft_workspace",
    #                   "CustomerUploaded": "false",
    #                   "FileVisibleExternal": "False"}
    #     daclassificationoutfileid = await da.upload(jsonfilepath, metadata=dametadata)
    #     dbrecordJson['daclassificationoutfileid'] = daclassificationoutfileid
    #     logger.info(f"{request_id}-{requestUuid}-Uploaded classification output file to dell attachments {daclassificationoutfileid}")
    # except CustomBaseException as e:
    #     logger.error(f'{request_id}-{requestUuid}-unable to upload file to dell attachments: {e}')
    #     raise e
    # else:
    #     dbrecordJson['daclassificationoutfileid'] = str(uuid.uuid4())
    clean_outdir(fileid)
    # logger.info(f"{request_id}-{requestUuid}-Cleaned outdir")
    # try:
    #     db=DatabaseManager()
    #     db.update_document(
    #                 where_clause={'daoriginal_fileid': dbrecordJson['daoriginal_fileid'], 'requestid': dbrecordJson['requestid']},
    #                 update_values={'daclassificationoutfileid': dbrecordJson['daclassificationoutfileid']}
    #             )
    #     logger.info(f"{request_id}-{requestUuid}-update classification output file id to database")
    # except CustomBaseException as e:
    #     logger.error(f'{request_id}-{requestUuid}- {e}')
    #     raise e
    db.update_document(where_clause={'requestid': request_id,'daoriginal_fileid': fileid},update_values={'status': 'COMPLETED'}) 
    logger.info(f"{request_id}-{requestUuid}-Classification and Redaction Completed")
    send_status("PIPELINE", "COMPLETED", "Classification and redaction pipeline completed")
    
    return dbrecordJson
 


def remove_keys(pdict: dict, keylist: list = [ 'createdBy', 'updatedBy', 'createdOn', 'updatedOn']):
    for key in keylist:    
        if key in pdict: pdict.pop(key, None)
 
async def main(header, payload, producer, debug=False):
    remove_keys(payload)
    remove_keys(header)

    cfg=Configuration()
    cfg.load_active_config() 
   
    fdict = {'id':payload["daFileId"],
             'name':payload["name"],
             'filepath':os.path.join(os.path.join(cfg.INDIR,cfg.GTL_FLOW_DIR,payload["daFileId"], payload["name"]))
             }
    # if Path(payload["name"]).suffix not in SUPPORTED_FILES:
    #     raise FileFormatNotSupported(fileformat=Path(payload["name"]).suffix)
    
    if not Path(fdict['filepath']).exists():
        logger.info(f"{header['requestId']}-{payload["uuid"]}-Downloading file from dell attachments")
        da = DellAttachments(cfg.DEBUG)
        fdict = await da.download(filedict=fdict)
        logger.info(f"{header['requestId']}-{payload["uuid"]}-File downloaded from dell attachments")
 
    fileid = fdict['id']
    filepath = fdict['filepath']
    # payload = await _classify_and_redact(fileid, filepath,**payload,**header, debug=cfg.DEBUG)
    payload = await _classify_and_redact(
        fileid, filepath, 
        debug=debug, 
        producer=producer, 
        **payload, **header, config=cfg
    )
    
    return payload