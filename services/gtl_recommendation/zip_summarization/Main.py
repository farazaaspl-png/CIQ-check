import os
import pandas as pd
from pathlib import Path

from config import Configuration
from core.db.crud import DatabaseManager
from core.exceptions import FileFormatNotSupported
from services.gtl_recommendation.zip_summarization.workflow import GlobalContext, WorkflowOrchestrator

from core.utility import get_custom_logger
logger = get_custom_logger(__name__)

def remove_keys(pdict: dict, keylist: list = ['createdBy', 'updatedBy', 'createdOn', 'updatedOn', 'uploadedBy', 'leadTime', 'processingTime', 'status']):
    for key in keylist:    
        if key in pdict: pdict.pop(key, None)

def main(header,payload,producer=None):
    remove_keys(payload)
    remove_keys(header)

    cfg = Configuration()
    cfg.load_active_config()

    requestid = header['requestId']
    fuuid = payload["uuid"]
    dafileid = payload['daFileId']
    # event_type = header['eventType']
    event_sub_type = header['eventSubType']
    filename = payload["name"].strip()
    fsuffix = Path(filename).suffix.lower()
    request_dir = Path(os.path.join(cfg.DATA_DIR,cfg.GTL_FLOW_DIR,dafileid))

    args = {'request_dir' : request_dir,
            'zip_filepath' : Path(os.path.join(request_dir, filename)),
            'zip_extract_dir': Path(os.path.join(request_dir,'extract'))}
    
    if any([fsuffix !='.zip', event_sub_type != 'MANUAL_UPLOAD']):
        raise FileFormatNotSupported(fileformat=fsuffix)
    
    db = DatabaseManager()
    zip_details = db.get_tzip_file_details(requestid=requestid,fuuid=fuuid,dafileid=dafileid)
    logger.info(f"{fuuid} - Pulled zip details-{zip_details.shape[0]} rows")

    
    
    enabled_stages = {1:False if args['zip_filepath'].exists() else True,
                      2:True if zip_details.shape[0] == 0 else False,
                      3:False if zip_details.shape[0] > 0 and zip_details[zip_details['is_supported']].shape[0] == zip_details[zip_details['content_extracted']].shape[0] else True,
                      4:False if zip_details.shape[0] > 0 and zip_details[zip_details['is_supported']].shape[0] == zip_details['summary'].count() else True,
                      5:True}
    
    cnt_stage = sum(value for value in enabled_stages.values())
    args['stage_cnt'] = cnt_stage
    if zip_details.shape[0]==0:
        rowdict = {'requestid': requestid,
                   'fuuid' : fuuid,
                   'daoriginal_fileid' : dafileid,
                   'filename' : filename,
                   'dtpm_phase' : payload.get('phase'),
                   'ip_type' : '|'.join(payload.get('ipTypes',[])),
                   'ipid' : payload.get('ipId'),
                   'document_type' : 'Field Template',
                   'created_by' : 'System',
                   'status' : f'Stage 0/{cnt_stage}: Started',
                   'projectid' : payload.get('projectId'),
                   'uploadedby' : payload.get('uploadedBy')}
        db.insert_document(**rowdict)
    else:
        docdf = db.get_documents(requestid=requestid,fuuid=fuuid,daoriginal_fileid=dafileid)
        if pd.notnull(docdf['description'].iloc[0]):
            enabled_stages[5] = False
        

    context = GlobalContext(enabled_stages = enabled_stages,
                            args = args,
                            file_details = zip_details[['filename','is_supported','content_extracted','summary']].set_index('filename').to_dict(orient='index'),
                            header = header,
                            payload = payload)

    orchestrator = WorkflowOrchestrator(enabled_stages,producer=producer)
    orchestrator.run(context)

    # return(out_payload)