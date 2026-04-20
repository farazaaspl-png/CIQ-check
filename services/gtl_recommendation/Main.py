import os
from sqlalchemy import text as querytext
from pathlib import Path
from core.dispatcher import SUPPORTED_FILES
from config import Configuration
from core.db.crud import DatabaseManager
from services.gtl_recommendation.workflow import GlobalContext,WorkflowOrchestrator
from core.exceptions import FileFormatNotSupported

from core.utility import get_custom_logger
logger = get_custom_logger(__name__)

def remove_keys(pdict: dict, keylist: list = ['createdBy', 'updatedBy', 'createdOn', 'updatedOn', 'leadTime', 'processingTime', 'status']):
    for key in keylist:    
        if key in pdict: pdict.pop(key, None)

def get_execution_stages(config, event_type, event_sub_type, file_suffix, currentstage='start'):
    qry = querytext(f"""select * from {config.DATABASE_SCHEMA}.{config.DOCUMENT_RULE_TABLE} 
                        where event_type=:event_type and event_sub_type=:event_sub_type and file_suffix=:file_suffix""")
    params = {'event_type':event_type, 'event_sub_type':event_sub_type, 'file_suffix': file_suffix if file_suffix in ('.pdf','.zip') else 'others'}

    df = DatabaseManager().query_database(qry, params)
    exe_stage = df.to_dict('records')[0]

    currentstage = currentstage.lower().replace(' ','_')
    stageseq = {'start':0,
                'download':1,
                'convert':2,
                'content_extraction':3,
                'similarity':4,
                'sensitive_item_extraction':5,
                'classification':6,
                'redaction':7,
                'upload':8,
                'grading':9}
                # 'generatepayload':8}
    # for key in stageseq.keys():
    #     if key in exe_stage:
    #         logger.info(f"Stage {key} (stageno {stageseq[key]}): database value = {exe_stage[key]}, currentstage = {currentstage} (stageno {stageseq.get(currentstage, 'N/A')})")
    
    exe_stage = {stageseq[key]:(False if stageseq[key]<=stageseq[currentstage] else val) for key,val in exe_stage.items() if key not in ('event_type', 'event_sub_type', 'file_suffix')}
    # logger.info(f"FINAL ENABLED STAGES: {exe_stage}")
    return exe_stage
        

def main(header, payload,producer=None):
    # send_message=None,
    remove_keys(payload)
    remove_keys(header)

    cfg = Configuration()
    cfg.load_active_config()

    requestid = header['requestId']
    fuuid = payload["uuid"]
    dafileid = payload['daFileId']
    event_type = header['eventType']
    event_sub_type = header['eventSubType']
    filename = payload["name"].strip()
    fsuffix = Path(filename).suffix.lower()
    db = DatabaseManager()
    
    if fsuffix =='.zip' and event_sub_type != 'MANUAL_UPLOAD':
        raise FileFormatNotSupported(fileformat=fsuffix)
    elif fsuffix not in SUPPORTED_FILES:
        rowdict = {'requestid': requestid,
                   'fuuid' : fuuid,
                   'daoriginal_fileid' : dafileid,
                   'document_type' : payload.get('source','Field Template'),
                   'filename' : filename,
                   'dtpm_phase' : payload.get('phase'),
                   'projectid' : payload.get('projectId'),
                   'ip_type' : '|'.join(payload.get('ipTypes',[])),
                   'created_by' : 'System',
                   'status' : 'Not Supported',
                   'type' : payload.get('type'),
                   'url' : payload.get('url'),
                   'uploadedby' : payload.get('uploadedBy')}
        db.insert_document(**rowdict)
        raise FileFormatNotSupported(fileformat=fsuffix)
    
    
    doc_state = db.get_documents_state(requestid=requestid,fuuid=fuuid,dafileid=dafileid)
    logger.info(f"{fuuid} - Pulled current state of request-{doc_state.shape[0]} rows")

    if doc_state.shape[0]==0:
        request_dir = Path(os.path.join(cfg.DATA_DIR,cfg.GTL_FLOW_DIR,dafileid))
        doc_state = {'requestid' : requestid,
                     'fuuid' : fuuid,
                     'dafileid' : dafileid,
                     'request_dir' : request_dir,
                     'filepath' : Path(os.path.join(request_dir, filename)),
                     'ispdf' : fsuffix == '.pdf',
                     'extraction_input_file': Path(os.path.join(request_dir,"Content Extraction","extraction_input.txt")),
                     'classification_input_file': Path(os.path.join(request_dir,"Content Extraction","classification_input.txt")),
                     'stageno':0,
                     'stagename':'start'}
        enabled_stages = get_execution_stages(cfg,event_type, event_sub_type, fsuffix)
        db.insert_document_state(**doc_state)
        cnt_stage = sum(value for value in enabled_stages.values())
        logger.info(f"{fuuid} - inserted document state")

        # if event_type == "UPLOAD_NEW_FILE":
        isupd = db.update_document(where_clause={'fuuid': fuuid, 'dafileid': dafileid},
                                   update_values={'status': 'REJECTED'} )
        if isupd:
            logger.info(f"{fuuid} - Status updated for old dafileid {dafileid} to REJECTED")
        else:
            isupd = db.update_document(where_clause={'fuuid': fuuid, 'daoriginal_fileid': dafileid},
                                   update_values={'status': 'REJECTED'}) 
            if isupd:
                logger.info(f"{fuuid} - Marked previous sent file as rejected")
        
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
        logger.info(f"{fuuid} - Inserted document row in database {dafileid}")
    else:
        doc_state = doc_state.to_dict(orient='records')[0]
        enabled_stages = get_execution_stages(cfg,event_type, event_sub_type, doc_state['filepath'].suffix, doc_state['stagename'])
        cnt_stage = sum(value for value in enabled_stages.values())
        logger.info(f"{fuuid} - Resuming execution from stage {doc_state['stagename']}")
    
    doc_state['stage_cnt'] = cnt_stage
    context = GlobalContext(enabled_stages = enabled_stages,
                            state = doc_state,
                            header = header,
                            payload = payload,)
    orchestrator = WorkflowOrchestrator(enabled_stages,producer=producer)
    # send_message=send_message
    orchestrator.run(context)
    # return(payload)