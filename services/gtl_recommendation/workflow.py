from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass, field
import os
import shutil
import time
from typing import Dict, List
from pathlib import Path
from zipfile import BadZipFile
import pendulum
import json

from config import Configuration
from core.db.crud import DatabaseManager
from core.dellattachments import DellAttachments
from core.dispatcher import Dispatcher
from core.exceptions import FileFormatNotSupported, FileReadError, UnExpectedError
from core.s3_helper import StorageManager
from core.utility import get_custom_logger #,remove_control_chars
from services.gtl_recommendation.classification.classification import Classifier
from services.gtl_recommendation.conversion.converter import FileConverter
from services.gtl_recommendation.sensitive_text_ext.extractor import TextExtractor
from services.gtl_recommendation.similarity.template_identifier import DocumentIdentifier
from services.gtl_recommendation.similarity.Content_similarity import ContentSimilarity
from services.gtl_recommendation.grading.grading import ContentGrader

logger = get_custom_logger(__name__)

# ----------------------------
# GLOBAL CONTEXT CONFIG
# ----------------------------
@dataclass
class GlobalContext:
    enabled_stages: Dict[int, bool]
    state: Dict = field(default_factory=dict)
    header: Dict = field(default_factory=dict)
    payload: Dict = field(default_factory=dict)

# ----------------------------
# ABSTRACT STATE
# ----------------------------
class WorkflowStage(ABC):
    
    def __init__(self):
        self.cfg=Configuration()
        self.cfg.load_active_config()
        self.s3 = StorageManager()
        self.db = DatabaseManager()
        
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def stageno(self) -> str:
        pass

    @abstractmethod
    def execute(self, context: GlobalContext):
        pass

    def should_execute(self, context: GlobalContext) -> bool:
        return context.enabled_stages.get(self.stageno, True)
    
    def send_updates_to_db(self,context:GlobalContext, doc_upd_params={}, state_upd_params={}):
        stgno = self.stageno if self.stageno<context.state['stage_cnt'] else self.stageno - (9-context.state['stage_cnt'])
        status = f'Stage {stgno}/{context.state["stage_cnt"]}: {self.name}'
        doc_upd_params['status'] = status
        state_upd_params['status'] = status
        state_upd_params['stageno'] = self.stageno
        state_upd_params['stagename'] = self.name

        upd = self.db.update_document(
                    where_clause={'requestid': context.state['requestid'], 'fuuid': context.state['fuuid'], 'daoriginal_fileid': context.state['dafileid']},
                    update_values = doc_upd_params
                )
        self.db.update_document_state(
                where_clause={'requestid': context.state['requestid'], 'fuuid': context.state['fuuid'], 'dafileid': context.state['dafileid']},
                update_values = state_upd_params
            )
            
    def get_file_local(self,fileUuid, input_file_path):
        if not input_file_path.exists():
            if self.s3.exists(input_file_path):
                filepath = self.s3.download(self.s3._make_s3_key(input_file_path,self.cfg.DATA_DIR))
                logger.info(f"{fileUuid}-Downloaded file from S3 Storage: {filepath}")

    def get_dir_local(self, fileUuid, dir):
        if not dir.exists():
            if self.s3.exists(dir, is_dir=True):
                _ = self.s3.download_all(self.s3._make_s3_key(dir,self.cfg.DATA_DIR))
                logger.info(f"{fileUuid}-Downloaded directory from S3 Storage: {dir}")


    def get_file_content(self, fileUuid, input_file_path):
        self.get_file_local(fileUuid, input_file_path)
        with open(input_file_path, "r", encoding="utf-8") as f:
            return(f.read())
    
    def send_message(self, context: GlobalContext, producer):
        stgno = self.stageno if self.stageno<context.state['stage_cnt'] else self.stageno - (7-context.state['stage_cnt'])
        requestid = context.state['requestid']
        fuuid     = context.state['fuuid']
        dafileid  = context.state['dafileid']

        response_headers = {
            "eventType"   : "IP_GOLDEN_COPY_REQUEST_RECOMMENDATION_ACK",
            "eventSubType": "PROCESSED_FILE",
            "createdOn"   : pendulum.now("UTC").format("ddd MMM DD HH:mm:ss [UTC] YYYY"),
            "requestId"   : str(requestid),   
        }

        df = self.db.get_vwclassificationout_row(
            requestid=requestid,
            fuuid=fuuid,
            daoriginal_fileid=dafileid
        )
        uuid_columns = ['requestid', 'fuuid', 'daoriginal_fileid', 'dafileid', 'dasanitizationoutfileid']
        df[uuid_columns] = df[uuid_columns].fillna('').astype(str)
        df = df.drop('requestid', axis=1)
        # Handle UnDefined values
        if stgno < context.state['stage_cnt']:
            df = df.replace('UnDefined', '')
        
        response_payload = df.to_dict(orient='records')[0]
        # response_payload.pop('requestid') 
        response_payload['requestUuid'] = str(fuuid)
        # response_payload['status']  = context.state.get('status', '')  # ← fix 2: add status

        context.state['outpayload'] = response_payload
        logger.info(f"{fuuid}- Payload generated after stage: {self.name}")
        response_headers["eventSubType"] = response_headers["eventSubType"] if response_payload['status']=='Processed' else 'STAGE_STATUS'
        
        try:
            producer.send_message(self.cfg.OUTPUT_TOPIC, response_headers, response_payload)
        except Exception as e:
            logger.error(f"{fuuid}- Error sending message: {e}", exc_info=True)
            if not stgno < context.state['stage_cnt']:
                raise

# ----------------------------
# CONCRETE STATES
# ----------------------------
class DownloadStage(WorkflowStage):

    @property
    def name(self) -> str:
        return "Download"
    
    @property
    def stageno(self) -> str:
        return 1

    def execute(self, context: GlobalContext):
        dafileid = context.state['dafileid']
        # filename = context.state['name']
        # requestId = context.state['requestid']
        fuuid = context.state['fuuid']
        filepath = context.state['filepath']
        
        if not self.should_execute(context):
            logger.info(f"{fuuid}-[SKIP] {self.name}")
            return
        
        fdict = {'id':dafileid,
                 'name':filepath.name,
                 'filepath':filepath
                 }

        if Path(fdict['filepath']).exists():
            pass
        elif self.s3.exists(fdict['filepath']):
            filepath = self.s3.download(self.s3._make_s3_key(fdict['filepath'],self.cfg.DATA_DIR))
            logger.info(f"{fuuid}-Downloaded file from S3 Storage: {filepath}")
        else:
            logger.info(f"{fuuid}-Downloading file from dell attachments")
            da = DellAttachments(self.cfg.DEBUG)
            fdict = asyncio.run(da.download(filedict=fdict))
            logger.info(f"{fuuid}-Downloaded file from dell attachments: {filepath}")

            self.s3.upload(fdict['filepath'])
            logger.info(f"{fuuid}- File uploaded to dell attachments")
        context.state['filepath'] = fdict['filepath']

        self.send_updates_to_db(context)

class ConvertFileStage(WorkflowStage):

    @property
    def name(self) -> str:
        return "Convert"
    
    @property
    def stageno(self) -> str:
        return 2

    def execute(self, context: GlobalContext):
        dafileid = context.state['dafileid']
        # requestId = context.state['requestid']
        fuuid = context.state['fuuid']
        filepath = context.state['filepath']

        if not self.should_execute(context):
            logger.info(f"{fuuid}-[SKIP] {self.name}")
            return
        
        self.get_file_local(fuuid, filepath)

        conv = FileConverter(filepath = filepath, fileid = dafileid, debug = self.cfg.DEBUG)
        converted_filepath = conv.convert()
        context.state['converted_filepath'] = converted_filepath
        context.state['waspdf'] = True

        self.send_updates_to_db(context,
                                doc_upd_params={'filename':converted_filepath.name,
                                                'waspdf':True},
                                state_upd_params={'converted_filepath': converted_filepath})


class ExtractFileContentsStage(WorkflowStage):

    @property
    def name(self) -> str:
        return "Content Extraction"

    @property
    def stageno(self) -> str:
        return 3
    
    def execute(self, context: GlobalContext):
        dafileid = context.state['dafileid']
        # requestId = context.state['requestid']
        fuuid = context.state['fuuid']
        filepath = context.state['filepath']
        if context.state.get('converted_filepath') is not None:
            filepath = context.state['converted_filepath']
        
        
        if not self.should_execute(context):
            logger.info(f"{fuuid}-[SKIP] {self.name}")
            return

        self.get_file_local(fuuid, filepath)

        dispatcher = Dispatcher(filepath,dafileid=dafileid,analyze_images=self.cfg.IMAGE_ANALYZE_SWITCH,debug=self.cfg.DEBUG)
        extractor = dispatcher.getExtractor()
        try:
            listoftabledata = None
            extractor.extract_content()
            for ocr in [True,False]:
                # result = extractor.get_filecontent(get_ocr=ocr)
                filecontent, listoftabledata = extractor.get_filecontent(get_ocr=ocr)

                # filecontent = extractor.get_filecontent(get_ocr=ocr)
                input_file = context.state['extraction_input_file' if ocr else 'classification_input_file']
                input_file.parent.mkdir(parents=True, exist_ok=True)
                with open(input_file, 'w', encoding='utf-8') as outf:
                    outf.write(filecontent)
                self.s3.upload(input_file, overwrite = True) 

                if ocr and len(listoftabledata)>0:
                    self.s3.delete_files(self.s3._make_s3_key(Path(input_file).parent,self.cfg.DATA_DIR),'.json')
                    for idx, tabdata in enumerate(listoftabledata):
                        input_file_json = Path(Path(input_file).parent,f'table_{idx}.json')
                        with open(input_file_json, 'w', encoding='utf-8') as outf:
                            json.dump(tabdata, outf, ensure_ascii=False, indent=2)
                        self.s3.upload(input_file_json, overwrite=True)

                self.send_updates_to_db(context,
                                        state_upd_params={'extraction_input_file': context.state['extraction_input_file'],
                                                        'classification_input_file': context.state['classification_input_file']})
                            
        except BadZipFile as e:
            logger.error(f"{fuuid}-Error processing file | file: {filepath} - {e}", exc_info=True)
            # db.update_document(where_clause={'requestid': request_id,'daoriginal_fileid': fileid},update_values={'status': 'Bad Zip File'})
            # send_status("CONTENT_EXTRACTION", "COMPLETED", "Bad Zip File")
            raise FileFormatNotSupported(f'{filepath.suffix} with restricted access')
        except Exception as e:
            logger.error(f"{fuuid}-Error processing file | file: {filepath} - {e}", exc_info=True)
            raise UnExpectedError(e)

class SimilarityStage(WorkflowStage):
 
    @property
    def name(self) -> str:
        return "Similarity Scoring"
 
    @property
    def stageno(self) -> str:
        return 4
 
    def execute(self, context: GlobalContext):
        dafileid  = context.state['dafileid']
        fuuid     = context.state['fuuid']
        requestid = context.state['requestid']
        filepath = context.state['filepath']
        input_file = context.state['extraction_input_file']
 
        if not self.should_execute(context):
            logger.info(f"{fuuid}-[SKIP] {self.name}")
            return

        # Step-1 Document Identification
        filecontent = self.get_file_content(fuuid, input_file)
        idf = DocumentIdentifier(filecontent, debug=self.cfg.DEBUG, **context.header, **context.payload)
        identification_out = idf._search_and_rank_files()
        
        if not identification_out:
            logger.info(f"{fuuid}-[SKIP] {self.name}")            
            self.db.update_gtl_synopsis(where_clause = {'requestid': requestid,'fuuid': fuuid,
                            'daoriginal_fileid': dafileid,},update_message ={"Similarity Summary": ("No template found for this file")})
            self.send_updates_to_db(context)
            return
            
        template_dafileid = identification_out.get('dafileid')
        template_filename = identification_out.get('filename')
 
        logger.info(f"{fuuid}- Template identified: dafileid={template_dafileid} | filename={template_filename}")
 
        # context.state['template_dafileid'] = template_dafileid
        # context.state['template_filename'] = template_filename
 
        template_payload = {
            "filename":template_filename,
            "templatedaFileId":template_dafileid}
        
        template_path = (Path(self.cfg.DATA_DIR) / self.cfg.GTL_FLOW_DIR / str(template_dafileid) / template_filename)

        # Step-2 Similarity and Description
        content_sim = ContentSimilarity(textContent=filecontent, debug=self.cfg.DEBUG, **context.payload,**template_payload)
        content_sim_out= content_sim.similarity_and_description(template_path)
       
        if not content_sim_out:
            logger.info(f"{fuuid}-[SKIP] {self.name}")
            self.send_updates_to_db(context)
            return

        similarity_score  = content_sim_out.get("similarity_score")
        description = content_sim_out.get("description")
 
        logger.info(f"{fuuid}- Similarity done: avg_fuzz={similarity_score:.2f}")
 
        
        # context.state['similarity_score']       = similarity_score
        # context.state['similarity_description'] = description
        
        #message for updating gtl_synopsis
        if similarity_score and description:
            message = {"Similarity Summary": (
                    f"This file {filepath.name} is similar to "
                    f"{template_filename} | "
                    f"Similarity Score: {similarity_score:.2f}% | "
                    f"Description: {description}"
                )}
        self.db.update_gtl_synopsis(where_clause = {'requestid': requestid,'fuuid': fuuid,
                                    'daoriginal_fileid': dafileid,},update_message =message)
        self.send_updates_to_db(context,
            doc_upd_params={'similarity': similarity_score,'mathcingdafileid':template_dafileid})
            # state_upd_params={'similarity': similarity_score})

class SensitiveItemsExtStage(WorkflowStage):

    @property
    def name(self) -> str:
        return "Sensitive Item Extraction"
    
    @property
    def stageno(self) -> str:
        return 5

    def execute(self, context: GlobalContext):
        requestid = context.state['requestid']
        fuuid = context.state['fuuid']
        dafileid = context.state['dafileid']

        input_file = context.state['extraction_input_file']
        filepath = context.state['filepath']
        if context.state.get('converted_filepath') is not None:
            filepath = context.state['converted_filepath']

        if not self.should_execute(context):
            logger.info(f"{fuuid}-[SKIP] {self.name}")
            return
        
        threshold_dict={'.docx':self.cfg.REDACTION_THRESHOLD_DOCX,
                        '.xlsx':self.cfg.REDACTION_THRESHOLD_XLSX,
                        '.pptx':self.cfg.REDACTION_THRESHOLD_PPTX,
                        'others':self.cfg.REDACTION_THRESHOLD_OTHERS}

        self.get_dir_local(fuuid, Path(input_file).parent)

        json_files = list(Path(input_file).parent.rglob("*.json"))

        filecontent = self.get_file_content(fuuid, input_file)
        threshold = threshold_dict[filepath.suffix.lower()] if filepath.suffix.lower() in ('.docx','.xlsx','.pptx') else threshold_dict['others']
        extractor = TextExtractor(requestid = requestid,
                                  fuuid = fuuid,
                                  dafileid = dafileid,
                                  inputText = filecontent,
                                  correlationid = self.cfg.CORR_ID_REDACTION, 
                                  debug = self.cfg.DEBUG, 
                                  threshold = threshold,
                                  filepath = filepath,
                                  json_files = json_files)
        
        isfound, sensitive_items = extractor.extract_sensitive_info()
        context.state['has_sensitive_items'] = isfound
        if isfound:
            context.state['sensitive_items'] = sensitive_items

        self.send_updates_to_db(context, state_upd_params={'has_sensitive_items': context.state['has_sensitive_items']})
        
        if not isfound:
            context.enabled_stages[7] =  False
            if not context.state.get('waspdf',False):
                context.enabled_stages[8] =  False


class ClassifyStage(WorkflowStage):

    @property
    def name(self) -> str:
        return "Classification"

    @property
    def stageno(self) -> str:
        return 6

    def execute(self, context: GlobalContext):
        # requestId = context.state['requestid']
        fuuid = context.state['fuuid']
        input_file = context.state['classification_input_file']

        if not self.should_execute(context):
            logger.info(f"{fuuid}-[SKIP] {self.name}")
            return
        
        filecontent = self.get_file_content(fuuid, input_file)
        cls = Classifier(filecontent, debug=self.cfg.DEBUG, **context.header, **context.payload, waspdf = context.state.get('waspdf',False))
        classification_out = cls.classify()
        if 'filename' in classification_out.keys():
            context.state['redacted_filename'] = classification_out['filename']
            self.send_updates_to_db(context,
                                    doc_upd_params=classification_out,
                                    state_upd_params={'redacted_filename': context.state.get('redacted_filename')})
        else:
            self.send_updates_to_db(context,
                                    doc_upd_params=classification_out)

class RedactionStage(WorkflowStage):

    @property
    def name(self) -> str:
        return "Redaction"
    
    @property
    def stageno(self) -> str:
        return 7

    def execute(self, context: GlobalContext):        
        dafileid = context.state['dafileid']
        requestid = context.state['requestid']
        fuuid = context.state['fuuid']
        request_dir = context.state['request_dir']
        sensitive_items = context.state.get('sensitive_items')
        filepath = context.state['filepath']
        if context.state.get('converted_filepath') is not None:
            filepath = context.state['converted_filepath']

        if not self.should_execute(context):
            logger.info(f"{fuuid}-[SKIP] {self.name}")
            return
        
        self.get_file_local(fuuid, filepath)
        dispatcher = Dispatcher(filepath, dafileid=dafileid, analyze_images=self.cfg.IMAGE_ANALYZE_SWITCH, debug=self.cfg.DEBUG)
        textRedactor, imageRedactor = dispatcher.getRedactors(outdir=os.path.join(request_dir, self.name))

        #replace all the sensitive items from text contents of original file.
        # isTextRedacted, totalRedacted = textRedactor.sanitize(sensitiveInfoList = sensitive_items, **context.state)
        isTextRedacted, totalRedacted = textRedactor.sanitize(**context.state)

        out_filepath, redacted_items_filepath = textRedactor.save(outdir=os.path.join(request_dir, self.name),filename = context.state.get('redacted_filename',filepath.name))
        self.s3.upload(out_filepath, overwrite = True)
        
        context.state['out_filepath'] = out_filepath
        if isTextRedacted:
            self.s3.upload(redacted_items_filepath, overwrite = True)
            context.state['redacted_items_filepath'] = redacted_items_filepath
            
        context.state['istextredacted'] = isTextRedacted
        context.state['isimageredacted'] = False
        isImageRedacted = False
        #replace all the sensitive items from image contents of original file.
        if imageRedactor is not None:
            isImageRedacted = imageRedactor.redact(tobe_redacted = sensitive_items, **context.state)
            context.state['isimageredacted'] = isImageRedacted

        if isImageRedacted:
            self.s3.upload(out_filepath, overwrite = True)

        logger.info(f"Updating gtl_synopsis")
        message = {"Redaction Summary": ("There are no sensitive items found in this file."
            if totalRedacted == 0 else f"{totalRedacted} sensitive item's got redacted for this file.")}
        self.db.update_gtl_synopsis(where_clause = {'requestid': requestid,'fuuid': fuuid,
                                    'daoriginal_fileid': dafileid,},update_message =message)
        self.send_updates_to_db(context,
                                state_upd_params={'istextredacted': context.state['istextredacted'],
                                                  'isimageredacted': context.state['isimageredacted'],
                                                  'out_filepath': context.state.get('out_filepath'),
                                                  'redacted_items_filepath': context.state.get('redacted_items_filepath')})

class UploadOutputs(WorkflowStage):

    @property
    def name(self) -> str:
        return "Upload"
    
    @property
    def stageno(self) -> str:
        return 8

    def execute(self, context: GlobalContext): 
        # requestId = context.state['requestid']
        fuuid = context.state['fuuid']

        if not self.should_execute(context):
            logger.info(f"{fuuid}-[SKIP] {self.name}")
            return

        dametadata = {"EPS_ProjectId": "consulting_workspace_IP_uploads",
                      "EPSSection": "other",
                      "SubSystem" : "consulting_ip_draft_workspace",
                      "CustomerUploaded": "false",
                      "FileVisibleExternal": "False"}
        da = DellAttachments(debug=self.cfg.DEBUG)

        if context.state.get('istextredacted',False):
            self.get_file_local(fuuid, context.state.get('redacted_items_filepath'))
            redacted_items_dafileid = asyncio.run(da.upload(context.state.get('redacted_items_filepath'), metadata=dametadata))
            logger.info(f"{fuuid}-Uploaded redacted items list to dell attachments {redacted_items_dafileid}")
            context.state['redacted_items_dafileid'] = redacted_items_dafileid

        if any([context.state.get('istextredacted',False),context.state.get('isimageredacted',False)]):
            self.get_file_local(fuuid, context.state.get('out_filepath'))

            redacted_file_dafileid = asyncio.run(da.upload(context.state.get('out_filepath'), metadata=dametadata))
            logger.info(f"{fuuid}-Uploaded redacted file to dell attachments {redacted_file_dafileid}")
            context.state['out_dafileid'] = redacted_file_dafileid
        elif context.state.get('waspdf',False):
            self.get_file_local(fuuid, context.state.get('converted_filepath'))
            convert_pdf_dafileid = asyncio.run(da.upload(context.state.get('converted_filepath'), metadata=dametadata))
            logger.info(f"{fuuid}-Uploaded converted file to dell attachments {convert_pdf_dafileid}")
            context.state['out_dafileid'] = convert_pdf_dafileid

        self.send_updates_to_db(context,
                                doc_upd_params={'dafileid': context.state.get('out_dafileid'),'redacted_items_dafileid': context.state.get('redacted_items_dafileid')},
                                state_upd_params={'redacted_items_dafileid': context.state.get('redacted_items_dafileid'),
                                                  'out_dafileid': context.state.get('out_dafileid')})

class GradingStage(WorkflowStage):

    @property
    def name(self) -> str:
        return "Grading"
    
    @property
    def stageno(self) -> str:
        return 9
    
    def execute(self, context: GlobalContext):
        requestid = context.state['requestid']
        fuuid = context.state['fuuid']
        daoriginal_fileid = context.state['dafileid']
        redacted_filepath = Path(context.state['out_filepath'])
        # redacted_filename = redacted_filepath.name
        redacted_file_dafileid = context.state.get('out_dafileid',context.state['dafileid'])
        
        # filepath = context.state['filepath']
        # if context.state.get('converted_filepath') is not None:
        #     filepath = context.state['converted_filepath']
        
        if not self.should_execute(context):
            logger.info(f"{fuuid}-[SKIP] {self.name}")
            return
        
        self.get_file_local(fuuid, redacted_filepath)

        df = self.db.get_documents(dafileid = redacted_file_dafileid)
        dafileid = redacted_file_dafileid
        if df.shape[0]==0:
            df = self.db.get_documents(daoriginal_fileid = daoriginal_fileid)
            dafileid = daoriginal_fileid
        blank_values = ['', ' ', 'NA', 'N/A', 'null', 'None', 'NaN', 'nan', '#N/A']
        df = df.replace(blank_values, None)
        filerec = df.to_dict(orient="records")[0]

        grader = ContentGrader(filepath=redacted_filepath, dafileid=redacted_file_dafileid, debug=self.cfg.DEBUG)
        grade_result = grader.main(**filerec)
        grade_result['fuuid'] = fuuid
        grade_result['requestid'] = requestid
        grade_result['dafileid'] = dafileid
        
        # Insert initial grading row with 4 id's
        self.db.insert_grading(**grade_result)
        logger.info(f"{fuuid}-Inserted grading row")
        
        # Send status update
        self.send_updates_to_db(context)
        
        logger.info(f"{fuuid}-Grading completed successfully for {redacted_filepath.suffix.lower()}")
        



# class GenerateOutPayload(WorkflowStage):

#     @property
#     def name(self) -> str:
#         return "GeneratePayload"
    
#     @property
#     def stageno(self) -> str:
#         return 8

#     def execute(self, context: GlobalContext): 
#         # requestid = context.state['requestid']
#         fileUuid = context.state['fuuid']
#         # dafileid = context.state['dafileid']
        
#         if not self.should_execute(context):
#             logger.info(f"{fileUuid}-[SKIP] {self.name}")
#             return
        
        # df = self.db.get_vwclassificationout_row(requestid=requestid, fuuid=fileUuid, daoriginal_fileid=dafileid)
        # # uuid_columns = df.select_dtypes(include=['object']).columns
        # uuid_columns = ['requestid', 'fuuid', 'daoriginal_fileid', 'dafileid', 'dasanitizationoutfileid']
        # # df[uuid_columns] = df[uuid_columns].apply(lambda x: x.astype(str))  
        # df[uuid_columns] = df[uuid_columns].fillna('').astype(str)
 
        # logger.info(f"{fileUuid}-Queried vwclassificationout view to get complete record for a document")
        # context.state['outpayload'] = df.to_dict(orient='records')[0]


# ----------------------------
# ORCHESTRATOR
# ----------------------------
class WorkflowOrchestrator:
    class_map = {1:DownloadStage,
                 2:ConvertFileStage,
                 3:ExtractFileContentsStage,
                 4:SimilarityStage,
                 5:SensitiveItemsExtStage,
                 6:ClassifyStage,
                 7:RedactionStage,
                 8:UploadOutputs,
                 9:GradingStage}
                #  8:GenerateOutPayload}
    
    @staticmethod
    def create_object(class_seq_no):
        class_constructor = WorkflowOrchestrator.class_map.get(class_seq_no)
        if class_constructor:
            return class_constructor()
        else:
            raise ValueError(f"Unknown class seqno: {class_seq_no}")
        
    def __init__(self,enabled_stages,producer=None):
        enabled_stages = {key:val for key,val in enabled_stages.items() if val}
        self.stages = [WorkflowOrchestrator.create_object(seqno) for seqno in sorted(enabled_stages.keys())]
        self.producer = producer
        self.db = DatabaseManager()

    def run(self, context: GlobalContext):
        requestid = context.state['requestid']
        fuuid = context.state['fuuid']
        logger.info(f"{requestid}~~{fuuid}========== WORKFLOW STARTED ==========")

        last_stage = None

        for stage in self.stages:
            try:
                logger.info(f"{requestid}~~{fuuid}--- Processing Stage: {stage.name} ---")
                stage.execute(context) 
                if self.producer:
                    stage.send_message(context, self.producer)
                    logger.info(f"{fuuid}- Payload generated after stage: {stage.name}")
                last_stage = stage  # Update the last completed stage
            except Exception as e:
                logger.error(f"{requestid}~~{fuuid}- WORKFLOW FAILED AT {stage.name}")
                raise e

        self.db.update_document(
            where_clause={'requestid': context.state['requestid'],'fuuid': context.state['fuuid'],'daoriginal_fileid': context.state['dafileid']},
            update_values={'status': 'Processed'})
            
        if last_stage and self.producer:
            last_stage.send_message(context, self.producer)
        
        self.clean_up(fuuid,context.state['request_dir'])
        logger.info(f"{requestid}~~{fuuid}========== WORKFLOW COMPLETED ==========")
        # return context.state['outpayload']
    
    def clean_up(self,fuuid, request_dir):
        try:
            path = os.path.abspath(request_dir)

            # Prevent deleting root directories
            if path in ("/", "C:\\", "D:\\"):
                raise ValueError("Refusing to delete root directory")

            if os.path.exists(path):
                shutil.rmtree(path)
                logger.info(f"{fuuid}-->Removed: {path}")
            else:
                logger.info(f"{fuuid}-->Directory not found:{path}")
        except PermissionError:
            time.sleep(10)  # wait for 10 seconds
            try:
                shutil.rmtree(path)
            except PermissionError as e:
                logger.error(f"{fuuid}-->Failed to delete {path}: {e}", exc_info=True)
