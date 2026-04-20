import asyncio, os, shutil, time, zipfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict
from pathlib import Path
from zipfile import BadZipFile
import pendulum
from config import Configuration
from core.db.crud import DatabaseManager
from core.dellattachments import DellAttachments
from core.dispatcher import SUPPORTED_FILES, Dispatcher
from core.exceptions import TextExtractionForZipError, ZipSummaryGenerationError , NoValidFilesInZip
from core.genai.open_ai_client import OpenAiHelper
from core.s3_helper import StorageManager
from core.utility import get_custom_logger, split_text
import services.gtl_recommendation.zip_summarization.prompt as pmt

logger = get_custom_logger(__name__)

# ----------------------------
# GLOBAL CONTEXT CONFIG
# ----------------------------a
@dataclass
class GlobalContext:
    enabled_stages: Dict[int, bool]
    args: Dict = field(default_factory=dict)
    file_details: Dict = field(default_factory=dict)
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
        self.engine = OpenAiHelper(correlationid=self.cfg.CORR_ID_SUMMARIZATION)
        self.semaphore = asyncio.Semaphore(os.cpu_count() * 2)
        self.negative_list = ['None','','Not specified','Not Available',
                              'Not explicitly mentioned','Not Provided',
                              'Not specified in the document','None explicitly stated']
        
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
    
    def send_updates_to_db(self,context:GlobalContext, doc_upd_params={}):
        stgno = self.stageno if self.stageno<context.args['stage_cnt'] else self.stageno - (5-context.args['stage_cnt'])
        doc_upd_params['status'] = f'Stage {stgno}/{context.args['stage_cnt']}: {self.name}'

        upd = self.db.update_document(
                    where_clause={'requestid': context.header['requestId'], 'fuuid': context.payload['uuid'], 'daoriginal_fileid': context.payload['daFileId']},
                    update_values = doc_upd_params
                )
        
    def get_file_local(self,fuuid, input_file_path):
        if not input_file_path.exists():
            if self.s3.exists(input_file_path):
                filepath = self.s3.download(self.s3._make_s3_key(input_file_path,self.cfg.DATA_DIR))
                logger.info(f"{fuuid}-Downloaded file from S3 Storage: {filepath}")
        
    def get_file_content(self, fuuid, input_file_path):
        self.get_file_local(fuuid, input_file_path)
        with open(input_file_path, "r", encoding="utf-8") as f:
            return(f.read())
        

    async def _call_llm_async(self, context:GlobalContext, text: str, idx: str = '1/1', ischunked: bool = False) -> dict:
        """Call LLM asynchronously with semaphore control"""
        dafileid = context.payload['daFileId']
        async with self.semaphore:
            prompt = pmt.build_prompt(text, ischunked=ischunked)
            response = self.engine.get_json_text_to_text(prompt,fileid=dafileid,requestid=f'chunk-{idx}-{str(ischunked)}')
            logger.debug(f'LLM Call completed for chunk {idx} - Response: {response}')

            if isinstance(response, list) and response:
                response = response[0]
            if not isinstance(response, dict):
                raise ValueError(f"LLM returned invalid response for chunk {idx}: {response}")
            
            logger.info(f'{dafileid}-->Description Generated successfully for Chunk: {idx}')
            return response
        
    def send_message(self, context: GlobalContext, producer):
        # ── use context.header/payload, not context.args ──
        stgno = self.stageno if self.stageno<context.args['stage_cnt'] else self.stageno - (5-context.args['stage_cnt'])
        requestid = context.header['requestId']
        fuuid     = context.payload['uuid']
        dafileid  = context.payload['daFileId']

        response_headers = {
            "eventType"   : "IP_GOLDEN_COPY_REQUEST_RECOMMENDATION_ACK",
            "eventSubType": "PROCESSED_FILE",
            "createdOn"   : pendulum.now("UTC").format("ddd MMM DD HH:mm:ss [UTC] YYYY"),
            "requestId"   : requestid,
        }

        df = self.db.get_vwclassificationout_row(
            requestid=requestid,
            fuuid=fuuid,
            daoriginal_fileid=dafileid
        )
        uuid_columns = ['requestid', 'fuuid', 'daoriginal_fileid', 'dafileid', 'dasanitizationoutfileid']
        df[uuid_columns] = df[uuid_columns].fillna('').astype(str)
        df = df.drop('requestid', axis=1)
        # Handle UnDefined valuess
        if stgno < context.args['stage_cnt']:
            df = df.replace('UnDefined', '')

        response_payload = df.to_dict(orient='records')[0]
        # response_payload.pop('requestid')
        response_payload['requestUuid'] = fuuid

        context.args['outpayload'] = response_payload  
        logger.info(f"{fuuid}- Payload generated after stage: {self.name}")
        response_headers["eventSubType"] = response_headers["eventSubType"] if response_payload['status']=='Processed' else 'STAGE_STATUS'
        try:
            if response_payload['status']=='Processed':
                producer.send_message(self.cfg.OUTPUT_TOPIC, response_headers, response_payload)
 
        except Exception as e:
            logger.error(f"{fuuid}- Error sending message: {e}", exc_info=True)
            if not stgno < context.args['stage_cnt']:
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
        daFileId = context.payload['daFileId']
        # filename = context.state['name']
        # requestId = context.header['requestId']
        fuuid = context.payload['uuid']
        zip_filepath = context.args['zip_filepath']
        
        if not self.should_execute(context):
            logger.info(f"{fuuid}-[SKIP] {self.name}")
            return
        
        if self.s3.exists(zip_filepath):
            zip_filepath = self.s3.download(self.s3._make_s3_key(zip_filepath,self.cfg.DATA_DIR))
            logger.info(f"{fuuid}-Downloaded Zip file from S3 Storage: {zip_filepath}")
        else:
            logger.info(f"{fuuid}-Downloading Zip file from dell attachments")
            fdict = {'id':daFileId,
                     'name':zip_filepath.name,
                     'filepath':zip_filepath}
            da = DellAttachments(self.cfg.DEBUG)
            fdict = asyncio.run(da.download(filedict=fdict))
            logger.info(f"{fuuid}-Downloaded Zip file from dell attachments: {zip_filepath}")

            self.s3.upload(zip_filepath)
            logger.info(f"{fuuid}- Zip File uploaded to dell attachments")

        self.send_updates_to_db(context)

class ExtractZipFile(WorkflowStage):
 
    @property
    def name(self) -> str:
        return "Zip Extraction"
   
    @property
    def stageno(self) -> str:
        return 2
    
    def exclude_hidden_files(self,filepath: Path) -> bool:
            path_str = str(filepath)
            filename = filepath.name
            file_extension = filepath.suffix.lower()
            
            if '__MACOSX' in path_str:
                return True
           
            if filename == '.DS_Store':
                return True
            
            if filename.lower() == 'thumbs.db':
                return True
            
            if filename.lower() == 'desktop.ini':
                return True
            
            if file_extension == '.zip':
                return True

            if filename.startswith('.'):
                return True
            
            return False
    
    def execute(self, context: GlobalContext):
        dafileid = context.payload['daFileId']
        requestid = context.header['requestId']
        fuuid = context.payload['uuid']
        zip_filepath = context.args['zip_filepath']
        zip_extract_dir = context.args['zip_extract_dir']
 
        if not self.should_execute(context):
            logger.info(f"{fuuid}-[SKIP] {self.name}")
            return  
       
        self.get_file_local(fuuid, zip_filepath)
 
        if os.path.exists(zip_extract_dir):
            shutil.rmtree(zip_extract_dir)
            logger.info(f"{fuuid}-->Removed:{zip_extract_dir}")
       
        zip_extract_dir.mkdir(parents=True, exist_ok=True)
 
        with zipfile.ZipFile(zip_filepath, "r") as zip_ref:
            zip_ref.extractall(zip_extract_dir)
   
        logger.info(f"Extracted {zip_filepath} to {zip_extract_dir}")
        
        all_files = [f for f in zip_extract_dir.rglob("*") if f.is_file()]
        logger.info(f"Extracted {len(all_files)} total files")
        
        extracted_files = [f for f in all_files if not self.exclude_hidden_files(f)]
        
        logger.info(f"{fuuid} - {len(extracted_files)} valid files ready for processing")
 
        if not extracted_files:
            raise NoValidFilesInZip()
        
        is_supported_file = lambda f: f.suffix.lower() in SUPPORTED_FILES
       
        for file in extracted_files:
            self.s3.upload(file)
 
        filerow = [{'requestid': requestid,
                    'fuuid': fuuid,
                    'dafileid': dafileid,
                    'filename': str(file).replace(str(zip_extract_dir), '').lstrip('\\').lstrip('//'),
                    'is_supported': is_supported_file(file)}
                   for file in extracted_files]
        self.db.insert_zip_file_details(filerow)
 
        for file in extracted_files:
            filename = str(file).replace(str(zip_extract_dir), '').lstrip('\\').lstrip('//')
            if filename not in context.file_details.keys():
                context.file_details[filename] = {'is_supported': is_supported_file(Path(filename))}
       
        self.send_updates_to_db(context)  



class ExtractFileContentsStage(WorkflowStage):

    @property
    def name(self) -> str:
        return "Content Extraction"

    @property
    def stageno(self) -> str:
        return 3
    
    def execute(self, context: GlobalContext):
        dafileid = context.payload['daFileId']
        requestid = context.header['requestId']
        fuuid = context.payload['uuid']
        file_details = context.file_details

        if not self.should_execute(context):
            logger.info(f"{fuuid}-[SKIP] File content extraction stage")
            return
        
        is_success = []
        for filename in file_details.keys():
            filepath = Path(os.path.join(context.args['zip_extract_dir'],filename))

            txt_file_path = filepath.with_suffix('.txt')
            self.get_file_local(fuuid, filepath)

            if txt_file_path.exists() or not file_details[filename]['is_supported']:
                is_success.append(True)
                continue

            dispatcher = Dispatcher(filepath,dafileid=dafileid,analyze_images=self.cfg.IMAGE_ANALYZE_SWITCH,debug=self.cfg.DEBUG)
            
            try:
                dispatcher = Dispatcher(filepath,dafileid=dafileid,analyze_images=self.cfg.IMAGE_ANALYZE_SWITCH,debug=self.cfg.DEBUG)
                extractor = dispatcher.getExtractor()
                extractor.extract_content()
                filecontent, _ = extractor.get_filecontent(get_ocr=False)
                txt_file = filepath.with_suffix('.txt')
                with open(txt_file, 'w', encoding='utf-8') as outf:
                    outf.write(filecontent)
                self.s3.upload(txt_file, overwrite = True)

                context.file_details[filename]['content_extracted'] = True
                self.db.update_tzip_file_details(where_clause={'requestid':requestid,'fuuid':fuuid, 'dafileid':dafileid,'filename': filename},
                                                 update_values={'content_extracted':True})
                logger.info(f'{fuuid}~~{filename}- Contents Extracted')
                is_success.append(True)
            except BadZipFile as e:
                logger.error(f"{fuuid}~~{filename}-Error processing file | file: - {e}", exc_info=True)
                is_success.append(False)
                continue
            except Exception as e:
                logger.error(f"{fuuid}~~{filename}-Error processing file | file: - {e}", exc_info=True)
                is_success.append(False)
                continue
        
        if (sum(is_success)/len(is_success))<0.60:
            raise TextExtractionForZipError(f'Failed to extract contents from {sum(is_success)}/{len(is_success)} files')
        
        self.send_updates_to_db(context)

class GenerateFileSummaryStage(WorkflowStage):

    @property
    def name(self) -> str:
        return "Generate File Summary"

    @property
    def stageno(self) -> str:
        return 4
    
    async def _process_file_in_chunks(self, context, text: str) -> dict:   
        chunklst = split_text(text, chunk_size=self.cfg.CHUNK_SIZE_CLASSIFICATION, chunk_overlap=self.cfg.OVER_LAP_SIZE_CLASSIFICATION)
        
        if len(chunklst) == 1:
            response = await self._call_llm_async(context,text=text)
            return response
        
        tasks = [asyncio.create_task(self._call_llm_async(context, text=chunk, idx=f"{idx+1}/{len(chunklst)}", ischunked=True)) for idx, chunk in enumerate(chunklst)]
        response_list = await asyncio.gather(*tasks, return_exceptions=False)

        summary_lst = []
        for idx, response in enumerate(response_list):
            for key, value in response.items():
                if key.lower() == 'summary':
                    if value and value not in self.negative_list:
                        summary_lst.append(value)
        
        
        consolidation_prompt = pmt.build_consolidation_prompt(summary_lst)
        response = self.engine.get_json_text_to_text(consolidation_prompt,fileid=context.payload['daFileId'],requestid=context.header['requestId'])
        logger.debug(f'Consolidated summary generated - Response: {response}')

        if isinstance(response, list) and response:
            response = response[0]
        if not isinstance(response, dict):
            raise ValueError(f"LLM returned invalid response for chunk : {response}")
        
        return response

    
    def execute(self, context: GlobalContext):
        dafileid = context.payload['daFileId']
        requestid = context.header['requestId']
        fuuid = context.payload['uuid']
        file_details = context.file_details
        if not self.should_execute(context):
            logger.info(f"{fuuid}-[SKIP] {self.name}")
            return
        
        is_success = []
        for filename in file_details.keys():
            filepath = Path(os.path.join(context.args['zip_extract_dir'],filename))
            txt_file_path = filepath.with_suffix('.txt')
            if not txt_file_path.exists() or context.file_details[filename].get('summary') is not None or not file_details[filename]['is_supported']:
                is_success.append(True)
                continue
            
            try:
                filecontent = self.get_file_content(fuuid, txt_file_path)
                response = asyncio.run(self._process_file_in_chunks(context, filecontent))
                context.file_details[filename]['summary']=response.get('summary')
                self.db.update_tzip_file_details(where_clause={'requestid':requestid,'fuuid':fuuid, 'dafileid':dafileid,'filename': filename},
                                                 update_values={'summary':response.get('summary')})
                logger.info(f"{fuuid}~~{filename}-->LLM processing completed for {filepath.name}")
                is_success.append(True)
            except BadZipFile as e:
                logger.error(f"{fuuid}~~{filename}-Error processing file | file: {filepath} - {e}", exc_info=True)
                is_success.append(False)
                continue
            except Exception as e:
                logger.error(f"{fuuid}~~{filename}-Error processing file | file: {filepath} - {e}", exc_info=True)
                is_success.append(False)
                continue
        
        if (sum(is_success)/len(is_success))<0.60:
            raise ZipSummaryGenerationError()
        
        self.send_updates_to_db(context)

class GenerateZipSummaryStage(WorkflowStage):

    @property
    def name(self) -> str:
        return "Generate Zip File Summary"

    @property
    def stageno(self) -> str:
        return 5
    
    def execute(self, context: GlobalContext):
        dafileid = context.payload['daFileId']
        requestid = context.header['requestId']
        fuuid = context.payload['uuid']
        file_details = context.file_details
        
        if not self.should_execute(context):
            logger.info(f"{fuuid}-[SKIP] {self.name}")
            return
        
        file_summaries_lst = [row['summary'] for row in file_details.values() if row['is_supported'] and row.get('summary') is not None]
        consolidation_prompt = pmt.build_prompt_for_zip(file_summaries_lst)

        response = self.engine.get_json_text_to_text(consolidation_prompt,fileid=context.payload['daFileId'],requestid=context.header['requestId'])
        logger.debug(f'Zip summary generated - Response: {response}')

        if isinstance(response, list) and response:
            response = response[0]
        if not isinstance(response, dict):
            raise ValueError(f"LLM returned invalid response for zip summary: {response}")
        
        final_description = response['summary']
        final_description += f'<br><b>Zip file Contents:</b><br>'
        file_short_desc_list = '<ul>'
        other_files_list = '<b>Others:<b><ul>'
        for fname in file_details.keys():
            if file_details[fname]['is_supported']:
                # file_short_desc_list += f'<li><b>{fname}</b> --> {file_details[fname]['short_summary']}</li>'
                file_short_desc_list += f'<li><b>{fname}</b></li>'
            else:
                other_files_list += f'<li>{fname}</li>'
        
        if len(file_short_desc_list)>4:
            final_description +=file_short_desc_list+'</ul>'

        if len(file_short_desc_list)>4:
            final_description +=other_files_list+'</ul>'

        context.args['ZipSummary'] = final_description

        self.send_updates_to_db(context,doc_upd_params={'description':final_description})
    
        
# class GenerateOutPayload(WorkflowStage):

#     @property
#     def name(self) -> str:
#         return "GeneratePayload"
    
#     @property
#     def stageno(self) -> str:
#         return 6

#     def execute(self, context: GlobalContext): 
#         # requestId = context.header['requestId']
#         fuuid = context.payload['uuid']
#         # dafileid = context.payload['daFileId']
        
#         if not self.should_execute(context):
#             logger.info(f"{fuuid}-[SKIP] GeneratePayload")
#             return
        
        # df = self.db.get_vwclassificationout_row(requestid=requestId, fuuid=fuuid, daoriginal_fileid=dafileid)
        # # uuid_columns = df.select_dtypes(include=['object']).columns
        # uuid_columns = ['requestid', 'fuuid', 'daoriginal_fileid', 'dafileid', 'dasanitizationoutfileid']
        # # df[uuid_columns] = df[uuid_columns].apply(lambda x: x.astype(str))  
        # df[uuid_columns] = df[uuid_columns].fillna('').astype(str)
 
        # logger.info(f"{requestId}-{fuuid}-Queried vwclassificationout view to get complete record for a document")
        # context.args['outpayload'] = df.to_dict(orient='records')[0]


# ----------------------------
# ORCHESTRATOR
# ----------------------------
class WorkflowOrchestrator:
    class_map = {1:DownloadStage,
                 2:ExtractZipFile,
                 3:ExtractFileContentsStage,
                 4:GenerateFileSummaryStage,
                 5:GenerateZipSummaryStage
                #  6:GenerateOutPayload
                 }
    
    @staticmethod
    def create_object(class_seq_no):
        class_constructor = WorkflowOrchestrator.class_map.get(class_seq_no)
        if class_constructor:
            return class_constructor()
        else:
            raise ValueError(f"Unknown class seqno: {class_seq_no}")
        
    def __init__(self,enabled_stages,producer=None):

        enabled_stages = {key:val for key,val in enabled_stages.items() if val}
        self.stages = [WorkflowOrchestrator.create_object(seqno) for seqno in enabled_stages.keys()]
        self.producer = producer
        self.db = DatabaseManager()

    def run(self, context: GlobalContext):
        # requestid = context.header['requestId']
        fuuid = context.payload['uuid']
        logger.info(f"{fuuid}========== WORKFLOW STARTED ==========")
        # print(f'First convert {context}')
        last_stage = None

        for stage in self.stages:
            try:
                logger.info(f"{fuuid}--- Processing Stage: {stage.name} ---")
                stage.execute(context) 
                if self.producer:
                    stage.send_message(context, self.producer)
                    logger.info(f"{fuuid}- Payload generated after stage: {stage.name}")
                    # message_tasks.append(task)

                last_stage = stage
            except Exception as e:
                logger.error(f"{fuuid}- WORKFLOW FAILED AT {stage.name}")
                raise e

        self.db.update_document(where_clause={'requestid': context.header['requestId'],'fuuid': context.payload['uuid'],'daoriginal_fileid': context.payload['daFileId']},
                                update_values={'status': 'Processed'})

        if last_stage and self.producer:
           last_stage.send_message(context, self.producer)
        self.clean_up(fuuid,context.args['request_dir'])
        logger.info(f"{fuuid}========== WORKFLOW COMPLETED ==========")
        # return context.args['outpayload']
    
    def clean_up(self,fuuid, request_dir):
        try:
            path = os.path.abspath(request_dir)

            # Prevent deleting root directories
            if path in ("/", "C:\\", "D:\\"):
                raise ValueError("Refusing to delete root directory")

            if os.path.exists(path):
                shutil.rmtree(path)
                logger.error(f"{fuuid}-->Removed: {path}")
            else:
                logger.error(f"{fuuid}-->Directory not found:{path}")
        except PermissionError:
            time.sleep(10)  # wait for 10 seconds
            try:
                shutil.rmtree(path)
            except PermissionError as e:
                logger.error(f"{fuuid}-->Failed to delete {path}: {e}", exc_info=True)