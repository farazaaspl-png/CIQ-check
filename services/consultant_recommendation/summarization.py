import asyncio, nest_asyncio, logging
import json, os, re
from pathlib import Path
import uuid, pandas as pd
 
from core.genai.open_ai_client import OpenAiHelper
from services.consultant_recommendation.prompt import build_description_prompt,build_description_consolidation_prompt
from core.embedding.vectorizer_content import ContentVectorInterface
from core.db.crud import DatabaseManager
from core.exceptions import NotAStatementOfWork, UnableToGenerateSummary
from core.utility import get_custom_logger, split_text
from config import Config as cfg
from config import Configuration

logger = get_custom_logger(__name__)
nest_asyncio.apply()


class SowSummarizer:

    LM_REMOVE_JUNKS = lambda value: re.sub(r'[^\w\s\.,\)\(\-\?\}\{\\~!@#\$%^&\*\_></"\':;\]\[\|=\+`]', '', value, flags=re.UNICODE)

    db = DatabaseManager(cfg.DEBUG)
    def __init__(self, fileid: uuid, textContent: str, debug: bool = False):
        self.textContent = textContent
        self.cfg = Configuration()
        self.cfg.load_active_config()
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.fileid = fileid
        self.response = None
        self.summary = ''
        self.tags = {}
        self.finalOutput = {'summary': ''}
        self.engine = OpenAiHelper(correlationid=self.cfg.CORR_ID_SUMMARIZATION)

        ## CR0312: Converting list to set
        self.negative_list = ['None', '', 'Not specified', 'Not Available',
                              'Not explicitly mentioned', 'Not Provided',
                              'Not specified in the document', 'None explicitly stated']

        self.semaphore = asyncio.Semaphore(os.cpu_count() * 4)

        self.vec = ContentVectorInterface(cfg.DOCUMENT_CONTENT_STORE)

        if self.debug:
            logger.setLevel(logging.DEBUG)

    async def _call_llm_description_async(self, text: str, idx: int = 0, ischunked: bool = False, vector_search_results: list = None) -> dict:
        """Call LLM for description extraction"""
        async with self.semaphore:
            # result = self.vec.search_content(text, k=5, threshold=0.2)
            # vector_search_results = result['content'].tolist()
            prompt = build_description_prompt(text, ischunked=ischunked,
                                            #   vector_search_results=vector_search_results
                                              )
            response = self.engine.get_json_text_to_text(prompt, fileid=self.fileid, requestid=f'description-chunk-{str(idx)}-{str(ischunked)}')

            logger.debug(f'{self.fileid}-->Description LLM Call completed for chunk {idx}')

            if isinstance(response, list) and response:
                response = response[0]
            if not isinstance(response, dict):
                raise ValueError(f"LLM returned invalid description response for chunk {idx}: {response}")

            logger.info(f'{self.fileid}-->Description extracted successfully for Chunk: {idx}')
            return response

    async def summarize_in_chunks_async(self) -> dict:
        """Main async method to process description chunks and consolidate"""
        chunklst = split_text(self.textContent,chunk_size=self.cfg.CHUNK_SIZE_RECOMMENDATION,chunk_overlap=self.cfg.OVER_LAP_SIZE_RECOMMENDATION)
        logger.info(f'{self.fileid}-->{len(chunklst)} chunks generated')
        if len(chunklst) == 1:
            return await self._call_llm_description_async(text=self.textContent)
        
        task = [asyncio.create_task(self._call_llm_description_async(text=chunk, idx=idx + 1, ischunked=True)) for idx, chunk in enumerate(chunklst)]
        response_list = await asyncio.gather(*task, return_exceptions=False)
        logger.info(f'{self.fileid}-->Descriptions generated for each chunk successfully')
        logger.debug(f'Response List: {response_list}')
        customerName = None
        description_lst = []
        sowlanguage = 'English'

        for idx, response in enumerate(response_list):
            custname = response.pop('Customer Name', response.pop('customer name', response.pop('CustomerName', None)))
            sowlanguage = response.pop('Language', response.pop('language', 'English'))

            if customerName is None and custname not in self.negative_list:
                customerName = custname

            desc_dict = {}
            for key, value in response.items():
                if value and value not in self.negative_list:
                    desc_dict[key] = value
            description_lst.append(desc_dict)

        #Consolidate all responses for each chunk
        if len(description_lst) > 1:
            consolidation_prompt = build_description_consolidation_prompt(description_lst, sowlanguage)
            logger.debug(f'{self.fileid}-->Consolidation Prompt: {consolidation_prompt}')
            response  = self.engine.get_json_text_to_text(consolidation_prompt,fileid=self.fileid,requestid='SOW-consolidation')

            if isinstance(response , list) and response :
                response  = response[0]
            if not isinstance(response , dict):
                raise ValueError(f"LLM returned invalid consolidation response: {response}")

        #     logger.info(f'{self.fileid}-->Description consolidated successfully')
        # else:
        #     consolidated_description = description_lst[0] if description_lst else {}

        # ========== MERGE FINAL RESPONSE ==========
        ret_response = {}
        ret_response['Customer Name'] = customerName
        # ret_response['Language'] = sowlanguage

        for key, value in response.items():
            ret_response[key] = value

        logger.info(f'{self.fileid}-->Consolidated Description Summary generated successfully')
        logger.debug(f'{self.fileid}-->Final Response: {ret_response}')
        return ret_response

    def summarize(self) -> dict:
        """Main method to run summarizer pipeline."""
        logger.info(f'{self.fileid}-->Processing file with chunking')

        # ========== STEP 1: DESCRIPTION PIPELINE ==========
        self.response = asyncio.run(self.summarize_in_chunks_async())

        self.__format_response()
        if pd.isnull(self.summary) or self.summary == '':
            self.response = asyncio.run(self.summarize_in_chunks_async())
            self.__format_response()

        if pd.isnull(self.summary) or self.summary == '':
            raise UnableToGenerateSummary(self.fileid)

        logger.debug(f'{self.fileid}-->Summary: {self.summary}')
        logger.debug(f'{self.fileid}-->Others: {self.tags}')

        self.finalOutput['summary'] = SowSummarizer.LM_REMOVE_JUNKS(self.summary)
        self.finalOutput = (self.finalOutput | self.tags)

        return self.finalOutput

    def __format_response(self) -> str:
        """Format response into HTML"""
        def formatedvalue(val):
            # vallst = val.split('|')
            if len(val) > 1:
                return ('<li>'.join([f"{item.strip()}</li>" for item in val]), True)
            else:
                return (val, False)

        try:
            for key, value in self.response.items():
                logger.debug(f"{self.fileid}-->In Format Json Response:{key}-->{value}")
                if value is None or value in self.negative_list:
                    continue

                key = key.lower()
                if key == 'error':
                    raise NotAStatementOfWork(self.fileid, f"Not a Statement Of Work. Response: {self.response}")
                elif key == 'customer name':
                    self.summary += '<b>Customer Name:</b><br>' + (value) + '<br>'
                elif key == 'objective':
                    self.summary += '<b>Objective:</b><br>' + value + '<br><br>'
                elif key == 'scope of work':
                    formatedval, hasmultiple = formatedvalue(value)
                    if hasmultiple:
                        self.summary += '<b>Scope of Work:</b><ul><li>' + (formatedval) + '</ul>'
                    else:
                        self.summary += '<b>Scope of Work:</b><br>' + formatedval + '<br><br>'
                elif key == 'deliverables':
                    formatedval, hasmultiple = formatedvalue(value)
                    if hasmultiple:
                        self.summary += '<b>Deliverables:</b><ul><li>' + (formatedval) + '</ul>'
                    else:
                        self.summary += '<b>Deliverables:</b><br>' + formatedval + '<br><br>'
                else:
                    if isinstance(value, list):
                        self.tags[key] = value
                    else:
                        self.tags[key] = list(map(lambda x: x.strip(), value.split('|')))

        except Exception as e:
            logger.error(f"{self.fileid}-->Error while formatting response: {e}", exc_info=True)

    def saveJson(self):
        """Save JSON output"""
        OUTPUT_DIR = Path(os.path.join(self.cfg.DATA_DIR, cfg.CONSULTANT_FLOW_DIR, self.fileid))
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        try:
            json_path = os.path.join(OUTPUT_DIR, f"SowSummary.json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(self.finalOutput, f, indent=2)
            logger.info(f"{self.fileid}-->Json File Saved at {json_path}")
        except Exception as e:
            logger.error(f"{self.fileid}-->Saving JSON failed at '{json_path}': {e}", exc_info=True)
