import asyncio, nest_asyncio,logging
import json, os, re
from pathlib import Path
import uuid, pandas as pd

from core.genai.open_ai_client import OpenAiHelper           # wrapper around the OpenAI client
from services.gtl_feedback.prompt import build_prompt, build_consolidation_prompt 
from core.db.crud import DatabaseManager
from core.exceptions import  UnableToGenerateSummary
from core.utility import get_custom_logger, split_text
from config import Configuration

logger = get_custom_logger(__name__)
nest_asyncio.apply()
class Summarizer:
    
    LM_REMOVE_JUNKS = lambda value:re.sub(r'[^\w\s\.,\)\(\-\?\}\{\\~!@#\$%^&\*\_></"\':;\]\[\|=\+`]', '', value, flags=re.UNICODE)
    db = DatabaseManager()
    def __init__(self, fileid: uuid , textContent: str, debug: bool = False):
        self.textContent = textContent
        self.debug = debug
        self.fileid = fileid
        self.cfg=Configuration()
        self.cfg.load_active_config()
        self.response = None
        self.summary = ''
        self.tags = {}
        self.finalOutput = {'summary':''}
        self.engine = OpenAiHelper(correlationid=self.cfg.CORR_ID_SUMMARIZATION)

        ## CR0312: Converting list to set
        self.negative_list = ['None','','Not specified','Not Available',
                              'Not explicitly mentioned','Not Provided',
                              'Not specified in the document','None explicitly stated']

        self.semaphore = asyncio.Semaphore(os.cpu_count()*4)
        if self.debug:
            logger.setLevel(logging.DEBUG)


    async def _call_llm_async(self, text: str,idx: str = '1/1', ischunked: bool = False) -> dict:
        """Main method to run summarizer pipeline."""
        async with self.semaphore:

            prompt = build_prompt(text, ischunked=ischunked)
            # logger.debug(f'Prompt generated for chunk {idx}: {prompt}')
            response = self.engine.get_json_text_to_text(prompt,fileid=self.fileid,requestid=f'chunk-{idx}-{str(ischunked)}')
            # rawresponse ={
            #     "requestid": f'chunk-{idx}-{str(ischunked)}',
            #     "fileid": self.fileid,
            #     "raw_response": response
            # }
            # Summarizer.db.insert_raw_response([rawresponse])
            logger.debug('LLM Call completed for chunk {idx}- Response: %s', response)

            if isinstance(response, list) and response:
                response = response[0]
            if not isinstance(response, dict):
                raise ValueError(f"LLM returned invalid response for chunk {idx}: {response}")
            logger.info(f'{self.fileid}-->Description Generated successfully for Chunk: {idx}')
            return response
    
    async def summarize_in_chunks_async(self) -> dict:
        chunklst = split_text(self.textContent,chunk_size=self.cfg.CHUNK_SIZE_CLASSIFICATION, chunk_overlap=self.cfg.OVER_LAP_SIZE_CLASSIFICATION)
        logger.info(f'{self.fileid}-->{len(chunklst)} chunks generated')
        if len(chunklst) == 1:
            return await self._call_llm_async(text = self.textContent)
        
        task=[asyncio.create_task(self._call_llm_async(text = chunk, idx = f"{idx+1}/{len(chunklst)}", ischunked = True )) for idx, chunk in enumerate(chunklst)]
        response_list = await asyncio.gather(*task,return_exceptions=False)
        logger.info(f'{self.fileid}-->Description generated for each chunk successfully')
        # logger.debug(f'Response List: {response_list}')
        description_lst = []
        ## CR0312: Moving the constant outside of the loop
        doclanguage = 'English'
        title = None
        for idx , response in enumerate(response_list):
            titletemp=response.pop('title',response.pop('Title',None))
            if title is None and titletemp not in self.negative_list :
                title = titletemp 
            doclanguage=response.pop('Language',response.pop('language','English'))
            for key,value in response.items():
                if key.lower() == 'description':
                    if value and value not in self.negative_list :
                        description_lst.append(value)
        
        #Consolidate all responses for each chunk
        consolidation_prompt = build_consolidation_prompt(description_lst, doclanguage)
        logger.debug(f'Consolidation Prompt: {consolidation_prompt}')
        response = self.engine.get_json_text_to_text(consolidation_prompt,fileid=self.fileid)

        if isinstance(response, list) and response:
            response = response[0]
        if not isinstance(response, dict):
            raise ValueError(f"LLM returned invalid response: {response}")
        ret_response ={}
        ret_response['title'] = title
        for key,value in response.items():
            ret_response[key] = value

        logger.info(f'{self.fileid}-->Consolidated Summary generated successfully')
        logger.debug(f'{self.fileid}-->Consolidated Summary generated successfully - Response: {response}')
        return ret_response
    
    def summarize(self) -> dict:
        """Main method to run summarizer pipeline."""
        logger.info(f'{self.fileid}-->Processing file with chunking')
        self.response = asyncio.run(self.summarize_in_chunks_async())

        self.__format_response()
        if pd.isnull(self.finalOutput['description']) or self.finalOutput['description'] == '':
            self.response = asyncio.run(self.summarize_in_chunks_async())
            self.__format_response()

        if pd.isnull(self.finalOutput['description']) or self.finalOutput['description'] == '':
            raise UnableToGenerateSummary(self.fileid)

        logger.debug(f'{self.fileid}-->Summary: {self.finalOutput['description']}')
        logger.info(f'{self.fileid}-->Summarization Completed')
        if self.debug:
            self.saveJson()
        
        return self.finalOutput

    def __format_response(self) -> str:
        formatedvalue = lambda value: '<li>'.join([f"{val.strip()}</li>" for idx, val in enumerate(value.split('|'))])
        try:
            for key, value in self.response.items():
                if key.lower() in ['description']:
                    synopsis, summary = '', ''
                    for k, v in value.items():
                        if k.lower() in ['description']:
                            if v and v not in ('None', '', 'Not specified'):
                                summary += f"<b>{k}</b>:<br>{v}<br><br>"
                                
                        else:
                            if v and v not in ('None', '', 'Not specified'):
                                synopsis += f'<b>{k}</b>:<ul><li>{formatedvalue(v)}</ul>'
                    self.finalOutput['description'] = Summarizer.LM_REMOVE_JUNKS(summary)
                    self.finalOutput['gtl_synopsis'] = Summarizer.LM_REMOVE_JUNKS(synopsis)
                if key.lower() in ['title']:
                    if value and value not in ('None', '', 'Not specified'):
                        self.finalOutput['title'] = Summarizer.LM_REMOVE_JUNKS(value)
        except Exception as e:
            logger.error(f"{self.fileid}-->Error while formatting response: {e}", exc_info=True)

    def saveJson(self):
        OUTPUT_DIR = Path(os.path.join(self.cfg.DATA_DIR,self.cfg.CONSULTANT_FLOW_DIR,self.fileid))
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        try:
            json_path = os.path.join(OUTPUT_DIR, f"Summary.json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(self.finalOutput, f, indent=2)
            logger.info(f"{self.fileid}-->Json File Saved at {json_path}")
        except Exception as e:
            logger.error(f"{self.fileid}-->Saving JSON failed at '{json_path}': {e}", exc_info=True)

