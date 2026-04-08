import asyncio, nest_asyncio,logging
import json, os, re
from pathlib import Path
import uuid, pandas as pd

from core.genai.open_ai_client import OpenAiHelper           # wrapper around the OpenAI client
from services.consultant_recommendation.prompt import build_prompt, build_consolidation_prompt 
from core.db.crud import DatabaseManager
from core.exceptions import NotAStatementOfWork, UnableToGenerateSummary
from core.utility import get_custom_logger, split_text
from config import Config as cfg
from config import Configuration

logger = get_custom_logger(__name__)
nest_asyncio.apply()
class SowSummarizer:
    
    LM_REMOVE_JUNKS = lambda value:re.sub(r'[^\w\s\.,\)\(\-\?\}\{\\~!@#\$%^&\*\_></"\':;\]\[\|=\+`]', '', value, flags=re.UNICODE)
    catalogue_offer_df = None
    offerswithhints = None
    offerslist = []
    
    db = DatabaseManager(cfg.DEBUG)
    def __init__(self, fileid: uuid , textContent: str, debug: bool = False):
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
        self.finalOutput = {'summary':''}
        self.engine = OpenAiHelper(correlationid=self.cfg.CORR_ID_SUMMARIZATION)

        ## CR0312: Converting list to set
        self.negative_list = ['None','','Not specified','Not Available',
                              'Not explicitly mentioned','Not Provided',
                              'Not specified in the document','None explicitly stated']

        self.semaphore = asyncio.Semaphore(os.cpu_count()*4)
        if SowSummarizer.catalogue_offer_df is None:
            SowSummarizer.catalogue_offer_df = SowSummarizer.db.get_vwofferfamilydata()
            SowSummarizer.catalogue_offer_df['offer'] = SowSummarizer.catalogue_offer_df['offer'].str.upper()
            SowSummarizer.offerslist = SowSummarizer.catalogue_offer_df['offer'].drop_duplicates().to_list()
            SowSummarizer.offerswithhints = '\n'.join(SowSummarizer.catalogue_offer_df[['offer','hints']]
                               .drop_duplicates()
                               .apply(lambda rows: f'{rows['offer']} [HINTS: Look for keywords-{rows['hints']}]' if rows['hints'] else rows['offer'],axis=1).to_list())
        
        if self.debug:
            logger.setLevel(logging.DEBUG)


    async def _call_llm_offers_async(self, text: str,idx:int = 0, ischunked: bool = False) -> dict:
        """Main method to run summarizer pipeline."""
        async with self.semaphore:

            prompt = build_prompt(text,offers=SowSummarizer.offerswithhints, ischunked=ischunked)
            # logger.debug(f'Prompt generated for chunk {idx}: {prompt}')
            response = self.engine.get_json_text_to_text(prompt,fileid=self.fileid,requestid=f'chunk-{str(idx)}-{str(ischunked)}')
            # rawresponse ={
            #     "requestid": f'chunk-{str(idx)}-{str(ischunked)}',
            #     "fileid": self.fileid,
            #     "raw_response": response
            # }
            # SowSummarizer.db.insert_raw_response([rawresponse])
            logger.debug('{self.fileid}-->LLM Call completed for chunk {idx}- Response: %s', response)

            if isinstance(response, list) and response:
                response = response[0]
            if not isinstance(response, dict):
                raise ValueError(f"LLM returned invalid response for chunk {idx}: {response}")
            logger.info(f'{self.fileid}-->Offers predicted successfully for Chunk: {idx}')
            return response
    
    async def summarize_in_chunks_async(self) -> dict:
        chunklst = split_text(self.textContent,chunk_size=self.cfg.CHUNK_SIZE_RECOMMENDATION, chunk_overlap=self.cfg.OVER_LAP_SIZE_RECOMMENDATION)
        logger.info(f'{self.fileid}-->{len(chunklst)} chunks generated')
        if len(chunklst) == 1:
            return await self._call_llm_offers_async(text = self.textContent)
        
        task=[asyncio.create_task(self._call_llm_offers_async(text = chunk, idx = idx+1,ischunked = True )) for idx, chunk in enumerate(chunklst)]
        response_list = await asyncio.gather(*task,return_exceptions=False)
        logger.info(f'{self.fileid}-->Offers generated for each chunk successfully')
        # logger.debug(f'Response List: {response_list}')
        customerName = None
        description_lst = []
        predicted_offer_lst = []
        ## CR0312: Moving the constant outside of the loop
        offers_keys = {'offers','offer'}
        sowlanguage = 'English'
        for idx , response in enumerate(response_list):
            custname=response.pop('Customer Name',response.pop('customer name',response.pop('CustomerName',None)))
           
            sowlanguage=response.pop('Language',response.pop('language','English'))
           
            if customerName is None and custname not in self.negative_list :
                customerName = custname 

            desc_dict = {}
            for key,value in response.items():
                if key.lower() in offers_keys:
                    if isinstance(value, list):
                        # for off in value:
                        #     predicted_offer_lst.append(off)
                        ## CR0312: Extending the list
                        predicted_offer_lst.extend(value)
                    else:
                        predicted_offer_lst.append(value)
                else:
                    if value and value not in self.negative_list :
                        desc_dict[key] = value

            description_lst.append(desc_dict)
        
        predicted_offerdf = pd.DataFrame(predicted_offer_lst)
        
        predicted_offerdf = predicted_offerdf[predicted_offerdf['OfferName'].isin(SowSummarizer.offerslist)]
        ## CR0312: Replaced apply with astype, TODO: need to test and confirm
        predicted_offerdf['Relevance Score'] = predicted_offerdf['Relevance Score'].astype('float')
        # predicted_offerdf['Relevance Score'] = predicted_offerdf['Relevance Score'].apply(float)
        predicted_offerdf = predicted_offerdf.groupby('OfferName').agg(
                                            RelevanceScore=pd.NamedAgg(column="Relevance Score", aggfunc="max"),
                                            NoOfChunks=pd.NamedAgg(column="Relevance Score", aggfunc="size"),
                                            Reason=pd.NamedAgg(column="Reason", aggfunc=lambda x: '. '.join(x))
                                        ).reset_index().sort_values(['RelevanceScore','NoOfChunks'], ascending=False).rename(columns={'RelevanceScore':'Relevance Score'})
        # predicted_offerdf = predicted_offerdf.set_index('OfferName').join(SowSummarizer.catalogue_offer_df.rename(columns={'offer':'OfferName'})[['OfferName','hints']].set_index('OfferName'),how='inner')
        # predicted_offerdf['hints'] = predicted_offerdf['hints'].apply(lambda val: f'Look for keywords-{val}' if len(val)>0 else '')
        offer_lst = predicted_offerdf.to_dict('records')

        #Consolidate all responses for each chunk
        consolidation_prompt = build_consolidation_prompt(description_lst, offer_lst, sowlanguage)
        logger.debug(f'{self.fileid}-->Consolidation Prompt: {consolidation_prompt}')
        response = self.engine.get_json_text_to_text(consolidation_prompt,fileid=self.fileid,requestid='SOW-consolidation')

        if isinstance(response, list) and response:
            response = response[0]
        if not isinstance(response, dict):
            raise ValueError(f"LLM returned invalid response: {response}")
        ret_response ={}
        ret_response['Customer Name'] = customerName
        for key,value in response.items():
            ret_response[key] = value

        logger.info(f'{self.fileid}-->Consolidated Summary generated successfully')
        logger.debug(f'{self.fileid}-->Offers predicted successfully - Response: {response}')
        return ret_response
    
    def summarize(self) -> dict:
        """Main method to run summarizer pipeline."""
        logger.info(f'{self.fileid}-->Processing file with chunking')
        self.response = asyncio.run(self.summarize_in_chunks_async())

        self.__format_response()
        if pd.isnull(self.summary) or self.summary == '':
            self.response = asyncio.run(self.summarize_in_chunks_async())
            self.__format_response()

        if pd.isnull(self.summary) or self.summary == '':
            raise UnableToGenerateSummary(self.fileid)

        logger.debug(f'{self.fileid}-->Summary: {self.summary}')
        logger.debug(f'{self.fileid}-->Others: {self.tags}')

        self.finalOutput['summary']=SowSummarizer.LM_REMOVE_JUNKS(self.summary)
        self.finalOutput = (self.finalOutput|self.tags)

        logger.debug(f'{self.fileid}-->Summarization Final Output: {self.finalOutput}')
        logger.info(f'{self.fileid}-->Summarization Completed')
        if self.debug:
            self.saveJson()
        
        return self.finalOutput

    def __format_response(self) -> str:
        # wrapper = textwrap.TextWrapper(width=200)
        # formatedvalue = lambda value:'<li>'.join([f"{val.strip()}</li>" for idx,val in enumerate(value.split('|'))])
        def formatedvalue(val):
            vallst= val.split('|')
            if len(vallst)>1:
                return ('<li>'.join([f"{item.strip()}</li>" for item in vallst]), True)
            else:
                return (val, False)
        
        try:

            for key,value in self.response.items():
                logger.debug(f"{self.fileid}-->In Format Json Response:{key}-->{value}")
                if value is None or value in self.negative_list:
                    continue
                # CR0312: lower called on key only once. 
                # CR0312: TODO: Replace in with exact match and create constants out of the for loop.
                # CR0312: TODO: Is it possible to create html templace and replace values?
                lk = key.lower()
                if lk == 'error':
                    raise NotAStatementOfWork(self.fileid,f"Not a Statement Of Work. Response: {self.response}")
                elif lk == 'customer name':
                    # word_list = wrapper.wrap(text=value)
                    self.summary+='<b>Customer Name:</b><br>'+(value)+'<br>'
                elif lk == 'objective':
                    # word_list = wrapper.wrap(text=value)
                    # self.summary+='<br><b>Objective:</b><br>'+('<br>'.join(word_list))
                    self.summary+='<b>Objective:</b><br>'+value+'<br><br>'
                elif lk == 'scope of work':
                    formatedval, hasmultiple = formatedvalue(value)
                    if hasmultiple:
                        self.summary+='<b>Scope of Work:</b><ol><li>'+(formatedval)+'</ol>'
                    else:
                        self.summary+='<b>Scope of Work:</b><br>'+formatedval+'<br><br>'
                elif lk == 'deliverables':
                    formatedval, hasmultiple = formatedvalue(value)
                    if hasmultiple:
                        self.summary+='<b>Deliverables:</b><ol><li>'+(formatedval)+'</ol>'
                    else:
                        self.summary+='<b>Deliverables:</b><br>'+formatedval+'<br><br>'
                elif lk in ['offers','offer']:
                    
                    html_table_header = "<b>Sow inclines towards below Offers:</b><br><table>"
                    html_table_header += "<tr><th>Offer Name</th><th>Relevance Score</th></tr>"
                    html_table = ''
                    if isinstance(value, list):
                        value = sorted(value, key=lambda d: float(d.get("Relevance Score",d.get('RelevanceScore'))), reverse=True) 

                        for off in value:
                            if 'RelevanceScore' in off:
                                off['Relevance Score'] = off.pop('RelevanceScore')
                            
                            conf_score=float(off.get('Relevance Score'))
                                
                            if (off.get('OfferName') in SowSummarizer.offerslist) and conf_score > self.cfg.THRESHOLD_SOW_OFFER_SCORE:
                                html_table +=f"<tr><td>{off.get('OfferName').title()}</td><td>{conf_score*100}%</td></tr>"
                            
                    else:
                        conf_score=float(value.get('Relevance Score',value.get('RelevanceScore')))
                        if (value.get('OfferName') in SowSummarizer.offerslist) and conf_score > self.cfg.THRESHOLD_SOW_OFFER_SCORE:
                            html_table +=f"<tr><td>{value.get('OfferName').title()}</td><td>{conf_score*100}%</td></tr>"
                    
                    if len(html_table) > 0:
                        self.summary += html_table_header + html_table + "</table>"
                    # self.summary+=html_table
                    self.tags['offer'] = value

                else: 
                    if isinstance(value, list):
                        self.tags[lk] = value
                    else:
                        self.tags[lk] = list(map(lambda x: x.strip(), value.split('|')))

        except Exception as e:
            logger.error(f"{self.fileid}-->Error while formatting response: {e}", exc_info=True)

    def saveJson(self):
        OUTPUT_DIR = Path(os.path.join(self.cfg.DATA_DIR,cfg.CONSULTANT_FLOW_DIR,self.fileid))
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        try:
            json_path = os.path.join(OUTPUT_DIR, f"SowSummary.json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(self.finalOutput, f, indent=2)
            logger.info(f"{self.fileid}-->Json File Saved at {json_path}")
        except Exception as e:
            logger.error(f"{self.fileid}-->Saving JSON failed at '{json_path}': {e}", exc_info=True)

