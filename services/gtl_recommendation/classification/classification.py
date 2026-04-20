import asyncio, nest_asyncio, pandas as pd
import json, os, re
import logging
from thefuzz import process, fuzz
from typing import List, Dict
from pathlib import Path
from core.db.crud import DatabaseManager
from core.genai.open_ai_client import OpenAiHelper
from services.gtl_recommendation.classification.prompt import build_consolidation_prompt, build_prompt, build_iptype_prompt, build_consolidation_with_iptype_prompt, build_metadata_regeneration_prompt
from config import Configuration
from core.utility import get_custom_logger,split_text
logger = get_custom_logger(__name__)
# logger.propagate = False
nest_asyncio.apply()
 
class Classifier:
   
    LM_REMOVE_JUNKS = lambda value:re.sub(r'[^\w\s\.,\)\(\-\?\}\{\\~!@#\$%^&\*\_></"\':;\]\[\|=\+`]', '', value, flags=re.UNICODE)
    catalogue_offer_df = None
    offers = None
    ip_types_df = None  # Class variable for IP types
    offerslist = None
    def __init__(self, textContent: str, debug: bool = False, **kwargs):
        self.textContent = textContent
        self.cfg=Configuration()
        self.cfg.load_active_config()
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
        # Extract fileid and iptypes from kwargs
        self.dafileid = kwargs.get('daFileId')  # Use 'uuid' from payload
        self.request_id = kwargs.get('requestId')  # Use 'uuid' from payload
        self.fuuid = kwargs.get('uuid')
        self.filename = kwargs.get('name')
        self.master_pattern = ""
        self.waspdf = kwargs.get('waspdf',False)
        self.iptypes = kwargs.get('ipTypes', [])  # Get ipTypes from frontend
        self.response = None
        self.finalOutput = {}
        self.engine = OpenAiHelper(correlationid=self.cfg.CORR_ID_CLASSIFICATION)
        self.semaphore = asyncio.Semaphore(2)
        
        self.negative_list = ['None','','Not specified','Not Available',
                              'Not explicitly mentioned','Not Provided',
                              'Not specified in the document','None explicitly stated']
        self.WRONG_AUTHOR = ['Unknown','dell','dell inc.','test','Not mentioned','not exclusively mentioned', 'dell sas']
        self.db = DatabaseManager()
        # Initialize offers if not already loaded
        if Classifier.catalogue_offer_df is None or Classifier.ip_types_df is None:
            
            Classifier.catalogue_offer_df = self.db.get_vwofferfamilydata()
            Classifier.offers = '\n'.join(Classifier.catalogue_offer_df[['offer','hints']]
                               .drop_duplicates()
                               .apply(lambda rows: f'{rows['offer'].upper()}  [HINTS: Look for "{rows['hints'].lower()}" keywords]' if rows['hints'] else rows['offer'].upper(),axis=1).to_list())
            Classifier.offerslist = Classifier.catalogue_offer_df['offer'].str.upper().drop_duplicates().to_list()
            
            # Initialize IP types if not already loaded
            Classifier.ip_types_df = self.db.get_unique_ip_types()
 
 
    async def _call_llm_offers_async(self, text: str, idx: str = '1/1', ischunked: bool = False) -> dict:
        async with self.semaphore:
            prompt = build_prompt(text, offerslst=Classifier.offers, ischunked=ischunked)
            response = self.engine.get_json_text_to_text(prompt, fileid=self.dafileid,requestid=f'classification-chunk-{str(idx)}-{str(ischunked)}')
 
            if isinstance(response, list) and response:
                response = response[0]
            if not isinstance(response, dict):
                raise ValueError(f"LLM returned invalid response: {response}")
            logger.info(f'{self.dafileid}-->Offers predicted successfully for Chunk: {idx}')
            logger.debug(f'{self.dafileid}-->Offers predicted successfully - Response: {response}')
            return response

    #modify signature to take dictionary of response
    async def _predict_iptype_from_description(self, response_dict: dict) -> dict:
        """Helper method to predict IP type from title and description using LLM."""
        gendesc = {"Title":response_dict.get('title', ''),
                  "Description":response_dict.get('description', response_dict.get('Description', {})).get('Description', '') }
        
        ip_types_list = '\n'.join(Classifier.ip_types_df['ip_type'].drop_duplicates().str.upper().to_list())
        prompt = build_iptype_prompt(str(gendesc), ip_types_list)
        
        response = self.engine.get_json_text_to_text(prompt, fileid=self.dafileid,requestid='ip-type-prediction')
        
        if isinstance(response, list) and response:
            response = response[0]
        if not isinstance(response, dict):
            raise ValueError(f"LLM returned invalid response: {response}")
        
        logger.info(f'{self.dafileid}-->IP Type predicted from description')
        logger.debug(f'{self.dafileid}-->IP Type Response: {response}')
        return response

    async def classify_no_chunk(self) -> dict:
        """Classification for non-chunked files (small files)."""
        # Step 1: Call LLM for offers
        response = await self._call_llm_offers_async(text=self.textContent)

        # Step 2: Check if we need to predict IP type
        if not self.iptypes or len(self.iptypes) == 0:
            logger.info(f'{self.dafileid}-->No IP types from frontend, predicting IP type')
            iptype_response = await self._predict_iptype_from_description(response)
            response['ip_types'] = iptype_response.get('ip_types', [])
        else:
            logger.info(f'{self.dafileid}-->IP types provided from frontend: {self.iptypes}')
            # response['ip_types'] = []
        return response
 
 
    async def classify_in_chunks(self) -> dict:
        chunklst = split_text(self.textContent, chunk_size=self.cfg.CHUNK_SIZE_CLASSIFICATION, chunk_overlap=self.cfg.OVER_LAP_SIZE_CLASSIFICATION)
       
        # NEW: If only 1 chunk, use classify_no_chunk instead
        if len(chunklst) == 1:
            return await self.classify_no_chunk()
       
        # Call LLM for each chunk (now returns only offers, no IP type)
        task = [asyncio.create_task(self._call_llm_offers_async(text=chunk, idx=f"{idx+1}/{len(chunklst)}", ischunked=True)) for idx, chunk in enumerate(chunklst)]
        response_list = await asyncio.gather(*task, return_exceptions=False)
       
        logger.info(f'{self.dafileid}-->Offers generated for each chunk successfully')
 
        # Process offers
        title = None
        language = None
        author = None
        description_lst = []
        predicted_offer_lst = []
       
        for idx, response in enumerate(response_list):
            if title is None and response.get('title') is not None:
                if response.get('title').lower() not in self.negative_list:
                    title = response.get('title')
            
            if language is None and response.get('language') is not None:
                if response.get('language').lower() not in self.negative_list:
                    language = response.get('language')
            
            
            if author is None and response.get('author') is not None:
                if response.get('author').lower() not in self.negative_list + self.WRONG_AUTHOR:
                    author = response.get('author')

            description_lst.append(response.get('description'))
            predicted_offer_lst.append(response.get('offer'))
       
        predicted_offerdf = pd.DataFrame(predicted_offer_lst)
            
        predicted_offerdf = predicted_offerdf[predicted_offerdf['OfferName'].isin(Classifier.offerslist)]
                
        if predicted_offerdf.shape[0] == 0:
            logger.warning(f'{self.dafileid}-->No matching offers found in catalogue')
            offer_lst = {}
        else:
            predicted_offerdf['Relevance Score'] = predicted_offerdf['Relevance Score'].apply(float)
            predicted_offerdf = predicted_offerdf.groupby('OfferName').agg(
                RelevanceScore=pd.NamedAgg(column="Relevance Score", aggfunc="max"),
                NoOfChunks=pd.NamedAgg(column="Relevance Score", aggfunc="size"),
                Reason=pd.NamedAgg(column="Reason", aggfunc=lambda x: '. '.join(x))
            ).reset_index().sort_values(['RelevanceScore', 'NoOfChunks'], ascending=False).rename(columns={'RelevanceScore': 'Relevance Score'})
            # predicted_offerdf = predicted_offerdf[predicted_offerdf['Relevance Score'] >= 0.6]
            # predicted_offerdf.rename(columns={'Confidence Score': 'Relevance Score'}, inplace=True)
            offer_lst = predicted_offerdf.to_dict('records')
        
        # NEW: Check if we need to predict IP types
        if self.iptypes is None or len(self.iptypes) == 0:
            logger.info(f'{self.dafileid}-->No IP types from frontend, including IP type prediction in consolidation')
            ip_types_list = '\n'.join(Classifier.ip_types_df['ip_type'].drop_duplicates().str.upper().to_list())
            
            consolidation_prompt = build_consolidation_with_iptype_prompt(descriptionlist = description_lst,
                                                              offerlist = offer_lst, 
                                                              iptypelist = ip_types_list, 
                                                              language = 'english')
        else:
            logger.info(f'{self.dafileid}-->IP types provided from frontend: {self.iptypes}, skipping IP type prediction')
            
            consolidation_prompt = build_consolidation_prompt(descriptionlist = description_lst, 
                                                              offerlist = offer_lst, 
                                                              language = 'english')
 
        response = self.engine.get_json_text_to_text(consolidation_prompt, fileid=self.dafileid,requestid='consolidation_classfication')
 
        if isinstance(response, list) and response:
            response = response[0]
        if not isinstance(response, dict):
            raise ValueError(f"LLM returned invalid response: {response}")
        response['title'] = title
        response['author'] = author
        # response['language'] = language
        logger.info(f'{self.dafileid}-->Offers and IP Types consolidated successfully')
        logger.debug(f'{self.dafileid}-->Consolidated Response: {response}')
        return response
    
    def _redact_text(self, text: str, lookup: dict) -> str:
        if not self.master_pattern:
            return text
        

        # match is regex Match object passed and it calls automatically.
        def replace_func(match):
            # this will print text which got mached
            matched_text = match.group(0)
            placeholder = lookup.get(matched_text.lower(), "")
            
            return placeholder

        return self.master_pattern.sub(replace_func, str(text))

    def regenerate_metadata(self, redacted_metadata: Dict) -> Dict:
        if not redacted_metadata:
            return redacted_metadata

        prompt = build_metadata_regeneration_prompt(redacted_metadata)
        response = self.engine.get_json_text_to_text(
            prompt,
            fileid=self.dafileid,
            requestid="metadata-regeneration"
        )

        if response and isinstance(response, dict):
            return response
    
    def sanitize_metadata(self):
        df_sensitive = self.db.get_sensitiveinfo_list(dafileid=self.dafileid, requestid=self.request_id, fuuid=self.fuuid)
        lookup = {
            str(row['sensitivetext']).lower(): f"<{str(row['category']).upper().replace(' ', '_')}>"
            for _, row in df_sensitive.iterrows()
        }
        # get list of sensitivetext and sort it descending with length
        sorted_terms = sorted(lookup.keys(), key=len, reverse=True)
        pattern_string = "|".join(re.escape(term) for term in sorted_terms if term)
        self.master_pattern = re.compile(f"({pattern_string})", re.IGNORECASE) if pattern_string else None

        doc_metadata = {
            "filename" : self.filename,
            "title" : self.finalOutput.get("title"),
            "description" : self.finalOutput.get("description")
        }

        redacted_item = {
                label: self._redact_text(val, lookup) 
                for label, val in doc_metadata.items()
            }
        if not any([doc_metadata['filename']==redacted_item['filename'], doc_metadata['title']==redacted_item['title'],doc_metadata['description']==redacted_item['description']]):
            regeneratedmetadata = self.regenerate_metadata(redacted_item)
            self.finalOutput['filename'] = regeneratedmetadata['filename']
            self.finalOutput['title']= regeneratedmetadata['title']
            self.finalOutput['description'] = regeneratedmetadata['description']
            logger.info(f"Redacted metedata for classification output for fileid : {self.dafileid}")

        if doc_metadata['filename'].lower()!=redacted_item['filename'].lower():
            self.finalOutput['gtl_synopsis'] += f'<hr><b>Filename has been sanitized.</b><br> <b>Original File Name</b> : {doc_metadata['filename']}'

 
    def classify(self) -> dict:
        """Main method to run classification pipeline."""
        def best_match(query, choices):
            match = process.extractOne(query, choices, scorer=fuzz.token_set_ratio)
            return match[0].upper() if match else None
        
        self.response = asyncio.run(self.classify_in_chunks())
       
        self.__format_response()
        if 'offer' not in self.finalOutput:
            self.response = asyncio.run(self.classify_in_chunks())
            self.__format_response()
        
        self.sanitize_metadata()

        if self.debug:
            self.saveJson()
        finalIpType = self.iptypes if len(self.iptypes)>0 else self.finalOutput['ip_type']
        finalIpType = [best_match(typ, Classifier.ip_types_df['ip_type'].str.upper().to_list()) for typ in finalIpType]
        fIpTypeDf = Classifier.ip_types_df[Classifier.ip_types_df['ip_type'].str.upper().isin(finalIpType)]
        
        self.finalOutput['ip_type'] = '|'.join(fIpTypeDf['final_ip_type'].drop_duplicates().to_list())
        self.finalOutput['dtpm_phase'] = '|'.join(fIpTypeDf['dtpm_phase'].drop_duplicates().to_list())
        # self.finalOutput['requestid'] = self.request_id
        # self.finalOutput['status'] = 'Classification Completed'
        # self.db.update_document(where_clause={'requestid': self.request_id,'daoriginal_fileid': self.dafileid},update_values=self.finalOutput)
        logger.info(f'{self.dafileid}-->Classification completed successfully')

        return self.finalOutput
           
    def __format_response(self) -> str:
        formatedvalue = lambda value: '<li>'.join([f"{val.strip()}</li>" for idx, val in enumerate(value.split('|'))])
        try:
            for key, value in self.response.items():
                if value is None:
                    continue
                if key.lower() in ['description']:
                    description = ''
                    gtl_synopsis = ''
                    # gtl_synopsis = '<i><b>Note: This document is recreated from a pdf file.</b></i><br>' if self.waspdf else ''
                    desired_order = ['Description', 'Others', 'Key Points', 'Sections']
                    sorted_dict = {key: value[key] for key in desired_order if key in value}
                    for k, v in sorted_dict.items():
                        if k.lower() in ['description']:
                            if v and v not in self.negative_list:
                                # description += f"<b>{k}</b>:<br>{v}<br><br>"
                                description = v
                        else:
                            if v and v not in self.negative_list:
                                if k.lower() in ['others']:
                                    gtl_synopsis += f'<p>{formatedvalue(v)}</p>'
                                else:
                                    gtl_synopsis += f'<b>{k}</b>:<ul><li>{formatedvalue(v)}</ul>'
                    self.finalOutput['description'] = Classifier.LM_REMOVE_JUNKS(description)
                    self.finalOutput['gtl_synopsis'] = Classifier.LM_REMOVE_JUNKS(gtl_synopsis)

                if key.lower() in ['title']:
                    if value.lower() not in self.negative_list:
                        self.finalOutput['title'] = Classifier.LM_REMOVE_JUNKS(value)

                if key.lower() in ['author']:
                    if value.lower() not in self.negative_list + self.WRONG_AUTHOR:
                        short_author = Classifier.LM_REMOVE_JUNKS(value)
                        self.finalOutput['author'] = short_author[:90]

                if key.lower() in ['offers', 'offer']:
                    html_table_header = "<hr><b>Document inclines towards below Offers:</b><br><table>"
                    html_table_header += "<tr><th>Offer Name</th><th>Relevance Score</th></tr>"
                    html_table = ''

                    if isinstance(value, list):
                        value = sorted(value, key=lambda d: float(d.get("Relevance Score",d.get('RelevanceScore'))), reverse=True) 
                        finalOfferList = []
                        for off in value:
                            if 'RelevanceScore' in off:
                                    off['Relevance Score'] = off.pop('RelevanceScore')
                            relv_score=float(off.get('Relevance Score'))
                            if (off.get('OfferName').upper() in Classifier.offerslist) and relv_score > self.cfg.THRESHOLD_DOC_OFFER_SCORE:
                                finalOfferList.append(off)
                                html_table +=f"<tr><td>{off.get('OfferName').title()}</td><td>{relv_score*100}%</td></tr>"
                        if len(finalOfferList)>0:
                            self.finalOutput['offer'] = finalOfferList[0].get('OfferName')
                            self.finalOutput['relevance_score'] = finalOfferList[0].get('Relevance Score')
                    else:
                        relv_score=float(value.get('Relevance Score',value.get('RelevanceScore')))
                        if (value.get('OfferName', '').upper() in Classifier.offerslist) and relv_score > self.cfg.THRESHOLD_DOC_OFFER_SCORE:
                            self.finalOutput['offer'] = value.get('OfferName')
                            self.finalOutput['relevance_score'] = relv_score
                            html_table +=f"<tr><td>{value.get('OfferName').title()}</td><td>{relv_score*100}%</td></tr>"
                    
                    if len(html_table) > 0:
                        self.finalOutput['gtl_synopsis'] += html_table_header + html_table + "</table>"

                if key.lower() in ['ip_types']:
                    if isinstance(value, list):
                        value = sorted(value, key=lambda d: float(d.get("Relevance Score",d.get('RelevanceScore'))), reverse=True) 
                        finalTypeList = []
                        for iptype in value:
                            if 'RelevanceScore' in iptype:
                                iptype['Relevance Score'] = iptype.pop('RelevanceScore')
                            relv_score=float(iptype.get('Relevance Score'))
                            if (iptype.get('Type','NotPredicted').upper() in Classifier.ip_types_df['ip_type'].str.upper().to_list()) and relv_score > self.cfg.THRESHOLD_DOC_TYPE_SCORE:
                                finalTypeList.append(iptype)
                        self.finalOutput['ip_type'] = [typ.get('Type') for typ in finalTypeList] 
                    else:
                        relv_score=float(value.get('Relevance Score',value.get('RelevanceScore')))
                        if (value.get('Type', 'NotPredicted').upper() in Classifier.ip_types_df['ip_type'].str.upper().to_list()) and relv_score > self.cfg.THRESHOLD_DOC_TYPE_SCORE:
                            self.finalOutput['ip_type'] = [value.get('Type')] 
        except Exception as e:
            logger.error(f"{self.dafileid}-->Error while formatting response: {e}", exc_info=True)
 
    def saveJson(self):
        # Convert the UUID to a string before building the path
        OUTPUT_DIR = Path(os.path.join(self.cfg.DATA_DIR,self.cfg.GTL_FLOW_DIR,str(self.dafileid)))
        # Make sure the directory exists
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        try:
            json_path = OUTPUT_DIR / "DocSummary.json"
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(self.finalOutput, f, indent=2)
            logger.info(f"{self.dafileid}-->Json File Saved at {json_path}")
        except Exception as e:
            logger.error(f"{self.dafileid}-->Saving JSON failed at '{json_path}': {e}", exc_info=True)