import asyncio, nest_asyncio
from pathlib import Path
import logging
import re, uuid, os
from typing import List, Dict

import pandas as pd 
import json

from core.s3_helper import StorageManager
from core.db.crud import DatabaseManager
from core.exceptions import NoSensitiveItemFound
import services.gtl_recommendation.sensitive_text_ext.prompt as pmt
from core.genai.open_ai_client import OpenAiHelper
from services.gtl_recommendation.sensitive_text_ext.regex_pattern import REGEX_PATTERNS
from core.utility import get_custom_logger, split_text, chunk_list
from config import Configuration
logger = get_custom_logger(__name__)
# logger.propagate = False
nest_asyncio.apply()

class TextExtractor:
    def __init__(self,requestid:uuid =None, fuuid:uuid=None, dafileid:uuid=None,inputText="", correlationid = None, debug: bool = False, filepath: Path=None, threshold: float= 0.6, json_files: List[Path]=[]):
        self.requestid = requestid
        self.fuuid = fuuid 
        self.dafileid = dafileid
        self.filepath = filepath
        self.json_files = json_files

        self.correlationid = correlationid
        self.inputText = inputText
        self.jsonInputText = []
        self.threshold = threshold
        self.tobe_redacted = []
    

        self.responseList: List[Dict] = []
        self.sensitiveInfoList: List[Dict] = []
        self.semaphore = asyncio.Semaphore(2)
        if inputText != "":
            self.llm = OpenAiHelper(correlationid=correlationid)
        self.cfg=Configuration()
        self.cfg.load_active_config()

        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)

    def get_s3_filecontent(self):
                
        s3 = StorageManager()
        filepath = Path(os.path.join(self.cfg.GTL_FLOW_DIR,self.dafileid,'extraction_input.txt'))

        local_path = s3.download(s3._make_s3_key(filepath))
        logger.info(f"Downloaded to: {local_path}")

        with open(local_path, "r", encoding="utf-8") as f:
            self.inputText=f.read()
        
        logger.info(f"Full text pulled from s3")

    def _extract_items_from_json(self):
        if len(self.json_files) == 0:
            return []
        db = DatabaseManager()
        lookup_df = db.query_database(f"SELECT lower(colname) col_name FROM {self.cfg.DATABASE_SCHEMA}.tredaction_col_lookup")
        sensitive_records = []
        for file in self.json_files:
            with open(file, 'rb') as f:
                # records = json.load(f)
                # Robust: read bytes, decode UTF-8; fallback to latin-1 with replacement
                raw = f.read()
            try:
                text = raw.decode("utf-8")
                used_encoding = "utf-8"
            except UnicodeDecodeError:
                text = raw.decode("latin-1", errors="replace")
                used_encoding = "latin-1/replace"
 
            logger.info(f"{self.dafileid}-->Decoded {file} using {used_encoding}")
            records = json.loads(text)
            
            orig_str = '--- TABLE START ---\n'+ str('\n'.join([str(record) for record in records]) + '\n') + '--- TABLE END ---\n'
            df = pd.DataFrame(records)
            if df.shape[0]==0:
                continue
            for col in df.columns:
                if col.lower() in lookup_df['col_name'].values:
                    tempdf = df[[col]].rename(columns={col: 'sensitivetext'}).dropna(how='all')
                    tempdf['label'] = re.sub(r'_DUP_\d+$', '', col.upper())
                    tempdf['score'] = 1.0
                    tempdf['source'] = 'json'
                    tempdf[col] = tempdf['sensitivetext'].astype(str).str.strip()
                    sens_records = tempdf.to_dict(orient='records')
                    sensitive_records.extend(sens_records)
                    df.drop(columns=[col], inplace=True)
            
            for col in df.columns:
                column_matches = []
                for value in df[col].dropna():
                    value = str(value)
                    for label, pattern in REGEX_PATTERNS.items():
                        # Add anchors to match the entire value
                        full_pattern = rf'^{pattern}$'
                        if re.match(full_pattern, value, re.IGNORECASE):
                            column_matches.append({
                                "label": label,
                                "sensitivetext": value.strip(),  # Use the entire value
                                "context": value.strip(),  # Context is the full value since we're matching whole value
                                "score": 1.0,
                                "source": "json-regex"
                            })
                            break
                if column_matches:
                    sensitive_records.extend(column_matches)
                    df.drop(columns=[col], inplace=True)
                    logger.info(f"Found {len(column_matches)} full-value matches in column '{col}', column dropped")
            if df.shape[1]>0:
                record = df.to_dict(orient='records')
                replace_text = '--- TABLE START ---\n'+ str('\n'.join([str(rec) for rec in record]) + '\n') + '--- TABLE END ---\n'
                self.inputText = self.inputText.replace(orig_str, replace_text)
            else:
                self.inputText = self.inputText.replace(orig_str, "")

        seen = {(item['label'].strip().lower(), 
                 item['sensitivetext'].strip().lower()):item
                for item in sensitive_records}

        sensitive_records = list(seen.values())

        return sensitive_records

    def _extract_items_regex(self, REGEX_PATTERNS, source):
        sensitiveTextList = []
        for label, pattern in REGEX_PATTERNS.items():
            if source == "LLM":
                label, pattern = pattern, label

            matches = list(re.finditer(pattern, self.inputText, re.IGNORECASE))
            for mat in reversed(matches):
                try:
                    match_str = mat.group(1)
                    if match_str is None:
                        continue
                    startIdx = mat.start() + mat.group().find(mat.group(1))
                    endIdx = startIdx + len(match_str)
                except IndexError:
                    match_str = mat.group(0)
                    startIdx = mat.start()
                    endIdx = mat.end()
                # sensitiveTextList.append({
                #     "label": label,
                #     "sensitivetext": match_str,
                #     "context": self.inputText[max(0, startIdx - 50):min(len(self.inputText), endIdx + 50)]
                # })
                # Find the start of the line
                line_start = self.inputText.rfind('\\n', 0, startIdx)
                line_start = line_start+2 if line_start != -1 else self.inputText.rfind('\n', 0, startIdx)+1
                # if line_start == 0:  # No newline found, we're at the beginning
                #     line_start = 0
                # else:
                #     line_start += 1
                
                if startIdx-line_start > 50:
                    line_start = self.inputText.rfind(' ', 0, startIdx-50)
                    # line_start = startIdx - 50

                # Find the end of the line
                line_end = self.inputText.find('\\n', endIdx)    
                line_end = line_end if line_end != -1 else self.inputText.find('\n', endIdx)

                if line_end == -1:  # No newline found, we're at the end
                    line_end = len(self.inputText)
   
                if line_end-endIdx > 50:
                    line_end = self.inputText.find(' ', endIdx + 50) 
                    # line_end = endIdx + 50

                sensitiveTextList.append({
                    "label": label,
                    "sensitivetext": str(match_str).strip(),
                    "context": str(self.inputText[line_start:line_end])
                })
        logger.info(f"{self.dafileid}-{source}-Length of regex matches: {len(sensitiveTextList)}")
        if len(sensitiveTextList)==0:
            return []

        self._verify_with_llm(sensitiveTextList,source = source)   
        sensitiveInfoList = self._flatten_respones(source=source)
        logger.info(f"{self.dafileid}-{source}-Length of regex matches after validation: {len(sensitiveInfoList)}")  

        if source=='REGEX':   
            self._apply_redactions(sensitiveInfoList)
        return sensitiveInfoList  

    def _apply_redactions(self,sensitiveInfoList):
        """NEW METHOD - Safe replacement AFTER LLM validation"""
        for item in sensitiveInfoList:
            if float(item.get('score',0)) > 0.5:
                placeholder = f"<{item['label'].replace(' ', '_')}>"
                # SAFE regex replacement with word boundaries
                pattern = rf'\b{re.escape(item["sensitivetext"])}\b'
                self.inputText = re.sub(pattern, placeholder, self.inputText, flags=re.IGNORECASE)
        pattern = r'^\s*<.*?>\s*$'
        cleaned = re.sub(pattern, '', self.inputText, flags=re.MULTILINE)
        self.inputText = re.sub(r'\n{2,}', '\n', cleaned).strip()
 
    def _verify_with_llm(self,sensitiveTextList,source):
        #remove duplicates from identified list
        seen = {(item['label'].strip().lower(), 
                   item['sensitivetext'].strip().lower(), 
                   item['context'].strip().lower()):item
                  for item in sensitiveTextList}
        sensitiveTextList = list(seen.values())
        logger.info(f"{self.dafileid}-{source}-Length of {source} matches after deduplication: {len(sensitiveTextList)}")  
    
        #group by label and sensitivetext and take top 5 contexts
        df = pd.DataFrame(sensitiveTextList)
        df['context_length'] = df['context'].str.len()
        sensitiveTextList = []

        for (label, sensitivetext), group in df.groupby(['label', 'sensitivetext']):
            top_5 = group.nlargest(5, 'context_length')

            contexts = [f"Matched Line {idx+1}:- {row['context']}" 
                       for idx, (_, row) in enumerate(top_5.iterrows())]

            sensitiveTextList.append({
                'label': label,
                'sensitivetext': sensitivetext,
                'contexts': contexts
            })
        logger.info(f"{self.dafileid}-{source}-Length of {source} matches after grouping: {len(sensitiveTextList)}")  
        
        #call llm to validate
        self.responseList = []
        for item in chunk_list(sensitiveTextList,8000):
            prompt = pmt.build_validation_prompt(item)
            validation_response = self.llm.get_json_text_to_text(prompt, fileid=self.dafileid,requestid=f'redaction-{source.lower()}-validation-NoItems-{str(len(item))}')
            if len(validation_response) > 0:
                self.responseList.append(validation_response)
            logger.info(f"{self.dafileid}-{source}- Input validation items: {len(item)} - Output validated items: {len(validation_response)}")
    
    async def _call_llm(self,text, idx =0):
        async with self.semaphore:
            for cat in pmt.ParameterList.keys():
                prompt = pmt.build_prompt(cat,text,Path(self.filepath).suffix.lower())
                response = self.llm.get_json_text_to_text(prompt,fileid=self.dafileid,requestid=f'redaction-chunk-{cat}-{str(idx)}')
                if len(response) > 0:
                    self.responseList.append(response)
                    
            logger.info(f"{self.dafileid}-->Sensitive Items Extraction completed for Chunk: {idx}")

    async def _process_async(self):
        self.responseList = []
        chunks = split_text(self.inputText, chunk_size = self.cfg.CHUNK_SIZE_REDACTION, chunk_overlap = self.cfg.OVER_LAP_SIZE_REDACTION)
        logger.info(f"{self.dafileid}-->Total chunks: {len(chunks)} for redaction")
        if len(chunks) == 1:
            await self._call_llm(self.inputText)
            logger.info(f"{self.dafileid}-->Completed processing single chunk")
        else:
            task=[asyncio.create_task(self._call_llm(chunk,idx)) for idx, chunk in enumerate(chunks)]
            await asyncio.gather(*task,return_exceptions=False)
            logger.info(f"{self.dafileid}-->Completed processing all the chunks")

        sensitiveInfoList = self._flatten_respones(source='Llm')
        logger.info(f"{self.dafileid}-->Completed flatten , match size: {len(sensitiveInfoList)}")
        return sensitiveInfoList

    def _flatten_respones(self,source):
        required_keys = ["label", "sensitivetext", "score"]
        sensitiveInfoList = []
        for js in self.responseList:
            logger.debug(js)
            try:
                if (len(js.keys())==0) or ('error' in js):
                    continue
                elif  all(key in js for key in required_keys):
                    sensitiveInfoList.append(js)
                else:
                    [sensitiveInfoList.append(sjs) for key in js.keys() for sjs in js[key] if all(key in sjs for key in required_keys) and isinstance(sjs, dict)]

            except Exception as e:
                logger.error(f"{self.dafileid}-->Failed to parse response: {js} Error: {e}", exc_info=True)

        src = {'source': source}
        sensitiveInfoList = [{**d, **src} for d in sensitiveInfoList]
        return sensitiveInfoList
                
    def extract_sensitive_info(self):
        json_sensitive_items, regex_sensitive_items, llm_sensitive_items = [], [], []
        if self.inputText== "":
            self.get_s3_filecontent()
            logger.info(f"Length of content from s3: {len(self.inputText)}")

        logger.info(f"{self.dafileid}-->Extracting sensitive info from JSON data. Input Text size:{len(self.inputText)}")
        json_sensitive_items = self._extract_items_from_json()
        logger.info(f"{self.dafileid}-->Completed JSON Extraction. Input Text size:{len(self.inputText)}, final JSON size: {len(json_sensitive_items)}")

        if len(self.inputText) >50:
            logger.info(f"{self.dafileid}-->Extracting sensitive info using Regural expression. Input Text size:{len(self.inputText)}")
            regex_sensitive_items = self._extract_items_regex(REGEX_PATTERNS, "REGEX")
            logger.info(f"{self.dafileid}-->Completed RegEx Extraction. Input Text size:{len(self.inputText)}, final regex size: {len(regex_sensitive_items)}")

        if len(self.inputText) >50:
            llm_sensitive_items = asyncio.run(self._process_async())
            logger.info(f"{self.dafileid}-->Completed extracting sensitive information from text , LLM match size: {len(llm_sensitive_items)}")

            # llm_sensitive_items = dict((item['label'], re.escape(item['sensitivetext'])) for item in llm_sensitive_items 
            #                            if item.get('sensitivetext') is not None 
            #                            and float(item.get('score', 0)) > self.threshold 
            #                            and len(item.get('sensitivetext', '').lower().strip()) > 3)
            llm_sensitive_items = dict((re.escape(item['sensitivetext']).lower(), item['label']) for item in llm_sensitive_items
                                       if item.get('sensitivetext') is not None 
                                       and float(item.get('score', 0)) > self.threshold 
                                       and len(item.get('sensitivetext', '').lower().strip()) > 3)
            logger.info(f"Sending llm items for Validation: {len(llm_sensitive_items)}")

            llm_sensitive_items = self._extract_items_regex(llm_sensitive_items, "LLM")
            logger.info(f"Completed Validating LLM identified items: {len(llm_sensitive_items)}")

        if len(regex_sensitive_items) + len(llm_sensitive_items) + len(json_sensitive_items) == 0:
            logger.info(f"{self.dafileid}-->No sensitive information found")
            return False, []
        else:
            self.sensitiveInfoList = regex_sensitive_items + llm_sensitive_items + json_sensitive_items
            logger.info(f"Total items identified: {len(self.sensitiveInfoList)}")
            # self._filter_sensitive_list()
            # logger.info(f"Now adding inclusion items and removing exclusion items: {len(self.sensitiveInfoList)}")
            self.save()
            return len(self.tobe_redacted)>0, self.tobe_redacted
            # raise NoSensitiveItemFound(self.dafileid)
        
    def save(self):
        def _sanitize_for_postgres(value):
            """Remove NUL bytes (0x00, \\u0000) that crash PostgreSQL."""
            if isinstance(value, str):
                return value.replace('\x00', '').replace('\u0000', '')
            elif isinstance(value, dict):
                return {k: _sanitize_for_postgres(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [_sanitize_for_postgres(item) for item in value]
            return value

        def safe_float(value, default=0.0):
            try:
                return float(value)
            except (ValueError, TypeError):
                return default

        try:
            db = DatabaseManager()
            filename = self.filepath.name if self.filepath else "unknown_file"
            self.sensitiveInfoList = _sanitize_for_postgres(self.sensitiveInfoList)
            nonsensitiveitems = [item['sensitivetext'] for item in self.sensitiveInfoList if safe_float(item.get('score',0.0)) == 0]
            for item in self.sensitiveInfoList:
                if item['sensitivetext'] in nonsensitiveitems and safe_float(item['score'])>0:
                    item['source'] = item['source'] + '-defaulted-' + str(item['score'])
                    item['score'] = 0.0
                    
            # 1. Insert extraction results
            db.insert_extraction_results(self.requestid, self.fuuid, self.dafileid, filename, self.sensitiveInfoList)

            # 2. Fetch static config
            inclusion_lst, exclusion_lst = db.get_redaction_config_dict()
            # exc_set = {text.lower().strip() for text in exclusion_lst}
            threshold_items = []
            for js in self.sensitiveInfoList:
                if not pd.isnull(js['label']) and safe_float(js.get('score', 0)) > self.threshold and len(js.get('sensitivetext', '').lower().strip()) > 3 and js.get('sensitivetext', '').lower().strip() not in exclusion_lst:
                    threshold_items.append({
                        'sensitivetext': js['sensitivetext'],
                        'label': js['label']
                    })

            threshold_items.extend(inclusion_lst)
            self.tobe_redacted = threshold_items
            # self.tobe_redacted.sort(key=lambda x: len(x['sensitivetext']), reverse=True)
            logger.info(f"Filtered: {len(self.tobe_redacted)} items")
            # 5. Insert final redaction list
            db.insert_toberedacted_results(self.requestid, self.fuuid, self.dafileid, filename, self.tobe_redacted)

            logger.info(f"{self.dafileid}-->Extraction pipeline complete with request_uuid: {self.fuuid}")
        except Exception as e:
            logger.error(f"{self.dafileid}-->DB insertion failed: {e}", exc_info=True)
            raise