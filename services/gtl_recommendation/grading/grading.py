
from __future__ import annotations
import ast, os, re, uuid, asyncio, json, html
from pathlib import Path
from typing import Dict, List
import pandas as pd


from config import Configuration
from core.genai.open_ai_client import OpenAiHelper
from core.utility import split_text, get_custom_logger
from core.dispatcher import Dispatcher
from core.exceptions import UnableToGradeTheDocument, InvalidMetadataError
from services.gtl_recommendation.grading.content_check_prompts import build_consolidation_prompt
logger = get_custom_logger(__name__)


class ContentGrader:
    def __init__(self, filepath:Path, dafileid:uuid =None , debug:bool = False):
        self.filepath =  Path(filepath)
        self.filename = self.filepath.stem
        self.ext = self.filepath.suffix
        
        
        self.max_concurrency = os.cpu_count()

        self.cfg = Configuration()
        self.cfg.load_active_config()
        self.chunk_size = self.cfg.CHUNK_SIZE_SPELLCHECK
        self.chunk_overlap = self.cfg.OVER_LAP_SIZE_SPELLCHECK

        self.llm = OpenAiHelper(correlationid=self.cfg.CORR_ID_GRADING) 

        self.dafileid = dafileid
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)

    def _clean_text(self, text: str) -> str:
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        return text.strip()

    def _get_chunks(self):
        cleaned = self._clean_text(self.textContent)
        chunklst = split_text(
            cleaned,
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap
        )
        return [
            {"title": f"Chunk {i+1}/{len(chunklst)}",
             "content": chunk.strip()}
            for i, chunk in enumerate(chunklst)
        ]

    def _aggregate(self, chunk_results: List[Dict[str, List[str]]], total_lines: int, total_words: int, chunk_count: int):

        df = pd.DataFrame(chunk_results)
        misspelled, incorrect_lines = df.sum().to_dict().values()

        grammar_accuracy = ((total_lines - incorrect_lines) / total_lines * 100) if total_lines else 0.0
        spelling_accuracy = ((total_words - misspelled) / total_words * 100) if total_words else 0.0

        return {
            "misspelled_words": misspelled,
            "incorrect_lines": incorrect_lines,
            "total_lines": total_lines,
            "total_words": total_words,
            "grammar_accuracy": round(grammar_accuracy, 2),
            "spelling_accuracy": round(spelling_accuracy, 2),
            "chunk_count": chunk_count,
        }
    
    def _get_counts(self):
        """
        Convert each line to dictionary if possible, then count sentences longer than 20 characters 
        in each dictionary value. Ignores lines starting with === or ---.
        Counts unique dictionary keys and all other words.

        Returns:
            dict: A dictionary with total words, total sentences, and list of identified sentences
        """
        lines = self.textContent.strip().split('\n')
        all_sentences = []
        total_words = 0
        unique_keys = set()

        # Pattern to split sentences (handles ., ?, ! followed by whitespace or end of line)
        sentence_pattern = r'[.!?]+\s+|[.!?]+$'
        # Pattern to count words (splits on whitespace)
        word_pattern = r'\s+'

        for line_num, line in enumerate(lines, 1):
            line = line.strip()

            # Skip empty lines and lines starting with === or ---
            if not line or line.startswith('===') or line.startswith('---'):
                continue
            
            # Try to convert line to dictionary
            dict_data = None
            try:
                dict_data = ast.literal_eval(line)
                if isinstance(dict_data, dict):
                    is_dict = True
                else:
                    dict_data = None
                    is_dict = False
            except (ValueError, SyntaxError):
                is_dict = False

            if is_dict and dict_data:
                # Collect unique dictionary keys
                for key in dict_data.keys():
                    if isinstance(key, str):
                        # Check if key is a sentence (longer than 20 characters)
                        if len(key.strip()) > 20:
                            all_sentences.append(key.strip())
                            # Count words in this key sentence
                            key_words = re.split(word_pattern, key.strip())
                            total_words += len([w for w in key_words if w.strip()])
                        else:
                            unique_keys.add(key)

                # Evaluate each dictionary value for sentences and word counting
                for key, value in dict_data.items():
                    if isinstance(value, str):
                        # Count words in the entire value
                        value_words = re.split(word_pattern, value)
                        total_words += len([w for w in value_words if w.strip()])

                        # Split value into sentences
                        sentences = re.split(sentence_pattern, value)

                        # Filter out empty strings and collect sentences longer than 20 characters
                        valid_sentences = [s.strip() for s in sentences if len(re.split(word_pattern, s)) > 3]

                        for sentence in valid_sentences:
                            all_sentences.append(sentence)
            else:
                # Handle non-dictionary lines (regular text)
                # Count words in the entire line
                line_words = re.split(word_pattern, line)
                total_words += len([w for w in line_words if w.strip()])

                # Extract sentences
                sentences = re.split(sentence_pattern, line)
                valid_sentences = [s.strip() for s in sentences if len(re.split(word_pattern, s)) > 3]

                for sentence in valid_sentences:
                    all_sentences.append(sentence)

        # Count words in unique keys
        for key in unique_keys:
            key_words = re.split(word_pattern, key)
            total_words += len([w for w in key_words if w.strip()])

        return {
                    "total_words": total_words,
                    "total_sentences": len(all_sentences),
                    "identified_sentences": all_sentences,
                    "column_names": list(unique_keys),
                    "column_count": len(unique_keys)
                }

    async def _async_evaluate_chunk(self, prompt_builder: callable, chunk: dict, semaphore: asyncio.Semaphore | None):
        async def _run():
            prompt = prompt_builder(
                document_title=self.document_title if self.document_title is not None else self.filename,
                section_title=chunk["title"],
                section_text=chunk["content"],
            )
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                None,
                lambda: self.llm.get_json_text_to_text(
                    prompt,
                    fileid=self.dafileid,
                    requestid=f"Grading_Spellcheck_{chunk['title']}",
                )
            )

        if semaphore:
            async with semaphore:
                return await _run()
        return await _run()

    async def get_spellcheck_results(self, prompt_builder: callable, textContent: str, document_title: str) -> dict: 
        self.textContent = textContent
        self.document_title = document_title

        cnt = self._get_counts()
        total_words = cnt["total_words"]
        total_lines = cnt["total_sentences"]
        logger.info(f"{self.dafileid}: Total words: {total_words}, Total lines: {total_lines}")

        chunks = self._get_chunks()
        logger.info(f"{self.dafileid}: Number of chunks: {len(chunks)}")
        
        semaphore = asyncio.Semaphore(self.max_concurrency) if self.max_concurrency else None
        tasks = [self._async_evaluate_chunk(prompt_builder, ch, semaphore) for ch in chunks]
        chunk_results = await asyncio.gather(*tasks)
        logger.info(f"{self.dafileid}: Completed evaluating all chunks")
        return  self._aggregate(chunk_results, total_lines, total_words, len(chunks))
    
    def get_contentcheck_results(self, prompt_builder: callable, textContent: str, document_title: str, description:str = None, document_type: str = None, phase:str = None) -> dict:
        self.textContent = textContent
        document_title = document_title if document_title is not None else self.filename

        chunks = self._get_chunks()
        logger.info(f"{self.dafileid}: Number of chunks: {len(chunks)}")

        response_context = []

        for chunk in chunks:
            logger.info(f"{self.dafileid}: Processing {chunk['title']}")
            
            prompt = prompt_builder(
                chunk = chunk["content"],
                document_title = document_title,
                chunk_title = chunk["title"],
                description = description,
                response_context=response_context[-5:] if response_context else None,
                document_type=document_type,
                phase=phase
            )
            response = self.llm.get_json_text_to_text(prompt, fileid=self.dafileid, requestid=f"Grading_Contentcheck_{chunk['title']}")
            if not response:
                logger.warning(f"{self.dafileid}:{chunk['title']} failed, skipping...")
                continue
            response_context.append(response)

        if len(response_context) == 0:
            raise UnableToGradeTheDocument(fileid=self.dafileid)

        return response_context
    
    def consolidate_results(self, prompt_builder: callable, chunk_results: list, document_title: str, description:str = None, document_type: str = None, phase:str = None) -> dict:
        """Consolidate results from multiple chunks."""
        document_title = document_title if document_title is not None else self.filename

        consolidation_prompt = prompt_builder(response_context = chunk_results,
                                              document_title = document_title,
                                              fext = self.ext,
                                              description = description,
                                              document_type = document_type,
                                              phase = phase)
        consolidated_result = self.llm.get_json_text_to_text(consolidation_prompt, fileid=self.dafileid, requestid=f"Grading_Consolidation")
        if len(consolidated_result) == 0:
            raise UnableToGradeTheDocument(fileid=self.dafileid)
        return consolidated_result

    def grade_document(self, prompt_builder: callable, textContent: str, document_title: str, description:str = None, document_type: str = None, phase:str = None) -> dict:
        """Synchronous wrapper for convenience."""
        def assign_grade(score):
            if score >= 80:
                return 'Very High'
            elif score >= 70:
                return 'High'
            elif score >= 60:
                return 'Medium'
            elif score >= 50:
                return 'Low'
            else:
                return 'Very Low'
        # return asyncio.run(self.get_spellcheck_results(prompt_builder, textContent, document_title))
        response_context = self.get_contentcheck_results(prompt_builder = prompt_builder, 
                                                         textContent = textContent,
                                                         document_title = document_title,
                                                         description = description,
                                                         document_type = document_type,
                                                         phase = phase)
        logger.info(f"{self.dafileid}: Chunk level content check results: {len(response_context)} chunks")
        consolidated_result = self.consolidate_results(prompt_builder = build_consolidation_prompt, 
                                                       chunk_results = response_context, 
                                                       document_title = document_title, 
                                                       description = description, 
                                                       document_type = document_type, 
                                                       phase = phase)
        logger.info(f"{self.dafileid}: Completed consolidating the results")

        kpi_dict = {key:value['score'] for key,value in consolidated_result['kpis'].items()}
        df = pd.DataFrame([{'KpiName':key,'Score':value['score'],'Reason':value['reason']} for key, value in consolidated_result['kpis'].items()])
        kpi_dict['grade_score'] = float(df['Score'].mean())     
        kpi_dict['grade'] = assign_grade(kpi_dict['grade_score'])

        html_list = "<b>Grading Summary : </b><br><ul>"
        for reason in df['Reason']:
            escaped_reason = html.escape(str(reason))
            html_list += f"  <li>{escaped_reason}</li>"
        html_list += "</ul>"
        kpi_dict['summary']=html_list
        return kpi_dict

    def main(self, **kwargs): 
        document_title = kwargs.get('title')
        # if not document_title:
        #     raise InvalidMetadataError(["title"], "title of document is required")

        if not document_title:
            document_title = self.filename
            logger.warning(f"{self.dafileid}: No title provided using filename: {document_title}")
        description = kwargs.get('description')
        document_type = kwargs.get('ip_type')
        phase = kwargs.get('dtpm_phase')

        dispatcher = Dispatcher(filepath = self.filepath, dafileid = self.dafileid, analyze_images=self.cfg.IMAGE_ANALYZE_SWITCH, debug=self.debug)
        extractor = dispatcher.getExtractor()
        _ = extractor.extract_content()
        textcontent, _ = extractor.get_filecontent()
        logger.info(f"{self.dafileid}: Text content extracted: {len(textcontent)} characters")
        prompt_builder = dispatcher.getContentCheckerPromptBuilder()

        result = self.grade_document(prompt_builder = prompt_builder, 
                                  textContent = textcontent, 
                                  document_title = document_title, 
                                  description = description, 
                                  document_type = document_type, 
                                  phase = phase)        
        return result
