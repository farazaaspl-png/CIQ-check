import logging
from pathlib import Path
from typing import Dict, List, Optional
from rapidfuzz import fuzz
from core.dispatcher import Dispatcher
from core.genai.open_ai_client import OpenAiHelper
from core.utility import split_text, get_custom_logger
from core.exceptions import EmptyFileError
from config import Configuration
from services.gtl_recommendation.similarity.prompt import build_chunk_prompt, build_consolidation_prompt
from core.dellattachments import DellAttachments
from core.s3_helper import StorageManager
import asyncio
logger = get_custom_logger(__name__)

class ContentSimilarity:

    def __init__(self,textContent: str=None, debug: bool = False,**kwargs):
        self.textContent = textContent
        self.cfg = Configuration()
        self.cfg.load_active_config()
        self.llm = OpenAiHelper()
        self.s3 = StorageManager()
        self.filename    = kwargs.get("name", "")
        self.dafileid    = kwargs.get("daFileId")
        self.template_filename = kwargs.get("filename", "")
        self.template_dafileid = kwargs.get("templatedaFileId")
        self.finalOutput = {}
        self.chunk_size    = self.cfg.VECTOR_CHUNK_SIZE
        self.chunk_overlap = self.cfg.VECTOR_OVER_LAP_SIZE
        self.fuzzy_threshold = self.cfg.FUZZ_RATIO_SIMILARITY_THRESHOLD
        if debug:
            logger.setLevel(logging.DEBUG)

    # # Extract + chunk                                                     
    def _extract_template_text(self, filepath: Path, dafileid: str = None) -> List[str]:
        
        dispatcher = Dispatcher(filepath,dafileid=dafileid,analyze_images=self.cfg.IMAGE_ANALYZE_SWITCH,debug=self.cfg.DEBUG)
        extractor = dispatcher.getExtractor()
        extractor.extract_content()
        file_content, _ = extractor.get_filecontent(True)

        if not file_content or file_content.strip() == "":
            raise EmptyFileError()

        # logger.info(f"Extracted {len(file_content):,} chars from {filepath.name}")

        return file_content
    
    def _chunk_file(self,file_content :str):
        chunks = split_text(file_content,chunk_size= self.chunk_size,chunk_overlap = self.chunk_overlap)
        logger.info(f"Split into {len(chunks)} chunks")
        return chunks
    
    @staticmethod
    def _calculate_average_fuzz(fuzz_scores: List[float]) -> float:        
        scores = [s for s in fuzz_scores if s is not None]
        if not scores:
            return 0.0
        avg = sum(scores) / len(scores)
        logger.info(f"Average fuzz score: {avg:.2f} (from {len(scores)} chunks)")
        return round(avg, 2)

    def _chunk_to_chunk_similarity(self,input_chunks: List[str],template_chunks: List[str]) -> Dict:
        
        total = len(input_chunks)
        fuzz_scores  = []
        desc_lst     = []
        final_result = []
        called_llm   = 0
        llm_skipped  = 0

        for idx, (input_chunk, template_chunk) in enumerate(zip(input_chunks, template_chunks)):

            #Calculate fuzz Score
            fuzz_score = fuzz.ratio(input_chunk, template_chunk)
            # logger.info(f"input_chunk-{input_chunk}")
            # logger.info(f"input_chunk-{input_chunks}")
            # logger.info(f"template_chunk-{template_chunks}")
            # logger.debug(f"Chunk {idx + 1}/{total} : fuzz_score={fuzz_score:.2f}")
            # logger.debug(f"input chunk {input_chunk}")
            # logger.debug(f"template chunk {template_chunk}")

            fuzz_scores.append(fuzz_score)

            if fuzz_score >= self.fuzzy_threshold:
                logger.debug(f"Chunk {idx} fuzz PASSED ({fuzz_score:.2f}) — skipping LLM.")
                llm_skipped += 1
                continue

            
            logger.info(f"Chunk {idx + 1}/{total}  fuzz={fuzz_score:.2f} < {self.fuzzy_threshold} — calling LLM …")

            prompt = build_chunk_prompt(input_chunk=input_chunk,template_chunk=template_chunk)

            responce = self.llm.get_json_text_to_text(prompt)

            if not responce:
                logger.warning(f"Chunk {idx}: LLM returned empty — storing fuzz score only.")
                continue
            desc_lst.append(responce.get("description", ""))
            called_llm += 1

            logger.debug(f"Chunk {idx}  fuzz={fuzz_score:.2f} ")
        
        if not desc_lst:                                                     
            logger.warning("No descriptions to consolidate — all chunks were similar.")
            final_result.append(fuzz_scores)
            final_result.append("The input document closely matches the template with no significant differences detected.")
            return final_result
        
        prompt = build_consolidation_prompt(desc_lst=desc_lst)

        logger.info(f"Sending {len(desc_lst)} descriptions to consolidation LLM ")
        final_responce = self.llm.get_json_text_to_text(prompt)

        final_result.append(fuzz_scores)
        final_result.append(final_responce.get("description", ""))
        logger.info(f"Chunk comparison done: total={total} | "f"llm_called={called_llm} | "f"llm_skipped={llm_skipped}")
        return final_result

    def similarity_and_description(self,filepath: Path) -> Dict:

        # template_path = (Path(self.cfg.DATA_DIR) / self.cfg.GTL_FLOW_DIR / str(self.template_dafileid) / self.template_filename)
        fdict = {'id':self.template_dafileid,
                 'name':filepath.name,
                 'filepath':filepath
                 }
        if filepath.exists():
            pass
       
        elif self.s3.exists(filepath):
            filepath = self.s3.download(self.s3._make_s3_key(filepath,self.cfg.DATA_DIR))
            logger.info(f"{self.template_dafileid}-Downloaded file from S3 Storage: {filepath}")
        else:
            logger.info(f"{self.template_dafileid}-Downloading file from dell attachments")
            da = DellAttachments(self.cfg.DEBUG)
            fdict = asyncio.run(da.download(filedict=fdict))
            logger.info(f"{self.template_dafileid}-Downloaded file from dell attachments: {filepath}")
 
            self.s3.upload(fdict['filepath'])
            logger.info(f"{self.template_dafileid}- File uploaded to dell attachments")

        # logger.info(f"Input    :{input_path}")
        # logger.info(f"Template :{template_path}")

        # input_chunks    = self._extract_text_and_chunk(input_path,    dafileid=dafileid)
        # template_chunks = self._extract_text_and_chunk(template_path, dafileid=template_dafileid)
        
        template_textContent = self._extract_template_text(filepath,self.template_dafileid)
        input_chunks = self._chunk_file(self.textContent)
        template_chunks = self._chunk_file(template_textContent)
        
        final_result = self._chunk_to_chunk_similarity(input_chunks=input_chunks,template_chunks=template_chunks)
        
        # calculate Average fuzz scores 
        avg_fuzz     = self._calculate_average_fuzz(final_result[0])

        self.finalOutput = {"filename":self.filename,"dafileid":self.dafileid,"similarity_score":  avg_fuzz,
            "description": final_result[1]}

        return self.finalOutput