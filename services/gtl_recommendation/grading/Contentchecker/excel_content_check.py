import json
import uuid
from pathlib import Path
from datetime import datetime
import re

from config import Configuration
from core.genai.open_ai_client import OpenAiHelper
from core.utility import split_text, get_custom_logger
from core.exceptions import EmptyFileError
from core.db.crud import DatabaseManager
from services.gtl_recommendation.grading.Contentchecker.prompt import build_excel_contentcheck_prompt

logger = get_custom_logger(__name__)


class ExcelContentChecker:
    def __init__(self, filepath:Path, dafileid:uuid = None, debug:bool = False):
        self.filename = Path(filepath).name
        self.cfg = Configuration()
        self.cfg.load_active_config()
        self.chunk_size = self.cfg.CHUNK_SIZE_SPELLCHECK
        self.chunk_overlap = self.cfg.OVER_LAP_SIZE_SPELLCHECK
        self.llm = OpenAiHelper(correlationid=self.cfg.CORR_ID_GRADING)
        self.db = DatabaseManager()


    def _clean_text(self, textContent: str) -> str:
        """Clean extracted Excel text for processing."""
        textContent = re.sub(r'\n{3,}', '\n\n', textContent)
        textContent = re.sub(r'[ \t]+', ' ', textContent)
        # removing formulas if present in cells
        textContent = re.sub(r'=+[A-Z0-9]+', '', textContent)  
        return textContent.strip()

    def _chunk_text(self, textContent: str):
        """Chunk Excel content for processing, maintaining sheet structure."""
        cleaned = self._clean_text(textContent)
        chunklst = split_text(cleaned, chunk_size=self.cfg.CHUNK_SIZE_SPELLCHECK, chunk_overlap=self.cfg.OVER_LAP_SIZE_SPELLCHECK)
        return [{"title": f"Excel Chunk {i+1}", "content": c.strip()} for i, c in enumerate(chunklst)]

    def evaluate(self, textContent: str, document_title: str) -> dict:
        """Evaluate Excel content using sequential processing."""

        self.textContent = textContent
        self.document_title = document_title
        chunks = self._chunk_text(self.textContent)

        previous_result = None

        for i, chunk in enumerate(chunks):
            logger.info(f"Processing Excel chunk {i+1}/{len(chunks)}")

            prompt = build_excel_contentcheck_prompt(
                chunk=chunk["content"],
                previous_result=json.dumps(previous_result) if previous_result else None,
                document_title=self.document_title,
                filename=self.filename
            )
            cur = self.llm.get_json_text_to_text(prompt)
            if not cur:
                logger.warning(f"Excel chunk {i+1} failed, skipping...")
                continue
            previous_result = cur

        final_result = previous_result or {}
        # doc_kpis = final_result.get("document_kpis", {})

        # rounded_kpis = {k: round(v, 2) if v is not None else None for k, v in doc_kpis.items()}

        # update_values = rounded_kpis
        # doc_summary = final_result.get("document_summary", {})
        # if doc_summary:
        #     update_values["summary"] = json.dumps(doc_summary)

        return final_result