import os
import uuid
import asyncio
from pathlib import Path

import pandas as pd
import re
from typing import Dict, List
from config import Configuration
from core.genai.open_ai_client import OpenAiHelper
from core.utility import split_text, get_custom_logger
from services.gtl_recommendation.grading.Spellchecker.prompt import build_docx_spellcheck_prompt

logger = get_custom_logger(__name__)


class DocSpellchecker:
    def __init__(self, filepath:Path, dafileid:uuid =None , debug:bool = False):
        # self.file_path = Path(file_path)
        # self.textContent = textContent
        self.filename = Path(filepath).stem
        # self.document_title = self.filename
        self.max_concurrency = os.cpu_count()

        self.cfg = Configuration()
        self.cfg.load_active_config()
        self.chunk_size = self.cfg.CHUNK_SIZE_SPELLCHECK
        self.chunk_overlap = self.cfg.OVER_LAP_SIZE_SPELLCHECK

        self.llm = OpenAiHelper(correlationid=self.cfg.CORR_ID_GRADING) 

        self.dafileid = dafileid

    def _get_chunks(self):
        chunklst = split_text(
            self.textContent,
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap
        )
        return [
            {"title": f"Chunk {i+1}",
             "content": chunk.strip()}
            for i, chunk in enumerate(chunklst)
        ]

    async def _evaluate_chunk_async(self, chunk: dict, semaphore: asyncio.Semaphore | None):
        async def _run():
            prompt = build_docx_spellcheck_prompt(
                document_title=self.document_title if self.document_title is not None else self.filename,
                section_title=chunk["title"],
                section_text=chunk["content"],
            )
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                None,
                lambda: self.llm.get_json_text_to_text(
                    prompt,
                    requestid="Grading_Spellcheck",
                    fileid=self.dafileid
                )
            )

        if semaphore:
            async with semaphore:
                return await _run()
        return await _run()

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

    async def evaluate(self, textContent: str, document_title: str) -> dict: 
        self.textContent = textContent
        self.document_title = document_title
        # document_text = self._extract_text()
        total_lines = len([ln for ln in self.textContent.split("\n") if ln.strip()])
        total_words = len(re.findall(r"\b[a-zA-Z]+\b", self.textContent))

        chunks = self._get_chunks()

        semaphore = asyncio.Semaphore(self.max_concurrency) if self.max_concurrency else None
        tasks = [self._evaluate_chunk_async(ch, semaphore) for ch in chunks]
        chunk_results = await asyncio.gather(*tasks)

        return  self._aggregate(chunk_results, total_lines, total_words, len(chunks))