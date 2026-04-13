from __future__ import annotations
import ast
import re
import os
import pandas as pd
import uuid
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

from config import Configuration
from core.genai.open_ai_client import OpenAiHelper
from core.readers.excelreader import ExcelExtractor
from core.utility import split_text
from core.db.crud import DatabaseManager
from core.utility import get_custom_logger
from services.gtl_recommendation.grading.Spellchecker.prompt import build_xlsx_spellcheck_prompt


logger = get_custom_logger(__name__)

class ExcelSpellchecker:
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

        self.db = DatabaseManager()
        self.dafileid = dafileid

 
        self.llm = OpenAiHelper(correlationid=self.cfg.CORR_ID_GRADING)

    def _extract_text(self) -> str:
        extractor = ExcelExtractor(
            filepath=self.file_path, analyze_images=self.cfg.IMAGE_ANALYZE_SWITCH, debug=self.cfg.DEBUG, fileid=self.dafileid,
        )
        extractor.extract_content()
        content, _ = extractor.get_filecontent(get_ocr=False)
        if not content.strip():
            raise ValueError("Empty document")
        return content

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
            prompt = build_xlsx_spellcheck_prompt(
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
        cnt = self._get_counts()
        total_words = cnt["total_words"]
        total_lines = cnt["total_sentences"]

        chunks = self._get_chunks(self.textContent)

        semaphore = asyncio.Semaphore(self.max_concurrency) if self.max_concurrency else None
        tasks = [self._evaluate_chunk_async(ch, semaphore) for ch in chunks]
        chunk_results = await asyncio.gather(*tasks)
        return self._aggregate(chunk_results, total_lines, total_words, len(chunks)) 

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
                        valid_sentences = [s.strip() for s in sentences if len(s.strip()) > 20]

                        for sentence in valid_sentences:
                            all_sentences.append(sentence)
            else:
                # Handle non-dictionary lines (regular text)
                # Count words in the entire line
                line_words = re.split(word_pattern, line)
                total_words += len([w for w in line_words if w.strip()])

                # Extract sentences
                sentences = re.split(sentence_pattern, line)
                valid_sentences = [s.strip() for s in sentences if len(s.strip()) > 20]

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

    # def grade_document(self, output_dir: str = "./output") -> dict:
    #     """Synchronous wrapper for convenience."""
    #     return asyncio.run(self.spellcheck_in_chunks(output_dir))


# if __name__ == "__main__":
#     FILE_PATH = r"C:\Users\Vedish_Kabara\Downloads\validate xlsx\MOP-Ceph-Openshift-LLD-v1.0_Ahmed Nashaat.xlsx"
#     out = ExcelSpellchecker(FILE_PATH).grade_document()
#     print(json.dumps(out, indent=2))