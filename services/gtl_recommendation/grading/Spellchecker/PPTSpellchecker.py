from __future__ import annotations
 
import json
import re
import uuid
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
 
from config import Configuration
from core.genai.open_ai_client import OpenAiHelper, _SELECTED_TEXT_TO_TEXT_MODEL
from core.readers.pptxreaders import PptxExtractor
from core.utility import split_text
from core.db.crud import DatabaseManager
from core.utility import get_custom_logger
from services.gtl_recommendation.grading.Spellchecker.PPTprompt import build_pptx_spellcheck_prompt

logger = get_custom_logger(__name__)

# Slides with fewer words than this are image/divider slides — skip LLM call
_MIN_WORDS_PER_SLIDE = 5
 
# Regex patterns that indicate a slide boundary in extracted PPTX text
_SLIDE_BOUNDARY_PATTERNS = [
    re.compile(r"^slide\s*\d+", re.IGNORECASE),   # "Slide 1", "Slide 12"
    re.compile(r"^-{3,}$"),                         # "---" separator
]
 
 
class PptxSpellchecker:
    def __init__(self, file_path: str, max_concurrency: int = 3):
        self.file_path = Path(file_path)
        if self.file_path.suffix.lower() != ".pptx":
            raise ValueError("Only PPTX files are supported")
 
        self.document_title = self.file_path.stem
        self.max_concurrency = max_concurrency
 
        self.cfg = Configuration()
        self.cfg.load_active_config()
        self.chunk_size = self.cfg.CHUNK_SIZE_SPELLCHECK
        self.chunk_overlap = self.cfg.OVER_LAP_SIZE_SPELLCHECK

        self.db=DatabaseManager()
        self.requestid=uuid.uuid4()
        self.fuuid=uuid.uuid4()
        self.dafileid=uuid.uuid4()
        self.daoriginalfileid=uuid.uuid4()
 
        self.llm = OpenAiHelper(correlationid=self.cfg.CORR_ID_GRADING)
 
    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------
 
    def _extract_text(self) -> str:
        extractor = PptxExtractor(
            filepath=self.file_path,
            analyze_images=self.cfg.IMAGE_ANALYZE_SWITCH,
            debug=self.cfg.DEBUG,
            fileid=self.dafileid,
        )
        extractor.extract_content()
        content, _ = extractor.get_filecontent(get_ocr=False)
        if not content.strip():
            raise ValueError("Empty PPTX document — no text content found")
        
        return content
 
    # ------------------------------------------------------------------
    # Slide-aware chunking
    # ------------------------------------------------------------------
 
    @staticmethod
    def _is_slide_boundary(line: str) -> bool:
        """Return True if the line looks like a slide separator in extracted PPTX text."""
        stripped = line.strip()
        return any(p.match(stripped) for p in _SLIDE_BOUNDARY_PATTERNS)
 
    def _split_into_slides(self, document_text: str) -> List[str]:
        """
        Split extracted text into individual slide blocks.
 
        If the extractor embeds slide markers (e.g. 'Slide 1', '---'), we split on
        those. If no markers are detected we fall back to splitting on double newlines,
        treating each paragraph block as a logical slide unit.
        """
        lines = document_text.splitlines()
        slides: List[str] = []
        current: List[str] = []
 
        has_markers = any(self._is_slide_boundary(ln) for ln in lines)
 
        if has_markers:
            for line in lines:
                if self._is_slide_boundary(line):
                    if current:
                        slides.append("\n".join(current).strip())
                    current = []
                else:
                    current.append(line)
            if current:
                slides.append("\n".join(current).strip())
        else:
            # No slide markers — split on double blank lines (paragraph blocks)
            blocks = re.split(r"\n{2,}", document_text)
            slides = [b.strip() for b in blocks if b.strip()]
 
        return [s for s in slides if s]
 
    def _group_slides_into_chunks(self, slides: List[str]) -> Tuple[List[dict], int]:
        """
        Group slides into chunks that respect chunk_size while never cutting mid-slide.
        Returns (chunks, skipped_slide_count).
 
        Near-empty slides (image-only / divider slides) are counted but excluded from
        LLM processing to avoid noise and wasted API calls.
        """
        chunks: List[dict] = []
        skipped = 0
        current_lines: List[str] = []
        current_word_count = 0
        current_line_count = 0
        slide_start = 1
        slide_index = 0
 
        def _flush(end_idx: int):
            if current_lines:
                text = "\n".join(current_lines).strip()
                chunks.append({
                    "title": f"Slides {slide_start}-{end_idx}",
                    "content": text,
                    "word_count": current_word_count,
                    "line_count": current_line_count,
                })
 
        for i, slide_text in enumerate(slides, start=1):
            slide_index = i
            word_count = len(re.findall(r"\b[a-zA-Z]+\b", slide_text))
 
            # Skip near-empty slides (image-only, title-only dividers, etc.)
            if word_count < _MIN_WORDS_PER_SLIDE:
                skipped += 1
                continue
 
            slide_lines = [ln for ln in slide_text.split("\n") if ln.strip()]
            slide_line_count = len(slide_lines)
 
            # If adding this slide would exceed chunk_size, flush current chunk first
            if current_word_count + word_count > self.chunk_size and current_lines:
                _flush(i - 1)
                current_lines = []
                current_word_count = 0
                current_line_count = 0
                slide_start = i
 
            current_lines.extend(slide_lines)
            current_word_count += word_count
            current_line_count += slide_line_count
 
        # Flush remaining slides
        _flush(slide_index)
 
        return chunks, skipped
 
    def _chunk_text(self, document_text: str) -> Tuple[List[dict], int, int]:
        """
        Returns (chunks, total_slides, skipped_slides).
        Falls back to generic split_text chunking if slide splitting yields nothing.
        """
        slides = self._split_into_slides(document_text)
        total_slides = len(slides)
 
        if slides:
            chunks, skipped = self._group_slides_into_chunks(slides)
            if chunks:
                return chunks, total_slides, skipped
 
        # Fallback: no slide structure detected — use generic chunking
        chunklst = split_text(
            document_text,
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap,
        )
        chunks = [
            {
                "title": f"Chunk {i + 1}",
                "content": chunk.strip(),
                "word_count": len(re.findall(r"\b[a-zA-Z]+\b", chunk)),
                "line_count": len([ln for ln in chunk.split("\n") if ln.strip()]),
            }
            for i, chunk in enumerate(chunklst)
        ]
        return chunks, total_slides, 0
 
    # ------------------------------------------------------------------
    # LLM evaluation
    # ------------------------------------------------------------------
 
    async def _evaluate_chunk_async(self, chunk: dict, semaphore: asyncio.Semaphore | None):
        async def _run():
            prompt = build_pptx_spellcheck_prompt(
                document_title=self.document_title,
                section_title=chunk["title"],
                section_text=chunk["content"],
            )
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(
                None,
                self.llm.get_json_text_to_text,
                prompt,
                _SELECTED_TEXT_TO_TEXT_MODEL,
            )
            spelling = resp.get("spelling", [])
            grammar = resp.get("grammar", [])
            has_errors = bool(spelling or grammar)
            return {
                "spelling": spelling if isinstance(spelling, list) else [],
                "grammar": grammar if isinstance(grammar, list) else [],
                "has_errors": has_errors,
            }
 
        if semaphore:
            async with semaphore:
                return await _run()
        return await _run()
 
    # ------------------------------------------------------------------
    # Aggregation (PPTX-specific metrics)
    # ------------------------------------------------------------------
 
    @staticmethod
    def _count_words(text: str) -> int:
        """Count only alphabetic words — excludes numbers, codes, symbols."""
        return len(re.findall(r"\b[a-zA-Z]+\b", text))
 
    def _aggregate(self, chunk_results: List[dict], total_words: int) -> dict:
        all_spelling: List[str] = []
        all_grammar: List[str] = []
 
        for r in chunk_results:
            all_spelling.extend(r.get("spelling", []))
            all_grammar.extend(r.get("grammar", []))
 
        # Deduplicate — presentations often repeat phrases across slides
        spelling_total = len(set(all_spelling))
        grammar_total = len(set(all_grammar))
 
        # Spelling accuracy — % of words that are correctly spelled
        if total_words == 0:
            spelling_accuracy = 100.0
        else:
            spelling_accuracy = round(max(0.0, 100 - (spelling_total / total_words * 100)), 2)
 
        # Grammar accuracy — % of words not affected by grammar mistakes
        if total_words == 0:
            grammar_accuracy = 100.0
        else:
            grammar_accuracy = round(max(0.0, 100 - (grammar_total / total_words * 100)), 2)
 
        return {
            "spelling_accuracy": spelling_accuracy,
            "grammar_accuracy": grammar_accuracy,
        }
 
    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------
 
    async def evaluate(self, output_dir: str = "./output") -> dict:
        # Insert placeholder grading row at start
        try:
            self.db.insert_grading(
                requestid=self.requestid,
                fuuid=self.fuuid,
                dafileid=self.dafileid,
                daoriginalfileid=self.daoriginalfileid,
                # spelling_accuracy=None,
                # grammar_accuracy=None,
            )
            logger.info(f"Inserted placeholder grading row: {self.fuuid}")
        except Exception as e:
            logger.error(f"Failed to insert grading row: {self.fuuid}", exc_info=True)
            raise e
 
        document_text = self._extract_text()
        total_words = self._count_words(document_text)
 
        chunks, total_slides, skipped_slides = self._chunk_text(document_text)
 
        semaphore = asyncio.Semaphore(self.max_concurrency) if self.max_concurrency else None
        tasks = [self._evaluate_chunk_async(ch, semaphore) for ch in chunks]
        chunk_results = await asyncio.gather(*tasks)
 
        results = self._aggregate(chunk_results, total_words)
 
        # Update grading row with actual accuracies
        try:
            updated = self.db.update_grading(
                where_clause={
                    "requestid": self.requestid,
                    "fuuid": self.fuuid,
                    "dafileid": self.dafileid,
                    "daoriginalfileid": self.daoriginalfileid,
                },
                update_values={
                    "grammar_accuracy": results.get("grammar_accuracy"),
                    "spelling_accuracy": results.get("spelling_accuracy"),
                },
            )
            logger.info(f"Updated grading row ({updated} rows): {self.fuuid}")
        except Exception as e:
            logger.error(f"Failed to update grading row: {self.fuuid}", exc_info=True)
            raise e
 
        payload = {"results": results}
 
        if self.cfg.DEBUG:
            output_path = Path(output_dir)
            output_path.mkdir(exist_ok=True, parents=True)
            outfile = output_path / f"{self.document_title}_spelling_grammar_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(outfile, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            payload["output_json_path"] = str(outfile)
 
        return payload