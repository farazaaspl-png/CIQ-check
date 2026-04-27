import os, warnings
import uuid
import tempfile
import pandas as pd
import pytesseract
from pathlib import Path
from typing import Dict, List
from bs4 import BeautifulSoup
from markitdown import MarkItDown
from pdf2image import convert_from_path

from core.utility import get_custom_logger, chunk_document

logger = get_custom_logger(__name__)
warnings.filterwarnings("ignore", category=RuntimeWarning, module="pydub")


DOC_FILES   = {".docx"}
PPT_FILES   = {".pptx"}
EXCEL_FILES = {".xlsx", ".xls"}
PDF_FILES   = {".pdf"}
HTML_FILES  = {".html", ".htm"}

SUPPORTED_FORMATS = DOC_FILES | PPT_FILES | EXCEL_FILES | PDF_FILES | HTML_FILES

class DocumentProcessor:
    """
    Reads supported document formats, converts them to Markdown,
    and produces context-aware chunks ready for LLM prompts.

    Usage:
        processor = DocumentProcessor(dafileid=uuid.uuid4())
        chunks    = processor.process_document(file_path)
    """

    def __init__(self, dafileid: uuid.UUID = None):
        self.dafileid   = dafileid
        self._md_engine = MarkItDown()

    # ── Public API ────────────────────────────────────────────────────────────

    def read_document(self, file_path: str) -> str:
        """
        Convert a file to a Markdown string.
            :param file_path: Path to the source document.
            :returns:         Markdown string.
            :raises ValueError: Unsupported file format.
        """
        path = Path(file_path)
        ext  = path.suffix.lower()

        if ext not in SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format '{ext}'. "
            )

        _readers = {
            ".docx": self._read_docx,
            ".pptx": self._read_pptx,
            ".xlsx": self._read_xlsx,
            ".xls":  self._read_xlsx,
            ".pdf":  self._read_pdf,
            ".html": self._read_html,
            ".htm":  self._read_html,
        }

        logger.info(f"{self.dafileid}: reading {path.name}")
        return _readers[ext](path)

    def process_document(self, file_path: str) -> List[Dict]:
        """
        Convert a file to Markdown and return context-aware chunks.

            :param file_path: Path to the source document.
            :returns: List of chunk dicts (title, content, metadata).
        """
        path = Path(file_path)
        try:
            markdown = self.read_document(file_path)
            chunks   = chunk_document(content=markdown, source_name=path.name)
            logger.info(f"{self.dafileid}: {path.name} → {len(chunks)} chunks") # can remove 
            return chunks
        except Exception as e:
            logger.error(f"{self.dafileid}: failed to process {path.name}: {e}") # we can remove 
            return []

    def process_directory(self, input_dir: str, output_dir: str) -> List[Dict]:
        """
        For each file:
          - Converts to Markdown and saves a .md file in output_folder.
          - Produces context-aware chunks.

        :param input_folder:  Directory containing source documents.
        :param output_folder: Directory where .md files are saved.
        :returns:             Aggregated list of all chunks.
        """
        input_path  = Path(input_dir)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        all_chunks: List[Dict] = []

        for ext in SUPPORTED_FORMATS:
            for file_path in input_path.glob(f"*{ext}"):
                try:
                    markdown = self.read_document(str(file_path))

                    md_out = output_path / f"{file_path.stem}.md"
                    md_out.write_text(markdown, encoding="utf-8")
                    logger.info(f"{self.dafileid}: saved {md_out.name}")

                    chunks = chunk_document(content=markdown, source_name=file_path.name)
                    all_chunks.extend(chunks)
                    logger.info(f"{self.dafileid}: {file_path.name} → {len(chunks)} chunks")

                except Exception as e:
                    logger.error(f"{self.dafileid}: failed to process {file_path.name}: {e}")
                    continue

        logger.info(
            f"{self.dafileid}: directory processed — "
            f"{len(all_chunks)} total chunks from {input_path}"
        )
        return all_chunks


    def _read_docx(self, path: Path) -> str:
        return self._md_engine.convert(str(path)).text_content

    def _read_pptx(self, path: Path) -> str:
        return self._md_engine.convert(str(path)).text_content

    def _read_xlsx(self, path: Path) -> str:
        """
        Using pandasbased reader instead of MarkItDown because MarkItDown
        flattens multi-sheet workbooks and loses table structure.
        Single-cell rows are treated as section headings (###).
        Multi-cell rows are collected and rendered as Markdown tables.
        """
        output  = []
        xl_file = pd.ExcelFile(path)

        for sheet_name in xl_file.sheet_names:
            df = pd.read_excel(xl_file, sheet_name=sheet_name, header=None)

            if df.empty:
                continue

            df = df.dropna(how="all").dropna(axis=1, how="all")
            output.append(f"# Sheet: {sheet_name}\n")

            table_buffer = []

            for _, row in df.iterrows():
                row_values = [str(v).strip() if pd.notna(v) else "" for v in row]
                non_empty  = sum(1 for v in row_values if v)

                if non_empty == 1:
                    if table_buffer:
                        output.append(_rows_to_md_table(table_buffer))
                        table_buffer = []
                    output.append(f"### {' '.join(row_values).strip()}\n")
                else:
                    table_buffer.append(row_values)

            if table_buffer:
                output.append(_rows_to_md_table(table_buffer))

        return "\n".join(output)

    def _read_pdf(self, path: Path) -> str:
        """
        Stage 1 — MarkItDown: fast, sufficient for text-based PDFs.
        Stage 2 — pytesseract OCR: fallback for scanned/image-based PDFs.
        """
        try:
            content = self._md_engine.convert(str(path)).text_content.strip()
            if len(content) >= 200:
                return content
            logger.info(f"{self.dafileid}: MarkItDown output too short for {path.name}, switching to OCR")
        except Exception as e:
            logger.warning(f"{self.dafileid}: MarkItDown failed for {path.name}: {e}")

        images = convert_from_path(path, dpi=300)
        logger.info(f"{self.dafileid}: extracted {len(images)} pages from {path.name}")

        extracted_text = ""
        for i, image in enumerate(images):
            text = pytesseract.image_to_string(image)
            extracted_text += text + "\n\n"

        logger.info(f"{self.dafileid}: {len(extracted_text)} characters extracted from {path.name}")

        if not extracted_text.strip():
            logger.info(f"{self.dafileid}: no text extracted from {path.name}")
            return ""

        return extracted_text

    def _read_html(self, path: Path) -> str:
        """
        BeautifulSoup strips navigation, scripts, and boilerplate before
        MarkItDown processes the clean body content.
        MarkItDown expects a file path, so cleaned HTML is written to a
        temporary file and deleted after conversion.
        """
        raw  = path.read_text(encoding="utf-8", errors="replace")
        soup = BeautifulSoup(raw, "html.parser")

        for tag in soup(["script", "style", "nav", "header",
                         "footer", "aside", "form", "noscript", "iframe"]):
            tag.decompose()

        content_node = (
            soup.find("main")
            or soup.find("article")
            or soup.find("body")
            or soup
        )

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".html", delete=False, encoding="utf-8"
        ) as tmp:
            tmp.write(str(content_node))
            tmp_path = tmp.name

        try:
            return self._md_engine.convert(tmp_path).text_content
        finally:
            os.unlink(tmp_path)

def _rows_to_md_table(rows: List[List[str]]) -> str:
    """Convert a list-of-lists to a Markdown table string via pandas."""
    if not rows:
        return ""

    if len(rows) > 1:
        headers = [h if h and h != "nan" else f"Col {i}" for i, h in enumerate(rows[0])]
        data    = [row[:len(headers)] for row in rows[1:]]
        df      = pd.DataFrame(data, columns=headers)
    else:
        df = pd.DataFrame(rows)

    df = df.replace("nan", "").replace("None", "")
    return df.to_markdown(index=False) + "\n\n"




if __name__ == "__main__":
    INPUT_DIR  = r"C:\Users\Maithili_Joshi\ip_content_management\ip_content_management\input"
    OUTPUT_DIR = r"C:\Users\Maithili_Joshi\ip_content_management\output"

    if os.path.exists(INPUT_DIR):
        processor = DocumentProcessor(dafileid=uuid.uuid4())
        processor.process_directory(INPUT_DIR, OUTPUT_DIR)
    else:
        print(f"Input folder not found: {INPUT_DIR}")