import sys
from pathlib import Path
import json
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from core.genai.open_ai_client import OpenAiHelper, _SELECTED_TEXT_TO_TEXT_MODEL
from core.readers.docreader import DocumentExtractor
from core.utility import split_text
from core.exceptions import EmptyFileError

# Ensure project root on path
ROOT_DIR = Path(__file__).parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


SYSTEM_PROMPT = """
You are a document quality validator focused on spelling and grammar.

TASK:
- Count clear, undeniable spelling errors.
- Count objectively incorrect grammar that breaks meaning.

RULES:
- Ignore technical terms, acronyms, and domain-specific vocabulary.
- Ignore content inside < > (treated as redacted).
- Be conservative: if uncertain, do not count as an error.

OUTPUT JSON ONLY:
{
  "misspelled": <number of misspelled words>,
  "incorrectline": <number of lines with grammatical errors>
}
""".strip()


def extract_text(file_path: str) -> str:
    extractor = DocumentExtractor(
        filepath=Path(file_path),
        analyze_images=False,
        debug=False,
        fileid=uuid.uuid4()
    )
    extractor.extract_content(
        headerfooter=False,
        notes=False,
        indexes=False,
        comments=False
    )
    content = extractor.get_filecontent(get_ocr=False)
    extractor.clean_up()
    if not content.strip():
        raise EmptyFileError()
    return content


def chunk_text(document_text: str, chunk_size: int = 2000, chunk_overlap: int = 200):
    chunks_raw = split_text(document_text, chunk_size, chunk_overlap)
    return [
        {
            "title": f"Chunk {i+1}",
            "content": chunk.strip(),
            "word_count": len(chunk.split()),
            "line_count": len([ln for ln in chunk.split("\n") if ln.strip()])
        }
        for i, chunk in enumerate(chunks_raw)
    ]


def evaluate_chunk(llm: OpenAiHelper, chunk: dict, document_title: str) -> dict:
    prompt = f"""{SYSTEM_PROMPT}

DOCUMENT TITLE: {document_title}
SECTION: {chunk['title']}

SECTION CONTENT:
<<<
{chunk['content']}
>>>
"""
    return llm.get_json_text_to_text(prompt, p_model=_SELECTED_TEXT_TO_TEXT_MODEL)


def aggregate_results(chunk_results, total_lines: int, total_words: int, chunk_count: int):
    misspelled = sum(r.get("misspelled", 0) for r in chunk_results)
    incorrect_lines = sum(r.get("incorrectline", 0) for r in chunk_results)

    line_accuracy = ((total_lines - incorrect_lines) / total_lines * 100) if total_lines else 0.0
    spelling_accuracy = ((total_words - misspelled) / total_words * 100) if total_words else 0.0
    combined_accuracy = round((line_accuracy * 0.6) + (spelling_accuracy * 0.4), 2)

    return {
        "misspelled_words": misspelled,
        "incorrect_lines": incorrect_lines,
        "total_lines": total_lines,
        "total_words": total_words,
        "line_accuracy": round(line_accuracy, 2),
        "spelling_accuracy": round(spelling_accuracy, 2),
        "combined_accuracy": combined_accuracy,
        "chunk_count": chunk_count,
    }


def validate_document(file_path: str, document_title: str = None, output_dir: str = "./output"):
    document_title = document_title or Path(file_path).stem
    llm = OpenAiHelper()

    document_text = extract_text(file_path)
    total_lines = len([ln for ln in document_text.split("\n") if ln.strip()])
    total_words = len(document_text.split())

    chunks = chunk_text(document_text)
    chunk_results = []

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(evaluate_chunk, llm, ch, document_title): ch for ch in chunks}
        for future in as_completed(futures):
            result = future.result()
            chunk_results.append(result)

    results = aggregate_results(chunk_results, total_lines, total_words, len(chunks))

    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)
    outfile = output_path / f"{document_title}_spelling_grammar_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload = {
        "metadata": {
            "file_path": file_path,
            "document_title": document_title,
            "evaluation_date": datetime.now().isoformat(),
            "evaluator_version": "1.0",
            "evaluation_type": "spelling_grammar",
            "chunking_method": "RecursiveCharacterTextSplitter (core.utility)"
        },
        "results": results
    }
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"Validation complete. Results saved to: {outfile}")
    return payload


if __name__ == "__main__":
    # Example usage: update file_path and output_dir as needed
    FILE_PATH = r"C:\Users\Maithili_Joshi\ip_content_management\ip_content_management\input\rBIA_Preparation_Document_20250707_IPUpload_Anthony Lally.docx"
    OUTPUT_DIR = r"C:\Users\Maithili_Joshi\ip_content_management\output"
    validate_document(FILE_PATH, output_dir=OUTPUT_DIR)