import fitz
from pathlib import Path
from pdf2docx import Converter
class ScannedPDFError(RuntimeError):
    """Raised when the PDF contains only images (no searchable text)."""
    pass

def _has_searchable_text(pdf_file: str) -> bool:
    """
    Simple heuristic: return True if at least one page yields non‑empty text.
    """
    doc = fitz.open(pdf_file)
    for page in doc:
        if page.get_text().strip():
            doc.close()
            return True
    doc.close()
    return False

def _convert_pdf_to_docx(pdf_path: str, docx_path: str) -> None:
    """
    Convert *pdf_path* → *docx_path* using pdf2docx.
    """
    cv = Converter(pdf_path)
    cv.convert(docx_path, start=0, end=None)   
    cv.close()
    print(f"✅ Conversion complete: {docx_path}")

def pdf_to_docx(pdf_file: str, docx_file: str) -> str | None:
   
    try:
        pdf_path = Path(pdf_file)
        docx_path = Path(docx_file)

        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {pdf_file}")

        if not _has_searchable_text(str(pdf_path)):
            raise ScannedPDFError(
                "The PDF appears to contain only images (scanned document). "
                "OCR is required before conversion."
            )

        _convert_pdf_to_docx(str(pdf_path), str(docx_path))
        return str(docx_path)

    except ScannedPDFError as se:
        print(f"❌ {se}")

    except Exception as e:
        print(f"❌ Error: {e}")

    return None

def pdf_to_docx_from_dispatcher(dispatcher, docx_file: str) -> str | None:
   
    pdf_path = str(dispatcher.filepath) 
    return pdf_to_docx(pdf_path, docx_file)



   