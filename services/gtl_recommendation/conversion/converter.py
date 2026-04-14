import warnings, os, logging
from pathlib import Path
# from PIL import Image
# import pypandoc
# from pypdf import PdfReader, PdfWriter
import pytesseract
from pdf2docx import Converter
from pdf2image import convert_from_path
from docx import Document
import fitz
import uuid
# from core.doc_converter import DocConverter
from core.pdf_to_pptx import pdf_to_pptx_final
from core.s3_helper import StorageManager
from core.libreoffice_converter import LibreOfficeConverter
from config import Configuration
from core.utility import get_custom_logger #,remove_control_chars

logger = get_custom_logger(__name__)
# logger.propagate = False
warnings.filterwarnings('ignore')
logging.getLogger('pdf2docx').setLevel(logging.ERROR)
logging.getLogger('pdf2image').setLevel(logging.ERROR)

class FileConverter:
    def __init__(self, filepath: str, fileid: uuid = None, debug: bool = False):
        self.orig_filepath = Path(filepath)
        self.filepath = Path(filepath)
        self.fileid = fileid
        self.cfg=Configuration()
        self.cfg.load_active_config()
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
        # self.analyze_images = analyze_images

    def is_slides_pdf(self, min_large_font_pct=50, max_words_per_line=12):
        def is_slide_ratio(page):
            width, height = page.rect.width, page.rect.height
            ratio = round(width / height, 2)
            return ratio in (1.33, 1.78)  # 4:3 or 16:9
        doc = fitz.open(self.filepath)

        large_font_pages = 0
        short_lines_pages = 0
        slide_ratio_pages = 0

        for page in doc:
            words = page.get_text("words")
            font_sizes = []

            line_word_counts = {}

            for w in words:
                font_sizes.append(w[3])  # font size
                y = round(w[1])          # y position for line grouping
                line_word_counts.setdefault(y, 0)
                line_word_counts[y] += 1

            if not font_sizes:
                continue

            avg_font = sum(font_sizes) / len(font_sizes)
            large_fonts = [f for f in font_sizes if f > avg_font * 1.1]

            if len(large_fonts) / len(font_sizes) * 100 >= min_large_font_pct:
                large_font_pages += 1

            # Short-line structure = slide-like bullet formatting
            short_lines = [count for count in line_word_counts.values() if count <= max_words_per_line]
            if len(short_lines) / len(line_word_counts) >= 0.7:
                short_lines_pages += 1

            if is_slide_ratio(page):
                slide_ratio_pages += 1

        wght = [0.20,0.30,0.50]
        percent_large_fonts = large_font_pages / len(doc)
        percent_short_lines = short_lines_pages / len(doc)
        percent_slide_ratio = slide_ratio_pages / len(doc)

        # print(percent_large_fonts, percent_short_lines, percent_slide_ratio)

        slide_score = (wght[0] * percent_large_fonts) + (wght[1] * percent_short_lines) + (wght[2] * percent_slide_ratio)
        doc.close()
        return slide_score*100 >= 60, slide_score*100
    
    def pdf_to_docx(self,calledforSow = False):
        try:
            if not self._has_searchable_text():
                logger.info(f"{self.fileid}-PDF {self.filepath} contains no searchable text. Trying Ocr")
                images = convert_from_path(self.filepath,dpi=300)
                logger.info(f"{self.fileid}-Extracted all the images from pdf")
                if self.debug:
                    # Create a directory to store the images
                    image_dir = Path(os.path.join(self.filepath.parent,self.filepath.stem, "images"))
                    image_dir.mkdir(parents=True, exist_ok=True)

                # Initialize an empty string to store the extracted text
                extracted_text = ""

                # Iterate over the images and perform OCR
                for i, image in enumerate(images):
                    # Save the image to a file
                    if self.debug:
                        image.save(os.path.join(image_dir, f"image_{i+1}.jpg"), "JPEG")

                    # Perform OCR using Tesseract
                    text = pytesseract.image_to_string(image)
                    # Append the extracted text to the main string
                    extracted_text += text + "\n\n"
                # Remove control characters from extracted_text
                
                # extracted_text = remove_control_chars(extracted_text)
                # extracted_text = re.sub(r'[\x00-\x1f\x80-\x9f]', '', extracted_text)
                logger.info(f"{self.fileid}-Text extraction completed: {len(extracted_text)} characters extracted from images")

                if len(extracted_text) != 0:
                    # Create a new .docx file and add the extracted text
                    docx_file = self.filepath.with_suffix('.docx')
                    document = Document()
                    document.add_paragraph(extracted_text)
                    document.save(docx_file)

                    # Log the extracted text
                    logger.info(f"{self.fileid}-Doc file save at {docx_file}")
                else:
                    logger.info("{self.fileid}-No text extracted from images")
            else:
                if calledforSow:
                    raise Exception('Unable to convert pdf to docx')
                self._convert_pdf_to_docx()
                logger.info(f"{self.fileid}-✅Pdf converted to docx: {self.filepath.with_suffix('.docx')}")
        except Exception as e:
            logger.warning(f"{self.fileid}-Failed to convert pdf to docx. {e}",exc_info=True)
            raise e
        
    def _has_searchable_text(self) -> bool:
        """
        Simple heuristic: return True if at least one page yields non‑empty text.
        """
        doc = fitz.open(self.filepath)
        for page in doc:
            if page.get_text().strip():
                doc.close()
                return True
        doc.close()
        return False
    
    def _convert_pdf_to_docx(self, filepath: Path=None) -> Path:
        if filepath is None:
            filepath = self.filepath

        # """Convert PDF to DOCX"""
        try:
            logger.info(f"{self.fileid}-Converting {filepath} to DOCX")
            cv = Converter(filepath)
            cv.convert(
                self.filepath.with_suffix('.docx'),
                # start=0,
                # end=None
                # multi_processing=True
            )
        except Exception as e:
            logger.warning(f"{self.fileid}-Error converting PDF to DOCX: {e}",exc_info=True)
            raise Exception(f"{self.fileid}-Error converting PDF to DOCX: {e}")
        finally:
            cv.close()

    def _convert_pdf_to_pptx_libreoffice(self):
        """Convert PDF to PPTX using LibreOffice (2-step: PDF->PPT->PPTX)."""
        lc = LibreOfficeConverter(self.filepath, fileid=self.fileid)
        lc.convert_pdf_to_pptx()

    def _convert_pdf_to_docx_libreoffice(self):
        """Convert PDF to DOCX using LibreOffice (2-step: PDF->DOC->DOCX)."""
        lc = LibreOfficeConverter(self.filepath, fileid=self.fileid)
        lc.convert_pdf_to_docx()

    def convert(self):
        s3 = StorageManager()
        try:
            if not self.filepath.exists():
                _ = s3.download(s3._make_s3_key(self.filepath,self.cfg.DATA_DIR))

            isSlidePdf, score = self.is_slides_pdf()
            logger.info(f"{self.fileid}-isSlidePdf: {isSlidePdf}, Score: {score}")
            if isSlidePdf:
                converted_filepath = self.filepath.with_suffix('.pptx')
                try:
                    self._convert_pdf_to_pptx_libreoffice()
                except Exception as e:
                    logger.error(f"{self.fileid}-Failed to convert PDF to PPTX using LibreOffice. Error: {e}", exc_info=True)
                    pdf_to_pptx_final(pdf_path=self.filepath, pptx_path=self.filepath.with_suffix('.pptx'), mode="screen")
                
                s3.upload(converted_filepath,overwrite=True)
            else:
                converted_filepath = self.filepath.with_suffix('.docx')
                try:
                    self._convert_pdf_to_docx_libreoffice()
                except Exception as e:
                    logger.error(f"{self.fileid}-Failed to convert PDF to DOCX using LibreOffice. Error: {e}", exc_info=True)
                    self.pdf_to_docx()
    
                s3.upload(converted_filepath,overwrite=True)
            return converted_filepath
        except Exception as e:
            logger.error(f"{self.fileid}-Failed to convert pdf to docx. Fallback to PdfExtractor. Error: {e}", exc_info=True)
            raise e