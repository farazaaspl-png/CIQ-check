#dispatcher 
import warnings, os, logging, subprocess
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

from core.exceptions import FileFormatNotSupported, UnExpectedError,CustomBaseException
from core.readers.pdfreader import PdfExtractor
from core.readers.docreader import DocumentExtractor
from core.readers.pptxreaders import PptxExtractor
from core.readers.flatfilereaders import FlatFileReader
from core.readers.excelreader import ExcelExtractor

from core.doc_converter import DocConverter
# from core.pdf_to_pptx import pdf_to_pptx_final
from services.gtl_recommendation.redaction.text.RedactorFlatFile import FlatFileRedactor
from services.gtl_recommendation.redaction.text.RedactorDoc import DocRedactor
from services.gtl_recommendation.redaction.text.RedactorPpt import PowerPointRedactor
from services.gtl_recommendation.redaction.text.RedactorExcel import ExcelRedactor
from services.gtl_recommendation.redaction.image.RedactorDoc import DocRedactor as ImageDocRedactor
from services.gtl_recommendation.redaction.image.Redactorxlsx import ExcelRedactor as ImageExcelRedactor
from services.gtl_recommendation.redaction.image.RedactorPptx import PPTXRedactor as ImagePptxRedactor
from config import Configuration
from core.s3_helper import StorageManager
from core.utility import get_custom_logger #,remove_control_chars


logger = get_custom_logger(__name__)
# logger.propagate = False
warnings.filterwarnings('ignore')
logging.getLogger('pdf2docx').setLevel(logging.ERROR)
logging.getLogger('pdf2image').setLevel(logging.ERROR)
# DOC_FILES = ['.docx', '.doc', '.docm']
DOC_FILES = ['.docx','.doc']
PDF_FILES = ['.pdf']
PPT_FILES = ['.pptx', '.potx','.ppt']
FLAT_FILES = ['.csv','.txt','.psv','.json', '.htm', '.tm7', '.html','.log']
# PPT_FILES = ['.pptx','.ppt']
EXCEL_FILES = ['.xlsx', '.xls']#, '.xlsm', '.xlsb']
# EXCEL_FILES = []

SUPPORTED_FILES = DOC_FILES + PDF_FILES + PPT_FILES + EXCEL_FILES + FLAT_FILES
class Dispatcher:
    
    def __init__(self, filepath: str, dafileid: uuid = None, analyze_images: bool = False, debug: bool = False):
        self.orig_filepath = Path(filepath)
        self.filepath = Path(filepath)
        self.dafileid = dafileid

        self.cfg=Configuration()
        self.cfg.load_active_config()
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.analyze_images = analyze_images
    
    def getSOWExtractor(self):
        """Read content from different file types"""
        try:
            # Convert legacy formats to modern equivalents before routing
            if self.filepath.suffix.lower() == '.docm':
                converter = DocConverter(self.filepath, self.debug)
                converter.convert_file()
                self.filepath = self.filepath.with_suffix('.docx')
            elif self.filepath.suffix.lower() == '.doc':
                if not self.filepath.with_suffix('.docx').exists():
                    self._convert_doc_to_docx_libreoffice()
                self.filepath = self.filepath.with_suffix('.docx')
            

            if self.filepath.suffix.lower() in DOC_FILES:
                return DocumentExtractor(filepath = self.filepath, fileid = self.dafileid, debug = self.debug, analyze_images = self.analyze_images)
            
            # elif self.filepath.suffix.lower() in PPT_FILES:
            #     return PptxExtractor(filepath = self.filepath, fileid = self.dafileid, debug = self.debug, analyze_images = self.analyze_images)
            
            elif self.filepath.suffix.lower() in PDF_FILES:
                try:
                    isSlidePdf, score = self.is_slides_pdf()
                    logger.info(f"isSlidePdf: {isSlidePdf}, Score: {score}")
                    if isSlidePdf:
                        # converter = PDFToPPTXConverter(debug = self.debug)
                        # converter.convert(self.filepath,self.filepath.with_suffix('.pptx'))
                        # pdf_to_pptx_final(pdf_path=self.filepath, pptx_path=self.filepath.with_suffix('.pptx'), mode="screen")

                        self._convert_pdf_to_pptx_libreoffice()
                        self.filepath = self.filepath.with_suffix('.pptx')
                        return PptxExtractor(filepath = self.filepath, waspdf = True, fileid = self.dafileid, debug = self.debug, analyze_images = self.analyze_images)
                    else:
                        # self.pdf_to_docx(calledforSow = True)

                        
                        self._convert_pdf_to_docx_libreoffice()
                        return DocumentExtractor(filepath = self.filepath.with_suffix('.docx'), waspdf = True, fileid = self.dafileid, debug = self.debug, analyze_images = self.analyze_images)
                    # self._covert_pdf_to_docx()
                    # logger.info(f"✅Pdf converted to docx: {self.filepath.with_suffix('.docx')}")
                except Exception as e:
                    logger.warning(f"Failed to convert pdf to docx. Fallback to PdfExtractor. Error: {e}", exc_info=True)
                    return PdfExtractor(filepath = self.filepath, fileid = self.dafileid, debug = self.debug, analyze_images = self.analyze_images)    
            elif self.filepath.suffix.lower() in EXCEL_FILES:
                return ExcelExtractor(filepath = self.filepath, fileid = self.dafileid, debug = self.debug, analyze_images = self.analyze_images)

            else:
                raise FileFormatNotSupported(fileformat=self.filepath.suffix)
        except CustomBaseException as e:
            raise e
        except Exception as e:
            raise UnExpectedError(error = e)
        
    def getExtractor(self):
        """Read content from different file types"""
        try:
            # Convert legacy formats to modern equivalents before routing
            if self.filepath.suffix.lower() == '.docm':
                converter = DocConverter(self.filepath, self.debug)
                converter.convert_file()
                self.filepath = self.filepath.with_suffix('.docx')
            elif self.filepath.suffix.lower() == '.doc':
                if not self.filepath.with_suffix('.docx').exists():
                    self._convert_doc_to_docx_libreoffice()
                self.filepath = self.filepath.with_suffix('.docx')
            elif self.filepath.suffix.lower() == '.ppt':
                if not self.filepath.with_suffix('.pptx').exists():
                    self._convert_ppt_to_pptx_libreoffice()
                self.filepath = self.filepath.with_suffix('.pptx')
            elif self.filepath.suffix.lower() == '.xls':
                if not self.filepath.with_suffix('.xlsx').exists():
                    self._convert_xls_to_xlsx_libreoffice()
                self.filepath = self.filepath.with_suffix('.xlsx')

            if self.filepath.suffix.lower() in DOC_FILES:
                return DocumentExtractor(filepath = self.filepath, fileid = self.dafileid, debug = self.debug, analyze_images = self.analyze_images)
            
            elif self.filepath.suffix.lower() in PPT_FILES:
                return PptxExtractor(filepath = self.filepath, fileid = self.dafileid, debug = self.debug, analyze_images = self.analyze_images)
            
            elif self.filepath.suffix.lower() in PDF_FILES:
                try:
                    isSlidePdf, score = self.is_slides_pdf()
                    logger.info(f"isSlidePdf: {isSlidePdf}, Score: {score}")
                    if isSlidePdf:
                        if not self.filepath.with_suffix('.pptx').exists():
                            # converter = PDFToPPTXConverter(debug=self.debug)
                            # converter.convert(self.filepath,self.filepath.with_suffix('.pptx'))
                            # pdf_to_pptx_final(pdf_path=self.filepath, pptx_path=self.filepath.with_suffix('.pptx'), mode="screen")
                            self._convert_pdf_to_pptx_libreoffice()
                        self.filepath = self.filepath.with_suffix('.pptx')
                        return PptxExtractor(filepath = self.filepath, waspdf = True, fileid = self.dafileid, debug = self.debug, analyze_images = self.analyze_images)
                    else:
                        if not self.filepath.with_suffix('.docx').exists():
                            # self.pdf_to_docx()
                            self._convert_pdf_to_docx_libreoffice()
                        self.filepath = self.filepath.with_suffix('.docx')
                        return DocumentExtractor(filepath = self.filepath.with_suffix('.docx'), waspdf = True, fileid = self.dafileid, debug = self.debug, analyze_images = self.analyze_images)
                except Exception as e:
                    logger.warning(f"Failed to convert pdf to docx. Fallback to PdfExtractor. Error: {e}", exc_info=True)
                    return PdfExtractor(filepath = self.filepath, fileid = self.dafileid, debug = self.debug, analyze_images = self.analyze_images)
            elif self.filepath.suffix.lower() in EXCEL_FILES:
                return ExcelExtractor(filepath = self.filepath, fileid = self.dafileid, debug = self.debug, analyze_images = self.analyze_images)

            elif self.filepath.suffix.lower() in FLAT_FILES:
                return FlatFileReader(filepath = self.filepath, fileid = self.dafileid, debug = self.debug)
            else:
                raise FileFormatNotSupported(fileformat=self.filepath.suffix)
        except CustomBaseException as e:
            raise e
        except Exception as e:
            raise UnExpectedError(error = e)
        
        
    def getRedactors(self,outdir):
        # Convert legacy formats to modern equivalents before routing
        if self.filepath.suffix.lower() == '.doc':
            if not self.filepath.with_suffix('.docx').exists():
                self._convert_doc_to_docx_libreoffice()
            self.filepath = self.filepath.with_suffix('.docx')
        elif self.filepath.suffix.lower() == '.ppt':
            if not self.filepath.with_suffix('.pptx').exists():
                self._convert_ppt_to_pptx_libreoffice()
            self.filepath = self.filepath.with_suffix('.pptx')
        elif self.filepath.suffix.lower() == '.xls':
            if not self.filepath.with_suffix('.xlsx').exists():
                self._convert_xls_to_xlsx_libreoffice()
            self.filepath = self.filepath.with_suffix('.xlsx')

        self.outfilepath = Path(os.path.join(outdir,self.filepath.name))
        """Read content from different file types"""
        try:
            
            # ---------- DOCX / DOC ----------
            if self.filepath.suffix.lower() in DOC_FILES:
                return DocRedactor(self.filepath, self.dafileid, self.debug), ImageDocRedactor(self.outfilepath, self.dafileid, self.debug, analyze_images = self.analyze_images)
            
            # ---------- PPTX ----------
            elif self.filepath.suffix.lower() in PPT_FILES:
                return PowerPointRedactor( self.filepath, self.dafileid, self.debug), ImagePptxRedactor(self.outfilepath, self.dafileid, self.debug, analyze_images = self.analyze_images)
            elif self.filepath.suffix.lower() in FLAT_FILES:
                return FlatFileRedactor(self.filepath, self.dafileid, self.debug), None
            # ---------- PDF ----------
            elif self.filepath.suffix.lower() in PDF_FILES:
                self.filepath = self.filepath.with_suffix('.txt')
                return FlatFileRedactor(self.filepath, self.dafileid, self.debug), None
                # self.pdf_to_docx()
                # self.outfilepath = self.outfilepath.with_suffix('.docx')
                # Now work with the newly‑created DOCX
                # return DocRedactor(self.filepath.with_suffix('.docx'), self.dafileid), ImageDocRedactor(self.outfilepath, self.dafileid)
            # ---------- EXCEL ----------
            elif self.filepath.suffix.lower() in EXCEL_FILES:
                return ExcelRedactor(self.filepath, self.dafileid, self.debug), ImageExcelRedactor(self.outfilepath, self.dafileid, self.debug,analyze_images = self.analyze_images)
            else:
                # ---------- FALLBACK ----------
                raise FileFormatNotSupported(fileformat=self.filepath.suffix)
        except CustomBaseException as e:
            raise e
        except Exception as e:
            logger.error(f"Error in getRedactors: {e}",exc_info=True)
            raise UnExpectedError(error = e)

    def _convert_doc_to_docx_libreoffice(self):
        """Convert DOC to DOCX using LibreOffice (single step)."""
        libreoffice_path = getattr(self.cfg, 'LIBREOFFICE_PATH')
        parent_dir = str(self.filepath.parent)
        base_name = self.filepath.stem

        logger.info(f"{self.dafileid}-Converting {self.filepath.name} to DOCX via LibreOffice...")

        command = [
            libreoffice_path, "--headless",
            "--convert-to", "docx",
            "--outdir", parent_dir,
            str(self.filepath.resolve())
        ]

        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
            logger.info(f"{self.dafileid}-Successfully converted to {base_name}.docx")
            converted_path = self.filepath.with_suffix('.docx')
            s3 = StorageManager()
            s3.upload(str(converted_path), overwrite=True)
            logger.info(f"{self.dafileid}-Uploaded {converted_path.name} to S3")
        except subprocess.CalledProcessError as e:
            logger.error(f"{self.dafileid}-LibreOffice DOC to DOCX conversion failed: {e.stderr}", exc_info=True)
            raise Exception(f"{self.dafileid}-LibreOffice DOC to DOCX conversion failed: {e.stderr}")

    def _convert_ppt_to_pptx_libreoffice(self):
        """Convert PPT to PPTX using LibreOffice (single step)."""
        libreoffice_path = getattr(self.cfg, 'LIBREOFFICE_PATH')
        parent_dir = str(self.filepath.parent)
        base_name = self.filepath.stem

        logger.info(f"{self.dafileid}-Converting {self.filepath.name} to PPTX via LibreOffice...")

        command = [
            libreoffice_path, "--headless",
            "--convert-to", "pptx",
            "--outdir", parent_dir,
            str(self.filepath.resolve())
        ]

        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
            logger.info(f"{self.dafileid}-Successfully converted to {base_name}.pptx")
            converted_path = self.filepath.with_suffix('.pptx')
            s3 = StorageManager()
            s3.upload(str(converted_path), overwrite=True)
            logger.info(f"{self.dafileid}-Uploaded {converted_path.name} to S3")
        except subprocess.CalledProcessError as e:
            logger.error(f"{self.dafileid}-LibreOffice PPT to PPTX conversion failed: {e.stderr}", exc_info=True)
            raise Exception(f"{self.dafileid}-LibreOffice PPT to PPTX conversion failed: {e.stderr}")

    def _convert_xls_to_xlsx_libreoffice(self):
        """Convert XLS to XLSX using LibreOffice (2-step: XLS->ODS->XLSX)."""
        libreoffice_path = getattr(self.cfg, 'LIBREOFFICE_PATH')
        parent_dir = str(self.filepath.parent)
        base_name = self.filepath.stem
        ods_path = self.filepath.with_suffix('.ods')

        logger.info(f"{self.dafileid}-Converting {self.filepath.name} to ODS via LibreOffice (Step 1/2)...")

        command_step1 = [
            libreoffice_path, "--headless",
            "--convert-to", "ods",
            "--outdir", parent_dir,
            str(self.filepath.resolve())
        ]

        command_step2 = [
            libreoffice_path, "--headless",
            "--convert-to", "xlsx",
            "--outdir", parent_dir,
            str(ods_path.resolve())
        ]

        try:
            subprocess.run(command_step1, check=True, capture_output=True, text=True)
            logger.info(f"{self.dafileid}-Upgrading {base_name}.ods to XLSX (Step 2/2)...")
            subprocess.run(command_step2, check=True, capture_output=True, text=True)

            if ods_path.exists():
                os.remove(ods_path)

            logger.info(f"{self.dafileid}-Successfully converted to {base_name}.xlsx")
            converted_path = self.filepath.with_suffix('.xlsx')
            s3 = StorageManager()
            s3.upload(str(converted_path), overwrite=True)
            logger.info(f"{self.dafileid}-Uploaded {converted_path.name} to S3")
        except subprocess.CalledProcessError as e:
            logger.error(f"{self.dafileid}-LibreOffice XLS to XLSX conversion failed: {e.stderr}", exc_info=True)
            raise Exception(f"{self.dafileid}-LibreOffice XLS to XLSX conversion failed: {e.stderr}")

    def _convert_pdf_to_pptx_libreoffice(self):
        """Convert PDF to PPTX using LibreOffice (2-step: PDF->PPT->PPTX)."""
        libreoffice_path = getattr(self.cfg, 'LIBREOFFICE_PATH')
        parent_dir = str(self.filepath.parent)
        filename = self.filepath.name
        base_name = self.filepath.stem

        logger.info(f"{self.dafileid}-Converting {filename} to PPT via LibreOffice (Step 1/2)...")

        # STEP 1: PDF -> PPT (Preserves formatting)
        command_step1 = [
            libreoffice_path, "--headless",
            "--infilter=impress_pdf_import",
            "--convert-to", "ppt",
            "--outdir", parent_dir,
            str(self.filepath)
        ]

        # STEP 2: PPT -> PPTX (Validates format)
        command_step2 = [
            libreoffice_path, "--headless",
            "--convert-to", "pptx",
            "--outdir", parent_dir,
            str(self.filepath.with_suffix('.ppt'))
        ]

        try:
            subprocess.run(command_step1, check=True, capture_output=True, text=True)
            logger.info(f"{self.dafileid}-Upgrading {base_name}.ppt to PPTX (Step 2/2)...")
            subprocess.run(command_step2, check=True, capture_output=True, text=True)

            # Clean up the intermediate .ppt file
            ppt_intermediate = self.filepath.with_suffix('.ppt')
            if ppt_intermediate.exists():
                os.remove(ppt_intermediate)

            logger.info(f"{self.dafileid}-Successfully converted to {base_name}.pptx")
        except subprocess.CalledProcessError as e:
            logger.error(f"{self.dafileid}-LibreOffice PDF to PPTX conversion failed: {e.stderr}", exc_info=True)
            raise Exception(f"{self.dafileid}-LibreOffice PDF to PPTX conversion failed: {e.stderr}")

    def _convert_pdf_to_docx_libreoffice(self):
        """Convert PDF to DOCX using LibreOffice (2-step: PDF->DOC->DOCX)."""
        libreoffice_path = getattr(self.cfg, 'LIBREOFFICE_PATH')
        parent_dir = str(self.filepath.parent)
        filename = self.filepath.name
        base_name = self.filepath.stem

        logger.info(f"{self.dafileid}-Converting {filename} to DOC via LibreOffice (Step 1/2)...")

        # STEP 1: PDF -> DOC (Preserves formatting)
        command_step1 = [
            libreoffice_path, "--headless",
            "--infilter=writer_pdf_import",
            "--convert-to", "doc",
            "--outdir", parent_dir,
            str(self.filepath)
        ]

        # STEP 2: DOC -> DOCX (Validates format)
        command_step2 = [
            libreoffice_path, "--headless",
            "--convert-to", "docx",
            "--outdir", parent_dir,
            str(self.filepath.with_suffix('.doc'))
        ]

        try:
            subprocess.run(command_step1, check=True, capture_output=True, text=True)
            logger.info(f"{self.dafileid}-Upgrading {base_name}.doc to DOCX (Step 2/2)...")
            subprocess.run(command_step2, check=True, capture_output=True, text=True)

            # Clean up the intermediate .doc file
            doc_intermediate = self.filepath.with_suffix('.doc')
            if doc_intermediate.exists():
                os.remove(doc_intermediate)

            logger.info(f"{self.dafileid}-Successfully converted to {base_name}.docx")
        except subprocess.CalledProcessError as e:
            logger.error(f"{self.dafileid}-LibreOffice PDF to DOCX conversion failed: {e.stderr}", exc_info=True)
            raise Exception(f"{self.dafileid}-LibreOffice PDF to DOCX conversion failed: {e.stderr}")

    def _non_blank_pages(self, pdf_path: Path) -> list[int]:
        """
        Return a list of zero‑based page indices that contain at least one
        non‑whitespace character.  This helper is used to avoid converting
        completely empty PDF pages (which would otherwise become blank DOCX pages).
        """
        doc = fitz.open(str(pdf_path))
        good_pages = []
        for i, page in enumerate(doc):
            txt = page.get_text("text").strip()

            if txt:                     # keep page only if it has text
                good_pages.append(i)
        doc.close()
        return good_pages
        
    def _convert_pdf_to_docx(self, filepath: Path=None) -> Path:
        if filepath is None:
            filepath = self.filepath

        # """Convert PDF to DOCX"""
        # pages_to_keep = self._non_blank_pages(filepath)
        # if not pages_to_keep:
        #     logger.warning(
        #         f"All pages of {filepath} appear blank – "
        #         "converting whole PDF anyway."
        #     )
        #     pages_to_keep = None
        try:
            logger.info(f"Converting {filepath} to DOCX")
            cv = Converter(filepath)
            cv.convert(
                self.filepath.with_suffix('.docx'),
                start=0,
                end=None
                # multi_processing=True
            )
            # if pages_to_keep is not None:
            #     logger.debug(f"Got pages to convert: {pages_to_keep}")
            #     cv.convert(
            #         self.filepath.with_suffix('.docx'),
            #         start=0,
            #         end=None,
            #         pages=pages_to_keep,
            #         debug=False
            #     )
            # else:
            #     logger.warning(f"No pages to convert. Trying anyways.")
            #     cv.convert(
            #         self.filepath.with_suffix('.docx'),
            #         start=0,
            #         end=None,
            #         multi_processing=True
            #     )
            # return self.filepath.with_suffix('.docx')
        except Exception as e:
            logger.warning(f"Error converting PDF to DOCX: {e}",exc_info=True)
            raise Exception(f"Error converting PDF to DOCX: {e}")
        finally:
            if hasattr(cv, 'close') and callable(getattr(cv, 'close')):
                cv.close()
            

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

    def pdf_to_docx(self,calledforSow = False):
        try:
            if not self._has_searchable_text():
                logger.info(f"PDF {self.filepath} contains no searchable text. Trying Ocr")
                images = convert_from_path(self.filepath,dpi=300)
                logger.info(f"Extracted all the images from pdf")
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
                logger.info(f"Text extraction completed: {len(extracted_text)} characters extracted from images")

                if len(extracted_text) != 0:
                    # Create a new .docx file and add the extracted text
                    docx_file = self.filepath.with_suffix('.docx')
                    document = Document()
                    document.add_paragraph(extracted_text)
                    document.save(docx_file)

                    # Log the extracted text
                    logger.info(f"Doc file save at {docx_file}")
                else:
                    logger.info("No text extracted from images")
            else:
                if calledforSow:
                    raise Exception('Unable to convert pdf to docx')
                self._convert_pdf_to_docx()
                logger.info(f"✅Pdf converted to docx: {self.filepath.with_suffix('.docx')}")
        except Exception as e:
            logger.warning(f"Failed to convert pdf to docx. {e}",exc_info=True)
            raise e

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

    # def pdf_to_docx(self):
    #     try:

    #         if not self._has_searchable_text():
    #             logger.info(f"PDF {self.filepath} contains no searchable text. Trying Ocr")
    #             images = convert_from_path(self.filepath)
    #             logger.info(f"Extracted all the images from pdf")
    #             pdflist = []
    #             for i, image in enumerate(images):
    #                 pdf = pytesseract.image_to_pdf_or_hocr(image, extension='pdf')
    #                 pdflist.append(pdf)
    #             logger.info(f"Converted Images to Bytes pdf")

    #             writer = PdfWriter()
    #             # Append each file to the writer
    #             for pdf in pdflist:
    #                 writer.append(PdfReader(io.BytesIO(pdf)))
                
    #             outfilepath = self.filepath.with_stem(self.filepath.stem+"_ocr")
    #             # Write the merged PDF to a new file
    #             with open(outfilepath, "wb") as output_file:
    #                 writer.write(output_file)
    #             logger.info(f"Created merged pdf: {outfilepath}")

    #             self._convert_pdf_to_docx(outfilepath)
                
    #         else:
    #             self._convert_pdf_to_docx()
    #             logger.info(f"✅Pdf converted to docx: {self.filepath.with_suffix('.docx')}")

    #     except Exception as e:
    #         logger.error(f"❌ Error: {e}")