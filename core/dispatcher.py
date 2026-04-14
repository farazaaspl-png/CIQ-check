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
from core.libreoffice_converter import LibreOfficeConverter
from core.pdf_to_pptx import pdf_to_pptx_final
from services.gtl_recommendation.redaction.text.RedactorFlatFile import FlatFileRedactor
from services.gtl_recommendation.redaction.text.RedactorDoc import DocRedactor
from services.gtl_recommendation.redaction.text.RedactorPpt import PowerPointRedactor
from services.gtl_recommendation.redaction.text.RedactorExcel import ExcelRedactor
from services.gtl_recommendation.redaction.image.RedactorDoc import DocRedactor as ImageDocRedactor
from services.gtl_recommendation.redaction.image.Redactorxlsx import ExcelRedactor as ImageExcelRedactor
from services.gtl_recommendation.redaction.image.RedactorPptx import PPTXRedactor as ImagePptxRedactor
# from services.gtl_recommendation.grading.spell_check_prompts import build_docx_spellcheck_prompt, build_xlsx_spellcheck_prompt, build_pptx_spellcheck_prompt
from services.gtl_recommendation.grading import spell_check_prompts as scp
from services.gtl_recommendation.grading import content_check_prompts as ccp

# from services.gtl_recommendation.grading.Spellchecker.DocSpellchecker import DocSpellchecker
# from services.gtl_recommendation.grading.Contentchecker.doc_content_check import DocContentChecker
# from services.gtl_recommendation.grading.Spellchecker.ExcelSpellchecker import ExcelSpellchecker
# from services.gtl_recommendation.grading.Contentchecker.excel_content_check import ExcelContentChecker
# from services.gtl_recommendation.grading.Spellchecker.PPTSpellchecker import PptxSpellchecker
# from services.gtl_recommendation.grading.Contentchecker.ppt_content_check import PptxContentChecker
         
from config import Configuration
from core.s3_helper import StorageManager
from core.utility import get_custom_logger #,remove_control_chars


logger = get_custom_logger(__name__)
# logger.propagate = False
warnings.filterwarnings('ignore')
logging.getLogger('pdf2docx').setLevel(logging.ERROR)
logging.getLogger('pdf2image').setLevel(logging.ERROR)
# DOC_FILES = ['.docx', '.doc', '.docm']

DOC_FILES = ['.docx','.doc','.docm','.dot','.rtf']
PDF_FILES = ['.pdf']
PPT_FILES = ['.pptx', '.potx','.ppt','.pot','.pps']
FLAT_FILES = ['.csv','.txt','.psv','.json', '.htm', '.tm7', '.html','.log']
# PPT_FILES = ['.pptx','.ppt']
EXCEL_FILES = ['.xlsx', '.xls', '.xlt', '.csv']#, '.xlsm', '.xlsb']
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
            if self.filepath.suffix.lower() in ('.docm','.doc'):
                converter = DocConverter(self.filepath)
                converter.convert_file()
                self.filepath = self.filepath.with_suffix('.docx')

            if self.filepath.suffix.lower() in DOC_FILES:
                return DocumentExtractor(filepath = self.filepath, fileid = self.dafileid, debug = self.debug, analyze_images = self.analyze_images)
            
            elif self.filepath.suffix.lower() in PDF_FILES:
                try:
                    isSlidePdf, score = self.is_slides_pdf()
                    logger.info(f"isSlidePdf: {isSlidePdf}, Score: {score}")
                    if isSlidePdf:
                        # converter = PDFToPPTXConverter(debug = self.debug)
                        # converter.convert(self.filepath,self.filepath.with_suffix('.pptx'))
                        pdf_to_pptx_final(pdf_path=self.filepath, pptx_path=self.filepath.with_suffix('.pptx'), mode="screen")
                        self.filepath = self.filepath.with_suffix('.pptx')
                        return PptxExtractor(filepath = self.filepath, waspdf = True, fileid = self.dafileid, debug = self.debug, analyze_images = self.analyze_images)
                    else:
                        self.pdf_to_docx(calledforSow = True)
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
            #Convert non-readable file formats to latest file formats.
            if self.filepath.suffix.lower() in ('.doc','.docm','.dot','.rtf'):
                if not self.filepath.with_suffix('.docx').exists():
                    try:
                        self._convert_doc_to_docx_libreoffice()
                    except Exception as e:
                        logger.warning(f"Failed to convert doc to docx using LibreOffice. Error: {e}", exc_info=True)
                        converter = DocConverter(self.filepath,self.debug)
                        converter.convert_file()
                self.filepath = self.filepath.with_suffix('.docx')

            elif self.filepath.suffix.lower() in ('.potx','.ppt','.pot','.pps'):
                if not self.filepath.with_suffix('.pptx').exists():
                    try:
                        self._convert_ppt_to_pptx_libreoffice()
                        self.filepath = self.filepath.with_suffix('.pptx')
                    except Exception as e:
                        logger.warning(f"Failed to convert ppt to pptx using LibreOffice. Error: {e}", exc_info=True)

            elif self.filepath.suffix.lower() in ('.xls', '.xlt', '.csv'):
                if not self.filepath.with_suffix('.xlsx').exists():
                    try:
                        self._convert_xls_to_xlsx_libreoffice()
                        self.filepath = self.filepath.with_suffix('.xlsx')
                    except Exception as e:
                        logger.warning(f"Failed to convert xls to xlsx using LibreOffice. Error: {e}", exc_info=True)

            #return the correct extractor object
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
                            try:
                                #try converting pdf using libre office
                                self._convert_pdf_to_pptx_libreoffice()
                            except Exception as e:
                                logger.warning(f"Failed to convert pdf to pptx using LibreOffice. Error: {e}", exc_info=True)
                                #fallback to old method
                                pdf_to_pptx_final(pdf_path=self.filepath, pptx_path=self.filepath.with_suffix('.pptx'), mode="screen")
                        self.filepath = self.filepath.with_suffix('.pptx')
                        return PptxExtractor(filepath = self.filepath, waspdf = True, fileid = self.dafileid, debug = self.debug, analyze_images = self.analyze_images)
                    else:
                        if not self.filepath.with_suffix('.docx').exists():
                            try:
                                #try converting pdf using libre office
                                self._convert_pdf_to_docx_libreoffice()
                            except Exception as e:
                                logger.warning(f"Failed to convert pdf to docx using LibreOffice. Error: {e}", exc_info=True)
                                #fallback to old method
                                self.pdf_to_docx()
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
        lc = LibreOfficeConverter(self.filepath, fileid=self.dafileid)
        lc.convert_doc_to_docx(upload=True)

    def _convert_ppt_to_pptx_libreoffice(self):
        """Convert PPT to PPTX using LibreOffice (single step)."""
        lc = LibreOfficeConverter(self.filepath, fileid=self.dafileid)
        lc.convert_ppt_to_pptx(upload=True)

    def _convert_xls_to_xlsx_libreoffice(self):
        """Convert XLS to XLSX using LibreOffice (2-step: XLS->ODS->XLSX)."""
        lc = LibreOfficeConverter(self.filepath, fileid=self.dafileid)
        lc.convert_xls_to_xlsx(upload=True)

    def _convert_pdf_to_pptx_libreoffice(self):
        """Convert PDF to PPTX using LibreOffice (2-step: PDF->PPT->PPTX)."""
        lc = LibreOfficeConverter(self.filepath, fileid=self.dafileid)
        lc.convert_pdf_to_pptx()

    def _convert_pdf_to_docx_libreoffice(self):
        """Convert PDF to DOCX using LibreOffice (2-step: PDF->DOC->DOCX)."""
        lc = LibreOfficeConverter(self.filepath, fileid=self.dafileid)
        lc.convert_pdf_to_docx()

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
    
    def getSpellCheckerPromptBuilder(self):
        """Get spellchecker prompt builder based on file format"""
        try:
            file_extension = self.filepath.suffix.lower()
            
            # ---------- DOCX ----------
            if file_extension == '.docx':
                return scp.build_docx_spellcheck_prompt
            # ---------- EXCEL ----------
            elif file_extension == '.xlsx':
                return scp.build_xlsx_spellcheck_prompt
            # ---------- PPTX ----------
            elif file_extension == '.pptx':
                return scp.build_pptx_spellcheck_prompt
            # ---------- Not supported ----------
            else:
                raise FileFormatNotSupported(fileformat=self.filepath.suffix)
        except CustomBaseException as e:
            raise e
        except Exception as e:
            logger.error(f"Error in getSpellcheckerPrompt: {e}",exc_info=True)
            raise UnExpectedError(error = e)
        
    def getContentCheckerPromptBuilder(self):
        """Get contentchecker prompt builder based on file format"""
        try:
            file_extension = self.filepath.suffix.lower()
            
            # ---------- DOCX ----------
            if file_extension == '.docx':
                return ccp.build_doc_contentcheck_prompt
            # ---------- EXCEL ----------
            elif file_extension == '.xlsx':
                return ccp.build_xls_contentcheck_prompt
            # ---------- PPTX ----------
            elif file_extension == '.pptx':
                return ccp.build_ppt_contentcheck_prompt
            # ---------- Not supported ----------
            else:
                raise FileFormatNotSupported(fileformat=self.filepath.suffix)
        except CustomBaseException as e:
            raise e
        except Exception as e:
            logger.error(f"Error in getSpellcheckerPrompt: {e}",exc_info=True)
            raise UnExpectedError(error = e)
    
    # def getGraders(self):
    #     """Get appropriate graders based on file format"""
    #     try:
    #         file_extension = self.filepath.suffix.lower()
            
    #         # ---------- DOCX ----------
    #         if file_extension == '.docx':
    #             spellchecker = DocSpellchecker(filepath = self.filepath, dafileid=self.dafileid, debug = self.debug)
    #             contentchecker = DocContentChecker(filepath = self.filepath, dafileid=self.dafileid, debug = self.debug)
    #             return spellchecker, contentchecker
    #         # ---------- EXCEL ----------
    #         # for now just filepath is passed will be changed in future
    #         elif file_extension == '.xlsx':
    #             spellchecker = ExcelSpellchecker(filepath = self.filepath, dafileid=self.dafileid, debug = self.debug)
    #             contentchecker = ExcelContentChecker(filepath = self.filepath, dafileid=self.dafileid, debug = self.debug)
    #             return spellchecker, contentchecker
    #         # ---------- PPTX ----------
    #         # for now just filepath is passed will be changed in future
    #         elif file_extension == '.pptx':
    #             spellchecker = PptxSpellchecker(filepath = self.filepath, dafileid=self.dafileid, debug = self.debug)
    #             contentchecker = PptxContentChecker(filepath = self.filepath, dafileid=self.dafileid, debug = self.debug)
    #             return spellchecker, contentchecker
    #         # ---------- Not supported ----------
    #         else:
    #             logger.warning(f"Unsupported file format for grading: {file_extension}")
    #              # ---------- FALLBACK ----------
    #             raise FileFormatNotSupported(fileformat=self.filepath.suffix)
    #     except CustomBaseException as e:
    #         raise e
    #     except Exception as e:
    #         logger.error(f"Error in getGraders: {e}",exc_info=True)
    #         raise UnExpectedError(error = e)
        
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