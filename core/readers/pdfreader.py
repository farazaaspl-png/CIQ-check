import re
import fitz
import os, shutil, pdfplumber, uuid, pytesseract
import time
import logging
from typing import List, Dict, Any, Tuple
from PIL import Image
from pathlib import Path

from core.imagehandlers.imagehelper import ImageProcessor
from core.utility import get_custom_logger
from core.exceptions import EmptyFileError
logger = get_custom_logger(__name__)
# logger.propagate = False


class NoTextFoundError(Exception):
    """Raised when a PDF contains no selectable text items."""
    pass

def _bbox_intersection_area(b1, b2) -> float:
    """
    Compute intersection area between two bboxes each as (x0, y0, x1, y1).
    Returns intersection area (0 if none).
    """
    x0 = max(min(b1[0], b1[2]), min(b2[0], b2[2]))
    y0 = max(min(b1[1], b1[3]), min(b2[1], b2[3]))
    x1 = min(max(b1[0], b1[2]), max(b2[0], b2[2]))
    y1 = min(max(b1[1], b1[3]), max(b2[1], b2[3]))
    width = x1 - x0
    height = y1 - y0
    if width <= 0 or height <= 0:
        return 0.0
    return width * height

def _bbox_area(b):
    w = abs(b[2] - b[0])
    h = abs(b[3] - b[1])
    return w * h

LM_REMOVE_JUNKS = lambda value:re.sub(r'[^\w\s\.,\)\(\-\?\}\{\\~!@#\$%^&\*\_></"\':;\]\[\|=\+`]', '', value, flags=re.UNICODE)

class PdfExtractor:
    
    def __init__(self, filepath: Path, debug: bool = False, analyze_images: bool = True, fileid: uuid = None):
        """
        constructor: filepath, debug=False

        Properties set after extraction:
         - extracted_items: List[dict] sequential items with 'type' ∈ {'text','table','image'}
         - fileContent: joined string of all items in order
        """
        self.filepath = Path(filepath)
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.analyze_images = analyze_images
        self.fileid = fileid
        
        # create media directory
        self.OUTPUT_DIR = Path(os.path.join(filepath.parent, filepath.stem.replace('-','').replace(' ', '_')))
        self.media_dir = Path(os.path.join(self.OUTPUT_DIR, 'media'))
        if self.media_dir.exists():
            shutil.rmtree(self.media_dir)
        self.media_dir.mkdir(parents=True)

        self.extracted_items: List[Dict[str, Any]] = []
        self.fileContent: str = ""
        # overlap threshold to decide if a text line belongs to a table/image.
        # If intersection area / text_line_area >= this threshold then we treat it as overlapping.
        self._overlap_skip_threshold = 0.25  # 25% overlap -> skip text line

        self.image_processor = ImageProcessor(media_dir = self.media_dir,fileid = self.fileid, debug = self.debug)
        self.ratelimit_exceeded = False
        self.detected_text_lines_total=0

    def _has_text(self):
        doc = fitz.open(self.filepath)
        page = doc[0]
        text = page.get_text()
        ret = False
        if text.strip() == "":
            ret = False
        else:
            ret = True
        doc.close()
        return ret

    def _get_table_data(self,page):
        tables = []
        try:
            for tb in page.find_tables():
                # pdfplumber Table has bbox attribute as (x0, top, x1, bottom)
                bbox = tb.bbox
                table_data = tb.extract()
                tables.append({"bbox": bbox, "data": table_data})
                logger.debug(f"{self.fileid}--> Found table bbox={bbox} rows={len(table_data)}")
            return tables
        except Exception as e:
            logger.warning(f"{self.fileid}-->find_tables failed:{e}",exc_info=True)

    def _get_image_data(self,page):
        images = []
        try:
            for img in page.images:
                # print(img)
                # pdfplumber image dict commonly has keys 'x0','x1','top','bottom' or 'y0','y1'
                x0 = img.get("x0", img.get("x", 0))
                x1 = img.get("x1", x0 + img.get("width", 0))
                top = img.get("top", img.get("y0", img.get("y", 0)))
                bottom = img.get("bottom", top + img.get("height", 0))
                bbox = (x0, top, x1, bottom)
                images.append({"bbox": bbox, "obj": img})
                logger.debug(f"{self.fileid}--> Found image bbox={bbox}")
            return images
        except Exception as e:
            logger.warning(f"{self.fileid}-->page.images failed:{e}",exc_info=True)

    def _get_lines(self, page):
        words = page.extract_words(use_text_flow=True)
        lines = []
        if words:
            words_sorted = sorted(words, key=lambda w: (round(w.get("top", 0), 1), w.get("x0", 0)))
            current_top = None
            current_line = None
            for w in words_sorted:
                wtext = w.get("text", "")
                top = float(w.get("top", 0))
                bottom = float(w.get("bottom", top))
                x0 = float(w.get("x0", 0))
                x1 = float(w.get("x1", 0))
                if current_top is None:
                    current_top = top
                    current_line = {"text": wtext, "x0": x0, "x1": x1, "top": top, "bottom": bottom}
                else:
                    # group by vertical proximity
                    if abs(top - current_top) <= 3.0:
                        current_line["text"] += (" " + wtext)
                        current_line["x1"] = max(current_line["x1"], x1)
                        current_line["bottom"] = max(current_line["bottom"], bottom)
                    else:
                        lines.append(current_line)
                        current_top = top
                        current_line = {"text": wtext, "x0": x0, "x1": x1, "top": top, "bottom": bottom}
            if current_line and current_line.get("text"):
                lines.append(current_line)
        return lines
    
    def _build_element_list(self, page_no, lines, tables, images):
        # Build element list: tables, images, and text lines (but skip text lines that belong to tables/images)
        elements = []

        # add table elements
        table_bboxes = []  # for quick overlap checks
        for tb in tables:
            bbox = tb["bbox"]
            elements.append({"type": "table", "bbox": bbox, "data": tb["data"], "page": page_no})
            table_bboxes.append(bbox)

        # add image elements
        image_bboxes = []
        for im in images:
            bbox = im["bbox"]
            elements.append({"type": "image", "bbox": bbox, "obj": im["obj"], "page": page_no})
            image_bboxes.append(bbox)

        # add text lines only if they don't significantly overlap a table or image
        for ln in lines:
            text_bbox = (ln["x0"], ln["top"], ln["x1"], ln["bottom"])
            text_area = _bbox_area(text_bbox) or 1.0
            skip = False
            # check overlap with tables
            for tbbox in table_bboxes:
                inter = _bbox_intersection_area(text_bbox, tbbox)
                if inter > 0 and (inter / text_area) >= self._overlap_skip_threshold:
                    # skip this text line because it largely overlaps a table
                    skip = True
                    # logger.info(f"  Skipping text line (overlaps table) page {page_no}: {ln['text'][:80]!r}")
                    break
            if skip:
                continue
            # check overlap with images
            for ibbox in image_bboxes:
                inter = _bbox_intersection_area(text_bbox, ibbox)
                if inter > 0 and (inter / text_area) >= self._overlap_skip_threshold:
                    skip = True
                    # logger.info(f"  Skipping text line (overlaps image) page {page_no}: {ln['text'][:80]!r}")
                    break
            if skip:
                continue
            # Accept the text line
            elements.append({"type": "text", "bbox": text_bbox, "text": ln["text"], "page": page_no})
            self.detected_text_lines_total += 1
        return elements
    
    def _save_image_from_page(self, page, bbox, page_no: int) -> str:
        """
        Save the cropped region defined by bbox from the page as an image file.
        Returns the relative file path under output/media/<pdf_basename>/
        """
        x0, y0, x1, y1 = bbox
        os.makedirs(self.media_dir, exist_ok=True)
        fn = f"page{page_no}_x{int(x0)}_y{int(y0)}_w{int(max(1, x1-x0))}_h{int(max(1, y1-y0))}.png"
        outpath = os.path.join(self.media_dir, fn)

        try:
            pil_img = page.to_image(resolution=150).original
            left = int(max(0, x0))
            top = int(max(0, y0))
            right = int(min(pil_img.width, x1))
            bottom = int(min(pil_img.height, y1))
            cropped = pil_img.crop((left, top, right, bottom))
            cropped.save(outpath)
            logger.debug(f"{self.fileid}-->Saved image  to {outpath}")
            return os.path.relpath(outpath)
        except Exception as e:
            logger.debug(f"{self.fileid}-->Image crop save failed, trying fallback:{e}")
            try:
                page_image = page.to_image(resolution=150)
                crop_bbox = (x0, y0, x1, y1)
                page_image.crop(crop_bbox).save(outpath, format="PNG")
                logger.debug(f"{self.fileid}-->(fallback) Saved image to {outpath}")
                return os.path.relpath(outpath)
            except Exception as ex:
                logger.debug(f"{self.fileid}-->Final image fallback: creating placeholder image:{ex}")
                placeholder_img = Image.new("RGB", (200, 100), color=(220, 220, 220))
                ph_path = os.path.join(self.media_dir, f"page{page_no}_image_placeholder.png")
                placeholder_img.save(ph_path)
                return os.path.relpath(ph_path)
            
    def _fill_extracted_items_in_order(self, page_no, page, elements_sorted):
        for el in elements_sorted:
            if el["type"] == "text":
                item = {"type": "text", "content": el["text"], "page": el["page"]}
                self.extracted_items.append(item)
            elif el["type"] == "table":
                # Build table string using ~ for columns and \n for rows, enclosed in markers
                table_rows = []
                for row in el["data"]:
                    safe_cells = [("" if c is None else str(c).strip()) for c in row]
                    row_text = " ~ ".join(safe_cells)
                    table_rows.append(row_text)
                formatted = "--- TABLE START ---\n" + ("\n".join(table_rows)) + "\n--- TABLE END ---\n"
                item = {"type": "table", "content": formatted, "raw_rows": el["data"], "page": el["page"]}
                self.extracted_items.append(item)
            elif el["type"] == "image":
                img_path = Path(self._save_image_from_page(page, el["bbox"], page_no))
                placeholder = f"[Image: {img_path.name}"
                ocr_text = self.image_processor.get_ocr_text(img_path).strip()

                if self.analyze_images and not self.ratelimit_exceeded and len(ocr_text) > 10:
                    description = self._describe_image(img_path)
                    placeholder += f"~|Description:{description}"
                if len(ocr_text)>0:
                    placeholder += f"~|Content:{ocr_text}"
                placeholder += "]"
                item = {"type": "image", "content": placeholder, "file": img_path, "page": el["page"]}
                self.extracted_items.append(item)

    def _describe_image(self,imgpath: Path):
        try:
            analysis_result = self.image_processor.analyze_image(Path(imgpath))
            category = analysis_result.get('category', 'UNKNOWN').upper()
            
            # Check if category should be excluded from detailed analysis
            excluded_categories = ['LOGO', 'SIGNATURE', 'STAMP']
            
            if category not in excluded_categories:
                return analysis_result.get('description', 'No description available')
        except Exception as e:
            logger.warning(f"{self.fileid}-->Image analysis failed for {imgpath}: {e}",exc_info=True)

    def extract_content(self) -> Tuple[List[Dict[str, Any]], str]:
        
        self.has_text = self._has_text()
        logger.info(f'{self.fileid}-->File has text: {self.has_text}')
        if not self.has_text:
            ocr_text = self.ocr_images()
            self.fileContent = ocr_text
            return self.fileContent

        logger.info(f'{self.fileid}-->Extracting content from...{self.filepath}')
        with pdfplumber.open(self.filepath) as pdf:
            logger.info(f"{self.fileid}-->Number of pages: {len(pdf.pages)}")
            for page_no, page in enumerate(pdf.pages):
                #CHanged by kedar: added a debug print
                logger.info(f"processing page: {page_no}")
                logger.debug(f"{self.fileid}-->Processing page {page_no}-{page.__sizeof__()}")
                # 1) detect tables
                tables = self._get_table_data(page)

                # 2) detect images
                images = self._get_image_data(page)

                # 3) extract lines
                lines = self._get_lines(page)

                # 4) build element list
                elements = self._build_element_list(page_no, lines, tables, images)

                # sort elements top-to-bottom (small center y first), left-to-right
                def elem_key(e):
                    x0, y0, x1, y1 = e["bbox"]
                    center_y = (y0 + y1) / 2.0
                    center_x = (x0 + x1) / 2.0
                    return (round(center_y, 1), round(center_x, 1))
                elements_sorted = sorted(elements, key=elem_key)

                # Append to extracted_items (in order) with proper formatting
                self._fill_extracted_items_in_order(page_no, page, elements_sorted)

        logger.info(f'{self.fileid}-->Extracted {len(self.extracted_items)} items and {self.detected_text_lines_total} lines')
        if self.detected_text_lines_total <= 10:
            self.extracted_items =[]
            self.detected_text_lines_total = 0
            with fitz.open(self.filepath) as doc:
                for idx,page in enumerate(doc):
                    item = {"type": "text", "content": page.get_text()}
                    self.extracted_items.append(item)
                    self.detected_text_lines_total+=1

        # self.fileContent = self.get_filecontent()
        # out_text_path = self.filepath.with_suffix('.txt')
        # with open(out_text_path, 'w', encoding='utf-8') as outf:
        #     outf.write(self.fileContent)

        # if self.OUTPUT_DIR.exists():
        #     shutil.rmtree(self.OUTPUT_DIR)
        logger.info(f'{self.fileid}-->Completed Extracting content from...{self.filepath}')
        return self.fileContent

    def ocr_images(self):
        """
        Perform OCR on images when no text lines are detected.
        """
        ocr_text = ""
        doc = fitz.open(self.filepath)
        for page_num in range(len(doc)):
            page = doc[page_num]
            image_list = page.get_images()
            for img_num, img in enumerate(image_list):
                xref = img[0]
                pix = fitz.Pixmap(doc, xref)
                img_path = os.path.join(self.media_dir,f"image_{page_num}_{img_num}.png")
                pix.save(img_path)
                text = self.image_processor.get_ocr_text(Path(img_path))
                if text is not None and len(text) > 0:
                    ocr_text += text.strip() + "\n"
        doc.close()
        # Return the OCR text
        return ocr_text.strip()
    
    def get_filecontent(self, get_ocr = False):
        # After iterating pages, raise if no text lines at all
        if not self.has_text:
            pass
        elif self.detected_text_lines_total == 0:
            ocr_text = self.ocr_images()
            self.fileContent = ocr_text
        else:
            parts = []
            for it in self.extracted_items:
                if it["type"] == "text":
                    parts.append(it["content"])
                elif it["type"] == "table":
                    parts.append(it["content"])
                elif it["type"] == "image":
                    img_parts = it.get("content",'').split('~|')
                    img_info =''
                    if get_ocr:
                        if len(img_parts) == 3:
                            img_info += img_parts[0] + '~|' + img_parts[2] + '\n'
                        elif len(img_parts) == 2 and img_parts[1].startswith('CONTENT:'):
                            img_info += line + '\n'
                    else:
                        if len(img_parts) == 3:
                            img_info += '~|'.join(img_parts[0:2]) + ']\n'
                        elif len(img_parts) == 2 and img_parts[1].startswith('DESCRIPTION:'):
                            img_info += line + '\n'
                        else:
                            continue
                    parts.append(img_info)
            self.fileContent = "\n".join(parts)

        logger.info(f'{self.fileid}-->Reassembled the file content of len {len(self.fileContent)} with get_ocr = {get_ocr}')

        # Extra sanity: remove accidental consecutive duplicate lines that sometimes occur (defensive)
        lines_out = []
        prev = None
        for line in self.fileContent.splitlines():
            line = LM_REMOVE_JUNKS(line).strip()
            if line == prev or len(line.split())<3 or line.startswith('©') \
                or line.lower().endswith('confidential') or line.__contains__('...............')\
                or line.lower().startswith('page') or line.lower().startswith('sr #') or line.lower().startswith('copyright')\
                or line.lower().startswith('table of contents'):
                # skip exact duplicate line immediately repeated
                continue
            lines_out.append(line)
            prev = line
        self.fileContent = "\n".join(lines_out)
        logger.info(f'{self.fileid}-->Removed duplicates from file content of len {len(self.fileContent)}')
        if len(self.fileContent) == 0:
            raise EmptyFileError
        return self.fileContent,[]
    
    def clean_up(self):
        if self.debug:
            out_text_path = os.path.join(self.OUTPUT_DIR,"filecontent.txt")
            with open(out_text_path, 'w', encoding='utf-8') as outf:
                outf.write(self.fileContent)
        if self.filepath.with_suffix('.txt').exists():
            os.remove(self.filepath.with_suffix('.txt'))

        # if self.OUTPUT_DIR.exists():
        #     try:
        #         shutil.rmtree(self.OUTPUT_DIR)
        #     except PermissionError:
        #         time.sleep(10)  # wait for 1 second
        #         try:
        #             shutil.rmtree(self.OUTPUT_DIR)
        #         except PermissionError as e:
        #             logger.error(f"{self.fileid}-->Failed to delete {self.OUTPUT_DIR}: {e}")

        if self.filepath.parent.exists():
            try:
                shutil.rmtree(self.filepath.parent)
            except PermissionError:
                time.sleep(10)  # wait for 10 seconds
                try:
                    shutil.rmtree(self.filepath.parent)
                except PermissionError as e:
                    logger.error(f"{self.fileid}-->Failed to delete {self.filepath.parent}: {e}", exc_info=True)
