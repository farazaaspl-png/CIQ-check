from pathlib import Path
import re
from typing import Dict
import os, time, uuid, zipfile, shutil, pytesseract
from lxml import etree
# from langdetect import detect, DetectorFactory
from openai import RateLimitError
from PIL import Image
import logging
from html.parser import HTMLParser
import pandas as pd


# Import your existing ImageProcessor
from core.exceptions import EmptyFileError
from core.imagehandlers.imagehelper import ImageProcessor  # Replace with your actual import
from core.utility import get_custom_logger, _DOCUMENT_INFO_TABLES, _OTHER_CONTROL_TABLES, _SUBSTRINGS_TO_SKIP
logger = get_custom_logger(__name__)
# logger.propagate = False

# DetectorFactory.seed = 0

class _HTMLTextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts = []
        self._skip_tags = {'script', 'style'}
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self._skip_tags:
            self._skip_depth += 1
        if tag.lower() in {'p', 'div', 'br', 'tr', 'li', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'td', 'th'}:
            self._parts.append('\n')

    def handle_endtag(self, tag):
        if tag.lower() in self._skip_tags:
            self._skip_depth = max(0, self._skip_depth - 1)

    def handle_data(self, data):
        if self._skip_depth == 0:
            self._parts.append(data)

    def get_text(self):
        return re.sub(r'\n{3,}', '\n\n', ''.join(self._parts)).strip()

def qname_local(el):
    if el is None:
        return None
    try:
        return etree.QName(el).localname
    except (ValueError, TypeError, AttributeError) as e:
        logger.warning(f"Invalid element passed to qname_local: {type(el)}, {el}")
        return None

class DocumentExtractor:

    NS = {
        'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main',
        'a': 'http://schemas.openxmlformats.org/drawingml/2006/main',
        'r': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships',
        'wp': 'http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing',
        'v': 'urn:schemas-microsoft-com:vml'
    }

    def __init__(self, filepath: Path, waspdf: bool = False, debug: bool = False, analyze_images: bool = True, fileid: uuid = None):
        self.analyze_images = analyze_images
        self.debug = debug
        self.waspdf = waspdf
        self.fileid = fileid
        if self.debug:
            logger.setLevel(logging.DEBUG)

        self.filepath = filepath
        if not self.filepath.exists():
            raise FileNotFoundError(filepath)
       
        # create media directory
        self.OUTPUT_DIR = Path(os.path.join(filepath.parent, filepath.stem.replace('-','').replace(' ', '_')))
        if not self.OUTPUT_DIR.exists():
            self.OUTPUT_DIR.mkdir(parents=True)

        self.media_dir = Path(os.path.join(self.OUTPUT_DIR, 'media'))
        if self.media_dir.exists():
            shutil.rmtree(self.media_dir)
        self.media_dir.mkdir(parents=True)

        self.assembled = [f"Document: {self.filepath.name}"]
        self._saved_media = {}
        self.fileContent = None

        self.image_processor = ImageProcessor(media_dir = self.media_dir,fileid = fileid, debug = debug)
        self.ratelimit_exceeded = False
                    

    def __read_rels_map(self, doc_zip, rels_path):
        """Parse a .rels file from the zip and return map {rId: target_path_in_zip}"""
        rel_map = {}
        if rels_path not in doc_zip.namelist():
            return rel_map
        try:
            xml = doc_zip.read(rels_path)
            root = etree.fromstring(xml)
            for rel in root.findall('.//{http://schemas.openxmlformats.org/package/2006/relationships}Relationship'):
                rid = rel.get('Id') or rel.get('Id'.lower()) or rel.get('id')
                target = rel.get('Target')
                if not rid or not target:
                    continue
                normalized = target.replace('../', '')
                if not normalized.startswith('word/'):
                    normalized = 'word/' + normalized.lstrip('/')
                rel_map[rid] = normalized
        except Exception:
            pass
        return rel_map
   
    def __save_media_target(self, zipf: zipfile.ZipFile, target: str):
        """Save a target path from inside the zip into media_dir"""
        if target in self._saved_media:
            return self._saved_media[target]
        if target not in zipf.namelist():
            alt = target.replace('word/', '')
            if alt in zipf.namelist():
                target = alt
            else:
                return None
        outfn = self.media_dir / Path(target).name
        i = 1
        base, suf = outfn.stem, outfn.suffix
        while outfn.exists():
            outfn = self.media_dir / f"{base}_{i}{suf}"
            i += 1
        with zipf.open(target) as rf, open(outfn, 'wb') as wf:
            wf.write(rf.read())
        self._saved_media[target] = str(outfn)
        return outfn
   
    def __extract_media_by_rid(self, zipf, rel_map, rid):
        """Resolve rId -> target via rel_map then save media; return local path or None."""
        if not rid:
            return None
        target = rel_map.get(rid)
        if not target:
            return None
        return Path(self.__save_media_target(zipf, target))

    def __extract_alt_chunk(self, rId: str, docZip: zipfile.ZipFile, doc_rels: dict) -> str:
        if not rId:
            return ''
        zip_path = doc_rels.get(rId)
        if not zip_path:
            logger.warning(f"{self.fileid}-->altChunk rId={rId} not found in relationships")
            return ''
        candidates = [zip_path, zip_path.replace('word/', '', 1), zip_path.lstrip('/')]
        raw_bytes = None
        for candidate in candidates:
            if candidate in docZip.namelist():
                raw_bytes = docZip.read(candidate)
                break
        if raw_bytes is None:
            logger.warning(f"{self.fileid}-->altChunk target not found in zip: {zip_path}")
            return ''
        try:
            raw_text = raw_bytes.decode('utf-8')
        except UnicodeDecodeError:
            raw_text = raw_bytes.decode('latin-1')
        lower_head = raw_bytes[:200].lower()
        # HTML chunk (most common from docx4j)
        if b'<html' in lower_head or b'<!doctype html' in lower_head:
            extractor = _HTMLTextExtractor()
            extractor.feed(raw_text)
            return extractor.get_text()
        # OOXML / XML chunk
        try:
            chunk_root = etree.fromstring(raw_bytes)
            texts = [
                elem.text for elem in chunk_root.iter()
                if (elem.tag.endswith('}t') or elem.tag == 't') and elem.text
            ]
            return ' '.join(texts).strip()
        except etree.XMLSyntaxError:
            pass
        # Plain text fallback
        return raw_text.strip()

    def _describe_image(self,imgpath: Path):
        self.image_processor.get_ocr_text(imgpath)
        image_info = f"[IMAGE:{imgpath.name}]"
        if self.analyze_images and not self.ratelimit_exceeded:
            try:
                self.image_processor.analyze_image(imgpath,image_size_threshold = 200000 if self.waspdf else 10000)
            except RateLimitError as e:
                logger.warning(f"{self.fileid}-->Rate limit exhausted for API calls: {e}",exc_info=True)
                self.ratelimit_exceeded = True     
            except Exception as e:
                logger.warning(f"{self.fileid}-->Image analysis failed for {imgpath}: {e}",exc_info=True)
        
        return image_info


    def __extract_segments_from_paragraph(self, par_el, rel_map, zipf):
        """Walk the immediate child nodes of a <w:p> in order and return a list of segments"""
        seq = []

        def getTexts(child):
            texts = []
            for t in child.findall('.//w:t', namespaces=self.NS):
                text = None
                if t.text is not None:
                    text = t.text.strip()
                if text and text not in texts:
                    texts.append(text)
            return texts
        
       
        def look_images_in_run(child):
            try:
                # Drawing-based images inside this run
                for blip in child.findall('.//a:blip', namespaces=self.NS):
                    rid = blip.get('{%s}embed' % self.NS['r']) or blip.get('embed')
                    imgpath = self.__extract_media_by_rid(zipf, rel_map, rid)
                    if imgpath:
                        image_info = self._describe_image(imgpath)
                        if image_info:
                            seq.append(image_info)

                # Legacy VML images inside w:pict
                for im in child.findall('.//v:imagedata', namespaces=self.NS):
                    rid = im.get('{%s}id' % self.NS['r']) or im.get('id')
                    imgpath = self.__extract_media_by_rid(zipf, rel_map, rid)
                    if imgpath:
                        image_info = self._describe_image(imgpath)
                        if image_info:
                            seq.append(image_info)


                # Legacy VML images inside w:pict
                for im in child.findall('.//v:imagedata', namespaces=self.NS):
                    rid = im.get('{%s}id' % self.NS['r']) or im.get('id')
                    imgpath = self.__extract_media_by_rid(zipf, rel_map, rid)
                    if imgpath:
                        image_info = self._describe_image(imgpath)
                        if image_info:
                            seq.append(image_info)
            except RateLimitError as e:
                return

        # Iterate child elements in document order
        for child in par_el:
            tag_local = qname_local(child)
            if tag_local is None:
                continue
            if tag_local == 'r':
                texts = getTexts(child)
                if texts:
                    seq.append(' '.join(texts))
                look_images_in_run(child)
            else:
                for t in getTexts(child):
                    seq.append(t)
                look_images_in_run(child)
        
        # Collapse adjacent text segments into single strings for cleanliness
        merged = []
        buf = []
        for s in seq:
            if s.startswith('[IMAGE:'):
                if buf:
                    merged.append(''.join(buf))
                    buf = []
                merged.append(s)
            else:
                buf.append(s)
        if buf:
            merged.append(' '.join(buf))
        # Strip empty strings
        merged = [m for m in merged if m and not (m.strip() == '')]
        return merged

    def __element_to_text_with_images(self, el, rel_map, zipf):
        """Extract paragraphs inside `el` (cell/body/comment/footnote) preserving inline images; return single string."""
        parts = []
        # attempt to find paragraph children in order
        for p in el.findall('.//w:p', namespaces=self.NS):
            # But to preserve order within a cell, iterate using child order of direct descendants when possible.
            parts_seq = self.__extract_segments_from_paragraph(p, rel_map, zipf)
            # join parts of this paragraph with no extra separator (we'll separate paragraphs by single space)
            if parts_seq:
                parts.append(' '.join(parts_seq))
        return ' '.join(parts).strip()
    
    def __table_to_rows(self, tbl_el, rel_map, zipf):
        rows = []
        for tr in tbl_el.findall('.//w:tr', namespaces=self.NS):
            cells = []
            for tc in tr.findall('.//w:tc', namespaces=self.NS):
                cell_text = self.__element_to_text_with_images(tc, rel_map, zipf)
                cell_text = ' '.join(cell_text.splitlines()).strip()
                cells.append(cell_text)
            rows.append(cells)
        return rows

    def __iterate_part(self, root_el, zipf, rel_map_for_part):
        """
        Yield ('p', [segments...]) and ('tbl', rows) for top-level children of a root-like element.
        Works for document body (root_el=document root) and header/footer (root_el parsed header xml root).
        """
        root_el_tag = qname_local(root_el)
        body = root_el.find('w:body', namespaces=self.NS) if root_el_tag == 'document' else root_el
        if body is None:
            # maybe header/footer root itself contains paragraphs
            body = root_el
        for child in body:
            self._last_element = child
            tag = qname_local(child)
            if tag is None:
                continue
            if tag == 'p':
                segs = self.__extract_segments_from_paragraph(child, rel_map_for_part, zipf)
                if segs:
                    yield ('p', segs)
            elif tag == 'tbl':
                yield ('tbl', self.__table_to_rows(child, rel_map_for_part, zipf))
            else:
                # other elements: try to extract text and images
                txt = self.__element_to_text_with_images(child, rel_map_for_part, zipf)
                if txt:
                    yield ('p', [txt])

    # generic processor for header/footer parts
    def __process_parts(self, prefix, label, zipf, doc_rels):
        for name in sorted([n for n in zipf.namelist() if n.startswith(prefix) and n.endswith('.xml')]):
            self.assembled.append(f"=== {label}: {name} ===")
            rels_path = 'word/_rels/' + Path(name).name + '.rels'
            relmap = self.__read_rels_map(zipf, rels_path) or doc_rels
            try:
                root = etree.fromstring(zipf.read(name))
                for typ, data in self.__iterate_part(root, zipf, relmap):
                    if typ == 'p':
                        self.assembled.append(' '.join(data))
                    elif typ == 'tbl':
                        self.assembled.append('--- TABLE START ---')
                        for row in data:
                            self.assembled.append('~'.join(row))
                        self.assembled.append('--- TABLE END ---')
            except Exception as e:
                logger.warning(f"{self.fileid}-->[error reading {name}: {e}]",exc_info=True)

    # footnotes & endnotes & comments (use doc_rels for images inside these parts)
    def __process_notes(self,path, label, container_tag, zipf, doc_rels):
        if path in zipf.namelist():
            self.assembled.append(f"=== {label} ===")
            root = etree.fromstring(zipf.read(path))
            for part in root.findall(container_tag, namespaces=self.NS):
                pid = part.get('id')
                if pid and pid not in ('-1', '0'):
                    # iterate as a part; the iterator expects an element, so we can pass the note element itself
                    parts = []
                    for typ, data in self.__iterate_part(part, zipf, doc_rels):
                        if typ == 'p':
                            parts.append(' '.join(data))
                        elif typ == 'tbl':
                            parts.append('TABLE_IN_NOTE')
                    if parts:
                        self.assembled.append(f"[{label[:-1].lower()} id={pid}] " + ' '.join(parts))

    # def _check_language(self,filecontent):
    #     if not filecontent.strip():
    #         return
    #     lang = detect(filecontent)
    #     if lang != "en":
    #         raise ValueError(f"Document language detected as '{lang}', not English.")

    def _is_toc_paragraph(self, element: etree._Element) -> bool:
        """
        Return True if *element* is a <w:p> that represents a Table‑of‑Contents.
        The detection is deliberately lightweight – it only looks for the
        field‑begin marker followed by an instrText that starts with "TOC".
        """
        # 1️⃣ Look for the field‑begin marker
        fld_begin = element.find('.//w:fldChar[@w:fldCharType="begin"]', namespaces=self.NS)
        if fld_begin is None:
            return False

        # 2️⃣ Find the following <w:instrText> (may be a sibling or descendant)
        instr = element.find('.//w:instrText', namespaces=self.NS)
        if instr is None:
            return False

        # 3️⃣ Check that the instruction actually starts with the TOC keyword
        return instr.text and instr.text.strip().upper().startswith("TOC")
    
    def has_all_images_and_no_text(self, docZip, doc_rels, doc_root):
        """
        Check if the document contains only images and no text.
        
        Args:
            docZip (zipfile.ZipFile): The ZIP file containing the document.
            doc_rels (dict): The relationships map for the document.
            doc_root (etree.Element): The root element of the document XML.
        
        Returns:
            bool: True if the document contains only images and no text, False otherwise.
        """
        analyze_images = self.analyze_images
        self.analyze_images = False
        # Check if any text is extracted from the document
        has_text = False
        for typ, data in self.__iterate_part(doc_root, docZip, doc_rels):
            if typ == 'p':
                if any(item for item in data if not item.startswith("[IMAGE:")):
                    has_text = True
                    break

        # Check if all extracted items are images
        all_images = True
        for typ, data in self.__iterate_part(doc_root, docZip, doc_rels):
            if typ == 'p':
                if any(item for item in data if not item.startswith("[IMAGE:")):
                    all_images = False
                    break
        self.analyze_images = analyze_images
        return not has_text and all_images

    def extract_text_from_images(self, docZip):
        for name in docZip.namelist():
            # Check if the file is an image
            if name.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif', '.tiff')):
                try:
                    # Extract the image from the zip file
                    image_data = docZip.read(name)
                    # Save the image to a temporary file
                    temp_image_path = os.path.join(self.media_dir, os.path.basename(name))
                    with open(temp_image_path, 'wb') as f:
                        f.write(image_data)
                    # Extract text from the image using OCR
                    text = pytesseract.image_to_string(Image.open(temp_image_path))
                    self.assembled.append(text)
                    # Remove the temporary image file
                    # os.remove(temp_image_path)
                except Exception as e:
                    logger.warning(f"{self.fileid}-->Error extracting text from image {name}: {e}",exc_info=True)

    def extract_content(self,headerfooter=False,notes=False,indexes=False,comments=False):
        
        logger.info(f"{self.fileid}-->Started extracting content from {self.filepath}")
        with zipfile.ZipFile(self.filepath, 'r') as docZip:
            if 'word/document.xml' not in docZip.namelist():
                raise ValueError(f"{self.filepath} doesn't contain word/document.xml")
            
            doc_rels = self.__read_rels_map(docZip, 'word/_rels/document.xml.rels')
            doc_xml = docZip.read('word/document.xml')
            doc_root = etree.fromstring(doc_xml)

            # Check if the document contains only images and no text
            if self.has_all_images_and_no_text(docZip, doc_rels, doc_root):
                logger.info(f"{self.fileid}-->The document contains only images and no text.")
                self.extract_text_from_images(docZip)
            # else:
            #     for typ, data in self.__iterate_part(doc_root, docZip, doc_rels):
            #         if typ == 'p':
            #             if (not indexes) and getattr(self, "_last_element", None) and self._is_toc_paragraph(self._last_element):
            #                 # Reset the temporary holder and simply continue – TOC omitted
            #                 self._last_element = None
            #                 continue
            #             # data is list of segments; join with space for final line but keep images inline
            #             self.assembled.append(' '.join(data))
            #         elif typ == 'tbl':
            #             self.assembled.append('--- TABLE START ---')
            #             for row in data:
            #                 if ''.join(row):
            #                     self.assembled.append(' ~ '.join(cell if cell is not None else '' for cell in row))
            #             self.assembled.append('--- TABLE END ---')
            #     logger.info(f"{self.fileid}-->Extracted content from all the parts of file")
            
            else:                
                body = doc_root.find(f'{{{self.NS["w"]}}}body')
                if body is None:
                    body = doc_root
 
                for elem in body:
                    if not isinstance(elem.tag, str):
                        continue  # skip comment nodes, processing instructions, etc.
                    local = etree.QName(elem.tag).localname if '}' in elem.tag else elem.tag
                    if local == 'altChunk':
                        rId = elem.get(f'{{{self.NS["r"]}}}id') or elem.get('id') or ''
                        chunk_text = self.__extract_alt_chunk(rId, docZip, doc_rels)
                        if chunk_text:
                            for line in chunk_text.splitlines():
                                line = line.strip()
                                if line:
                                    self.assembled.append(line)
                        continue
 
                    if local == 'p':
                        self._last_element = elem
                        if (not indexes) and getattr(self, "_last_element", None) and self._is_toc_paragraph(self._last_element):
                            self._last_element = None
                            continue
                        segs = self.__extract_segments_from_paragraph(elem, doc_rels, docZip)
                        if segs:
                            self.assembled.append(' '.join(segs))
 
                    elif local == 'tbl':
                        self._last_element = elem
                        self.assembled.append('--- TABLE START ---')
                        for row in self.__table_to_rows(elem, doc_rels, docZip):
                            if ''.join(row):
                                self.assembled.append(' ~ '.join(cell if cell is not None else '' for cell in row))
                        self.assembled.append('--- TABLE END ---')
 
                    else:
                        self._last_element = elem
                        txt = self.__element_to_text_with_images(elem, doc_rels, docZip)
                        if txt:
                            self.assembled.append(txt)
 
                logger.info(f"{self.fileid}-->Extracted content from all the parts of file")

                if headerfooter:
                    # process_parts(self, prefix, label, zipf, doc_rels)
                    self.__process_parts('word/header', 'HEADER', docZip, doc_rels)
                    self.__process_parts('word/footer', 'FOOTER', docZip, doc_rels)
                    logger.info(f"{self.fileid}-->Extracted content from header and footer of file")

                if notes:
                    self.__process_notes('word/footnotes.xml', 'FOOTNOTES', './/w:footnote', docZip, doc_rels)
                    self.__process_notes('word/endnotes.xml', 'ENDNOTES', './/w:endnote', docZip, doc_rels)
                    logger.info(f"{self.fileid}-->Extracted content from notes of file")

                if comments and 'word/comments.xml' in docZip.namelist():
                    self.assembled.append('=== COMMENTS ===')
                    cm_root = etree.fromstring(docZip.read('word/comments.xml'))
                    for c in cm_root.findall('.//w:comment', namespaces=self.NS):
                        cid = c.get('id') or c.get('{%s}id' % self.NS['w'])
                        auth = c.get('author') or ''
                        parts = []
                        for typ, data in self.__iterate_part(c, docZip, doc_rels):
                            if typ == 'p':
                                parts.append(' '.join(data))
                        self.assembled.append(f"[comment id={cid} author={auth}] " + ' '.join(parts))
                    logger.info(f"{self.fileid}-->Extracted content comments of file")

                # extract any remaining media files (word/media/*) not yet saved
                for m in sorted([n for n in docZip.namelist() if n.startswith('word/media/')]):
                    if m not in self._saved_media:
                        local = Path(self.__save_media_target(docZip, m))
                        if local:
                            try:
                                image_info = self._describe_image(local)
                            except RateLimitError as rl:
                                break
                            if image_info:
                                self.assembled.append(image_info)
                logger.info(f"{self.fileid}-->Tried extracting content from remaining media files of file")

                # Embedded objects and macros (existing code)
                embeds = [n for n in docZip.namelist() if n.startswith('word/embeddings/') or n.startswith('word/oleObjects/')]
                if embeds:
                    self.assembled.append('=== EMBEDDED OBJECTS ===')
                    for e in embeds:
                        outfn = self.media_dir / Path(e).name
                        with docZip.open(e) as ef, open(outfn, 'wb') as of:
                            of.write(ef.read())
                        self.assembled.append(str(outfn))
                    logger.info(f"{self.fileid}-->Extracted content from embedded objects of file")

                if 'word/vbaProject.bin' in docZip.namelist():
                    self.assembled.append('=== MACROS (vbaProject.bin present) ===')
                    self.assembled.append('word/vbaProject.bin')
                    logger.info(f"{self.fileid}-->Extracted content from macros of file")
        
        content_text = "\n".join(self.assembled).strip()
        if len(content_text) < 50:
            raise EmptyFileError()

    
    def _table_parser(self, datarows):
        if not datarows or len(datarows) < 2:
            return None

        # The first row should be headers - keep empty cells but create made-up names
        headers = []
        for i, h in enumerate(datarows[0]):
            if h.strip():
                headers.append(h.strip())
            else:
                headers.append(f"Column_{i}")
    
        # Process data rows
        data_rows = []
        for row in datarows[1:]:
            clean_row = [cell.strip() for cell in row if cell.strip()]
            if clean_row:
                data_rows.append(clean_row)

        if not data_rows:
            return None

        # Ensure all rows have the same number of columns
        max_cols = max(len(headers), max(len(row) for row in data_rows))

        # Pad headers if needed
        while len(headers) < max_cols:
            headers.append(f"Column_{len(headers)}")

        # Pad data rows if needed
        padded_data = []
        for row in data_rows:
            padded_row = row + [None] * (max_cols - len(row))
            padded_data.append(padded_row)

        try:
            return pd.DataFrame(padded_data, columns=headers)
        except Exception as e:
            logger.warning(f"{self.fileid}-->Table parsing error: {e}")
            return None
            
    def get_filecontent(self, get_ocr = False):
        CONTROL_TABLE_NAMES = _OTHER_CONTROL_TABLES + (_DOCUMENT_INFO_TABLES if get_ocr else [])
        self.fileContent = ''
        logger.debug(f"{self.fileid}-->Images collected:- {self.image_processor.image_dict}")
        listoftabledata = []
        tablerows = False
        cnt = 0
        iscontrol_table = False
        self.tab_data=[]
        tab_data = []
        previous_line = ''

        for line in self.assembled:
            if tablerows:
                # Process table rows
                if line == '--- TABLE END ---':
                    # End of table - process collected data
                    if cnt>0 and not iscontrol_table:
                        self.tab_data.append(tab_data.copy())
                        # self.fileContent += previous_line + '\n' + '\n'.join(tab_data) + '\n'
                        df = self._table_parser(tab_data)
                        if df is None:
                            tablerows = False
                            cnt = 0
                            tab_data = []
                            previous_line = line
                            continue

                        if df.shape[0] > 0:
                            records = df.drop_duplicates().to_dict('records')

                            for idx, rec in enumerate(records):
                                clean_dict = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in rec.items() if v is not None and v != '' and v != []}
                                records[idx] = clean_dict
                            
                            if get_ocr:
                                listoftabledata.append(records)

                            # Add the line before table
                            if previous_line and previous_line != '--- TABLE START ---':
                                self.fileContent += previous_line + '\n'

                            self.fileContent += '--- TABLE START ---\n'
                            self.fileContent += '\n'.join([str(record) for record in records]) + '\n'
                            self.fileContent += '--- TABLE END ---\n'

                    # Reset table processing state
                    tablerows = False
                    cnt = 0
                    tab_data = []
                    previous_line = line
                    continue
                else:
                     # Collect table row data
                    if not iscontrol_table:
                        image_placeholder = self.replace_image_placeholders(text=line, image_dict=self.image_processor.image_dict,ocr=get_ocr)
                        # Split by ~ but keep the structure intact
                        row_data = list(dict.fromkeys(image_placeholder.split('~')))
                        tab_data.append(row_data)
                        cnt += 1
                    continue
            
            # Non-table processing
            if line.startswith('[IMAGE:'):
                image_placeholder = self.replace_image_placeholders(text=line, image_dict=self.image_processor.image_dict,ocr=get_ocr)  + '\n'
                if len(image_placeholder)>3:
                    if previous_line != '--- TABLE END ---':
                        self.fileContent += previous_line + '\n'
                    self.fileContent += image_placeholder
                    previous_line = ''  # Reset after image
                    # previous_line = line.strip()
            else:
                if line == '--- TABLE START ---':
                    iscontrol_table = previous_line.lower() in CONTROL_TABLE_NAMES
                    tab_data = []
                    tablerows, cnt = True ,0
                else:
                    if not (previous_line.startswith(tuple(_SUBSTRINGS_TO_SKIP)) or 
                            previous_line.strip() == line.strip() or 
                            previous_line == '--- TABLE END ---' or
                            line == '--- TABLE START ---'):
                        if previous_line:
                            self.fileContent += previous_line + '\n'
                    previous_line = line.strip()
        
        # Add the last line if it exists and is not a table marker
        if previous_line and previous_line not in ['--- TABLE START ---', '--- TABLE END ---']:
            self.fileContent += previous_line + '\n'

        self.fileContent = self.replace_image_placeholders(text=self.fileContent, image_dict=self.image_processor.image_dict,ocr=True,text_only=True) 
        logger.info(f'{self.fileid}-->Reassembled the file content of len {len(self.fileContent)} with get_ocr = {get_ocr}')

        return self.fileContent, listoftabledata

    def replace_image_placeholders(
        self,
        text: str,
        image_dict: Dict[str, Dict],
        placeholder_pat: str = r'\[IMAGE:\s*([^\]\s]+)\]',
        ocr: bool = False,
        text_only: bool = False
    ) -> str:
        excluded_categories = ['LOGO', 'SIGNATURE AND STAMP']
        def _replacer(match: re.Match) -> str:
            img_name = match.group(1)               # e.g. "image3.png"
            # ``image_dict`` keys are the **file name only**, not the full path
            img_details = image_dict.get(img_name)
            if not img_details: return ""
            if ocr:
                ocr_text = img_details.get("ocr_text")
                if not ocr_text: return ""
                if text_only: return ocr_text
                return f"[IMAGE: {img_name} ~ CONTENT: {ocr_text}]"
            else:
                llm_response = img_details.get('llm_response')
                if not llm_response: return ""
                if llm_response.get('category', 'UNKNOWN').upper() in excluded_categories: return ""
            
                description = llm_response.get('description','').strip()
                if len(description)==0: return ""
                return f"[IMAGE: {img_name} ~ DESCRIPTION: {description}]"
            
        return re.sub(placeholder_pat, _replacer, text, flags=re.IGNORECASE)
             
    def clean_up(self):
        if self.debug:
            out_text_path = os.path.join(self.OUTPUT_DIR,"filecontent.txt")
            with open(out_text_path, 'w', encoding='utf-8') as outf:
                outf.write(self.fileContent)

        if self.waspdf:
            os.remove(self.filepath)

        if self.filepath.parent.exists():
            try:
                shutil.rmtree(self.filepath.parent)
            except PermissionError:
                time.sleep(10)  # wait for 10 seconds
                try:
                    shutil.rmtree(self.filepath.parent)
                except PermissionError as e:
                    logger.error(f"{self.fileid}-->Failed to delete {self.filepath.parent}: {e}", exc_info=True)

