from pathlib import Path
import tempfile
import time
from typing import Dict
import uuid, zipfile, xml.etree.ElementTree as ET, re, os, shutil
import logging
from openai import RateLimitError
import pandas as pd

from core.exceptions import EmptyFileError
from core.imagehandlers.imagehelper import ImageProcessor
from core.utility import get_custom_logger, _DOCUMENT_INFO_TABLES, _OTHER_CONTROL_TABLES, _SUBSTRINGS_TO_SKIP
logger = get_custom_logger(__name__)

class PptxExtractor:
    """Reads PPTX extracting text, tables, images in visual order."""

    NS = {
        'p': 'http://schemas.openxmlformats.org/presentationml/2006/main',
        'a': 'http://schemas.openxmlformats.org/drawingml/2006/main',
        'r': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
    }

    def __init__(self, filepath:Path, waspdf: bool = False, debug: bool = False, analyze_images: bool = True, fileid: uuid = None):
        self.analyze_images = analyze_images
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.waspdf = waspdf
        self.fileid = fileid

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
        self.fileContent = ''

        self.image_processor = ImageProcessor(media_dir = self.media_dir,fileid = fileid, debug=self.debug)
        self.ratelimit_exceeded = False
        self.relationship_map = None
        self.content_types_map = None
        with zipfile.ZipFile(filepath) as pptx_zip:
            # pptx_zip.extractall(self.OUTPUT_DIR)
            self.relationship_map = self._build_relationship_map(pptx_zip)
            # self.content_types_map = self._build_content_types_map(pptx_zip)
    
    def extract_content(self):
        def _extract():
            logger.info(f'{self.fileid}-->Started extracting content from {self.filepath}')
            with zipfile.ZipFile(self.filepath) as pptx_zip:
                slide_files = sorted(
                    [f for f in pptx_zip.namelist() if f.startswith("ppt/slides/") and not f.startswith("ppt/slides/_rels/")],
                    key=lambda s: int(re.findall(r"(\d+)", s)[-1])
                )
                logger.info(f'{self.fileid}-->Sorted {len(slide_files)} slides in order')
                for slide_index, slide_file in enumerate(slide_files, start=1):
                    self._extract_slide(pptx_zip, slide_file, slide_index)
            self.fileContent = "\n".join(self.assembled)
        
        try:
            _extract()
        except ValueError as ve:
            logger.warning(f'{self.fileid}-->got value error repairing the file')
            self.repair_pptx()
            _extract()

        # if len(self.assembled)<5:
        #     raise EmptyFileError()

        content_text = "\n".join(self.assembled).strip()
        if len(content_text) < 50:
            raise EmptyFileError()
        
        # self._clean_up()
        # logger.info(f'{self.fileid}-->Completed cleaning up the temporary files')
        return self.fileContent
    
    def _post_process_shapes_for_reading_order(self, shapes):
        """
        Post-process shapes to handle special cases and improve reading order.
        """
        if not shapes:
            return shapes

        # Separate titles from other content
        titles = [s for s in shapes if s.get("type") == "title"]
        non_titles = [s for s in shapes if s.get("type") != "title"]

        # Sort titles by position (usually there's only one)
        titles.sort(key=lambda x: (x["y"], x["x"]))

        # Sort non-title content with enhanced logic
        non_titles.sort(key=self._enhanced_sort_key)

        # Combine: titles first, then other content
        return titles + non_titles
    
    def _enhanced_sort_key(self, item):
        """
        Enhanced sorting key that considers multiple factors for optimal reading order.
        Considers vertical bands, row grouping, and content type priority.
        """
        y = item["y"]
        x = item["x"]
        item_type = item.get("type", "text")

        # Row tolerance for items that should be read together
        # 50,000 EMUs ≈ 0.05 inches - groups items that are visually aligned
        row_tolerance = 50000

        # Group items into logical rows
        row_group = y // row_tolerance

        # Type priority for consistent ordering within the same row
        type_priority = {"text": 0, "table": 1, "image": 2}
        priority = type_priority.get(item_type, 99)

        return (row_group, x, priority)

    def _extract_slide(self, pptx_zip, slide_file, slide_num):
        xml = ET.fromstring(pptx_zip.read(slide_file))
        shapes = []

        # Collect all shapes with coordinates
        for sp in xml.findall(".//p:sp", self.NS):
            shape_data =self._get_shape_data(sp)
            if shape_data is not None:
                shapes.append(self._get_shape_data(sp))
        logger.info(f'{self.fileid}-->Collected all the shape data for slide {slide_num}')

        # Collect tables separately
        for graphicFrame in xml.findall(".//p:graphicFrame", self.NS):
            table_data = self._get_table_data(graphicFrame)
            if table_data is not None:
                shapes.append(self._get_table_data(graphicFrame))
        logger.info(f'{self.fileid}-->Collected all the tables data for slide {slide_num}')

        # Collect images
        imagecnt = 0
        for pic in xml.findall(".//p:pic", self.NS):
            image_info = self._get_image_data(pic,pptx_zip,slide_file)
            if image_info is not None:
                shapes.append(image_info)
                imagecnt+=1
        logger.info(f'{self.fileid}-->Collected {imagecnt} the image data for slide {slide_num}')

        # Sort by visual order (top to bottom, then left to right)
        # shapes.sort(key=lambda x: (x['y'], x['x']))
        shapes = self._post_process_shapes_for_reading_order(shapes)

        # slide_output = [f"<Slide {slide_num} - Start>"]
        self.assembled.append(f"=== Slide {slide_num} - Start ===")
        image_counter = 1
        # print(shapes)
        logger.debug(f'{self.fileid}-->Extracted text for slide {slide_num} - {shapes}')
        for item in shapes:
            if item["type"] == "title":
                self.assembled.append(f"Title:- {item['para'][0]['text']}".strip())

            elif item["type"] == "text":
                
                if len(item['para']) > 1:
                    self.assembled.append(f"Content:-")
                for p in item['para']:
                    if p["level"] == 0: tabs = "* "
                    else: tabs = (" " * p["level"])+'- '  # bullet hierarchy

                    self.assembled.append(f"{tabs}{p['text']}".strip())
                    # if p["bullet"]:
                    #     slide_output.append(f"{tabs}- {p['text']}")
                    # else:
                    #     slide_output.append(f"{tabs}{p['text']}")

            elif item["type"] == "table":
                self.assembled.append('--- TABLE START ---')
                for row in item['rows']:
                    self.assembled.append(f"{row}".strip())
                self.assembled.append('--- TABLE END ---')

            elif item["type"] == "image":
                # Name or incremental placeholder
                name = item['name'] or f"{image_counter}"
                imageinfo = f"[IMAGE: {name}]"
                # imageinfo = f"[IMAGE: {name}"
                # if item.get('description'):
                #     imageinfo += f"~|DESCRIPTION: {item.get('description','')}"
                # if len(item.get('content',''))>0:
                #     imageinfo += f"~|CONTENT: {item.get('content')}"
                # imageinfo += "]"

                self.assembled.append(imageinfo)
                image_counter += 1

        self.assembled.append(f"=== Slide {slide_num} - End ===")
        logger.info(f'{self.fileid}-->Extracted all the text for slide {slide_num}')
        # return "\n".join(slide_output)

    # def _get_coords(self, shape):
    #     off = shape.find(".//a:off", self.NS)
    #     if off is not None:
    #         return int(off.attrib.get("x", 0)), int(off.attrib.get("y", 0))
    #     return 99999999, 99999999
    def _get_coords(self, shape):
        """Extract coordinates with proper transform handling"""
        # Try to get offset from transform first
        xfrm = shape.find(".//p:xfrm", self.NS)
        if xfrm is not None:
            off = xfrm.find(".//a:off", self.NS)
            ext = xfrm.find(".//a:ext", self.NS)
            if off is not None:
                x = int(off.attrib.get("x", 0))
                y = int(off.attrib.get("y", 0))
                width = int(ext.attrib.get("cx", 0)) if ext is not None else 0
                height = int(ext.attrib.get("cy", 0)) if ext is not None else 0
                return x, y, width, height

        # Fallback to simple offset
        off = shape.find(".//a:off", self.NS)
        if off is not None:
            x = int(off.attrib.get("x", 0))
            y = int(off.attrib.get("y", 0))
            return x, y, 0, 0

        # Better fallback - use a large but reasonable value
        return 50000, 50000, 0, 0
    
    def _get_shape_data(self, sp):
        x, y, width, height = self._get_coords(sp)
        title = sp.find(".//p:nvSpPr/p:nvPr/p:ph[@type='title']", self.NS)

        paragraphs = []

        for p in sp.findall(".//a:p", self.NS):
            lvl_attr = p.find(".//a:pPr", self.NS)
            level = int(lvl_attr.attrib.get("lvl", 0)) if lvl_attr is not None and "lvl" in lvl_attr.attrib else 0

            # Check if this has bullet formatting
            has_bullet = p.find(".//a:buChar", self.NS) is not None or \
                         p.find(".//a:buFont", self.NS) is not None

            text_runs = [t.text for t in p.findall(".//a:t", self.NS) if t.text]
            line_text = " ".join(text_runs).strip()

            if line_text:
                paragraphs.append({
                    "text": line_text,
                    "bullet": has_bullet,
                    "level": level
                })

        shape_type = "title" if title is not None else "text"
        if len(paragraphs) > 0:
            # return {"type": shape_type, "x": x, "y": y, "para": paragraphs}
            return {"type": shape_type, "x": x, "y": y, "width": width, "height": height, "para": paragraphs}

    # def _get_table_data(self, frame):
    #     x, y, width, height = self._get_coords(frame)
    #     rows_data = []

    #     for row in frame.findall(".//a:tr", self.NS):
    #         cells = []
    #         for cell in row.findall(".//a:tc", self.NS):
    #             text_runs = cell.findall(".//a:t", self.NS)
    #             cell_text = " ".join([t.text for t in text_runs if t.text])
    #             cells.append(cell_text.strip())
    #         row_line = "~".join(cells)
    #         rows_data.append(row_line)
    #     if len(rows_data) > 0: 
    #         # return {"type": "table", "x": x, "y": y, "rows": rows_data}
    #         return {"type": "table", "x": x, "y": y, "width": width, "height": height, "rows": rows_data}
    def _get_table_data(self, frame):
        x, y, width, height = self._get_coords(frame)

        # First, collect all cells with their merge information
        all_cells = []
        for row_idx, row in enumerate(frame.findall(".//a:tr", self.NS)):
            for col_idx, cell in enumerate(row.findall(".//a:tc", self.NS)):
                text_runs = cell.findall(".//a:t", self.NS)
                cell_text = " ".join([t.text for t in text_runs if t.text]).strip()

                # Check for horizontal merging (gridSpan) and vertical merging (rowSpan)
                grid_span = cell.get("gridSpan")
                row_span = cell.get("rowSpan")

                all_cells.append({
                    'row_idx': row_idx,
                    'col_idx': col_idx,
                    'text': cell_text,
                    'grid_span': int(grid_span) if grid_span else 1,
                    'row_span': int(row_span) if row_span else 1
                })

        # Process merged cells and create the final table structure
        processed_table = self._process_table_merges(all_cells)

        # Convert to row_data format
        rows_data = []
        for row_idx in sorted(processed_table.keys()):
            row_cells = []
            for col_idx in sorted(processed_table[row_idx].keys()):
                row_cells.append(processed_table[row_idx][col_idx])
            row_line = "~".join(row_cells)
            rows_data.append(row_line)

        if len(rows_data) > 0: 
            return {"type": "table", "x": x, "y": y, "width": width, "height": height, "rows": rows_data}

    def _process_table_merges(self, all_cells):
        """
        Process both horizontal and vertical merges in the table.
        Returns a dictionary structure with properly populated merged cells.
        """
        if not all_cells:
            return {}
        # Create a grid to track all cell positions
        max_row = max(cell['row_idx'] for cell in all_cells) + 1
        max_col = max(cell['col_idx'] for cell in all_cells) + 1

        # Initialize empty grid
        table_grid = {}
        for row_idx in range(max_row):
            table_grid[row_idx] = {}
            for col_idx in range(max_col):
                table_grid[row_idx][col_idx] = ""

        # Process each cell and handle merges
        for cell in all_cells:
            row_idx = cell['row_idx']
            col_idx = cell['col_idx']
            cell_text = cell['text']
            grid_span = cell['grid_span']
            row_span = cell['row_span']

            # Skip if this position is already filled (part of a previous merge)
            if table_grid[row_idx][col_idx]:
                continue
            
            # Fill all merged positions with the same text
            for r_offset in range(row_span):
                for c_offset in range(grid_span):
                    merged_row = row_idx + r_offset
                    merged_col = col_idx + c_offset
                    if merged_row < max_row and merged_col < max_col:
                        table_grid[merged_row][merged_col] = cell_text

        return table_grid

    def _describe_image(self,imgpath: Path):
        self.image_processor.get_ocr_text(imgpath)
        if self.analyze_images and (not self.ratelimit_exceeded):
            try:
                self.image_processor.analyze_image(Path(imgpath),image_size_threshold = 200000 if self.waspdf else 10000)
                # analysis_result = self.image_processor.analyze_image(Path(imgpath))
                # category = analysis_result.get('category', 'UNKNOWN').upper()
                # # Check if category should be excluded from detailed analysis
                # excluded_categories = ['LOGO', 'SIGNATURE', 'STAMP']

                # if category not in excluded_categories:
                #     image_info = {}
                #     description = analysis_result.get('description')
                #     if description:
                #         image_info['description'] = description
                #     return image_info
            except RateLimitError as e:
                logger.warning(f'{self.fileid}-->Rate limit exhausted for API calls: {e}',exc_info=True)   
                self.ratelimit_exceeded = True     
            except Exception as e:
                logger.warning(f'{self.fileid}-->Image analysis failed for {imgpath}: {e}',exc_info=True)

    def _get_relationship_target(self, pptx_zip, slide_file, rel_id):
        # Get the relationships file for the slide
        rels_file = f'ppt/slides/_rels/{slide_file.split("/")[-1]}.rels'
        if rels_file in pptx_zip.namelist():
            rels_xml = ET.fromstring(pptx_zip.read(rels_file))
            for rel in rels_xml.findall('.//{http://schemas.openxmlformats.org/package/2006/relationships}Relationship'):
                if rel.attrib.get('Id') == rel_id:
                    return rel.attrib.get('Target')
        return None

    def _get_image_data(self, pic,pptx_zip,slide_file):
        logger.debug(f'{self.fileid}-->pic: {ET.tostring(pic)} -- slidename: {slide_file}')
        x, y, width, height = self._get_coords(pic)
        image_info = {"type": "image", "x": x, "y": y, "width": width, "height": height}

        blip = pic.find(".//a:blip", self.NS)
        logger.debug(f'{self.fileid}-->got blip: {ET.tostring(blip)}')
        if blip is None:
            return
        
        rel_id = blip.attrib.get(f"{{{self.NS['r']}}}embed", None)
        logger.debug(f'{self.fileid}-->got rid: {rel_id}')
        # Extract the image from the PPTX file
        if rel_id is None:
            return
        
        file_name = self.relationship_map.get(Path(slide_file).name+'.rels').get(rel_id)
        logger.debug(f'{self.fileid}-->got image file name: {file_name}')
        if (file_name is None) or (not file_name.endswith(('.jpg', '.png', '.gif', '.jpeg', '.bmp', '.tiff', 'svg'))):
            return
        
        image_zip_path = file_name.replace('..','ppt')
        # Save the image to a temporary file
        image_path = Path(os.path.join(self.media_dir, Path(image_zip_path).name))
        image_info['name'] = image_path.name
        with open(image_path, "wb") as f:
            f.write(pptx_zip.read(image_zip_path))

        logger.debug(f'{self.fileid}-->Image file saved to {image_path} file')
        # ocr_text = self.image_processor.get_ocr_text(image_path).strip()
        # image_info["content"]= ocr_text

        # if self.analyze_images and (not self.ratelimit_exceeded) and (len(ocr_text) > 10):
        #     image_desc = self._describe_image(image_path)
        #     if image_desc:
        #         image_info["description"]= image_desc.get("description")
        self._describe_image(image_path)
        return image_info
    
    def _build_relationship_map(self, pptx_zip):
        relationship_map = {}
        for file in pptx_zip.namelist():
            if file.startswith('ppt/') and file.endswith('.rels'):
                rels_xml = pptx_zip.read(file)
                rels_root = ET.fromstring(rels_xml)
                rellst = {}
                for rel in rels_root.findall('.//{http://schemas.openxmlformats.org/package/2006/relationships}Relationship'):
                    rel_id =rel.attrib.get('Id')
                    target = rel.attrib.get('Target')
                    if not rel_id or not target:
                        rel_id = rel.attrib.get('{http://schemas.openxmlformats.org/package/2006/relationships}Id')
                        target = rel.attrib.get('{http://schemas.openxmlformats.org/package/2006/relationships}Target')
                    if rel_id and target:
                        rellst[rel_id] = target
                relationship_map[Path(file).name] = rellst
        return relationship_map
    
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


    # def get_filecontent(self, get_ocr = False):
    #     CONTROL_TABLE_NAMES = _OTHER_CONTROL_TABLES + (_DOCUMENT_INFO_TABLES if get_ocr else [])
    #     self.fileContent = ''
    #     logger.debug(f"{self.fileid}-->Images collected:- {self.image_processor.image_dict}")
    #     tablerows = False
    #     iscontrol_table = False
    #     tab_data = []
    #     previous_line = ''
    #     listoftabledata = []
    #     for line in self.assembled:
    #         if tablerows:
    #             if line == '--- TABLE END ---':
    #                 if not iscontrol_table:
    #                     # --- Smart table parser logic ---
    #                     # isnotheader = True
    #                     # has_table_data = True
    #                     # while isnotheader:
    #                     #     if len(tab_data) == 0:
    #                     #         isnotheader = False
    #                     #         has_table_data = False
    #                     #     elif (len(tab_data[0]) == 1
    #                     #         or len(list(dict.fromkeys(tab_data[0]))) == 1
    #                     #         or len(tab_data[0]) < sum([len(row) for row in tab_data]) // len(tab_data)):
    #                     #         self.fileContent += '~'.join(list(dict.fromkeys(tab_data.pop(0)))) + '\n'
    #                     #     elif len(tab_data) > 1 and len(tab_data[0]) < len(tab_data[1]):
    #                     #         self.fileContent += '~'.join(list(dict.fromkeys(tab_data.pop(0)))) + '\n'
    #                     #     else:
    #                     #         isnotheader = False
    #                     #         has_table_data = True

    #                     # if has_table_data and len(tab_data) > 1:
    #                     #     if len(list(set(tab_data[0]) & set(tab_data[1]))) > 1:
    #                     #         new_header = [a + b if a != b else a for a, b in zip(tab_data[0], tab_data[1])]
    #                     #         tab_data.pop(0)
    #                     #         tab_data[0] = new_header

    #                     #     max_cols = len(tab_data[0])
    #                     #     padded_data = []
    #                     #     for row in tab_data[1:]:
    #                     #         padded_row = row + [None] * (max_cols - len(row))
    #                     #         padded_data.append(padded_row)
    #                     #     df = pd.DataFrame(padded_data, columns=tab_data[0])
    #                     df = self._table_parser(tab_data)
    #                     if df is None:
    #                         continue

    #                     if df.shape[0] > 0:
    #                         records = df.drop_duplicates().to_dict('records')

    #                         for idx, rec in enumerate(records):
    #                             clean_dict = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in rec.items() if v is not None and v != '' and v != []}
    #                             records[idx] = clean_dict
                            
    #                         if get_ocr:
    #                             listoftabledata.append(records)

    #                         # self.fileContent += previous_line + '\n' + '\n'.join(records) + '\n'
    #                         if previous_line and previous_line != '--- TABLE END ---':
    #                             self.fileContent += previous_line + '\n'
    #                             self.fileContent += '--- TABLE START ---\n'
    #                             self.fileContent += '\n'.join([str(record) for record in records]) + '\n'
    #                             self.fileContent += '--- TABLE END ---\n'

    #                 tablerows = False
    #                 tab_data = []
    #                 previous_line = line
    #                 continue

    #             if not iscontrol_table:
    #                 if line.startswith('[IMAGE:'):
    #                     image_placeholder = self.replace_image_placeholders(
    #                         text=line, image_dict=self.image_processor.image_dict, ocr=get_ocr)
    #                     if len(line.strip()) > 0:
    #                         tab_data.append(list(dict.fromkeys(image_placeholder.split('~'))))
    #                 elif len(line.strip()) > 0:
    #                     tab_data.append(line.split('~'))
    #             continue

    #         if line.startswith('[IMAGE:'):
    #             image_placeholder = self.replace_image_placeholders(
    #                 text=line, image_dict=self.image_processor.image_dict, ocr=get_ocr) + '\n'
    #             if len(image_placeholder) > 3:
    #                 self.fileContent += image_placeholder
    #         else:
    #             if line == '--- TABLE START ---':
    #                 iscontrol_table = previous_line.lower() in CONTROL_TABLE_NAMES
    #                 tab_data = []
    #                 tablerows = True
    #             else:
    #                 if not (previous_line.lower().startswith(tuple(_SUBSTRINGS_TO_SKIP))
    #                         or previous_line.strip() == line.strip()
    #                         or previous_line == '--- TABLE END ---'):
    #                     self.fileContent += previous_line + '\n'
    #                 previous_line = line

    #     self.fileContent = self.replace_image_placeholders(
    #         text=self.fileContent, image_dict=self.image_processor.image_dict, ocr=True, text_only=True)
    #     logger.info(f'{self.fileid}-->Reassembled the file content of len {len(self.fileContent)} with get_ocr = {get_ocr}')
    #     return self.fileContent, listoftabledata
    
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
                return f"[IMAGE: {img_name} | CONTENT: {ocr_text}]"
            else:
                llm_response = img_details.get('llm_response')
                if not llm_response: return ""
                if llm_response.get('category', 'UNKNOWN').upper() in excluded_categories: return ""
            
                description = llm_response.get('description','').strip()
                if len(description)==0: return ""
                return f"[IMAGE: {img_name} | DESCRIPTION: {description}]"
            
        return re.sub(placeholder_pat, _replacer, text, flags=re.IGNORECASE)

    def clean_up(self):
        if self.debug:
            out_text_path = os.path.join(self.OUTPUT_DIR,"filecontent.txt")
            with open(out_text_path, 'w', encoding='utf-8') as outf:
                outf.write(self.fileContent)

        if self.waspdf:
            os.remove(self.filepath)
        
        # if self.OUTPUT_DIR.exists():
        #     try:
        #         shutil.rmtree(self.OUTPUT_DIR)
        #     except PermissionError:
        #         time.sleep(10)  # wait for 1 second
        #         try:
        #             shutil.rmtree(self.OUTPUT_DIR)
        #         except PermissionError as e:
        #             logger.error(f'{self.fileid}-->Failed to delete {self.OUTPUT_DIR}: {e}')

        if self.filepath.parent.exists():
            try:
                shutil.rmtree(self.filepath.parent)
            except PermissionError:
                time.sleep(10)  # wait for 10 seconds
                try:
                    shutil.rmtree(self.filepath.parent)
                except PermissionError as e:
                    logger.error(f"{self.fileid}-->Failed to delete {self.filepath.parent}: {e}", exc_info=True)

    def repair_pptx(self) -> Path:
        tmp_dir = Path(tempfile.mkdtemp())
        repaired_path: Path | None = None

        try:
            # ----------------------------------------------------------
            # 1️⃣  Unzip the original PPTX
            # ----------------------------------------------------------
            with zipfile.ZipFile(self.filepath, "r") as zin:
                zin.extractall(tmp_dir)

            # ----------------------------------------------------------
            # 2️⃣  Remove the most common corrupt parts
            # ----------------------------------------------------------
            # a) presentation.xml – if it cannot be parsed, drop it
            pres_path = tmp_dir / "ppt" / "presentation.xml"
            if pres_path.is_file():
                try:
                    ET.parse(pres_path)
                except ET.ParseError:
                    pres_path.unlink()
                    logger.info(f"{self.fileid} --> Removed corrupted presentation.xml")

            # b) slide XML files (ppt/slides/slide*.xml)
            slides_dir = tmp_dir / "ppt" / "slides"
            if slides_dir.is_dir():
                for slide_xml in slides_dir.glob("slide*.xml"):
                    try:
                        ET.parse(slide_xml)
                    except ET.ParseError:
                        slide_xml.unlink()
                        logger.info(f"{self.fileid} --> Removed corrupted {slide_xml.relative_to(tmp_dir)}")

            # c) slide master XML files – they are analogous to Excel's styles.xml
            masters_dir = tmp_dir / "ppt" / "slideMasters"
            if masters_dir.is_dir():
                for master_xml in masters_dir.glob("slideMaster*.xml"):
                    try:
                        ET.parse(master_xml)
                    except ET.ParseError:
                        master_xml.unlink()
                        logger.info(f"{self.fileid} --> Removed corrupted {master_xml.relative_to(tmp_dir)}")

            # d) (Optional) any stray styles.xml that may appear in the PPTX
            style_path = tmp_dir / "ppt" / "styles.xml"
            if style_path.is_file():
                style_path.unlink()
                logger.info(f"{self.fileid} --> Removed stray styles.xml")

            # ----------------------------------------------------------
            # 3️⃣  Re‑zip the cleaned folder
            # ----------------------------------------------------------
            repaired_path = tmp_dir / self.filepath.name
            with zipfile.ZipFile(repaired_path, "w", zipfile.ZIP_DEFLATED) as zout:
                for file in tmp_dir.rglob("*"):
                    if file.is_file():
                        # Preserve the relative path inside the zip
                        arc_name = file.relative_to(tmp_dir)
                        zout.write(file, arc_name)

            # ----------------------------------------------------------
            # 4️⃣  Atomically replace the original file
            # ----------------------------------------------------------
            try:
                # Prefer true atomic replace (POSIX & Windows 10+)
                os.replace(str(repaired_path), str(self.filepath))
            except Exception:  # Fallback for older platforms
                shutil.move(str(repaired_path), str(self.filepath))

            logger.info(f"{self.fileid} --> Repaired PPTX written to {self.filepath}")

        finally:
            # Clean up the temporary directory – ignore any errors
            try:
                shutil.rmtree(tmp_dir)
            except Exception:
                pass
        