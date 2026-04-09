# core/readers/excelreader.py
from __future__ import annotations

from numpy import average
import os, re, shutil, time, zipfile, uuid, logging, xml.etree.ElementTree as ET, pandas as pd, numpy as np
from pathlib import Path
from typing import Dict, List, Set

from openai import RateLimitError
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN

from core.exceptions import EmptyFileError
from core.imagehandlers.imagehelper import ImageProcessor
from core.utility import get_custom_logger, _WHITE_VARIATIONS, _DOCUMENT_INFO_TABLES, _OTHER_CONTROL_TABLES, _SUBSTRINGS_TO_SKIP
from config import Config as cfg

logger = get_custom_logger(__name__)

def run_dbscan(X, eps=0.5, min_samples=2, scale=True, metric='manhattan'):
    X_use = StandardScaler().fit_transform(X) if scale else X
    model = DBSCAN(eps=eps, min_samples=min_samples, metric= metric)
    model.fit(X_use)
    return model, X_use

class ExcelExtractor:
    """
    Extract plain‑text from an ``.xlsx`` workbook **without using openpyxl**.

    * Cells in the same row are concatenated with the ``~`` character.
    * Rows are separated by a newline (``\\n``).
    * If a cell contains an image, the image file is written to ``self.media_dir`` and a
      placeholder ``[IMAGE:<filename>]`` is inserted in its place.
    """

    # --------------------------------------------------------------------- #
    #  Construction / folder preparation
    # --------------------------------------------------------------------- #
    def __init__(self, filepath: Path, debug: bool = False, analyze_images: bool = False, fileid: uuid.UUID = None):
        self.analyze_images = analyze_images
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)

        self.fileid = fileid
        self.filepath = Path(filepath)

        if not self.filepath.exists():
            raise FileNotFoundError(filepath)

        # ----- output folder (mirrors the original class) -----------------
        self.OUTPUT_DIR = Path(
            os.path.join(
                self.filepath.parent,
                self.filepath.stem.replace("-", "").replace(" ", "_"),
            )
        )
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        self.media_dir = Path(os.path.join(self.OUTPUT_DIR, "xl", "media"))
        if self.media_dir.exists():
            shutil.rmtree(self.media_dir)
        self.media_dir.mkdir(parents=True, exist_ok=True)

        # ----- helper that may be used later for OCR / analysis ----------
        self.image_processor = ImageProcessor(
            media_dir=self.media_dir, fileid=self.fileid, debug=self.debug
        )
        self.ratelimit_exceeded = False

        # ----- will be filled once the zip is opened --------------------
        self._shared_strings: List[str] = []
        self.sheet_cells = {}
        self.assembled = [f"Document: {self.filepath.name}"]
        self._header_styles = set()

    # --------------------------------------------------------------------- #
    #  Private helpers – ZIP / XML handling
    # --------------------------------------------------------------------- #
    def _load_shared_strings(self, zipf: zipfile.ZipFile) -> None:
        """Populate ``self._shared_strings`` from xl/sharedStrings.xml."""
        try:
            with zipf.open("xl/sharedStrings.xml") as f:
                tree = ET.parse(f)
        except KeyError:
            # No shared strings – keep list empty
            self._shared_strings = []
            return

        root = tree.getroot()
        ns = self._get_namespace(root.tag)

        self._shared_strings = [
            "".join(t.itertext()) for t in root.findall(f".//{{{ns}}}si")
        ]

    def _get_sheet_names(self, zipf: zipfile.ZipFile) -> Dict[str, str]:
        """
        Extract sheet names from workbook.xml and map them to sheet file paths.
        
        Returns a dictionary mapping sheet file paths (e.g., "xl/worksheets/sheet1.xml") 
        to user-defined sheet names.
        """
        sheet_name_map = {}
        try:
            with zipf.open("xl/workbook.xml") as f:
                tree = ET.parse(f)
        except KeyError:
            # No workbook.xml – return empty mapping
            return sheet_name_map

        root = tree.getroot()
        ns = self._get_namespace(root.tag)

        # Find all sheet elements
        for sheet in root.findall(f".//{{{ns}}}sheet"):
            sheet_name = sheet.get("name", "")
            sheet_id = sheet.get("sheetId", "")
            r_id = sheet.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id", "")
            
            if sheet_name and r_id:
                # Map the relationship ID to the actual sheet file path
                sheet_path = self._resolve_sheet_relationship(zipf, r_id)
                if sheet_path:
                    sheet_name_map[sheet_path] = sheet_name

        return sheet_name_map

    def _resolve_sheet_relationship(self, zipf: zipfile.ZipFile, r_id: str) -> str:
        """
        Resolve a relationship ID to a sheet file path using workbook.rels.
        
        Returns the sheet file path (e.g., "xl/worksheets/sheet1.xml") or empty string if not found.
        """
        try:
            with zipf.open("xl/_rels/workbook.xml.rels") as f:
                tree = ET.parse(f)
        except KeyError:
            return ""

        root = tree.getroot()
        ns = self._get_namespace(root.tag)

        for rel in root.findall(f".//{{{ns}}}Relationship"):
            if rel.get("Id") == r_id:
                target = rel.get("Target", "")
                if target.startswith("worksheets/"):
                    return f"xl/{target}"
        return ""

    @staticmethod
    def _get_namespace(tag: str) -> str:
        """Extract the XML namespace from a tag like '{ns}name'."""
        if tag[0] == "{":
            return tag[1:].split("}")[0]
        return ""

    def _load_styles(self, zipf: zipfile.ZipFile) -> None:
        """Load style information from xl/styles.xml to identify header formatting."""
        try:
            with zipf.open("xl/styles.xml") as f:
                tree = ET.parse(f)
        except KeyError:
            # No styles.xml - no formatting information available
            self._header_styles = set()
            return
    
        root = tree.getroot()
        ns = self._get_namespace(root.tag)

        # Parse fonts to identify bold fonts
        bold_font_indices = set()
        fonts = root.find(f".//{{{ns}}}fonts")
        if fonts is not None:
            for idx, font in enumerate(fonts.findall(f".//{{{ns}}}font")):
                if font.find(f".//{{{ns}}}b") is not None:  # Bold font
                    bold_font_indices.add(idx)

        # Parse fills to identify background colors
        colored_fill_indices = set()
        fills = root.find(f".//{{{ns}}}fills")
        if fills is not None:
            for idx, fill in enumerate(fills.findall(f".//{{{ns}}}fill")):
                pattern_fill = fill.find(f".//{{{ns}}}patternFill")
                if pattern_fill is not None:
                    fg_color = pattern_fill.find(f".//{{{ns}}}fgColor")
                    if fg_color is not None:  # Has foreground color
                         # Check if color is not white
                        rgb = fg_color.get("rgb")
                        if rgb is None:
                            continue
                        # print(idx,'-',rgb)
                        is_white = False
                        # Check for white color (FFFFFF or variations)
                        rgb_clean = rgb[-6:] if len(rgb) == 8 else rgb  # Remove alpha if present
                        rgb_clean = rgb_clean.upper()
                        # print(idx,'-rgbclean-',rgb_clean)
                        # Check if the color is in the white variations list
                        if rgb_clean in _WHITE_VARIATIONS:
                            is_white = True
                        else:
                            # Additional check: if the RGB values are very close to white (high values)
                            # Convert hex to RGB and check if all components are > 240 (near white)
                            try:
                                r = int(rgb_clean[0:2], 16)
                                g = int(rgb_clean[2:4], 16)
                                b = int(rgb_clean[4:6], 16)
                                if r > 240 and g > 240 and b > 240:
                                    is_white = True
                                # print(idx,'-rgbclean-',[r,g,b])
                            except ValueError:
                                pass  # Invalid hex format, keep is_white as False

                        if not is_white and fg_color.get("theme","99999") == "0":  # Theme color 0 is often white
                            is_white = True
    
                        if not is_white:
                            colored_fill_indices.add(idx)
        # print('bold--->',bold_font_indices)
        # print('fill--->',colored_fill_indices)
        # Parse cell style formats (xf) to identify header styles
        self._header_styles = set()
        cell_xfs = root.find(f".//{{{ns}}}cellXfs")
        if cell_xfs is not None:
            for idx, xf in enumerate(cell_xfs.findall(f".//{{{ns}}}xf")):
                
                # Check if this style has header characteristics
                is_header_style = []

                # Check for bold font
                font_idx = xf.get("fontId")
                if font_idx and int(font_idx) in bold_font_indices:
                    is_header_style.append(True)
                else:
                    is_header_style.append(False)

                # Check for background color
                fill_idx = xf.get("fillId")
                if fill_idx and int(fill_idx) in colored_fill_indices:
                    is_header_style.append(True)
                else:
                    is_header_style.append(False)

                # Check for center alignment
                alignment = xf.find(f".//{{{ns}}}alignment")
                if alignment is not None and alignment.get("horizontal") == "center":
                    is_header_style.append(True)
                else:
                    is_header_style.append(False)

                # if sum(is_header_style)>1:
                if ((is_header_style[0] == True and any(is_header_style[1:])) or (is_header_style[1] == True and any(is_header_style[0::2]))):
                    self._header_styles.add(idx)

                # if idx in (5,8,9):
                #     print(f"{idx} Bold:{font_idx}")
                #     print(f"{idx} fill:{fill_idx}")
                #     print(f"{idx} alignment:{alignment.text}")
                #     print(f"{idx} Header styles: {self._header_styles}")        
    
    def _is_header_cell(self, style_idx: str, cell_value: str = "") -> bool:
        """Determine if a cell is likely a header based on its formatting."""
        if not style_idx or not hasattr(self, '_header_styles'):
            return False

        try:
            idx = int(style_idx)
            return (idx in self._header_styles) and len(cell_value.split())<=3
        except (ValueError, AttributeError):
            return False

    def _parse_sheet(self, zipf: zipfile.ZipFile, sheet_path: str) -> Dict[str, str]:
        # -----------------------------------------------------------------
        # 1️⃣ Load the sheet XML
        # -----------------------------------------------------------------
        with zipf.open(sheet_path) as f:
            tree = ET.parse(f)
        root = tree.getroot()
        ns = self._get_namespace(root.tag)
    
        # -----------------------------------------------------------------
        # 2️⃣ Determine hidden columns (if any)
        # -----------------------------------------------------------------
        hidden_cols = set()
        for col in root.findall(f".//{{{ns}}}col"):
            if col.get("hidden") == "1":
                min_idx = int(col.get("min", "1"))
                max_idx = int(col.get("max", str(min_idx)))
                for idx in range(min_idx, max_idx + 1):
                    hidden_cols.add(self._col_idx_to_letter(idx))
    
        # -----------------------------------------------------------------
        # 3️⃣ Parse merged‑cell ranges – we will fill a dict that maps every
        #     cell coordinate inside a merged region to the *value* of the
        #     top‑left cell of that region.
        # -----------------------------------------------------------------
        merge_elem = root.find(f".//{{{ns}}}mergeCells")
        mergedcells_value_cord : Dict[str, str] = {}
        if merge_elem is not None:
            for mc in merge_elem.findall(f".//{{{ns}}}mergeCell"):
                ref = mc.get("ref")                     # e.g. "A1:C1"
                if not ref:
                    continue
                start, end = ref.split(":")
                start_col, start_row = re.match(r"([A-Z]+)(\d+)", start).groups()
                end_col,   end_row   = re.match(r"([A-Z]+)(\d+)", end).groups()
                start_col_idx = self._col_letter_to_idx(start_col)
                end_col_idx   = self._col_letter_to_idx(end_col)
                start_row_i = int(start_row)
                end_row_i   = int(end_row)
    
                for r in range(start_row_i, end_row_i + 1):
                    for c in range(start_col_idx, end_col_idx + 1):
                        coord = f"{self._col_idx_to_letter(c)}{r}"
                        if coord!=start:
                            mergedcells_value_cord[coord] = start
        # -----------------------------------------------------------------
        # 4️⃣ Build the image map (anchored + un‑anchored drawings)
        # -----------------------------------------------------------------
        image_map = self._extract_images_for_sheet(zipf, sheet_path)
    
        # Keep track of which image coordinates were actually used – needed
        # for requirement 3 (orphan images).
        used_image_coords: Set[str] = set()

        cellvalue = {}
        for row in root.findall(f".//{{{ns}}}row"):
            # -----------------------------------------------------------------
            # 5.1 Skip hidden rows
            # -----------------------------------------------------------------
            # if row.get("hidden") == "1":
            #     continue
            
            for c in row.findall(f".//{{{ns}}}c"):
                coord = c.get("r")          # e.g. "B3"
                if not coord:
                    continue
                
                # -------------------------------------------------------------
                # 5.2 Image placeholder – takes precedence
                # -------------------------------------------------------------
                if coord in image_map:
                    cellvalue[coord] = {'value': image_map[coord], 'is_header': False}
                    used_image_coords.add(coord)
                    continue
                
                # -------------------------------------------------------------
                # 5.3 Hidden columns – ignore the cell entirely
                # -------------------------------------------------------------
                col_letter = "".join(filter(str.isalpha, coord))
                if col_letter in hidden_cols:
                    continue
                
                # -------------------------------------------------------------
                # 5.4 Resolve the cell value (shared‑string, inlineStr, etc.)
                # -------------------------------------------------------------
                cell_type = c.get("t")      # 's' = shared string, 'inlineStr', etc.
                style_idx = c.get("s")      # Style index for header detection
                value = ""
    
                if cell_type == "inlineStr":
                    is_elem = c.find(f".//{{{ns}}}is")
                    if is_elem is not None:
                        value = "".join(is_elem.itertext())
                else:
                    v_elem = c.find(f".//{{{ns}}}v")
                    if v_elem is not None:
                        raw = v_elem.text or ""
                        if cell_type == "s":            # shared‑string index
                            try:
                                idx = int(raw)
                                value = self._shared_strings[idx]
                            except (ValueError, IndexError):
                                value = raw
                        else:
                            value = raw

                # if not value and coord in mergedcells_value_cord:
                if (not value and coord in mergedcells_value_cord and mergedcells_value_cord[coord] in cellvalue): 
                    # value = cellvalue.get(mergedcells_value_cord[coord])
                     # Get the value from the merged cell and preserve its header status
                    merged_coord = mergedcells_value_cord[coord]
                    if merged_coord in cellvalue:
                        value = cellvalue[merged_coord]['value']
                        # Use the same header status as the source cell
                        is_header = cellvalue[merged_coord]['is_header']
                    else:
                        is_header = False
                else:
                    is_header = self._is_header_cell(style_idx) 

                if value:
                    cellvalue[coord] = {'value': str(value).strip(),
                                        'is_header': is_header}

                # if value in ('1','2','3','4','198.18.203.185-198.18.203.190'):
                #     print(f"{style_idx}: {cellvalue[coord]}")

        # -----------------------------------------------------------------
        # 6️⃣ Add blank cells for the full range
        # -----------------------------------------------------------------
        if cellvalue:
            # Determine the full range of the sheet
            all_coords = list(cellvalue.keys())
            min_row = min(int(re.match(r"([A-Z]+)(\d+)", coord).groups()[1]) for coord in all_coords)
            max_row = max(int(re.match(r"([A-Z]+)(\d+)", coord).groups()[1]) for coord in all_coords)
            min_col = min(self._col_letter_to_idx(re.match(r"([A-Z]+)(\d+)", coord).groups()[0]) for coord in all_coords)
            max_col = max(self._col_letter_to_idx(re.match(r"([A-Z]+)(\d+)", coord).groups()[0]) for coord in all_coords)

            # Add blank cells for missing coordinates in the range
            for row_idx in range(min_row, max_row + 1):
                for col_idx in range(min_col, max_col + 1):
                    coord = f"{self._col_idx_to_letter(col_idx)}{row_idx}"
                    if coord not in cellvalue:
                        cellvalue[coord] = {'value': '', 'is_header': False}

        # -----------------------------------------------------------------
        # 7️⃣ Orphan‑image placeholders – images whose coordinates never
        #     matched a cell (e.g., floating drawings).  Append them after the
        #     last block.
        # -----------------------------------------------------------------
        for coord, imagedata in image_map.items():
            if coord not in used_image_coords:
                cellvalue[coord]={'value': imagedata,'is_header': False}

        logger.info(f"{self.fileid}-->Extracted extracted all cells from {Path(sheet_path).name}")
        return cellvalue
    

    @staticmethod
    def _col_idx_to_letter(idx: int) -> str:
        """Convert 1‑based column index to Excel letter (A, B, …, AA, …)."""
        result = ""
        while idx:
            idx, rem = divmod(idx - 1, 26)
            result = chr(65 + rem) + result
        return result
    
    @staticmethod
    def _col_letter_to_idx(col_letter: str) -> int:
        if not col_letter or not col_letter.isalpha():
            raise ValueError(f"Invalid column letter: {col_letter!r}")

        col_letter = col_letter.upper()
        idx = 0
        for char in col_letter:
            # Convert 'A' → 1, 'B' → 2, …, 'Z' → 26
            idx = idx * 26 + (ord(char) - ord('A') + 1)
        return idx

    def _describe_image(self,imgpath: Path):
        self.image_processor.get_ocr_text(imgpath)
        image_info = f"[IMAGE:{imgpath.name}]"
        if self.analyze_images and not self.ratelimit_exceeded:
            try:
                self.image_processor.analyze_image(imgpath,image_size_threshold=10000)
            except RateLimitError as e:
                logger.warning(f"{self.fileid}-->Rate limit exhausted for API calls: {e}",exc_info=True)
                self.ratelimit_exceeded = True      
            except Exception as e:
                logger.warning(f"{self.fileid}-->Image analysis failed for {imgpath}: {e}",exc_info=True)
        
        return image_info
    
    # -----------------------------------------------------------------
    #  Image extraction helpers
    # -----------------------------------------------------------------
    def _extract_images_for_sheet(
        self, zipf: zipfile.ZipFile, sheet_path: str
    ) -> Dict[str, str]:
        """
        Return a mapping ``cell_coordinate -> saved_image_filename`` for the given sheet.

        The OOXML relationship chain is:
        * xl/worksheets/sheetX.xml  →  xl/worksheets/_rels/sheetX.xml.rels
        * that .rels file maps a ``rId`` to a drawing file (e.g. xl/drawings/drawing1.xml)
        * the drawing XML contains ``<xdr:twoCellAnchor>`` / ``<xdr:oneCellAnchor>``
          elements that point to an image file and a start cell (e.g. "B3").

        All image files live in ``xl/media/``; they are extracted to ``self.media_dir``.
        """
        image_map: Dict[str, str] = {}

        # -----------------------------------------------------------------
        # 1️⃣ Locate the .rels file for this sheet
        # -----------------------------------------------------------------
        sheet_rel_path = sheet_path.replace(
            "worksheets/", "worksheets/_rels/"
        ) + ".rels"
        try:
            with zipf.open(sheet_rel_path) as f:
                rels_tree = ET.parse(f)
        except KeyError:
            return image_map  # No relationships → no images

        rels_root = rels_tree.getroot()
        rels_ns = self._get_namespace(rels_root.tag)

        # -----------------------------------------------------------------
        # 2️⃣ Find drawing relationship(s)
        # -----------------------------------------------------------------
        drawing_targets = []
        for rel in rels_root.findall(f".//{{{rels_ns}}}Relationship"):
            if rel.get("Type", "").endswith(
                "/drawing"
            ):  # type = http://schemas.openxmlformats.org/officeDocument/2006/relationships/drawing
                target = rel.get("Target")
                if target:
                    # The target is relative to the worksheet folder
                    drawing_targets.append(
                        os.path.normpath(
                            os.path.join(
                                os.path.dirname(sheet_path),  # e.g. xl/worksheets/
                                target
                            )
                        ).replace('\\','/')
                    )
        # -----------------------------------------------------------------
        # 3️⃣ For each drawing, parse anchors → image files
        # -----------------------------------------------------------------
        for drawing_path in drawing_targets:
            try:
                with zipf.open(drawing_path) as f:
                # with zipf.open("xl\\drawings\\drawing1.xml") as f:
                    dr_tree = ET.parse(f)
            except KeyError:
                continue

            dr_root = dr_tree.getroot()
            dr_ns = self._get_namespace(dr_root.tag)

            # Mapping between picture id (r:embed) and image file name
            pic_id_to_file: Dict[str, str] = {}

            # <xdr:pic> → <a:blip r:embed="rId5"/>
            for pic in dr_root.findall(f".//{{{dr_ns}}}pic"):
                blip = pic.find(
                    f".//{{{dr_ns}}}blip"
                )  # the blip node lives in the same namespace in most files
                if blip is None:
                    # Some files use a different namespace for the blip element
                    blip = pic.find(".//{http://schemas.openxmlformats.org/drawingml/2006/main}blip")
                if blip is None:
                    continue
                embed = blip.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed")
                if not embed:
                    continue

                # Resolve the embed id to an actual image file via the drawing’s .rels
                # drawing rels are stored in xl/drawings/_rels/<drawing>.xml.rels
                drawing_rel_path = (
                    os.path.dirname(drawing_path)
                    + "/_rels/"
                    + os.path.basename(drawing_path)
                    + ".rels"
                )
                try:
                    with zipf.open(drawing_rel_path) as rel_f:
                        dr_rels = ET.parse(rel_f)
                except KeyError:
                    continue

                dr_rels_root = dr_rels.getroot()
                dr_rels_ns = self._get_namespace(dr_rels_root.tag)

                for rel in dr_rels_root.findall(f".//{{{dr_rels_ns}}}Relationship"):
                    if rel.get("Id") == embed:
                        target = rel.get("Target")
                        if target:
                            # Target is something like "../media/image1.png"
                            img_path = os.path.normpath(
                                os.path.join(os.path.dirname(drawing_path), target)
                            ).replace('\\','/')
                            # Strip any leading "../" parts to get the path inside the zip
                            img_path = img_path.replace("../", "")
                            pic_id_to_file[embed] = os.path.basename(img_path)
                        break

            # -----------------------------------------------------------------
            # 4️⃣ Walk anchors to map cell coordinate → picture id → filename
            # -----------------------------------------------------------------
            for anchor in dr_root.findall(f".//{{{dr_ns}}}twoCellAnchor") + dr_root.findall(
                f".//{{{dr_ns}}}oneCellAnchor"
            ):
                # Starting cell
                from_elem = anchor.find(f".//{{{dr_ns}}}from")
                if from_elem is None:
                    continue
                col_elem = from_elem.find(f".//{{{dr_ns}}}col")
                row_elem = from_elem.find(f".//{{{dr_ns}}}row")
                if col_elem is None or row_elem is None:
                    continue

                col_idx = int(col_elem.text) + 1  # zero‑based → 1‑based
                row_idx = int(row_elem.text) + 1
                cell_coord = (
                    self._col_idx_to_letter(col_idx) + str(row_idx)
                )  # e.g. "B3"

                # Find the picture id inside this anchor
                # logger.info(ET.tostring(anchor, encoding='unicode'))
                blip = anchor.find(f".//{{{dr_ns}}}blip")
                if blip is None:
                    # In some files the blip lives under <pic><blip>
                    blip = anchor.find(
                        f".//{{{dr_ns}}}pic/{{{dr_ns}}}blip"
                    )
                if blip is None:
                    blip = anchor.find(f".//{{http://schemas.openxmlformats.org/drawingml/2006/main}}blip")
                if blip is None:
                    blip = anchor.find(
                        f".//{{http://schemas.openxmlformats.org/drawingml/2006/main}}pic/{{{dr_ns}}}blip"
                    )
                if blip is None:
                    continue
                embed = blip.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed")
                if not embed:
                    continue
                img_filename = pic_id_to_file.get(embed)
                if not img_filename:
                    continue

                # -----------------------------------------------------------------
                # 5️⃣ Persist the image file (if not already done)
                # -----------------------------------------------------------------
                img_zip_path = f"xl/media/{img_filename}"
                out_path = self.media_dir / img_filename
                if not out_path.exists():
                    try:
                        with zipf.open(img_zip_path) as src, open(out_path, "wb") as dst:
                            shutil.copyfileobj(src, dst)
                    except KeyError:
                        logger.warning(
                            f"{self.fileid}-->Image file {img_zip_path} listed in drawing but not found in zip",
                            exc_info=True,
                        )
                        continue

                # Record the placeholder mapping
                # image_map[cell_coord] = img_filename
                image_map[cell_coord] = self._describe_image(out_path)
        logger.debug(f"Images maps found {image_map}")
        return image_map
        
    # --------------------------------------------------------------------- #
    #  Public API
    # --------------------------------------------------------------------- #
    def extract_content(self) -> str:
        """
        Return the whole workbook as a single string:

        * Cells are separated by ``~``.
        * Rows are separated by new‑lines.
        * Image cells are replaced by ``[IMAGE:<filename>]`` (the image is saved
          to ``self.media_dir``).
        """
        sheet_cells = {}
        with zipfile.ZipFile(self.filepath, "r") as zipf:
            # Load shared strings once – many sheets reference them
            self._load_shared_strings(zipf)
            
            # Load styles for header detection
            self._load_styles(zipf)

            # Get sheet names from workbook.xml
            sheet_name_map = self._get_sheet_names(zipf)

            # -----------------------------------------------------------------
            # Iterate over every worksheet XML file
            # -----------------------------------------------------------------
            sheet_files = [
                name
                for name in zipf.namelist()
                if name.startswith("xl/worksheets/") and name.endswith(".xml")
            ]
            
            for sheet_path in sheet_files:
                cellvalues = self._parse_sheet(zipf, sheet_path)
                if cellvalues:
                    # Use user-defined sheet name if available, otherwise fallback to filename
                    sheet_name = sheet_name_map.get(sheet_path, Path(sheet_path).name)
                    sheet_cells[sheet_name] = cellvalues
        self._assemble_rows(sheet_cells)
        
        # if len(self.assembled)<2:
        #     raise EmptyFileError()

        content_text = "\n".join(self.assembled).strip()
        if len(content_text) < 50:
            raise EmptyFileError()

    def calculate_adaptive_eps(self, sheetdf):
        """Calculate adaptive eps based on data density"""
        coords = sheetdf[sheetdf['value'].str.strip() != ''][['col', 'row']].values

        if len(coords) < 2:
            return 1.5

        # Calculate nearest neighbor distances
        from sklearn.neighbors import NearestNeighbors
        nbrs = NearestNeighbors(n_neighbors=2, metric='manhattan').fit(coords)
        distances, _ = nbrs.kneighbors(coords)

        # Use median of nearest neighbor distances as eps
        eps = np.median(distances[:, 1]) * 1.5  # 1.5x the median distance

        # Ensure eps is at least 1.0 to handle close tables
        eps = max(eps, 1.0)

        return eps

    def optimize_clustering(self,sheetdf):
        """Optimized clustering for table detection"""
        # Calculate adaptive eps
        eps = self.calculate_adaptive_eps(sheetdf)
        # Use smaller eps for tighter clustering
        # create column to identify blank columns
        # sheetdf['is_blank'] = sheetdf['value'].str.strip().eq('').astype(int)
        model, _ = run_dbscan(sheetdf[['col','row']].values, eps=eps, min_samples=1, scale=False, metric='manhattan')
        sheetdf['cluster'] = model.labels_
        sheetdf = sheetdf.astype({'col':int,'row':int,'cluster':int})
        sheetdf.sort_values(['cluster','row','col'],inplace=True)
        return sheetdf

    def _assemble_rows(self,sheet_cells: Dict[str, Dict[str, str]]) -> None:
        self.sheetdict = {}
        self.sheetdictout = {}
        for sheetname, sheetcells in sheet_cells.items():
            self.sheetdictout['sheetname'] = sheetcells
            self.assembled.append(f"=== {sheetname} ===")
            sheetdf = pd.DataFrame([(coord,cell_data['value'], cell_data['is_header']) for coord,cell_data in sheetcells.items()],columns=('coord','value', 'is_header'))
            sheetdf = pd.concat([sheetdf,sheetdf['coord'].str.extract(r'([A-Z]+)(\d+)', expand=True)],axis=1).rename(columns={0:'col',1:'row'})
            sheetdf['col'] = sheetdf['col'].apply(self._col_letter_to_idx)
            sheetdf['row'] = sheetdf['row'].astype(int)
            # Remove rows where all values are blank
            rows_with_data = sheetdf.groupby('row')['value'].apply(lambda x: x.str.strip().ne('').any())
            valid_rows = rows_with_data[rows_with_data].index
            sheetdf = sheetdf[sheetdf['row'].isin(valid_rows)]

            # Remove columns where all values are blank
            cols_with_data = sheetdf.groupby('col')['value'].apply(lambda x: x.str.strip().ne('').any())
            valid_cols = cols_with_data[cols_with_data].index
            sheetdf = sheetdf[sheetdf['col'].isin(valid_cols)]

            # Remove empty rows above headers
            headercolsdf = sheetdf[sheetdf['is_header']]
            if headercolsdf.shape[0] > 0:
                headercolsdf = headercolsdf.groupby('row').apply(lambda x: x['col'].tolist()).reset_index()
                EmptyRowsAbove = []
                for norows in range(1,4):
                    EmptyRowslst = headercolsdf.apply(lambda x: (x['row']-norows, x[0]) if x['row']-norows>0 and sheetdf[(sheetdf['row'] == x['row']-norows) & (sheetdf['col'].isin(x[0]))]['value'].str.strip().eq('').all() else None,axis=1).dropna().to_list()
                    EmptyRowsAbove.extend(EmptyRowslst)
                for row in EmptyRowsAbove:
                    sheetdf = sheetdf[~((sheetdf['row']==row[0]) & (sheetdf['col'].isin(row[1])))]

            # Cluster closest cells together
            sheetdf = self.optimize_clustering(sheetdf)
            self.sheetdict[sheetname]=sheetdf

            #extract horizontal tables
            ishorizontal = sheetdf.groupby(['cluster','col'])[['value','is_header']].apply(lambda x: all(x['is_header']) and len(x['value'].drop_duplicates())==len(x['is_header'])).reset_index()
            horizontal_cluster = ishorizontal[ishorizontal[0]].cluster.drop_duplicates().to_list()
            isvertical = sheetdf.groupby(['cluster','row'])[['value','is_header']].apply(lambda x: all(x['is_header']) and len(x['value'].drop_duplicates())==len(x['is_header'])).reset_index()
            vertical_cluster = isvertical[isvertical[0]].cluster.drop_duplicates().to_list()
            horizontal_cluster = [cl for cl in horizontal_cluster if cl not in vertical_cluster]
            if len(horizontal_cluster)>0:
                horizontal_tables_df = sheetdf[sheetdf['cluster'].isin(horizontal_cluster)]   
                horizontal_tables_df = horizontal_tables_df.groupby(['cluster','col'])[['value','is_header']].apply(lambda x: {'rows': '~'.join(x['value']),'rowtype': 'HDR' if sum(x['is_header']) > len(x['is_header']) - sum(x['is_header']) else 'VAL'}).reset_index()
                new_cols = pd.json_normalize(horizontal_tables_df[0])
                horizontal_tables_df = pd.concat([horizontal_tables_df.drop(0, axis=1), new_cols], axis=1)
                horizontal_tables_df['rows'] = horizontal_tables_df['rows'].apply(lambda x: x if x.replace('~', '') != '' else None)
                horizontal_tables_df.dropna(subset=['rows'], inplace=True)
                horizontal_tables_df['rows'] = horizontal_tables_df['rowtype']+':'+horizontal_tables_df['rows']
                horizontal_tables_df.drop(columns=['col','rowtype'],inplace=True)
                self.assembled.extend(horizontal_tables_df.groupby('cluster').agg(lambda x: '--- TABLE START ---~|~'+('~|~'.join(x))+'~|~--- TABLE END ---~|~' 
                                                               if len(x)>2 else '~|~'.join(x))['rows'].reset_index(drop=True).str.split('\~\|\~').explode().to_list())

                sheetdf = sheetdf[~sheetdf['cluster'].isin(horizontal_cluster)]    

            if sheetdf.shape[0]>0:   
                sheetdf = sheetdf.groupby(['cluster','row'])[['value','is_header']].apply(lambda x: {'rows': '~'.join(x['value']),'rowtype': 'HDR' if sum(x['is_header']) > len(x['is_header']) - sum(x['is_header']) else 'VAL'}).reset_index()
                new_cols = pd.json_normalize(sheetdf[0])
                sheetdf = pd.concat([sheetdf.drop(0, axis=1), new_cols], axis=1)
                sheetdf['rows'] = sheetdf['rows'].apply(lambda x: x if x.replace('~', '') != '' else None)
                sheetdf.dropna(subset=['rows'], inplace=True)
                sheetdf['rows'] = sheetdf['rowtype']+':'+sheetdf['rows']
                sheetdf.drop(columns=['row','rowtype'],inplace=True)
                self.assembled.extend(sheetdf.groupby('cluster').agg(lambda x: '--- TABLE START ---~|~'+('~|~'.join(x))+'~|~--- TABLE END ---~|~' 
                                                                   if len(x)>2 else '~|~'.join(x))['rows'].reset_index(drop=True).str.split('\~\|\~').explode().to_list())

            logger.info(f"{self.fileid}-->Clustered and joined data in sheet {sheetname}")

        # self.assembled = list(set(self.assembled))
    def _table_parser(self,datarows):
        datarows = [line[4:] for line in datarows]
        datarows = ['emptycell' + line  if line.startswith('~') else line for line in datarows]
        datarows = [line.split('~') for line in datarows]
        isnotheader = True
        has_table_data = True
        while isnotheader:
            if len(datarows) == 0:
                isnotheader =  False
                has_table_data = False
            elif "" in datarows[0] or len(datarows[0]) == 1 or len(list(dict.fromkeys(datarows[0]))) == 1 or len(datarows[0]) < sum([len(row) for row in datarows])//len(datarows) :
                self.fileContent += '~'.join(list(dict.fromkeys(datarows.pop(0)))).strip('~ ') + '\n'
            elif len(datarows)>1:
                if len(datarows[0]) < len(datarows[1]):
                    self.fileContent += '~'.join(list(dict.fromkeys(datarows.pop(0)))).strip('~ ') + '\n'
                else:
                    isnotheader =  False
                    has_table_data = True
            elif len(datarows) == 1:
                self.fileContent += '~'.join(list(dict.fromkeys(datarows.pop(0)))).strip('~ ') + '\n'
                isnotheader =  False
                has_table_data = False
            else:
                isnotheader =  False
                has_table_data = True
        if not has_table_data:
            return None

        if len(list(set(datarows[0]) & set(datarows[1])))>1:
            new_header = [a + b if a!=b else a for a, b in zip(datarows[0], datarows[1])]
            datarows.pop(0)
            datarows[0] = new_header
        
        header_counts = {}
        new_header = []

        for col in datarows[0]:
            if col in header_counts:
                header_counts[col] += 1
                new_col_name = f"{col}_dup_{header_counts[col]}"
                new_header.append(new_col_name)
            else:
                header_counts[col] = 0
                new_header.append(col)

        datarows[0] = new_header

        max_cols = len(datarows[0])
 
        # Pad each row to match the header length
        padded_data = []
        for row in datarows[1:]:
            padded_row = row + [None] * (max_cols - len(row))
            padded_data.append(padded_row)
        
        # # Check for unusual condition and fix column mismatch
        # max_data_cols = max([len(row) for row in padded_data]) if padded_data else max_cols
        
        # if max_cols != max_data_cols:
        #     # Normal mismatch handling (original logic)
        #     if max_cols < max_data_cols:
        #         # Header is shorter, pad header to match data
        #         datarows[0] = datarows[0] + [None] * (max_data_cols - max_cols)
        #         max_cols = max_data_cols
        #         # Re-pad all data rows to match the new header length
        #         padded_data = []
        #         for row in datarows[1:]:
        #             padded_row = row + [None] * (max_cols - len(row))
        #             padded_data.append(padded_row)
                
        return(pd.DataFrame(padded_data, columns=datarows[0]))
        
    def get_filecontent(self, get_ocr = False):
        self.fileContent = "" #self.filepath.name
        logger.debug(f"{self.fileid}-->Images collected:- {self.image_processor.image_dict}")
        listoftabledata =[]
        istablerow = False
        for line in self.assembled:
            # if line[4:].startswith('[IMAGE:'):
            if '[IMAGE:' in line[4:]:
                image_placeholder = self.replace_image_placeholders(text=line[4:], image_dict=self.image_processor.image_dict,ocr=get_ocr)  + '\n'
                if len(image_placeholder)>3:
                    if istablerow and len(image_placeholder.replace('~','').strip())>0:
                        # tabledata.append(list(dict.fromkeys(image_placeholder.split('~'))))
                        tabledata.append(line[:4]+image_placeholder.strip('~ '))
                    elif len(image_placeholder.replace('~','').strip())>0:
                        self.fileContent += '~'.join(list(dict.fromkeys(image_placeholder.split('~')))).strip('~ ') + '\n'
            elif line == '--- TABLE START ---':
                istablerow = True
                tabledata = []
            elif line == '--- TABLE END ---':
                df = self._table_parser(tabledata)
                if df is None:
                    istablerow = False
                    continue
                if df.shape[0]>0:
                    records = df.drop_duplicates().to_dict('records')
                    
                    for idx, rec in enumerate(records):
                        clean_dict = {k: v for k, v in rec.items() if v is not None and v != '' and v != []}
                        records[idx] = clean_dict
                    
                    if get_ocr:
                        listoftabledata.append(records)
                    self.fileContent += '--- TABLE START ---\n'
                    self.fileContent += '\n'.join([str(record) for record in records]) + '\n'
                    self.fileContent += '--- TABLE END ---\n'
                istablerow = False
            else:
               if istablerow and len(line.strip())>0:
                   tabledata.append(line)
               else:
                    if line[:4] in ['HDR:','VAL:']:
                        line = '~'.join(list(dict.fromkeys(line[4:].split('~')))).strip('~ ')
                    self.fileContent += line + '\n'

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
                ocr_text = img_details.get("ocr_text","").strip()
                if ocr_text is None: return ""
                if len(ocr_text)==0: return ""
                if text_only: return ocr_text.strip()
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

        # if self.waspdf:
        #     os.remove(self.filepath)

        if self.filepath.parent.exists():
            try:
                shutil.rmtree(self.filepath.parent)
            except PermissionError:
                time.sleep(10)  # wait for 10 seconds
                try:
                    shutil.rmtree(self.filepath.parent)
                except PermissionError as e:
                    logger.error(f"{self.fileid}-->Failed to delete {self.filepath.parent}: {e}", exc_info=True)
        