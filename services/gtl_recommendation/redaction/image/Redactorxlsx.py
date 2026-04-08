import os, logging, shutil, zipfile, jpype, uuid
# import pathlib
from xml.etree import ElementTree as ET
from pathlib import Path
from typing import List, Dict
# from PIL import Image as PILImage
# import openpyxl
# from aspose.imaging import Image as AsposeImage
# from aspose.imaging.imageoptions import PngOptions
from asposediagram.api import *
from services.gtl_recommendation.sensitive_text_ext.extractor import TextExtractor
from core.imagehandlers.imagehelper import ImageProcessor
from config import Config as cfg
from core.utility import get_custom_logger
# from dotenv import load_dotenv
logger = get_custom_logger(__name__)

logging.getLogger().setLevel(logging.INFO)
if not jpype.isJVMStarted():
    jpype.startJVM()


class ExcelRedactor:
    def __init__(self,filepath: Path,fileid: uuid = None, debug: bool = False,analyze_images: bool = False):
        self.filepath = filepath
        self.fileid = fileid
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.filename = filepath.name
        self.records: List[Dict] = []
        self.filecontent: str = ''

        self.analyze_images = analyze_images
        self.out_dir = Path(
            os.path.join(filepath.parent, filepath.stem.replace('-', '').replace(' ', '_'))
        )
        if self.out_dir.exists():
            shutil.rmtree(self.out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.media_dir=Path(os.path.join(self.out_dir,"xl","media"))
        self.processing_dir = Path(os.path.join(self.media_dir,'processing'))

    def extract_images_from_excel(self):
        try:
            with zipfile.ZipFile(self.filepath, 'r') as zip_file:
                zip_file.extractall(self.out_dir)
            logger.info(f"{self.fileid}-->Extracted XLSX to: {self.out_dir}")

            if not (self.media_dir.exists() and any(self.media_dir.iterdir())):
                logger.info(f'{self.fileid}-->No Media File present in XLSX')
                return False
            return True
        except Exception as e:
            logger.warning(f"{self.fileid}-->ERROR extracting DOCX: {e}",exc_info=True)
            return False
        
    
    def redact(self, drawlabel: bool = False, tobe_redacted: List[dict] = None, imgprocessor: ImageProcessor = None, **kwargs) -> None:
        try:

            if not self.extract_images_from_excel():
                return False
            
            if imgprocessor is not None:
                self.imgprop = imgprocessor
            else:
                self.imgprop = ImageProcessor(media_dir=self.media_dir, correlationid=cfg.CORR_ID_REDACTION, fileid=self.fileid, debug=cfg.DEBUG)

            self.imgprop.set_directories(media_dir = self.media_dir)
            self.imagewatermarked = False
            if self.analyze_images:
                self.results, self.imagewatermarked = self.imgprop.categorize_and_watermark_images()
            self.imagecontent = self.imgprop.extract_text(self.media_dir)

            self.textmasked = False
            if self.imagecontent:
                if tobe_redacted is not None and len(tobe_redacted) > 0:
                    self.imgprop.mask_sensitive_info(tobe_redacted, drawlabel)
                    self.textmasked = True
                else:
                    self.extractor = TextExtractor(self.fileid, self.imagecontent, correlationid=cfg.CORR_ID_REDACTION)
                    self.extractor.extract_sensitive_info()
                    logger.info(f"{self.fileid}-->Sensitive Info: {self.extractor.sensitiveInfoList}")
                    if len(self.extractor.sensitiveInfoList) > 0:
                        self.imgprop.mask_sensitive_info(self.extractor.sensitiveInfoList, drawlabel)
                        self.textmasked = True

            if self.imagewatermarked or self.textmasked:
                self.recreate_xlsx()
                # return self.filepath
            
        except Exception as e:
            logger.warning(f"{self.fileid}-->ERROR redacting Images XLSX: {e}",exc_info=True)
            return False
        # finally:
        #     if self.out_dir.exists():
        #         shutil.rmtree(self.out_dir)
        return self.imagewatermarked or self.textmasked
            
    def replace_image_references_in_folder(self, old_filename: str, new_filename: str):
        """
        It goes through extracted workbook folder
        and replace any occurence of old_filename with new_filename in XML/RELS
        parts in .xml 
        """
        PNG_CONTENT_TYPE = "image/png"
        EMF_CONTENT_TYPE = "image/x-emf"

        # replace in xml rels files
        for p in self.out_dir.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix.lower() in {".xml", ".rels"} or p.name == "[Content_Types].xml":
                txt = p.read_text(encoding="utf-8", errors="ignore")
                if old_filename in txt:
                    p.write_text(txt.replace(old_filename, new_filename), encoding="utf-8")

        # Ensure a PNG default entry exists and update any Override
        ct_path = self.out_dir / "[Content_Types].xml"
        if ct_path.exists():
            try:
                tree = ET.parse(str(ct_path))
                root = tree.getroot()

                # add Default for .png if missing
                if not any(el.tag.endswith("Default") and el.attrib.get("Extension") == "png"
                           for el in root):
                    default_el = ET.Element(
                        "{http://schemas.openxmlformats.org/package/2006/content-types}Default"
                    )
                    default_el.set("Extension", "png")
                    default_el.set("ContentType", PNG_CONTENT_TYPE)
                    root.append(default_el)
                    tree.write(str(ct_path), encoding="utf-8", xml_declaration=True)

                # update Override entries that still point to the old file
                for el in root:
                    if el.tag.endswith("Override") and old_filename in el.attrib.get("PartName", ""):
                        new_part = el.attrib["PartName"].replace(old_filename, new_filename)
                        el.set("PartName", new_part)
                        el.set("ContentType", PNG_CONTENT_TYPE)
                tree.write(str(ct_path), encoding="utf-8", xml_declaration=True)
            except Exception as exc:  
                logger.warning(f"{self.fileid}-->Failed to update [Content_Types].xml: {exc}",exc_info=True)
 
    def recreate_xlsx(self) -> None:
        
        if not any(self.imgprop.redacted_dir.iterdir()):
            logger.info(f"{self.fileid}-->No Media File redacted in XLSX")
            return

        for img_path in self.imgprop.redacted_dir.iterdir():
            target_path = self.media_dir / img_path.name
            try:
                shutil.copy(img_path, target_path)
            except Exception as exc:  
                logger.warning(
                    f"{self.fileid}-->Failed to copy redacted image {img_path} → {target_path}: {exc}",exc_info=True
                )
        _files = [
            (f.name, f.stem) for f in self.imgprop.redacted_dir.iterdir()
            if f.suffix.lower() in ('.emf', '.wmf', '.vsdx')
        ]

        for old_name, stem in _files:
            # The redacted version we just copied is a PNG (e.g. image1.png)
            # Replace every reference to the old vector file with the new PNG.
            self.replace_image_references_in_folder(
                old_filename=old_name,
                new_filename=f"{stem}.png"
            )

        # if self.processing_dir.exists():
        #     shutil.rmtree(self.processing_dir)

        #  Zip the folder back into the original .xlsx file
        try:
            with zipfile.ZipFile(self.filepath, 'w', zipfile.ZIP_DEFLATED) as zip_out:
                for file_path in Path(self.out_dir).rglob('*'):
                    if file_path.is_file():
                        arc_name = file_path.relative_to(self.out_dir)
                        zip_out.write(file_path, arc_name)
            logger.info(f"{self.fileid}-->Created new XLSX: {self.filepath}")
        except Exception as exc:  
            logger.warning(f"{self.fileid}-->ERROR creating XLSX: {exc}",exc_info=True)
