
import os, logging, shutil, zipfile, jpype, uuid, xml.etree.ElementTree as ET
from typing import List, Dict
# from copy import deepcopy
from pathlib import Path
from PIL import Image as PILImage
from aspose.imaging import Image as AsposeImage
from aspose.imaging.imageoptions import PngOptions
from asposediagram.api import *

from services.gtl_recommendation.sensitive_text_ext.extractor import TextExtractor
from core.imagehandlers.imagehelper import ImageProcessor
# from core.db.crud import DatabaseManager
from config import Config as cfg
logging.getLogger().setLevel(logging.INFO)
if not jpype.isJVMStarted():
    jpype.startJVM()

from core.utility import get_custom_logger
logger = get_custom_logger(__name__)

class DocRedactor:

    def __init__(self, filepath: Path ,fileid:uuid=None, debug: bool = False, analyze_images: bool=False):
        self.filepath = filepath
        self.fileid = fileid
        self.debug=debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.filename = filepath.name
        self.records: List[Dict] = []
        self.filecontent: str = ''

        self.analyze_images: bool = analyze_images
        self.out_dir: Path = Path(os.path.join(filepath.parent, filepath.stem.replace('-','').replace(' ', '_')))
        if self.out_dir.exists():
            shutil.rmtree(self.out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.media_dir = Path(os.path.join(self.out_dir,r'word\media'))
        self.processing_dir = Path(os.path.join(self.media_dir,'processing'))

    def extract_docx(self) -> bool:
        try:
            with zipfile.ZipFile(self.filepath, 'r') as zip_file:
                zip_file.extractall(self.out_dir)
            logger.info(f"{self.fileid}-->Extracted DOCX to: {self.out_dir}")
            
            if not (self.media_dir.exists() and any(self.media_dir.iterdir())):
                logger.info(f'{self.fileid}-->No Media File present in DOCX')
                return False
            return True
        except Exception as e:
            logger.warning(f"{self.fileid}-->ERROR extracting DOCX: {e}",exc_info=True)
            return False
        
    def move_to_processing_dir(self):
        self.processing_dir.mkdir(parents=True, exist_ok=True)
        for image_path in self.media_dir.iterdir():
            pngpath = os.path.join(self.processing_dir, image_path.stem+'.png')
            if image_path.is_dir():
                continue
            try: 
                if image_path.suffix.lower() in ('.emf'): 
                    PILImage.open(image_path).save(pngpath)

                elif image_path.suffix.lower() in ('.wmf'): 
                    with AsposeImage.load(str(image_path)) as image:
                        png_options = PngOptions()
                        image.save(str(pngpath), png_options)

                elif image_path.suffix.lower() in ('.vsdx'):
                    if not jpype.isJVMStarted():
                        jpype.startJVM() 
                    diagram = Diagram(image_path)
                    # Save the diagram as a PNG image
                    diagram.save(pngpath, SaveFileFormat.PNG)
                    
                else:
                    shutil.copy(image_path, self.processing_dir)
            except Exception as e:  
                logger.warning(f"{self.fileid}-->ERROR processing image: {e}",exc_info=True)
            # finally:
                # if jpype.isJVMStarted():
                #     jpype.shutdownJVM()
    
    def list_image_files(self) -> List[Path]:
        # exts = {".png", ".jpg", ".jpeg", ".bmp", ".gif"}
        return [p for p in self.media_dir.rglob("*") if p.is_file()]
    
    def recreate_docx(self) -> None:
        if not any(self.imgprop.redacted_dir.iterdir()):
            logger.info(f'{self.fileid}-->No Media File redacted in DOCX')

        for img_path in self.imgprop.redacted_dir.iterdir():
            targetpath = os.path.join(self.media_dir, img_path.name)
            shutil.copy(img_path,targetpath)

        _files = [(f.name,f.stem) for f in self.imgprop.redacted_dir.iterdir() if f.suffix in ('.emf','.wmf','.vsdx')]
        for filename, filestem in _files:
            self.replace_image_references_in_folder(old_filename = filename,new_filename = filestem+'.png')

        shutil.rmtree(self.processing_dir)

        try:
            with zipfile.ZipFile(self.filepath, 'w', zipfile.ZIP_DEFLATED) as zip_out:
                for file_path in Path(self.out_dir).rglob('*'):
                    if file_path.is_file():
                        arc_name = file_path.relative_to(self.out_dir)
                        zip_out.write(file_path, arc_name)
            logger.info(f"{self.fileid}-->Created new DOCX: {self.filepath}")
        except Exception as e:
            logger.warning(f"{self.fileid}-->ERROR creating DOCX: {e}",exc_info=True)

        # shutil.rmtree(self.out_dir)


    def redact(self,drawlabel: bool = False , tobe_redacted: List[dict] = None, imgprocessor: ImageProcessor = None, **kwargs) -> None:
        try:
            if not self.extract_docx():
                return False
            self.move_to_processing_dir()
            if imgprocessor is not None:
                self.imgprop =imgprocessor
            else:
                self.imgprop = ImageProcessor(media_dir = self.processing_dir, correlationid= cfg.CORR_ID_REDACTION,fileid = self.fileid, debug=cfg.DEBUG)
            self.imgprop.set_directories(media_dir = self.processing_dir)
            
            self.imagewatermarked = False
            if self.analyze_images:
                self.results, self.imagewatermarked = self.imgprop.categorize_and_watermark_images()
            self.imagecontent = self.imgprop.extract_text(self.media_dir)

            self.textmasked = False
            if self.imagecontent is not None:
                if tobe_redacted is not None and len(tobe_redacted)>0:
                    self.imgprop.mask_sensitive_info(tobe_redacted,drawlabel)
                    self.textmasked = True
                else:
                    self.extractor = TextExtractor(self.fileid,self.imagecontent,correlationid= cfg.CORR_ID_REDACTION)
                    self.extractor.extract_sensitive_info()
                    logger.info(f"{self.fileid}-->Sensitive Info: {self.extractor.sensitiveInfoList}")
                    if len(self.extractor.sensitiveInfoList) > 0:
                        self.imgprop.mask_sensitive_info(self.extractor.sensitiveInfoList,drawlabel)
                        self.textmasked = True

            if self.imagewatermarked or self.textmasked:
                self.recreate_docx()
                # return self.filepath

            # else:
            #     shutil.rmtree(self.out_dir)
        except Exception as e:
            logger.warning(f"{self.fileid}-->ERROR redacting Images DOCX: {e}",exc_info=True)
            return False
        # finally:
        #     if self.out_dir.exists():
        #         shutil.rmtree(self.out_dir)
        return self.imagewatermarked or self.textmasked

    def replace_image_references_in_folder(self, old_filename: str ,new_filename: str):
        """
        Updates an extracted .docx directory in place:
          - Replaces references to old_filename with new_filename across all XML/RELS files
          - Optionally fixes [Content_Types].xml entries for the specific part and adds PNG default if missing
          - Optionally deletes the old media file in /word/media
        Returns a summary dict with counts and actions taken.
        """
        PNG_CONTENT_TYPE = "image/png"
        EMF_CONTENT_TYPE = "image/x-emf"

        # 1) Replace references in all XML/RELS files
        for p in self.out_dir.rglob("*"):
            if not p.is_file():
                continue
            # Target only text-based Office XML parts
            if p.suffix.lower() in {".xml", ".rels"} or p.name == "[Content_Types].xml":
                text = p.read_text(encoding="utf-8", errors="ignore")
                if old_filename in text:
                    text = text.replace(old_filename, new_filename)
                    p.write_text(text, encoding="utf-8")

        # 2) Fix [Content_Types].xml (Override + ensure PNG Default)
        content_types_path = self.out_dir / "[Content_Types].xml"
        content_types_updated = False
        ensured_png_default = False

        if content_types_path.exists():
            try:
                # Register default namespace handling (Content_Types has no default ns usually)
                tree = ET.parse(str(content_types_path))
                root = tree.getroot()

                # Ensure Default for .png exists
                has_png_default = any(
                    el.tag.endswith("Default") and el.attrib.get("Extension") == "png"
                    for el in root
                )
                if not has_png_default:
                    default_el = ET.Element("{http://schemas.openxmlformats.org/package/2006/content-types}Default")
                    default_el.set("Extension", "png")
                    default_el.set("ContentType", PNG_CONTENT_TYPE)
                    root.append(default_el)
                    ensured_png_default = True
                    content_types_updated = True

                # Update any Override entry for the *specific part* if it still mentions old filename
                # e.g., <Override PartName="/word/media/image3.emf" ContentType="image/x-emf"/>
                for el in root:
                    if el.tag.endswith("Override"):
                        part_name = el.attrib.get("PartName", "")
                        if part_name.endswith(f"/{old_filename}"):
                            # Update the PartName to new file and content type to PNG
                            el.set("PartName", part_name[: -len(old_filename)] + new_filename)
                            el.set("ContentType", PNG_CONTENT_TYPE)
                            content_types_updated = True

                # Also, if any Override already points to the new PNG but still has EMF content type, fix it
                for el in root:
                    if el.tag.endswith("Override"):
                        part_name = el.attrib.get("PartName", "")
                        if part_name.endswith(f"/{new_filename}") and el.attrib.get("ContentType") != PNG_CONTENT_TYPE:
                            el.set("ContentType", PNG_CONTENT_TYPE)
                            content_types_updated = True

                if content_types_updated or ensured_png_default:
                    tree.write(str(content_types_path), encoding="utf-8", xml_declaration=True)
            except ET.ParseError:
                # Fall back: if XML malformed, we already did a raw text replace above, so continue
                pass

        # 3) Delete old media file (/word/media/image3.emf)
        old_media_path = self.out_dir / "word" / "media" / old_filename
        if old_media_path.exists():
            old_media_path.unlink()
            
