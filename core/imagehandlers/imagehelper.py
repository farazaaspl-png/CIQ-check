import re
import numpy as np
import base64, os, json, shutil, logging, pandas as pd
import uuid
from pathlib import Path
from typing import Any, Dict, List
from PIL import Image, ImageDraw, ImageFont
import imagehash
import pytesseract
from pytesseract import Output
import cv2
from aspose.imaging.imageoptions import PngOptions
# from IPython.display import display
import  jpype     
# import  asposediagram  
# from cairosvg import svg2png

from core.imagehandlers import prompt as pmt
from core.genai.open_ai_client import OpenAiHelper

from core.utility import get_custom_logger
logger = get_custom_logger(__name__)
# logger.propagate = False

if not jpype.isJVMStarted():
    jpype.startJVM() 
from asposediagram.api import *

class ImageProcessor:
    def __init__(self, media_dir: Path, correlationid: uuid = None,fileid: uuid = None, debug: bool = False):
        self.media_dir = media_dir
        self.redacted_dir = Path(os.path.join(self.media_dir, "redacted"))
        self.toredact_dir = Path(os.path.join(self.media_dir, "to_redact"))
        self.categorization_result = Path(os.path.join(self.media_dir, "categorization_result.json"))
        self.fileid = fileid
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.llm_helper = OpenAiHelper(correlationid=correlationid)
        self.image_dict = {}
        self._extensions_to_convert = ['.emf','.wmf','.vsdx','.svg']

    def set_directories(self, media_dir: Path):
        self.media_dir = media_dir
        self.redacted_dir = Path(os.path.join(self.media_dir, "redacted"))
        self.toredact_dir = Path(os.path.join(self.media_dir, "to_redact"))
        self.categorization_result = Path(os.path.join(self.media_dir, "categorization_result.json"))

    def convert_to_png(self, image_path: Path, pngpath: Path = None) -> Path:
        if not pngpath:
            pngpath = image_path.with_suffix(".png")
        if image_path.suffix.lower() in ('.emf'): 
            Image.open(image_path).save(pngpath)

        elif image_path.suffix.lower() in ('.wmf'): 
            with Image.load(str(image_path)) as image:
                png_options = PngOptions()
                image.save(str(pngpath), png_options)

        elif image_path.suffix.lower() in ('.vsdx'):
            if not jpype.isJVMStarted():
                jpype.startJVM()
            diagram = Diagram(image_path)
            # Save the diagram as a PNG image
            diagram.save(pngpath, SaveFileFormat.PNG)
            # if jpype.isJVMStarted():
            #     jpype.shutdownJVM()
        # elif image_path.suffix.lower() in ('.svg'):
        #     svg2png(url=str(image_path), write_to=str(pngpath))

        return pngpath
    
    def encode_image(self,image_path: Path) -> str:
        # suffix = image_path.suffix.lower()
        # mime = "image/png" if suffix == ".png" else "image/gif" if suffix == ".gif" else "image/jpeg"
        with image_path.open("rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")

        return f"data:image/{image_path.suffix.lower().replace('.', '')};base64,{b64}"
    
    # def get_duplicate_image(self, img) -> Path:
    #     for img_path.

    def _detect_language(self, img):
        """Enhanced language detection with more languages and better fallback"""
        try:
            # Ensure image has proper DPI for better OSD
            if hasattr(img, 'info') and "dpi" not in img.info:
                img.info["dpi"] = (70, 70)
            
            # Try OSD first
            osd = pytesseract.image_to_osd(img)
            SCRIPT_TO_LANGS = {
                "Latin": "eng+fra+spa+por+deu+ita+nld",
                "Han": "chi_sim+chi_tra+jpn+kor",
                "Hiragana": "jpn",
                "Katakana": "jpn", 
                "Hangul": "kor",
                "Cyrillic": "rus+ukr+bul",
                "Arabic": "ara",
                "Devanagari": "hin+mar+san"
            }
            for line in osd.splitlines():
                if line.startswith("Script:"):
                    script = line.split(":")[1].strip()
                    return SCRIPT_TO_LANGS.get(script, 'eng')
        except Exception as exc:
            logger.debug(f"{self.fileid}-->Language detection failed: {exc}")
        
        # Fallback to comprehensive language set
        return 'eng+fra+spa+por+deu+ita'

    def get_ocr_text(self, image_path: Path) -> str:
        def _is_junk_line(line: str) -> bool:
            """
            Heuristics to decide whether a single line is garbage.
            Returns True if the line should be discarded.
            """
            checks = []
            # 1️⃣ Very short line (e.g. a single character or two)
            if len(line.strip()) < 3:
                logger.debug(f"length of text - {len(line.strip())}")
                checks.append(1)
            else:
                checks.append(0)

            # 2️⃣ Too many non‑alphanumeric symbols ( > 40% )
            non_alpha = len(re.findall(r'[^A-Za-z0-9]', line))
            if non_alpha / max(len(line), 1) > 0.2:
                logger.debug(f"Ratio of non-alpha - {non_alpha / max(len(line), 1)}")
                checks.append(1)
            else:
                checks.append(0)

            words = re.findall(r"\b\w+\b", line)
            avg_word_len = sum(len(w) for w in words) / len(words) if len(words)>0 else 0
            if avg_word_len < 5:
                logger.debug(f"Average aplha word length - {avg_word_len}")
                checks.append(1)
            else:
                checks.append(0)

            # 3️⃣ Repeated characters like “====”, “----”, “.....”
            if re.fullmatch(r'[\W_]{3,}', line):
                logger.debug(f"Repeated characters - {re.fullmatch(r'[\W_]{3,}', line)}")
                checks.append(1)
            else:
                checks.append(0)

            # 4️⃣ Very low word count ( < 2 words )
            words = line.split()
            avg_word_len = sum(len(w) for w in words) / len(words) if len(words)>0 else 0
            if avg_word_len < 5:
                logger.debug(f"Average word length - {avg_word_len}")
                checks.append(1)
            else:
                checks.append(0)

            # 5️⃣ All‑caps with no spaces – often a header or a scan artifact
            if line.isupper() and " " not in line and len(line) > 5:
                logger.debug(f"Upper case - {line.isupper()}")
                checks.append(1)
            else:
                checks.append(0)
            wt = [0.1,0.25,0.25,0.1,0.20,0.1]
            tot = sum(checks)
            logger.debug(checks)
            avg = sum([checks[i]*wt[i] for i in range(len(checks))])
            if avg>0.5:
                return True
            else:
                return False
            
        ocr_text =''
        try:
            img_name = image_path.name
            if image_path.suffix.lower() in self._extensions_to_convert:
                pngpath = self.convert_to_png(image_path)
                image_path = pngpath
                
            img = cv2.imread(image_path)
            img_details = {"img_hash": imagehash.average_hash(Image.open(image_path)), 
                           "size" : os.path.getsize(image_path)}
            if img is None:
                img = np.array(Image.open(image_path).convert("RGB"))

            lang_list = self._detect_language(img)
            if img is not None and img_name not in self.image_dict.keys():# and img.size > 0:
                # img = cv2.imread(image_path)
                img = cv2.resize(img, None, fx=1.25, fy=1.25, interpolation=cv2.INTER_LINEAR)
                img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
                ret, img = cv2.threshold(img, 130, 255, cv2.THRESH_BINARY)
                img = cv2.bilateralFilter(img, 9, 75, 75)
                lang_list = "eng+fra+spa+jpn+kor+por"
                ocr_text = pytesseract.image_to_string(img, lang=lang_list, config="--psm 3")
                if ocr_text is not None and ocr_text.strip() != '':
                     if not _is_junk_line(ocr_text):
                        img_details['ocr_text'] = ocr_text
                self.image_dict[img_name] = img_details  # Save img
        except Exception as e:
            logger.warning(f"{self.fileid}-->Error performing OCR on image {image_path}: {e}",exc_info=True)
        return ocr_text
    
    def check_image_for_llm_call(self, image_path: Path,image_size_threshold) -> Dict[str, Any]:
        imgdetails = self.image_dict.get(image_path.name)
        if imgdetails is None:
            self.get_ocr_text(image_path)
            imgdetails = self.image_dict.get(image_path.name)
        if 'llm_response' in imgdetails.keys():
            return False
        
        if imgdetails['size']<image_size_threshold:
            return False

        img_hash = imgdetails['img_hash']
        similar_images = []
        for imgname, imgdet in self.image_dict.items():
            diff = img_hash - imgdet['img_hash']
            if diff < 5 and imgname != image_path.name and 'llm_response' in imgdet.keys():
                imgdet['diff'] = diff
                similar_images.append({'imagename': imgname, 'diff': diff})
        if not len(similar_images) > 0:
            return True
        
        df = pd.DataFrame(similar_images)
        df.sort_values(by='diff', inplace=True, ascending=True, ignore_index=True)
        dupimagename = df.iloc[0]['imagename']
        self.image_dict[image_path.name]['llm_response']=  self.image_dict[dupimagename]['llm_response']
        return False
        
    def analyze_image(self, image_path: Path,image_size_threshold = 200000) -> Dict[str, Any]:
        # print(f"DEBUG: ImageProcessor.analyze_image called with: {image_path}")
        needsLlmcall = self.check_image_for_llm_call(image_path,image_size_threshold)
        if not needsLlmcall:
            return
        try:
            imgdetails = self.image_dict.get(image_path.name,{})
            if len(imgdetails) > 0 and 'llm_response' in imgdetails.keys():
                response = imgdetails['llm_response']
            else:
                userprompt = pmt.get_image_detection_prompt()
                if image_path.suffix in self._extensions_to_convert:
                    pngpath = self.convert_to_png(image_path)
                    img_url = self.encode_image(pngpath)
                else:
                    img_url = self.encode_image(image_path)
    
                response = self.llm_helper.get_json_image_to_text(userprompt, img_url,fileid=self.fileid)

                if len(response.keys()) >0:
                    self.image_dict[image_path.name]['llm_response']=  response
                logger.debug(f"Image processed by LLM {image_path.name} - {self.image_dict.get(image_path.name)}")     
            
            if len(response.keys()) == 0:
                return {
                    "image_path": str(image_path),
                    "error": f"Failed to parse LLM output: {response}",
                    "raw_response": response
                }
            else:
                response["image_path"] = str(image_path.name)
                return response
        except Exception as e:
            logger.error(f"{self.fileid}-->DEBUG: Exception in analyze_image: {str(e)}", exc_info=True)
            import traceback
            traceback.print_exc()
            raise e

        
    def categorize_and_watermark_images(self) -> List[Dict]:
        self.redacted_dir.mkdir(parents=True, exist_ok=True)
        self.toredact_dir.mkdir(parents=True, exist_ok=True)
        results: List[Dict] = []
        imagewatermarked: bool = False

        for img_path in self.media_dir.iterdir():
            try:
                if img_path.is_dir() or img_path.suffix.lower() == ".json":
                    continue
                # print(img_path)
                img_path = Path(img_path)
                img_out_path = Path(os.path.join(self.redacted_dir, img_path.name))
                result = self.analyze_image(img_path)
                if result is None:
                    continue
                results.append(result)

                if result.get("category") in ("INTERNAL")\
                  or (result.get("category") == "LOGO" 
                        and result.get("sub-category") in ("DELL TECHNOLOGIES LOGO","INTERNAL LOGO", "VENDOR LOGO")):
                    continue
                elif result.get("category")!='OTHER':
                    self.add_diagonal_watermark(
                                image_path=img_path,
                                text=result.get("sub-category", result.get("category")),
                                output_path=img_out_path,
                                opacity=100,  # Lower for more transparency
                                )
                    imagewatermarked = True
                else:
                    shutil.copy(str(img_path), self.toredact_dir / img_path.name)
            except Exception as e:
                logger.error(f"{self.fileid}-->DEBUG: Exception in categorize_and_watermark_images: {e}", exc_info=True)
                result = {"image_path": str(img_path),
                          "error": f"Exception during analysis: {str(e)}",
                        }

        with self.categorization_result.open("w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        return results, imagewatermarked
    
    def extract_text(self, mediadir:Path = None, lang="eng", config="--psm 6") -> str:
        self.redacted_dir.mkdir(parents=True, exist_ok=True)
        self.txtlist = []
        img_dir = mediadir if mediadir is not None else self.media_dir
        if not (img_dir.exists() and any(img_dir.iterdir())):
            logger.info(f'{self.fileid}-->No Media File to redact')
            return None
        
        for img_path in img_dir.iterdir():
            if img_path.is_dir() or img_path.suffix.lower() not in {".png", ".jpg", ".jpeg", ".bmp", ".tiff"}:
                continue
            img = cv2.imread(img_path)
            if img is None:
                img = np.array(Image.open(img_path).convert("RGB"))

            lang_list = self._detect_language(img)

            if img is None or img.size == 0:
                logger.warning(f"{self.fileid}-->Failed to read image {img_path} (cv2.imread returned None). Skipping.")
                continue
            # Convert to RGB (pytesseract expects RGB)
            rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
            data = pytesseract.image_to_data(
                rgb,
                lang=lang_list,
                output_type=Output.DICT,
                config=config,  # Assume a uniform block of text; tweak if needed
            )
            n_items = len(data["level"])
            
            for i in range(n_items):
                if not data["text"][i].strip():
                    continue
                row ={}
                row["text"]=data["text"][i]
                row["conf"]=data["conf"][i]
                row["left"]=data["left"][i]
                row["top"]=data["top"][i]
                row["width"]=data["width"][i]
                row["height"]=data["height"][i]
                row['imagepath']=img_path
                self.txtlist.append(row)

        if len(self.txtlist) > 0:
            df = pd.DataFrame(self.txtlist)
            txt = ' '.join(df[df['conf']>75]['text'].to_list())
            return txt
    
    def draw_boxes(self, img_path: Path, boxes: List[Dict],drawlabel: bool = False):
        img = cv2.imread(img_path)
        for box in boxes:
            (x, y, w, h) = (int(box["left"]),
                            int(box["top"]),
                            int(box["width"]),
                            int(box["height"]))

            # Draw rectangle
            cv2.rectangle(img, (x, y), (x + w, y + h), (0, 255, 0), cv2.FILLED)
            if drawlabel:
                label = f"{box["label"]}"
                cv2.putText(
                        img,
                        label,
                        (x, y-2),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        0.2,
                        (0, 0, 0),
                        1,
                        cv2.LINE_AA,
                    )
            # display(Image.fromarray(img))
        outpath = Path(os.path.join(self.redacted_dir, Path(img_path).name))
        logger.info(f"{self.fileid}-->Saving redacted image to {outpath}")
        cv2.imwrite(str(outpath), img)

    def mask_sensitive_info(self, sensitiveInfoList: List[Dict],drawLabel = False):
        sensdf= pd.DataFrame(sensitiveInfoList)
        if 'label' not in sensdf.columns:
            sensdf.rename(columns={'category': 'label'}, inplace=True)

        # Find records containing any quote characters
        quote_chars = ['`', "'", "’"]
        mask = sensdf['sensitivetext'].str.contains('|'.join(quote_chars), na=False)
        records_with_quotes = sensdf[mask].copy()

        # Create duplicated records with quote replacements
        duplicated_list = [sensdf]
        for quote_char in quote_chars+[' ']:
            records_with_quotes['sensitivetext'] = records_with_quotes['sensitivetext'].str.replace(r"[`'’]", quote_char, regex=True)
            duplicated_list.append(records_with_quotes.copy())

        # Combine original dataframe with duplicated records
        if len(duplicated_list)>1:
            sensdf = pd.concat(duplicated_list, ignore_index=True)
            
        imgtxtdf = pd.DataFrame(self.txtlist)
        # sensdf = sensdf.set_index('sensitivetext').join(imgtxtdf.set_index('text'),how='inner')
        # More efficient approach using merge with a custom condition
        merged = sensdf.merge(imgtxtdf, how='cross')  # Cross join all combinations
        
        # Filter where sensitivetext is contained in text
        merged = merged[merged.apply(lambda row: row['sensitivetext'] in row['text'], axis=1)]
        
        # Drop duplicate columns if any and reset index
        sensdf = merged.reset_index(drop=True)

        for path in sensdf["imagepath"].drop_duplicates():
            _boxes =sensdf[sensdf.imagepath == path][["left","top","width","height","label"]].to_dict(orient='records')
            self.draw_boxes(path, _boxes, drawLabel)

    def add_diagonal_watermark(self, image_path, text, output_path, font_path=None, opacity=110, text_color=(255, 255, 255), box_color=(255, 0, 0), box_opacity=200, angle=30):
        """
        Adds a big bold diagonal watermark with a semi-transparent box behind it.
        """

        # --- Load image ---
        image = Image.open(image_path).convert("RGBA")
        W, H = image.size

        # --- Create overlay ---
        overlay = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)

        # --- Pick font size relative to image size ---
        font_size = int(min(W, H) / 6)
        font_path = r"core\imagehandlers\Arial-bold.ttf"
        if os.path.exists(font_path):
            font = ImageFont.truetype(font_path, font_size)
        else:
            font = ImageFont.load_default()

        # --- Get text size (compatible with Pillow ≥10) ---
        def get_text_size(d, txt, fnt):
            if hasattr(d, "textbbox"):
                bbox = d.textbbox((0, 0), txt, font=fnt)
                return bbox[2] - bbox[0], bbox[3] - bbox[1]
            else:
                return d.textsize(txt, font=fnt)

        text_w, text_h = get_text_size(draw, text, font)

        # --- Create separate image for text + box ---
        margin = max(10, font_size // 5)
        txt_img = Image.new("RGBA", (text_w + 2 * margin, text_h + 2 * margin), (0, 0, 0, 0))
        txt_draw = ImageDraw.Draw(txt_img)

        # Draw semi-transparent box behind text
        txt_draw.rectangle(
            [ (0, 0), (text_w + 2 * margin, text_h + 2 * margin) ],
            fill=(*box_color, box_opacity)
        )

        # Draw text (centered within the box)
        stroke_w = max(1, font_size // 18)
        txt_draw.text(
            (margin, margin),
            text,
            font=font,
            fill=(*text_color, opacity),
            stroke_width=stroke_w,
            stroke_fill=(0, 0, 0, int(opacity * 0.8))
        )

        # --- Rotate text+box ---
        rotated = txt_img.rotate(angle, expand=True)

        # --- Paste onto overlay (centered) ---
        x = (W - rotated.width) // 2
        y = (H - rotated.height) // 2
        overlay.paste(rotated, (x, y), rotated)

        # --- Combine with original image ---
        result = Image.alpha_composite(image, overlay)
        result.convert("RGB").save(output_path, "JPEG")
        # display(result.convert('RGB'))
        # print(f"✅ Saved watermarked image with box → {output_path}")


#++++++++++++++++++++++++++++++++++++++++++++++++++++New Code++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # def _has_text(self, img_path: Path) -> bool:
    #    """Fast pre-filter to detect if image likely contains text"""
    #    try:
    #        # Load image
    #        img = cv2.imread(img_path)
    #        if img is None:
    #            img = np.array(Image.open(img_path).convert("RGB"))
           
    #        # Method 1: Enhanced edge detection with multiple thresholds
    #        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
           
    #        # Try different edge detection thresholds for better logo detection
    #        edges1 = cv2.Canny(gray, 30, 100)
    #        edges2 = cv2.Canny(gray, 50, 150)
    #        edges = cv2.bitwise_or(edges1, edges2)
           
    #        # Find contours that might be text
    #        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
           
    #        # Filter for text-like contours (more lenient for logos)
    #        text_contours = 0
    #        for contour in contours:
    #            x, y, w, h = cv2.boundingRect(contour)
    #            aspect_ratio = w / h if h > 0 else 0
               
    #            # More lenient text-like characteristics for logos:
    #            # - Wider aspect ratio range (0.05 to 15)
    #            # - Smaller minimum size (5x5 pixels)
    #            # - Allow larger contours (up to 70% of image)
    #            img_h, img_w = gray.shape
    #            if (0.05 <= aspect_ratio <= 15 and 
    #                w >= 5 and h >= 5 and 
    #                w < img_w * 0.7 and h < img_h * 0.7):
    #                text_contours += 1
           
    #        # If we found text-like contours, likely has text
    #        if text_contours >= 2:  # Reduced threshold from 3 to 2
    #            return True
           
    #        # Method 2: Multiple quick Tesseract checks with different preprocessing
    #        try:
    #            # Check 1: Original grayscale
    #            small_img = cv2.resize(gray, None, fx=0.5, fy=0.5)
    #            quick_text1 = pytesseract.image_to_string(small_img, config="--psm 6", lang='eng')
               
    #            # Check 2: Inverted (for dark text on light background)
    #            inverted = cv2.bitwise_not(gray)
    #            small_inverted = cv2.resize(inverted, None, fx=0.5, fy=0.5)
    #            quick_text2 = pytesseract.image_to_string(small_inverted, config="--psm 6", lang='eng')
               
    #            # Check 3: Thresholded (for better contrast)
    #            thresh = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2)
    #            small_thresh = cv2.resize(thresh, None, fx=0.5, fy=0.5)
    #            quick_text3 = pytesseract.image_to_string(small_thresh, config="--psm 6", lang='eng')
               
    #            # Combine results from all checks
    #            all_texts = [quick_text1, quick_text2, quick_text3]
               
    #            for quick_text in all_texts:
    #                if quick_text and len(quick_text.strip()) > 2:  # Reduced from 3 to 2
    #                    # More lenient junk filter
    #                    non_alpha = len(re.findall(r'[^A-Za-z0-9\s]', quick_text))
    #                    if non_alpha / len(quick_text) < 0.7:  # Increased from 0.5 to 0.7
    #                        return True
                           
    #        except Exception as e:
    #            logger.debug(f"{self.fileid}-->Quick Tesseract checks failed: {e}")
           
    #        # Method 3: Check for high contrast regions (common in logos)
    #        try:
    #            # Calculate image variance as a simple text indicator
    #            variance = np.var(gray)
    #            if variance > 1000:  # Threshold for high contrast images
    #                return True
    #        except:
    #            pass
           
    #        return False
           
    #    except Exception as e:
    #        logger.debug(f"{self.fileid}-->Text detection failed for {img_path}: {e}")
    #        # Default to True if detection fails (don't skip potentially important images)
    #        return True

    # def extract_text(self, mediadir: Path = None, lang="eng", config="--psm 6") -> str:
    #     self.redacted_dir.mkdir(parents=True, exist_ok=True)
    #     self.txtlist = []
    #     img_dir = mediadir if mediadir is not None else self.media_dir
    #     if not (img_dir.exists() and any(img_dir.iterdir())):
    #         logger.info(f'{self.fileid}-->No Media File to redact')
    #         return None

    #     # Reduced PSM modes for faster processing
    #     psm_modes = ["--psm 6", "--psm 3"]  # Only 2 modes instead of 4

    #     for img_path in img_dir.iterdir():
    #         if img_path.is_dir() or img_path.suffix.lower() not in {".png", ".jpg", ".jpeg", ".bmp", ".tiff"}:
    #             continue
            
    #         # FAST PRE-FILTER: Skip images that likely don't contain text
    #         if not self._has_text(img_path):
    #             logger.info(f"{self.fileid}-->Skipping {img_path.name} - no text detected")
    #             continue
            
    #         # Handle vector formats by converting to PNG
    #         if img_path.suffix.lower() in self._extensions_to_convert:
    #             pngpath = self.convert_to_png(img_path)
    #             if pngpath.exists():
    #                 img_path = pngpath
    #             else:
    #                 logger.warning(f"{self.fileid}-->Failed to convert {img_path} to PNG. Skipping.")
    #                 continue
                
    #         # Load original image for coordinate accuracy
    #         original_img = None
    #         try:
    #             original_img = cv2.imread(img_path)
    #             if original_img is None:
    #                 original_img = np.array(Image.open(img_path).convert("RGB"))
    #         except Exception as e:
    #             logger.error(f"{self.fileid}-->Error loading image {img_path}: {e}")
    #             continue
            
    #         if original_img is None or original_img.size == 0:
    #             logger.warning(f"{self.fileid}-->Failed to read image {img_path}. Skipping.")
    #             continue
            
    #         # Enhanced language detection
    #         lang_list = self._detect_language(original_img)

    #         # Optimized OCR with early termination
    #         best_result = []
    #         best_score = 0

    #         for psm_mode in psm_modes:
    #             try:
    #                 # Strategy 1: OCR on original image (preserves coordinates)
    #                 result = self._perform_ocr(original_img, lang_list, psm_mode, img_path, 
    #                                         preprocessing_type="original")
    #                 score = self._calculate_ocr_score(result)
    #                 if score > best_score:
    #                     best_score = score
    #                     best_result = result

    #                 # Early termination: if we got good results, stop trying more strategies
    #                 if score > 1000:  # Good confidence threshold
    #                     break
                    
    #                 # Strategy 2: OCR with color inversion (only for dark backgrounds)
    #                 result = self._perform_ocr(original_img, lang_list, psm_mode, img_path,
    #                                         preprocessing_type="invert")
    #                 score = self._calculate_ocr_score(result)
    #                 if score > best_score:
    #                     best_score = score
    #                     best_result = result

    #                 # Early termination if inversion worked well
    #                 if score > 1000:
    #                     break

    #             except Exception as e:
    #                 logger.debug(f"{self.fileid}-->PSM mode {psm_mode} failed for {img_path}: {e}")
    #                 continue
                
    #         # Error Recovery: If all strategies failed, try basic fallback
    #         if not best_result:
    #             try:
    #                 logger.info(f"{self.fileid}-->All strategies failed for {img_path}, trying fallback")
    #                 rgb = cv2.cvtColor(original_img, cv2.COLOR_BGR2RGB)
    #                 data = pytesseract.image_to_data(
    #                     rgb,
    #                     lang=lang_list,
    #                     output_type=Output.DICT,
    #                     config="--psm 6",
    #                 )

    #                 for i in range(len(data["level"])):
    #                     if data["text"][i].strip() and data["conf"][i] > 50:
    #                         row = {
    #                             "text": data["text"][i],
    #                             "conf": data["conf"][i],
    #                             "left": data["left"][i],
    #                             "top": data["top"][i],
    #                             "width": data["width"][i],
    #                             "height": data["height"][i],
    #                             "imagepath": img_path,
    #                             "psm_mode": "fallback"
    #                         }
    #                         best_result.append(row)

    #             except Exception as e:
    #                 logger.error(f"{self.fileid}-->Fallback OCR failed for {img_path}: {e}")

    #         self.txtlist.extend(best_result)

    #     if len(self.txtlist) > 0:
    #         df = pd.DataFrame(self.txtlist)
    #         final_text = ' '.join(df['text'].to_list())
    #         return final_text

    # def _perform_ocr(self, original_img, lang_list, psm_mode, img_path, preprocessing_type="original"):
    #     """Perform OCR with different preprocessing strategies while preserving coordinates"""
    #     img_to_ocr = original_img.copy()
    #     scale_factor = 1.0

    #     if preprocessing_type == "invert":
    #         # Color inversion for dark background images like KOHL'S
    #         img_to_ocr = cv2.bitwise_not(original_img)

    #     elif preprocessing_type == "enhanced":
    #         # Enhanced preprocessing with coordinate scaling
    #         scale_factor = 1.25
    #         img_to_ocr = cv2.resize(original_img, None, fx=scale_factor, fy=scale_factor, 
    #                                interpolation=cv2.INTER_LINEAR)

    #         # Convert to grayscale
    #         gray = cv2.cvtColor(img_to_ocr, cv2.COLOR_BGR2GRAY)

    #         # Adaptive thresholding
    #         gray = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
    #                                     cv2.THRESH_BINARY, 11, 2)

    #         # Bilateral filter for noise reduction
    #         img_to_ocr = cv2.bilateralFilter(gray, 9, 75, 75)

    #         # Convert back to 3-channel for tesseract
    #         img_to_ocr = cv2.cvtColor(img_to_ocr, cv2.COLOR_GRAY2BGR)

    #     # Convert to RGB for tesseract
    #     rgb = cv2.cvtColor(img_to_ocr, cv2.COLOR_BGR2RGB)

    #     # Perform OCR
    #     data = pytesseract.image_to_data(
    #         rgb,
    #         lang=lang_list,
    #         output_type=Output.DICT,
    #         config=psm_mode,
    #     )

    #     # Process results with coordinate correction
    #     results = []
    #     for i in range(len(data["level"])):
    #         if data["text"][i].strip():
    #             # Scale coordinates back to original image dimensions
    #             if preprocessing_type == "enhanced":
    #                 left = int(data["left"][i] / scale_factor)
    #                 top = int(data["top"][i] / scale_factor)
    #                 width = int(data["width"][i] / scale_factor)
    #                 height = int(data["height"][i] / scale_factor)
    #             else:
    #                 left = data["left"][i]
    #                 top = data["top"][i]
    #                 width = data["width"][i]
    #                 height = data["height"][i]

    #             row = {
    #                 "text": data["text"][i],
    #                 "conf": data["conf"][i],
    #                 "left": left,
    #                 "top": top,
    #                 "width": width,
    #                 "height": height,
    #                 "imagepath": img_path,
    #                 "psm_mode": psm_mode,
    #                 "preprocessing": preprocessing_type
    #             }
    #             results.append(row)

    #     return results

    # def _calculate_ocr_score(self, results):
    #     """Calculate OCR score based on confidence and text quantity"""
    #     if not results:
    #         return 0

    #     confidences = [r["conf"] for r in results if r["conf"] > 0]
    #     if not confidences:
    #         return 0

    #     avg_confidence = sum(confidences) / len(confidences)
    #     text_length = sum(len(r["text"]) for r in results)

    #     # Bonus for inverted preprocessing (helps with dark backgrounds)
    #     preprocessing_bonus = 1.1 if any(r.get("preprocessing") == "invert" for r in results) else 1.0

    #     return avg_confidence * len(results) * preprocessing_bonus
