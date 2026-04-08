import uuid,os,re,csv,pandas as pd
import logging
from pathlib import Path
from typing import List, Dict, Tuple

from core.db.crud import DatabaseManager
from core.exceptions import NoSensitiveItemFound
from core.utility import get_custom_logger, _detect_encoding
from config import Config as cfg
logger = get_custom_logger(__name__)

class FlatFileRedactor:
    
    def __init__(self, filepath: Path, dafileid: uuid.UUID = None, debug: bool = False):
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        
        self.filepath = filepath
        self.dafileid = dafileid
    
        self.records: List[Dict] = []
        self.filecontent: str = ''
        self.redacted_content: str = ''
        self.sensitive_info: List[Dict]=[]

        if debug:
            logger.setLevel(logging.DEBUG)   

    def escape_custom(self, s: str) -> str:
        """Escape regex special characters."""
        SPECIAL_CHARS = r'.^$*+?{}[]\|()#'
        escaped = []
        for ch in s:
            if ch in SPECIAL_CHARS:
                escaped.append('\\' + ch)
            else:
                escaped.append(ch)
        return ''.join(escaped)
    
    def _remove_hyperlinks(self):
        url_pattern = re.compile(r'(https?://\S+|www\.\S+|ftp://\S+)', re.IGNORECASE)
        return list(url_pattern.finditer(self.filecontent))
    
    def _load_file_content(self, strip_whitespace: bool = True, remove_blank_lines: bool = True) -> str:
        """Read flat file content with detected encoding, mirroring FlatFileReader behavior."""
        encoding = _detect_encoding(self.filepath)
        logger.info(f"{self.dafileid}: Detected encoding: {encoding}")
        with open(self.filepath, "r", encoding=encoding, errors="replace") as f:
            lines = f.readlines()

        assembled = [f"Document: {self.filepath.name}"]
        for line in lines:
            if strip_whitespace:
                line = line.strip()
            if remove_blank_lines and line == "":
                continue
            assembled.append(line)

        self.filecontent = "\n".join(assembled)
        return self.filecontent

    def _replace_sensitive_text(self, text: str, label: str, pattern: str) -> str:
        label = "" if pd.isna(label) else str(label)
        safe_pattern = self.escape_custom(str(pattern))

        # Handle quote variations in the pattern
        if any(quote in safe_pattern for quote in '`\'’'):
            # Replace escaped single quote with a character class that matches both ' and `
            quote_variations = safe_pattern.replace("\\'", "[`'’]?")
        else:
            quote_variations = safe_pattern

        # Add word boundaries to avoid matching within words
        word_boundary_pattern = rf'\b{quote_variations}\b'
        try:
            len_diff=0
            for m in re.finditer(word_boundary_pattern, text, re.IGNORECASE):
                if m.lastindex:
                    match_str = m.group(1)
                    startIdx = m.start() + m.group().find(m.group(1))
                    endIdx = startIdx + len(m.group(1))
                else:
                    match_str = m.group(0)
                    startIdx = m.start()
                    endIdx = m.end()
                
                
                # if match_str in FlatFileRedactor._EXCLUSION_ITEMS:
                #     continue
                placeholder = f"<{cfg.PREFIX}{label.replace(' ', '_')}>"
                self.records.append({
                    "start": startIdx,
                    "end": endIdx,
                    "label": label,
                    "sensitivetext": match_str,
                    "placeholder": placeholder,
                    "context": text[max(0, startIdx - 40):min(len(text), endIdx + 40)]
                })
                text = text[:startIdx+len_diff] + placeholder + text[endIdx+len_diff:]
                len_diff = len_diff + len(placeholder)-(endIdx-startIdx)
        except Exception as e:
            logger.warning(f"Regex pattern '{label}' failed: {e}",exc_info=True)
        
        return text
    
    
    def set_sensitiveinfo(self):
        logger.info(f"{self.dafileid}-->Fetching sensitive data from DB")
        db = DatabaseManager()
        df = db.get_vwtoberedacted(requestid=self.requestid, fuuid=self.fuuid, dafileid = self.dafileid)
        
        if df.empty:
            raise NoSensitiveItemFound()
        
        self.sensitive_data = df[['category', 'sensitivetext']].drop_duplicates().to_dict(orient='records')
            
    def sanitize(self, **kwargs):    
        self.requestid = kwargs.get('requestid')
        self.fuuid = kwargs.get('fuuid')
        self.dafileid = kwargs.get('dafileid')
        
        self._load_file_content()
        self._remove_hyperlinks()
        self.set_sensitiveinfo()  

        for tr in self.sensitive_data:
            if pd.isnull(tr['category']):
                continue
            self._replace_sensitive_text(text=self.filecontent,label=tr['category'], pattern=tr['sensitivetext'])
        
        totalRedacted = len(self.records)
        return len(self.records) > 0, totalRedacted
    
    def save(self, outdir: str, filename:str = None) -> Tuple[Path, Path]:
        try:
            fname = filename if filename is not None else self.filepath.name
            OutPathFile = Path(os.path.join(outdir,fname))
            OutPathFile.parent.mkdir(parents=True, exist_ok=True)
            # OutPathFile = Path(os.path.join(outdir, str(self.dafileid), fname))
            # if not OutPathFile.parent.exists():
            #     OutPathFile.parent.mkdir(parents=True)
            with open(OutPathFile, 'w', encoding='utf-8') as f:
                f.write(self.redacted_content)
            logger.info(f"Saved redacted file to {OutPathFile}")

            if len(self.records)>0:
                db = DatabaseManager()
                db.insert_redacted_results(self.requestid, self.fuuid, self.dafileid, fname, self.records)
                logger.info(f"Redacted {len(self.records)} sensitive items")
                
                redactedItemsFilepath = os.path.join(outdir, f"{OutPathFile.stem}_redacted.csv")
                pd.DataFrame(pd.json_normalize(self.records)).drop(['start', 'end'], axis=1).to_csv(redactedItemsFilepath, index=False, quoting=csv.QUOTE_ALL)
                logger.info(f"Saved redaction csv to {redactedItemsFilepath}")
                return Path(OutPathFile), Path(redactedItemsFilepath)
            else:
                return Path(OutPathFile),None
        except Exception as e:
            logger.error(f"Error saving files: {e}", exc_info=True)
            raise

