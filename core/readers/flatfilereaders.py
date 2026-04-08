from pathlib import Path
from typing import Optional
import uuid, logging
from core.exceptions import EmptyFileError
from core.utility import get_custom_logger, _detect_encoding

class FlatFileReader:
    def __init__(self, filepath: Path, debug: bool = False, fileid: Optional[uuid.UUID] = None):   
        self.debug = debug
        self.fileid = fileid or uuid.uuid4()

        self.filepath = filepath
        if not self.filepath.exists():
            raise FileNotFoundError(filepath)
        
        self.assembled = [f"Document: {self.filepath.name}"]
        self.fileContent = None
        self.logger = get_custom_logger(__name__)
        if debug:
            self.logger.setLevel(logging.DEBUG)
    
    def _extract_lines(self, encoding: str):
        try:
            with open(self.filepath, 'r', encoding=encoding, errors='replace') as f:
                lines = f.readlines()
            return lines
        except Exception as e:
            self.logger.error(f"{self.fileid}: Error reading file: {e}", exc_info=True)
            raise
    
    
    def extract_content(self, remove_blank_lines: bool = True, 
                       strip_whitespace: bool = True):
        
        self.logger.info(f'{self.fileid}: Started extracting content from {self.filepath}')
        encoding = _detect_encoding(self.filepath)
        self.logger.info(f'{self.fileid}: Detected encoding: {encoding}')
        lines = self._extract_lines(encoding)
        for line in lines:
            if strip_whitespace:
                line = line.strip()
            if remove_blank_lines and line == '':
                continue
            self.assembled.append(line)
        
        _,_ = self.get_filecontent()
        self.logger.info(f'{self.fileid}: Completed extracting {len(lines)} lines')
        # if len(self.assembled)<5:
        #     raise EmptyFileError()
        content_text = "\n".join(self.assembled).strip()
        if len(content_text) < 50:
            raise EmptyFileError()
        
        return self.fileContent
    
    def get_filecontent(self, get_ocr = False):
        self.fileContent = '\n'.join(self.assembled)
        return self.fileContent, []
    
    def clean_up(self):
        pass