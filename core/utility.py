import base64
import json
import logging
import math
import os
from typing import Any, Dict, List
from langchain_text_splitters import RecursiveCharacterTextSplitter
from PIL import Image
from io import BytesIO
# from transformers import AutoTokenizer
from tokenizers import Tokenizer
from pathlib import Path
from pythonjsonlogger import jsonlogger
import chardet
from config import Config as cfg
import contextvars
from core.log_helper import add_session_file_handler

logging.getLogger('transformers').setLevel(logging.ERROR)

session_id_var = contextvars.ContextVar("session_id", default="-")
request_id_var = contextvars.ContextVar("request_id", default="-")

class ContextFilter(logging.Filter):
    def filter(self, record):
        record.request_id = request_id_var.get()
        record.session_id = session_id_var.get()
        return True

_DOCUMENT_INFO_TABLES = ['document information',
                         'document version',
                         'document version control',
                         'document history',
                         'record of revisions',
                         'revision history',
                         'drafting and release history',
                         'version history',
                         'dell document control',
                         'revisions']

_OTHER_CONTROL_TABLES = ['document distribution',
                        'contacts',
                        'document control',
                        'dell document approval',
                        'document reviewers',
                        'distribution/circulation list',
                        'version approvals:']
_SUBSTRINGS_TO_SKIP = ['table of contents', 'index of tables', 'index of figures', 'table ', 'figure ']

_WHITE_VARIATIONS = [
# Comprehensive white detection
            'FFFFFF',  # Pure white
            'FFFFF0',  # Ivory
            'FFFFFA',  # Alice blue
            'F0F8FF',  # Light blue white
            'F5F5F5',  # White smoke
            'F8F8FF',  # Ghost white
            'FFFAFA',  # Snow
            'FAFAFA',  # Off white
            'FDF5E6',  # Old lace
            'FFF8DC',  # Cornsilk
            'FFFACD',  # Lemon chiffon
            'FFF5EE',  # Seashell
            'FFF0F5',  # Lavender blush
            'F5FFFA',  # Mint cream
            'F0FFF0',  # Honeydew
            'FFF8DC',  # Cornsilk
            'FAEBD7',  # Antique white
            'F5F5DC',  # Beige
            'FFE4B5',  # Moccasin
            'FFDEAD',  # Navajo white
            'FFE4C4',  # Bisque
            'FFDAB9',  # Peach puff
            'FFEBCD',  # Papaya whip
            'FFFFE0',  # Light yellow
            'FFFACD',  # Lemon chiffon
            'F0E68C',  # Khaki
            'E6E6FA',  # Lavender
            'D8BFD8',  # Thistle
            'DDA0DD',  # Plum
            'EE82EE',  # Violet
            'DA70D6',  # Orchid
            'FF00FF',  # Magenta
            'FF1493',  # Deep pink
            'C71585',  # Medium violet red
            'DB7093',  # Pale violet red
            'FFB6C1',  # Light pink
            'FFC0CB',  # Pink
            'FFD700',  # Gold
            'FFFF00',  # Yellow
            'F0E68C',  # Khaki
            'BDB76B',  # Dark khaki
            'F5DEB3',  # Wheat
            'FFE4B5',  # Moccasin
            'FFDEAD',  # Navajo white
            'FAEBD7',  # Antique white
            'F5F5DC',  # Beige
            'FFE4C4',  # Bisque
            'FFDAB9',  # Peach puff
            'FFEBCD',  # Papaya whip
            'FFFFE0',  # Light yellow
            'FFFACD',  # Lemon chiffon
            'FAFAD2',  # Light goldenrod yellow
            'EEE8AA',  # Pale goldenrod
            'F0E68C',  # Khaki
            'BDB76B',  # Dark khaki
            'FFFFF0',  # Ivory
            'F5FFFA',  # Mint cream
            'F0FFF0',  # Honeydew
            'FFF5EE',  # Seashell
            'FFF0F5',  # Lavender blush
            'FFF8DC',  # Cornsilk
            'FFFACD',  # Lemon chiffon
            'FFF8DC',  # Cornsilk
            'FAEBD7',  # Antique white
            'F5F5DC',  # Beige
            'FFE4B5',  # Moccasin
            'FFDEAD',  # Navajo white
            'FFE4C4',  # Bisque
            'FFDAB9',  # Peach puff
            'FFEBCD',  # Papaya whip
            'FFFFE0',  # Light yellow
            'FFFACD',  # Lemon chiffon
            'FAFAD2',  # Light goldenrod yellow
            'EEE8AA',  # Pale goldenrod
            'F0E68C',  # Khaki
            'BDB76B',  # Dark khaki
            'FFFFE5',
        ]
##############DO NOT DELETE THIS##############################################################################################################
# pip install transformers
# from transformers import AutoTokenizer
# embedding_model = AutoTokenizer.from_pretrained("ibm-granite/granite-embedding-278m-multilingual",use_auth_token="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")
# embedding_model.save_pretrained(r"C:\Users\Lekhnath_Pandey\CIQ\ip_content_management\core\tokenizers\granite-embedding-278m-multilingual")
# from transformers import AutoTokenizer
# tok = AutoTokenizer.from_pretrained("meta-llama/Llama-3.3-70B-Instruct",use_auth_token="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")
# tok.save_pretrained(r"C:\Users\Lekhnath_Pandey\CIQ\ip_content_management\core\tokenizers\llama-3-3-70b-instruct")

# from transformers import AutoTokenizer
# tok = AutoTokenizer.from_pretrained("meta-llama/Llama-3.2-11B-Vision-Instruct",use_auth_token="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")
# tok.save_pretrained(r"C:\Users\Lekhnath_Pandey\CIQ\ip_content_management\core\tokenizers\llama-vision")

# from transformers import AutoTokenizer
# embedding_model = AutoTokenizer.from_pretrained("nomic-ai/nomic-embed-text-v1.5",use_auth_token="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")
# embedding_model.save_pretrained(r"C:\Users\Lekhnath_Pandey\CIQ\ip_content_management\core\tokenizers\nomic-embed-text-v1.5")

# from transformers import AutoModel, AutoTokenizer

# model_name = "nomic-ai/nomic-embed-text-v1.5"

# tokenizer = AutoTokenizer.from_pretrained(model_name)
# model = AutoModel.from_pretrained(model_name)

# inputs = tokenizer(["hello world"], return_tensors="pt")
# embeddings = model(**inputs).last_hidden_state.mean(dim=1)
# print(embeddings.shape)
# from transformers import ViTFeatureExtractor
# tok = ViTFeatureExtractor.from_pretrained("meta-llama/Llama-3.2-11B-Vision-Instruct",
#                                            use_auth_token="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",
#                                            cache_dir = "core/tokenizers")
# tok.save_pretrained(r"C:\Users\Lekhnath_Pandey\CIQ\ip_content_management\core\tokenizers\llama-vision")

# WHERE lower("substring"(doc.filename::text, length(doc.filename::text) - strpos(reverse(doc.filename::text), '.'::text) + 1, length(doc.filename::text))) = ANY (ARRAY['.doc'::text, '.docx'::text, '.docm'::text, '.pdf'::text, '.ppt'::text, '.pptx'::text, '.pptm'::text, '.txt'::text, '.csv'::text, '.xlsm'::text, '.xlsx'::text, '.xls'::text])
################################################################################################################################################3

def chunk_list(input_list, chunk_size=8000):
    """Yields successive n-sized chunks from a list."""
    # for i in range(0, len(input_list), chunk_size):
    #     yield input_list[i:i + chunk_size]
    current_chunk: List[Dict[str, Any]] = []
    current_len = 0

    for d in input_list:
        # Serialize the dict – `ensure_ascii=False` keeps Unicode readable,
        # `separators` removes unnecessary whitespace to get a compact size.
        d_str = json.dumps(d, ensure_ascii=False, separators=(",", ":"))
        d_len = len(d_str)

        # If a single dict itself exceeds the limit, we still yield it alone.
        # This prevents an infinite loop and lets the caller decide what to do.
        if d_len > chunk_size and not current_chunk:
            yield [d]
            continue

        # Would adding this dict overflow the limit?
        if current_len + d_len > chunk_size:
            # Emit the accumulated chunk and start a new one.
            yield current_chunk
            current_chunk = [d]
            current_len = d_len
        else:
            # Append dict to the current chunk.
            current_chunk.append(d)
            current_len += d_len

    # Emit any leftovers.
    if current_chunk:
        yield current_chunk

def _detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        rawdata = file.read()
        result = chardet.detect(rawdata)
        return result['encoding']

def split_text(filecontent, chunk_size, chunk_overlap) -> list[str]:
    splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    return splitter.split_text(filecontent)

def get_custom_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    # print(logger.handlers)
    # Add filter first before any handlers
    logger.addFilter(ContextFilter())
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        handler = logging.StreamHandler()
        # formatter = logging.Formatter('[%(levelname)s|%(name)s|L%(lineno)d] %(asctime)s | %(message)s',datefmt='%Y-%m-%dT%H:%M:%S%z')
        # formatter = jsonlogger.JsonFormatter("%(levelname)s %(name)s %(lineno)d %(asctime)s %(message)s",datefmt='%Y-%m-%dT%H:%M:%S%z')
        formatter = jsonlogger.JsonFormatter("%(session_id)s %(request_id)s %(levelname)s %(name)s %(lineno)d %(asctime)s %(message)s",
                                             datefmt='%Y-%m-%dT%H:%M:%S%z',
                                             defaults={"session_id": "-", "request_id": "-"})

        handler.setFormatter(formatter)
        # logger.addFilter(ContextFilter())
        add_session_file_handler(logger)

        logger.addHandler(handler)
        logger.propagate = False
    return logger


def count_tokens(text: str, results: str= '') -> int:
    # tokenizer = AutoTokenizer.from_pretrained(Path(os.path.join(r"core/tokenizers",cfg.TEXT_TO_TEXT_MODEL)))
    # return len(tokenizer.encode(text)) + len(tokenizer.encode(results))
    # Load local tokenizer directly (no transformers needed)
    tokenizer_path = os.path.join(r"core/tokenizers", cfg.TEXT_TO_TEXT_MODEL, "tokenizer.json")
    tokenizer = Tokenizer.from_file(tokenizer_path)
    return len(tokenizer.encode(text).ids) + len(tokenizer.encode(results).ids)

# def preprocess_image(image_path: str) -> dict:
#     # Load the image
#     image = Image.open(image_path)
    
#     # Load the feature extractor
#     feature_extractor = AutoFeatureExtractor.from_pretrained(Path(r"core/tokenizers/llama-vision"))
    
#     # Preprocess the image
#     inputs = feature_extractor(images=image, return_tensors="pt")
    
#     return inputs

# def preprocess_image_from_url(image_url: str) -> dict:
#     # Load the image from the URL
#     image_data = image_url.split(",")[1]
#     # Decode the base64-encoded image data
#     image_bytes = base64.b64decode(image_data)
#     # Load the image from the decoded bytes
#     image = Image.open(BytesIO(image_bytes))
    
#     # Load the feature extractor
#     feature_extractor = AutoFeatureExtractor.from_pretrained(Path(r"core/tokenizers/llama-vision"))
    
#     # Preprocess the image
#     inputs = feature_extractor(images=image, return_tensors="pt")
    
    # return inputs

def count_vision_tokens(text: str,image_url: str, results: str) -> int:
    # Load the image from the URL
    image_data = image_url.split(",")[1]
    # Decode the base64-encoded image data
    image_bytes = base64.b64decode(image_data)
    # Load the image from the decoded bytes
    image = Image.open(BytesIO(image_bytes))
    w, h = image.size
    PATCH = 14
    tokens_w = math.ceil(w / PATCH)
    tokens_h = math.ceil(h / PATCH)

    # tokenizer = AutoTokenizer.from_pretrained(Path(os.path.join(r"core/tokenizers",cfg.IMAGE_TO_TEXT_MODEL)))
    # Load local tokenizer directly (no transformers needed)
    tokenizer_path = os.path.join(r"core/tokenizers", cfg.IMAGE_TO_TEXT_MODEL, "tokenizer.json")
    tokenizer = Tokenizer.from_file(tokenizer_path)

    # imageinput = preprocess_image_from_url(image_url)
    total_tokens = len(tokenizer.encode(text)) + (tokens_w * tokens_h)
    if results is not None:
        total_tokens = total_tokens + len(tokenizer.encode(results))
    return total_tokens 

# def remove_control_chars(text):
#     for char in control_chars:
#         text = text.replace(char, '')
#     return text
