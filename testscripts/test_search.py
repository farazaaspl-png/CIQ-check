# ---- Setup ----
import logging
import os
from pprint import pprint
import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
import json 

# OPTIONAL: only if imports fail
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config import Configuration as cfg
from core.embedding.vectorizer_content import ContentVectorInterface
from core.utility import get_custom_logger

logger = get_custom_logger(__name__)
logging.basicConfig(level=logging.INFO)


# ---- Inputs ----
query = "give me relevant docs on servicenow"
top_k = 10

logger.info(f"Running real search_and_rank for query='{query}', k={top_k}")


vec = ContentVectorInterface(cfg.DOCUMENT_CONTENT_STORE)
res = vec.search_and_rank_v2("Azure implementation", k = top_k)
pprint(res)

# with open("debug.json", "w", encoding="utf-8") as f:
#     json.dump(res, f, indent=2, ensure_ascii=False)
