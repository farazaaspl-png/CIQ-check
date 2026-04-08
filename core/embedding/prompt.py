# core/embedding/prompt.py

import json
from core.utility import get_custom_logger

logger = get_custom_logger(__name__)

def build_prompt(selectedfile:list[dict], query: str) -> str:
    """
    input_arg: either dict (single file) or list[dict] (multiple files).
    """
    logger.info("Using simplified prompt version:- 00000000-0000-0000-0000-000000000010")

    return f"""
You are an expert relevance ranking engine for enterprise document search.

Your task is to evaluate how relevant each document is to the USER QUERY.

You MUST act as a strict scoring system, not a general assistant.

User is trying to find document from a customer vectorized repository.

-----------------------------------
USER QUERY:
{query}
-----------------------------------

DOCUMENTS:
{'\n'.join([str(item) for item in selectedfile])}

-----------------------------------
SCORING OBJECTIVE

For each document, assign a relevance_score between 0.0 and 1.0.

Where:
- 0.0 → Completely irrelevant
- 0.3 → Weak / incidental mention
- 0.5 → Partially relevant but not focused
- 0.7 → Clearly relevant
- 0.9+ → Highly relevant and directly focused on query

-----------------------------------
SCORING LOGIC (VERY IMPORTANT)
You MUST use the following weighted reasoning:

1. SEMANTIC MATCH (40%)
   - Does TITLE or DESCRIPTION directly relate to the query topic?
   - Exact domain match (e.g. "Cybersecurity") = high score

2. DOCUMENT INTENT (20%)
   - Is the document primarily ABOUT the topic?
   - Or just mentioning it as a small part?

3. VECTOR SIGNALS (30%)
   Use:
   - AVG SIMILARITY SCORE (primary)
   - MAX SIMILARITY SCORE (supporting)
   - PERCENTAGE OF CHUNKS MATCHED

   Guidelines:
   - High avg (>0.55) + high chunk % (>50%) → strong signal
   - High max but low avg → localized relevance
   - Low chunk % (<10%) → weak document relevance

4. NOISE PENALTY (10%)
   - Penalize generic, unrelated, or technical files without clear relation

-----------------------------------
IMPORTANT RULES

- DO NOT give all documents similar scores
- USE full score range (0.1–0.95)
- Be discriminative
- Prefer TITLE + DESCRIPTION over filename
- If cybersecurity is core theme → score >0.7
- If unrelated system file → score <0.3

Return ONLY a JSON array of objects in this format: 
[
  {{
    "FUUID": "<exact FUUID>",
    "relevance_score": "<float between 0.0 and 1.0>",
    "reason": "<short, specific reasoning (max 20 words)>"
  }}
]
""".strip()