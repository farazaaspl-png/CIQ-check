from core.utility import get_custom_logger

logger = get_custom_logger(__name__)


def build_chunk_prompt(input_chunk: str, template_chunk: str) -> str:
    logger.info("Using prompt version: 00000-00000-00000-00000-00000")

    return f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.

You are given a CHUNK (excerpt) from two documents:
  - INPUT CHUNK    : The document submitted for classification
  - TEMPLATE CHUNK : The reference template it was matched against


Your task is to write a DESCRIPTION that
explains how this INPUT CHUNK differs from the TEMPLATE CHUNK —
focus on  Missing content, Extra Content and  Intent Drift.

INPUT CHUNK:
<<<
{input_chunk}
>>>

TEMPLATE CHUNK:
<<<
{template_chunk}
>>>

OUTPUT FORMAT:
Return ONLY a JSON array of objects in this format:
{{
  "description": "<Description of how this chunks differs? (2-5 sentences)>"
}}

STRICT RULES:
- description must be in English and 2-3 sentences only.
- Do NOT return a similarity score.
- Do NOT include any explanation outside the JSON object.
"""


def build_consolidation_prompt(desc_lst: list[str]) -> str:
    logger.info("Using prompt version:  00000-00000-00000-00000-00000")

    return f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.
For provided list of Descriptions from each chunk to chunk difference 
Generate a consolidated Description How the Input Document Deviates from the Template Document.

List of Descriptions:
----------------------------------------------------
{desc_lst}
----------------------------------------------------
OUTPUT FORMAT:
Return ONLY a JSON array of objects in this format:
{{
  "description": "<2-3 sentence DESCRIPTION>"
}}

STRICT RULES:
- difference_description must be in English and 2-3 sentences only.
- Do NOT repeat chunk-level details verbatim — synthesise them into a cohesive summary.
- If a section list is empty, return an empty string "".
- Do NOT include any explanation outside the JSON object.
"""