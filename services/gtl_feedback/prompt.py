# def build_prompt(text: str, ischunked: bool = False) -> str:
#     return f"""You are a concise TECHNICAL CONSULTANT for Dell Group of Companies.
# From below document content, Provide a detailed and structured description of the document.
# <<<
# {text}
# >>>

# Return ONLY a JSON array of objects in this format:  
# [
#   {{"title": "Title of the document",
#     "description": {{"Description": "What is the document trying to address? (2-5 sentences)",
#                      "Key points": "What are the key points? (| separated list)",
#                      "Sections": "Describe each section of the document? (| separated list)",
#                      "Others": "Any other important information?"}}
#    }}
# ]

# STRICT RULES:
# - Output must be JSON only (no explanations, no extra text)
# """
from core.utility import get_custom_logger
logger = get_custom_logger(__name__)

def build_prompt(text: str, ischunked = False) -> str:
    logger.info('Using prompt version:- 00000000-0000-0000-0000-000000000005')
    return f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.
From below DOCUMENT,
    1. Extract TITLE of the document.
    2. Generate DESCRIPTION of the document.     

Content of DOCUMENT:
<<<
{text}
>>>

OUTPUT FORMAT:
Return ONLY a JSON array of objects in this format:
[
  {{
   "title": "Title of the DOCUMENT",
   "description": {{"Description": "What is the DOCUMENT trying to address? (2-5 sentences)",
                     "Key Points": "What are the key points? (| separated list)",
                     "Sections": "Describe each section of the DOCUMENT? (| separated list)",
                     "Others": "Any other important information?"}},
    "Language": "Actual LANGUAGE of the DOCUMENT",
  }}
]

STRICT RULES:
- Do not generate Sections if it is not present in the document
- Description should be generated in same language as the document
{"- LANGUAGE of DESCRIPTION and TITLE should be same as the document" if not ischunked else ''}
"""

def build_consolidation_prompt(descriptionlist, language = 'english'):
    logger.info('Using prompt version:- 00000000-0000-0000-0000-000000000028')
    return f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.
For provided list of DESCRIPTION, generated from multiple chunks of a DOCUMENT,
 1. Generate a consolidated DESCRIPTION (i.e. Description, Key Points, Sections and Others)

Below is the list of DESCRIPTIONS of each chunk of the DOCUMENT: 
-----------------------------------------------------------------------------------------------------------------
{descriptionlist}
-----------------------------------------------------------------------------------------------------------------

OUTPUT FORMAT:
Return ONLY a JSON array of objects in this format:   
[
  {{
   "description": {{"Description": "Consolidated Description of what is the DOCUMENT trying to address. (2-5 sentences)",
                    "Key Points": "Consolidated list of maximum 10, 1 line sentences of all the key points. (| separated list)",
                    "Sections": "Consolidated list of section of the DOCUMENT. (| separated list)",
                    "Others": "Consolidated list of other important information."}},
  }}
]

STRICT RULES:
- Do not include Key Points that is too generic. 
- Do not include Sections that is too generic.
{"- Description, Key Points, Sections and Others should be in "+language.upper()+" LANGUAGE." if language.lower() != 'english' else ''}
"""
