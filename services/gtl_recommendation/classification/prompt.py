# prompt.py
from core.utility import get_custom_logger

logger = get_custom_logger(__name__)

# -------------------------------------------------------------------
# DOCUMENT + OFFER PROMPT (UNCHANGED)
# -------------------------------------------------------------------
def build_prompt(text: str, offerslst: str,ischunked = False) -> str:
    logger.info('Using prompt version:- 00000000-0000-0000-0000-000000000005')

    return f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.
From below {"chunk of" if ischunked else ''} DOCUMENT,
    1. Extract TITLE of the document.
    2. Detect LANGUAGE of the document.
    3. Generate description of the document.
    4. SELECT all the OFFERS from provided LIST OF OFFERS that are relevant to the this DOCUMENT.
    5. provide relevance score between 0-1 for the SELECTED OFFER
    6. Re-Validate the SELECTED OFFER based on description of the DOCUMENT. i.e. Description, Key points, Sections and Others.
    7. recalulate relevance score for the SELECTED OFFER        

Content of DOCUMENT:
<<<
{text}
>>>

List of available OFFERS:
-----------------------------------------------------------------------------------------------------------------
{offerslst}
-----------------------------------------------------------------------------------------------------------------

OUTPUT FORMAT:
Return ONLY a JSON array of objects in this format:
[
  {{
   "title": "Title of the document (Should Not be Filename)",
   "description": {{"Description": "What is the document trying to address? (2-5 sentences)",
                     "Key Points": "What are the key points? (| separated list)",
                     "Sections": "Describe each section of the document? (| separated list",
                     "Others": "Any other important information?"}},
   "offer": {{"OfferName":"Name of offer provided", "Relevance Score": "relevance score of the offer","Reason": "Reason for the offer selection"}},
   "language": "Actual LANGUAGE of the DOCUMENT",
   "author": "Name or Email Address of WRITER/AUTHOR of the document"
  }}
]

STRICT RULES:
- Do not generate Sections if it is not present in the document
- for Hints, + means 'and' & / sign means 'or'.
- If no relevant OFFER is found select Any 1 offer with lower relevance score
- Do not include WRITER/AUTHOR if not exclusively mentioned in the document
- LANGUAGE of description should strictly in ENGLISH
"""
# {"- LANGUAGE of description should be same as detected" if not ischunked else ''}
# -------------------------------------------------------------------
# IP TYPE ONLY PROMPT
# -------------------------------------------------------------------
def build_iptype_prompt(description: str, iptypelist: str) -> str:
    """
    Prompt to predict IP type only, given a document description.
    """
    logger.info('Using IP type prompt version:- 00000000-0000-0000-0000-000000000003')

    return f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.
For provided TITLE and DESCRIPTION of a document, SELECT the TYPEs from the provided list of TYPES that are relevant to this document.
DOCUMENT DESCRIPTION:
<<<
{description}
>>>

List of available TYPES:
-----------------------------------------------------------------------------------------------------------------
{iptypelist}
-----------------------------------------------------------------------------------------------------------------

OUTPUT FORMAT:
Return ONLY a JSON array of objects in this format:
[
  {{
   "ip_types": [{{"Type": "Type of the document", "Relevance Score": "Relevance Score between 0-1", "Reason": "Reason for selection of Type"}}]
  }}
]

STRICT RULES:
- select atleast one TYPE from the provided list
"""

def build_consolidation_prompt(descriptionlist, offerlist,language = 'english'):
    logger.info('Using prompt version:- 00000000-0000-0000-0000-000000000002')
    return f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.
For provided list of DESCRIPTION and OFFER, generated for multiple chunks of document:
    1. Generate a consolidated DESCRIPTION.
    2. Validate and recalculate the Relevance Score of the provided list of OFFERS, based on all description

Below is the list of DESCRIPTION of each chunk of document: 
-----------------------------------------------------------------------------------------------------------------
{descriptionlist}
-----------------------------------------------------------------------------------------------------------------

Below is the list of OFFERS of the document:
-----------------------------------------------------------------------------------------------------------------
{offerlist}
-----------------------------------------------------------------------------------------------------------------

OUTPUT FORMAT:
Return ONLY a JSON array of objects in this format:   
[
  {{
   "description": {{"Description": "What is the document trying to address? (2-5 sentences)",
                     "Key Points": "What are the key points? (| separated list)",
                     "Sections": "Describe each section of the document? (| separated list",
                     "Others": "Any other important information?"}},
   "offer": {{"OfferName":"OfferName as provided", "Relevance Score": "Recalculated Relevance Score","Reason": "Reason as provided"}}
   }}
]

STRICT RULES:
- Do not generate Sections if it is not present in the document
{"- All items in description should be in "+language.upper()+" LANGUAGE." if language.lower() != 'english' else ''}
"""

# -------------------------------------------------------------------
# CONSOLIDATION PROMPT (SINGLE STRICT RULE BLOCK)
# -------------------------------------------------------------------
def build_consolidation_with_iptype_prompt(descriptionlist, offerlist, iptypelist,language = 'english'):
    """
    Overloaded function that handles both with and without IP type prediction.
    """
    logger.info('Using prompt version:- 00000000-0000-0000-0000-000000000005')

    base_prompt = f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.
For provided list of DESCRIPTION and OFFER, generated for multiple chunks of document:
    1. Generate a consolidated DESCRIPTION.
    2. Validate and recalculate the Relevance Score of the provided list of OFFERS, based on all description
    3. SELECT all TYPEs from provided list of TYPE that are relevant to this document.
    4. Validate and provide relevance score for each of the selected TYPE.

Below is the list of DESCRIPTION of each chunk of document:
-----------------------------------------------------------------------------------------------------------------
{descriptionlist}
-----------------------------------------------------------------------------------------------------------------

Below is the list of OFFERS of the document:
-----------------------------------------------------------------------------------------------------------------
{offerlist}
-----------------------------------------------------------------------------------------------------------------

Below is the list of TYPE of the document:
-----------------------------------------------------------------------------------------------------------------
{iptypelist}
-----------------------------------------------------------------------------------------------------------------

OUTPUT FORMAT:
Return ONLY a JSON array of objects in this format:
[
  {{
   "description": {{"Description": "What is the document trying to address? (2-5 sentences)",
                     "Key Points": "What are the key points? (| separated list)",
                     "Sections": "Describe each section of the document? (| separated list",
                     "Others": "Any other important information?"}},
   "offer": {{"OfferName":"OfferName as provided", "Relevance Score": "Recalculated Relevance Score","Reason": "Reason as provided"}},
   "ip_types": [{{"Type": "Type of the document", "Relevance Score": "Relevance Score of the Type","Reason": "Reason for the Type selection"}}]
  }}
]

STRICT RULES:
- Do not generate Sections if it is not present in list of DESCRIPTION
{"- All items in description ONLY (i.e. Description, Key Points, Sections and Others) should be in "+language.upper()+" LANGUAGE." if language.lower() != 'english' else ''}
"""
    return base_prompt 


def build_metadata_regeneration_prompt(metadata: dict) -> str:

    return f"""
You are a professional technical documentation assistant.

Task:
- The input metadata contains placeholders like <NAME>, <IP_ADDRESS>.
- Remove ALL placeholders enclosed in anglular brackets <>.
- Rewrite the content to be meaningful, clean, and professional.
- Preserve technical meaning.
- Do NOT invent sensitive information.
- Regenerate only readable neutral content.

Input Metadata:
<<<
{metadata}
>>>

Return ONLY valid JSON:

{{
  "filename": "<clean filename>",
  "title": "<clean title>",
  "description": "<rewritten description>"
}}

STRICT RULES:
- Do NOT include any REGION NAME, CUSTOMER NAME, or COMPANY NAME in the filename or title; keep them neutral.
"""