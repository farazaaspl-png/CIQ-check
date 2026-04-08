# prompt.py
from core.db.crud import DatabaseManager
from core.utility import get_custom_logger

logger = get_custom_logger(__name__)

def get_offer_list(withhints = False):
   db = DatabaseManager()
   offerdf = db.get_vwofferfamilydata()

   if withhints:
       logger.info('using offer list with hints')
       return '\n'.join(offerdf[['offer','hints']].drop_duplicates().apply(lambda rows: f'{rows.offer.upper()}  [HINTS: Look for "{rows.hints.lower()}" keywords]' if rows.hints else rows.offer.upper(),axis=1).to_list())
   else:
       logger.info('using offer list without hints')
       return '\n'.join(offerdf['offer'].drop_duplicates().to_list())

# "00000000-0000-0000-0000-000000000007"
# def build_prompt(text: str) -> str:
#     return (f"""
#  You are a concise TECHNICAL WRITER for Dell Group of Companies .
#  From below statement of work,
#     1. Extract CUSTOMER NAME
#     2. generate a summary of the all the works 
#     3. EXTRACT all the OFFERS provided to customer in it.
#        - DO NOT GENERATE NEW OFFERS
#     4. provide confidence score for each SELECTED OFFER
#     5. validate and filter out OFFERS based on Summary of SOW, recalulate confidence score. i.e. Objective, Scope of Work and Deliverables.
# STATEMENT OF WORKS File Content:
#  <<<
# {text}
# >>>
# List of available OFFERS:
# {get_offer_list(True)}

# Return ONLY a JSON array of objects in this format:  
# [
#   {{
# "Customer Name": "Name of customer or company",
# "Objective": "What is the project trying to achieve? (2-5 sentences)",
# "Scope of Work": "What is overall scope of work?",
# "Deliverables": "List tangible outputs (documents, systems, integrations, prototypes, development, infrastructure provisioning)",
# "Offer": ["OfferName":"Name of offer provided", "Confidence Score": "Confidence score of the offer","Reasoning": "Reasoning for the offer selection"] }}
# ]

# STRICT RULES:
# - IF DOCUMENT DOESN'T LOOK LIKE SOW, RETURN [{{"ERROR": "DOCUMENT DOESN'T LOOK LIKE SOW"}}]
# - Do not generate Scope of Work and Deliverables if it is not there in the document
# - Scope of Work and Deliverables should be extracted from the document and should be | separated
# - OFFERS should be REASONED and SELECTED from the list of offers provided
# - If no relevant OFFER is found select Any 1 offer with lower confidence score
# """)

# "00000000-0000-0000-0000-000000000009"
def build_prompt(text: str,offers: str,ischunked = False) -> str:
    logger.info('Using prompt version:- 00000000-0000-0000-0000-000000000028')
    return (f"""
 You are a TECHNICAL CONSULTANT for Dell Group of Companies.
 From below {"chunk of" if ischunked else ''} STATEMENT OF WORK,
    1. Extract CUSTOMER NAME
    2. Generate a SUMMARY of the all the works
    3. SELECT all the OFFERS from provided from LIST OF OFFERS that are relevant to the this STATEMENT OF WORK (Use HINTS if available).
    4. provide relevance score between 0-1 for each SELECTED OFFER
    5. validate and filter out SELECTED OFFERS based on Summary of SOW. i.e. Objective, Scope of Work and Deliverables.
    6. recalulate relevance score for each SELECTED OFFER
            
Content of STATEMENT OF WORK:
 <<<
{text}
>>>

List of available OFFERS:
-----------------------------------------------------------------------------------------------------------------
{offers}
-----------------------------------------------------------------------------------------------------------------
Return ONLY a JSON array of objects in this format:  
[
  {{
"Customer Name": "Name of customer or company",
"Objective": "What is the project trying to achieve? (2-5 sentences)",
"Scope of Work": "List of 1 line sentences (of explaination)for all services covered in Scope of Work. (| separated)",
"Deliverables": "List tangible outputs (like documents, reports, systems, integrations, prototypes, development, infrastructure provisioning etc) (| separated)",
"Language": "Actual LANGUAGE of the STATEMENT OF WORK",
"Offer": ["OfferName":"Name of offer provided", "Relevance Score": "relevance score of the offer","Reason": "Reason for the offer selection"] }}
]

STRICT RULES:
- Do not generate Scope of Work and Deliverables if it is not provided in the STATEMENT OF WORK
- for Hints, + means 'and' & / sign means 'or'.
- If no relevant OFFER is found select Any 1 offer with lower relevance score
{"- LANGUAGE of Objective, Scope of Work and Deliverables should be same as of the STATEMENT OF WORK" if not ischunked else ''}
""")

def build_consolidation_prompt(descriptionlist, offerlist,language = 'english'):
    logger.info('Using prompt version:- 00000000-0000-0000-0000-000000000028')
    return f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.
For provided list of SUMMARY and OFFER, generated from multiple chunks of a STATEMENT OF WORK:
    1. Generate a consolidated Summary (i.e. Objective, Scope of Work and Deliverables)
    2. Validate and recalculate the Relevance Score of all the OFFERS, USING all the provided Objective, Scope of Work, Deliverables and NoOfChunks an Offer appeared in.

Below is the list of SUMMARY of each chunk of STATEMENT OF WORK: 
-----------------------------------------------------------------------------------------------------------------
{descriptionlist}
-----------------------------------------------------------------------------------------------------------------

Below is the list of OFFERS of the STATEMENT OF WORK:
-----------------------------------------------------------------------------------------------------------------
{offerlist}
-----------------------------------------------------------------------------------------------------------------

OUTPUT FORMAT:
Return ONLY a JSON array of objects in this format:   
[
  {{
"Objective": "Consolidated Objective of what is the project trying to achieve?  (2-5 sentences)",
"Scope of Work": "Consolidated list of maximum 10, 1 line sentences of all the Scope Of Work. (| separated)",
"Deliverables": "Consolidated list of less than 10 most important Deliverables (| separated)",
"Offer": ["OfferName":"OfferName as provided", "Relevance Score": "recalculated relevance score of the offer (between 0-1)","Reason": "Reason as provided"] }}
]

STRICT RULES:
- Do not include Scope of Work that is too generic. 
- Do not include Deliverables that is too generic.
{"- Objective, Scope of Work and Deliverables should be in "+language.upper()+" LANGUAGE." if language.lower() != 'english' else ''}
"""

# "00000000-0000-0000-0000-000000000008"
# def build_prompt(text: str) -> str:
#     return (f"""
#  You are a concise TECHNICAL WRITER for Dell Group of Companies .
#  From below statement of work,
#     1. Extract CUSTOMER NAME
#     2. generate a summary of the all the works 
#     3. EXTRACT all the OFFERS provided to customer in it.
#        - DO NOT GENERATE NEW OFFERS
#     4. provide confidence score for each SELECTED OFFER
#     5. validate and filter out OFFERS based on Summary of SOW, recalulate confidence score. i.e. Objective, Scope of Work and Deliverables.
# STATEMENT OF WORKS File Content:
#  <<<
# {text}
# >>>
# List of available OFFERS:
# {get_offer_list(True)}

# Return ONLY a JSON array of objects in this format:  
# [
#   {{
# "Customer Name": "Name of customer or company",
# "Objective": "What is the project trying to achieve? (2-5 sentences)",
# "Scope of Work": "What is overall scope of work?",
# "Deliverables": "List tangible outputs (documents, systems, integrations, prototypes, development, infrastructure provisioning)",
# "Offer": ["OfferName":"Name of offer provided", "Confidence Score": "Confidence score of the offer","Reasoning": "Reasoning for the offer selection"] }}
# ]

# STRICT RULES:
# - IF DOCUMENT DOESN'T LOOK LIKE SOW, RETURN [{{"ERROR": "DOCUMENT DOESN'T LOOK LIKE SOW"}}]
# - Do not generate Scope of Work and Deliverables if it is not there in the document
# - Scope of Work and Deliverables should be extracted from the document and should be | separated
# - OFFERS should be REASONED and SELECTED from the list of offers provided
# - If no relevant OFFER is found select Any 1 offer with lower confidence score
# """)

# "00000000-0000-0000-0000-000000000005"
# def build_prompt(text: str) -> str:
#     return (f"""
#  You are a concise TECHNICAL WRITER for Dell Group of Companies .
#  From below statement of work,
#     1. Extract CUSTOMER NAME
#     2. generate a summary of the all the works 
#     3. EXTRACT all the OFFERS provided to customer in it.
#     4. provide confidence score for each SELECTED OFFER
#     5. validate and filter out OFFERS based on Summary of SOW. i.e. Objective, Scope of Work and Deliverables.
# STATEMENT OF WORKS File Content:
#  <<<
# {text}
# >>>
# List of available OFFERS:
# {get_offer_list()}

# Return ONLY a JSON array of objects in this format:  
# [
#   {{
# "Customer Name": "Name of customer or company",
# "Objective": "What is the project trying to achieve? (2-5 sentences)",
# "Scope of Work": "What is overall scope of work?",
# "Deliverables": "List tangible outputs (documents, systems, integrations, prototypes, development, infrastructure provisioning)",
# "Offer": ["OfferName":"Name of offer provided", "Confidence Score": "Confidence score of the offer","Reasoning": "Reasoning for the offer selection"] }}
# ]

# STRICT RULES:
# - IF DOCUMENT DOESN'T LOOK LIKE SOW, RETURN [{{"ERROR": "DOCUMENT DOESN'T LOOK LIKE SOW"}}]
# - Do not generate Scope of Work and Deliverables if it is not there in the document
# - Scope of Work and Deliverables should be extracted from the document and should be | separated
# - DO NOT GENERATE NEW OFFERS, ONLY USE EXISTING OFFERS
# - OFFERS should be REASONED and SELECTED from the list of offers provided
# - Atleast 1 OFFER should be SELECTED
# """)

# "00000000-0000-0000-0000-000000000006"
# def build_prompt(text: str) -> str:
#     return (f"""
#  You are a concise TECHNICAL WRITER for Dell Group of Companies .
#  From below statement of work,
#     1. Extract CUSTOMER NAME
#     2. generate a summary of the all the works. Including objective, scope of work and deliverables
#     3. Using generated summary and SOW file content, extract all the OFFERS provided to customer.
#          - DO NOT GENERATE NEW OFFERS, ONLY USE EXISTING OFFERS
#     4. provide confidence score for each SELECTED OFFER
            
# STATEMENT OF WORKS File Content:
#  <<<
# {text}
# >>>

# List of available OFFERS:
# {get_offer_list()}

# Return ONLY a JSON array of objects in this format:  
# [
#   {{
# "Customer Name": "Name of customer or company",
# "Objective": "What is the project trying to achieve? (2-5 sentences)",
# "Scope of Work": "What is overall scope of work?",
# "Deliverables": "List tangible outputs (documents, systems, integrations, prototypes, development, infrastructure provisioning)",
# "Offer": ["OfferName":"Name of offer provided", "Confidence Score": "Confidence score of the offer","Reasoning": "Reasoning for the offer selection"] }}
# ]

# STRICT RULES:
# - IF DOCUMENT DOESN'T LOOK LIKE SOW, RETURN [{{"ERROR": "DOCUMENT DOESN'T LOOK LIKE SOW"}}]
# - Do not generate Scope of Work and Deliverables if it is not there in the document
# - Scope of Work and Deliverables should be extracted from the document and should be | separated
# - OFFERS should be REASONED and SELECTED from the list of offers provided
# - Atleast 1 OFFER should be SELECTED
# """)

# def build_prompt(text: str) -> str:
#     return (f"""
#  You are a concise TECHNICAL WRITER for Dell Group of Companies .
#  From below statement of work,
#     1. Extract CUSTOMER NAME
#     2. generate a summary of the all the works 
#         - Extract Customer Name, Objective, Scope Of Work, Deliverables
#         - Scope of Work and Deliverables should be extracted from the document and should be | separated
#         - DO NOT GENERATE Scope of Work and Deliverables if it is not there in the document
#     3. EXTRACT all the OFFERS provided to customer in it.
#         - DO NOT GENERATE NEW OFFER
#         - OFFERS should be SELECTED from the list of offers provided only
#         - Atleast 1 OFFER should be SELECTED
#         - if no relevant OFFER is found select Any 1 offer with lower confidence score
#     4. provide confidence score for each OFFER between 0.0 to 1.0
#     5. IF DOCUMENT DOESN'T LOOK LIKE SOW, RETURN [{{"ERROR": "DOCUMENT DOESN'T LOOK LIKE SOW"}}]
# STATEMENT OF WORKS File Content:
#  <<<
# {text}
# >>>
# List of available OFFERS:
# {get_offer_list()}

# Return ONLY a JSON array of objects in this format:  
# [
#   {{
# "Customer Name": "Name of customer or company",
# "Objective": "What is the project trying to achieve? (2-5 sentences)",
# "Scope of Work": "What is overall scope of work?",
# "Deliverables": "List tangible outputs (documents, systems, integrations, prototypes, development, infrastructure provisioning)",
# "Offer": ["OfferName":"Name of offer provided", "Confidence Score": "Confidence score of the offer","Reasoning": "Reasoning for the offer selection"] }}
# ]
# """)

# def build_prompt(text: str) -> str:
#     return (f"""
# SYSTEM / INSTRUCTION FOR MODEL:
# You are a concise, precise TECHNICAL WRITER for Dell Group of Companies. Your job is to read a Statement of Work (SOW) and produce a single JSON array (only JSON, nothing else) that extracts customer information, summarizes objectives, and selects applicable OFFERS from a provided canonical list. Be literal, do not invent items, and do not hallucinate.

# INPUTS (these will be provided to you exactly as shown):
# 1) STATEMENT OF WORKS File Content:
# <<<
# {text}
# >>>

# 2) List of available OFFERS:
# <<<
# {get_offer_list()}
# >>>

# OUTPUT SPEC — REQUIRED JSON (must be valid JSON array):
# Return ONLY a JSON array of objects, one object per input SOW. Each object must follow this schema exactly:

# [
#   {{
#     "Customer Name": string,                      // or empty string if not present
#     "Objective": string,                          // 2-5 sentences summarizing project objective; omit if not present
#     "Scope of Work": string,                      // extracted text segments joined by " | " (omit if not present)
#     "Deliverables": string,                       // extracted deliverables joined by " | " (omit if not present)
#     "Offers": [                                   // array must contain at least one object (see rules)
#       {{
#         "OfferName": string,                      // must be exactly one of the provided offers
#         "ConfidenceScore": number,                // 0.0 to 1.0 (decimal) — probability this offer is present/applicable
#         "Reasoning": string                       // 1-2 short sentences citing exact SOW phrases or sections that justify selection
#       }},
#       ...
#     ]
#   }}
# ]

# SCORING RULES:
# • Provide ConfidenceScore as a decimal between 0.0 and 1.0 (e.g., 0.85).
# • Use ~0.9-1.0 for near-certain direct matches (explicit phrase or deliverable).
# • Use ~0.6-0.8 for strong but implied matches.
# • Use ~0.3-0.5 for weak or partial matches.
# • Do NOT invent scores; ground them in matching text.

# STRICT RULES (enforce these exactly):
# 1. If the document clearly is NOT an SOW (no objective/scope/deliverable language, no roles/terms of work, etc.), return exactly:
#    [{{"ERROR": "DOCUMENT DOESN'T LOOK LIKE SOW"}}]
#    and nothing else.
# 2. Do NOT generate new offers. Only select from the provided List of available OFFERS.
# 3. Always select at least 1 Offer. If no offer convincingly matches, select the single best match with a low confidence (e.g., 0.25) and explain why.
# 4. Scope of Work and Deliverables must be extracted verbatim where present — do not paraphrase them into new content. If multiple lines/clauses are relevant, join them with " | ".
# 5. If Scope of Work or Deliverables do not exist in the SOW, omit the corresponding fields from the JSON object.
# 6. Offer Reasoning must cite exact text (short quote up to ~20 words) or exact section headings from the SOW that justify selection.
# 7. Output MUST be valid JSON and NOTHING ELSE — no commentary, no extra fields, no top-level keys other than the array.

# BEHAVIORAL NOTES:
# • Prefer exact extraction over paraphrase for Scope / Deliverables. For Objective you may paraphrase but keep to 2–5 sentences.
# • If multiple customers appear, use the primary customer (the party receiving the services); list that name. If ambiguous, pick the largest named corporate entity.
# • Keep outputs compact and machine-readable.

# END OF PROMPT
#   """)