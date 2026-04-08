from core.utility import get_custom_logger
logger = get_custom_logger(__name__)

def build_prompt(text: str, ischunked: bool = False) -> str:
    """Build prompt for LLM processing"""
    logger.info('Using prompt version:- 00000000-0000-0000-0000-000000000000')
    return f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.
SUMMARIZE below content in 2-5 sentences.

CONTENT:
<<<
{text}
>>>

OUTPUT FORMAT:
Return ONLY a JSON array of objects in this format:
[
  {{
   "summary": "What is the CONTENT trying to address? (2-5 sentences)"
  }}
]

STRICT RULES:
- summary generated should in ENGLISH LANGUAGE
"""
   
def build_consolidation_prompt(desc_list:list[str], language:str = 'english') -> str:
        """Build consolidation prompt for single file chunks"""
        logger.info('Using prompt version:- 00000000-0000-0000-0000-000000000000')
        return f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.
For provided list of summaries generated from multiple chunks of a document,
Generate a consolidated SUMMARY for the document.

List of SUMMARIES:
-----------------------------------------------------------------------------------------------------------------
{desc_list}
-----------------------------------------------------------------------------------------------------------------

OUTPUT FORMAT:
Return ONLY a JSON array of objects in this format:  
[
  {{
    "summary": "what the document is addressing? (2-5 sentences)"
  }}
]

STRICT RULES:
- summary generated should in ENGLISH LANGUAGE
"""
   
def build_prompt_for_zip(desc_list, language:str = 'english') -> str:
        """Build final consolidation prompt for all files"""
        logger.info('Using prompt version:- 00000000-0000-0000-0000-000000000000')
        return f"""
You are a TECHNICAL CONSULTANT for Dell Group of Companies.
Below are the summary of each of files in a Zip File.
Generate a SUMMARY, which explains all the contents of the Zip file.

list of SUMMARY's:
-----------------------------------------------------------------------------------------------------------------
{desc_list}
-----------------------------------------------------------------------------------------------------------------

OUTPUT FORMAT:
Return ONLY a JSON array of objects in this format:  
[
  {{
  "summary": "What the ZIP file contents are? (2-5 sentences)"    
  }}
]

STRICT RULES:
- summary generated should in ENGLISH LANGUAGE
"""