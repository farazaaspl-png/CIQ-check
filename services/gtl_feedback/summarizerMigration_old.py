import logging
import re
import textwrap
from core.genai.open_ai_client import OpenAiHelper
from config import Config as cfg
logger = logging.getLogger(__name__)


engine = OpenAiHelper(correlationid=cfg.CORR_ID_CLASSIFICATION)

LM_REMOVE_JUNKS = lambda value: re.sub(r'[^\w\s\.,\)\(\-\?\}\{\\~!@#\$%^&\*\_></"\':;\]\[\|=\+`]', '', value, flags=re.UNICODE)

def build_prompt(text: str) -> str:
    return f"""You are a concise TECHNICAL CONSULTANT for Dell Group of Companies.
From below document content, Provide a detailed and structured description of the document.
<<<
{text}
>>>

Return ONLY a JSON array of objects in this format:  
[
  {{"title": "Title of the document",
    "description": {{"Description": "What is the document trying to address? (2-5 sentences)",
                     "Key points": "What are the key points? (| separated list)",
                     "Sections": "Describe each section of the document? (| separated list)",
                     "Others": "Any other important information?"}}
   }}
]

STRICT RULES:
- Output must be JSON only (no explanations, no extra text)
"""

def __format_response(response) -> str:
    finalOutput = {}
    wrapper = textwrap.TextWrapper(width=200)
    # formatedvalue = lambda value:'\n    '.join([f"{idx+1}. {val.strip()}" for idx,val in enumerate(value.split('|'))])
    formatedvalue = lambda value:'<li>'.join([f"{val.strip()}</li>" for idx,val in enumerate(value.split('|'))])
    try:
        for key,value in response.items():
            if key == 'description':
                summary=''
                for k,v in value.items():
                    if k == 'Description':
                        # word_list = wrapper.wrap(text=v)
                        # summary+=f"<b>{k}</b>:<br>{'<br>'.join(word_list)}'<br>'"
                        summary+=f"<b>{k}</b>:<br>{v}<br>"
                    else:
                        summary+=f'<b>{k}</b>:<ul><li>{formatedvalue(v)}</ul>'
                finalOutput['description'] = LM_REMOVE_JUNKS(summary)
            if key == 'title':
                finalOutput['title'] = LM_REMOVE_JUNKS(value)
            # if key == 'offer':
            #     finalOutput['offer'] = re.sub(r'\s+', ' ',LM_REMOVE_JUNKS(value['OfferName'])).strip()
            #     finalOutput['confidence_score'] = value['Confidence Score']
        return finalOutput
    except Exception as e:
        logger.error(f"Error while formatting response: {e}", exc_info=True)

def generate_summary(text_content: str, fileid: str) -> dict:
    if not text_content or not isinstance(text_content, str):
        raise ValueError(f"Invalid text_content: must be non-empty string, got {type(text_content)}")
    
    if not fileid or not isinstance(fileid, str):
        raise ValueError(f"Invalid fileid: must be non-empty string, got {type(fileid)}")
    
    
    
    prompt = build_prompt(text_content)
    response = engine.get_json_text_to_text(prompt, fileid=fileid)
    fout = __format_response(response)
    return fout