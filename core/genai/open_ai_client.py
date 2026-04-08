# import os
# from pathlib import Path
import random
import json, uuid
import functools
import time
# import torch
from typing import List
from json.decoder import JSONDecodeError
from openai import AuthenticationError, InternalServerError, NotFoundError, OpenAI, RateLimitError
import openai
import logging

from langchain_core.embeddings import Embeddings
# from transformers import AutoModel, AutoTokenizer
# from sentence_transformers import SentenceTransformer
# import torch.nn.functional as F

import core.genai.dell_helper as dh
from core.db.crud import DatabaseManager
from config import Configuration
from core.utility import get_custom_logger,count_tokens,count_vision_tokens
from config import Config as cfg

openai.verify_ssl_certs = False  # for testing only!

logger = get_custom_logger(__name__)
# logger.propagate = False

# AVAILABLE_MODELS = ["mixtral-8x7b-instruct-v01", "llamaguard-7b", "mistral-7b-instruct-v03", "phi-3-mini-128k-instruct", 
#                     "phi-3-5-moe-instruct", "llama-3-8b-instruct", "llama-3-1-8b-instruct", "llama-3-2-3b-instruct",
#                     "codellama-13b-instruct", "codestral-22b-v0-1", "llama-3-3-70b-instruct", "gpt-oss-120b"]
# VISION_MODELS = ["florence-2-large-ft","llama-3-2-11b-vision-instruct","llava-v1-6-34b-hf-vllm","pixtral-12b-2409"]
# EMBEDDING_MODELS = ["nomic-embed-text-v1-5"]

_SELECTED_TEXT_TO_TEXT_MODEL = cfg.TEXT_TO_TEXT_MODEL
# _SELECTED_TEXT_TO_TEXT_MODEL = "gpt-oss-120b"
_SELECTED_IMAGE_TO_TEXT_MODEL = cfg.IMAGE_TO_TEXT_MODEL

STREAM = False
BASE_URL=cfg.GEN_AI_API_LINK


def api_logger(func):
    """Decorator to log API calls with timing and status."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        attempt = 1
        start_time = time.time()
        while True:
            try:
                result = func(*args, **kwargs)
                status = "success"
                return result
            except (RateLimitError,AuthenticationError,InternalServerError) as exc:
                if attempt >= cfg.MAX_API_RETRIES:
                    logger.error(
                        f"Rate limit exceeded after {cfg.MAX_API_RETRIES} attempts "
                        f"for {func.__name__}: {exc}",
                        exc_info=True,
                    )
                    status = "failed"
                    result = str(exc)
                    raise exc
                
                logger.warning(
                    f"{func.__name__} :--> Attempt {attempt}/{cfg.MAX_API_RETRIES} Failed with {exc}."
                    f"Retrying in {cfg.API_RETRY_BACKOFF}s...",
                    exc_info=True,
                )
                status = "token-expired" if isinstance(exc,AuthenticationError) else "retry"  
                result = str(exc)
                attempt += 1
                continue
            except Exception as e:
                status = "failed"
                result = str(e)
                raise e
            finally:
                if cfg.LOG_API_CALLS and len(args) > 0 and args[1]:
                    exec_time = round(time.time() - start_time, 4)
                    if func.__name__ == 'get_json_text_to_text':
                        token_size = count_tokens(args[1],str(result) if status=="success" else '')
                    else:
                        token_size = count_vision_tokens(args[1],args[2],str(result) if status=="success" else '')

                    db=DatabaseManager()
                    db.log_api_call(
                        func_name=func.__name__,
                        fileid=kwargs.get('fileid', '4faf903e-95be-48ad-a384-edce50edcd31'),
                        requestid=kwargs.get('requestid', "Not Provided"),
                        token_size=token_size,
                        prompt_length=len(args[1]),
                        prompt=args[1], 
                        exec_time=exec_time,
                        status=status,
                        response=json.dumps(result)
                    )

                if status=="retry":
                    time.sleep(cfg.API_RETRY_BACKOFF+random.randint(5, 15))
                elif status == "token-expired":
                    args[0].OpenAiClient = None

    return wrapper

class OpenAiHelper:

    def __init__(self, correlationid: uuid = None, streaming: bool = False):
        self.correlation_id = str(correlationid) if correlationid else str(uuid.uuid4())
        self._streaming = streaming
        self.OpenAiClient = None
        self.db=DatabaseManager()
    
    def __setOpenAiClient(self):
        if self.OpenAiClient is None:
            self.OpenAiClient = self.__getOpenAiClient()

    def __getOpenAiClient(self):
        dh.update_certifi()
        default_headers = dh.get_default_headers_based_on_authentication(self.correlation_id)
        http_client= dh.get_http_client_based_on_authentication()

        client = OpenAI(
            base_url = BASE_URL,
            http_client = http_client,
            api_key='',  # This is replaced with the token generation based on the authentication provider, this should be left blank
            default_headers = default_headers
        )
        return client
    
    @api_logger
    def get_json_text_to_text(self,p_prompt,p_model=_SELECTED_TEXT_TO_TEXT_MODEL,**kwargs):
        self.__setOpenAiClient()
        SYSTEM_PROMPT = "You are a careful, precise JSON generator."
        try:
            # print(self.correlation_id)
            completion = self.OpenAiClient.chat.completions.create(
                            model=p_model,
                            messages = [
                                {"role": "system", "content": SYSTEM_PROMPT},
                                {"role": "user", "content": p_prompt},
                            ],
                            stream=False,
                            response_format= {"type": "json_object"},
                        )
            response = json.loads(completion.choices[0].message.content)
        except JSONDecodeError as e:
            logger.warning(f"Failed to decode JSON: {e}",exc_info=True)
            logger.warning('/**********************ORIG response*****************************/')
            logger.warning(completion.choices[0].message.content)
            response = {}
        # print(completion.choices[0].text)
        return response
    
    @api_logger
    def get_json_image_to_text(self, p_sys_prompt: str, p_img_url: str, p_model: str = _SELECTED_IMAGE_TO_TEXT_MODEL,**kwargs) -> str:
        self.__setOpenAiClient()
        use_stream = self._streaming
        try:
            completion = self.OpenAiClient.chat.completions.create(
                                    model=p_model,
                                    messages=[{
                                        "role": "user",
                                        "content": [
                                            {"type": "text", "text": p_sys_prompt},
                                            {"type": "image_url", "image_url": {"url": p_img_url}},
                                        ]
                                    }],
                                    stream=use_stream,
                                    response_format= {"type": "json_object"}
                                )
            response = json.loads(completion.choices[0].message.content)
        except JSONDecodeError as e:
            logger.warning(f"Failed to decode JSON: {e}",exc_info=True)
            logger.warning('/**********************ORIG response*****************************/')
            logger.warning(completion.choices[0].message.content)
            # response = completion.choices[0].message.content
            response = {}
        except NotFoundError as e:
            logger.warning(f"Model didn't provide a response: {e}",exc_info=True)
            response = {}
        except RateLimitError as e:
            raise e
        return response
    
    @api_logger
    def get_raw_response(self, p_sys_prompt: str, p_model: str = "gpt-oss-120b"):
        """Return full raw OpenAI response."""
        # self.__validate_model(p_txt_model =p_model)
        self.__setOpenAiClient()
        response =self.OpenAiClient.chat.completions.create(
                        model=p_model,
                        messages=[{"role": "user", "content": p_sys_prompt}],
                        stream=self._streaming
                    )
        return response
    
    def get_embedding(self, p_text, p_model="nomic-embed-text-v1-5"):
        # self.__validate_model(p_embedding_model=p_model)
        self.__setOpenAiClient()
        emb = self.OpenAiClient.embeddings.create(input = p_text, model=p_model)
        return emb.data[0].embedding
  

# if cfg.EMBED_LOCAL:
#     model_path = os.path.join(r"core/tokenizers",cfg.EMBEDDING_MODEL)
#     tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
#     model = AutoModel.from_pretrained(model_path, local_files_only=True)
#     device = torch.device("cpu")
#     model = model.to(device)               # explicit CPU placement
#     model.eval() 

#     torch.set_num_threads(os.cpu_count())
#     torch.set_num_interop_threads(os.cpu_count())    
    # model = SentenceTransformer(model_path, local_files_only=True)


class EmbeddingInterface(Embeddings):
      
    def __init__(self, model_name = r"nomic-embed-text-v1", correlationid: uuid = None, debug: bool =False):
        self.correlation_id = str(correlationid) if correlationid else str(uuid.uuid4())
        self.debug = debug
        
        self.cfg=Configuration()
        self.cfg.load_active_config()

        if self.debug:
            logger.setLevel(logging.DEBUG)
        if not self.cfg.EMBED_LOCAL:
            self.model_name = model_name
            dh.update_certifi()
            default_headers = dh.get_default_headers_based_on_authentication(self.correlation_id)
            http_client= dh.get_http_client_based_on_authentication()
            self.client = OpenAI(
                base_url = BASE_URL,
                http_client = http_client,
                api_key='',  
                default_headers = default_headers
            )

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """Embed search docs."""
        if not self.cfg.EMBED_LOCAL:
            resp = self.client.embeddings.create(model=self.model_name,
                                                 input=texts)
            return [a.embedding for a in  resp.data]
        
        # encoded = tokenizer(
        #     texts,
        #     padding=True,
        #     truncation=True,
        #     return_tensors="pt",
        # ).to("cpu")     
        # with torch.inference_mode():
        #     outputs = model(**encoded)
            
        # batch_emb = outputs.last_hidden_state.mean(dim=1)
        # norm_emb = F.normalize(batch_emb, p=2, dim=1)
        # return norm_emb.cpu().numpy().tolist()
        
        
    def embed_query(self, text: str) -> List[float]:
        """Embed query text."""
        return self.embed_documents([text])[0]
        