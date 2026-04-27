import asyncio, nest_asyncio, logging
import json, os
from pathlib import Path
import uuid, pandas as pd

from core.genai.open_ai_client import OpenAiHelper
from services.consultant_recommendation.prompt import build_offer_prompt,build_offer_consolidation_prompt,get_offer_list
from core.embedding.vectorizer_content import ContentVectorInterface
from core.db.crud import DatabaseManager
from core.exceptions import UnableToGenerateSummary
from core.utility import get_custom_logger, split_text
from config import Config as cfg
from config import Configuration

logger = get_custom_logger(__name__)
nest_asyncio.apply()


class Classfication:

    catalogue_offer_df = None
    offerswithhints = None
    offerslist = []

    db = DatabaseManager(cfg.DEBUG)

    def __init__(self, fileid: uuid, textContent: str, debug: bool = False):
        self.textContent = textContent
        self.cfg = Configuration()
        self.cfg.load_active_config()
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.fileid = fileid
        self.response = None
        self.finalOutput = {}
        self.engine = OpenAiHelper(correlationid=self.cfg.CORR_ID_SUMMARIZATION)

        self.negative_list = ['None', '', 'Not specified', 'Not Available',
                              'Not explicitly mentioned', 'Not Provided',
                              'Not specified in the document', 'None explicitly stated']

        self.semaphore = asyncio.Semaphore(os.cpu_count() * 4)

        # Initialize class-level offer data
        if Classfication.catalogue_offer_df is None:
            Classfication.catalogue_offer_df = Classfication.db.get_vwofferfamilydata()
            Classfication.catalogue_offer_df['offer'] = Classfication.catalogue_offer_df['offer'].str.upper()
            Classfication.offerslist = Classfication.catalogue_offer_df['offer'].drop_duplicates().to_list()
            Classfication.offerswithhints = get_offer_list(withhints=True)

        self.vec = ContentVectorInterface(cfg.DOCUMENT_CONTENT_STORE)

        if self.debug:
            logger.setLevel(logging.DEBUG)

    async def _call_llm_offers_async(self, text: str, idx: int = 0, ischunked: bool = False,
                                     vector_search_results: list = None, suggested_offers: list = None) -> dict:
        """Call LLM for offer extraction and scoring"""
        async with self.semaphore:
            result = self.vec.search_content(text, k=5, threshold=0.2)
            suggested_offers = result['offer'].unique().tolist()
            vector_search_results = result['content'].tolist()
            prompt = build_offer_prompt(text,offers=Classfication.offerswithhints,ischunked=ischunked,
                vector_search_results=vector_search_results,
                suggested_offers=suggested_offers
            )

            response = self.engine.get_json_text_to_text(prompt,fileid=self.fileid,requestid=f'offer-chunk-{str(idx)}-{str(ischunked)}')

            logger.debug(f'{self.fileid}-->Offer LLM Call completed for chunk {idx}')

            if isinstance(response, list) and response:
                response = response[0]
            if not isinstance(response, dict):
                raise ValueError(f"LLM returned invalid offer response for chunk {idx}: {response}")

            logger.info(f'{self.fileid}-->Offers predicted successfully for Chunk: {idx}')
            return response

    async def summarize_in_chunks_async(self) -> dict:
        """Main async method to process offer chunks and consolidate"""
        chunklst = split_text(self.textContent,chunk_size=self.cfg.CHUNK_SIZE_RECOMMENDATION,chunk_overlap=self.cfg.OVER_LAP_SIZE_RECOMMENDATION)
        logger.info(f'{self.fileid}-->{len(chunklst)} chunks generated')
        if len(chunklst) == 1:
            return await self._call_llm_offers_async(text=self.textContent)

        task = [asyncio.create_task(self._call_llm_offers_async(text=chunk, idx=idx + 1, ischunked=True)) for idx, chunk in enumerate(chunklst)]
        offer_response_list = await asyncio.gather(*task, return_exceptions=False)
        logger.info(f'{self.fileid}-->Offers generated for each chunk successfully')

        predicted_offer_lst = []
        offers_keys = {'offers', 'offer'}

        for idx, response in enumerate(offer_response_list):
            for key, value in response.items():
                if key.lower() in offers_keys:
                    if isinstance(value, list):
                        predicted_offer_lst.extend(value)
                    else:
                        predicted_offer_lst.append(value)
        
        predicted_offerdf = pd.DataFrame(predicted_offer_lst)

        if predicted_offerdf.empty:
            logger.warning(f'{self.fileid}-->No offers predicted from any chunk')
            offer_lst = []
        else:
            predicted_offerdf = predicted_offerdf[predicted_offerdf['OfferName'].isin(Classfication.offerslist)]
            predicted_offerdf['Relevance Score'] = predicted_offerdf['Relevance Score'].astype('float')

            predicted_offerdf = predicted_offerdf.groupby('OfferName').agg(
                RelevanceScore=pd.NamedAgg(column="Relevance Score", aggfunc="max"),
                NoOfChunks=pd.NamedAgg(column="Relevance Score", aggfunc="size"),
                Reason=pd.NamedAgg(column="Reason", aggfunc=lambda x: '. '.join(x))
            ).reset_index().sort_values(['RelevanceScore', 'NoOfChunks'], ascending=False).rename(columns={'RelevanceScore': 'Relevance Score'})

            offer_lst = predicted_offerdf.to_dict('records')
        
        if len(offer_lst) > 0:
            consolidation_prompt = build_offer_consolidation_prompt(offer_lst, language='English')
            logger.debug(f'{self.fileid}-->Offer Consolidation Prompt generated{consolidation_prompt}')

            response = self.engine.get_json_text_to_text(consolidation_prompt,fileid=self.fileid,requestid="consolidated_offer")

            if isinstance(response, list) and response:
                response = response[0]
            if not isinstance(response, dict):
                raise ValueError(f"LLM returned invalid offer consolidation response: {response}")

            logger.info(f'{self.fileid}-->Offers consolidated successfully')
        else:
            response = {'Offer': []}

        logger.info(f'{self.fileid}-->Consolidated Offers generated successfully')
        logger.debug(f'{self.fileid}-->Final Offer Response: {response}')
        return response

    def summarize(self) -> dict:
        """Main method to run offer summarizer pipeline."""
        logger.info(f'{self.fileid}-->Processing offers with chunking')
        self.response = asyncio.run(self.summarize_in_chunks_async())

        self.__format_response()

        if not self.finalOutput:
            self.response = asyncio.run(self.summarize_in_chunks_async())
            self.__format_response()

        if not self.finalOutput:
            raise UnableToGenerateSummary(self.fileid)

        logger.debug(f'{self.fileid}-->Offer Final Output: {self.finalOutput}')
        logger.info(f'{self.fileid}-->Offer Summarization Completed')
        if self.debug:
            self.saveJson()

        return self.finalOutput

    def __format_response(self) -> str:
        """Format offer response into HTML table"""
        try:
            for key, value in self.response.items():
                logger.debug(f"{self.fileid}-->In Format Offer Response:{key}-->{value}")
                if value is None or value in self.negative_list:
                    continue

                lk = key.lower()
                if lk in ['offers', 'offer']:
                    html_table_header = "<b>Sow inclines towards below Offers:</b><br><table>"
                    html_table_header += "<tr><th>Offer Name</th><th>Relevance Score</th></tr>"
                    html_table = ''

                    if isinstance(value, list):
                        value = sorted(value, key=lambda d: float(d.get("Relevance Score", d.get('RelevanceScore'))), reverse=True)

                        for off in value:
                            if 'RelevanceScore' in off:
                                off['Relevance Score'] = off.pop('RelevanceScore')

                            conf_score = float(off.get('Relevance Score'))

                            if (off.get('OfferName') in Classfication.offerslist) and conf_score > self.cfg.THRESHOLD_SOW_OFFER_SCORE:
                                html_table += f"<tr><td>{off.get('OfferName').title()}</td><td>{conf_score * 100}%</td></tr>"

                    else:
                        conf_score = float(value.get('Relevance Score', value.get('RelevanceScore')))
                        if (value.get('OfferName') in Classfication.offerslist) and conf_score > self.cfg.THRESHOLD_SOW_OFFER_SCORE:
                            html_table += f"<tr><td>{value.get('OfferName').title()}</td><td>{conf_score * 100}%</td></tr>"

                    if len(html_table) > 0:
                        self.finalOutput['offer_summary'] = html_table_header + html_table + "</table>"

                    self.finalOutput['offer'] = value

        except Exception as e:
            logger.error(f"{self.fileid}-->Error while formatting offer response: {e}", exc_info=True)

    def saveJson(self):
        """Save JSON output"""
        OUTPUT_DIR = Path(os.path.join(self.cfg.DATA_DIR, cfg.CONSULTANT_FLOW_DIR, self.fileid))
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        try:
            json_path = os.path.join(OUTPUT_DIR, f"OfferSummary.json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(self.finalOutput, f, indent=2)
            logger.info(f"{self.fileid}-->Json File Saved at {json_path}")
        except Exception as e:
            logger.error(f"{self.fileid}-->Saving JSON failed at '{json_path}': {e}", exc_info=True)