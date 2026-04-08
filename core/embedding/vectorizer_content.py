# vectorizer_content.py
import asyncio
import json
import logging
import os
import uuid
import pandas as pd, numpy as np
from pathlib import Path
from typing import Dict, List
from langchain_core.documents import Document

from core.dellattachments import DellAttachments
from core.dispatcher import SUPPORTED_FILES, Dispatcher
from core.embedding.prompt import build_prompt
from core.embedding.vectordb import VectorDatabaseManager
from core.embedding.vectorstore import CustomPgvectorStore
from core.exceptions import DocumentsAlreadyVectorized, UnableToFindAnyDocument
from core.genai.open_ai_client import EmbeddingInterface, OpenAiHelper
from core.db.crud import DatabaseManager
from core.s3_helper import StorageManager
from core.utility import chunk_list, get_custom_logger, split_text
from config import Configuration

logger = get_custom_logger(__name__)

class ContentVectorInterface:
    """Optimized content vectorization and LLM validation interface"""
    def __init__(self, table_name: str, debug: bool = False):
        self.table_name = table_name
        self.vdb = VectorDatabaseManager(debug)
        self.cfg = Configuration()
        self.cfg.load_active_config()
        embeddings = EmbeddingInterface(
            model_name = self.cfg.EMBEDDING_MODEL,
            correlationid = self.cfg.CORR_ID_EMBEDDINGS,
        )
        self.vector_store = CustomPgvectorStore(
            embeddings=embeddings,
            table_name=self.table_name,
            vectordb=self.vdb,
            embedding_length=768,
            debug=debug,
        )
 
        self.s3 = StorageManager()
 
        correlationid = uuid.uuid4()
        self.llm = OpenAiHelper(correlationid=correlationid)
       
        if debug:
            logger.setLevel(logging.DEBUG)
 
    def download_file(self, fuuid, dafileid, filepath) -> Dict:
        """Async file download wrapper."""
        fdict = {'id':dafileid,
                 'name':filepath.name,
                 'filepath':filepath
                 }
        if filepath.exists():
            return
       
        if self.s3.exists(filepath):
            filepath = self.s3.download(self.s3._make_s3_key(filepath,self.cfg.DATA_DIR))
            logger.info(f"{fuuid}-Downloaded file from S3 Storage: {filepath}")
        else:
            logger.info(f"{fuuid}-Downloading file from dell attachments")
            da = DellAttachments(self.cfg.DEBUG)
            fdict = asyncio.run(da.download(filedict=fdict))
            logger.info(f"{fuuid}-Downloaded file from dell attachments: {filepath}")
 
            self.s3.upload(fdict['filepath'])
            logger.info(f"{fuuid}- File uploaded to dell attachments")
 
 
    def __add_doc_list_to_vectorstore(self, doclist: List[Document]):
        start_idx = 0
        for size in range(0, len(doclist), self._NO_OF_BULK_LOADS):
            max_idx = min(size + self._NO_OF_BULK_LOADS, len(doclist))
            logger.info(f"Adding content chunks {start_idx}:{max_idx}")
            self.vector_store.add_documents(doclist[start_idx:max_idx])
            start_idx = max_idx
 
    def _generate_doc_list(self, content: str, metadata: Dict) -> List[Document]:
        doclist = []
        chunks = split_text(content, chunk_size=self.cfg.VECTOR_CHUNK_SIZE, chunk_overlap=self.cfg.VECTOR_OVER_LAP_SIZE)
        tot_chunks = len(chunks)
        for idx, chunk in enumerate(chunks):
            metadata['chunk_idx'] = idx
            metadata['tot_chunks'] = tot_chunks
            doclist.append(Document(page_content=chunk, metadata=metadata))
        return doclist
 
    def vectorize_documents_by_reqid(self, reqid: str) -> None:
        """Vectorize all approved dafileids for reqid - NO double vw lookup."""
        logger.info(f"Vectorizing reqid: {reqid}")
        db = DatabaseManager()
       
        # ✅ ONE-TIME lookup: gets all approved dafileids + metadata for this reqid
        docs_details_df = db.get_vwdocument_row(requestid=reqid, status='APPROVED')
        if docs_details_df.empty:
            logger.warning(f"No approved docs for reqid: {reqid}")
            return
        docs_details_df = docs_details_df[docs_details_df['filename'].apply(lambda fname: True if Path(fname).suffix.lower() in SUPPORTED_FILES else False)]
        dafileid_lst = docs_details_df['dafileid'].to_list()
        logger.info(f'Total {len(dafileid_lst)} files for the requestid-{str(reqid)}')
        self.vectorize_by_dafileids(dafileid_lst)
 
    def vectorize_by_dafileids(self, dafileids: List[str]) -> None:
        """Process single file using pre-fetched doc_info - NO DB lookup."""
        if self.table_name != self.cfg.DOCUMENT_CONTENT_STORE:
            raise ValueError(f'Invalid collection name. Function is only applicable for collection "{self.cfg.DOCUMENT_CONTENT_STORE}".')
        logger.info(f"Vectorizing documents for dafileids: {dafileids}")
 
        existingdafileids = self.vdb.get_existing_dafileids(self.table_name,dafileids)
        logger.info(f'No of Existing dafileids - {len(existingdafileids)}')
        filtered_dafileids = [item for item in dafileids if item not in existingdafileids]
       
        if len(filtered_dafileids)==0:
            logger.error(f"Cannot find any unvectorized document from dafileids: {filtered_dafileids}")
            raise DocumentsAlreadyVectorized()
       
        db = DatabaseManager()
        # ✅ ONE-TIME lookup: gets all approved dafileids + metadata for this reqid
        docs_metadata_df = db.get_vwdocument_row(sel_cols=['fuuid','dafileid','ipid','filename','dtpm_phase','document_type','ip_type','offer','offerfamily', 'practice'], dafileid=filtered_dafileids, status='APPROVED')
        if docs_metadata_df.empty:
            logger.warning("No approved docs found")
            return
       
        docs_metadata_df = docs_metadata_df.astype({'fuuid': str, 'dafileid': str, 'ipid': str})
        # docs_metadata_df = docs_metadata_df[~docs_metadata_df['dafileid'].isin(existingdafileids)]
 
        if docs_metadata_df.empty:
            logger.warning("Request files are already vectorized")
            return
       
        docs_metadata_df = docs_metadata_df[docs_metadata_df['filename'].apply(lambda fname: True if Path(fname).suffix.lower() in SUPPORTED_FILES else False)]
       
        if docs_metadata_df.empty:
            logger.warning("Request files are not supported")
            return
       
        logger.info(f'Total {docs_metadata_df.shape[0]} files to be vectorized')
        for row in docs_metadata_df.to_dict(orient='records'):
            fuuid = row['fuuid']
            dafileid = row['dafileid']
            filepath = Path(os.path.join(self.cfg.DATA_DIR, self.cfg.GTL_FLOW_DIR,str(dafileid),row['filename']))
            self.download_file(fuuid = fuuid, dafileid = dafileid, filepath = filepath)
 
            dispatcher = Dispatcher(filepath, dafileid=dafileid, analyze_images=self.cfg.IMAGE_ANALYZE_SWITCH, debug=self.cfg.DEBUG)
            extractor = dispatcher.getExtractor()
            extractor.extract_content()
            filecontent,_ = extractor.get_filecontent(True)
            logger.info(f"{fuuid} - Extracted {len(filecontent)} chars: {dafileid}")
       
            # Vectorize
            doclist = self._generate_doc_list(filecontent, row)
            if len(doclist)>0:
                self.__add_doc_list_to_vectorstore(doclist)
                logger.info(f"{fuuid} - Vectorized: {dafileid}")
 
       
    def delete_documents_by_dafileids(self, dafileidlist: list):
        self.vector_store.delete(dafileidlist)
   
    def update_vectors(self, dafileidlist: list):
        self.vector_store.delete(dafileidlist)
        self.vectorize_by_dafileids(dafileidlist)
    
    def search_content(self, query:str,k: int = 100000,threshold=0.20) -> pd.DataFrame:
        logger.info(f"Searcing document: '{query}' , k={k}")
        
        # ========== STEP 1: SEMANTIC SEARCH ==========
        results = self.vector_store.similarity_search(query, k=k, threshold=threshold)
        if not results:
            raise UnableToFindAnyDocument()
        df = pd.DataFrame(results)
        df_out = pd.json_normalize(df['cmetadata'])
        df_out = df.drop(columns=['cmetadata']).join(df_out)
        return df_out

    def search_and_rank(self, query: str, k: int = 100000) -> pd.DataFrame:
        
        df_out = self.search_content(query,k=k, threshold=self.cfg.DEEP_SEARCH_SIMILARITY_THRESHOLD)
        df_out = df_out.groupby('fuuid').agg(
            max_score = pd.NamedAgg(column="score", aggfunc="max"),
            chunks_matched = pd.NamedAgg(column="chunk_idx", aggfunc="count"),
            tot_chunks = pd.NamedAgg(column="tot_chunks", aggfunc="max")).reset_index().sort_values('max_score',ascending=False)
        df_out['coverage_ratio'] = (df_out['chunks_matched'].astype(float) /df_out['tot_chunks'].astype(float))*100.0
        df_out['scaled_max_score'] = (df_out['max_score'] - self.cfg.DEEP_SEARCH_SIMILARITY_THRESHOLD) / (df_out['max_score'].max() - self.cfg.DEEP_SEARCH_SIMILARITY_THRESHOLD)*100.0
        df_out['max_score'] = df_out['max_score']*100.0
        df_out['relevance_score'] = df_out.apply(lambda row: self.cfg.SCORE_WT*(row['scaled_max_score'])+self.cfg.COVERAGE_RATIO_WT*(row['coverage_ratio']),axis=1)
        bins = [int(buck) for buck in self.cfg.DEEP_SEARCH_RELEVANCE_BUCKET.split(',')]
        labels = ['very low', 'low', 'medium', 'high', 'very high']
        # Apply pd.cut()
        df_out['relevance'] = pd.cut(df_out['relevance_score'], bins=bins, labels=labels, include_lowest=True)
        return df_out[['fuuid', 'relevance_score', 'relevance']]

    def _flatten_respones(self,responseList):
        required_keys = ["FUUID", "relevance_score", "reason"]
        filelist = []
        for js in responseList:
            logger.debug(js)
            try:
                if (len(js.keys())==0) or ('error' in js):
                    continue
                elif  all(key in js for key in required_keys):
                    filelist.append(js)
                else:
                    [filelist.append(sjs) for key in js.keys() for sjs in js[key] if all(key in sjs for key in required_keys) and isinstance(sjs, dict)]

            except Exception as e:
                logger.error(f"Failed to parse response: {js} Error: {e}", exc_info=True)

        return filelist

    def search_and_rank_with_llm(self, query: str, k: int = 100000) -> pd.DataFrame:
        df_out = self.search_content(query,k=k, threshold=self.cfg.DEEP_SEARCH_SIMILARITY_THRESHOLD)
        df_out = df_out.groupby('fuuid').agg(
                    max_similarity_score = pd.NamedAgg(column="score", aggfunc="max"),
                    avg_similarity_score = pd.NamedAgg(column="score", aggfunc="mean"),
                    # min_similarity_score = pd.NamedAgg(column="score", aggfunc="min"),
                    chunks_matched = pd.NamedAgg(column="chunk_idx", aggfunc="count"),
                    tot_chunks = pd.NamedAgg(column="tot_chunks", aggfunc="max")).reset_index().sort_values('max_similarity_score',ascending=False)
        df_out['percentage_of_chunks_matched'] = (df_out['chunks_matched'].astype(float) /df_out['tot_chunks'].astype(float))*100.0
        df_out.drop(columns=['chunks_matched','tot_chunks'], inplace=True)
        logger.info(f"Found {len(df_out)} documents")

        db = DatabaseManager()
        docs_df = db.get_documents(fuuid=df_out['fuuid'].unique().tolist())
        logger.info(f"Found {len(docs_df)} documents in database")

        df_out['fuuid'] = df_out['fuuid'].astype(str)
        docs_df['fuuid'] = docs_df['fuuid'].astype(str)

        df_out = df_out.set_index('fuuid').join(docs_df[['fuuid','filename','title','description']].set_index('fuuid')).reset_index()
        df_out.columns =[col.upper().replace('_', ' ') for col in df_out.columns]

        # Remove None, empty strings, and NaN values
        df_out_dict = [
            {k: v for k, v in record.items() if v is not None and v != '' and not (isinstance(v, float) and np.isnan(v))} 
            for record in df_out.to_dict(orient='records')
        ]
        logger.info(f"Prepared {len(df_out_dict)} documents for LLM ranking")
        # ========== RERANKING ==========
        responseList = []
        for i, batch in enumerate(chunk_list(df_out_dict,8000)):
            logger.info(f"LLM Call {i+1}: {len(batch)} documents")
            try:
                llm_prompt = build_prompt(batch, query)
                llm_response = self.llm.get_json_text_to_text(llm_prompt)
                if len(llm_response) > 0:
                    responseList.append(llm_response)
                break
            except Exception as e:
                logger.error(f"LLM Call {i+1} failed: {e}")

        filelist = self._flatten_respones(responseList)
        df_out = pd.DataFrame(filelist)
        logger.info(f"LLM ranked {len(df_out)} documents")
        # df_out = df_out[df_out['relevance_score']>0.5]
        # if df_out.shape[0]==0:
        #     raise UnableToFindAnyDocument()

        df_out['relevance_score'] = df_out['relevance_score']*100

        bins = [int(buck) for buck in self.cfg.DEEP_SEARCH_RELEVANCE_BUCKET.split(',')]
        labels = ['very low', 'low', 'medium', 'high', 'very high']
        # Apply pd.cut()
        df_out['relevance'] = pd.cut(df_out['relevance_score'], bins=bins, labels=labels, include_lowest=True)
        df_out.rename(columns={'FUUID': 'fuuid'}, inplace=True)
        
        return df_out[['fuuid', 'relevance_score', 'relevance', 'reason']]

    #====================================================Previous Code till here======================================================

    # def search(self, query: str, k: int = 10000, include_details: bool = False) -> List[Dict]:
    #     """
    #     Main search method with LLM validation.
        
    #     Args:
    #         query: Search query
    #         k: Number of results
    #         include_details: Return full document info if True
        
    #     Returns:
    #         List of dictionaries with search results
    #     """
    #     logger.info(f"Search: '{query}', k={k}, details={include_details}")
        
    #     # Get search chunks
    #     chunks_df = self.search_content(query,k=k, threshold=self.cfg.DEEP_SEARCH_SIMILARITY_THRESHOLD)
        
    #     # Group and score with LLM
    #     documents = self._group_chunks_by_document(chunks_df)
    #     if not documents:
    #         return []
        
    #     llm_scores = self._get_llm_scores(documents, query)
    #     if not llm_scores:
    #         return []
        
    #     # Apply bucketing and return results
    #     results = self._create_results_with_bucketing(llm_scores)
        
    #     if include_details:
    #         return self._enrich_with_metadata(results)
    #     return results

    # # === Convenience Methods ===
    
    # def search_minimal(self, query: str, k: int = 10000) -> List[Dict]:
    #     """Search with minimal output"""
    #     return self.search(query, k, include_details=False)

    # def search_detailed(self, query: str, k: int = 10000) -> List[Dict]:
    #     """Search with full document details"""
    #     return self.search(query, k, include_details=True)

    # # # === Core Internal Methods ===
    
    # # def _get_search_chunks(self, query: str, k: int) -> pd.DataFrame:
    # #     """Get relevant chunks from vector store"""
    # #     try:
    # #         results = self.vector_store.similarity_search(
    # #             query, k=k, threshold=self.cfg.DEEP_SEARCH_SIMILARITY_THRESHOLD
    # #         )
    # #         if not results:
    # #             return pd.DataFrame()
    # #         df = pd.DataFrame(results)
    # #         return pd.json_normalize(df['cmetadata']).join(df.drop(columns=['cmetadata']))
    # #     except Exception as e:
    # #         logger.error(f"Vector search failed: {e}")
    # #         return pd.DataFrame()

    # def _group_chunks_by_document(self, chunks_df) -> List[Dict]:
    #     """Group chunks by fuuid for LLM processing"""
        
    #     # Fetch metadata
    #     unique_fuuids = chunks_df['fuuid'].unique().tolist()
    #     db = DatabaseManager()
    #     docs_df = db.get_documents(fuuid=unique_fuuids)
    #     doc_metadata = self._batch_fetch_metadata(unique_fuuids)
        
    #     # Group chunks
    #     files_by_fuuid = {}
    #     for _, row in chunks_df.iterrows():
    #         fuuid = row["fuuid"]
    #         if fuuid not in files_by_fuuid:
    #             meta = doc_metadata.get(fuuid, {"title": "Unknown", "description": ""})
    #             files_by_fuuid[fuuid] = {
    #                 "fuuid": fuuid,
    #                 "title": meta["title"],
    #                 "description": meta["description"],
    #                 "chunks": []
    #             }
            
    #         files_by_fuuid[fuuid]["chunks"].append({
    #             "chunk_idx": row.get("chunk_idx", 0),
    #             "content": row.get("content", ""),
    #             "score": row.get("score", 0.0)
    #         })
        
    #     return list(files_by_fuuid.values())

    # def _get_llm_scores(self, documents: List[Dict], query: str) -> List[Dict]:
    #     """Get LLM relevance scores"""
    #     if not documents:
    #         return []
        
    #     all_scores = []
    #     for i, batch in enumerate(chunk_list(documents)):
    #         logger.info(f"LLM Call {i+1}: {len(batch)} documents")
    #         try:
    #             llm_prompt = build_prompt(batch, query)
    #             llm_raw = self.llm.get_json_text_to_text(llm_prompt)
                
    #             if isinstance(llm_raw, dict) and "chunks" in llm_raw:
    #                 all_scores.extend(llm_raw["chunks"])
    #                 logger.info(f"LLM Call {i+1}: {len(llm_raw['chunks'])} scores")
    #         except Exception as e:
    #             logger.error(f"LLM Call {i+1} failed: {e}")
        
    #     return all_scores

    # def _create_results_with_bucketing(self, llm_scores: List[Dict]) -> List[Dict]:
    #     """Create results with config-based relevance bucketing"""
    #     if not llm_scores:
    #         return []
        
    #     # Get config buckets
    #     bins = [int(buck) for buck in self.cfg.DEEP_SEARCH_RELEVANCE_BUCKET.split(',')]
    #     labels = ['very low', 'low', 'medium', 'high', 'very high']
        
    #     results = []
    #     for score in llm_scores:
    #         fuuid = score.get('fuuid')
    #         if not fuuid:
    #             continue
                
    #         relevance_score = score.get('relevance_score', 0.0)
    #         score_percentage = relevance_score * 100
            
    #         # Determine relevance category
    #         relevance = 'very low'
    #         for i in range(len(bins) - 1):
    #             if bins[i] <= score_percentage < bins[i + 1]:
    #                 relevance = labels[i]
    #                 break
    #         if score_percentage >= bins[-1]:
    #             relevance = labels[-1]
            
    #         results.append({
    #             "fuuid": fuuid,
    #             "relevance_score": relevance_score,
    #             "relevance": relevance
    #         })
        
    #     results.sort(key=lambda x: x['relevance_score'], reverse=True)
    #     return results

    # def _enrich_with_metadata(self, results: List[Dict]) -> List[Dict]:
    #     """Enrich results with document metadata"""
    #     if not results:
    #         return []
        
    #     fuuids = [r['fuuid'] for r in results]
    #     metadata = self._batch_fetch_metadata(fuuids)
        
    #     enriched_results = []
    #     for result in results:
    #         fuuid = result['fuuid']
    #         meta = metadata.get(fuuid, {})
            
    #         enriched_result = {
    #             "fuuid": fuuid,
    #             "relevance_score": result['relevance_score'],
    #             "relevance": result['relevance'],
    #             "title": meta.get('title', 'Unknown'),
    #             "filename": meta.get('filename', ''),
    #             "description": meta.get('description', ''),
    #             "dafileid": meta.get('dafileid', '')
    #         }
    #         enriched_results.append(enriched_result)
        
    #     return enriched_results

    # def _batch_fetch_metadata(self, fuuids: List[str]) -> Dict[str, Dict]:
    #     """Batch fetch document metadata"""
    #     metadata = {}
    #     db = DatabaseManager()
        
    #     for fuuid in fuuids:
    #         try:
    #             docs_df = db.get_documents(fuuid=fuuid)
    #             if not docs_df.empty:
    #                 doc_row = docs_df.iloc[0]
    #                 metadata[fuuid] = {
    #                     "title": doc_row.get("title", "Unknown"),
    #                     "filename": doc_row.get("filename", ""),
    #                     "description": doc_row.get("description", ""),
    #                     "dafileid": doc_row.get("dafileid", "")
    #                 }
    #         except Exception as e:
    #             logger.warning(f"Failed to fetch metadata for {fuuid}: {e}")
    #             metadata[fuuid] = {"title": "Unknown", "filename": "", "description": "", "dafileid": ""}
        
    #     return metadata

    # # === Essential Vectorization Methods ===
    
    # def vectorize_by_dafileids(self, dafileids: List[str]) -> None:
    #     """Vectorize documents by dafileids"""
    #     if self.table_name != self.cfg.DOCUMENT_CONTENT_STORE:
    #         raise ValueError(f'Invalid collection name for {self.table_name}')
        
    #     # Get non-vectorized documents
    #     existing = self.vdb.get_existing_dafileids(self.table_name, dafileids)
    #     filtered = [item for item in dafileids if item not in existing]
        
    #     if not filtered:
    #         logger.warning("All documents already vectorized")
    #         return
        
    #     # Process documents
    #     db = DatabaseManager()
    #     docs_df = db.get_vwdocument_row(dafileid=filtered, status='APPROVED')
        
    #     if docs_df.empty:
    #         logger.warning("No approved documents found")
    #         return
        
    #     # Vectorize each document
    #     for _, row in docs_df.iterrows():
    #         self._vectorize_single_document(row)

    # def _vectorize_single_document(self, doc_row):
    #     """Vectorize a single document"""
    #     try:
    #         # Download and extract content
    #         filepath = self._get_document_path(doc_row)
    #         self._download_document(doc_row['fuuid'], doc_row['dafileid'], filepath)
            
    #         content = self._extract_content(filepath)
    #         if content:
    #             chunks = self._create_chunks(content, doc_row)
    #             self.vector_store.add_documents(chunks)
    #             logger.info(f"Vectorized: {doc_row['dafileid']}")
    #     except Exception as e:
    #         logger.error(f"Failed to vectorize {doc_row['dafileid']}: {e}")

    # def _get_document_path(self, doc_row):
    #     """Get document file path"""
    #     return Path(os.path.join(
    #         self.cfg.DATA_DIR, 
    #         self.cfg.GTL_FLOW_DIR, 
    #         str(doc_row['dafileid']), 
    #         doc_row['filename']
    #     ))

    # def _download_document(self, fuuid, dafileid, filepath):
    #     """Download document if needed"""
    #     if filepath.exists():
    #         return
            
    #     from core.s3_helper import StorageManager
    #     s3 = StorageManager()
        
    #     if s3.exists(filepath):
    #         s3.download(s3._make_s3_key(filepath, self.cfg.DATA_DIR))
    #     else:
    #         import asyncio
    #         from core.dellattachments import DellAttachments
    #         da = DellAttachments(self.cfg.DEBUG)
    #         fdict = {'id': dafileid, 'name': filepath.name, 'filepath': filepath}
    #         asyncio.run(da.download(filedict=fdict))

    # def _extract_content(self, filepath):
    #     """Extract content from document"""
    #     from core.dispatcher import Dispatcher
        
    #     dispatcher = Dispatcher(filepath, dafileid=None, debug=False)
    #     extractor = dispatcher.getExtractor()
    #     extractor.extract_content()
    #     content, _ = extractor.get_filecontent(True)
    #     return content

    # def _create_chunks(self, content, metadata):
    #     """Create document chunks"""
        
    #     chunks = split_text(
    #         content, 
    #         chunk_size=self.cfg.VECTOR_CHUNK_SIZE, 
    #         chunk_overlap=self.cfg.VECTOR_OVER_LAP_SIZE
    #     )
        
    #     doc_chunks = []
    #     for idx, chunk in enumerate(chunks):
    #         chunk_metadata = metadata.copy()
    #         chunk_metadata['chunk_idx'] = idx
    #         doc_chunks.append(Document(page_content=chunk, metadata=chunk_metadata))
        
    #     return doc_chunks

    # def delete_documents_by_dafileids(self, dafileidlist: list):
    #     """Delete documents by dafileids"""
    #     self.vector_store.delete(dafileidlist)

    # # === Backward Compatibility ===
    
    # def search_and_rank_v2(self, query: str, k: int = 10000) -> List[Dict]:
    #     """Backward compatibility - same as search_minimal"""
    #     return self.search_minimal(query, k)

    # def search_with_details(self, query: str, k: int = 10000) -> List[Dict]:
    #     """Backward compatibility - same as search_detailed"""
    #     return self.search_detailed(query, k)