import logging
from pathlib import Path
from typing import Dict, List
import pandas as pd
from core.dispatcher import Dispatcher
from core.embedding.vectordb import VectorDatabaseManager
from core.embedding.vectorstore import CustomPgvectorStore
from core.genai.open_ai_client import EmbeddingInterface
from core.utility import split_text, get_custom_logger
from core.exceptions import EmptyFileError
from config import Configuration
from pgvector.sqlalchemy import Vector
from sqlalchemy import Integer, bindparam, text
from rapidfuzz import fuzz

logger = get_custom_logger(__name__)


class DocumentIdentifier:

    def __init__(self,textContent: str,debug: bool = False,**kwargs):
        self.textContent = textContent
        self.cfg = Configuration()
        self.cfg.load_active_config()
        self.dafileid = kwargs.get('daFileId')  # Use 'uuid' from payload
        # self.request_id = kwargs.get('requestId')  # Use 'uuid' from payload
        # self.fuuid = kwargs.get('uuid')
        self.filename = kwargs.get('name')
        self.iptypes = kwargs.get('ipTypes', []) 
        self.phase = kwargs.get('phase')
        self.finalOutput = {}
        self.table_name       = self.cfg.DOCUMENT_CONTENT_STORE
        self.cosine_threshold    = self.cfg.VECTOR_SEARCH_SIMILARITY_THRESHOLD
        self.fuzzy_threshold    = self.cfg.FUZZ_RATIO_SIMILARITY_THRESHOLD
        self.chunk_size       = self.cfg.VECTOR_CHUNK_SIZE
        self.chunk_overlap    = self.cfg.VECTOR_OVER_LAP_SIZE
        # self.min_coverage     = min_coverage
        # self.embed_batch_size = embed_batch_size
        # self.top_k_per_chunk  = top_k_per_chunk

        vdb        = VectorDatabaseManager(debug)
        embeddings = EmbeddingInterface(model_name = self.cfg.EMBEDDING_MODEL,correlationid = self.cfg.CORR_ID_EMBEDDINGS)
        self.vector_store = CustomPgvectorStore(
            embeddings       = embeddings,
            table_name       = self.table_name,
            vectordb         = vdb,
            embedding_length = 768,
            recreate_table   = False,
            debug            = debug
        )

        if debug:
            logger.setLevel(logging.DEBUG)

    # def _extract_and_chunk_file(self,filepath : Path,dafileid : str = None) -> List[str]:
        
    #     dispatcher = Dispatcher(filepath,dafileid = dafileid,analyze_images = self.cfg.IMAGE_ANALYZE_SWITCH,debug          = self.cfg.DEBUG)
    #     extractor = dispatcher.getExtractor()
    #     extractor.extract_content()

    #     file_content,listoftabledata = extractor.get_filecontent(True)
    #     # file_content = raw[0] if isinstance(raw, tuple) else raw

    #     logger.info(f"Extracted {len(file_content):,} chars from {filepath.name}")

    #     if not file_content or file_content.strip() == "":
    #         raise EmptyFileError()

    #     # chunks = split_text(file_content,chunk_size= self.chunk_size,chunk_overlap = self.chunk_overlap)
    #     # logger.info(f"Split into {len(chunks)} chunks")
    #     # return chunks
    #     return file_content

    def _chunk_file(self,file_content :str):
        chunks = split_text(file_content,chunk_size= self.chunk_size,chunk_overlap = self.chunk_overlap)
        logger.info(f"Split into {len(chunks)} chunks")
        return chunks

    def _embed_chunks(self, chunks: List[str]) -> List:
        """Embed chunks in batches respecting API batch size limit."""
        total            = len(chunks)
        chunk_embeddings = []
        embed_batch_size = 32

        for batch_start in range(0, total, embed_batch_size):
            batch_end = min(batch_start + embed_batch_size, total)
            batch     = chunks[batch_start:batch_end]

            logger.info(f"embedding batch "f"{batch_start + 1}-{batch_end}/{total}")

            batch_embeddings = self.vector_store.embeddings.embed_documents(batch)
            chunk_embeddings.extend(batch_embeddings)

        logger.info(f"All embeddings ready: "f"vectors={len(chunk_embeddings)}, "f"size={len(chunk_embeddings[0])}")
        return chunk_embeddings

    def _vector_search(self,chunk_vector : list,k: int = 5,threshold : float = 0.55,**filters) -> list:
        """Cosine similarity search via pgvector."""
        conditions = []

        #Fileformat filter
        file_format = filters.get('file_format')
        conditions.append(f"cmetadata->>'filename' LIKE '%{file_format}'")

        # Phase filter
        phase = filters.get('phase')
        if phase:
            values_str = "','".join(phase)
            conditions.append(f"cmetadata->>'phase' in ('{values_str}')")

        # IP type filter
        iptypes = filters.get('ip_types')
        if iptypes:
            values_str = "','".join(iptypes)
            conditions.append(f"cmetadata->>'ip_type' in ('{values_str}')")

        # Threshold filter
        if threshold:
            conditions.append(f"1-(embedding <=> (:chunk_embedding)::vector) > {threshold}")

        WHERE_CLAUSE = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''

        sql = text(f"""
            SELECT content, cmetadata,
                   1-(embedding <=> (:chunk_embedding)::vector) AS cos_score
            FROM {self.table_name}
            {WHERE_CLAUSE}
            ORDER BY embedding <=> (:chunk_embedding)::vector ASC
            LIMIT :top_k;
        """).bindparams(
            bindparam("chunk_embedding", type_=Vector),
            bindparam("top_k", type_=Integer)
        )

        params = {"chunk_embedding": chunk_vector, "top_k": k}

        with self.vector_store.vecdb.engine.connect() as conn:
            result = conn.execute(sql, params)
            return result.mappings().all()

    def _similarity_search(self,input_chunk_text : str,repo_chunk_text  : str,threshold: float):
    
        """fuzz.ratio similarity"""
        similarity_score = fuzz.ratio(input_chunk_text.lower().strip(),repo_chunk_text.lower().strip())

        if similarity_score < threshold :
            logger.debug(f"Revalidation FAILED: "f"score={similarity_score:.2f} < "f"threshold={threshold * 100:.2f}")
            return None

        logger.debug(f"Revalidation PASSED: score={similarity_score:.2f}")
        return similarity_score

    def _search_template(self,chunks: List[str],chunk_embeddings : List,**filters) -> pd.DataFrame:
        raw_results = []
        total       = len(chunks)
        passed      = 0
        failed_at_cosine  = 0
        failed_at_fuzzy  = 0

        for chunk_idx, (chunk_text, chunk_vector) in enumerate(zip(chunks, chunk_embeddings)):
            logger.debug(f"Processing chunk {chunk_idx + 1}/{total}")

            # -- Step 1: cosine search --------------------------------
            results = self._vector_search(chunk_vector = chunk_vector,k =5,threshold = self.cosine_threshold,**filters)

            if not results:
                logger.debug(f"Chunk {chunk_idx} cosine: no match")
                failed_at_cosine += 1
                continue

            logger.debug(f"Chunk {chunk_idx} cosine candidates: {len(results)}")

            # -- Step 2: fuzz.ratio all candidates --------------------
            scored = [(self._similarity_search(chunk_text,top_chunk["content"],
                        self.fuzzy_threshold),top_chunk) for top_chunk in results]

            valid = [(score, c) for score, c in scored if score is not None]

            if not valid:
                logger.debug(f"Chunk {chunk_idx} fuzz FAILED "f"for all {len(results)} candidates")
                failed_at_fuzzy += 1
                continue

            best_score, best_chunk = max(valid, key=lambda x: x[0])
            metadata = dict(best_chunk["cmetadata"])

            logger.debug(f"Chunk {chunk_idx} PASSED → "f"dafileid={metadata.get('dafileid')} | "f"fuzz={best_score:.2f}")

            raw_results.append({
                "input_chunk_idx" : chunk_idx,
                "dafileid"        : str(metadata.get("dafileid")),
                "filename"        : metadata.get("filename", ""),
                "score"           : best_score,
            })
            passed += 1

        logger.info(f"Search complete: total={total} | "f"passed={passed} | "f"failed_cosine={failed_at_cosine} | "f"failed_fuzzy={failed_at_fuzzy}"
        )

        df_out = pd.DataFrame(raw_results)
        if df_out.empty:
            logger.warning("No chunks passed both stages.")
            return pd.DataFrame()

        return df_out

    def _search_and_rank_files(self) -> dict:
        """Main method to run Template Identification pipeline."""

        min_coverage = 0.70
        if self.iptypes:
            self.iptypes = [ip for ip in self.iptypes if ip is not None] or None


        chunks = self._chunk_file(self.textContent)

        total_input_chunks = len(chunks)
        logger.info(f"Total input chunks: {total_input_chunks}")

        chunk_embeddings = self._embed_chunks(chunks)
        self.chunk_test = chunks
        self.chunk_embeddings_test = chunk_embeddings
        if not chunk_embeddings:
            logger.warning("No embeddings produced.")
            return {}

        filters = {'file_format': Path(self.filename).suffix.lower(),'ip_types': self.iptypes,'phase':self.phase}

        logger.info(f"Filters -> " f"file_format={filters['file_format']} | "f"ip_types={filters['ip_types']}")

        df_raw = self._search_template(chunks = chunks,chunk_embeddings = chunk_embeddings,**filters)

        if df_raw.empty:
            logger.warning("No matches found.")
            return {}

        # Aggregate
        df_out = df_raw.groupby('dafileid').agg(
            filename       = pd.NamedAgg(column="filename",        aggfunc="first"),
            chunks_matched = pd.NamedAgg(column="input_chunk_idx", aggfunc="count"),
            max_score      = pd.NamedAgg(column="score",           aggfunc="max"),
            avg_score      = pd.NamedAgg(column="score",           aggfunc="mean"),
        ).reset_index().sort_values(['chunks_matched', 'max_score'],ascending = [False, False])

        df_out['coverage_pct'] = (df_out['chunks_matched'] / total_input_chunks) * 100.0

        # Min coverage is — 70%
        min_chunks_required = total_input_chunks * min_coverage
        df_out = df_out[df_out['chunks_matched'] >= min_chunks_required]

        if df_out.empty:
            logger.warning(f"No document met min_coverage="f"{min_coverage * 100:.0f}%")
            return {}
        
        self.finalOutput = df_out.iloc[0].to_dict()

        logger.info(f"Template Identification Complete -> "
            f"dafileid: {df_out.iloc[0]['dafileid']} | "
            f"filename: {df_out.iloc[0]['filename']} | "
            f"chunks_matched: {df_out.iloc[0]['chunks_matched']}"
            f"/{total_input_chunks}"
        )

        # return df_out[['dafileid', 'filename', 'chunks_matched', 'coverage_pct']]
        return self.finalOutput
