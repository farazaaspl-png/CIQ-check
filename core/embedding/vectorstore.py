#vectorstore.py
import logging


import pandas as pd
from pgvector.sqlalchemy import Vector
from sqlalchemy import bindparam
from sqlalchemy import Integer, text
from langchain_core.embeddings import Embeddings
from langchain_core.documents import Document

from core.embedding.vectordb import VectorDatabaseManager
from core.utility import get_custom_logger
logger = get_custom_logger(__name__)
                  
class CustomPgvectorStore:
    """Custom pgvector table store (1 table per collection name)"""

    def __init__(self, vectordb: VectorDatabaseManager, table_name: str, embeddings: Embeddings, embedding_length: int = 768, recreate_table: bool = False, debug: bool = False):
        self.vecdb = vectordb
        self.table_name = table_name
        self.embeddings = embeddings
        self.embedding_length = embedding_length

        if recreate_table:
            self.vecdb.drop_table(self.table_name)
        self.vecdb.create_vector_table(self.table_name,self.embedding_length)
        # self.vecdb.create_halfvector_table(self.table_name,self.embedding_length)

        if debug:
            logger.setLevel(logging.DEBUG)

    def get_all_chunks_by_docids(self, docids: list):
        """Fetch all chunks for given document IDs."""
        from sqlalchemy import text
        
        sql = text(f"""
            SELECT docid, content, cmetadata, chunk_idx
            FROM {self.table_name}
            WHERE docid = ANY(:docids)
            ORDER BY docid, cmetadata->>'chunk_idx'
        """)
        
        with self.vecdb.engine.connect() as conn:
            result = conn.execute(sql, {"docids": docids})
            return result.mappings().all()


    def delete_table(self):
        self.vecdb.drop_table(self.table_name)
    
    def delete(self,dafileids:list):
        self.vecdb.delete_embeddings_by_dafileids(self.table_name,dafileids)

    def add_documents(self, doclist:list[Document]):
        """Generate embeddings + insert into custom table."""

        embeddings = self.embeddings.embed_documents([doc.page_content for doc in doclist])
        records = [{'cmetadata': doc.metadata, 'content': doc.page_content, 'embedding': embeddings[idx]} for idx, doc in enumerate(doclist)]
        self.vecdb.insert_embeddings(self.table_name,records)

    def similarity_search(self, query: str, k: int = 5, filters: dict = None, threshold: float = 0.0):
        """Perform similarity search on the custom table."""
        logger.info(f'Embedding query...{query}')
        query_vec = self.embeddings.embed_query(query)
        logger.info(f'Completed Embedding query...{query}')
        WHERE_CLAUSE = ''
        if filters:
            op={'$in':'in',
                '$nin':'not in'}
            WHERE_CLAUSE = 'where '+' and '.join([f"cmetadata->>'{key}' {op[opkey]} ('{'\',\''.join(opval)}')"
                                        for key, value in filters.items()
                                        for opkey, opval in value.items()])+ ' '
            if threshold:
                WHERE_CLAUSE += f'and 1-(embedding <=> (:query_embedding)::vector) > {threshold} '
        elif threshold:
            WHERE_CLAUSE = f'where 1-(embedding <=> (:query_embedding)::vector) > {threshold} '

        sql = text(f"""
                SELECT content, cmetadata, 1-(embedding <=> (:query_embedding)::vector) AS score
                FROM {self.table_name}
                {WHERE_CLAUSE}
                ORDER BY embedding <=> (:query_embedding)::vector
                LIMIT :top_k;
                """).bindparams(bindparam("query_embedding", type_=Vector),
                                bindparam("top_k", type_=Integer))
        
        params = {"query_embedding": query_vec,"top_k": k}
        # compiled = sql.params(**params).compile(
        #         dialect=self.vecdb.engine.dialect,
        #         compile_kwargs={"literal_binds": True}
        #     )
        # final_sql = str(compiled)
        # logger.info(f"Executing query: {final_sql}")
        
        # final_sql = sql.bindparams(bindparam("query_embedding", value=query_vec, type_=Vector),
        #                            bindparam("top_k", value=k, type_=Integer)).compile(
        #     dialect=self.vecdb.engine.dialect,
        #     compile_kwargs={"literal_binds": True}
        # ).string

        # logger.debug(final_sql)

        with self.vecdb.engine.connect() as conn:
            logger.info("=== Final SQL sending to PostgreSQL ===")
            result = conn.execute( sql,params)
            return result.mappings().all()

        # return [
        #     {"content": row[0], "cmetadata": row[1], "distance": row[2]}
        #     for row in results
        # ]

    def get_all_chunks_by_dafileids(self, dafileids: list):
        """Fetch all chunks for given dafileids from cmetadata."""
        from sqlalchemy import text
        
        sql = text(f"""
            SELECT content, cmetadata
            FROM {self.table_name}
            WHERE cmetadata->>'dafileid' = ANY(:dafileids)
            ORDER BY cmetadata->>'dafileid', (cmetadata->>'chunk_idx')::int
        """)
        
        with self.vecdb.engine.connect() as conn:
            result = conn.execute(sql, {"dafileids": dafileids})
            return result.mappings().all()


    def get_chunk_counts_by_dafileids(self, dafileids: list):
        """
        Get total chunk count per dafileid for coverage ratio.
        
        Args:
            dafileids: List of dafileid (fuuid) to count chunks for
            
        Returns:
            DataFrame[dafileid, total_chunks]
        """
        from sqlalchemy import text
        
        placeholders = ','.join([f"'{id_}'" for id_ in dafileids])
        sql = text(f"""
            SELECT 
                cmetadata->>'dafileid' as dafileid,
                COUNT(*) as total_chunks
            FROM consult_np.{self.table_name}
            WHERE cmetadata->>'dafileid' IN ({placeholders})
            GROUP BY cmetadata->>'dafileid';
        """)
        
        with self.vecdb.engine.connect() as conn:
            df_counts = pd.read_sql(sql, conn)
        
        logger.info(f"Chunk counts: {df_counts.to_dict('records')}")
        return df_counts.set_index('dafileid')['total_chunks']


    # def get_chunk_counts_by_fuuids(self, fuuids: list) -> pd.DataFrame:
    #     """
    #     Get total chunk count per fuuid (for coverage ratio).
    #     """
    #     from sqlalchemy import text
        
    #     placeholders = ','.join([f"'{uuid_}'" for uuid_ in fuuids])
    #     sql = text(f"""
    #         SELECT 
    #             cmetadata->>'fuuid' as fuuid,
    #             COUNT(*) as total_chunks
    #         FROM consult_np.{self.table_name}
    #         WHERE cmetadata->>'fuuid' IN ({placeholders})
    #         GROUP BY cmetadata->>'fuuid';
    #     """)
        
    #     with self.vecdb.engine.connect() as conn:
    #         df_counts = pd.read_sql(sql, conn)
        
    #     logger.info(f"FUUID chunk counts: {df_counts.to_dict('records')}")
    #     return df_counts.set_index('fuuid')['total_chunks']


    # def get_chunks_by_keys(self, key_pairs: list[tuple]) -> dict:
    #     """
    #     Fetch content by (dafileid, chunk_idx) pairs from pgvector table.
        
    #     Args:
    #         key_pairs: [(dafileid_str, chunk_idx_str|int), ...]
        
    #     Returns:
    #         {(dafileid, chunk_idx): content_str, ...}  # Missing keys return None
    #     """
        
    #     if not key_pairs:
    #         logger.warning("Empty key_pairs")
    #         return {}
        
    #     # Build parameterized WHERE clause
    #     conditions = []
    #     params = {}
    #     for i, (daf, idx) in enumerate(key_pairs):
    #         key = f"k{i}"
    #         conditions.append(
    #             f"(cmetadata->>'dafileid' = :daf_{key} AND (cmetadata->>'chunk_idx')::int = :idx_{key})"
    #         )
    #         params[f"daf_{key}"] = str(daf)  # Ensure string
    #         params[f"idx_{key}"] = int(idx)   # Ensure int
        
    #     where_clause = " OR ".join(conditions)
        
    #     sql = text(f"""
    #         SELECT 
    #             cmetadata->>'dafileid' as dafileid,
    #             cmetadata->>'chunk_idx' as chunk_idx,
    #             content
    #         FROM {self.table_name} 
    #         WHERE {where_clause}
    #     """)
        
    #     try:
    #         with self.vecdb.engine.connect() as conn:
    #             result = conn.execute(sql, params)
    #             rows = result.mappings().all()
            
    #         content_map = {
    #             (row['dafileid'], int(row['chunk_idx'])): row['content']
    #             for row in rows
    #         }
            
    #         found = len(content_map)
    #         requested = len(key_pairs)
    #         logger.info(f"Fetched {found}/{requested} chunks by key")
            
    #         # if found < requested:
    #         #     missing = set(key_pairs) - set(content_map.keys())
    #         #     logger.warning(f"Missing {len(missing)} chunks: {list(missing)[:3]}...")
    #         logger.info(f"found length of chunk_idx, dafileid : content  ->  {found}")
    #         return content_map
            
    #     except Exception as e:
    #         logger.error(f"Chunk fetch failed: {e}")
    #         return {}