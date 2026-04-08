import pandas as pd
from typing import List
# from pathlib import Path
from sqlalchemy.ext.asyncio.engine import create_async_engine
from sqlalchemy import UUID, String, and_, cast, create_engine, text, Table, MetaData, select, delete, update
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# from langchain_core.embeddings import Embeddings
from langchain_postgres import PGVector
# from langchain_postgres.vectorstores import DistanceStrategy 
from langchain_core.documents import Document
from langchain_core.runnables import chain

from core.exceptions import DocumentsAlreadyVectorized, UnableToFindAnyDocument
from core.genai.open_ai_client import EmbeddingInterface
from core.db.crud import DatabaseManager
from core.utility import split_text

# logging.getLogger().setLevel(logging.INFO)
from config import Config as cfg
from core.utility import get_custom_logger
logger = get_custom_logger(__name__)
# logger.propagate = False

class VectorDatabaseManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(VectorDatabaseManager, cls).__new__(cls)
            # cls._instance.engine = create_engine(cfg.VECTOR_DB_CONNECTION_STR,
            #                                       pool_size=4,              # default: 5
            #                                       max_overflow=5,           # allows up to 30 total connections
            #                                       pool_timeout=60,           # seconds to wait before giving up
            #                                       pool_recycle=1800,
            #                                       echo=False)
            cls._instance.engine = create_async_engine(cfg.VECTOR_DB_CONNECTION_STR,
                                                  pool_size=4,              # default: 5
                                                  max_overflow=5,           # allows up to 30 total connections
                                                  pool_timeout=60,           # seconds to wait before giving up
                                                  pool_recycle=1800,
                                                  echo=False)

            cls._instance.Session = sessionmaker(bind=cls._instance.engine,
                                                 expire_on_commit=True)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'session'):
            self.session = self.Session()

class VectorInterface:

    _NO_OF_BULK_LOADS = 20
    _CHUNK_SIZE = 5000
    _OVER_LAP_SIZE = 100

    def __init__(self, collection_name: str):
        self.collection_name = collection_name
    
    def __initialize(self):
        embeddings = EmbeddingInterface(correlationid = cfg.CORR_ID_EMBEDDINGS)
        self.vector_store = PGVector(
            embeddings=embeddings,
            collection_name=self.collection_name,
            connection=cfg.VECTOR_DB_CONNECTION_STR,
            # connection= VectorDatabaseManager().engine,
            embedding_length = 768,
            use_jsonb=True,
            create_extension=False,
            logger = logger,
            async_mode=True
            )
        self.vector_store.create_extension()

    def test_connection(self):
        self.__initialize()

    def get_vector_store(self):
        self.__initialize()
        return self.vector_store
        
    def __delete_collection(self):
        self.__initialize()
        self.vector_store.delete_collection()   
 
    def __generate_doc_list(self,docstovecdf: pd.DataFrame):
        docstovecdf['dafileid'] = docstovecdf['dafileid'].astype(str)
        docstovecdf['ipid'] = docstovecdf['ipid'].astype(str)
        self.rowstovectorize = docstovecdf.to_dict(orient='records')

        self.metadatacolumns = ['dafileid','ipid','document_type','ip_type','dtpm_phase','practice','offerfamily','offer','filename','title']
        doclist = []
        for row in self.rowstovectorize:
            metadata = ''

            if len(row['ip_type'])>0:
                # metadata = 'This is '+row['ip_type'] + ' types of document required at ' + row['dtpm_phase'] +' phase of a project.\n'
                metadata = 'This is '+row['ip_type'] + ' document required at ' + row['dtpm_phase'] +' phase of a project.\n'
            else: 
                metadata = 'This document is required at ' + row['dtpm_phase'] +' phase of a project.\n'
            
            if row['title']:
                metadata +='It is titled \"' + row['title'] + '\".\n'
            metadata += 'It should be used for \"' + row['offer'].title() + '\" offer provided to customers.\n'
            metadata += '\"' + row['offer'].title() + '\" belongs to offer family \"'+ row['offerfamily'].title() + '\"'
            metadata += ' and \"' + row['practice'].title() + '\" practice.\n'
            # metadata += row['description'] + '\n' 
            splitMetadata = split_text(metadata, chunk_size = self._CHUNK_SIZE, chunk_overlap = self._OVER_LAP_SIZE)
            for chunk in splitMetadata:
                doclist.append(Document(page_content = chunk, metadata = {key: value for key, value in row.items() if key in self.metadatacolumns}))
        return doclist
    
    def vectorize_documents_by_requestids(self, requestids: List):
        if self.collection_name != cfg.COLLECTIONNAME:
            raise ValueError(f'Invalid collection name. Function is only applicable for collection "{cfg.COLLECTIONNAME}".')
        logger.info(f"{requestids} - Get details to vectorize")
        db = DatabaseManager()
        with db.engine.connect() as conn:
            metadata = MetaData(schema=cfg.DATABASE_SCHEMA)
            vwdocuments = Table(cfg.DOCUMENTS_VIEW, metadata, autoload_with=conn)
            query = select(vwdocuments).where(
                        vwdocuments.c.requestid.in_(requestids),
                        vwdocuments.c.dafileid.is_not(None),
                        vwdocuments.c.practice.is_not(None),
                        vwdocuments.c.status.in_(['APPROVED'])
                    )
            self.docstovecdf = pd.read_sql(query, conn)

        logger.info(f"{requestids} - Filter out documents already vectorized")
        vdb = VectorDatabaseManager()
        with vdb.engine.connect() as conn:
            metadata = MetaData(schema='consult_np')
            emb = Table('langchain_pg_embedding', metadata, autoload_with=conn)
            collection = Table('langchain_pg_collection', metadata, autoload_with=conn)
            query = select(cast(emb.c.cmetadata['dafileid'].astext, UUID).label('dafileid')).select_from(emb)
            query = query.where(emb.c.collection_id == select(collection.c.uuid).select_from(collection).where(collection.c.name == cfg.COLLECTIONNAME),
                                cast(emb.c.cmetadata['dafileid'].astext, UUID).in_(self.docstovecdf['dafileid'].values.tolist()))

            self.existingdafileids = pd.read_sql(query, conn)
        # logger.info(self.existingdafileids.columns)

        self.docstovecdf = self.docstovecdf[~self.docstovecdf['dafileid'].isin(self.existingdafileids['dafileid'].values.tolist())]

        logger.info(f"{requestids} - Found {self.docstovecdf.shape[0]} documents for vectorization")
        if self.docstovecdf.shape[0] == 0:
            logger.error(f"{requestids} - Cannot find any unvectorized document", exc_info=True)
            raise DocumentsAlreadyVectorized()
        
        self.doclist = self.__generate_doc_list(self.docstovecdf)
        logger.info(f"{requestids} - Generated vector document list of length: {len(self.doclist)}")

        self.__initialize()
        start_idx = 0 
        for size in range(0, len(self.doclist), self._NO_OF_BULK_LOADS):
            max_idx = (size + self._NO_OF_BULK_LOADS) if (size + self._NO_OF_BULK_LOADS) <= len(self.doclist) else len(self.doclist)+1
            logger.info(f"{requestids} - Adding documents from {start_idx} to {max_idx} to vector store")
            self.vector_store.add_documents(self.doclist[start_idx: max_idx])
            start_idx=max_idx
            
        logger.info(f"{requestids} - Vectorization completed")

    # def vectorize_documents_by_requestids(self, requestids: List):
    #     if self.collection_name != cfg.COLLECTIONNAME:
    #         raise ValueError(f'Invalid collection name. Function is only applicable for collection "{cfg.COLLECTIONNAME}".')
    #     logger.info(f"{requestids} - Vectorizing documents")
    #     db = DatabaseManager()
    #     with db.engine.connect() as conn:
    #         metadata = MetaData()
    #         vwdocuments = Table(cfg.DOCUMENTS_VIEW, metadata, autoload_with=conn)
    #         emb = Table('langchain_pg_embedding', metadata, autoload_with=conn)
    #         collection = Table('langchain_pg_collection', metadata, autoload_with=conn)
    #         query = select(vwdocuments).where(
    #                     vwdocuments.c.requestid.in_(requestids),
    #                     vwdocuments.c.dafileid.is_not(None),
    #                     vwdocuments.c.practice.is_not(None),
    #                     vwdocuments.c.status.in_(['APPROVED']),
    #                     ~vwdocuments.c.dafileid.in_(
    #                         select(cast(emb.c.cmetadata['dafileid'].astext, UUID)).select_from(emb).where(emb.c.collection_id == select(collection.c.uuid).select_from(collection).where(collection.c.name == cfg.COLLECTIONNAME))
    #                     )
    #                 )
    #         self.docstovecdf = pd.read_sql(query, conn)
    #     logger.info(f"{requestids} - Found {self.docstovecdf.shape[0]} documents for vectorization")
    #     if self.docstovecdf.shape[0] == 0:
    #         logger.error(f"{requestids} - Cannot find any unvectorized document")
    #         raise DocumentsAlreadyVectorized()
        
    #     self.doclist = self.__generate_doc_list(self.docstovecdf)
    #     logger.info(f"{requestids} - Generated vector document list of length: {len(self.doclist)}")

    #     self.__initialize()
    #     start_idx = 0 
    #     for size in range(0, len(self.doclist), self._NO_OF_BULK_LOADS):
    #         max_idx = (size + self._NO_OF_BULK_LOADS) if (size + self._NO_OF_BULK_LOADS) <= len(self.doclist) else len(self.doclist)+1
    #         logger.info(f"{requestids} - Adding documents from {start_idx} to {max_idx} to vector store")
    #         self.vector_store.add_documents(self.doclist[start_idx: max_idx])
    #         start_idx=max_idx
            
    #     logger.info(f"{requestids} - Vectorization completed")

    def vectorize_documents_by_dafileids(self, dafileids: List[str]):
        if self.collection_name != cfg.COLLECTIONNAME:
            raise ValueError(f'Invalid collection name. Function is only applicable for collection "{cfg.COLLECTIONNAME}".')
        logger.info(f"Vectorizing documents for dafileids: {dafileids}")
            
        vdb = VectorDatabaseManager()
        with vdb.engine.connect() as conn:
            metadata = MetaData(schema='consult_np')
            emb = Table('langchain_pg_embedding', metadata, autoload_with=conn)
            collection = Table('langchain_pg_collection', metadata, autoload_with=conn)
            query = select(cast(emb.c.cmetadata['dafileid'].astext, UUID).label('dafileid')).select_from(emb)
            query = query.where(emb.c.collection_id == select(collection.c.uuid).select_from(collection).where(collection.c.name == cfg.COLLECTIONNAME),
                                cast(emb.c.cmetadata['dafileid'].astext, UUID).in_(dafileids))

            self.existingdafileids = pd.read_sql(query, conn)
        
        if self.existingdafileids.shape[0] == len(dafileids):
            logger.error(f"Cannot find any unvectorized document for dafileids: {dafileids}", exc_info=True)
            raise DocumentsAlreadyVectorized()
        
        db = DatabaseManager()
        with db.engine.connect() as conn:
            metadata = MetaData(schema=cfg.DATABASE_SCHEMA)
            vwdocuments = Table(cfg.DOCUMENTS_VIEW, metadata, autoload_with=conn)
            query = select(vwdocuments).where(
                vwdocuments.c.dafileid.in_(dafileids), #document’s dafileid is in the passed list.
                vwdocuments.c.status.in_(['APPROVED']),
                vwdocuments.c.practice.is_not(None),
                ~vwdocuments.c.dafileid.in_(self.existingdafileids['dafileid'].values.tolist())
            )
            self.docstovecdf = pd.read_sql(query, conn)
    
        self.doclist = self.__generate_doc_list(self.docstovecdf)
    
        self.__initialize()
        start_idx = 0
        for size in range(0, len(self.doclist), self._NO_OF_BULK_LOADS):
            max_idx = self._NO_OF_BULK_LOADS if size + self._NO_OF_BULK_LOADS <= len(self.doclist) else len(self.doclist)+1
            self.vector_store.add_documents(self.doclist[start_idx: max_idx])
            start_idx = max_idx
        logger.info(f"Vectorization completed for dafileids: {dafileids}")

    # def vectorize_documents_by_dafileids(self, dafileids: List[str]):
    #     if self.collection_name != cfg.COLLECTIONNAME:
    #         raise ValueError(f'Invalid collection name. Function is only applicable for collection "{cfg.COLLECTIONNAME}".')
    #     logger.info(f"Vectorizing documents for dafileids: {dafileids}")
    #     db = DatabaseManager()
    #     with db.engine.connect() as conn:
    #         metadata = MetaData()
    #         vwdocuments = Table(cfg.DOCUMENTS_VIEW, metadata, autoload_with=conn)
    #         emb = Table('langchain_pg_embedding', metadata, autoload_with=conn)
    #         query = select(vwdocuments).where(
    #             vwdocuments.c.dafileid.in_(dafileids), #document’s dafileid is in the passed list.
    #             vwdocuments.c.status.in_(['APPROVED']),
    #             vwdocuments.c.practice.is_not(None),
    #             ~vwdocuments.c.dafileid.in_(
    #                 select(cast(emb.c.cmetadata['dafileid'].astext, UUID)).select_from(emb) #document’s dafileid does not already exist in the embedding table (langchain_pg_embedding)
    #             )
    #         )
    #         self.docstovecdf = pd.read_sql(query, conn)
    
    #     if self.docstovecdf.shape[0] == 0:
    #         logger.error(f"Cannot find any unvectorized document for dafileids: {dafileids}")
    #         raise DocumentsAlreadyVectorized()
    
    #     self.doclist = self.__generate_doc_list(self.docstovecdf)
    
    #     self.__initialize()
    #     start_idx = 0
    #     for size in range(0, len(self.doclist), self._NO_OF_BULK_LOADS):
    #         max_idx = self._NO_OF_BULK_LOADS if size + self._NO_OF_BULK_LOADS <= len(self.doclist) else len(self.doclist)+1
    #         self.vector_store.add_documents(self.doclist[start_idx: max_idx])
    #         start_idx = max_idx
    #     logger.info(f"Vectorization completed for dafileids: {dafileids}")
    
    def delete_documents_by_dafileids(self, dafileidlist: list):
        """
        Delete documents whose cmetadata.dafileid matches any id in dafileidlist.
        Uses PGVector vector store delete with filter on nested JSON key.
        Args:
            dafileidlist (list): List of dafileid strings to delete.
        Returns:
            None (LangChain's delete method does not return deletion count)
        """
        logger.info(f"Deleting vectors for dafileids: {dafileidlist}")
        self.__initialize()  # Initialize vector store if needed
        filter_dict = {
            "cmetadata": {
                "dafileid": {
                    "$in": dafileidlist
                }
            }
        }
    
        # Perform deletion
        self.vector_store.delete(filter=filter_dict)

        logger.info(f"Deleted vectors for dafileids: {dafileidlist}")

    def update_vectors(self, dafileidlist: list):
        self.__initialize()  # Initialize vector store if needed
        self.delete_documents_by_dafileids(dafileidlist)
        self.vectorize_documents_by_dafileids(dafileidlist)


@chain
def document_retriever(pquery: str,pno_of_docs: int=10,pthreshold: float=None,pfilters: dict=None) -> List[Document]:
    
    vector = VectorInterface(collection_name=cfg.COLLECTIONNAME)
    vector_store = vector.get_vector_store()
    try:
        docs, scores = zip(*vector_store.similarity_search_with_relevance_scores(pquery,k = pno_of_docs,score_threshold=pthreshold,filter=pfilters))
    except ValueError as ve:
        logger.error(ve, exc_info=True)
        docs = []
        scores = []
    for doc, score in zip(docs, scores):
        doc.metadata["score"] = score
    
    return docs
  

def search_documents(query: str, no_of_docs: int=5, threshold: float=None, filters: dict=None, **kwargs):
    logger.info(f"Started searching documents for query: {query}")
    if not filters:
        db = DatabaseManager()
        with db.engine.connect() as conn:
            metadata = MetaData(schema=cfg.DATABASE_SCHEMA)
            vwrecommendations = Table(cfg.RECOMMENDATIONS_VIEW, metadata, autoload_with=conn)
           
            filterstatus_list = ['GENERATED', 'ACCEPTED', 'SENT', 'SKIPPED']
            if kwargs.get('includeSkipped', False):
                filterstatus_list = ['GENERATED', 'ACCEPTED', 'SENT']
           
            sqlquery = select(vwrecommendations.c.templateid.cast(type_=String)).where(
                and_(
                    vwrecommendations.c.projectid == kwargs.get('projectid'),
                    vwrecommendations.c.status.in_(filterstatus_list)
                )
            ).distinct()
            df = pd.read_sql(sqlquery, conn)
        logger.info(f"Selected files to skip: {df.shape[0]} files")
        
        logger.debug(f"Files to skip: {df['templateid'].to_list()}")
        vfilters = {"dafileid":{'$nin':df['templateid'].to_list()},
                    "dtpm_phase":{"$in":kwargs.get('dtpm_phase')}}

    else:
        vfilters = filters
   
    rec_list = document_retriever.invoke(
        query,
        pno_of_docs=no_of_docs,
        pthreshold=threshold,
        pfilters=vfilters
    )
   
    if len(rec_list) == 0:
        logger.error(f"Cannot find any document for query: {query}", exc_info=True)
        raise UnableToFindAnyDocument()
    else:
        df = pd.DataFrame([rec.metadata for rec in rec_list])
        df.replace({'None': None}, inplace=True)
        logger.info(f"Found {len(df)} documents")
        logger.info(f"Completed searching documents for query: {query}")
        return df