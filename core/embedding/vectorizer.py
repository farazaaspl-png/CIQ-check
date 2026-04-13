import logging
import pandas as pd
from typing import List
from sqlalchemy import String, and_,Table, MetaData, select
from langchain_core.documents import Document

from core.embedding.vectordb import VectorDatabaseManager
from core.embedding.vectorstore import CustomPgvectorStore
from core.exceptions import DocumentsAlreadyVectorized, UnableToFindAnyDocument
from core.genai.open_ai_client import EmbeddingInterface
from core.db.crud import DatabaseManager
from core.utility import split_text

from config import Config as cfg
from core.utility import get_custom_logger
logger = get_custom_logger(__name__)
                  
class VectorInterface:

    _NO_OF_BULK_LOADS = 20
    _CHUNK_SIZE = 5000
    _OVER_LAP_SIZE = 100

    def __init__(self, table_name: str, debug: bool = False):
        self.table_name = table_name
        self.vdb = VectorDatabaseManager(debug)
        embeddings = EmbeddingInterface(model_name=cfg.EMBEDDING_MODEL, correlationid = cfg.CORR_ID_EMBEDDINGS)
        self.vector_store = CustomPgvectorStore(
            embeddings = embeddings,
            table_name = self.table_name,
            vectordb = self.vdb,
            embedding_length = 768,
            debug = debug
            )
        
        if debug:
            logger.setLevel(logging.DEBUG)

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
    
    def __add_doc_list_to_vectorstore(self,doclist: List[Document]):
        start_idx = 0 
        for size in range(0, len(doclist), self._NO_OF_BULK_LOADS):
            max_idx = (size + self._NO_OF_BULK_LOADS) if (size + self._NO_OF_BULK_LOADS) <= len(doclist) else len(doclist)+1
            logger.info(f"Adding documents from {start_idx} to {max_idx} to vector store")
            self.vector_store.add_documents(doclist[start_idx: max_idx])
            start_idx=max_idx

    def vectorize_documents_by_requestids(self, requestids: List):
        if self.table_name != cfg.DOCUMENT_METASTORE_NAME:
            raise ValueError(f'Invalid table name. Function is only applicable for table "{cfg.table_name}".')
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
            docstovecdf = pd.read_sql(query, conn)

        logger.info(f"{requestids} - Filter out documents already vectorized")
        
        existingdafileids = self.vdb.get_existing_dafileids(self.table_name,docstovecdf['dafileid'].values.tolist())

        docstovecdf = docstovecdf[~docstovecdf['dafileid'].isin(existingdafileids)]

        logger.info(f"{requestids} - Found {docstovecdf.shape[0]} documents for vectorization")
        if docstovecdf.shape[0] == 0:
            logger.error(f"{requestids} - Cannot find any unvectorized document")
            raise DocumentsAlreadyVectorized()
        
        doclist = self.__generate_doc_list(docstovecdf)
        logger.info(f"{requestids} - Generated vector document list of length: {len(doclist)}")

        self.__add_doc_list_to_vectorstore(doclist)
        logger.info(f"{requestids} - Vectorization completed")

    def vectorize_documents_by_dafileids(self, dafileids: List[str]):
        #KH: changed table name
        # if self.table_name != cfg.DOCUMENT_METASTORE_NAME:
        if self.table_name != cfg.DOCUMENT_CONTENT_STORE:
            raise ValueError(f'Invalid collection name. Function is only applicable for collection "{cfg.DOCUMENT_CONTENT_STORE}".')
        logger.info(f"Vectorizing documents for dafileids: {dafileids}")
            
        existingdafileids = self.vdb.get_existing_dafileids(self.table_name,dafileids)
        
        if len(existingdafileids) == len(dafileids):
            logger.error(f"Cannot find any unvectorized document for dafileids: {dafileids}")
            raise DocumentsAlreadyVectorized()
        
        db = DatabaseManager()
        with db.engine.connect() as conn:
            metadata = MetaData(schema=cfg.DATABASE_SCHEMA)
            vwdocuments = Table(cfg.DOCUMENTS_VIEW, metadata, autoload_with=conn)
            query = select(vwdocuments).where(
                vwdocuments.c.dafileid.in_(dafileids), #document’s dafileid is in the passed list.
                vwdocuments.c.status.in_(['APPROVED']),
                vwdocuments.c.practice.is_not(None),
                ~vwdocuments.c.dafileid.in_(existingdafileids)
            )
            docstovecdf = pd.read_sql(query, conn)
    
        doclist = self.__generate_doc_list(docstovecdf)

        self.__add_doc_list_to_vectorstore(doclist)
        logger.info(f"Vectorization completed for dafileids: {dafileids}")

    def delete_documents_by_dafileids(self, dafileidlist: list):
        self.vector_store.delete(dafileidlist)
    
    def update_vectors(self, dafileidlist: list):
        self.vector_store.delete(dafileidlist)
        self.vectorize_documents_by_dafileids(dafileidlist)

    def search_documents(self,query: str, no_of_docs: int=5, threshold: float=0.0, filters: dict=None, **kwargs):
        logger.info(f"Started searching documents for query: {query}")
        projectid = kwargs.get('projectid')
        vfilters = {}
        if projectid:
            # includeSkipped = kwargs.get('includeSkipped', False)
            db = DatabaseManager()
            with db.engine.connect() as conn:
                metadata = MetaData(schema=cfg.DATABASE_SCHEMA)
                vwrecommendations = Table(cfg.RECOMMENDATIONS_VIEW, metadata, autoload_with=conn)

                filterstatus_list = ['ACCEPTED', 'SENT', 'SKIPPED']
                # if includeSkipped:
                #     filterstatus_list = ['GENERATED', 'ACCEPTED', 'SENT']

                sqlquery = select(vwrecommendations.c.templateid.cast(type_=String)).where(
                    and_(
                        vwrecommendations.c.projectid == projectid,
                        vwrecommendations.c.status.in_(filterstatus_list)
                    )
                ).distinct()
                df = pd.read_sql(sqlquery, conn)
            logger.info(f"Selected files to skip: {df.shape[0]} files")

            logger.debug(f"Files to skip: {df['templateid'].to_list()}")
            if df.shape[0]!=0:
                vfilters = {"dafileid":{'$nin':df['templateid'].to_list()}}
            # if kwargs.get('dtpm_phase'):
            #     vfilters["dtpm_phase"] = {"$in":kwargs.get('dtpm_phase')}

        if len(vfilters)==0 and filters is not None:
            vfilters = filters

        rec_list = self.vector_store.similarity_search(query=query,k=no_of_docs,threshold=threshold,filters=vfilters)
        if len(rec_list) == 0:
            logger.error(f"Cannot find any document for query: {query}")
            raise UnableToFindAnyDocument()
        else:
            df = pd.DataFrame([{**rec['cmetadata'],'score':rec['score']} for rec in rec_list])
            df.replace({'None': None}, inplace=True)
            logger.info(f"Found {len(df)} documents")
            logger.info(f"Completed searching documents for query: {query}")
            return df
