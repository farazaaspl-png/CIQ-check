import logging, threading, pandas as pd
from pgvector.sqlalchemy import Vector, HalfVector
from sqlalchemy import UUID, Column, DateTime, Integer, Text, cast, create_engine, func, Table, MetaData, select, delete, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import JSONB
from pgvector.sqlalchemy import HALFVEC

from core.exceptions import DatabaseWriteError
from config import Config as cfg
from core.utility import get_custom_logger
logger = get_custom_logger(__name__)

class VectorDatabaseManager:
    _instance = None
    _lock = threading.Lock()
    def __new__(cls,debug = False):
        if cls._instance is None:
            with cls._lock:
                cls._instance = super(VectorDatabaseManager, cls).__new__(cls)
                cls._instance.engine = create_engine(cfg.VECTOR_DB_CONNECTION_STR,
                                                  pool_size=4,              # default: 5
                                                  max_overflow=5,           # allows up to 30 total connections
                                                  pool_timeout=60,           # seconds to wait before giving up
                                                  pool_recycle=1800,
                                                  echo=False)
                cls._instance.Session = sessionmaker(bind=cls._instance.engine,
                                                 expire_on_commit=True)
        return cls._instance

    def __init__(self,debug = False):
        self.session = self.Session()
        if debug:
            logger.setLevel(logging.DEBUG)
        
    def get_existing_dafileids(self,table_name,dafileidlist: list):
        try:
            with self.engine.connect() as conn:
                metadata = MetaData(schema='consult_np')
                emb = Table(table_name, metadata, autoload_with=conn)
                query = select(cast(emb.c.cmetadata['dafileid'].astext, UUID).label('dafileid')).select_from(emb)
                query = query.where(cast(emb.c.cmetadata['dafileid'].astext, UUID).in_(dafileidlist))
                df = pd.read_sql(query, conn)
            return df['dafileid'].drop_duplicates().values.tolist()
        except Exception as e:
            logger.error(f'Failed while querying DB: {e}', exc_info=True)
            raise e
        
    def insert_embeddings(self,table_name,recordlist):
        try:
            with self.engine.connect() as conn:
                metadata = MetaData(schema='consult_np')
                Embedding_Table = Table(table_name, metadata, autoload_with=conn)
                conn.execute(Embedding_Table.insert(), recordlist)
                conn.commit()
        except Exception as e:
            logger.error(f'Failed while inserting embedding into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while inserting embedding into DB: {e}')
    
    def delete_embeddings_by_ids(self,table_name,ids):
        with self.engine.connect() as conn:
            metadata = MetaData(schema='consult_np')
            embedding_table = Table(table_name, metadata, autoload_with=conn)
            conn.execute(embedding_table.delete().where(embedding_table.c.id.in_(ids)))
            conn.commit()
    
    def delete_embeddings_by_dafileids(self,table_name,dafileids):
        with self.engine.connect() as conn:
            metadata = MetaData(schema='consult_np')
            embedding_table = Table(table_name, metadata, autoload_with=conn)
            conn.execute(embedding_table.delete().where(embedding_table.c.cmetadata['dafileid'].astext.in_(dafileids)))
            conn.commit()

    def drop_table(self, table_name: str):
        with self.engine.connect() as conn:
            metadata = MetaData(schema='consult_np')
            table = Table(table_name, metadata, autoload_with=conn)
            table.drop(conn,checkfirst=True)

    def create_table(self, table_name: str, *columns):
        with self.engine.connect() as conn:
            metadata = MetaData(schema='consult_np')
            table = Table(table_name, metadata, *columns)
            table.create(conn,checkfirst=True)
            conn.commit()
        
    def create_vector_table(self, table_name: str,embedding_length: int = 768):  
        self.create_table(table_name, 
                          Column('id', Integer, primary_key=True, autoincrement=True),
                          Column('content', Text, nullable=False),
                          Column('cmetadata', JSONB, nullable=False),
                          Column('embedding', Vector(embedding_length), nullable=False),
                          Column('created_date',DateTime(timezone=True), server_default=func.now(), nullable=False))
    
    def create_halfvector_table(self, table_name: str, embedding_length: int = 768):
        self.create_table(
            table_name,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('content', Text, nullable=False),
            Column('cmetadata', JSONB, nullable=False),
            Column('embedding', HALFVEC(embedding_length), nullable=False),
            Column('created_date', DateTime(timezone=True), server_default=func.now(), nullable=False)
        )