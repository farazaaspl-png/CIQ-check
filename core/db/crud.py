import functools
from pathlib import Path
import time
import  uuid, pandas as pd, dateutil.parser, warnings, pendulum
# from pathlib import Path
import logging
from psycopg2 import OperationalError
from sqlalchemy import JSON, TIMESTAMP, DateTime, Float, MetaData, PrimaryKeyConstraint, create_engine, Column, String, Integer, insert, text, Text
from sqlalchemy import UUID, Boolean, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import DBAPIError
from typing import Dict, List, Optional, Sequence, Optional
from datetime import datetime,timezone
# from sqlalchemy.sql import null
 
from core.exceptions import DatabaseReadError, DatabaseWriteError  
from config import Config as cfg
# from sqlalchemy.orm import Session
 
from core.utility import get_custom_logger
logger = get_custom_logger(__name__)
# logger.propagate = False
 
warnings.filterwarnings('ignore')
 
# ---------------- ORM Base ----------------
metadata_obj = MetaData(schema=cfg.DATABASE_SCHEMA)
Base = declarative_base(metadata=metadata_obj)
 
# ---------------- Models ----------------
 
class Event(Base):
    __tablename__ = cfg.AN_EVENT_TABLE
 
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_type = Column(String(100), nullable=False)
    event_sub_type = Column(String(100), nullable=True)
    created_on = Column(TIMESTAMP(timezone=False), nullable=True)
    request_id = Column(UUID(as_uuid=True), nullable=True)
    idx = Column(Integer, nullable=True)
    total_count = Column(Integer, nullable=True)
    is_request = Column(Boolean, nullable=False, default=True)
    payload = Column(JSON, nullable=True)
 

class TRedactionInclusion(Base):
    __tablename__ = cfg.REDACTION_INCLUSION_TABLE  # TODO add this table in confige.g. "tredaction_inclusion"

    toberedacted = Column(Text, primary_key=True)
    label = Column(String(100), nullable=False)


class TRedactionExclusion(Base):
    __tablename__ = cfg.REDACTION_EXCLUSION_TABLE  # e.g. "tredaction_exclusion"

    exclude = Column(Text, primary_key=True)


class TStatementOfWork(Base):
    __tablename__ = cfg.STATEMENTOFWORK_TABLE
 
    requestid = Column(UUID)
    projectid = Column(String(100))
    dafileid = Column(UUID)
    uniqueid = Column(UUID)
    sowfilename = Column(String(100))
    offer = Column(JSONB)
    summary = Column(Text)
    created_by = Column(String(225), nullable=True)
    updated_by = Column(String(225), nullable=True)
    created_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_date = Column(DateTime(timezone=True), onupdate=func.now())
 
    # recommendations = relationship("trecommendation", back_populates="sow")
    __table_args__ = (
        PrimaryKeyConstraint('requestid', 'projectid', 'dafileid','uniqueid', name='pk_tstatementofwork'),
    )
    
class TDocument(Base):
    __tablename__ = cfg.DOCUMENT_TABLE
    # projectid = Column(String(100),primary_key=True)
    requestid = Column(UUID,primary_key=True)
    fuuid = Column(UUID,primary_key=True)
    daoriginal_fileid = Column(UUID,primary_key=True)
    dafileid = Column(UUID)
    ipid = Column(UUID, nullable=True)
    projectid = Column(String(100), nullable=True)
    filename = Column(String(255))
    title = Column(String(255))
    description = Column(Text)
    gtl_synopsis = Column(Text)
    author = Column(String(100), nullable=True)
    dtpm_phase = Column(String(100))
    document_type = Column(String(50))
    ip_type = Column(String(255))
    offer = Column(String(255))
    relevance_score = Column(Float)
    usage_count = Column(Integer)
    redacted_items_dafileid = Column(UUID)
    waspdf = Column(Boolean)
    status = Column(String(100), nullable=True)
    type = Column(String(50))
    url = Column(String(2100))
    mathcingdafileid = Column(UUID)
    similarity = Column(Float)
    uploadedby = Column(String(100), nullable=True)
    created_by = Column(String(225), nullable=True)
    updated_by = Column(String(225), nullable=True)
    created_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_date = Column(DateTime(timezone=True), onupdate=func.now())

class TDocument_State(Base):
    __tablename__ = cfg.DOCUMENT_STATE_TABLE
    # projectid = Column(String(100),primary_key=True)
    requestid = Column(UUID,primary_key=True)
    fuuid = Column(UUID,primary_key=True)
    dafileid = Column(UUID,primary_key=True)
    request_dir = Column(String(255))
    filepath = Column(String(255))
    ispdf = Column(Boolean)
    converted_filepath = Column(String(255))
    extraction_input_file = Column(String(255))
    classification_input_file = Column(String(255))
    redacted_filename = Column(String(255))
    has_sensitive_items = Column(Boolean)
    istextredacted = Column(Boolean)
    isimageredacted = Column(Boolean)
    out_filepath = Column(String(255))
    redacted_items_filepath = Column(String(255))
    out_dafileid = Column(UUID)
    redacted_items_dafileid = Column(UUID)
    stageno = Column(Integer, nullable=True)
    stagename = Column(String(100), nullable=True)
    status = Column(String(100), nullable=True)
    inserted_on = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_on = Column(DateTime(timezone=True), onupdate=func.now())
 
class TRecommendation(Base):
    __tablename__ = cfg.RECOMMENDATION_TABLE
    requestid = Column(UUID, primary_key=True)
    projectid = Column(String(100), primary_key=True)
    sowfileid = Column(UUID, nullable=True)
    dafileid = Column(UUID, nullable=True)
    templateid = Column(UUID, primary_key=True)
    ipid = Column(UUID, nullable=True)
    phase = Column(String(50), nullable=True)
    status = Column(String(100),default='GENERATED')
    userquery = Column(Text, nullable=True)
    similarityscore = Column(Float, nullable=True)
    method = Column(String(50), nullable=True)
    created_by = Column(String(225), nullable=True, default='system')
    updated_by = Column(String(225), nullable=True)
    created_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_date = Column(DateTime(timezone=True), onupdate=func.now())
 
    # sow = relationship("tstatementofwork", back_populates="recommendations")
    # template = relationship("tgoldentemplate", back_populates="recommendations")
 
class TFeedback(Base):
    __tablename__ = cfg.FEEDBACK_TABLE
 
    fuuid = Column(UUID(as_uuid=True), primary_key=True)
    filename = Column(String(255), nullable=False)
    dafileid = Column(String(255), nullable=False)
    status = Column(String(100), nullable=False)
    feedback = Column(Text, nullable=True)
    created_by = Column(String(225), nullable=True)
    created_date = Column(DateTime(timezone=True),
                          server_default=func.now(),
                          nullable=False)
 
class TExtraction(Base):
    __tablename__ = cfg.EXTRACTION_TABLE
    id = Column(Integer, primary_key=True)
    requestid = Column(UUID)
    fuuid = Column(UUID)
    dafileid = Column(UUID)
    filename = Column(String(255))
    category = Column(String(255), nullable=True)
    sensitivetext = Column(Text, nullable=True)
    context = Column(Text, nullable=True)
    source = Column(String(50), nullable=False, server_default="unknown")
    reason = Column(Text, nullable=True)
    score = Column(Float, nullable=True)
    created_by = Column(String(225), nullable=True)
    created_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
 
 
class TToBeRedacted(Base):
    __tablename__ = cfg.TOBEREDACTED_TABLE
    id = Column(Integer, primary_key=True)
    requestid = Column(UUID)
    fuuid = Column(UUID)
    dafileid = Column(UUID)
    filename = Column(String(255))
    category = Column(String(255), nullable=True)
    sensitivetext = Column(Text, nullable=True)
    created_by = Column(String(225), nullable=True)
    created_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
 
class TRedacted(Base):
    __tablename__ = cfg.REDACTED_TABLE
    id = Column(Integer, primary_key=True)
    requestid = Column(UUID)
    fuuid = Column(UUID)
    dafileid = Column(UUID)
    filename = Column(String(255))
    category = Column(String(255))
    sensitivetext = Column(Text)
    placeholder = Column(String(255))
    context = Column(Text)
    iswrong = Column(Boolean, default = False)
    created_by = Column(String(225), nullable=True)
    created_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
 
class TApiCallLogs(Base):
    __tablename__ = cfg.APICALL_LOGS_TABLE
    id = Column(Integer, primary_key=True)
    func_name = Column(String(50))
    fileid = Column(UUID)
    requestid = Column(String(100))
    token_size = Column(Integer)
    prompt_length = Column(Integer)
    prompt = Column(Text)
    exec_time = Column(Float)
    status = Column(String(50))
    response = Column(JSONB, nullable=True)
    created_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
 
class TChangeDocument(Base):
    __tablename__ = cfg.CHANGE_DOCUMENT_TABLE
    id = Column(Integer, primary_key=True)
    dafileid = Column(String(255), nullable=False)      
    column_name = Column(String(255), nullable=False)
    oldvalue = Column(Text, nullable=False)
    newvalue = Column(Text, nullable=False)
    created_by = Column(String(225), nullable=True)
    created_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
 
class TLLMRawResponse(Base):
    __tablename__ = cfg.LLM_RAW_RESPONSE_TABLE
    id = Column(Integer, primary_key=True)
    requestid = Column(String(100), nullable=True)      
    fileid = Column(String(250), nullable=True)
    raw_response = Column(JSONB, nullable=True)
    created_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

class TDAApiCallLog(Base):
    """Simple table that stores a single JSONB column."""
    __tablename__ = cfg.DA_APICALL_LOGS_TABLE         # <-- exact table name
    id = Column(Integer, primary_key=True, autoincrement=True)
    calllog = Column(JSONB, nullable=True)
    created_date = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
class TZipFileDetails(Base):
    __tablename__ = cfg.ZIP_FILE_SUMMARY_TABLE 
    
    requestid = Column(UUID, primary_key=True)
    fuuid = Column(UUID, primary_key=True)
    dafileid = Column(UUID, primary_key=True)
    filename = Column(String(1000), primary_key=True)
    is_supported = Column(Boolean, default = False)
    content_extracted = Column(Boolean, default = False)
    summary = Column(Text, nullable=True)

class TDEEP_SEARCH_LOGS(Base):
    __tablename__ = cfg.DEEP_SEARCH_LOGS_TABLE
    id = Column(Integer, primary_key=True, autoincrement=True)
    requestid = Column(UUID, nullable=False)
    fuuid = Column(UUID, nullable=False)
    userquery = Column(String(4000), nullable=False)
    relevance_score = Column(Float)
    relevance = Column(Float)
    reason = Column(Text, nullable=True)
    created_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    is_app = Column(Boolean, default=False) 

class TCONSULTANT_FEEDBACK(Base):
    __tablename__ = cfg.CONSULTANT_FEEDBACK_TABLE  # You'll need to add this to config
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    requestid = Column(UUID(as_uuid=True), nullable=False)
    dafileid = Column(UUID(as_uuid=True), nullable=True)
    feedback = Column(String(50), nullable=False)
    message = Column(String(255), nullable=False)
    usercomments = Column(Text, nullable=True)
    created_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    
def retry_on_operational_error(max_attempts: int = 3, backoff: float = 1.0):
    """Retry a DB write when the underlying DBAPI raises OperationalError."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except (OperationalError, DBAPIError) as exc:
                    # SQLAlchemy may wrap the DBAPI error in DBAPIError
                    if isinstance(exc, DBAPIError) and not isinstance(exc.orig, OperationalError):
                        raise   # not the error we want to retry
                    attempt += 1
                    if attempt >= max_attempts:
                        logger.error(
                            f"Exceeded max retries ({max_attempts}) for {func.__name__}: {exc}",
                            exc_info=True
                        )
                        raise DatabaseWriteError(error=str(exc)) from exc
                    logger.warning(
                        f"OperationalError on attempt {attempt}/{max_attempts} for {func.__name__}: {exc}. "
                        f"Retrying in {backoff}s..."
                    )
                    # Dispose the broken engine/connection so the next acquire gets a fresh one
                    args[0].engine.dispose()
                    time.sleep(backoff)
        return wrapper
    return decorator

# ---------------- DB Manager ----------------
class DatabaseManager:
    _instance = None
 
    def __new__(cls,debug = False):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance.engine = create_engine(cfg.DATABASE_CONNECTION_STR,
                                                  pool_size=4,              # default: 4
                                                  max_overflow=5,           # allows up to 5 total connections
                                                  pool_timeout=60,           # seconds to wait before giving up
                                                  pool_recycle=1800,
                                                  pool_pre_ping=True, 
                                                  echo=False,
                                                  connect_args={'connect_timeout': 120})
            cls._instance.Session = sessionmaker(bind=cls._instance.engine,
                                                 expire_on_commit=True)
        return cls._instance
 
    def __init__(self, debug: bool = False):
        if not hasattr(self, 'session'):
            self.session = self.Session()

        if debug:
            logger.setLevel(logging.DEBUG)
 
    @retry_on_operational_error()
    def insert_event_record(self, header_dict: dict, payload_dict: dict, is_request: bool = True):
        """Insert event record with payload as JSON into the database."""
        def parse_date(date_string):
            try:
                # Try to parse the date string using dateutil
                if date_string.find('IST')>-1:
                        #    pendulum.from_format(datetime.strptime(date_string.replace('IST', '+0530'),'%a %b %d %H:%M:%S %z %Y').astimezone(timezone.utc).strftime('%a %b %d %H:%M:%S %z %Y').replace('+0000', 'UTC'))
                    return datetime.strptime(date_string.replace('IST', '+0530'),'%a %b %d %H:%M:%S %z %Y').astimezone(timezone.utc).strftime('%a %b %d %H:%M:%S %z %Y').replace('+0000', 'UTC')
                return dateutil.parser.parse(date_string)
            except ValueError:
                try:
                    # Try to parse the date string using pendulum
                    return pendulum.parse(date_string)
                except Exception as e:
                    # If all else fails, raise an error
                    raise ValueError(f"Unable to parse date string: {date_string}")
 
        try:
            created_on = parse_date(header_dict.get("createdOn"))
            logger.info(f"created_on: {created_on}")
 
            # try:
            #     created_on=pendulum.from_format(header_dict.get("createdOn"), "ddd MMM DD HH:mm:ss [UTC] YYYY")
            # except Exception as e:
            #     created_on = pendulum.from_format(datetime.strptime(header_dict.get("createdOn").replace('IST', '+0530'),'%a %b %d %H:%M:%S %z %Y').astimezone(timezone.utc).strftime('%a %b %d %H:%M:%S %z %Y').replace('+0000', 'UTC'))
           
            # Create Event with payload as JSON
            event = Event(
                event_type=header_dict.get("eventType"),
                event_sub_type=header_dict.get("eventSubType"),
                created_on=created_on,
                request_id=header_dict.get("requestId"),
                idx=header_dict.get("index"),
                total_count=header_dict.get("totalCount"),
                is_request=is_request,
                payload=payload_dict
            )
 
            logger.info(f"Inserting event {event.event_type} into database...")
            with self.Session() as session:
                session.add(event)
                session.commit()
                event_id = event.id
 
            message_type = "REQUEST" if is_request else "RESPONSE"
            logger.info(f"✅ Inserted {message_type} event with ID:{event_id}")
        except Exception as e:
            logger.error(f'Failed while inserting event into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while inserting event into DB: {e}')
        return event_id
   
    @retry_on_operational_error()
    def query_database(self, query: text,params: dict = None):
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(query, conn, params=params)
        except Exception as e:
            logger.error(f'Failed while querying DB: {e}', exc_info=True)
            raise DatabaseReadError(error = f'Failed while querying DB: {e}')

    @retry_on_operational_error()   
    def get_vwofferfamilydata(self):
        #(just like this for )
        # --- Read data from a view ---
        try:
            with self.engine.connect() as conn:
                query = text(f"""select * from  {cfg.DATABASE_SCHEMA}.{cfg.OFFERFAMILYDATA_VIEW} order by offer""")
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f'Failed while getting offers from DB: {e}', exc_info=True)
            raise DatabaseReadError(error = f'Failed while getting offers from DB: {e}')

    @retry_on_operational_error()
    def get_unique_ip_types(self):
        """Get unique list of IP types from vwdtpm_mapping view."""
        try:
            with self.engine.connect() as conn:
                query = text(f"""
                    SELECT distinct * 
                    FROM {cfg.DATABASE_SCHEMA}.{cfg.DTPMMAPPING_VIEW} 
                    ORDER BY ip_type
                """)
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f'Failed while getting unique IP types from DB: {e}', exc_info=True)
            raise DatabaseReadError(error=f'Failed while getting unique IP types from DB: {e}')
 
    @retry_on_operational_error()
    def insert_extraction_results(self, requestid: uuid, fuuid: uuid, dafileid: uuid, filename: str, extracteditems: list[dict]):
        try:
            with self.Session() as session:
                sow_objects = [
                    TExtraction(
                        requestid = requestid,
                        fuuid = fuuid,
                        dafileid = dafileid,
                        filename = filename,
                        category = data["label"],
                        sensitivetext = data["sensitivetext"],
                        context = data.get('context',''),
                        source = data.get("source",''),
                        score = data["score"],
                        reason = data.get("reason",''),
                        created_by = 'system'
                    )
                    for data in extracteditems
                    # if pd.isnull(data["sensitivetext"]) == False
                    # and pd.isnull(data["score"]) == False
                    # and pd.isnull(data["label"]) == False
                ]
 
                # Safe insert
                session.add_all(sow_objects)
                session.commit()
        except Exception as e:
            logger.error(f'Failed while inserting extraction results into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while inserting extraction results into DB: {e}')
 
    @retry_on_operational_error()
    def insert_toberedacted_results(self, requestid: uuid, fuuid: uuid, dafileid: uuid, filename: str, toberedacteditems: list[dict]):
        try:
            with self.Session() as session:
                sow_objects = [
                    TToBeRedacted(
                        requestid = requestid,
                        fuuid = fuuid,
                        dafileid = dafileid,
                        filename = filename,
                        category = data["label"],
                        sensitivetext = data["sensitivetext"],
                        created_by = 'system'
                    )
                    for data in toberedacteditems
                    # if pd.isnull(data["sensitivetext"]) == False
                    # and pd.isnull(data["label"]) == False
                ]
 
                # Safe insert
                session.add_all(sow_objects)
                session.commit()
        except Exception as e:
            logger.error(f'Failed while inserting toberedacted results into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while inserting toberedacted results into DB: {e}')
 
    @retry_on_operational_error()
    def insert_redacted_results(self, requestid: uuid, fuuid: uuid, dafileid: uuid, filename: str, redacteditems: list[dict]):
        try:
            with self.Session() as session:
                tredacted_objects = [
                    TRedacted(
                        requestid = requestid,
                        fuuid = fuuid,
                        dafileid = dafileid,
                        filename = filename,
                        category = data["label"],
                        sensitivetext = data["sensitivetext"],
                        placeholder = data["placeholder"],
                        context = "" if pd.isnull(data["context"]) else data["context"],
                        created_by = 'system'
                    )
                    for data in redacteditems
                    # if pd.isnull(data["sensitivetext"]) == False
                    # and pd.isnull(data["label"]) == False
                    # and pd.isnull(data["placeholder"]) == False
                ]
 
                # Safe insert
                session.add_all(tredacted_objects)
                session.commit()
        except Exception as e:
            logger.error(f'Failed while inserting redacted results into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while inserting redacted results into DB: {e}')
 
    @retry_on_operational_error()
    def delete_sow(self, **kwargs):
       try:
           with self.Session() as session:
               query = session.query(TStatementOfWork)
               for key, value in kwargs.items():
                   if hasattr(TStatementOfWork, key):
                       query = query.filter(getattr(TStatementOfWork, key) == value)
                   else:
                       logger.warning(f"Ignoring unknown filter key: {key}")
               query.delete()
               session.commit()
       except Exception as e:
           logger.error(f'Failed while deleting sow from DB: {e}', exc_info=True)
           raise DatabaseWriteError(error = f'Failed while deleting sow from DB: {e}')
       
    # ---------------- SOW ----------------
    @retry_on_operational_error()
    def insert_sow(self, sowrow: dict):
        try:
            with self.Session() as session:
                sow = TStatementOfWork(
                    requestid=sowrow["requestId"] if "requestId" in sowrow  else None,
                    projectid=sowrow["projectid"] if "projectid" in sowrow  else None,
                    dafileid=sowrow["dafileid"],
                    uniqueid=sowrow["uniqueid"],
                    sowfilename=sowrow["sowfilename"],
                    offer=sowrow["offer"] if "offer" in sowrow else None,
                    summary=sowrow["summary"] if "summary" in sowrow  else None,
                    # summaryvector=sowrow["summaryvector"] if "summaryvector" in sowrow else None,
                    # metadatavector=sowrow["metadatavector"] if "metadatavector" in sowrow  else None,
                    created_by = 'system'
                )
                session.add(sow)
                session.commit()
        except Exception as e:
            logger.error(f'Failed while inserting sow into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while inserting sow into DB: {e}')
   
    @retry_on_operational_error()
    def insert_bulk_sow(self, sowrowlist: list[dict]):
        try:
            with self.Session() as session:
                sow_objects = [
                    TStatementOfWork(
                        requestid=sowrow["requestId"] if "requestId" in sowrow  else None,
                        projectid=sowrow["projectid"] if "projectid" in sowrow  else None,
                        dafileid=sowrow["dafileid"],
                        uniqueid=sowrow["uniqueid"],
                        sowfilename=sowrow["sowfilename"],
                        offer=sowrow["offer"] if "offer" in sowrow else None,
                        summary=sowrow["summary"] if "summary" in sowrow  else None,
                        # summaryvector=sowrow["summaryvector"] if "summaryvector" in sowrow else None,
                        # metadatavector=sowrow["metadatavector"] if "metadatavector" in sowrow  else None,
                        created_by = 'system'
                    )
                    for sowrow in sowrowlist
                ]
                # Safe insert
                session.add_all(sow_objects)
                session.commit()
        except Exception as e:
            logger.error(f'Failed while inserting bulk sow into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while inserting bulk sow into DB: {e}')
    
    @retry_on_operational_error()
    def get_sows(self):
        try:
            with self.Session() as session:
                return session.query(TStatementOfWork).all()
        except Exception as e:
            logger.error(f'Failed while getting sows from DB: {e}', exc_info=True)
            raise DatabaseReadError(error = f'Failed while getting sows from DB: {e}')
        
    @retry_on_operational_error()
    def insert_consultant_feedback(self, **kwargs):
        try:
            with self.Session() as session:
                consultant_feedback = TCONSULTANT_FEEDBACK(**kwargs)
                session.add(consultant_feedback)
                session.commit()
                logger.info(f"Successfully inserted consultant feedback for requestid: {kwargs.get('requestid')}")
        except Exception as e:
            logger.error(f'Failed while inserting consultant feedback into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error=f'Failed while inserting consultant feedback into DB: {e}')
    
    @retry_on_operational_error()
    def update_sow_summary(self, project_id, summary):
        try:
            with self.Session() as session:
                sow = session.query(TStatementOfWork).filter_by(projectid=project_id).first()
                if sow:
                    sow.Summary = summary
                    session.commit()
        except Exception as e:
            logger.error(f'Failed while updating sow summary in DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while updating sow summary in DB: {e}')
 
    # def delete_sow(self, project_id):
    #     try:
    #         with self.Session() as session:
    #             sow = session.query(TStatementOfWork).filter_by(projectid=project_id).first()
    #             if sow:
    #                 session.delete(sow)
    #                 session.commit()
    #     except Exception as e:
    #         logger.error(f'Failed while deleting sow in DB: {e}', exc_info=True)
    #         raise DatabaseWriteError(error = e)
    
    @retry_on_operational_error()
    def insert_feedback(self, **kwargs):
        try:
            with self.Session() as session:
                feedback = TFeedback(**kwargs)
                session.add(feedback)
                session.commit()
        except Exception as e:
            logger.error(f'Failed while inserting feedback into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error =f'Failed while inserting feedback into DB: {e}')

    @retry_on_operational_error()   
    def insert_raw_response(self, rowlist: list[dict]):
        try:
            with self.Session() as session:
                responses = [
                    TLLMRawResponse(
                        requestid=row["requestid"] if "requestid" in row  else None,
                        fileid=row["fileid"] if "fileid" in row  else None,
                        raw_response=row["raw_response"] if "fileid" in row  else None,
                    )
                    for row in rowlist
                ]
                # Safe insert
                session.add_all(responses)
                session.commit()
        except Exception as e:
            logger.error(f'Failed while inserting raw response into DB: {e}', exc_info=True)
            # raise DatabaseWriteError(error =f'Failed while inserting raw response into DB: {e}')

    @retry_on_operational_error()   
    def insert_change_document(self, **kwargs):
        try:
            with self.Session() as session:
                change_document = TChangeDocument(**kwargs)
                session.add(change_document)
                session.commit()
                logger.info(f"Successfully inserted change document for dafileid: {kwargs.get('dafileid')}")
        except Exception as e:
            logger.error(f'Failed while inserting change document into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error=f'Failed while inserting change document into DB: {e}')
   
    # core/db/crud.py
    # def insert_change_document_bulk(self, rows: list[dict]) -> None:
    #     if not rows:
    #         return
    #     table = self.metadata.tables[cfg.CHANGE_DOCUMENT_TABLE]
    #     stmt = table.insert()
    #     with self.engine.begin() as conn:  
    #         conn.execute(stmt, rows)    
 
    # ---------------- GoldenTemplate ----------------
    @retry_on_operational_error()
    def get_documents(self, **kwargs):
        """
        Retrieves documents from the database, optionally filtered by specified criteria.
    
        Parameters:
        - **kwargs: Keyword arguments where the key is the column name in `tdocument` and the value is the filter value.
                    If the value is a list, the filter will use the IN operator.
    
        Returns:
        - A list of [TDocument] objects matching the filter criteria, or all documents if no filter is provided.
        """
        try:
            with self.Session() as session:
                query = session.query(TDocument)
 
                # Apply filters
                for key, value in kwargs.items():
                    if hasattr(TDocument, key):
                        # query = query.filter(getattr(TDocument, key) == value)
                        if isinstance(value, list):
                            # Use IN operator for list values
                            query = query.filter(getattr(TDocument, key).in_(value))
                        else:
                            # Use equality operator for single values
                            query = query.filter(getattr(TDocument, key) == value)
                    else:
                        logger.warning(f"Ignoring unknown filter key: {key}")
 
                return pd.read_sql(query.statement, session.bind)
        except Exception as e:
            logger.error(f'Failed while getting documents from DB: {e}', exc_info=True)
            raise DatabaseReadError(error = f'Failed while getting documents from DB: {e}')
    
    @retry_on_operational_error()
    def get_sensitiveinfo_list(self, **kwargs):
        """
        Retrieves records from TToBeRedacted as a DataFrame, 
        optionally filtered by specified criteria.
        """
        try:
            with self.Session() as session:
                # Query the whole model (returns all columns)
                query = session.query(TToBeRedacted)

                # Apply filters dynamically
                for key, value in kwargs.items():
                    if hasattr(TToBeRedacted, key):
                        query = query.filter(getattr(TToBeRedacted, key) == value)
                    else:
                        logger.warning(f"Ignoring unknown filter key for TToBeRedacted: {key}")

                return pd.read_sql(query.statement, session.bind)

        except Exception as e:
            logger.error(f'Failed while getting redaction data from DB: {e}', exc_info=True)
            raise DatabaseReadError(error=f'Failed while getting redaction data from DB: {e}')
    
    @retry_on_operational_error()
    def get_documents_state(self, **kwargs):
        """
        Retrieves documents state from the database, optionally filtered by specified criteria.
   
        Parameters:
        - **kwargs: Keyword arguments where the key is the column name in `tprocess_ips_state` and the value is the filter value.
   
        Returns:
        - A list of [TDocument_State] objects matching the filter criteria, or all documents if no filter is provided.
        """
        try:
            
            with self.Session() as session:
                query = session.query(TDocument_State)
 
                # Apply filters
                for key, value in kwargs.items():
                    if hasattr(TDocument_State, key):
                        query = query.filter(getattr(TDocument_State, key) == value)
                    else:
                        logger.warning(f"Ignoring unknown filter key: {key}")
                df = pd.read_sql(query.statement, session.bind)
                for col in ('request_dir','filepath','converted_filepath','extraction_input_file','classification_input_file','out_filepath','redacted_items_filepath'):
                    df[col]= df[col].apply(lambda v: Path(v) if v is not None else v)
                return df
        except Exception as e:
            logger.error(f'Failed while getting documents state from DB: {e}', exc_info=True)
            raise DatabaseReadError(error = f'Failed while getting documents state from DB: {e}')
        
    @retry_on_operational_error()
    def get_vwclassificationout_row(self, **kwargs):
        """
        Retrieves rows from the vwdocuments view, optionally filtered by specified criteria.
   
        Parameters:
        - **kwargs: Keyword arguments where the key is the column name and the value is the filter value.
   
        Returns:
        - A Pandas DataFrame containing the filtered rows.
        """
        try:
            with self.engine.connect() as conn:
                # Construct the base query
                query = f"""SELECT * FROM {cfg.DATABASE_SCHEMA}.{cfg.CLASSIFICATIONOUT_VIEW}"""
 
                # Apply filters
                conditions = []
                for key, value in kwargs.items():
                    if value is None:
                        conditions.append(f"{key} is Null")
                    else:
                        conditions.append(f"{key} = '{value}'")
 
                # Add the WHERE clause if there are conditions
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
 
                # Execute the query and return the results as a DataFrame
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f'Failed while getting {cfg.DATABASE_SCHEMA}.{cfg.CLASSIFICATIONOUT_VIEW} from DB: {e}', exc_info=True)
            raise DatabaseReadError(error = f'Failed while getting {cfg.DATABASE_SCHEMA}.{cfg.CLASSIFICATIONOUT_VIEW} from DB: {e}')

    @retry_on_operational_error()   
    def get_vwdocument_row(self, sel_cols:list=[],**kwargs):
        """
        Retrieves rows from the vwdocuments view, optionally filtered by specified criteria.
   
        Parameters:
        - **kwargs: Keyword arguments where the key is the column name and the value is the filter value.
   
        Returns:
        - A Pandas DataFrame containing the filtered rows.
        """
        select_list = ','.join(sel_cols) if len(sel_cols)>0 else '*'
        try:
            with self.engine.connect() as conn:
                # Construct the base query
                query = f"""SELECT {select_list} FROM {cfg.DATABASE_SCHEMA}.{cfg.DOCUMENTS_VIEW}"""
 
                # Apply filters
                conditions = []
                for key, value in kwargs.items():
                    # Check if value is iterable (but not a string) and use IN operator
                    if hasattr(value, '__iter__') and not isinstance(value, (str, bytes)):
                        # Convert iterable to tuple for SQL IN clause and escape each item
                        escaped_values = []
                        for item in value:
                            escaped_item = str(item).replace("'", "''")  # Escape single quotes
                            escaped_values.append(f"'{escaped_item}'")
                        conditions.append(f"{key} IN ({', '.join(escaped_values)})")
                    else:
                        conditions.append(f"{key} = '{value}'")
 
                # Add the WHERE clause if there are conditions
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
 
                # Execute the query and return the results as a DataFrame
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f'Failed while getting {cfg.DATABASE_SCHEMA}.{cfg.DOCUMENTS_VIEW} from DB: {e}', exc_info=True)
            raise DatabaseReadError(error = f'Failed while getting {cfg.DATABASE_SCHEMA}.{cfg.DOCUMENTS_VIEW} from DB: {e}')

    @retry_on_operational_error()   
    def get_vwdtpm_mapping(self):
    # --- Read data from a view ---
        try:
            with self.engine.connect() as conn:
                query = text(f"""select * from {cfg.DATABASE_SCHEMA}.{cfg.DTPMMAPPING_VIEW} order by ip_type""")
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f'Failed while getting DTPM mapping from DB: {e}', exc_info=True)
            raise DatabaseReadError(error=f'Failed while getting DTPM mapping from DB: {e}')
    
    @retry_on_operational_error()
    def get_vwtoberedacted(self, **kwargs):
        try:
            with self.engine.connect() as conn:
                query = f"""SELECT * FROM {cfg.DATABASE_SCHEMA}.{cfg.SHORT_LABEL_VIEW}"""
                conditions = []
                for key, value in kwargs.items():
                    if value is None:
                        conditions.append(f"{key} is Null")
                    else:
                        conditions.append(f"{key} = '{value}'")
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f'Failed getting {cfg.SHORT_LABEL_VIEW}: {e}', exc_info=True)
            raise DatabaseReadError(error=f'Failed getting {cfg.SHORT_LABEL_VIEW}: {e}')

    @retry_on_operational_error()   
    def insert_document(self, **kwargs):
        try:
            with self.Session() as session:
                document = TDocument(**kwargs)# if len(kwargs) > 0 else kwargs.get('docrow')
                session.add(document)
                session.commit()
                # return template.fileid  # return generated ID
        except Exception as e:
            logger.error(f'Failed while inserting document into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while inserting document into DB: {e}')
    
    @retry_on_operational_error()   
    def insert_deep_search_logs(self, rows):
        try:
            with self.Session() as session:
                stmt = insert(TDEEP_SEARCH_LOGS)
                session.execute(stmt, rows)
                session.commit()
                # return template.fileid  # return generated ID
        except Exception as e:
            logger.error(f'Failed while inserting Deep search logs into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while inserting Deep search logs into DB: {e}')
        
    @retry_on_operational_error()   
    def insert_document_state(self, **kwargs):
        kwargs={key:(str(val) if isinstance(val, Path) else val) for key,val in kwargs.items()}
        try:
            with self.Session() as session:
                doc_state = TDocument_State(**kwargs)# if len(kwargs) > 0 else kwargs.get('docrow')
                session.add(doc_state)
                session.commit()
                # return template.fileid  # return generated ID
        except Exception as e:
            logger.error(f'Failed while inserting document state into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while inserting document state into DB: {e}')
    
    @retry_on_operational_error()
    def update_document_state(self, where_clause: dict, update_values: dict):
        """
        Updates documents in the database based on the provided WHERE clause and update values.
   
        Parameters:
        - where_clause (dict): A dictionary where keys are column names and values are the conditions to match.
        - update_values (dict): A dictionary where keys are column names and values are the new values to update.
   
        Returns:
        - The number of rows updated.
        """
        update_values = {key:(str(val) if isinstance(val, Path) else val) for key,val in update_values.items()}
        try:
            with self.Session() as session:
                query = session.query(TDocument_State)
 
                # Apply WHERE clause filters
                for key, value in where_clause.items():
                    if hasattr(TDocument_State, key):
                        query = query.filter(getattr(TDocument_State, key) == value)
                    else:
                        logger.warning(f"Ignoring unknown column in WHERE clause: {key}")
 
                # Update the filtered rows
                updated_rows = query.update(update_values)
 
                # Commit the changes
                session.commit()
        except Exception as e:
            logger.error(f'Failed while updating document state in DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while updating document state in DB: {e}')

    @retry_on_operational_error()
    def delete_document_state(self, where_clause: dict):
        """
        Delete rows from the `[TDocument]` table that match the supplied WHERE clause.
    
        Parameters
        ----------
        where_clause : dict
            Mapping of column names to values that identify the rows to delete.
            Example: ``{'dafileid': 'DA001'}``.
    
        Returns
        -------
        int
            Number of rows that were deleted.
    
        Raises
        ------
        DatabaseWriteError
            If anything goes wrong while executing the delete.
        """
        try:
            with self.Session() as session:
                query = session.query(TDocument_State)
    
                # Build the filter from the supplied clause
                for key, value in where_clause.items():
                    if hasattr(TDocument_State, key):
                        query = query.filter(getattr(TDocument_State, key) == value)
                    else:
                        logger.warning(f"Ignoring unknown column in WHERE clause: {key}")
    
                # Perform the delete; ``synchronize_session=False`` is safe for bulk ops
                deleted_rows = query.delete(synchronize_session=False)
    
                # Commit the transaction
                session.commit()
                logger.info(f"Deleted {deleted_rows} row(s) from TDocument_State with filter {where_clause}")
    
                return deleted_rows
        except Exception as e:
            logger.error(f'Failed while deleting document(s) from DB: {e}', exc_info=True)
            raise DatabaseWriteError(error=f'Failed while deleting document(s) from DB: {e}')

    @retry_on_operational_error()
    def insert_bulk_document(self, docrowlist: list[dict]):
        try:
            with self.Session() as session:
                sow_objects = [
                    TDocument(
                        requestid=docrow["requestid"],
                        daoriginal_fileid = docrow["daoriginalfileid"] ,
                        dafileid = docrow["dafileid"] if "dafileid" in docrow  else None,
                        filename = docrow["filename"],
                        title = docrow["title"]  if "title" in docrow  else None,
                        description = docrow["description"] if "description" in docrow  else None,
                        dtpm_phase = docrow["dtpm_phase"],
                        document_type = docrow["document_type"],
                        ip_type = docrow["ip_type"],
                        offer = docrow["offer"],
                        confidence_score = docrow["confidence_score"] if "confidence_score" in docrow  else None,
                        # description_vector = docrow["description_vector"] if "description_vector" in docrow  else None,
                        # metadata_vector = docrow["metadata_vector"] if "metadata_vector" in docrow  else None,
                        created_by = 'system',
                        dasanitizationoutfileid = docrow["dasanitizationoutfileid"] if "dasanitizationoutfileid" in docrow  else None,
                        daclassificationoutfileid = docrow["daclassificationoutfileid"] if "daclassificationoutfileid" in docrow  else None,
                        status = docrow["status"] if "status" in docrow  else None,
                        ipid = docrow["ipid"] if "ipid" in docrow  else None,
                        initialgrade = docrow["initialgrade"] if "initialgrade" in docrow  else None,
                        similarityscore = docrow["initialgrade"] if "initialgrade" in docrow  else None,
                        priority = docrow["priority"] if "priority" in docrow  else None
                    )
                    for docrow in docrowlist
                ]
 
                # Safe insert
                session.add_all(sow_objects)
                session.commit()
        except Exception as e:
            logger.error(f'Failed while inserting document into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while inserting document into DB: {e}')
    
    @retry_on_operational_error()
    def update_document(self, where_clause: dict, update_values: dict):
        """
        Updates documents in the database based on the provided WHERE clause and update values.
   
        Parameters:
        - where_clause (dict): A dictionary where keys are column names and values are the conditions to match.
        - update_values (dict): A dictionary where keys are column names and values are the new values to update.
   
        Returns:
        - The number of rows updated.
        """
        try:
            with self.Session() as session:
                query = session.query(TDocument)
 
                # Apply WHERE clause filters
                for key, value in where_clause.items():
                    if hasattr(TDocument, key):
                        query = query.filter(getattr(TDocument, key) == value)
                    else:
                        logger.warning(f"Ignoring unknown column in WHERE clause: {key}")
 
                # Update the filtered rows
                updated_rows = query.update(update_values)
                # print(updated_rows)
                # Commit the changes
                session.commit()
                return updated_rows>0
        except Exception as e:
            logger.error(f'Failed while updating document in DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while updating document in DB: {e}')
        
    @retry_on_operational_error()
    def update_gtl_synopsis(self, totalRedacted: int, **filters):
        """
        Updates gtl_synopsis column using dynamic AND filters.
        """
        try:
            with self.Session() as session:
                query = session.query(TDocument)
                for key, value in filters.items():
                    if hasattr(TDocument, key):
                        query = query.filter(getattr(TDocument, key) == value)
                    else:
                        logger.warning(f"Ignoring unknown filter key for TDocument: {key}")

                document = query.first()

                if not document:
                    logger.warning(f"No document found for filters: {filters}")
                    return False

                if totalRedacted == 0:
                    redaction_line = "<br><b>Redaction Summary:</b> There are no sensitive items found in this file."
                else:
                    redaction_line = (f"<br><b>Redaction Summary:</b> " f"{totalRedacted} sensitive item's got redacted for this file.")

                existing_synopsis = document.gtl_synopsis or ""

                # Remove previous Redaction Summary if exists
                if "Redaction Summary:" in existing_synopsis:
                    existing_synopsis = existing_synopsis.split("<br><b>Redaction Summary:</b>")[0]

                updated_synopsis = existing_synopsis + redaction_line

                updated_rows = query.update({"gtl_synopsis": updated_synopsis})
                session.commit()
                return updated_rows > 0

        except Exception as e:
            logger.error(f"Failed to update gtl_synopsis: {e}", exc_info=True)
            raise DatabaseWriteError(
                error=f"Failed while updating gtl_synopsis: {e}"
            )
    
    @retry_on_operational_error()
    def delete_document(self, where_clause: dict):
        """
        Delete rows from the `[TDocument]` table that match the supplied WHERE clause.
    
        Parameters
        ----------
        where_clause : dict
            Mapping of column names to values that identify the rows to delete.
            Example: ``{'dafileid': 'DA001'}``.
    
        Returns
        -------
        int
            Number of rows that were deleted.
    
        Raises
        ------
        DatabaseWriteError
            If anything goes wrong while executing the delete.
        """
        try:
            with self.Session() as session:
                query = session.query(TDocument)
    
                # Build the filter from the supplied clause
                for key, value in where_clause.items():
                    if hasattr(TDocument, key):
                        query = query.filter(getattr(TDocument, key) == value)
                    else:
                        logger.warning(f"Ignoring unknown column in WHERE clause: {key}")
    
                # Perform the delete; ``synchronize_session=False`` is safe for bulk ops
                deleted_rows = query.delete(synchronize_session=False)
    
                # Commit the transaction
                session.commit()
                logger.info(f"Deleted {deleted_rows} row(s) from TDocument with filter {where_clause}")
    
                return deleted_rows
        except Exception as e:
            logger.error(f'Failed while deleting document(s) from DB: {e}', exc_info=True)
            raise DatabaseWriteError(error=f'Failed while deleting document(s) from DB: {e}')
    
    @retry_on_operational_error()
    def insert_recommendation(self, **kwargs):
        try:
            with self.Session() as session:
                recommendation = TRecommendation(**kwargs)
                session.add(recommendation)
                session.commit()
        except Exception as e:
            logger.error(f'Failed while inserting recommendation into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while inserting recommendation into DB: {e}')
   
    @retry_on_operational_error()
    def insert_bulk_recommendation(self, recrowlist: list[dict]):
        try:
            for row in recrowlist:
                row['status'] = 'GENERATED'
                row['created_by'] = 'system'
 
            with self.Session() as session:
               
                stmt = insert(TRecommendation)
                session.execute(stmt, recrowlist)
                session.commit()
        except Exception as e:
            logger.error(f'Failed while inserting recommendation into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while inserting recommendation into DB: {e}')
 
    # def update_recommendation_status(self, project_id, file_id, status):
    #     try:
    #         with self.Session() as session:
    #             rec = session.query(TRecommendation).filter_by(projectid=project_id, fileid=file_id).first()
    #             if rec:
    #                 rec.Status = status
    #                 session.commit()
    #     except Exception as e:
    #         logger.error(f'Failed while updating recommendation status in DB: {e}', exc_info=True)
    #         raise DatabaseWriteError(error = e)
 
    @retry_on_operational_error()
    def delete_recommendation(self, **filters):
        """
        Delete rows from `[TRecommendation](cci:2://file:///c:/Users/Lekhnath_Pandey/CIQ/ip_content_management/core/db/crud.py:96:0-115:83)` that match the supplied filter criteria.

        Parameters
        ----------
        **filters : dict
            Column‑name/value pairs used to build the ``WHERE`` clause.
            Example: ``project_id="P001", file_id="F123", status="SENT"``

        Returns
        -------
        int
            Number of rows that were deleted.

        Raises
        ------
        DatabaseWriteError
            If anything goes wrong while executing the delete.
        """
        if not filters:
            logger.warning("delete_recommendation called without any filter – no rows will be removed.")
            return 0

        try:
            with self.Session() as session:
                # Build a query that filters by the supplied columns
                query = session.query(TRecommendation).filter_by(**filters)

                # ``synchronize_session=False`` is safe for bulk deletes
                deleted_rows = query.delete(synchronize_session=False)

                session.commit()
                logger.info(
                    f"Deleted {deleted_rows} TRecommendation row(s) with filter {filters}"
                )
                # return deleted_rows
        except Exception as e:
            logger.error(
                f'Failed while deleting recommendation(s) in DB: {e}',
                exc_info=True,
            )
            raise DatabaseWriteError(error=f'Failed while deleting recommendation(s) in DB: {e}')
 
    @retry_on_operational_error()
    def query_recommendations(
        self,
        project_id: Optional[str] = None,
        file_id: Optional[str] = None,
        template_id: Optional[str] = None,
        status: Optional[str] = None,
        phase: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[TRecommendation]:
        """
        Query the [TRecommendation] table with optional filters.
 
        Parameters
        ----------
        project_id : str | None
            Filter by ``projectid`` column.
        file_id : str | None
            Filter by ``fileid`` column.
        template_id : str | None
            Filter by ``templateid`` column.
        status : str | None
            Filter by `[status] column.
        phase : Sequence[str] | None
            Filter rows whose ``phase`` array contains *any* of the supplied values.
            (SQLAlchemy translates this to the ``ANY`` operator for PostgreSQL arrays.)
        limit : int | None
            Maximum number of rows to return.
        offset : int | None
            Number of rows to skip (useful for pagination).
 
        Returns
        -------
        List[TRecommendation]
            List of matching [TRecommendation] ORM objects.
        """
        try:
            with self.Session() as session:
                query = session.query(TRecommendation)
 
                # Apply filters only when the argument is not None
                if project_id is not None:
                    query = query.filter(TRecommendation.projectid == project_id)
                if file_id is not None:
                    query = query.filter(TRecommendation.fileid == file_id)
                if template_id is not None:
                    query = query.filter(TRecommendation.templateid == template_id)
                if status is not None:
                    query = query.filter(TRecommendation.status == status)
 
                # ``phase`` is an ARRAY column – use ``any`` to test membership
                if phase:
                    # ``any`` works with a list/tuple of values
                    query = query.filter(TRecommendation.phase.any(phase))
 
                # Pagination
                if offset is not None:
                    query = query.offset(offset)
                if limit is not None:
                    query = query.limit(limit)
 
            return query.all()
        except Exception as e:
            logger.error(f'Failed while querying recommendations in DB: {e}', exc_info=True)
            raise DatabaseReadError(error = f'Failed while querying recommendations in DB: {e}')
       
    @retry_on_operational_error()
    def update_recommendation(self, where_clause: dict, update_values: dict):
        """
        Updates documents in the database based on the provided WHERE clause and update values.
   
        Parameters:
        - where_clause (dict): A dictionary where keys are column names and values are the conditions to match.
        - update_values (dict): A dictionary where keys are column names and values are the new values to update.
   
        Returns:
        - The number of rows updated.
        """
        try:
            with self.Session() as session:
                query = session.query(TRecommendation)
 
                # Apply WHERE clause filters
                for key, value in where_clause.items():
                    if hasattr(TRecommendation, key):
                        query = query.filter(getattr(TRecommendation, key) == value)
                    else:
                        logger.warning(f"Ignoring unknown column in WHERE clause: {key}")
 
                # Update the filtered rows
                updated_rows = query.update(update_values)
 
                # Commit the changes
                session.commit()
        except Exception as e:
            logger.error(f'Failed while updating recommendation in DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while updating recommendation in DB: {e}')
    
    @retry_on_operational_error()
    def insert_tda_api_call_log(self, log_dict: dict) -> int:
        try:
            new_row = TDAApiCallLog(calllog=log_dict)

            with self.Session() as session:
                session.add(new_row)
                session.commit()
        except Exception as e:
            logger.warning(f'Failed while logging dell attachments api call: {e}', exc_info=True)

    @retry_on_operational_error()
    def log_api_call(self, **kwargs):
        try:
            with self.Session() as session:
                log = TApiCallLogs(**kwargs)
                session.add(log)
                session.commit()
        except Exception as e:
            logger.error(f'Failed while inserting api call logs into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while inserting api call logs into DB: {e}')
        
    
    @retry_on_operational_error()
    def get_redaction_config_dict(self):
        try:
            with self.Session() as session:
                inclusion = session.query(TRedactionInclusion).all()
                exclusion = session.query(TRedactionExclusion).all()

             
            inclusion_lst = [{'sensitivetext': row.toberedacted, 'label': row.label, 'score': 1.0}  for row in inclusion]
            exclusion_lst = [row.exclude.lower() for row in exclusion]
            return inclusion_lst, exclusion_lst

        except Exception as e:
            logger.error("Failed to fetch redaction config tables", exc_info=True)
            raise DatabaseReadError(error=str(e))
    
    @retry_on_operational_error()   
    def insert_zip_file_details(self, recrowlist: list[dict]):
        try:
            with self.Session() as session:
               
                stmt = insert(TZipFileDetails)
                session.execute(stmt, recrowlist)
                session.commit()

                # return template.fileid  # return generated ID
        except Exception as e:
            logger.error(f'Failed while inserting zip details into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error = f'Failed while inserting zip details into DB: {e}')

    @retry_on_operational_error()
    def update_tzip_file_details(self, where_clause: dict, update_values: dict):
        try:
            with self.Session() as session:
                query = session.query(TZipFileDetails)

                # Apply WHERE clause filters
                for key, value in where_clause.items():
                    if hasattr(TZipFileDetails, key):
                        query = query.filter(getattr(TZipFileDetails, key) == value)
                    else:
                        logger.warning(f"Ignoring unknown column in WHERE clause: {key}")

                # Update the filtered rows
                updated_rows = query.update(update_values)

                # Commit the changes
                session.commit()
                
                logger.info(f"Updated {updated_rows} rows in tzip_file_summary table")
                return updated_rows
                
        except Exception as e:
            logger.error(f'Failed while updating tzip_file_summary in DB: {e}', exc_info=True)
            raise DatabaseWriteError(error=f'Failed while updating tzip_file_summary in DB: {e}')
        
    @retry_on_operational_error()
    def get_tzip_file_details(self, **kwargs):
        try:
            with self.Session() as session:
                query = session.query(TZipFileDetails)
 
                # Apply filters
                for key, value in kwargs.items():
                    if hasattr(TZipFileDetails, key):
                        query = query.filter(getattr(TZipFileDetails, key) == value)
                    else:
                        logger.warning(f"Ignoring unknown filter key: {key}")
 
                return pd.read_sql(query.statement, session.bind)
        except Exception as e:
            logger.error(f'Failed while getting zip details from DB: {e}', exc_info=True)
            raise DatabaseReadError(error = f'Failed while getting zip details from DB: {e}')

    @retry_on_operational_error()
    def get_fileids_by_dafileid(self, dafileids: List) -> Dict[str, str]:
        if not dafileids:
            return {}
        
        try:
            with self.Session() as session:
                query = text("""
                    SELECT dafileid::text as daf_key, fuuid::text 
                    FROM ciq_fssit.tdocument 
                    WHERE dafileid = ANY(CAST(:dafileids AS UUID[]))
                """)
                
                # Cast to UUID array
                uuid_list = [str(d) for d in dafileids]
                result = session.execute(query, {"dafileids": uuid_list}).fetchall()
                
                mapping = {row.daf_key.lower(): row.fuuid for row in result}
                for dafileid in dafileids:
                    key = str(dafileid).lower()
                    mapping.setdefault(key, None)
                
                logger.info(f"Found {len(result)}/{len(dafileids)} UUID matches")
                return mapping
                
        except Exception as e:
            logger.error(f'Failed get_fileids_by_dafileid: {e}')
            raise DatabaseReadError(f"Failed dafileid lookup: {e}")

    @retry_on_operational_error()
    def insert_consultant_feedback(self, **kwargs):
        try:
            with self.Session() as session:
                consultant_feedback = TCONSULTANT_FEEDBACK(**kwargs)
                session.add(consultant_feedback)
                session.commit()
                logger.info(f"Successfully inserted consultant feedback for requestid: {kwargs.get('requestid')}")
        except Exception as e:
            logger.error(f'Failed while inserting consultant feedback into DB: {e}', exc_info=True)
            raise DatabaseWriteError(error=f'Failed while inserting consultant feedback into DB: {e}')


    @retry_on_operational_error()
    def get_existing_dafileids(self, table_name: str, dafileid_list: List[str], schema: str = "consult_np") -> List[str]:
        """
        Check if dafileids already exist in embedding table.        
        """
        if not dafileid_list:
            return []
        
        try:
            with self.Session() as session:
                query = text(f"""
                    SELECT DISTINCT cmetadata->>'dafileid' as dafileid
                    FROM {schema}.{table_name} 
                    WHERE cmetadata->>'dafileid' = ANY(:dafileids)
                """)
                
                result = session.execute(
                    query, 
                    {"dafileids": [str(d) for d in dafileid_list]}
                ).fetchall()
                
                existing = [row.dafileid for row in result]
                logger.info(f"Found {len(existing)}/{len(dafileid_list)} existing dafileids in {schema}.{table_name}")
                return existing
                
        except Exception as e:
            logger.error(f"Failed get_existing_dafileids({table_name}): {e}")
            raise DatabaseReadError(f"Failed embedding check: {e}")
