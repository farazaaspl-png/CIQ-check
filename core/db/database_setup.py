import configparser, sys, enum, logging
from pathlib import Path
from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Integer, Text, Boolean,
    ForeignKey, ForeignKeyConstraint, ARRAY, Enum, UUID, DateTime, func
)
# from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.types import UserDefinedType
from sqlalchemy.exc import ProgrammingError

from core.utility import get_custom_logger
logger = get_custom_logger(__name__)
# ---------------- ENUM for Recommendation Status ----------------
class RecommendationStatus(enum.Enum):
    accepted = "accepted"
    rejected = "rejected"
    skipped = "skipped"

# Global enum type to reuse
recommendation_enum = Enum(
    RecommendationStatus,
    name="recommendation_status",
    validate_strings=True
)


DROP_IF_EXISTS = True
# ---------------- Load DB Config ----------------
def load_connection_string(configfile = r"config.ini", section = "database"):
    if not Path(configfile).exists():
        logger.error(f"Config file not found: {configfile}")
        raise FileNotFoundError(Path(configfile))

    config = configparser.ConfigParser()
    if not Path(r"config.ini").exists():
        logger.error('config file not found')
        sys.exit(0)
    config.read(Path(r'config.ini'))
    
    if not config.has_section(section):
        logger.error(f"Section {section} not found in {configfile}")
        raise Exception(f"Section {section} not found in {configfile}")
    return config.get(section, "connectionstr"), config.get(section, "schema", fallback="public")

class VECTOR(UserDefinedType):
    def __init__(self, dimensions):
        self.dimensions = dimensions

    def get_col_spec(self, **kw):
        return f"vector({self.dimensions})"
    
# ---------------- Define Tables ----------------
def define_tables(metadata):
    
    tstatementofwork = Table(
        "tstatementofwork", metadata,
        Column("projectid", String(100), unique=True),
        Column("fileid", UUID, primary_key=True),
        Column("customerjourney", String(100)),
        Column("offerfamily", ARRAY(Text)),
        Column("technology", ARRAY(Text)),
        Column("summary", Text),
        Column("summaryvector", VECTOR(1024)),
        Column("metadatavector", VECTOR(1024)),
        # Column("contentvector", VECTOR(1024)),
        Column("created_by", String(225), nullable=True),
        Column("updated_by", String(225), nullable=True),
        Column("created_date", DateTime(timezone=True), server_default=func.now(), nullable=False),
        Column("updated_date", DateTime(timezone=True), onupdate=func.now())
    )
    tdocument = Table(
        "tdocument", metadata,
        Column("projectid", String(100)),
        Column("originalfileid", UUID, unique=True),
        Column("fileid", UUID, primary_key=True),
        Column("filename", String(255)),
        Column("title", Text),
        Column("customerjourney", String(100)),
        Column("offerfamily", ARRAY(Text)),
        Column("technology", ARRAY(Text)),
        Column("description", Text),
        Column("phase", ARRAY(Text)),
        Column("type", String(50)),
        Column("version", String(50)),
        Column("owner", String(50)),
        Column("region", String(50)),
        Column("path", Text),
        Column("usagecount", Integer),
        Column("isgold", Boolean, default = False),
        Column("descriptionvector", VECTOR(1024)),
        Column("metadatavector", VECTOR(1024)),
        # Column("contentvector", VECTOR(1024)),
        Column("created_by", String(225), nullable=True),
        Column("updated_by", String(225), nullable=True),
        Column("created_date", DateTime(timezone=True), server_default=func.now(), nullable=False),
        Column("updated_date", DateTime(timezone=True), onupdate=func.now())
    )
    trecommendation = Table(
        "trecommendation", metadata,
        Column("sowfileid", String(100), ForeignKey("tstatementofwork.fileid")),
        Column("fileid", UUID, primary_key=True),
        Column("templateid", UUID, ForeignKey("tdocument.fileid")),
        Column("phase", ARRAY(Text)),
        Column("status", String(100)),
        Column("created_by", String(225), nullable=True),
        Column("updated_by", String(225), nullable=True),
        Column("created_date", DateTime(timezone=True), server_default=func.now(), nullable=False),
        Column("updated_date", DateTime(timezone=True), onupdate=func.now())
    )
    tsanitization = Table(
        "tsanitization", metadata,
        Column("id",Integer, primary_key=True),
        Column("fileid", UUID),
        Column("label", String(255)),
        Column("text", Text),
        Column("context", Text),
        Column("isredacted", Boolean, default = False),
        Column("iswrong", Boolean, default = False),
        Column("created_by", String(225), nullable=True),
        Column("updated_by", String(225), nullable=True),
        Column("created_date", DateTime(timezone=True), server_default=func.now(), nullable=False),
        Column("updated_date", DateTime(timezone=True), onupdate=func.now())
    )
    tfeedback = Table(
    "tfeedback", metadata,

    Column("uuid", UUID(as_uuid=True), primary_key=True),
    Column("name",      String(255), nullable=False),   
    Column("dafileid",  String(255), nullable=False),   
    Column("status",    String(100), nullable=False),   
    Column("feedback",  Text,        nullable=True),   
    Column("created_by", String(225), nullable=True),
    Column("created_by", String(225), nullable=True),
    Column("updated_by", String(225), nullable=True),
    Column("created_date", DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_date", DateTime(timezone=True), onupdate=func.now())
)
    return ([tstatementofwork, tdocument, trecommendation, tsanitization])

    


# ---------------- Main ----------------
def main():
    try:
        conn_str, schema = load_connection_string()
        engine = create_engine(conn_str, echo=False)
        metadata = MetaData()

        tables = define_tables(metadata)

        for table in tables:
            if engine.dialect.has_table(engine.connect(), table.name):
                logger.info(f"Table {schema}.{table.name} already exists")
                if DROP_IF_EXISTS:
                    table.drop(engine)
                    logger.info(f"Table {schema}.{table.name} dropped")
                    logger.info(f"Creating table {schema}.{table.name}...")
                    table.create(engine)
                    logger.info(f"Table {schema}.{table.name} created")
            else:
                logger.info(f"Creating table {schema}.{table.name}...")
                table.create(engine)
                logger.info(f"Table {schema}.{table.name} created")

    except ProgrammingError as e:
        logger.error(f"Error while creating tables: {e}")
    except Exception as e:
        logger.error(f"Error while setting up database: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()
