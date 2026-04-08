import os, logging, sys, re
from pathlib import Path

from sqlalchemy import create_engine, text
# from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
try:
    from dotenv import load_dotenv
    # load .env from root folder (parent of databasescript)
    ROOT_DIR = Path(__file__).resolve().parent.parent
    load_dotenv(dotenv_path=ROOT_DIR / ".env")
except Exception:
    pass

SCRIPT_DIR = Path(__file__).resolve().parent

def get_db_config():
    """Read DB connection string and schema from environment."""
    db_url = os.getenv("DATABASE_CONNECTION_STR")
    schema = os.getenv("DATABASE_SCHEMA")
    if not db_url:
        raise RuntimeError("DATABASE_CONNECTION_STR not set in .env")
    if not schema:
        raise RuntimeError("DATABASE_SCHEMA not set in .env")
    return db_url, schema

def get_counter_number(engine,schema: str) -> int:
    with engine.connect() as conn:
        result = conn.execute(
            text(f"""select split_part(script_name,'_',1)::int as index_number
                     from {schema}.tdeployedscripts
                     order by id desc 
                     limit 1"""),
        ).first()
    return result[0]

def extract_prefix(file_name: str) -> int:
    """Extract leading digits from filename, e.g. '01_init.sql' -> 1."""
    match = re.match(r"^(\d+)", file_name)
    return int(match.group(1)) if match else -1

def main() -> int:
    cwd = Path.cwd()
    logger.info(cwd)

    try:
        db_url, schema = get_db_config()
        engine = create_engine(db_url, pool_pre_ping=True, future=True)
    except Exception as e:
        logger.error(f"Database configuration failed: {e}", file=sys.stderr)
        return 2
    
    idx_num = get_counter_number(engine,schema)
    sql_files = sorted([f for f in SCRIPT_DIR.glob("*.sql") if f.is_file() and int(Path(f).name.split('_')[0])>idx_num],
                   key=lambda f: int(extract_prefix(f.name)))

    success = 0
    failures = 0

    for sql_file in sql_files:
        logger.info(f"\nApplying: {sql_file.name}")
        try:
            sql_text = sql_file.read_text(encoding="utf-8")
            if not sql_text.strip():
                logger.warning(f"[WARN] {sql_file.name} is empty; skipping.")
                continue

            with engine.begin() as conn:
                sql_text = sql_text.replace('ciq_fssit',schema)
                # logger.info(sql_text)

                conn.exec_driver_sql(sql_text)
                conn.execute(
                    text(f"INSERT INTO {schema}.tdeployedscripts (script_name) VALUES (:name);"),
                    {"name": sql_file.name},
                )

                logger.info(f"[OK] Applied {sql_file.name}")
                success += 1
        except (SQLAlchemyError, RuntimeError) as e:
            logger.error(f"Failed to apply {sql_file.name}: {e}",exc_info=True)
            failures += 1
        except Exception as e:
            logger.error(f"Unexpected error on {sql_file.name}: {e}",exc_info=True)
            failures += 1

    logger.info("\nSummary:")
    logger.info(f"  Successful: {success}")
    logger.info(f"  Failed:     {failures}")

    return 1 if failures > 0 else 0


if __name__ == "__main__":
    sys.exit(main())