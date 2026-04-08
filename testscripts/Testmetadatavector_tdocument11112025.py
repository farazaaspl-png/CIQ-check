# import logging
# import pandas as pd
# from pathlib import Path
# import configparser
# from sqlalchemy import create_engine, text

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# HARDCODED_REQUEST_ID = "00000000-0000-0000-0000-000000000001"
# TEST_TABLE = "tdocument_11112025"

# def get_engine():
#     connection_str = config.get('database', 'connectionstr')
#     return create_engine(connection_str)

# def get_openai_client():
#     import core.genai.dell_helper as dh
#     from openai import OpenAI
    
#     dh.update_certifi()
#     correlation_id = '66e4182a-66cf-48cb-84dd-ba7e96ae002b'
#     default_headers = dh.get_default_headers_based_on_authentication(correlation_id)
#     http_client = dh.get_http_client_based_on_authentication()
#     base_url = 'https://aia.gateway.dell.com/genai/dev/v1'
    
#     return OpenAI(
#         base_url=base_url,
#         http_client=http_client,
#         api_key='',
#         default_headers=default_headers
#     )

# def add_metadatavector_column():
#     engine = get_engine()
#     try:
#         with engine.connect() as conn:
#             conn.execute(text(f"""
#                 ALTER TABLE {TEST_TABLE} 
#                 ADD COLUMN IF NOT EXISTS metadatavector vector(768)
#             """))
#             conn.commit()
#             logger.info(f"Added metadatavector column to {TEST_TABLE}")
#     except Exception as e:
#         logger.error(f"Failed to add column: {e}")
#     finally:
#         engine.dispose()

# def generate_metadata_text(row: dict) -> str:
#     metadata = f'The document is named "{row["filename"]}".\n'
#     metadata += f'This is {row["ip_type"]} type of document required at {row["dtpm_phase"]} phase of a project.\n'
    
#     if pd.notna(row.get("offer")):
#         metadata += f'It should be used for "{row["offer"]}" offer provided to customers.\n'
        
#         if pd.notna(row.get("offerfamily")) and pd.notna(row.get("practice")):
#             metadata += f'"{row["offer"]}" belongs to offer family "{row["offerfamily"]}"'
#             metadata += f' and "{row["practice"]}" practice.\n'
    
#     return metadata

# def get_approved_documents():
#     engine = get_engine()
#     try:
#         with engine.connect() as conn:
#             query = text(f"""
#                 SELECT v.requestid, v.dafileid, v.filename, v.ip_type, v.dtpm_phase, 
#                        v.offer, v.offerfamily, v.practice, t.daoriginal_fileid
#                 FROM vwdocuments v
#                 JOIN {TEST_TABLE} t ON v.requestid = t.requestid AND v.dafileid = t.dafileid
#                 WHERE v.requestid = :requestid AND v.status = :status
#             """)
#             df = pd.read_sql(query, conn, params={
#                 'requestid': HARDCODED_REQUEST_ID,
#                 'status': 'APPROVED'
#             })
#             logger.info(f"Found {len(df)} approved documents to vectorize")
#             return df
#     finally:
#         engine.dispose()

# def vectorize_and_update():
#     logger.info(f"Starting vectorization process for {TEST_TABLE}")
    
#     df = get_approved_documents()
    
#     if df.empty:
#         logger.warning("No approved documents found to vectorize")
#         return
    
#     openai_client = get_openai_client()
#     engine = get_engine()
    
#     try:
#         for idx, row in df.iterrows():
#             try:
#                 metadata_text = generate_metadata_text(row)
                
#                 embedding_response = openai_client.embeddings.create(
#                     input=metadata_text,
#                     model="nomic-embed-text-v1-5"
#                 )
#                 vector = embedding_response.data[0].embedding
                
#                 with engine.connect() as conn:
#                     update_query = text(f"""
#                         UPDATE {TEST_TABLE}
#                         SET metadatavector = :vector
#                         WHERE requestid = :requestid AND daoriginal_fileid = :daoriginal_fileid
#                     """)
#                     conn.execute(update_query, {
#                         'vector': str(vector),
#                         'requestid': str(row['requestid']),
#                         'daoriginal_fileid': str(row['daoriginal_fileid'])
#                     })
#                     conn.commit()
                
#                 logger.info(f"Vectorized and updated document {idx + 1}/{len(df)}: {row['filename']}")
                
#             except Exception as e:
#                 logger.error(f"Failed to process document {row.get('filename', 'unknown')}: {e}")
#                 continue
        
#         logger.info(f"Successfully vectorized {len(df)} documents in {TEST_TABLE}")
        
#     finally:
#         engine.dispose()

# if __name__ == "__main__":
#     try:
#         add_metadatavector_column()
#         vectorize_and_update()
#     except Exception as e:
#         logger.error(f"Vectorization failed: {e}", exc_info=True)