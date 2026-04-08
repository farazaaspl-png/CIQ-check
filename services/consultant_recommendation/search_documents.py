# import asyncio
# import uuid, os, pandas as pd
# import logging
# # from pathlib import Path
# from sqlalchemy import Float, String, bindparam, text as querytext, Text
# from sqlalchemy.dialects import postgresql

# # from core.dellattachments import DellAttachments
# from core.db.crud import DatabaseManager
# from core.exceptions import UnableToFindAnyRecommendation
# # from core.exceptions import DatabaseWriteError, DellAttachmentsApiError, DellAttachmentsDownloadError, DellAttachmentsUploadError, UnableToFindAnyRecommendation
# from core.embedding.vectorizer import VectorInterface
# # from config import Config as cfg
# from config import Configuration
# from core.utility import get_custom_logger

# # logging.getLogger().setLevel(logging.INFO)
# logger = get_custom_logger(__name__)
# # logger.propagate = False
# semaphore = asyncio.Semaphore(os.cpu_count())



# async def generate_search_results(debug:bool=False, **kwargs):
#     # 
#     request_id = kwargs.get("requestId", str(uuid.uuid4()))
#     project_id = kwargs.get("projectId")
#     searchquery = kwargs.get("refineText")
#     # phase = kwargs.get("phases",[])
#     # vincludeSkipped = kwargs.get("includeSkipped",False)
#     cfg=kwargs.get("config",None)
#     # vincludeSkipped = True if vincludeSkipped == "TRUE" else False

#     # phase.append('na')
#     # phase = [x.lower() for x in phase]
#     logger.info(f"{request_id} - Started Looking for documents with key word {searchquery}")

    

#     db=DatabaseManager(cfg.DEBUG)
#     with db.engine.connect() as conn:
#         db.delete_recommendation(projectid=project_id, status="GENERATED")

#     if not cfg.VECTOR_SEARCH_SWITCH:
#         filterstatus_list = ['ACCEPTED', 'SENT', 'SKIPPED']
#         # if vincludeSkipped:
#         #     filterstatus_list = ['ACCEPTED', 'SENT']

#         #string_to_array(dtpm_phase, '|') && :phase
#         with db.engine.connect() as conn:
#             query = querytext(f"""SELECT grr.dafileid
#                                         ,grr.ipid
#                                         ,similarity(grr.metadata,:searchquery) similarityscore
#                                     FROM {cfg.DATABASE_SCHEMA}.{cfg.REFINE_RECOMMENDATION_VIEW} grr
#                                     WHERE grr.dafileid not in (SELECT rec.templateid  
#                                                                  FROM {cfg.DATABASE_SCHEMA}.{cfg.RECOMMENDATION_TABLE} rec
#                                                                 WHERE rec.projectid = :project_id
#                                                                   AND rec.status in :statusfilter)
#                                     AND similarity(grr.metadata,:searchquery) > :threshold
#                                  ORDER BY similarityscore DESC
#                                     LIMIT {cfg.DOCS_SEARCH_CNT}
#                                      """).bindparams(
#                                                     # bindparam("statusfilter", expanding=True),
#                                                     bindparam("searchquery", type_=String),
#                                                     bindparam("project_id", type_=String),          # change to postgresql.UUID if you store UUIDs
#                                                     bindparam("statusfilter", expanding=True, type_=String),
#                                                     # bindparam("phase", type_=postgresql.ARRAY(Text)),
#                                                     bindparam("threshold", type_=Float),
#                                                     # phase=postgresql.ARRAY(str)
#                                                     )
#             logger.info(f"{request_id} - Executing database function with request_id: {request_id}")
#             params={'searchquery': searchquery.lower(),
#                     'project_id': project_id, 
#                     'statusfilter': filterstatus_list,
#                     # 'phase':phase,
#                     'threshold':cfg.SEARCH_SIMILARITY_THRESHOLD}
#             compiled = query.params(**params).compile(
#                 dialect=conn.dialect,               # use the same PostgreSQL dialect as the live connection
#                 compile_kwargs={"literal_binds": True}
#             )
#             final_sql = str(compiled)
#             logger.debug(f"{request_id} - Executing query: {final_sql}")
#             searchdf = pd.read_sql(query, conn, params=params)
#             # conn.commit()
#     else:
    
#         vec = VectorInterface(table_name=cfg.DOCUMENT_METASTORE_NAME)
#         searchdf = vec.search_documents(searchquery,
#                                         threshold=cfg.SEARCH_SIMILARITY_THRESHOLD,
#                                         no_of_docs=cfg.DOCS_SEARCH_CNT,
#                                         # dtpm_phase=phase,
#                                         projectid=project_id
#                                         # includeSkipped=vincludeSkipped
#                                         )
        
#     logger.info(f"{request_id} - Retrieved {searchdf.shape[0]} rows")
#     if searchdf.shape[0]==0:
#         logger.warning(f"{request_id} - No recommendations found for requestId: {request_id}")
#         raise UnableToFindAnyRecommendation()
    
#     # searchdf.rename(columns={'projectid': 'FF_ProjectId','dafileid':'templateid'}, inplace=True)
#     searched_docs = searchdf.to_dict(orient='records')
#     logger.debug(f"{request_id} - Rec dictionary searched_docs {searched_docs}")
#     # phase.remove('na')
#     rec_rows =[{
#                 "requestid": request_id,   
#                 "projectid": project_id,
#                 "templateid": doc["dafileid"],
#                 "ipid": doc["ipid"],
#                 "userquery": searchquery,
#                 # "phase": '|'.join(phase),
#                 "similarityscore":float(doc.get("similarityscore",doc.get("score")))*100.0,
#                 "method": "similarity_search",
#                 }
#                 for doc in searched_docs]
    
    
#     for rec in rec_rows:
#         if not rec["ipid"]:
#             rec.pop("ipid")
    
#     # rec_row = {
#     #             "requestid": request_id,   
#     #             "projectid": project_id,
#     #             "templateid": doc["dafileid"],
#     #             # "ipid": doc["ipid"],
#     #             # "dafileid": out_file_id,
#     #             "userquery": searchquery,
#     #             "phase": '|'.join(phase),
#     #             "method": "vector_search",
#     #         }
#     # if doc["ipid"]:
#     #         rec_row["ipid"] = doc["ipid"]
#     # print(rec_rows)
    
#     # db.insert_recommendation(**rec_row)
#     db.insert_bulk_recommendation(rec_rows)

#     # for rows in rec_rows:
#     #     db.insert_recommendation(**rows)
        
#     # tasks = [asyncio.create_task(reupload_to_dellattachments(request_id, project_id,searchquery, phase, doc)) for doc in searched_docs]
#     # results = await asyncio.gather(*tasks, return_exceptions=True)
#     # logger.info(f"{request_id} - {sum(results)} files uploaded to dell attachments")
#     # if sum(results) == 0:
#     #     raise DellAttachmentsUploadError()  
    
#     with db.engine.connect() as conn:
#         query = querytext(f"""SELECT * FROM {cfg.DATABASE_SCHEMA}.{cfg.GENERATE_RECOMMENDATION_FUNCTION}(:request_id,True)""")
#         logger.info(f"{request_id} - Executing database function with request_id: {request_id}")
#         recdf = pd.read_sql(query, conn, params={'request_id': request_id})
#         conn.commit()
#     logger.info(f"{request_id} -  {recdf.shape[0]} recommendations from database function")

#     if recdf.empty:
#         logger.warning(f"{request_id} - No recommendations found for requestId: {request_id}")
#         raise UnableToFindAnyRecommendation()
    
#     recdf=recdf.rename(columns={'projectid':'FF_ProjectId','dafileid':'daFileId'})

#     reccommendations = recdf.to_dict(orient='records')
#     # if 'db' in locals(): del db
    
#     return reccommendations

# def remove_keys(pdict: dict, keylist: list = [ 'createdBy', 'updatedBy', 'createdOn', 'updatedOn']):
#     # removeKeys = [ 'createdBy', 'updatedBy', 'createdOn', 'updatedOn']
#     for key in keylist:    
#         if key in pdict: pdict.pop(key, None) 

# async def search_documents(header,payload,debug):
#     remove_keys(payload)
#     remove_keys(header)
#     if debug:
#         logger.setLevel(logging.DEBUG)
#     cfg = Configuration()
#     cfg.load_active_config()
#     # projectId = uuid.UUID(header.pop('projectId'))
#     # payload = await generate_search_results(projectId,**payload,**header)
#     payload = await generate_search_results(debug=cfg.DEBUG,**payload,**header,config=cfg) 
   
#     return payload