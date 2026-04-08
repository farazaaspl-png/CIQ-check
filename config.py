import os
from dotenv import load_dotenv,dotenv_values
from sqlalchemy import create_engine, text

changed_configs ={
'KAFKA_INPUT_TOPICS':'INPUT_TOPICS',
'KAFKA_OUTPUT_TOPIC':'OUTPUT_TOPIC',
'KAFKA_AUTO_OFFSET_RESET':'AUTO_OFFSET_RESET',
'KAFKA_ENABLE_AUTO_COMMIT':'ENABLE_AUTO_COMMIT',
'AN_EVENT':'AN_EVENT_TABLE',
'STATEMENTOFWORK':'STATEMENTOFWORK_TABLE',
'DOCUMENT':'DOCUMENT_TABLE',
'RECOMMENDATION':'RECOMMENDATION_TABLE',
'FEEDBACK':'FEEDBACK_TABLE',
'EXTRACTION':'EXTRACTION_TABLE',
'TOBEREDACTED':'TOBEREDACTED_TABLE',
'REDACTED':'REDACTED_TABLE',
'APICALL_LOGS':'APICALL_LOGS_TABLE',
'DAAPICALL_LOGS':'DA_APICALL_LOGS_TABLE',
'CHANGE_DOCUMENT':'CHANGE_DOCUMENT_TABLE',
'VWCLASSIFICATIONOUT':'CLASSIFICATIONOUT_VIEW',
'VWDOCUMENTS':'DOCUMENTS_VIEW',
'VWOFFERFAMILYDATA':'OFFERFAMILYDATA_VIEW',
'VWDTPMMAPPING':'DTPMMAPPING_VIEW',
'VWRECOMMENDATIONS':'RECOMMENDATIONS_VIEW',
'VWGETRECOMMENDATIONS':'GETRECOMMENDATIONS_VIEW',
'VWSTATEMENTOFWORK':'STATEMENTOFWORK_VIEW',
'FNGENERATERECOMMENDATION':'GENERATE_RECOMMENDATION_FUNCTION',
'COLLECTIONNAME':'COLLECTIONNAME',
'LLMRAWRESPONSE':'LLM_RAW_RESPONSE_TABLE'
}

def convert_variable(var):
    try:
        return int(var)
    except ValueError:
        try:
            return float(var)
        except ValueError:
            if var.lower() == 'true':
                return True
            elif var.lower() == 'false':
                return False
            else:
                return var
            
class Configuration:
    load_dotenv()
    def __init__(self):
        for key,value in dotenv_values().items():
            if key in ['KAFKA_BOOTSTRAP_SERVERS_INPUT','KAFKA_BOOTSTRAP_SERVERS_OUTPUT','KAFKA_INPUT_TOPICS']:
                setattr(Configuration, changed_configs.get(key,key), value.split(','))
            else:
                setattr(Configuration, changed_configs.get(key,key), convert_variable(value))
    
    def load_active_config(self) -> dict:
        """
        Execute:
            SELECT name, val FROM ciq_fssit.config WHERE isactive;

        * Returns a dict ``{name: val}``.
        * For every ``name`` that is **not already** an attribute of the
          `[Configuration](cci:2://file:///c:/Users/Lekhnath_Pandey/CIQ/ip_content_management/config.py:45:0-52:93)` class, creates a class attribute with that value.
        """
        # -----------------------------------------------------------------
        # 1️⃣  Obtain the DB connection string.
        # -----------------------------------------------------------------
        db_url = getattr(self, "DATABASE_CONNECTION_STR", None)
        db_schema = getattr(self, "DATABASE_SCHEMA", None)
        if not db_url:
            raise RuntimeError(
                "DATABASE_CONNECTION_STR not set – cannot query the database"
            )

        # -----------------------------------------------------------------
        # 2️⃣  Create a temporary SQLAlchemy engine.
        # -----------------------------------------------------------------
        engine = create_engine(db_url)

        # -----------------------------------------------------------------
        # 3️⃣  Run the query.
        # -----------------------------------------------------------------
        sql = text(f"""SELECT name, val FROM {db_schema}.config WHERE isactive""")

        with engine.connect() as conn:
            result = conn.execute(sql)
            # Build a dict from the result set
            active_cfg = {row[0]: row[1] for row in result.fetchall()}

        # -----------------------------------------------------------------
        # 4️⃣  Set each ``name`` as a class attribute **if not already present**.
        # -----------------------------------------------------------------
        for key, value in active_cfg.items():
            # if not hasattr(Configuration, key):
            if key in ['KAFKA_BOOTSTRAP_SERVERS_INPUT','KAFKA_BOOTSTRAP_SERVERS_OUTPUT','INPUT_TOPICS']:
                setattr(Configuration, changed_configs.get(key,key), value.split(','))
            else:
                setattr(Configuration, changed_configs.get(key,key), convert_variable(value))
            # setattr(Configuration, key, convert_variable(value))

        # -----------------------------------------------------------------
        # 5️⃣  Return the dict for callers that need it.
        # -----------------------------------------------------------------
        # return active_cfg

Config = Configuration()
Config.load_active_config()

    
# Load variables from .env into environment




# class Configuration:
#     """Service-specific configuration"""
    
#     def __init__(self):
#         load_dotenv()  
#         # for key, value in os.environ.items():
#         #     print(f"{key}={value}")
#         # Kafka settings
#         self.KAFKA_BOOTSTRAP_SERVERS_INPUT = os.getenv('KAFKA_BOOTSTRAP_SERVERS_INPUT', 'localhost:9092').split(',')
#         self.KAFKA_BOOTSTRAP_SERVERS_OUTPUT = os.getenv('KAFKA_BOOTSTRAP_SERVERS_OUTPUT', 'localhost:9092').split(',')
#         self.KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'secure-test-group')
#         self.INPUT_TOPICS = os.getenv('KAFKA_INPUT_TOPICS', 'SDS.EDP.CONSLTIQ.EVENTS.GE2').split(',')
#         self.OUTPUT_TOPIC = os.getenv('KAFKA_OUTPUT_TOPIC', 'CSG.TAA.SERVICES.AI.DEV.DEV')
#         self.SECURITY_PROTOCOL=os.getenv('SECURITY_PROTOCOL', 'SASL_SSL')       # or SASL_PLAINTEXT / SSL depending on cluster
#         self.SASL_MECHANISM=os.getenv('SASL_MECHANISM', 'PLAIN')             # or SCRAM-SHA-256 / SCRAM-SHA-512
#         self.SASL_PLAIN_USERNAME=os.getenv('SASL_PLAIN_USERNAME', 'svc_prdgpe')
#         self.SASL_PLAIN_PASSWORD=os.getenv('SASL_PLAIN_PASSWORD', '')
        
#         # Consumer settings
#         self.AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
#         self.ENABLE_AUTO_COMMIT = os.getenv('KAFKA_ENABLE_AUTO_COMMIT', 'true').lower() == 'true'
        
#         # Service settings
#         self.SERVICE_NAME = os.getenv('SERVICE_NAME', 'summerization_service')
#         self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
#         self.DISABLE_SENDING_MESSAGE = os.getenv('DISABLE_SENDING_MESSAGE', 'false').lower() == 'true'
        
#         # Health check
#         self.HEALTH_CHECK_PORT = int(os.getenv('HEALTH_CHECK_PORT', '8080'))

#         # USE_SSO='true'
#         self.USE_SSO=os.getenv("USE_SSO") 
#         #Below environment variables apply to OAuth, By default the token refersh happens on the client side , if you want to overrirde and set the token refresh to be done on server side set ENABLE_TOKEN_REFRESH_AT_SERVER_SIDE to true#
#         self.CLIENT_ID=os.getenv("CLIENT_ID") 
#         self.CLIENT_SECRET=os.getenv("CLIENT_SECRET") 
#         self.ENABLE_TOKEN_REFRESH_AT_SERVER_SIDE=os.getenv("ENABLE_TOKEN_REFRESH_AT_SERVER_SIDE") 
#         self.GEN_AI_API_LINK=os.getenv("GEN_AI_API_LINK")

#         self.INDIR=os.getenv("INDIR") 
#         self.OUTDIR=os.getenv("OUTDIR") 

#         self.PREFIX=os.getenv("PREFIX") 
#         self.REGEX=os.getenv("REGEX") 

#         #DELL ATTACHMENTS
#         self.DELL_ATTACHMENTS_SERVICE_TYPE=os.getenv("DELL_ATTACHMENTS_SERVICE_TYPE")                                               
#         self.DELL_ATTACHMENTS_URL=os.getenv("DELL_ATTACHMENTS_URL")
#         self.DELL_ATTACHMENTS=os.getenv("DELL_ATTACHMENTS")

#         self.DELL_ATTACHMENTS_AUTH_URL=os.getenv("DELL_ATTACHMENTS_AUTH_URL")
#         self.DELL_ATTACHMENTS_CLIENT_ID=os.getenv("DELL_ATTACHMENTS_CLIENT_ID")
#         self.DELL_ATTACHMENTS_CLIENT_SECRET=os.getenv("DELL_ATTACHMENTS_CLIENT_SECRET")
#         self.DELL_ATTACHMENTS_USER_ID=os.getenv("DELL_ATTACHMENTS_USER_ID")

#         self.DELL_ATTACHMENTS_INITIATE=os.getenv("DELL_ATTACHMENTS_INITIATE")
#         self.DELL_ATTACHMENTS_STREAM=os.getenv("DELL_ATTACHMENTS_STREAM")
#         self.DELL_ATTACHMENTS_COMPLETE=os.getenv("DELL_ATTACHMENTS_COMPLETE")
#         self.DELL_ATTACHMENTS_STATUS=os.getenv("DELL_ATTACHMENTS_STATUS")
#         self.DELL_ATTACHMENTS_DOWNLOAD=os.getenv("DELL_ATTACHMENTS_DOWNLOAD")
#         self.DELL_ATTACHMENTS_LISTFILES=os.getenv("DELL_ATTACHMENTS_LISTFILES")

#         #Correlation Ids
#         self.CORR_ID_CLASSIFICATION=os.getenv("CORR_ID_CLASSIFICATION")
#         self.CORR_ID_REDACTION=os.getenv("CORR_ID_REDACTION")
#         self.CORR_ID_SUMMARIZATION=os.getenv("CORR_ID_SUMMARIZATION")
#         self.CORR_ID_EMBEDDINGS=os.getenv("CORR_ID_EMBEDDINGS")
#         self.CORR_ID_DELIVERY_KIT_SUMMARIZATION=os.getenv("CORR_ID_DELIVERY_KIT_SUMMARIZATION")

#         self.THRESHOLD_SOW_OFFER_SCORE=os.getenv("THRESHOLD_SOW_OFFER_SCORE")     #Confidence Score
#         self.THRESHOLD_DOC_OFFER_SCORE=os.getenv("THRESHOLD_DOC_OFFER_SCORE") # Doc Confidence Score
#         self.VECTOR_SEARCH_DOCS_CNT=os.getenv("VECTOR_SEARCH_DOCS_CNT")  # Refine Number of Docs

#         # Database Objects
#         self.VECTOR_DB_CONNECTION_STR=os.getenv("VECTOR_DB_CONNECTION_STR")
#         self.DATABASE_CONNECTION_STR=os.getenv("DATABASE_CONNECTION_STR")
#         self.DATABASE_SCHEMA=os.getenv("DATABASE_SCHEMA")
#         self.AN_EVENT_TABLE=os.getenv("AN_EVENT")
#         self.STATEMENTOFWORK_TABLE=os.getenv("STATEMENTOFWORK")
#         self.DOCUMENT_TABLE=os.getenv("DOCUMENT")
#         self.RECOMMENDATION_TABLE=os.getenv("RECOMMENDATION")
#         self.FEEDBACK_TABLE=os.getenv("FEEDBACK")
#         self.EXTRACTION_TABLE=os.getenv("EXTRACTION")
#         self.TOBEREDACTED_TABLE=os.getenv("TOBEREDACTED")
#         self.REDACTED_TABLE=os.getenv("REDACTED")
#         self.APICALL_LOGS_TABLE=os.getenv("APICALL_LOGS")
#         self.CHANGE_DOCUMENT_TABLE=os.getenv("CHANGE_DOCUMENT")
#         self.CLASSIFICATIONOUT_VIEW=os.getenv("VWCLASSIFICATIONOUT")
#         self.DOCUMENTS_VIEW=os.getenv("VWDOCUMENTS")
#         self.OFFERFAMILYDATA_VIEW=os.getenv("VWOFFERFAMILYDATA")
#         self.RECOMMENDATIONS_VIEW=os.getenv("VWRECOMMENDATIONS")
#         self.GETRECOMMENDATIONS_VIEW=os.getenv("VWGETRECOMMENDATIONS")
#         self.STATEMENTOFWORK_VIEW=os.getenv("VWSTATEMENTOFWORK")
#         self.GENERATE_RECOMMENDATION_FUNCTION=os.getenv("FNGENERATERECOMMENDATION")
#         self.COLLECTIONNAME=os.getenv("COLLECTIONNAME")
#         self.DTPM_MAPPINGTABLE=os.getenv("DTPMMAPPING")
#         self.LLM_RAW_RESPONSE_TABLE=os.getenv("LLMRAWRESPONSE")

# Config = Configuration()