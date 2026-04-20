DROP TABLE ciq_fssit.tprocess_ips_rule;

CREATE TABLE ciq_fssit.tprocess_ips_rule (
	event_type varchar(100) NOT NULL,
	event_sub_type varchar(100) NOT NULL,
	file_suffix varchar(20) NOT NULL,
	download bool DEFAULT true NULL,
	"convert" bool DEFAULT true NULL,
	content_extraction bool DEFAULT true NULL,
	similarity bool DEFAULT true NULL,
	sensitive_item_extraction bool DEFAULT true NULL,
	classification bool DEFAULT true NULL,
	redaction bool DEFAULT true NULL,
	upload bool DEFAULT true NULL,
	grading bool DEFAULT true NULL,
	CONSTRAINT tprocess_ips_rule_pkey PRIMARY KEY (event_type, event_sub_type, file_suffix)
);

INSERT INTO ciq_fssit.tprocess_ips_rule (event_type,event_sub_type,file_suffix,download,"convert",content_extraction,similarity,sensitive_item_extraction,classification,redaction,grading,upload) VALUES
	 ('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','UPLOAD_NEW_FILE','.pdf',true,true,true,true,true,true,true,true,true),
	 ('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','UPLOAD_NEW_FILE','others',true,false,true,true,true,true,true,true,true),
	 ('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','MANUAL_UPLOAD','.pdf',true,true,true,true,true,true,false,true,true),
	 ('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','','.pdf',true,true,true,true,true,true,true,true,true),
	 ('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','','others',true,false,true,true,true,true,true,true,true),
	 ('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','REPROCESS_DOCUMENT','.pdf',true,true,true,true,true,true,true,true,true),
	 ('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','REPROCESS_DOCUMENT','others',true,false,true,true,true,true,true,true,true),
	 ('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','MANUAL_UPLOAD','others',true,false,true,true,true,true,false,true,true);


insert into ciq_fssit.config 
("name", val, "comment", isactive)
select name,val,comment,True from 
(select 'VECTOR_CHUNK_SIZE' name,'5000' val,'chunk size used for vectorization' comment union all
select 'VECTOR_OVER_LAP_SIZE' name,'100' val,'overlap size used for vectorization' comment union all
select 'SCORE_WT' name,'0.90' val,'Weight used for similarity score in Deep search' comment union all
select 'COVERAGE_RATIO_WT' name,'0.10' val,'Weight used for percent matched chunks in Deep search' comment union all
select 'DEEP_SEARCH_SIMILARITY_THRESHOLD' name,'0.50' val,'Threshold for vector similarity in Deep search' comment union all
select 'DEEP_SEARCH_RELEVANCE_BUCKET' name,'0,25,60,80,95,100' val,'Bucket Boundaries for relevance score' comment union all
select 'DEEP_SEARCH_PUSH_SIZE' name,'10' val,'No of items sent in one responce' comment union all
select 'DEEP_SEARCH_LOGS_TABLE' name,'tdeep_search_logs' val,'No of items sent in one responce' comment union all
select 'DEEP_SEARCH_RELEVANCE_SCORE_THRESHOLD' name,'0.5' val,'threshold for deep search relevance score' comment union all
select 'FUZZ_RATIO_SIMILARITY_THRESHOLD' name,'70' val,'Threshold for Template Identification' comment union all
select 'GRADING_TABLE' name,'tgrading' val,'database objects' comment union all
select 'VECTOR_SEARCH_SIMILARITY_THRESHOLD' name,'0.7' val,'Threshold for Template Identification' comment union all
select 'SHORT_LABEL_VIEW' name,'vwtoberedacted' val,'database objects' comment union all
select 'CONSULTANT_FEEDBACK_TABLE' name,'tconsultantfeedback' val,'database objects' comment union all
select 'CHUNK_SIZE_SPELLCHECK' name,'15000' val,'gtl flow' comment union all
select 'CORR_ID_GRADING' name,'3f9c2d7a-6b81-4c5e-9a12-8d4e7f2b1c90' val,'Gen Ai' comment union all
select 'OVER_LAP_SIZE_SPELLCHECK' name,'100' val,'gtl flow' comment) sqry
where not exists(select 1 from ciq_fssit.config cfg where cfg.name=sqry.name);

ALTER TABLE ciq_fssit.tchange_document ALTER COLUMN newvalue TYPE text;
ALTER TABLE ciq_fssit.tchange_document ALTER COLUMN oldvalue TYPE text;

DO $$ 
BEGIN 
    ALTER TABLE ciq_fssit.tdocument RENAME COLUMN mathcingdafileid TO matchingdafileid;
EXCEPTION 
    WHEN undefined_column THEN 
        NULL; -- Effectively "does nothing"
    WHEN duplicate_column THEN 
        NULL;
END $$;


update ciq_fssit.config set isactive = false WHERE name='REFINE_RECOMMENDATION_VIEW';



UPDATE ciq_fssit.tdocument t SET dafileid = NULL WHERE t.daoriginal_fileid = t.dafileid;

DROP TABLE if exists ciq_fssit.tconsultantfeedback;

CREATE TABLE ciq_fssit.tconsultantfeedback (
	id serial4 NOT NULL,
	requestid uuid NOT NULL,
	dafileid uuid NULL,
	feedback varchar(50) NOT NULL,
	message varchar(255) NULL,
	usercomments text NULL,
	created_date timestamptz DEFAULT now() NULL,
	CONSTRAINT tconsultantfeedback_feedback_check CHECK (((feedback)::text = ANY ((ARRAY['positive'::character varying, 'negative'::character varying])::text[])))
);

CREATE OR REPLACE VIEW ciq_fssit.vwdocuments
AS SELECT sqry.requestid,
    sqry.fuuid,
    sqry.daoriginal_fileid,
    sqry.dafileid,
    sqry.ipid,
    sqry.projectid,
    sqry.filename,
    sqry.title,
    sqry.description,
    sqry.gtl_synopsis,
    sqry.uploadedby author,
    sqry.dtpm_phase,
    sqry.document_type,
    sqry.ip_type,
    sqry.practice,
    sqry.offerfamily,
    sqry.offer,
    sqry.doc_off_relevance_score,
    sqry.gtl_email,
    sqry.usage_count,
    sqry.status,
    reccnt.acceptance * 100::numeric AS acceptance,
    sqry.redacted_items_dafileid,
    sqry.waspdf,
    sqry.created_by,
    sqry.updated_by,
    sqry.created_date,
    sqry.updated_date,
    sqry.offersimilarity,
    sqry.type,
    sqry.url,
    sqry.mathcingdafileid,
    sqry.similarity
   FROM ( SELECT doc.requestid,
            doc.fuuid,
            doc.daoriginal_fileid,
            COALESCE(doc.dafileid, doc.daoriginal_fileid) AS dafileid,
            doc.ipid,
            doc.projectid,
            doc.filename,
            doc.title,
            doc.description,
            doc.gtl_synopsis,
            doc.author,
            COALESCE(mp.dtpm_phase, doc.dtpm_phase) AS dtpm_phase,
            doc.document_type,
            doc.ip_type,
            ofd.practice,
            ofd.offerfamily,
            COALESCE(ofd.offer, doc.offer::text) AS offer,
            doc.relevance_score AS doc_off_relevance_score,
            ofd.gtl AS gtl_email,
            doc.usage_count,
            doc.status,
            doc.redacted_items_dafileid,
            doc.waspdf,
            doc.type,
            doc.url,
            doc.uploadedby,
            doc.created_by,
            doc.updated_by,
            to_char(doc.created_date, 'YYYY-MM-DD HH24:MI:SS'::text) AS created_date,
            to_char(doc.updated_date, 'YYYY-MM-DD HH24:MI:SS'::text) AS updated_date,
            similarity(doc.offer::text, ofd.offer) AS offersimilarity,
            doc.similarity,
            doc.mathcingdafileid,
            row_number() OVER (PARTITION BY doc.requestid, doc.daoriginal_fileid, doc.offer ORDER BY (similarity(doc.offer::text, ofd.offer)) DESC) AS rnk
           FROM ciq_fssit.tdocument doc
             JOIN ( SELECT ofr.practice,
                    ofr.offerfamily,
                    ofr.offer,
                    ofr.gtl
                   FROM ciq_fssit.vwofferfamilydata ofr
                UNION ALL
                 SELECT 'UnDefined'::text AS text,
                    'UnDefined'::text AS text,
                    'UnDefined'::text AS text,
                    NULL::character varying AS gtl) ofd ON similarity(COALESCE(doc.offer, 'UnDefined'::character varying)::text, ofd.offer) > 0.75::double precision
             LEFT JOIN ciq_fssit.tdtpm_mapping mp ON TRIM(BOTH FROM lower(doc.ip_type::text)) = TRIM(BOTH FROM lower(mp.ip_type::text))) sqry
     LEFT JOIN ( SELECT rec.templateid,
            NULLIF(sum(
                CASE
                    WHEN rec.status::text = 'ACCEPTED'::text THEN 1.0
                    ELSE 0.0::integer::numeric
                END), 0.0) / count(1)::numeric AS acceptance
           FROM ciq_fssit.trecommendation rec
          WHERE rec.status::text <> 'GENERATED'::text AND rec.method::text = 'offerbased'::text
          GROUP BY rec.templateid) reccnt ON reccnt.templateid = COALESCE(sqry.dafileid, sqry.daoriginal_fileid)
  WHERE sqry.rnk = 1;



DROP VIEW IF EXISTS ciq_fssit.vwclassificationout;
DROP TABLE IF EXISTS ciq_fssit.tgrading;

CREATE TABLE ciq_fssit.tgrading (
	requestid uuid NOT NULL,
	fuuid uuid NOT NULL,
	dafileid uuid NOT NULL,
	spelling_accuracy float8 NULL,
	grammar_accuracy float8 NULL,
	coherence float8 NULL,
	clarity float8 NULL,
	relevance float8 NULL,
	coverage float8 NULL,
	redundancy float8 NULL,
	grade_score float8 NULL,
	grade varchar(50) NULL,
	summary text NULL,
	CONSTRAINT tgrading_pk PRIMARY KEY (requestid, fuuid, dafileid)
);


CREATE OR REPLACE VIEW ciq_fssit.vwclassificationout
AS SELECT requestid,
    fuuid,
    daoriginal_fileid,
    dafileid,
    COALESCE(filename, ''::character varying) AS filename,
    COALESCE(title, ''::character varying) AS title,
    COALESCE(description, ''::text) AS description,
    COALESCE(gtl_synopsis, ''::text) AS gtl_synopsis,
    COALESCE(author, ''::character varying) AS author,
    COALESCE(dtpm_phase, ''::character varying) AS dtpm_phase,
    COALESCE(document_type, ''::character varying) AS document_type,
    ip_type,
    COALESCE(practice, ''::text) AS practice,
    COALESCE(offerfamily, ''::text) AS offerfamily,
    COALESCE(offer, ''::text) AS offer,
    COALESCE(gtl_email, ''::character varying) AS gtl_email,
    COALESCE(usage_count, 0) AS usage_count,
    COALESCE(status, ''::character varying) AS status,
    redacted_items_dafileid AS dasanitizationoutfileid,
    COALESCE(waspdf, false) AS waspdf,
    COALESCE(type, ''::character varying) AS type,
    COALESCE(url, ''::character varying) AS url,
    COALESCE(grade_score, 0.0::double precision) AS gradescore,
    COALESCE(grade, ''::character varying) AS initialgrade,
    COALESCE(similarity, 0.0::double precision) AS similarityscore,
        CASE
            WHEN grade::text = 'Very High'::text AND similaritybucket = 'Low'::text THEN 'P1'::text
            WHEN grade::text = 'Very High'::text AND similaritybucket = 'Medium'::text THEN 'P2'::text
            WHEN grade::text = 'Very High'::text AND similaritybucket = 'High'::text THEN 'P3'::text
            WHEN grade::text = 'High'::text AND similaritybucket = 'Low'::text THEN 'P4'::text
            WHEN grade::text = 'High'::text AND similaritybucket = 'Medium'::text THEN 'P5'::text
            WHEN grade::text = 'High'::text AND similaritybucket = 'High'::text THEN 'P6'::text
            WHEN grade::text = 'Medium'::text AND similaritybucket = 'Low'::text THEN 'P7'::text
            WHEN grade::text = 'Medium'::text AND similaritybucket = 'Medium'::text THEN 'P8'::text
            WHEN grade::text = 'Medium'::text AND similaritybucket = 'High'::text THEN 'P9'::text
            WHEN grade::text = 'Low'::text AND similaritybucket = 'Low'::text THEN 'P10'::text
            WHEN grade::text = 'Low'::text AND similaritybucket = 'Medium'::text THEN 'P11'::text
            WHEN grade::text = 'Low'::text AND similaritybucket = 'High'::text THEN 'P12'::text
            WHEN grade::text = 'Very Low'::text AND similaritybucket = 'Low'::text THEN 'P13'::text
            WHEN grade::text = 'Very Low'::text AND similaritybucket = 'Medium'::text THEN 'P14'::text
            WHEN grade::text = 'Very Low'::text AND similaritybucket = 'High'::text THEN 'P15'::text
            ELSE ''::text
        END AS priority
   FROM ( SELECT doc.requestid,
            doc.fuuid,
            doc.daoriginal_fileid,
            doc.dafileid,
            doc.filename,
            doc.title,
            doc.description,
            concat(doc.gtl_synopsis, '<br>', gr.summary) AS gtl_synopsis,
            doc.author,
            doc.dtpm_phase,
            doc.document_type,
            string_to_array(doc.ip_type::text, '|'::text) AS ip_type,
            doc.practice,
            doc.offerfamily,
            doc.offer,
            doc.gtl_email,
            doc.usage_count,
            doc.status,
            doc.redacted_items_dafileid,
            doc.waspdf,
            doc.type,
            doc.url,
            doc.similarity,
            gr.grade,
            gr.grade_score,
                CASE
                    WHEN doc.similarity > 75.0::double precision THEN 'High'::text
                    WHEN doc.similarity > 40.0::double precision THEN 'Medium'::text
                    ELSE 'Low'::text
                END AS similaritybucket
           FROM ciq_fssit.vwdocuments doc
             LEFT JOIN ciq_fssit.tgrading gr ON gr.requestid = doc.requestid AND gr.fuuid = doc.fuuid AND gr.dafileid = COALESCE(doc.dafileid, doc.daoriginal_fileid)) sqry;



truncate table ciq_fssit.tlabel_lookup;

-- Insert the updated values
INSERT INTO ciq_fssit.tlabel_lookup(category, "label") VALUES
('vendor organisation name', 'Vendor'),
('source code snippets', 'Snippet'),
('vendor person name', 'Vendor'),
('http_url', 'URL'),
('driver license number or dl', 'DL'),
('internal project name', 'Internal'),
('network device name', 'Device'),
('port number', 'Port'),
('linux/windows command', 'Cmd'),
('internal_email', '@mail'),
('api key/authentication token', 'Token'),
('domain name', 'Domain'),
('ssh credential', 'SSH Creds'),
('passphrase', 'Passwd'),
('email_address', '@mail'),
('active directory site', 'AD Site'),
('password policy', 'Policy'),
('customer email addresses', '@mail'),
('ssl/tls certificate', 'Certi'),
('internal person name', 'Internal'),
('internal email address', '@mail'),
('application name', 'App'),
('auth_token', 'Token'),
('ip_address', 'ip add'),
('email address', '@mail'),
('authentication token', 'Token'),
('active directory domain name', 'AD Dom'),
('active directory forest name', 'AD Forest'),
('vendor project name', 'Vendor'),
('dell internal person name', 'Internal'),
('customer person name', 'Cust'),
('customer email address', 'Cust'),
('customer organisation name', 'Cust'),
('incident number', 'Inc No'),
('service request number', 'SR No'),
('authentication credential', 'Creds'),
('service tag number', 'Tag No'),
('phone numbers', 'Cont'),
('customer team name', 'Cust'),
('Computer Name', 'Device'),
('Customer Solution Center Team', 'Cust'),
('Customer Specific Absolute URL', 'URL'),
('Dell Internal Absolute URL', 'URL'),
('Dell Internal Project Name', 'Internal'),
('Dell Team Name', 'Internal'),
('Group Name', 'grp'),
('Internal Server Name', 'server'),
('Mailing Address', '@mail'),
('PRODUCTION VLAN ID', 'VLAN'),
('SSL SUBNET', 'Subnet'),
('Serial Number', 'Serial No'),
('Server Name', 'Server'),
('Site Name', 'Site'),
('Storage Device Name', 'Device'),
('Syslog destination server', 'Server'),
('Vendor Team Name', 'Vendor'),
('Windows File Share Name', 'URI'),
('Tag Number', 'Tag No');





drop table if exists ciq_fssit.tfeedback_backup;
select * into ciq_fssit.tfeedback_backup from ciq_fssit.tfeedback;
 
DROP TABLE if exists ciq_fssit.tfeedback;
 
CREATE TABLE ciq_fssit.tfeedback (
	id serial4 NOT NULL,
	fuuid uuid NOT NULL,
	dafileid uuid NOT NULL,
	filename varchar(255) NULL,
	status varchar(50) NULL,
	feedback text NULL,
	created_by varchar(225) NULL,
	created_date timestamptz DEFAULT now() NOT NULL,
	"action" varchar(50) NULL,
	CONSTRAINT tfeedback_pkey PRIMARY KEY (id)
);
 
insert into ciq_fssit.tfeedback (fuuid,dafileid,filename,status,feedback,created_by,created_date,"action")
select fuuid,dafileid,filename,status,feedback,created_by,created_date,"action" from ciq_fssit.tfeedback_backup;