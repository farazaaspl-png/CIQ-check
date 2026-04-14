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
	 ('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','MANUAL_UPLOAD','.pdf',true,true,true,true,true,false,true,true,true),
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

update ciq_fssit.config set isactive = false WHERE name='REFINE_RECOMMENDATION_VIEW';

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

UPDATE ciq_fssit.tdocument t SET dafileid = NULL WHERE t.daoriginal_fileid = t.dafileid;

drop view if exists ciq_fssit.vwclassificationout;

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
    COALESCE(grade, ''::character varying) AS grade,
    COALESCE(similarity, 0.0::double precision) AS similarty,
    (case when grade = 'Very High' and similaritybucket='Low' 	 then 'P1'
    	  when grade = 'Very High' and similaritybucket='Medium' then 'P2'
    	  when grade = 'Very High' and similaritybucket='High'	 then 'P3'
    	  when grade = 'High' and similaritybucket='Low' 		 then 'P4'
    	  when grade = 'High' and similaritybucket='Medium' 	 then 'P5'
    	  when grade = 'High' and similaritybucket='High' 		 then 'P6'
    	  when grade = 'Medium' and similaritybucket='Low' 		 then 'P7'
    	  when grade = 'Medium' and similaritybucket='Medium' 	 then 'P8'
    	  when grade = 'Medium' and similaritybucket='High' 	 then 'P9'
    	  when grade = 'Low' and similaritybucket='Low' 		 then 'P10'
    	  when grade = 'Low' and similaritybucket='Medium' 	 	 then 'P11'
    	  when grade = 'Low' and similaritybucket='High' 		 then 'P12'
    	  when grade = 'Very Low' and similaritybucket='Low' 	 then 'P13'
    	  when grade = 'Very Low' and similaritybucket='Medium'  then 'P14'
    	  when grade = 'Very Low' and similaritybucket='High' 	 then 'P15'
    	  else null
    	end)::text AS priority
   FROM ( SELECT doc.requestid,
            doc.fuuid,
            doc.daoriginal_fileid,
            doc.dafileid,
            doc.filename,
            doc.title,
            doc.description,
            concat(doc.gtl_synopsis, '<hr>', gr.summary) AS gtl_synopsis,
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
            case when doc.similarity >75.0 then 'High'
            	 when doc.similarity >40.0 then 'Medium'
            	 else 'Low'
             end similaritybucket
           FROM ciq_fssit.vwdocuments doc
             LEFT JOIN ciq_fssit.tgrading gr ON gr.requestid = doc.requestid AND gr.fuuid = doc.fuuid AND gr.dafileid = COALESCE(doc.dafileid, doc.daoriginal_fileid)) sqry;

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

CREATE OR REPLACE VIEW ciq_cssit.vwdocuments
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
           FROM ciq_cssit.tdocument doc
             JOIN ( SELECT ofr.practice,
                    ofr.offerfamily,
                    ofr.offer,
                    ofr.gtl
                   FROM ciq_cssit.vwofferfamilydata ofr
                UNION ALL
                 SELECT 'UnDefined'::text AS text,
                    'UnDefined'::text AS text,
                    'UnDefined'::text AS text,
                    NULL::character varying AS gtl) ofd ON similarity(COALESCE(doc.offer, 'UnDefined'::character varying)::text, ofd.offer) > 0.75::double precision
             LEFT JOIN ciq_cssit.tdtpm_mapping mp ON TRIM(BOTH FROM lower(doc.ip_type::text)) = TRIM(BOTH FROM lower(mp.ip_type::text))) sqry
     LEFT JOIN ( SELECT rec.templateid,
            NULLIF(sum(
                CASE
                    WHEN rec.status::text = 'ACCEPTED'::text THEN 1.0
                    ELSE 0.0::integer::numeric
                END), 0.0) / count(1)::numeric AS acceptance
           FROM ciq_cssit.trecommendation rec
          WHERE rec.status::text <> 'GENERATED'::text AND rec.method::text = 'offerbased'::text
          GROUP BY rec.templateid) reccnt ON reccnt.templateid = COALESCE(sqry.dafileid, sqry.daoriginal_fileid)
  WHERE sqry.rnk = 1;