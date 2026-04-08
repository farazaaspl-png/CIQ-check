update ciq_fssit.config set val = 'nomic-embed-text-v1' where name = 'EMBEDDING_MODEL';
update ciq_fssit.config set val = 'fssit_document_content_store' where name = 'DOCUMENT_CONTENT_STORE';
insert into ciq_fssit.config (name,val,comment,isactive) values('VECTOR_CHUNK_SIZE',5000,'chunk size used for vectorization',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('VECTOR_OVER_LAP_SIZE',100,'overlap size used for vectorization',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('SCORE_WT',0.90,'Weight used for similarity score in Deep search',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('COVERAGE_RATIO_WT',0.10,'Weight used for percent matched chunks in Deep search',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('DEEP_SEARCH_SIMILARITY_THRESHOLD',0.50,'Threshold for vector similarity in Deep search',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('DEEP_SEARCH_RELEVANCE_BUCKET','0,25,60,80,95,100','Bucket Boundaries for relevance score',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('DEEP_SEARCH_PUSH_SIZE',10,'No of items sent in one responce',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('DEEP_SEARCH_LOGS_TABLE','tdeep_search_logs','No of items sent in one responce',True);
ALTER TABLE ciq_fssit.tzip_file_details DROP COLUMN short_summary;
ALTER TABLE ciq_fssit.tprocess_ips_rule DROP COLUMN generatepayload;
ALTER TABLE ciq_fssit.tdtpm_mapping ADD COLUMN final_ip_type VARCHAR(255);
update ciq_fssit.config set val = 'False' where name = 'EMBED_LOCAL';

CREATE TABLE ciq_fssit.tdeep_search_logs (
	id serial4 NOT NULL,
	requestid uuid NOT NULL,
	fuuid uuid NOT NULL,
	userquery varchar(4000) not NULL,
	relevance_score float null,
	relevance varchar(50) null,
	created_date timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT tdeep_search_logs_pkey PRIMARY KEY (id)
);

UPDATE ciq_fssit.tdtpm_mapping SET final_ip_type = replace(ip_type, ' and ', ' & ');

UPDATE ciq_fssit.tdtpm_mapping
SET final_ip_type = CASE
    WHEN ip_type = 'Impact or gap assessment' THEN 'Impact/gap assessment'
    WHEN ip_type = 'End to end guide or T-minus schedule' THEN 'End to end guide/ T-minus schedule'
    WHEN ip_type = 'Meeting agendas or content' THEN 'Meeting agendas/content'
    ELSE final_ip_type 
END
WHERE ip_type IN (
    'Impact or gap assessment',
    'End to end guide or T-minus schedule',
    'Meeting agendas or content'
);

DROP VIEW IF EXISTS ciq_fssit.vwdtpmmapping;
 
CREATE VIEW ciq_fssit.vwdtpmmapping AS
SELECT
    ip_type,
    final_ip_type,
    dtpm_phase
FROM ciq_fssit.tdtpm_mapping;

drop view if exists ciq_fssit.vwclassificationout;

CREATE OR REPLACE VIEW ciq_fssit.vwclassificationout
AS SELECT requestid,
    fuuid,
    daoriginal_fileid,
    COALESCE(dafileid, daoriginal_fileid) AS dafileid,
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
    0.0 AS gradescore,
    'A'::text AS grade,
    0.0 AS uniqueness
   FROM ( SELECT doc.requestid,
            doc.fuuid,
            doc.daoriginal_fileid,
            doc.dafileid,
            doc.filename,
            doc.title,
            concat('<b>Description</b>:<br>', doc.description, '<br><hr><br>', doc.gtl_synopsis) AS description,
            doc.gtl_synopsis,
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
            doc.waspdf
           FROM ciq_fssit.vwdocuments doc) sqry;