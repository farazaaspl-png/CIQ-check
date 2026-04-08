insert into ciq_fssit.config (name,val,comment,isactive) values('DATA_DIR','./ip_content_management','root directory',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('GTL_FLOW_DIR','DocumentProcessing','directory for glt flow',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('CONSULTANT_FLOW_DIR','SowRecommendation','directory for sa flow',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('DOCUMENT_STATE_TABLE','tprocess_ips_state','This table is used to maintain the state of execution',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('REDACTION_EXCLUSION_TABLE','tredaction_exclusion','This table holds the list of items not be excluded',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('REDACTION_INCLUSION_TABLE','tredaction_inclusion','This table is used to maintain the state of execution',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('DOCUMENT_RULE_TABLE','tprocess_ips_rule','This table is used to control the staged execution of ip process',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('NOTIFICATION_SWITCH','True','this is used to control the email notification',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('NOTIFICATION_DL','lekhnath.pandey@dellteam.com;vedish.kabara@dellteam.com;punit.gour@dellteam.com','List of email that will get the notification',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('ECS_ENDPOINT','https://cloudstorage-r2cpc1np-pc1np.dell.com','AWS S3 Endpoint',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('ECS_REGION','us-east-1','AWS S3 region',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('ECS_VERIFY','False','AWS S3 verify ssl',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('ECS_BUCKET','fssit','AWS S3 bucket',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('ZIP_FILE_SUMMARY_TABLE','tzip_file_details','database objects',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('DOCUMENT_CONTENT_STORE','fssit_document_contents','database objects',True);

ALTER TABLE ciq_fssit.tfeedback RENAME COLUMN msguuid TO fuuid;


CREATE TABLE ciq_fssit.tprocess_ips_state (
	requestid uuid NOT NULL,
	fuuid uuid NOT NULL,
	dafileid uuid NOT NULL,
	request_dir varchar(255) null,
	filepath varchar(255) NULL,
	ispdf bool,
    converted_filepath varchar(255) null,
    extraction_input_file varchar(255) ,
    classification_input_file varchar(255),
    redacted_filename varchar(255) null,
    has_sensitive_items  BOOL,
	istextredacted bool,
	isimageredacted bool,
	out_filepath varchar(255) null,
	redacted_items_filepath varchar(255) null,
	out_dafileid uuid null,
	redacted_items_dafileid varchar(255) null,
	stageno int null,
	stagename varchar(100) null,
	status varchar(100) null,
	inserted_on timestamp DEFAULT (now() AT TIME ZONE 'UTC'::text) NULL,
	updated_on timestamp NULL,
	CONSTRAINT tprocess_ips_state_pkey PRIMARY KEY (requestid,fuuid,dafileid)
);


CREATE TABLE ciq_fssit.tprocess_ips_rule (
	event_type varchar(100) not null,
	event_sub_type varchar(100) not null,
	file_suffix varchar(20) not null,
	download bool default True null,
	convert bool default True null,
	content_extraction bool default True null,
	sensitive_item_extraction bool default True null,
	classification bool default True null,
	redaction bool default True null,
	upload bool default True null,
	generatepayload bool default true null,
	CONSTRAINT tprocess_ips_rule_pkey PRIMARY KEY (event_type,event_sub_type,file_suffix)
);

insert into ciq_fssit.tprocess_ips_rule 
	(event_type,
	 event_sub_type,
	 file_suffix,
	 download,
	 convert,
	 content_extraction,
	 sensitive_item_extraction,
	 classification,
	 redaction,
	 upload,
	 generatepayload)
values
('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','UPLOAD_NEW_FILE','.pdf',true,true,true,true,true,true,true,true),
('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','UPLOAD_NEW_FILE','others',true,false,true,true,true,true,true,true),
('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','','.pdf',true,true,true,true,true,true,true,true),
('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','','others',true,false,true,true,true,true,true,true),
('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','MANUAL_UPLOAD','.pdf',true,true,true,true,true,false,true,true),
--('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','MANUAL_UPLOAD','.zip',true,false,false,false,false,false,false,true),
('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','MANUAL_UPLOAD','others',true,false,true,true,true,false,false,true);
--('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','REPROCESS_DOCUMENT','.pdf',true,true,true,true,true,true,true,true),
--('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION','REPROCESS_DOCUMENT','others',true,false,true,true,true,true,true,true),


select * 
  into ciq_fssit.tdocument_07022026
  from ciq_fssit.tdocument;

drop view  ciq_fssit.vwgetrefinerecommendation;
drop view  ciq_fssit.vwrecommendations;
drop view  ciq_fssit.vwgetrecommendation;
drop view ciq_fssit.vwclassificationout;
drop view  ciq_fssit.vwdocuments;
drop table ciq_fssit.tdocument;


CREATE TABLE ciq_fssit.tdocument (
	requestid uuid NOT NULL,
	fuuid uuid not null,
	daoriginal_fileid uuid NOT NULL,
	dafileid uuid NULL,
	ipid uuid NULL,
	projectid varchar(100) NULL,
	filename varchar(255) NULL,
	title varchar(255) NULL,
	description text NULL,
	gtl_synopsis text NULL,
	author varchar(100) NULL,
	dtpm_phase varchar(100) NULL,
	document_type varchar(50) NULL,
	ip_type varchar(255) NULL,
	offer varchar(255) NULL,
	relevance_score float8 NULL,
	usage_count int4 DEFAULT 0 NULL,
	redacted_items_dafileid uuid NULL,
	waspdf boolean null,
	status varchar(100) NULL,
	uploadedby varchar(100) NULL,
	created_by varchar(100) NULL,
	updated_by varchar(100) NULL,
	created_date timestamptz DEFAULT now() NOT NULL,
	updated_date timestamptz NULL,
	CONSTRAINT tdocument_pk PRIMARY KEY (requestid, fuuid, daoriginal_fileid)
);

insert into ciq_fssit.tdocument 
   (requestid,
	fuuid ,
	daoriginal_fileid ,
	dafileid,
	ipid ,
	projectid,
	filename,
	title ,
	description ,
	author,
	dtpm_phase,
	document_type,
	ip_type  ,
	offer,
	relevance_score ,
	usage_count,
	redacted_items_dafileid,
	status  ,
	uploadedby ,
	created_by ,
	updated_by ,
	created_date ,
	updated_date )
select requestid,
	fuuid ,
	daoriginal_fileid ,
	dafileid,
	ipid ,
	projectid,
	filename,
	title ,
	description ,
	author,
	dtpm_phase,
	document_type,
	ip_type  ,
	offer,
	relevance_score ,
	usage_count,
	dasanitizationoutfileid,
	status  ,
	uploadedby ,
	created_by ,
	updated_by ,
	created_date ,
	updated_date 
	from (
select *, row_number() over(partition by requestid,fuuid,daoriginal_fileid order by created_date desc,updated_date desc) rnk
from (
select doc.requestid,
	coalesce (case when doc.requestid ='00000000-0000-0000-0000-000000000001' then doc.daoriginal_fileid
		 else cast(x.fuuid as uuid)
	 end,fb.fuuid) fuuid,
	case when doc.requestid ='00000000-0000-0000-0000-000000000001' then doc.dafileid
		 else doc.daoriginal_fileid
	 end daoriginal_fileid,
	doc.dafileid,
	doc.ipid,
	doc.projectid,
	doc.filename,
	doc.title,
	doc.description,
	doc.author,
	doc.dtpm_phase,
	doc.document_type,
	doc.ip_type,
	doc.offer,
	doc.relevance_score,
	doc.usage_count,
	doc.dasanitizationoutfileid,
	doc.status,
	doc.uploadedby,
	doc.created_by,
	doc.updated_by,
	doc.created_date,
	doc.updated_date
from ciq_fssit.tdocument_07022026 doc --972
left join (select request_id,
				  cast(an.payload->>'uuid' as uuid) fuuid,
				  cast(nullif(replace(an.payload->>'daFileId','string',''),'') as uuid) dafileid  
			 from ciq_fssit.an_event an 
			where event_type='IP_GOLDEN_COPY_REQUEST_RECOMMENDATION')	x	
				on doc.requestid =x.request_id
				and doc.daoriginal_fileid  = x.dafileid
left join (				
	select replace(replace(replace(filename,'.pptx',''),'.pdf',''),'.docx','') filename,daoriginal_fileid fuuid,dafileid from ciq_fssit.tdocument_07022026 doc 
	where requestid ='00000000-0000-0000-0000-000000000001') fb on fb.filename=replace(replace(replace(doc.filename,'.pptx',''),'.pdf',''),'.docx','')
) where fuuid is not null )sq where rnk =1;

-- ciq_fssit.vwdocuments source

CREATE OR REPLACE VIEW ciq_fssit.vwdocuments
AS 
SELECT sqry.requestid,
       sqry.fuuid,
       sqry.daoriginal_fileid,
       sqry.dafileid,
       sqry.ipid,
       sqry.projectid,
       sqry.filename,
       sqry.title,
       sqry.description,
       sqry.gtl_synopsis,
       sqry.author,
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
       reccnt.acceptance * 100 AS acceptance,
       sqry.redacted_items_dafileid,
       sqry.waspdf,
       sqry.created_by,
       sqry.updated_by,
       sqry.created_date,
       sqry.updated_date,
       sqry.offersimilarity
  FROM (SELECT doc.requestid,
               doc.fuuid,
               doc.daoriginal_fileid,
               doc.dafileid,
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
               doc.created_by,
               doc.updated_by,
               to_char(doc.created_date, 'YYYY-MM-DD HH24:MI:SS'::text) AS created_date,
               to_char(doc.updated_date, 'YYYY-MM-DD HH24:MI:SS'::text) AS updated_date,
               similarity(doc.offer::text, ofd.offer) AS offersimilarity,
               row_number() OVER (PARTITION BY doc.requestid, doc.daoriginal_fileid, doc.offer ORDER BY (similarity(doc.offer::text, ofd.offer)) DESC) AS rnk
          FROM ciq_fssit.tdocument doc
          JOIN (SELECT ofr.practice,
                       ofr.offerfamily,
                       ofr.offer,
                       ofr.gtl
                  FROM ciq_fssit.vwofferfamilydata ofr
                 UNION ALL
                SELECT  'Unable To Predict'::text AS text,
                        'Unable To Predict'::text AS text,
                        'Unable To Predict'::text AS text,
                        NULL as gtl) ofd ON similarity(COALESCE(doc.offer, 'Unable To Predict'::character varying)::text, ofd.offer) > 0.75::double precision
          LEFT JOIN ciq_fssit.tdtpm_mapping mp ON TRIM(BOTH FROM lower(doc.ip_type::text)) = TRIM(BOTH FROM lower(mp.ip_type::text))) sqry
  LEFT JOIN ( SELECT rec.templateid,
                     count(1) / NULLIF(count(CASE
                                                 WHEN rec.status::text = 'ACCEPTED'::text THEN 1
                                                 ELSE NULL::integer
                                             END), 0) AS acceptance
                FROM ciq_fssit.trecommendation rec
               WHERE rec.status::text <> 'GENERATED'::text
            GROUP BY rec.templateid) reccnt ON reccnt.templateid = sqry.daoriginal_fileid
  WHERE sqry.rnk = 1;
 
 
 -- ciq_fssit.vwgetrecommendation source
CREATE OR REPLACE VIEW ciq_fssit.vwgetrecommendation
AS 
SELECT sowrequestid,
       projectid,
       sowfileid,
       templateid,
       ipid,
       filename,
       title,
       description,
       dtpm_phase,
       document_type,
       ip_type,
       practice,
       offerfamily,
       offer,
       usage_count,
       sow_off_relevance_score,
       similarityscore,
       acceptance,
       status
  FROM (SELECT DISTINCT 
               sow.requestid AS sowrequestid,
               sow.projectid,
               sow.dafileid AS sowfileid,
               doc.dafileid AS templateid,
               doc.ipid,
               doc.filename,
               doc.title,
               doc.description,
               doc.dtpm_phase,
               doc.document_type,
               doc.ip_type,
               doc.practice,
               doc.offerfamily,
               doc.offer,
               doc.usage_count,
               sow.relevance_score AS sow_off_relevance_score,
               similarity(sow.summary, doc.description)::double precision * 100.0::double precision AS similarityscore,
               doc.acceptance,
               doc.status,
               row_number() OVER (PARTITION BY sow.requestid, doc.dafileid ORDER BY sow.relevance_score DESC) AS rnk
          FROM ciq_fssit.vwstatementofwork sow
          JOIN ciq_fssit.vwdocuments doc ON sow.practice = doc.practice AND sow.offerfamily = doc.offerfamily AND sow.offer = doc.offer AND (doc.status::text = ANY (ARRAY['APPROVED'::character varying::text, 'REPLACED'::character varying::text]))) sqry
  WHERE rnk = 1;
 
 -- ciq_fssit.vwrecommendations source
CREATE OR REPLACE VIEW ciq_fssit.vwrecommendations
AS 
SELECT requestid,
       projectid,
       templateid,
       ipid,
       filename,
       title,
       description,
       practice,
       offerfamily,
       offer,
       status,
       documenttype,
       dtpmphase,
       iptype,
       eps_section,
       usage_count,
       sow_off_relevance_score,
       sow_off_relevance_score_min,
       sow_off_relevance_score_max,
       sow_off_relevance_score_scaled,
       similarityscore,
       similarityscore_min,
       similarityscore_max,
       similarityscore_scaled,
       acceptance,
       acceptance_min,
       acceptance_max,
       acceptance_scaled,
       relevancescore,
       CASE
           WHEN relevancescore < 25::double precision THEN 'Very Low'::text
           WHEN relevancescore >= 25::double precision AND relevancescore < 40::double precision THEN 'Low'::text
           WHEN relevancescore >= 40::double precision AND relevancescore < 60::double precision THEN 'Medium'::text
           WHEN relevancescore >= 60::double precision AND relevancescore < 75::double precision THEN 'High'::text
           WHEN relevancescore >= 75::double precision THEN 'Very High'::text
           ELSE NULL::text
       END AS relevance,
       method
  FROM (SELECT sqry.requestid,
               sqry.projectid,
               sqry.templateid,
               sqry.ipid,
               sqry.filename,
               sqry.title,
               sqry.description,
               sqry.practice,
               sqry.offerfamily,
               sqry.offer,
               sqry.status,
               sqry.documenttype,
               sqry.dtpmphase,
               sqry.iptype,
               sqry.eps_section,
               sqry.usage_count,
               sqry.sow_off_relevance_score,
               sqry.sow_off_relevance_score_min,
               sqry.sow_off_relevance_score_max,
               sqry.sow_off_relevance_score_scaled,
               sqry.similarityscore,
               sqry.similarityscore_min,
               sqry.similarityscore_max,
               sqry.similarityscore_scaled,
               sqry.acceptance,
               sqry.acceptance_min,
               sqry.acceptance_max,
               sqry.acceptance_scaled,
               sqry.method,
               CASE
                   WHEN sqry.method::text = 'offerbased'::text 
                   THEN (sqry.sow_off_relevance_score_scaled * wt.sow_off_relevance_score_wt + 
                         sqry.similarityscore_scaled * wt.similarityscore_wt + 
                         sqry.acceptance_scaled * wt.acceptance_wt) * 
                            (0.95::double precision + 0.05::double precision * LEAST(sqry.similarityscore_scaled, sqry.acceptance_scaled) / 100.0::double precision)
                   ELSE (sqry.similarityscore_scaled * wt.similarityscore_wt + 
                         sqry.acceptance_scaled * wt.acceptance_wt) *
                            (0.95::double precision + 0.05::double precision * LEAST(sqry.similarityscore_scaled, sqry.acceptance_scaled) / 100.0::double precision)
               END AS relevancescore
          FROM (SELECT rec.requestid,
                       rec.projectid,
                       rec.templateid,
                       rec.ipid,
                       doc.filename,
                       doc.title,
                       doc.description,
                       doc.practice,
                       doc.offerfamily,
                       doc.offer,
                       rec.status,
                       doc.document_type AS documenttype,
                       doc.dtpm_phase AS dtpmphase,
                       CASE
                           WHEN doc.ip_type IS NULL OR length(TRIM(BOTH FROM doc.ip_type)) = 0 THEN 'Not Applicable'::character varying(255)
                           ELSE doc.ip_type
                       END AS iptype,
                       CASE
                           WHEN doc.document_type::text = 'Delivery Template'::text THEN 'recommended_ip'::text
                           ELSE 'additional_field'::text
                       END AS eps_section,
                       doc.usage_count,
                       rec.sow_off_relevance_score,
                       mm.sow_off_relevance_score_min,
                       mm.sow_off_relevance_score_max,
                       COALESCE((rec.sow_off_relevance_score - mm.sow_off_relevance_score_min::double precision) 
                                    / NULLIF(mm.sow_off_relevance_score_max - mm.sow_off_relevance_score_min, 0::numeric)::double precision,
                                0::double precision) * 100::double precision AS sow_off_relevance_score_scaled,
                       rec.similarityscore,
                       mm.similarityscore_min,
                       mm.similarityscore_max,
                       COALESCE((rec.similarityscore - mm.similarityscore_min) 
                                    / NULLIF(mm.similarityscore_max - mm.similarityscore_min, 
                                0::double precision), 0.01::double precision) * 100::double precision AS similarityscore_scaled,
                       rec.acceptance,
                       mm.acceptance_min,
                       mm.acceptance_max,
                       COALESCE((rec.acceptance - mm.acceptance_min) 
                                    / NULLIF(mm.acceptance_max - mm.acceptance_min, 
                                0::double precision), 0.01::double precision) * 100::double precision AS acceptance_scaled,
                       rec.method
                  FROM ciq_fssit.trecommendation rec
                  JOIN ciq_fssit.vwdocuments doc ON rec.templateid = doc.dafileid
                  JOIN (SELECT inrec.method,
                               LEAST(min(inrec.sow_off_relevance_score::numeric), 0::numeric) AS sow_off_relevance_score_min,
                               max(inrec.sow_off_relevance_score::numeric) AS sow_off_relevance_score_max,
                               min(inrec.similarityscore) AS similarityscore_min,
                               max(inrec.similarityscore) AS similarityscore_max,
                               min(inrec.acceptance) AS acceptance_min,
                               max(inrec.acceptance) AS acceptance_max
                          FROM ciq_fssit.trecommendation inrec
                         GROUP BY inrec.method) mm ON rec.method::text = mm.method::text) sqry
          JOIN (SELECT config.comment AS method,
                        max(CASE
                                WHEN config.name::text = 'sow_off_relevance_score_wt'::text THEN config.val::double precision
                                ELSE NULL::double precision
                            END) AS sow_off_relevance_score_wt,
                        max(CASE
                                WHEN config.name::text = 'similarityscore_wt'::text THEN config.val::double precision
                                ELSE NULL::double precision
                            END) AS similarityscore_wt,
                        max(CASE
                                WHEN config.name::text = 'acceptance_wt'::text THEN config.val::double precision
                                ELSE NULL::double precision
                            END) AS acceptance_wt
                  FROM ciq_fssit.config
                 WHERE config.name::text = ANY (ARRAY['sow_off_relevance_score_wt'::character varying, 'similarityscore_wt'::character varying, 'acceptance_wt'::character varying]::text[])
                 GROUP BY config.comment) wt ON wt.method::text = CASE
                                                                    WHEN sqry.method::text <> 'offerbased'::text THEN 'search'::character varying
                                                                    ELSE sqry.method
                                                                   END::text) sqry2;
               
-- ciq_fssit.vwgetrefinerecommendation source

CREATE OR REPLACE VIEW ciq_fssit.vwgetrefinerecommendation
AS SELECT dafileid,
    ipid,
    lower(COALESCE(dtpm_phase, 'na'::character varying)::text)::character varying(100) AS dtpm_phase,
    concat(COALESCE(lower(offer), ''::text), ' ', COALESCE(lower(offerfamily), ''::text), ' ', COALESCE(lower(practice), ''::text), ' ', COALESCE(lower(ip_type::text), ''::character varying::text), ' ', COALESCE(lower(document_type::text), ''::character varying::text), ' ', COALESCE(lower(dtpm_phase::text), ''::character varying::text), ' ', COALESCE(lower(title::text), ''::character varying::text), ' ', filename, ' ', COALESCE(lower(author::text), ''::character varying::text)) AS metadata
   FROM ciq_fssit.vwdocuments v
  WHERE status::text = 'APPROVED'::text;
 


CREATE OR REPLACE VIEW ciq_fssit.vwclassificationout
AS 
SELECT requestid,
       fuuid,
       daoriginal_fileid,
       coalesce(dafileid,daoriginal_fileid) dafileid,
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
       COALESCE(usage_count,0) usage_count,
       COALESCE(status, ''::character varying) AS status,
       redacted_items_dafileid dasanitizationoutfileid,
       coalesce(waspdf,false) waspdf,
       0.0 gradescore,
       'A'  grade,
       0.0 uniqueness 
  FROM (SELECT doc.requestid,
               doc.fuuid,
               doc.daoriginal_fileid,
               doc.dafileid,
               doc.filename,
               doc.title,
               concat('<b>Description</b>:<br>',doc.description,'<br><hr><br>',doc.gtl_synopsis)  description,
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
 
select * into ciq_fssit.ttoberedacted_20260211 from ciq_fssit.ttoberedacted;
select * into ciq_fssit.tredacted_20260211 from ciq_fssit.tredacted;
select * into ciq_fssit.textraction_20260211 from ciq_fssit.textraction;

drop view ciq_fssit.vw_unredacted_items;
drop table ciq_fssit.ttoberedacted;
drop table ciq_fssit.tredacted;
drop table ciq_fssit.textraction;

CREATE TABLE ciq_fssit.ttoberedacted (
	id serial4 NOT NULL,
    requestid uuid NOT NULL,
	fuuid uuid NOT NULL,
	dafileid uuid NOT NULL,
	filename varchar(255) NOT NULL,
	category varchar(255) NULL,
	sensitivetext text NULL,
	created_by varchar(225) NULL,
	created_date timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT ttoberedacted_pkey PRIMARY KEY (id)
);

CREATE TABLE ciq_fssit.tredacted (
	id serial4 NOT NULL,
    requestid uuid NOT NULL,
	fuuid uuid NOT NULL,
	dafileid uuid NOT NULL,
	filename varchar(255) NOT NULL,
	category varchar(255) NULL,
	sensitivetext text NULL,
	placeholder varchar(255) NULL,
	context text NULL,
	iswrong bool NULL,
	created_by varchar(225) NULL,
	created_date timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT tredacted_pkey PRIMARY KEY (id)
);


CREATE TABLE ciq_fssit.textraction (
	id serial4 NOT NULL,
	requestid uuid NOT NULL,
	fuuid uuid NOT NULL,
    dafileid uuid NOT NULL,
	filename varchar(255) NOT NULL,
	category varchar(255) NULL,
	sensitivetext text NULL,
    context text NULL,
	score float8 NULL,
	created_by varchar(225) NULL,
	created_date timestamptz DEFAULT now() NOT NULL,
	iscorrect int4 NULL,
	reason varchar(5000) NULL,
	"source" varchar(255) DEFAULT 'unknown'::character varying NOT NULL,
	CONSTRAINT textraction_iscorrect_check CHECK ((iscorrect = ANY (ARRAY[0, 1]))),
	CONSTRAINT textraction_pkey PRIMARY KEY (id)
);

CREATE TABLE ciq_fssit.tzip_file_details (
	requestid uuid NOT NULL,
	fuuid uuid NOT NULL,
	dafileid uuid NOT NULL,
	filename varchar(1000) NOT NULL,
	is_supported bool NULL,
	content_extracted bool NULL,
	summary text NULL,
	short_summary text NULL,
	CONSTRAINT tzip_file_details_pkey PRIMARY KEY (requestid, fuuid, dafileid, filename)
);

update ciq_fssit.tdocument 
set gtl_synopsis = COALESCE(replace(substr(description,strpos(lower(description),'<b>key points</b>'),length(description)),'<br>',' '),'')
where description is not null;

update ciq_fssit.tdocument 
set description = COALESCE(replace(substr(description,length('<b>Description</b>:<br>')+1,abs(strpos(lower(description),'<b>key points</b>') -length('<b>Description</b>:<br>')-1)),'<br>',' '),'')
where description is not null;
 
update ciq_fssit.tdocument set gtl_synopsis =replace(gtl_synopsis,'Key points','Key Points')
where gtl_synopsis is not null;

DROP FUNCTION ciq_fssit.fngeneraterecommendation;

CREATE OR REPLACE FUNCTION ciq_fssit.fngeneraterecommendation(p_sowrequestid uuid, is_refine_recommendation boolean DEFAULT false)
 RETURNS TABLE(requestid text, 
 			   projectid character varying, 
 			   templateid text, 
 			   ipid text, 
 			--   dafileid text, 
 			   filename character varying, 
 			   title character varying, 
 			   description text, 
 			   practice text, 
 			   offerfamily text, 
 			   offer text, 
 			   status character varying, 
 			   source character varying, 
 			   phase character varying, 
 			   iptype character varying, 
 			   relevancescore numeric, 
 			 --  usage_count integer, 
 			   relevance text, 
 			   eps_section text)
 LANGUAGE plpgsql
AS $function$
begin
	if not is_refine_recommendation
	then
		delete from ciq_fssit.trecommendation rec where rec.requestid = p_sowrequestid;
	
		INSERT INTO ciq_fssit.trecommendation
				(requestid,
				projectid,
				sowfileid,
				templateid,
				ipid,
				sow_off_relevance_score,
				similarityscore,
                acceptance,
				usage_count,
				method,
				created_by,
				created_date)
		select  sqry.sowrequestid,
				sqry.projectid,
				sqry.sowfileid,
				sqry.templateid,
				sqry.ipid,
				sqry.sow_off_relevance_score,
				sqry.similarityscore,
			    sqry.acceptance,
				sqry.usage_count,
				'offerbased',
				'system',
				CURRENT_TIMESTAMP
		  from (select  gr.sowrequestid,
					    gr.projectid,
						gr.sowfileid,
						gr.templateid,
						gr.ipid,
						gr.sow_off_relevance_score,
						gr.similarityscore,
			    		gr.acceptance,
						gr.usage_count,
						row_number() over(partition by case when gr.document_type::text = 'Delivery Template'::text then 1 else 0 end order by gr.sow_off_relevance_score desc,COALESCE(gr.similarityscore,0.0) desc) rnk
				  from  ciq_fssit.vwgetrecommendation gr 
				 where  gr.sowrequestid = p_sowrequestid
	--	  		   and not exists(select 1
	--					   			from consult_np.fssit_trecommendation rec
	--					  		   where rec.projectid = gr.projectid
	--								 and rec.templateid = gr.templateid)
				) sqry;
		  --where sqry.rnk<=5;
	else

		UPDATE ciq_fssit.trecommendation AS trec
		   SET acceptance = kpi.acceptance
		  FROM (SELECT rec.templateid,
				       count(1)/nullif(count(CASE WHEN rec.status::text = 'ACCEPTED'::text THEN 1 ELSE NULL::integer END), 0) AS acceptance
			      FROM ciq_fssit.trecommendation rec
			     WHERE rec.status::text <> 'GENERATED'::text
		      GROUP BY rec.templateid) kpi
		 WHERE trec.requestid = p_sowrequestid
           and trec.templateid = kpi.templateid;

	end if;
	return query
	    select
	      COALESCE(rec.requestid::text,'')      	requestid,
	      COALESCE(rec.projectid,'')            	projectid,
	      COALESCE(rec.templateid::text,'')     	templateid,
	      COALESCE(rec.ipid::text,'')           	ipid,
	      --COALESCE(rec.dafileid::text,'')        	dafileid,
	      COALESCE(rec.filename,'')             	filename,
	      COALESCE(rec.title,'')               		title,
	      COALESCE(rec.description,'')            	description,
	      COALESCE(rec.practice,'')            		practice,
	      COALESCE(rec.offerfamily,'')          	offerfamily,
	      COALESCE(rec.offer,'')              		offer,
	      COALESCE(rec.status,'')              		status,
	      COALESCE(rec.documenttype,'')            "source",
	      COALESCE(rec.dtpmphase,'')              	phase,
	      COALESCE(rec.iptype,'')             		iptype,
--	      COALESCE(rec.similarityscore,0)::float  similarityscore,        -- MOVED UP
	      COALESCE(rec.relevancescore::numeric, 0)  relevancescore,         -- MOVED DOWN
	    --   COALESCE(rec.usage_count,0)          	 	usage_count,
		  COALESCE(rec.relevance,'')            	relevance,
	      COALESCE(rec.eps_section,'')           	eps_section
	    from ciq_fssit.vwrecommendations rec
	     where rec.requestid = p_sowrequestid;
--		where rec.projectid = COALESCE(
--							   (select tsow.projectid
--								 from consult_np.fssit_tstatementofwork tsow
--								where tsow.requestid = p_sowrequestid
--								limit 1),
--							   (select trec.projectid
--								 from consult_np.fssit_trecommendation trec
--								where trec.requestid = p_sowrequestid
--								limit 1));
end;$function$
;

CREATE TABLE ciq_fssit.tredaction_exclusion (
	"exclude" text NOT NULL,
	CONSTRAINT tredaction_exclusion_pkey PRIMARY KEY (exclude)
);

CREATE TABLE ciq_fssit.tredaction_inclusion (
	toberedacted text NOT NULL,
	"label" text NOT NULL,
	CONSTRAINT tredaction_inclusion_pkey PRIMARY KEY (toberedacted)
);

INSERT INTO ciq_fssit.tredaction_exclusion
("exclude")
VALUES('Microsoft');
INSERT INTO ciq_fssit.tredaction_exclusion
("exclude")
VALUES('Redhat');
INSERT INTO ciq_fssit.tredaction_exclusion
("exclude")
VALUES('COMPANY');
INSERT INTO ciq_fssit.tredaction_exclusion
("exclude")
VALUES('COMPANYNAME');
INSERT INTO ciq_fssit.tredaction_exclusion
("exclude")
VALUES('Dell');
INSERT INTO ciq_fssit.tredaction_exclusion
("exclude")
VALUES('Dell Inc');
INSERT INTO ciq_fssit.tredaction_exclusion
("exclude")
VALUES('Dell Technologies');

CREATE OR REPLACE VIEW ciq_fssit.vw_unredacted_items
AS SELECT t.dafileid,
    t.filename,
    t.sensitivetext AS unredacted_text
   FROM ciq_fssit.ttoberedacted t
     LEFT JOIN ciq_fssit.tredacted r ON t.sensitivetext = r.sensitivetext
  WHERE r.sensitivetext IS NULL;