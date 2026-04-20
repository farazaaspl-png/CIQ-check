INSERT INTO ciq_fssit.tprocess_ips_rule(event_type, event_sub_type, file_suffix, download, "convert", content_extraction, sensitive_item_extraction, classification, redaction, upload) VALUES('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION', 'REPROCESS_DOCUMENT', '.pdf', true, true, true, true, true, true, true);
INSERT INTO ciq_fssit.tprocess_ips_rule(event_type, event_sub_type, file_suffix, download, "convert", content_extraction, sensitive_item_extraction, classification, redaction, upload) VALUES('IP_GOLDEN_COPY_REQUEST_RECOMMENDATION', 'REPROCESS_DOCUMENT', 'others', true, false, true, true, true, true, true);
ALTER TABLE ciq_fssit.tchange_document ALTER COLUMN newvalue TYPE text;
ALTER TABLE ciq_fssit.tchange_document ALTER COLUMN oldvalue TYPE text;
ALTER TABLE ciq_fssit.tdeep_search_logs ADD COLUMN is_app Boolean DEFAULT false;
INSERT INTO ciq_fssit.tredaction_exclusion("exclude") VALUES('Dell Team');
INSERT INTO ciq_fssit.tlabel_lookup (category, "label") VALUES('vendor project name', 'Vendor');
INSERT INTO ciq_fssit.tlabel_lookup (category, "label") VALUES('dell internal person name', 'Internal');
INSERT INTO ciq_fssit.tlabel_lookup (category, "label") VALUES('customer team name', 'Customer');
insert into ciq_fssit.config (name,val,comment,isactive) values('DEEP_SEARCH_RELEVANCE_SCORE_THRESHOLD','0.5','threshold for deep search relevance score',True)
ALTER TABLE ciq_fssit.tdeep_search_logs ADD reason text NULL;


CREATE OR REPLACE VIEW ciq_fssit.vw_deep_search
AS SELECT l.id,
    l.requestid,
    l.fuuid,
    l.userquery,
    l.relevance_score,
    l.relevance,
    l.created_date,
    d.projectid,
    d.title,
    d.description,
    d.filename,
    d.document_type,
    d.ip_type,
    d.practice,
    d.offerfamily,
    d.offer,
    d.acceptance,
    d.dtpm_phase,
    d.author,
    d.gtl_email,
    d.usage_count
   FROM ciq_fssit.tdeep_search_logs l
     LEFT JOIN ciq_fssit.vwdocuments d ON d.fuuid = l.fuuid
  WHERE d.status::text = 'APPROVED'::text
  ORDER BY l.created_date DESC; 

CREATE TABLE ciq_fssit.tredaction_col_lookup (
	"colname" varchar(255),
	CONSTRAINT tredaction_col_lookup_pkey PRIMARY KEY ("colname")
);

insert into ciq_fssit.tredaction_col_lookup (colname)
values
 ('Firstname')
,('Name')
,('Lastname')
,('Date')
,('Date Opened')
,('Assigned To')
,('Users')
,('SSL POOL')
,('Server Name')
,('Customer')
,('Action Date')
,('Author')
,('Reviewers')
,('Email Address')
,('Phone Number')
,('Hostname')
,('Gateway')
,('Production VLAN ID')
,('MGMT IP')
,('Mgmt VLAN ID')
,('SSL Subnet')
,('SSLVPN VLAN ID')
,('Actual IP')
,('Natted IP');

ALTER TABLE ciq_fssit.textraction ALTER COLUMN reason TYPE text USING reason::text;


WITH acceptance_calc AS (
    SELECT 
        rec.templateid,
        rec.created_date,
        NULLIF(
            sum(CASE WHEN sub_rec.status::text = 'ACCEPTED'::text THEN 1.0 ELSE 0.0::integer END), 
            0.0
        ) / count(1) AS acceptances
    FROM ciq_fssit.trecommendation rec
    LEFT JOIN ciq_fssit.trecommendation sub_rec ON rec.templateid = sub_rec.templateid
        AND sub_rec.created_date < rec.created_date
        AND sub_rec.status::text <> 'GENERATED'::text
        AND sub_rec.method = 'offerbased'
    WHERE rec.status::text <> 'GENERATED'::text
    AND rec.method = 'offerbased'
    GROUP BY rec.templateid, rec.created_date
)
UPDATE ciq_fssit.trecommendation rec
SET acceptance = acc.acceptances*100
FROM acceptance_calc acc
WHERE rec.templateid = acc.templateid
AND rec.created_date = acc.created_date
AND rec.status::text <> 'GENERATED'::text
AND rec.method = 'offerbased';

drop view if exists ciq_fssit.vw_deep_search;
drop view if exists ciq_fssit.vwgetrecommendation;
drop view if exists ciq_fssit.vwrecommendations;
drop view if exists ciq_fssit.vwclassificationout;
drop view if exists ciq_fssit.vwgetrefinerecommendation;
drop view if exists ciq_fssit.vwdocuments;

ALTER TABLE ciq_fssit.tdocument DROP COLUMN if exists "type";
ALTER TABLE ciq_fssit.tdocument DROP COLUMN if exists "url";
ALTER TABLE ciq_fssit.tdocument DROP COLUMN if exists mathcingdafileid;
ALTER TABLE ciq_fssit.tdocument DROP COLUMN if exists similarity;


ALTER TABLE ciq_fssit.tdocument ADD "type" varchar(50) NULL;
ALTER TABLE ciq_fssit.tdocument ADD "url" varchar(2100) NULL;
ALTER TABLE ciq_fssit.tdocument ADD mathcingdafileid uuid NULL;
ALTER TABLE ciq_fssit.tdocument ADD similarity float8 NULL;


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
    sqry.offersimilarity,
    sqry.type,
    sqry.url,
    sqry.mathcingdafileid,
    sqry.similarity
   FROM ( SELECT doc.requestid,
            doc.fuuid,
            doc.daoriginal_fileid,
            coalesce(doc.dafileid, doc.daoriginal_fileid) dafileid,
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
			            NULLIF(sum(CASE WHEN rec.status::text = 'ACCEPTED'::text THEN 1.0 ELSE 0.0::integer END), 0.0 ) / count(1) AS acceptance
           		   FROM ciq_fssit.trecommendation rec
         		  WHERE rec.status::text <> 'GENERATED'::text
            		and rec.method = 'offerbased'
          		GROUP BY rec.templateid) reccnt ON reccnt.templateid = COALESCE(sqry.dafileid, sqry.daoriginal_fileid)
  WHERE sqry.rnk = 1;
 
 
 CREATE OR REPLACE VIEW ciq_fssit.vwgetrecommendation
AS SELECT sowrequestid,
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
   FROM ( SELECT DISTINCT sow.requestid AS sowrequestid,
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
AS SELECT requestid,
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
            WHEN relevancescore >= 26::double precision AND relevancescore < 50::double precision THEN 'Low'::text
            WHEN relevancescore >= 51::double precision AND relevancescore < 75::double precision THEN 'Medium'::text
            WHEN relevancescore >= 76::double precision AND relevancescore < 90::double precision THEN 'High'::text
            WHEN relevancescore >= 91::double precision THEN 'Very High'::text
            ELSE NULL::text
        END AS relevance,
    method,
    type,
    url,
    created_date
   FROM ( SELECT sqry.requestid,
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
                    WHEN sqry.method::text = 'offerbased'::text THEN (sqry.sow_off_relevance_score_scaled * wt.sow_off_relevance_score_wt + sqry.similarityscore_scaled * wt.similarityscore_wt + sqry.acceptance_scaled * wt.acceptance_wt) * (0.95::double precision + 0.05::double precision * LEAST(sqry.similarityscore_scaled, sqry.acceptance_scaled) / 100.0::double precision)
                    ELSE (sqry.similarityscore_scaled * wt.similarityscore_wt + sqry.acceptance_scaled * wt.acceptance_wt) * (0.95::double precision + 0.05::double precision * LEAST(sqry.similarityscore_scaled, sqry.acceptance_scaled) / 100.0::double precision)
                END AS relevancescore,
            sqry.type,
            sqry.url,
            sqry.created_date
           FROM ( SELECT rec.requestid,
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
                    doc.type,
                    doc.url,
                    rec.sow_off_relevance_score,
                    mm.sow_off_relevance_score_min,
                    mm.sow_off_relevance_score_max,
                    COALESCE((rec.sow_off_relevance_score - mm.sow_off_relevance_score_min::double precision) / NULLIF(mm.sow_off_relevance_score_max - mm.sow_off_relevance_score_min, 0::numeric)::double precision, 0::double precision) * 100::double precision AS sow_off_relevance_score_scaled,
                    rec.similarityscore,
                    mm.similarityscore_min,
                    mm.similarityscore_max,
                    COALESCE((rec.similarityscore - mm.similarityscore_min) / NULLIF(mm.similarityscore_max - mm.similarityscore_min, 0::double precision), 0.01::double precision) * 100::double precision AS similarityscore_scaled,
                    rec.acceptance,
                    mm.acceptance_min,
                    mm.acceptance_max,
                    COALESCE((rec.acceptance - mm.acceptance_min) / NULLIF(mm.acceptance_max - mm.acceptance_min, 0::double precision), 0.01::double precision) * 100::double precision AS acceptance_scaled,
                    rec.method,
                    rec.created_date
                   FROM ciq_fssit.trecommendation rec
                     JOIN ciq_fssit.vwdocuments doc ON rec.templateid = doc.dafileid
                     JOIN ( SELECT inrec.method,
                            	   min(val::numeric*100.0) AS sow_off_relevance_score_min,
                            	   max(inrec.sow_off_relevance_score::numeric) AS sow_off_relevance_score_max,
                            	   min(inrec.similarityscore) AS similarityscore_min,
                            	   max(inrec.similarityscore) AS similarityscore_max,
                            	   min(inrec.acceptance) AS acceptance_min,
                            	   max(inrec.acceptance) AS acceptance_max
                           	  FROM ciq_fssit.trecommendation inrec
                           	  join ciq_fssit.config on name='THRESHOLD_SOW_OFFER_SCORE'
                          GROUP BY inrec.method) mm ON rec.method::text = mm.method::text) sqry
             JOIN ( SELECT config.comment AS method,
                    max(
                        CASE
                            WHEN config.name::text = 'sow_off_relevance_score_wt'::text THEN config.val::double precision
                            ELSE NULL::double precision
                        END) AS sow_off_relevance_score_wt,
                    max(
                        CASE
                            WHEN config.name::text = 'similarityscore_wt'::text THEN config.val::double precision
                            ELSE NULL::double precision
                        END) AS similarityscore_wt,
                    max(
                        CASE
                            WHEN config.name::text = 'acceptance_wt'::text THEN config.val::double precision
                            ELSE NULL::double precision
                        END) AS acceptance_wt
                   FROM ciq_fssit.config
                  WHERE config.name::text = ANY (ARRAY['sow_off_relevance_score_wt'::character varying::text, 'similarityscore_wt'::character varying::text, 'acceptance_wt'::character varying::text])
                  GROUP BY config.comment) wt ON wt.method::text =
                CASE
                    WHEN sqry.method::text <> 'offerbased'::text THEN 'search'::character varying
                    ELSE sqry.method
                END::text) sqry2;
                
CREATE OR REPLACE VIEW ciq_fssit.vwclassificationout
AS SELECT requestid,
    fuuid,
    daoriginal_fileid,
    dafileid AS dafileid,
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
    coalesce(type,'') type,
    coalesce(url,'') url,
    0.0 AS gradescore,
    'A'::text AS grade,
    coalesce(similarity, 0.0) AS similarty,
    'High' priority,
    0.0 priority_score
   FROM ( SELECT doc.requestid,
            doc.fuuid,
            doc.daoriginal_fileid,
            doc.dafileid,
            doc.filename,
            doc.title,
            doc.description,
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
            doc.waspdf,
            doc.type,
            doc.url,
            doc.similarity
           FROM ciq_fssit.vwdocuments doc) sqry;
       
CREATE OR REPLACE VIEW ciq_fssit.vw_deep_search
AS SELECT l.id,
    l.requestid,
    l.fuuid,
    l.userquery,
    l.relevance_score,
    l.relevance,
    l.created_date,
    d.projectid,
    d.title,
    d.description,
    d.filename,
    d.document_type,
    d.ip_type,
    d.practice,
    d.offerfamily,
    d.offer,
    d.acceptance,
    d.dtpm_phase,
    d.author,
    d.gtl_email,
    d.usage_count
   FROM ciq_fssit.tdeep_search_logs l
     LEFT JOIN ciq_fssit.vwdocuments d ON d.fuuid = l.fuuid
  WHERE d.status::text = 'APPROVED'::text
  ORDER BY l.created_date DESC;
  
 
 CREATE OR REPLACE VIEW ciq_fssit.vwgetrefinerecommendation
AS SELECT dafileid,
    ipid,
    lower(COALESCE(dtpm_phase, 'na'::character varying)::text)::character varying(100) AS dtpm_phase,
    concat(COALESCE(lower(offer), ''::text), ' ', COALESCE(lower(offerfamily), ''::text), ' ', COALESCE(lower(practice), ''::text), ' ', COALESCE(lower(ip_type::text), ''::character varying::text), ' ', COALESCE(lower(document_type::text), ''::character varying::text), ' ', COALESCE(lower(dtpm_phase::text), ''::character varying::text), ' ', COALESCE(lower(title::text), ''::character varying::text), ' ', filename, ' ', COALESCE(lower(author::text), ''::character varying::text)) AS metadata
   FROM ciq_fssit.vwdocuments v
  WHERE status::text = 'APPROVED'::text;

DROP FUNCTION if exists ciq_fssit.fngeneraterecommendation(uuid, bool);
                                                             
-- DROP FUNCTION ciq_fssit.fngeneraterecommendation(uuid, bool);

CREATE OR REPLACE FUNCTION ciq_fssit.fngeneraterecommendation(p_sowrequestid uuid, is_refine_recommendation boolean DEFAULT false)
 RETURNS TABLE(requestid text, 
 			   projectid character varying, 
 			   templateid text, 
 			   ipid text, 
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
 			   relevance text, 
 			   eps_section text,
 			   type character varying,
               url character varying)
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
						gr.usage_count--,
						--row_number() over(partition by case when gr.document_type::text = 'Delivery Template'::text then 1 else 0 end order by gr.sow_off_relevance_score desc,COALESCE(gr.similarityscore,0.0) desc) rnk
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
				        NULLIF( sum(CASE WHEN sub_rec.status::text = 'ACCEPTED'::text THEN 1.0 ELSE 0.0::integer END), 0.0 ) / count(1) AS acceptance
			      FROM ciq_fssit.trecommendation rec
			     WHERE rec.status::text <> 'GENERATED'::text
   				   AND rec.method = 'offerbased'
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
	      COALESCE(rec.relevancescore::numeric, 0)  relevancescore,
		  COALESCE(rec.relevance,'')            	relevance,
	      COALESCE(rec.eps_section,'')           	eps_section,
		  COALESCE(rec.type,'')						type,
          COALESCE(rec.url,'')						url
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
