ALTER TABLE ciq_fssit.trecommendation ADD usage_count int4 NULL;
ALTER TABLE ciq_fssit.trecommendation ADD acceptance float8 NULL;
ALTER TABLE ciq_fssit.trecommendation ADD sow_off_relevance_score float8 NULL;
ALTER TABLE ciq_fssit.tapi_call_logs  ADD prompt TEXT NULL;
ALTER TABLE ciq_fssit.textraction  ADD source varchar(50) NULL;

insert into ciq_fssit.config (name,val,comment,isactive) values('sow_off_relevance_score_wt',0.70,'offerbased',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('similarityscore_wt',0.20,'offerbased',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('acceptance_wt',0.10,'offerbased',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('ECS_BUCKET','fssit','storage',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('ZIP_FILE_SUMMARY_TABLE','tzip_file_summary','database objects',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('similarityscore_wt',0.80,'search',True);
insert into ciq_fssit.config (name,val,comment,isactive) values('acceptance_wt',0.20,'search',True);
update ciq_fssit.trecommendation set method = 'offerbased' where method is null;
update ciq_fssit.trecommendation set sow_off_relevance_score = sow_off_relevance_score*100, similarityscore =similarityscore * 100;
update ciq_cssit.config set val='nomic-embed-text-v1' where name='EMBEDDING_MODEL';
commit;

drop view ciq_fssit.vwrecommendations; 
drop view ciq_fssit.vwgetrefinerecommendation;
drop view ciq_fssit.vwgetrecommendation;
drop view ciq_fssit.vwdocuments;
drop view ciq_fssit.vwstatementofwork;

CREATE TABLE ciq_fssit.tzip_file_summary (
    requestid uuid NOT NULL,
    dafileid  uuid NOT NULL,
    filename  text not null,
    file_id   uuid NOT NULL,
    briefdescription    TEXT NULL,
    consicedescription  TEXT NULL
);

CREATE OR REPLACE VIEW ciq_fssit.vwstatementofwork
AS SELECT requestid,
    projectid,
    dafileid,
    sowfilename,
    summary,
    offer,
    relevance_score,
    offerfamily,
    practice,
    offersimilarity,
    rnk
   FROM ( SELECT sow.requestid,
            sow.projectid,
            sow.dafileid,
            sow.sowfilename,
            sow.summary,
            ofd.offer,
            sow.relevance_score::double precision*100.0 relevance_score,
            ofd.offerfamily,
            ofd.practice,
            similarity(sow.offer, ofd.offer) AS offersimilarity,
            row_number() OVER (PARTITION BY sow.requestid, sow.projectid, sow.dafileid, sow.offer ORDER BY (similarity(sow.offer, ofd.offer)) DESC) AS rnk
           FROM ( SELECT sow_1.requestid,
                    sow_1.projectid,
                    sow_1.dafileid,
                    sow_1.sowfilename,
                    sow_1.summary,
                    TRIM(BOTH FROM offer_json.value ->> 'OfferName'::text) AS offer,
                    (offer_json.value ->> 'Relevance Score'::text) AS relevance_score
                   FROM ciq_fssit.tstatementofwork sow_1
                     CROSS JOIN LATERAL jsonb_array_elements(sow_1.offer) offer_json(value)
                  WHERE jsonb_typeof(sow_1.offer) = 'array'::text) sow
             JOIN ciq_fssit.vwofferfamilydata ofd ON similarity(sow.offer, ofd.offer) > 0.75::double precision
          WHERE sow.relevance_score::double precision > (select val::float from ciq_fssit.config where name='THRESHOLD_SOW_OFFER_SCORE' limit 1)) sqry
  WHERE rnk = 1;


CREATE OR REPLACE VIEW ciq_fssit.vwdocuments
AS 
SELECT sqry.requestid,
	   sqry.daoriginal_fileid,
	   sqry.dafileid,
	   sqry.filename,
	   sqry.title,
	   sqry.description,
	   sqry.author,
	   sqry.dtpm_phase,
	   sqry.document_type,
	   sqry.ip_type,
	   sqry.practice,
	   sqry.offerfamily,
	   sqry.offer,
	   sqry.doc_off_relevance_score,
	   sqry.status,
	   sqry.ipid,
	   sqry.initialgrade,
	   sqry.similarityscore,
	   sqry.priority,
	   sqry.dasanitizationoutfileid,
	   sqry.daclassificationoutfileid,
	   sqry.created_by,
	   sqry.updated_by,
	   sqry.created_date,
	   sqry.updated_date,
	   sqry.offersimilarity,
	   sqry.rnk,
	   sqry.usage_count,
	   reccnt.acceptance*100 acceptance
  FROM (SELECT doc.requestid,
			   doc.daoriginal_fileid,
			   doc.dafileid,
			   doc.filename,
			   doc.title,
			   doc.description,
			   COALESCE(mp.dtpm_phase, doc.dtpm_phase) AS dtpm_phase,
			   doc.document_type,
			   doc.ip_type,
			   ofd.practice,
			   ofd.offerfamily,
			   COALESCE(ofd.offer, doc.offer::text) AS offer,
			   doc.author,
			   doc.relevance_score AS doc_off_relevance_score,
			   doc.usage_count,
			   doc.status,
			   doc.ipid,
			   doc.initialgrade,
			   doc.similarityscore,
			   doc.priority,
			   doc.dasanitizationoutfileid,
			   doc.daclassificationoutfileid,
			   doc.created_by,
			   doc.updated_by,
			   to_char(doc.created_date, 'YYYY-MM-DD HH24:MI:SS'::text) AS created_date,
			   to_char(doc.updated_date, 'YYYY-MM-DD HH24:MI:SS'::text) AS updated_date,
			   similarity(doc.offer::text, ofd.offer) AS offersimilarity,
			   row_number() OVER (PARTITION BY doc.requestid, doc.daoriginal_fileid, doc.offer ORDER BY (similarity(doc.offer::text, ofd.offer)) DESC) AS rnk
          FROM ciq_fssit.tdocument doc
          JOIN (SELECT ofr.practice,
                       ofr.offerfamily,
                       ofr.offer
                  FROM ciq_fssit.vwofferfamilydata ofr
				 UNION ALL
                SELECT 'Unable To Predict'::text,
                       'Unable To Predict'::text,
                       'Unable To Predict'::text) ofd ON similarity(COALESCE(doc.offer, 'Unable To Predict'::character varying)::text, ofd.offer) > 0.75::double precision
     LEFT JOIN ciq_fssit.tdtpm_mapping mp ON TRIM(BOTH FROM lower(doc.ip_type::text)) = TRIM(BOTH FROM lower(mp.ip_type::text))) sqry
LEFT JOIN (SELECT rec.templateid,
				  count(1)/nullif(count(CASE WHEN rec.status::text = 'ACCEPTED'::text THEN 1 ELSE NULL::integer END), 0) AS acceptance
			 FROM ciq_fssit.trecommendation rec
			WHERE rec.status::text <> 'GENERATED'::text
		 GROUP BY rec.templateid) reccnt ON reccnt.templateid=sqry.daoriginal_fileid  
 WHERE rnk = 1;

CREATE OR REPLACE VIEW ciq_fssit.vwgetrefinerecommendation
AS SELECT dafileid,
    ipid,
    lower(COALESCE(dtpm_phase, 'na'::character varying)::text)::character varying(100) AS dtpm_phase,
    concat(COALESCE(lower(offer), ''::text), ' ', COALESCE(lower(offerfamily), ''::text), ' ', COALESCE(lower(practice), ''::text), ' ', COALESCE(lower(ip_type::text), ''::character varying::text), ' ', COALESCE(lower(document_type::text), ''::character varying::text), ' ', COALESCE(lower(dtpm_phase::text), ''::character varying::text), ' ', COALESCE(lower(title::text), ''::character varying::text), ' ', filename, ' ', COALESCE(lower(author::text), ''::character varying::text)) AS metadata
   FROM ciq_fssit.vwdocuments v
  WHERE status::text = 'APPROVED'::text;

CREATE OR REPLACE VIEW ciq_fssit.vwgetrecommendation
AS SELECT sowrequestid,
    projectid,
    sowfileid,
    templateid,
    filename,
    title,
    description,
    dtpm_phase,
    document_type,
    ip_type,
    practice,
    offerfamily,
    offer,
    status,
    ipid,
    initialgrade,
    priority,
    rnk,
    usage_count,
    sow_off_relevance_score,
    similarityscore,
    acceptance
   FROM ( SELECT DISTINCT sow.requestid AS sowrequestid,
            sow.projectid,
            sow.dafileid AS sowfileid,
            doc.dafileid AS templateid,
            doc.filename,
            doc.title,
            doc.description,
            doc.dtpm_phase,
            doc.document_type,
            doc.ip_type,
            doc.practice,
            doc.offerfamily,
            doc.offer,
            doc.status,
            doc.ipid,
            doc.initialgrade,
            doc.priority,
            row_number() OVER (PARTITION BY sow.requestid, doc.dafileid ORDER BY sow.relevance_score DESC) AS rnk,
            doc.usage_count,
            sow.relevance_score sow_off_relevance_score,
            (similarity(sow.summary, doc.description)::double precision)*100.0 AS similarityscore,
            doc.acceptance
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
	   dafileid,
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
	   initialgrade,
	 --  priority,
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
 from
   (select sqry.*
   		  ,case 
			when sqry.method='offerbased' 
				then (sqry.sow_off_relevance_score_scaled * wt.sow_off_relevance_score_wt
					  + sqry.similarityscore_scaled * wt.similarityscore_wt 
					  + sqry.acceptance_scaled * wt.acceptance_wt ) * (0.95 + (0.05*LEAST(sqry.similarityscore_scaled, sqry.acceptance_scaled)) / 100.0) 
			else (sqry.similarityscore_scaled * wt.similarityscore_wt + sqry.acceptance_scaled * wt.acceptance_wt) * (0.95 + (0.05*LEAST(sqry.similarityscore_scaled, sqry.acceptance_scaled)) / 100.0) 
		   end as relevancescore
      from 
		(SELECT rec.requestid,
				rec.projectid,
				rec.templateid,
				rec.ipid,
				rec.dafileid,
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
				doc.initialgrade,
				doc.usage_count,
				rec.sow_off_relevance_score,
				mm.sow_off_relevance_score_min,
				mm.sow_off_relevance_score_max,
				COALESCE((rec.sow_off_relevance_score - mm.sow_off_relevance_score_min) / NULLIF(mm.sow_off_relevance_score_max - mm.sow_off_relevance_score_min, 0), 0)*100 AS sow_off_relevance_score_scaled,
				rec.similarityscore,
				mm.similarityscore_min,
				mm.similarityscore_max,
				COALESCE((rec.similarityscore - mm.similarityscore_min) / NULLIF(mm.similarityscore_max - mm.similarityscore_min, 0), 0.01)*100 AS similarityscore_scaled,
				rec.acceptance,
				mm.acceptance_min,
				mm.acceptance_max,
				COALESCE((rec.acceptance - mm.acceptance_min) / NULLIF(mm.acceptance_max - mm.acceptance_min, 0), 0.01)*100 AS acceptance_scaled,
				rec.method
           FROM ciq_fssit.trecommendation rec
           JOIN ciq_fssit.vwdocuments doc ON rec.templateid = doc.dafileid
           join (SELECT inrec.method,
           			    least(min(inrec.sow_off_relevance_score::numeric),0) AS sow_off_relevance_score_min,
	            		max(inrec.sow_off_relevance_score::numeric) AS sow_off_relevance_score_max,
	            		min(inrec.similarityscore) AS similarityscore_min,
	            		max(inrec.similarityscore) AS similarityscore_max,
	            		min(inrec.acceptance) AS acceptance_min,
	            		max(inrec.acceptance) AS acceptance_max
           		   FROM ciq_fssit.trecommendation inrec
           		 group by inrec.method) mm on rec.method=mm.method) sqry
join (select comment method,
			 max(case when name ='sow_off_relevance_score_wt' then val::float end) sow_off_relevance_score_wt,
	   		 max(case when name ='similarityscore_wt' then val::float end) similarityscore_wt,
	   	 	 max(case when name ='acceptance_wt' then val::float end) acceptance_wt
  		from ciq_fssit.config 
 	   where name in ('sow_off_relevance_score_wt','similarityscore_wt','acceptance_wt')
 	group by comment) wt on wt.method=case when sqry.method<>'offerbased' then 'search' else sqry.method end)sqry2;
 
    
            
DROP FUNCTION ciq_fssit.fngeneraterecommendation(uuid, bool);

CREATE OR REPLACE FUNCTION ciq_fssit.fngeneraterecommendation(p_sowrequestid uuid, is_refine_recommendation boolean DEFAULT false)
 RETURNS TABLE(requestid text, 
 			   projectid character varying, 
 			   templateid text, 
 			   ipid text, 
 			   dafileid text, 
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
-- 			   similarityscore double precision, 
 			   relevancescore numeric,
			   usage_count integer, 
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
	      COALESCE(rec.requestid::text,'')        requestid,
	      COALESCE(rec.projectid,'')            projectid,
	      COALESCE(rec.templateid::text,'')       templateid,
	      COALESCE(rec.ipid::text,'')           ipid,
	      COALESCE(rec.dafileid::text,'')          dafileid,
	      COALESCE(rec.filename,'')             filename,
	      COALESCE(rec.title,'')               title,
	      COALESCE(replace(
	            substr(rec.description,
	               length('<b>Description</b>:<br>')+1,
	               abs(strpos(rec.description,'<b>Key points</b>')
	                -length('<b>Description</b>:<br>')-1)),'<br>',' '),'')            description,
	      COALESCE(rec.practice,'')            practice,
	      COALESCE(rec.offerfamily,'')          offerfamily,
	      COALESCE(rec.offer,'')              offer,
	      COALESCE(rec.status,'')              status,
	      COALESCE(rec.documenttype,'')            "source",
	      COALESCE(rec.dtpmphase,'')              phase,
	      COALESCE(rec.iptype,'')             iptype,
--	      COALESCE(rec.similarityscore,0)::float  similarityscore,        -- MOVED UP
	      COALESCE(rec.relevancescore::numeric, 0)    relevancescore,         -- MOVED DOWN
	      COALESCE(rec.usage_count,0)           usage_count,
		  COALESCE(rec.relevance,'')            relevance,
	      COALESCE(rec.eps_section,'')           eps_section
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