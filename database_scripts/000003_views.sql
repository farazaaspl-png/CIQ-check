CREATE OR REPLACE VIEW ciq_fssit.vwdtpmmapping
AS SELECT ip_type,
    dtpm_phase
   FROM ciq_fssit.tdtpm_mapping;

CREATE OR REPLACE VIEW ciq_fssit.vwstatementofwork
AS SELECT requestid,
    projectid,
    dafileid,
    sowfilename,
    summary,
    offer,
    confidence_score,
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
            sow.confidence_score,
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
                    offer_json.value ->> 'Relevance Score'::text AS confidence_score
                   FROM ciq_fssit.tstatementofwork sow_1
                     CROSS JOIN LATERAL jsonb_array_elements(sow_1.offer) offer_json(value)
                  WHERE jsonb_typeof(sow_1.offer) = 'array'::text) sow
             JOIN ciq_fssit.vwofferfamilydata ofd ON similarity(sow.offer, ofd.offer) > 0.75::double precision
          WHERE sow.confidence_score::double precision > 0.6::double precision) sqry
  WHERE rnk = 1;
 
CREATE OR REPLACE VIEW ciq_fssit.vw_unredacted_items
AS SELECT t.fileid,
    t.filename,
    t.sensitivetext AS unredacted_text
   FROM ciq_fssit.ttoberedacted t
     LEFT JOIN ciq_fssit.tredacted r ON t.sensitivetext = r.sensitivetext
  WHERE r.sensitivetext IS NULL;
  
 -- ciq_fssit.vwdocuments source

CREATE OR REPLACE VIEW ciq_fssit.vwdocuments
AS SELECT requestid,
    daoriginal_fileid,
    dafileid,
    filename,
    title,
    description,
    dtpm_phase,
    document_type,
    ip_type,
    practice,
    offerfamily,
    offer,
    confidence_score,
    usage_count,
    status,
    ipid,
    initialgrade,
    similarityscore,
    priority,
    dasanitizationoutfileid,
    daclassificationoutfileid,
    created_by,
    updated_by,
    created_date,
    updated_date,
    offersimilarity,
    rnk,
    author
   FROM ( SELECT doc.requestid,
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
            doc.relevance_score AS confidence_score,
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
             JOIN ( SELECT vwofferfamilydata.practice,
                    vwofferfamilydata.offerfamily,
                    vwofferfamilydata.offer
                   FROM ciq_fssit.vwofferfamilydata
                UNION ALL
                 SELECT 'Unable To Predict'::text,
                    'Unable To Predict'::text,
                    'Unable To Predict'::text) ofd ON similarity(COALESCE(doc.offer, 'Unable To Predict'::character varying)::text, ofd.offer) > 0.75::double precision
             LEFT JOIN ciq_fssit.tdtpm_mapping mp ON TRIM(BOTH FROM lower(doc.ip_type::text)) = TRIM(BOTH FROM lower(mp.ip_type::text))) sqry
  WHERE rnk = 1;
  
 -- ciq_fssit.vwclassificationout source

CREATE OR REPLACE VIEW ciq_fssit.vwclassificationout
AS SELECT requestid,
    daoriginal_fileid,
    dafileid,
    COALESCE(filename, ''::character varying) AS filename,
    COALESCE(title, ''::character varying) AS title,
    COALESCE(description, ''::text) AS description,
    COALESCE(dtpm_phase, ''::character varying) AS dtpm_phase,
    COALESCE(document_type, ''::character varying) AS document_type,
    ip_type,
    COALESCE(practice, ''::text) AS practice,
    COALESCE(offerfamily, ''::text) AS offerfamily,
    COALESCE(offer, ''::text) AS offer,
    COALESCE(initialgrade, ''::character varying) AS initialgrade,
    COALESCE(similarityscore, 0.0::double precision) AS similarityscore,
    COALESCE(priority, ''::character varying) AS priority,
    COALESCE(gtl_email, ''::character varying) AS gtl_email,
    dasanitizationoutfileid,
    daclassificationoutfileid,
    'test'::character varying AS author
   FROM ( SELECT doc.requestid,
            doc.daoriginal_fileid,
            doc.dafileid,
            doc.filename,
            doc.title,
            doc.description,
            COALESCE(mp.dtpm_phase, doc.dtpm_phase) AS dtpm_phase,
            doc.document_type,
            string_to_array(doc.ip_type::text, '|'::text) AS ip_type,
            ofd.practice,
            ofd.offerfamily,
            COALESCE(ofd.offer, doc.offer::text) AS offer,
            doc.relevance_score AS confidence_score,
            doc.usage_count,
            doc.status,
            doc.ipid,
            doc.initialgrade,
            doc.similarityscore,
            doc.priority,
            doc.dasanitizationoutfileid,
            doc.daclassificationoutfileid,
            ofd.gtl AS gtl_email,
            similarity(doc.offer::text, ofd.offer) AS offersimilarity,
            row_number() OVER (PARTITION BY doc.requestid, doc.daoriginal_fileid, doc.offer ORDER BY (similarity(doc.offer::text, ofd.offer)) DESC) AS rnk
           FROM ciq_fssit.tdocument doc
             LEFT JOIN ciq_fssit.vwofferfamilydata ofd ON similarity(doc.offer::text, ofd.offer) > 0.50::double precision
             LEFT JOIN ciq_fssit.tdtpm_mapping mp ON (TRIM(BOTH FROM lower(mp.ip_type::text)) IN ( SELECT TRIM(BOTH FROM lower(unnest(string_to_array(doc.ip_type::text, '|'::text)))) AS btrim))) sqry
  WHERE rnk = 1;
  
 -- ciq_fssit.vwgetrefinerecommendation source

CREATE OR REPLACE VIEW ciq_fssit.vwgetrefinerecommendation
AS SELECT dafileid,
    ipid,
    lower(COALESCE(dtpm_phase, 'na'::character varying)::text)::character varying(100) AS dtpm_phase,
    concat(COALESCE(lower(offer), ''::text), ' ', COALESCE(lower(offerfamily), ''::text), ' ', COALESCE(lower(practice), ''::text), ' ', COALESCE(lower(ip_type::text), ''::character varying::text), ' ', COALESCE(lower(document_type::text), ''::character varying::text), ' ', COALESCE(lower(dtpm_phase::text), ''::character varying::text), ' ', COALESCE(lower(title::text), ''::character varying::text), ' ', filename, ' ', COALESCE(lower(author::text), ''::character varying::text)) AS metadata
   FROM ciq_fssit.vwdocuments v
  WHERE status::text = 'APPROVED'::text;