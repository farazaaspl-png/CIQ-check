INSERT INTO ciq_fssit.config (id, "name", val, "comment", isactive, inserted_on, updated_on) VALUES(148, 'VECTOR_SEARCH_SIMILARITY_THRESHOLD', '0.7', 'Threshold for Template Identification', true, '2026-03-26 18:13:33.411', NULL);
INSERT INTO ciq_fssit.config (id, "name", val, "comment", isactive, inserted_on, updated_on) VALUES(149, 'FUZZ_RATIO_SIMILARITY_THRESHOLD', '70', 'Threshold for Template Identification', true, '2026-03-26 18:16:33.445', NULL);
INSERT INTO ciq_fssit.config (id, "name", val, "comment", isactive, inserted_on, updated_on)  VALUES(146, 'GRADING_TABLE', 'tgrading', 'database objects', true, '2026-03-18 09:43:42.611', NULL);
ALTER TABLE ciq_fssit.tchange_document ALTER COLUMN newvalue TYPE text;
ALTER TABLE ciq_fssit.tchange_document ALTER COLUMN oldvalue TYPE text;
ALTER TABLE ciq_fssit.tprocess_ips_rule ADD COLUMN similarity bool DEFAULT true null;
ALTER TABLE ciq_fssit.tprocess_ips_rule ADD COLUMN grading bool DEFAULT true null;
drop view ciq_fssit.vwgetrefinerecommendation;
DELETE FROM ciq_fssit.config WHERE name='REFINE_RECOMMENDATION_VIEW';

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
