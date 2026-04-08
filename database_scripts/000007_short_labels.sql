CREATE TABLE ciq_fssit.tlabel_lookup (         
    category varchar(255) NOT NULL UNIQUE, 
    label varchar(100)              
);

insert into ciq_fssit.config (name,val,comment,isactive) values('SHORT_LABEL_VIEW','vwtoberedacted','database objects',True);
INSERT INTO ciq_fssit.tlabel_lookup (category, "label") VALUES('vendor organisation name', 'Vendor');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")  VALUES('source code snippets', 'Snippet');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label") VALUES('vendor person name', 'Vendor');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('http_url', 'URL');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('driver license number or dl', 'DL');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('customer person name', 'Customer');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('internal project name', 'Internal');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('network device name', 'Device');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('port number', 'Port');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('customer email address', 'Customer');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('linux/windows command', 'Command');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('internal_email', 'Email');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('api key/authentication token', 'Token');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('customer organisation name', 'Customer');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('domain name', 'Domain');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('ssh credential', 'SSH Creds');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('passphrase', 'Password');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('email_address', 'Email');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('active directory site', 'AD Site');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('password policy', 'Policy');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('customer email addresses', 'Email');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('ssl/tls certificate', 'Certificate');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('incident number', 'Inc Number');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('internal person name', 'Internal');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('service request number', 'SR Number');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('authentication credential', 'Creadential');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('service tag number', 'Tag Number');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('internal email address', 'Email');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('application name', 'App Name');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('auth_token', 'Token');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('phone numbers', 'phone number');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('ip_address', 'ip address');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('email address', 'Email');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('authentication token', 'Token');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('active directory domain name', 'AD Domain');
INSERT INTO ciq_fssit.tlabel_lookup(category, "label")VALUES('active directory forest name', 'AD Forest');

CREATE OR REPLACE VIEW ciq_fssit.vwtoberedacted
AS SELECT t.id,
    t.requestid,
    t.fuuid,
    t.dafileid,
    t.filename,
    COALESCE(tl.label, t.category) AS category,
    t.category AS original_category,
    tl.label,
    t.sensitivetext
   FROM ciq_fssit.ttoberedacted t
     LEFT JOIN ciq_fssit.tlabel_lookup tl ON lower(t.category)::text = lower(tl.category::text);

-- thumbs up thumbs down 
CREATE TABLE ciq_fssit.tconsultantfeedback (
    id serial4 NOT NULL,
    requestid uuid NOT NULL,
	dafileid uuid NULL,
    feedback VARCHAR(50) NOT NULL CHECK (feedback IN ('positive', 'negative')),
    message VARCHAR(255) NOT NULL,
    usercomments TEXT,
    created_date TIMESTAMP WITH TIME ZONE DEFAULT now()
);

insert into ciq_fssit.config (name,val,comment,isactive) values('CONSULTANT_FEEDBACK_TABLE','tconsultantfeedback','database objects',True);