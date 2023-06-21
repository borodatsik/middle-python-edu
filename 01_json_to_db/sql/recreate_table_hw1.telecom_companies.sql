-- DROP TABLE IF EXISTS hw1.telecom_companies_okved_codes;	
DROP TABLE IF EXISTS hw1.telecom_companies;

CREATE TABLE hw1.telecom_companies (
	ogrn BIGINT PRIMARY KEY,
	inn BIGINT,
	kpp	BIGINT,
	name VARCHAR,
	okved_code VARCHAR,
	source_filename VARCHAR,
	load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);