DROP SCHEMA IF EXISTS {{ params.schema }} CASCADE;
CREATE SCHEMA {{ params.schema }};

CREATE TABLE {{ params.schema }}.telecom_companies (
	ogrn BIGINT PRIMARY KEY,
	inn BIGINT,
	kpp	BIGINT,
	name VARCHAR,
	okved_code VARCHAR,
	source_filename VARCHAR,
	load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

CREATE TABLE {{ params.schema }}.vacancies (
	id BIGINT PRIMARY KEY,
	employer VARCHAR,
	position VARCHAR,
	city VARCHAR,
	description VARCHAR,
	load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

CREATE TABLE {{ params.schema }}.key_skills (
	id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
	skill VARCHAR UNIQUE,
	load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

CREATE TABLE {{ params.schema }}.vacancies_key_skills (
	id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
	vacancy_id BIGINT
		REFERENCES {{ params.schema }}.vacancies(id)
			ON DELETE CASCADE,
	key_skill_id BIGINT
		REFERENCES {{ params.schema }}.key_skills(id)
			ON DELETE RESTRICT,
	load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	UNIQUE(vacancy_id, key_skill_id)
	);