from airflow.providers.postgres.hooks.postgres import PostgresHook

from top_key_skills import config
from top_key_skills.config import logger

def get_top_key_skills(vacancies_schema, telecom_companies_schema):
    import pandas as pd
    
    pg_hook = PostgresHook(postgres_conn_id=config.postgres_conn_id)
    engine = pg_hook.get_sqlalchemy_engine()
    
    vac_query = f"""
    SELECT
        v.id,
        v.employer,
        s.skill
    FROM
        {vacancies_schema}.{config.vacancies_table} v
        JOIN {vacancies_schema}.{config.vacancies_key_skills_table} vs
            ON v.id = vs.vacancy_id
        JOIN {vacancies_schema}.{config.key_skills_table} s
            ON vs.key_skill_id = s.id
    """
    vacancies = pd.read_sql(vac_query, con=engine)
    vacancies_clean = vacancies.copy()
    vacancies['employer'] = vacancies['employer'].str.lower()
    
    tc_query = f"""
    SELECT *
    FROM {telecom_companies_schema}.{config.egrul_table}
    """
    telecom_companies = pd.read_sql(tc_query, con=engine)
    telecom_companies['name'] = telecom_companies['name'].str.lower()
    telecom_companies['name'] = telecom_companies['name'].str.split('"').str[1]
    telecom_companies = telecom_companies.rename(
        columns={'name': 'employer'},
        )
    
    df = vacancies.merge(telecom_companies, how='inner', on='employer')
    vacancies = vacancies_clean[vacancies_clean['id'].isin(df['id'].unique())]
    vacancies_unique = vacancies[['id', 'employer']].drop_duplicates()
    print(f'Найдено вакансий middle python dev в телеком-компаниях: {vacancies_unique.shape[0]}')
    if not vacancies.empty:
        print(vacancies_unique)
        result = vacancies['skill'].value_counts().reset_index().head(10)
        print(f'Топ-10 самых востребованных ключевых навыков : {result}')
    else:
        print('Вакансий не найдено, топ навыков не составить')