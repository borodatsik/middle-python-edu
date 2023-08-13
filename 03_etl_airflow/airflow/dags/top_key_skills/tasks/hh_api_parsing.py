from airflow.providers.postgres.hooks.postgres import PostgresHook

from top_key_skills import config
from top_key_skills.config import logger

def get_vacancies_by_page(page, params=config.url_params):
    """Получение списка вакансий из страницы № page"""
    import requests
    import pandas as pd
    import json
    
    params = params.copy()
    params['page'] = page
    response = requests.get(
        config.base_api_url,
        headers=config.headers,
        params=params,
        )
    
    if response.status_code != 200:
        raise Exception(f'Не удалось получить список вакансий - "{response.url}"')
    
    data = json.loads(response.text)
    vacancies_by_page = pd.DataFrame(data['items'])
    
    return vacancies_by_page

def get_vacancies():
    """Получение списка вакансий"""
    import pandas as pd
    
    vacancies = pd.DataFrame()
    page = 0
    while True:
        vacancies_by_page = get_vacancies_by_page(page)
        if len(vacancies_by_page) > 0:
            temp = pd.DataFrame(vacancies_by_page)
            vacancies = pd.concat([vacancies, temp])
            page += 1
        else:
            break
            
    vacancies['city'] = vacancies['area'].str['name']
    vacancies['employer'] = vacancies['employer'].str['name']
    vacancies['position'] = vacancies['name']
    
    return vacancies[['id', 'position', 'employer', 'city', 'url']]

def get_vacancy_addnl_info(vacancy_url):
    """Получение дополнительной информации по вакансии"""
    import requests
    import json
    
    response = requests.get(
        vacancy_url,
        headers=config.headers,
        )
    
    if response.status_code != 200:
        raise Exception(f'response.status_code = {response.status_code} - Не удалось получить дополнительную информацию по вакансии "{vacancy_url}". response.text = {response.text}')
    
    data = json.loads(response.text)
    description = data['description']

    key_skills_dict = data.get('key_skills')
    if key_skills_dict:
        key_skills = '|'.join([skill['name'] for skill in key_skills_dict])
    else:
        key_skills = None
    
    return description, key_skills

def get_vacancies_addnl_info(vacancies, cooldown_timeout=60):
    """Получение дополнительной информации по вакансиям"""
    import time
    
    vacancy_addnl_info_columns = ['description', 'key_skills']
    for i, vacancy_tuple in enumerate(vacancies.iterrows()):
        _, vacancy = vacancy_tuple
        
        if i % 119 == 0 and i > 0:
            logger.info(f'Превышен лимит запросов к API, ожидание {cooldown_timeout} с')
            time.sleep(cooldown_timeout)
        current_vacancy_mask = vacancies['id'] == vacancy['id']
        description, key_skills = get_vacancy_addnl_info(vacancy['url'])
        vacancies.loc[current_vacancy_mask, vacancy_addnl_info_columns] = description, key_skills

    vacancies['key_skills'] = vacancies['key_skills'].str.split('|')
    
    return vacancies[['id', 'position', 'employer', 'city', 'description', 'key_skills']]

def transform_vacancies_data(vacancies):
    """Трансформация данных по вакансиям"""
    vacancies = vacancies.dropna()
    vacancies = vacancies.head(config.vacancies_limit)
    return vacancies
  
def load_vacancies(vacancies_src, schema):
    """Загрузка данных по вакансиям в базу данных"""
    vacancies = vacancies_src.copy()
    vacancies = vacancies.drop(columns=['key_skills'])
    pg_hook = PostgresHook(postgres_conn_id=config.postgres_conn_id)
    engine = pg_hook.get_sqlalchemy_engine()
    
    vacancies.to_sql(config.vacancies_table, schema=schema, con=engine, if_exists='append', index=False)

def refresh_key_skills(vacancies, schema):
    """Обновление данных по ключевым скиллам в связанных таблицах"""
    import pandas as pd
    
    pg_hook = PostgresHook(postgres_conn_id=config.postgres_conn_id)
    engine = pg_hook.get_sqlalchemy_engine()
    
    vacancies_key_skills = vacancies.explode(['key_skills'])[['id', 'key_skills']]
    vacancies_key_skills.columns = ['vacancy_id', 'skill']
    vacancies_key_skills = vacancies_key_skills.dropna()
    vacancies_key_skills['skill'] = vacancies_key_skills['skill'].str.lower()
    vacancies_key_skills = vacancies_key_skills.drop_duplicates()
    
    pg_hook.insert_rows(
        table=f'{schema}.{config.key_skills_table}',
        rows=list(
            vacancies_key_skills[['skill']].itertuples(index=False)
            ),
        target_fields=['skill'],
        replace=True,
        replace_index=['skill'],
        )
    
    key_skills = pd.read_sql(f'SELECT id AS key_skill_id, skill FROM {schema}.{config.key_skills_table}', con=engine)
    vacancies_key_skills = vacancies_key_skills.merge(key_skills, how='left', on='skill')
    vacancies_key_skills = vacancies_key_skills[['vacancy_id', 'key_skill_id']]
    
    pg_hook.insert_rows(
        table=f'{schema}.{config.vacancies_key_skills_table}',
        rows=list(
            vacancies_key_skills.itertuples(index=False)
            ),
        target_fields=list(vacancies_key_skills.columns),
        replace=True,
        replace_index=list(vacancies_key_skills.columns),
        )
        
def upload_hh(schema):
    """Task - получение и загрузка вакансий в базу данных"""
    logger.info('Получение основной информации о вакансиях')
    vacancies = get_vacancies()
    
    logger.info('Получение дополнительной информации о вакансиях')
    vacancies = get_vacancies_addnl_info(vacancies)
    
    logger.info('Обработка данных по вакансиям')
    vacancies = transform_vacancies_data(vacancies)
    
    logger.info('Загрузка вакансий в базу данных')
    load_vacancies(vacancies, schema)
    
    logger.info('Загрузка ключевых навыков из вакансий в базу данных')
    refresh_key_skills(vacancies, schema)
    
    logger.info('Загрузка данных по вакансиям завершена')