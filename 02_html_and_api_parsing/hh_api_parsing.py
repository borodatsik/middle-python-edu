import requests
import pandas as pd
import json
import time

import config
import psql
from helpers import time_decorator, create_db_schema

db = psql.PsqlConnector()
schema = config.api_schema

def get_vacancies_by_page(page, params=config.url_params):
    """Получение списка вакансий из страницы № page"""
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

@time_decorator
def get_vacancies():
    """Получение списка вакансий"""
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
    response = requests.get(
        vacancy_url,
        headers=config.headers,
        )
    
    if response.status_code != 200:
        raise Exception(f'{response.status_code = } - Не удалось получить дополнительную информацию по вакансии "{vacancy_url}". {response.text = }')
    
    data = json.loads(response.text)
    description = data['description']

    key_skills_dict = data.get('key_skills')
    if key_skills_dict:
        key_skills = '|'.join([skill['name'] for skill in key_skills_dict])
    else:
        key_skills = None
    
    return description, key_skills

@time_decorator
def get_vacancies_addnl_info(vacancies, cooldown_timeout=60):
    """Получение дополнительной информации по вакансиям"""
    vacancy_addnl_info_columns = ['description', 'key_skills']
    for i, vacancy_tuple in enumerate(vacancies.iterrows()):
        _, vacancy = vacancy_tuple
        
        if i % 119 == 0 and i > 0:
            time.sleep(cooldown_timeout)
        current_vacancy_mask = vacancies['id'] == vacancy['id']
        description, key_skills = get_vacancy_addnl_info(vacancy['url'])
        vacancies.loc[current_vacancy_mask, vacancy_addnl_info_columns] = description, key_skills

    vacancies['key_skills'] = vacancies['key_skills'].str.split('|')
    
    return vacancies[['id', 'position', 'employer', 'city', 'description', 'key_skills']]

@time_decorator
def transform_vacancies_data(vacancies):
    """Трансформация данных по вакансиям"""
    vacancies = vacancies.dropna()
    vacancies = vacancies.head(config.vacancies_limit)
    return vacancies

@time_decorator    
def load_vacancies(vacancies):
    """Загрузка данных по вакансиям в базу данных"""
    db.insert_values(
        vacancies.drop(columns=['key_skills']),
        schema,
        config.vacancies_table,
        on_conflict_clause='ON CONFLICT DO NOTHING',
        )
    
@time_decorator
def refresh_key_skills(vacancies):
    """Обновление данных по ключевым скиллам в связанных таблицах"""
    vacancies_key_skills = vacancies.explode(['key_skills'])[['id', 'key_skills']]
    vacancies_key_skills.columns = ['vacancy_id', 'skill']
    vacancies_key_skills = vacancies_key_skills.dropna()
    vacancies_key_skills['skill'] = vacancies_key_skills['skill'].str.lower()
    vacancies_key_skills = vacancies_key_skills.drop_duplicates()
    db.insert_values(
        vacancies_key_skills[['skill']],
        schema,
        config.key_skills_table,
        on_conflict_clause='ON CONFLICT (skill) DO NOTHING',
        )
    key_skills = db.read_query(f'SELECT id AS key_skill_id, skill FROM {schema}.{config.key_skills_table}')
    vacancies_key_skills = vacancies_key_skills.merge(key_skills, how='left', on='skill')
    vacancies_key_skills = vacancies_key_skills[['vacancy_id', 'key_skill_id']]
    db.insert_values(
        vacancies_key_skills,
        schema,
        config.vacancies_key_skills_table,
        on_conflict_clause='ON CONFLICT DO NOTHING',
        )
    
@time_decorator
def app():
    """Запуск скрипта hh_api_parsing.py"""
    create_db_schema(schema)
    vacancies = get_vacancies()
    vacancies = get_vacancies_addnl_info(vacancies)
    vacancies = transform_vacancies_data(vacancies)
    load_vacancies(vacancies)
    refresh_key_skills(vacancies)
    
if __name__ == "__main__":
    app()