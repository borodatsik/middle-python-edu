import os
from sqlalchemy.engine.url import make_url

# Параметры базы данных
### Строка подключения в виде
### postgresql+psycopg2://username:password@host:port/database
### сохранена в качестве переменной среды
db_uri_env_name = "MIDDLE_PYTHON_EDU_DB_SQLALCHEMY_CONN"
DB_URI = os.getenv(db_uri_env_name)
if not DB_URI:
    raise ImportError(
        f"Необходимо добавить переменную среды '{db_uri_env_name}' "
        "со значением вида 'postgresql+psycopg2://username:password@host:port/database', "
        "где username, password, host, port, database - параметры подключения к БД."
        )
db_dict = make_url(DB_URI)
DB_CREDENTIALS = dict(
    host=db_dict.host,
    port=db_dict.port,
    dbname=db_dict.database,
    user=db_dict.username,
    password=db_dict.password,
    )
    
html_schema = 'hw2_html_parsing'
api_schema = 'hw2_api_parsing'

vacancies_table = 'vacancies'
key_skills_table = 'key_skills'
vacancies_key_skills_table = 'vacancies_key_skills'

base_url = r'https://hh.ru/search/vacancy'
base_api_url = r'https://api.hh.ru/vacancies'

headers = {'User-agent': 'Mozilla/5.0'}
url_params = {
    'text': 'middle python developer',
    'search_field': 'name',
    }

vacancies_limit = 100