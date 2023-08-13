import os
import logging

logger = logging.getLogger(__name__)

# Параметры базы данных
postgres_conn_id = 'datsik_conn'

test_schema = 'hw3_test'
test_schema_hw1 = 'hw1'
preprod_schema = 'hw3_preprod'
prod_schema = 'hw3_prod'

egrul_table = 'telecom_companies'
vacancies_table = 'vacancies'
key_skills_table = 'key_skills'
vacancies_key_skills_table = 'vacancies_key_skills'

# Параметры файловой системы
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
script_dir = os.path.join(AIRFLOW_HOME, 'dags/top_key_skills')
bulk_data_dir = os.path.join(AIRFLOW_HOME, 'bulk_data')

egrul_download_test_filepath = os.path.join(bulk_data_dir, 'egrul.json.zip')
egrul_download_prod_filepath = os.path.join(bulk_data_dir, 'egrul_full_download.json.zip')
egrul_test_filepath = os.path.join(bulk_data_dir, 'egrul_test.json.zip')
egrul_preprod_filepath = os.path.join(bulk_data_dir, 'egrul_full.json.zip')

create_tables_sql_path = 'sql/recreate_schema_n_tables.sql'

# Параметры скрипта - ЕГРЮЛ
egrul_test_url = 'https://ofdata.ru/open-data/download/okved_2.json.zip'
egrul_prod_url = 'https://ofdata.ru/open-data/download/egrul.json.zip'
okved_primary_code = '61'
egrul_dtypes = {
    'ogrn': 'int64',
    'inn': 'int64',
    'kpp': 'int64',
    }
    
# Параметры скрипта - Headhunter API
base_api_url = 'https://api.hh.ru/vacancies'

headers = {'User-agent': 'Mozilla/5.0'}
url_params = {
    'text': 'middle python developer',
    'search_field': 'name',
    }

vacancies_limit = 100
