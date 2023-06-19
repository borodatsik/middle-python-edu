import os
from sqlalchemy.engine.url import make_url

# Параметры базы данных

# Строка подключения в виде
# postgresql+psycopg2://username:password@host:port/database
# сохранена в качестве переменной среды
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
    
schema = 'hw1'
okved_table = 'okved'

# Параметры файловой системы
bulk_data_dir = 'bulk_data'
okved_filename = 'okved_2.json.zip'
egrul_filename = 'egrul.json.zip'

okved_filepath = os.path.join(bulk_data_dir, okved_filename)
egrul_filepath = os.path.join(bulk_data_dir, egrul_filename)
