import pandas as pd
import zipfile
from joblib import Parallel, delayed

import config
import psql

db = psql.PsqlConnector()

def time_decorator(func):
    """Декоратор для отображения времени исполнения функции"""
    def decorator(*args, **kwargs):
        start_dttm = pd.to_datetime('today')
        result = func(*args, **kwargs)
        end_dttm = pd.to_datetime('today')
        diff = end_dttm - start_dttm
        print(f'{func.__name__} - время выполнения: {round(diff.total_seconds(), 2)} с')
        return result
    return decorator

def create_db_schema():
    """Создание структура базы данных"""
    db.execute_sql('sql/create_schema_hw1.sql')
    db.execute_sql('sql/recreate_table_hw1.okved.sql')
    db.execute_sql('sql/recreate_table_hw1.telecom_companies.sql')

@time_decorator
def upload_okved():
    """1 задание домашней работы - загрузка данных ОКВЭД"""
    okved = pd.read_json(config.okved_filepath, compression='zip')
    db.insert_values(okved, schema=config.schema, table=config.okved_table)
    
def get_filelist(zip_path, max_files_count=None):
    """Получение списка файлов в архиве
    
    Аргументы
    ----------
    zip_path: str
        Путь к zip-файлу.
    max_files_count: int, default: None
        Количество читаемых файлов (для тестирования).
    """
    with zipfile.ZipFile(zip_path, 'r') as zip_archive:
        filelist = zip_archive.namelist()
    if max_files_count:
        return filelist[:max_files_count]
    else:
        return filelist

def get_okved_code(item):
    """Получение кода ОКВЭД в данных ЕГРЮЛ.
    
    Аргументы
    ----------
    item: dict
        Словарь, ячейка данных в датафрейме.
    """
    if item.get('СвОКВЭД'):
        if item['СвОКВЭД'].get('СвОКВЭДОсн'):
            return item['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД']
        
def upload_egrul_job(filename, okved_primary_code, dtypes):
    """Функция для загрузки данных ЕГРЮЛ из одного JSON-файла.
    
    Аргументы
    ----------
    filename: str
        Название JSON-файла в архиве.
    okved_primary_code: str | int
        Фильтруемый код ОКВЭД.
    dtypes: dict
        Словарь-маппинг типов данных.
    """
    with zipfile.ZipFile(config.egrul_filepath, 'r') as zip_archive:
        with zip_archive.open(filename) as f:
            egrul = pd.read_json(f, dtype=dtypes)
            egrul['okved_code'] = egrul['data'].map(get_okved_code)
            egrul = egrul[
                (egrul['okved_code'].str.startswith(f'{okved_primary_code}.', na=False))
                | (egrul['okved_code'] == str(okved_primary_code))
                ]
            egrul['source_filename'] = filename
            egrul = egrul[[
                'ogrn',
                'inn',
                'kpp',
                'name',
                'okved_code',
                'source_filename',
                ]]
            db.insert_values(egrul, schema=config.schema, table=config.egrul_table)

@time_decorator
def upload_egrul():
    """2 задание домашней работы - загрузка данных ЕГРЮЛ"""
    filelist = get_filelist(config.egrul_filepath)
    Parallel(n_jobs=-1)(delayed(upload_egrul_job)(
        filename, config.okved_primary_code, config.egrul_dtypes) for filename in filelist)

def app():
    """Функция для запуска скрипта"""
    create_db_schema()
    upload_okved()
    upload_egrul()
    
if __name__ == "__main__":
    app()
