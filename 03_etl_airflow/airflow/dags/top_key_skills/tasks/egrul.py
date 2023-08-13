from airflow.providers.postgres.hooks.postgres import PostgresHook

from top_key_skills import config
from top_key_skills.config import logger

def get_filelist(zip_path, max_files_count=None):
    """Получение списка файлов в архиве
    
    Аргументы
    ----------
    zip_path: str
        Путь к zip-файлу.
    max_files_count: int, default: None
        Количество читаемых файлов (для тестирования).
    """
    from zipfile import ZipFile
    
    with ZipFile(zip_path, 'r') as zip_archive:
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
            
def get_egrul_data_by_file(egrul_filepath, filename, okved_primary_code, dtypes):
    """Функция для получения данных ЕГРЮЛ из одного JSON-файла.
    
    Аргументы
    ----------
    filename: str
        Название JSON-файла в архиве.
    okved_primary_code: str | int
        Фильтруемый код ОКВЭД.
    dtypes: dict
        Словарь-маппинг типов данных.
    """
    from zipfile import ZipFile
    import pandas as pd
    from numpy import NaN
    
    with ZipFile(egrul_filepath, 'r') as zip_archive:
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
            egrul = egrul.replace({'': NaN})
            egrul = egrul.dropna(how='any')
            return egrul
            
def upload_egrul(schema, egrul_filepath):
    """Task - загрузка данных ЕГРЮЛ в базу данных"""
    filelist = get_filelist(egrul_filepath)
    for filename in filelist:
        logger.info(f'Чтение файла {filename} из архива {egrul_filepath}')
        egrul = get_egrul_data_by_file(egrul_filepath, filename, config.okved_primary_code, config.egrul_dtypes)
        if not egrul.empty:
            pg_hook = PostgresHook(postgres_conn_id=config.postgres_conn_id)
            engine = pg_hook.get_sqlalchemy_engine()    
            egrul.to_sql(config.egrul_table, schema=schema, con=engine, if_exists='append', index=False)
            logger.info(f'Залиты данные в {schema}.{config.egrul_table}, кол-во строк: {egrul.shape[0]}')
        else:
            logger.info(f'В файле {filename} нет компаний с номером ОКВЭД {config.okved_primary_code}, пропускаю')