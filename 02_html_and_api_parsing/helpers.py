import pandas as pd
import psql

db = psql.PsqlConnector()

def time_decorator(func):
    """Декоратор для отображения времени исполнения функции"""
    def decorator(*args, **kwargs):
        start_dttm = pd.to_datetime('today')
        line_separator = '\n'
        print(f'{start_dttm} - {func.__name__} - {func.__doc__.split(line_separator)[0]}')
        result = func(*args, **kwargs)
        end_dttm = pd.to_datetime('today')
        diff = end_dttm - start_dttm
        print(f'{end_dttm} - {func.__name__} - время выполнения: {round(diff.total_seconds(), 2)} с')
        return result
    return decorator

@time_decorator
def create_db_schema(schema):
    """Создание структура базы данных"""
    db.execute_sql('sql/recreate_tables.sql', schema=schema)