import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine

import pandas as pd
from numpy import NaN

import config

class HW1_db():
    """Класс для работы с базой данных 'hw1'"""
    
    # -------- Initialize ---------------------------------------
    def __init__(self):
        self.db_credentials = config.DB_CREDENTIALS
        self.db_uri = config.DB_URI
    
    def replace_nans(self, df):
        """Замена пустот на None, под формат баз данных PostgreSQL.

        Аргументы
        ----------
        df : pandas.DataFrame
            Датафрейм для заливки в БД.

        Возвращается
        ----------
        df : pandas.DataFrame
            Датафрейм для заливки в БД с "отформатированными" пустотами.
        """
        df = df.where(pd.notna(df), None)
        df = df.replace({pd.NaT: None})
        df = df.replace({NaN: None})
        
        return df

    def connect(self):
        """Создание подключения к базе данных PostgreSQL."""
        conn = None
        try:
            if isinstance(self.db_credentials, str):
                conn = psycopg2.connect(self.db_credentials)
            else:
                conn = psycopg2.connect(**self.db_credentials)
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
            raise error
        return conn
            
    def insert_values(self, df, schema, table, on_conflict_clause=None):
        """Обертка для метода
        psycopg2.extras.execute_values с обработкой подключения.
        
        Аргументы
        ----------
        df : pandas.DataFrame
            Датафрейм для заливки в БД.
        schema: str
            Название схемы БД.
        table : str
            Название таблицы БД.
        on_conflict_clause: str
            Выражение для выполнения т.н. "UPSERT" - игнорирование или обработка данных при возникновении дубликатов.
            Например:
                "ON CONFLICT ON (name) DO NOTHING"
                или
                "ON CONFLICT (name) DO 
                UPDATE SET email = EXCLUDED.email"
        """
        if not on_conflict_clause:
            on_conflict_clause = ''
        
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            df = self.replace_nans(df)
            tuples = [tuple(x) for x in df.to_numpy()]
            cols = ','.join(list(df.columns))
            query  = f"INSERT INTO {schema}.{table}({cols}) VALUES %s {on_conflict_clause}"
            psycopg2.extras.execute_values(cursor, query, tuples)
            conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")
            conn.rollback()
            raise error
        finally:
            cursor.close()
            conn.close()

    def read_query(self, query):
        """Обертка для метода pd.read_sql_query
        с обработкой подключения
        
        Аргументы
        ----------
        query : str
            SELECT SQL-запрос.
        """
        conn = create_engine(self.db_uri)
        try:
            df = pd.read_sql_query(query, conn)
            return df
        finally:
            conn.close()

    def execute_query(self, query):
        """Метод для выполнения различных запросов, 
        не возвращающих таблицы
        
        Аргументы
        ----------
        query : str
            Любой SQL-запрос.
        """
        conn = self.connect()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")
            conn.rollback()
            cursor.close()
            raise error
        finally:
            conn.close()