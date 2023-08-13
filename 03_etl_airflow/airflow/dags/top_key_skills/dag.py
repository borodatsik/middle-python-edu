from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

from top_key_skills import config
from top_key_skills.tasks.egrul import upload_egrul
from top_key_skills.tasks.hh_api_parsing import upload_hh
from top_key_skills.tasks.top import get_top_key_skills

default_args = {
    'owner': 'datsik',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    }

dag_versions = dict(
    test=dict(
        download_egrul_url=config.egrul_test_url,
        download_egrul_filepath=config.egrul_download_test_filepath,
        read_egrul_filepath=config.egrul_test_filepath,
        schema=config.test_schema,
        telecom_companies_schema=config.test_schema_hw1,
        ),
    preprod=dict(
        download_egrul_url=config.egrul_test_url,
        download_egrul_filepath=config.egrul_download_test_filepath,
        read_egrul_filepath=config.egrul_preprod_filepath,
        schema=config.preprod_schema,
        telecom_companies_schema=config.preprod_schema,
        ),
    prod=dict(
        download_egrul_url=config.egrul_prod_url,
        download_egrul_filepath=config.egrul_download_prod_filepath,
        read_egrul_filepath=config.egrul_download_prod_filepath,
        schema=config.prod_schema,
        telecom_companies_schema=config.prod_schema,
        ),
    )
    
for version in dag_versions.keys():
    dag_id = f'datsik_top_key_skills_dag_{version}'
    globals()[dag_id] = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f'Searching top-10 key skills in vacancies - {version} version',
        start_date=datetime(2023, 1, 1),
        schedule_interval="@once",
        )
    
    with globals()[dag_id] as dag:
        create_tables = PostgresOperator(
            task_id="create_tables",
            postgres_conn_id=config.postgres_conn_id,
            sql=config.create_tables_sql_path,
            params={"schema": dag_versions[version]['schema']},
            )
        
        download_egrul = BashOperator(
            task_id='download_egrul',
            bash_command=f"wget {dag_versions[version]['download_egrul_url']} -O {dag_versions[version]['download_egrul_filepath']}",
            )
        
        upload_telecom_companies = PythonOperator(
            task_id="upload_telecom_companies",
            python_callable=upload_egrul,
            op_kwargs=dict(
                schema=dag_versions[version]['schema'],
                egrul_filepath=dag_versions[version]['read_egrul_filepath'],
                ),
            )
            
        upload_vacancies = PythonOperator(
            task_id="upload_vacancies",
            python_callable=upload_hh,
            op_kwargs=dict(
                schema=dag_versions[version]['schema'],
                ),
            )
            
        print_top_key_skills = PythonOperator(
            task_id="print_top_key_skills",
            python_callable=get_top_key_skills,
            op_kwargs=dict(
                vacancies_schema=dag_versions[version]['schema'],
                telecom_companies_schema=dag_versions[version]['telecom_companies_schema']
                ),
            )
        
        [download_egrul, create_tables] >> upload_telecom_companies
        upload_vacancies.set_upstream(create_tables)
        print_top_key_skills.set_upstream(upload_vacancies)
        print_top_key_skills.set_upstream(upload_telecom_companies)

