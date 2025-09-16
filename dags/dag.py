from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta, date
from pathlib import Path
import pandas as pd
import os




def extract_csv_to_csv(**kwargs):
    root = Path(__file__).resolve().parents[1]
    execution_date = kwargs['logical_date']
    source_path = root / "source_data" / "transacoes.csv"
    lake_path = root / "lake" / execution_date.strftime("%Y-%m-%d") / "csv" 
    lake_path.mkdir(parents=True, exist_ok=True)
    pd.read_csv(source_path).to_csv(lake_path / "transacoes.csv", index=False)
    
    




def extract_sql_to_csv(conn, **kwargs):
    root = Path(__file__).resolve().parents[1]
    execution_date = kwargs['logical_date']
    lake_path = root / "lake" / execution_date.strftime("%Y-%m-%d") / "sql"
    lake_path.mkdir(parents=True, exist_ok=True)
    hook = PostgresHook(postgres_conn_id=conn)    
    tables = hook.get_records('''SELECT table_name 
                                 FROM information_schema.tables 
                                 WHERE table_schema = 'public';          
                              ''')
    
    tables = [t[0] for t in tables]
    for table in tables:
        hook.get_pandas_df(f"SELECT * FROM {table}").to_csv(lake_path/f"{table}.csv",index=False)
    

def extract_csv_to_sql(conn, **kwargs):
    execution_date = kwargs["logical_date"]
    root = Path(__file__).resolve().parents[1]
    lake_path = root / "lake" / execution_date.strftime("%Y-%m-%d")

    hook = PostgresHook(postgres_conn_id=conn)
    engine = hook.get_sqlalchemy_engine()
    
    with engine.begin() as conn_:
        conn_.execute("CREATE SCHEMA IF NOT EXISTS warehouse;")

    for folder in os.listdir(lake_path):
        folder_path = lake_path / folder
        for file in folder_path.glob("*.csv"):
            table = file.stem  
            df = pd.read_csv(file)
            df.to_sql(
                name=table,
                con=engine,
                schema="warehouse",
                if_exists="replace",  
                index=False
            )


    

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 1),
    'schedule_interval':"@daily",
    "catchup": False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3)
}

with DAG(
    'lighthouse_pipeline',
    default_args=default_args,
    description='Extrai transacoes.csv e dados de banvic, carrega em lake e, do lake, carrega em warehouse',
    schedule_interval="35 4 * * *",  
    catchup=False
) as dag:

    extract_csv_to_csv = PythonOperator(
        task_id='extract_csv_to_csv',
        python_callable=extract_csv_to_csv,
        provide_context=True,  
    )

    extract_sql_to_csv = PythonOperator(
        task_id='extract_sql_to_csv',
        python_callable=extract_sql_to_csv,
        op_kwargs={'conn': 'postgres_banvic'},
        provide_context=True,  
    )

    extract_csv_to_sql = PythonOperator(
        task_id='extract_csv_to_sql',
        python_callable=extract_csv_to_sql,
        op_kwargs={'conn': 'postgres_warehouse'},
        provide_context=True,  
    )


    [extract_csv_to_csv,extract_sql_to_csv] >> extract_csv_to_sql