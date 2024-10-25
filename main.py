import os
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from tasks.main_dag_functions import extract_credit_file
from tasks.groups.sql_task_group import verify_sql_connection_and_schema, write_base_table
from tasks.groups.data_calculations import  get_total_num_fraud_transactions


from models.main import Base


# INFO: this links offers cheat sheet of airflow commands https://www.restack.io/docs/airflow-knowledge-apache-client-cheat-sheet-cli


# Load dotenv
load_dotenv()


# Define default airflow args

# default_args = {
#     'owner' 
# }

DB_USER = os.getenv('SUPBASE_POSTGRES_USER')
DB_PASS = os.getenv('SUPBASE_POSTGRES_PASS')
DB_HOST = os.getenv('SUPBASE_POSTGRES_HOST')
DB_PORT = os.getenv('SUPBASE_POSTGRES_PORT')
DB_DATA_BASE = os.getenv('SUPBASE_POSTGRES_DB')

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_DATA_BASE}", pool_size=5)

# Utility function to init connection to DB
def init_connection():
    
    try:
        with engine.connect() as connection:
        # If the connection is successful, no exception will be raised
            print("Database connection successful.")
            Base.metadata.create_all(engine)
    except Exception as e:
        print(f"An error occured: {str(e)}")
    
    
def init_session():
    Session = sessionmaker(bind=engine)
    session = Session()
    return session
    
   
    

def verify_sql_connection_and_schema() -> bool:
    """
    Verify that database and tables exist 
    """
    try:
        init_connection()
        return True
    except Exception as e:
        print(f"Connection failed with error: {str(e)}")
        return False

# Define main dag that will be responsible for ochestration of major tasks
main_dag = DAG(
    'main_dag',
    # default_args=-default_args
    start_date=datetime(2024,9,23),
    schedule_interval = '*/60 * * * *',
    catchup=False
)


# Define task operators for main dag
# Extract credit data task
extract_credit_data_task = PythonOperator(
    task_id='extract_credit_data_task',
    python_callable=extract_credit_file,
    dag=main_dag,
    provide_context=True
)


# Define Sql Task Group
# verify_sql_connection_schema_task = PythonOperator(
#     task_id='verify_sql_connection_schema_task',
#     python_callable=verify_sql_connection_and_schema,
#     dag=main_dag,
#     provide_context=True
# )

# write_base_table_task = PythonOperator(
#     task_id='write_base_table_task',
#     python_callable=write_base_table,
#     dag=main_dag,
#     provide_context=True
# )
    
# Define AWS tasks


# Define data tasks

create_total_num_fraud_transactions = PythonOperator(
    task_id='create_total_num_fraud_transactions',
    python_callable=get_total_num_fraud_transactions,
    dag=main_dag,
    provide_context=True
)

# extract_credit_data_task >> verify_sql_connection_schema_task >> write_base_table_task >> create_total_num_fraud_transactions
extract_credit_data_task >> create_total_num_fraud_transactions
# TODO:
#  Task to store csv file into s3 bucket

# Task to convert dataframe into lancedb format and store in s3.

# Task to write df to supbase table. 







# Get the following data sets from ETL

# - Get transactions sorted by date desc order [In Progress]

# - Get transactions sorted by transaction_amt  by desc order [ In Progress]

# - Get transactions grouped by zipcode [ Not started]

#  - Get transactions if fradulent grouped by zipcode and also max num of fradulent purchases [ Not Started ]

# 