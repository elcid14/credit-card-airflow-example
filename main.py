from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv

from tasks.main_dag_functions import extract_credit_file
from tasks.groups.sql_task_group import verify_sql_connection_and_schema, write_base_table



# Load dotenv
load_dotenv()


# Define default airflow args

# default_args = {
#     'owner'
    
# }

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

verify_sql_connection_schema_task = PythonOperator(
    task_id='verify_sql_connection_schema_task',
    python_callable=verify_sql_connection_and_schema,
    dag=main_dag,
    provide_context=True
)

write_base_table_task = PythonOperator(
    task_id='write_base_table_task',
    python_callable=write_base_table,
    dag=main_dag,
    provide_context=True
)

    

# Define AWS tasks


extract_credit_data_task >> verify_sql_connection_schema_task >> write_base_table_task

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