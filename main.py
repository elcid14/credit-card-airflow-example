import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator




# define function calls for task operators

def test_fn():
    print("HELLO WORLD")



# define default airflow args

# default_args = {
#     'owner'
    
# }

# define main dag that will be responsible for ochestration of major tasks

main_dag = DAG(
    'main_dag',
    # default_args=-default_args,
    schedule_interval = '*/5 * * * *',
    catchup=False
)


# define task operators
test_operator = PythonOperator(
    task_id='test_operator',
    python_callable=test_fn,
    dag=main_dag
)



