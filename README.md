# credit-card-airflow-example
A sample project using Apache Airflow, python, and SQL to show ETL concepts using credit card transaction data set. 

## Requirements

- Poetry for depedency management.
- Apache Airflow
- Python version 3.10.12
- AWS CLI tools
- AWS account with appropriate permissions.




## Data flow 

- Main DAG is responsible for cleaning, extracting and loading data.
- Each operator task is responsible for the specific logic to do this. 
- There is one Sub DAG responsible for then handling write to the various AWS data stores (S3, RDS, Dynamo etc..).
- If the Sub DAG has any failure we fail that dag and send failure to last task of Main DAG.
- Final output will be written to supbase postgres table. 








