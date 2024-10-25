import pandas as pd
import numpy as np

def get_total_num_fraud_transactions(task_instance):
    
    # Get the credit data from intial task
    credit_data = task_instance.xcom_pull(task_ids='extract_credit_data_task')
    # create random fraud value
    credit_fraud_randomized = credit_data.copy()
    credit_fraud_randomized['is_fraud'] = np.random.choice([True, False], size=len(credit_data))
    
    
    print({"message":"GETTING CREDIT DATA"})
    print(credit_fraud_randomized['is_fraud'])
    print({"message":"RETURNED DF OF FRAUDULENT TRANSACTIONS"})
    
    # calculate sum and then write to table
    
    num_fraud_transactions = credit_fraud_randomized['is_fraud'].sum()
    print({"message":"NUM FRAUD"})
    print(num_fraud_transactions)
    
    