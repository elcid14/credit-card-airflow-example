from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from models.main import CreditCardTransaction, Base
import pandas as pd
import asyncio
# import os


# DB_DRIVE = os.getenv('DB_DRIVE')
# DB_USER = os.getenv('SUPBASE_POSTGRES_USER')
# DB_PASS = os.getenv('SUPBASE_POSTGRES_PASS')
# DB_HOST = os.getenv('SUPBASE_POSTGRES_HOST')
# DB_PORT = os.getenv('SUPBASE_POSTGRES_PORT')
# DB_DATA_BASE = os.getenv('SUPBASE_POSTGRES_DB')

engine = create_engine("", pool_size=5)

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
        

        

def write_base_table(task_instance):
    
    """
    Creates the base table used for transaction data named: credit_card_transaction.
    Returns True if created succesfully or False if Error or exception
    """
    # Get task instance data for the schema and dataframe tasks
    schema_exists = task_instance.xcom_pull(task_ids='verify_sql_connection_schema_task')
    credit_data = task_instance.xcom_pull(task_ids='extract_credit_data_task')
    
    # Get sessoin and dataframe
    session = init_session()
    credit_data_df = pd.DataFrame(credit_data)
    print("SHCEMA EXISTS", schema_exists)
    print("CREDIT DATA TO WRITE",credit_data_df['merchant_zip'])
    
    try:
        if schema_exists:
            credit_transactions = [
                CreditCardTransaction(
                    id=row['id'], 
                    transaction_date=row['transaction_date'],  
                    credit_card_number=row['credit_card_number'], 
                    merchant=row['merchant'],  
                    category=row['category'],  
                    transaction_amount=row['transaction_amt'],  
                    purchaser_first_name=row['purchaser_first_name'],  
                    purchaser_last_name=row['purchaser_last_name'], 
                    purchaser_gender=row['purchaser_gender'], 
                    purchaser_street_address=row['purchaser_street_address'],  
                    purchaser_city=row['purchaser_city'],  
                    purchaser_state=row['purchaser_state'],  
                    purchaser_zip=row['purchaser_zip'],  
                    transaction_lat=row['transaction_lat'],  
                    transaction_long=row['transaction_long'],  
                    transaction_city_population=row['transaction_city_population'],  
                    purchaser_job=row['purchaser_job'],  
                    purchaser_dob=row['purchaser_dob'],  
                    transaction_id=row['transaction_id'],  
                    transaction_unix_time=row['transaction_unix_time'], 
                    merchant_lat=row['merchant_lat'],  
                    merchant_long=row['merchant_long'],  
                    merchant_zip=row['merchant_zip'],  
                    is_fraud=row['is_fraud']  
                )
                for _, row in credit_data_df.iterrows()
            ]
            session.add_all(credit_transactions)
            session.commit()
            print("TRANSACTION DATA WRITTEN TO BASE TABLE")
            
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Error occurred: {str(e)}")
        
    except Exception as e:
        print(f"An error occured: {str(e)}")
    
    finally:
        session.close()
        print("CLOSED CONNECTION")
    
    
    
    
    
    
    
    
    



    
    
    
    