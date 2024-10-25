from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base


# Define the base class for your models
Base = declarative_base()

class CreditCardTransaction(Base):
    __tablename__ = 'credit_card_transaction'
    
    id = Column(Integer, primary_key=True)
    transaction_date = Column(DateTime)
    credit_card_number = Column(String)
    merchant = Column(String)
    category = Column(String)
    transaction_amount = Column(Float)
    purchaser_first_name = Column(String)
    purchaser_last_name = Column(String)
    purchaser_gender = Column(String)
    purchaser_street_address = Column(String)
    purchaser_city = Column(String)
    purchaser_state = Column(String)
    purchaser_zip = Column(String)
    transaction_lat = Column(Float)
    transaction_long = Column(Float)
    transaction_city_population = Column(Integer)
    purchaser_job = Column(String)
    purchaser_dob = Column(DateTime)
    transaction_id = Column(String)
    transaction_unix_time = Column(Integer)
    merchant_lat = Column(Float)
    merchant_long = Column(Float)
    merchant_zip = Column(String)
    is_fraud = Column(Boolean)
    
    
class FraudlentTransactions(Base):
    __tablename__ = 'fradulent_transactions'
    id = Column(Integer, primary_key = True)
    transaction_date = Column(DateTime)
    credit_card_number = Column(String)
    merchant = Column(String)
    category = Column(String)
    transaction_amount = Column(Float)
    purchaser_first_name = Column(String)
    purchaser_last_name = Column(String)
    is_fraud = Column(Boolean)
    
class NumberOfFradulentTransactions(Base):
    __tablename__ = 'number_fradulent_transactions'
    number_fradulent_transactions = (Integer)