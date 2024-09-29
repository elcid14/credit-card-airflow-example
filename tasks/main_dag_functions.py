import pandas as pd

def extract_credit_file():
    """
    Extracts data from csv file and sorts according to transaction date in descinding order.
    Only returns top 1000 records to help with memeory management, in a real world scenario we would return entire data frame.
    """
    credit_data = pd.read_csv('../raw_data/archive/credit_card_transactions.csv', nrows=1000)
    credit_data['trans_date_trans_time'] = pd.to_datetime(credit_data['trans_date_trans_time'])
    credit_data_sorted = credit_data.sort_values(by='trans_date_trans_time', ascending=False)
    credit_data_final = credit_data_sorted.rename(columns={
        'Unnamed: 0' : 'id',
        'trans_date_trans_time' : 'transaction_date',
        'cc_num': 'credit_card_number',
        'amt': 'transaction_amt',
        'first' : 'purchaser_first_name',
        'last' : 'purchaser_last_name',
        'gender' : 'purchaser_gender',
        'street' : 'purchaser_street_address',
        'city' : 'purchaser_city',
        'state' : 'purchaser_state',
        'zip' : 'purchaser_zip',
        'lat' : 'transaction_lat',
        'long' : 'transaction_long',
        'city_pop' : 'transaction_city_population',
        'job' : 'purchaser_job',
        'dob' : 'purchaser_dob',
        'trans_num' : 'transaction_id',
        'unix_time' : 'transaction_unix_time',
        'merch_lat' : 'merchant_lat',
        'merch_long' : 'merchant_long',
        'merch_zipcode' : 'merchant_zip'
    }).fillna(0)
    return credit_data_final

    
