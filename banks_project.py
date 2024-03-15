# Importing the required librarie
from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import numpy as np
import sqlite3
def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing.'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url):
    ''' The purpose of this function is to extract the required
    information from the website and save it to a dataframe. The
    function returns the dataframe for further processing. '''
    log_progress("Starting data extraction...")
    
    # Fetch the webpage content
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    
    # Find the table with the required information
    table = data.find('span', id='By_market_capitalization').parent.find_next('table')
    
    # Extract table rows
    rows = table.find_all('tr')
    
    # Define the column names for the DataFrame
    table_attribs = ["Name", "MC_USD_Billion"]
    
    # Create an empty DataFrame to store the extracted data
    df = pd.DataFrame(columns=table_attribs)
    
    # Extract data from each row of the table
    for row in rows[1:]:
        cols = row.find_all('td')
        if len(cols) == 3: 
            bank_name = cols[1].text.strip()
            market_cap = cols[2].text.strip().replace(',', '').replace('\n', '')  # Remove commas and newlines
            try:
                market_cap = float(market_cap)  # Convert to float
            except ValueError:
                log_progress(f"Non-numeric value found in 'Market Cap': {market_cap}. Skipping...")
                continue
            
            # Append data to DataFrame
            df = pd.concat([df, pd.DataFrame([[bank_name, market_cap]], columns=table_attribs)], ignore_index=True)
    
    log_progress("Data extraction completed.")
    return df

# URL of the webpage containing the required table
url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'

# Call the extract function and print the resulting DataFrame
data_frame = extract(url)
print(data_frame)


import pandas as pd
import numpy as np

def transform(data_frame, csv_path):
    # Read the exchange rate CSV file and convert to a dictionary
    log_progress("Initialising Data transformation")
    exchange_rate_df = pd.read_csv(csv_path)
    exchange_rate_dict = exchange_rate_df.set_index('Currency')['Rate'].to_dict()
    
    # Add columns for MC_GBP_Billion, MC_EUR_Billion, and MC_INR_Billion
    data_frame['MC_GBP_Billion'] = [np.round(x * exchange_rate_dict['GBP'], 2) for x in data_frame['MC_USD_Billion']]
    data_frame['MC_EUR_Billion'] = [np.round(x * exchange_rate_dict['EUR'], 2) for x in data_frame['MC_USD_Billion']]
    data_frame['MC_INR_Billion'] = [np.round(x * exchange_rate_dict['INR'], 2) for x in data_frame['MC_USD_Billion']]
    log_progress("Transformation of data completed.")
    return data_frame

# Define the URL of the exchange rate CSV file
csv_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'


transformed_df = transform(data_frame, csv_path)

# Print the contents of the resulting DataFrame
print(transformed_df)


def load_to_csv(transformed_df, csv_path):
    """This function saves the DataFrame to a CSV file."""
    transformed_df.to_csv(csv_path , index=False)
    print(f"DataFrame successfully saved to {csv_path}")

# Define the path where you want to save the CSV file
output_csv_path = './Largest_banks_data.csv'

# Call the load_to_csv function with the transformed DataFrame and the output CSV path
load_to_csv(transformed_df, output_csv_path)
log_progress('Data saved to CSV file')


 
def load_to_db(conn, table_name):
    transformed_df.to_sql(table_name, conn, if_exists='replace', index=False)
    log_progress(f"Data successfully loaded into table in the database.")

conn = sqlite3.connect('Banks.db')

table_name = 'Largest_banks'

load_to_db(conn, table_name)


def run_query(query_statements, sql_connection):
    for query in query_statements:
        print(query)
        print(pd.read_sql(query, sql_connection), '\n')

query_statements = [
        'SELECT * FROM Largest_banks',
        'SELECT AVG(MC_GBP_Billion) FROM Largest_banks',
        'SELECT Name from Largest_banks LIMIT 5'
    ]
run_query(query_statements, conn)
conn.close()
log_progress('Querying Process Complete.')


#for row in rows[1:]:: This is a for loop that iterates over each row in the rows list, starting from the second row (index 1) and continuing to the end. The [1:] slicing syntax is used to exclude the first row, as it usually contains header information and not the actual data.
#cols = row.find_all('td'): Within each iteration of the loop, the code finds all <td> (table data) elements within the current row (row) using the find_all() method of the BeautifulSoup library. This returns a list of <td> elements representing the columns of the current row.
#if len(cols) == 3:: This conditional statement checks if the length of the cols list is equal to 3. This condition ensures that the current row has exactly three columns (table data elements). It's commonly used to filter out rows that don't match the expected structure of the table. If the condition is true, it means that the row contains the expected number of columns, and the subsequent code inside the conditional block will be executed.
#In summary, this code snippet loops through each row of a table, excluding the header row, and checks if the current row has exactly three columns. This filtering step helps ensure that only rows with the expected structure are processed further, while ignoring any header rows or rows with incorrect formatting.
