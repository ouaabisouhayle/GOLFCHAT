import pandas as pd
import sqlite3
import sys
import os


def infer_sqlite_type(pd_series_type):
    '''
    Infer SQLite data type from pandas series dtype.
    '''
    if pd_series_type == 'int64':
        return 'INTEGER'
    elif pd_series_type == 'float64':
        return 'REAL'
    elif pd_series_type == 'bool':
        return 'BOOLEAN'
    else:
        return 'TEXT'

def quote_identifier(s, errors="strict"):
    '''
    Quote an identifier such as table names and column names to be SQL safe.
    '''
    encodable = s.encode("utf-8", errors).decode("utf-8")
    nul_index = encodable.find("\x00")

    if nul_index >= 0:
        error = UnicodeEncodeError("NUL-terminated utf-8", s, nul_index, nul_index + 1, "NUL not allowed")
        error_handler = codecs.lookup_error(errors)
        replacement, _ = error_handler(error)
        encodable = encodable.replace("\x00", replacement)

    return '"' + encodable.replace('"', '""') + '"'

def csv_to_sqlite(csv_file_path, db_file_path):
    if os.path.exists(db_file_path):
        os.remove(db_file_path)  # Ensure no old database file persists

    df = pd.read_csv(csv_file_path)

    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    # Here, setting chunksize to a smaller value and method to None
    df.to_sql('data', conn, if_exists='replace', index=False, chunksize=100, method=None)

    conn.commit()
    conn.close()


def query_to_dataframe( query):
    """
    Executes a SQL query on a database specified by db_path and returns the results in a pandas DataFrame.

    Parameters:
    - db_path: Path to the SQLite database file.
    - query: SQL query to be executed.

    Returns:
    - A pandas DataFrame containing the results of the query.
    """
    # Connect to the SQLite database
    conn = sqlite3.connect('data.db')

    # Execute the query and convert the results into a DataFrame
    df = pd.read_sql_query(query, conn)

    # Close the connection
    conn.close()

    return df

def get_schema(_):
    conn = sqlite3.connect('data.db')

    # Create a cursor object
    cur = conn.cursor()

    # Query to extract the schema of the database
    cur.execute("SELECT type, name, sql FROM sqlite_master WHERE type='table' OR type='index' OR type='view'")

    # Fetch all the results
    schema = cur.fetchall()

    # Close the connection to the database
    conn.close()
    print(schema[0][2])
    return schema[0][2]


def run_query(query):
    print('\n running ')
    conn = sqlite3.connect('data.db')
    print(' here ')
    df = pd.read_sql_query(query, conn)

    conn.close()

    return df

import re

def extract_sql_query(text):
    """
    Extracts the SQL query from the given text, starting from the first occurrence of "SELECT" to the end of the first code block.

    Args:
        text (str): The text containing the SQL query.

    Returns:
        str: The extracted SQL query or an empty string if no query is found.
    """
    # Regular expression to match the SQL query within the backticks
    pattern = re.compile(r'```(.*?)```', re.DOTALL)
    
    match = pattern.search(text)
    if match:
        # Extract the code block content
        code_block = match.group(1).strip()
        # Find the first occurrence of "SELECT" and return from there to the end
        select_index = code_block.upper().find("SELECT")
        if select_index != -1:
            return code_block[select_index:]
        else:
            return ""
    else:
        return ""
    




    