import os
import re
import pandas as pd
import numpy as np
from firebase_functions import https_fn
from firebase_admin import initialize_app
from SQL_langchain_functions import (
    csv_to_sqlite,
    get_schema,
    run_query
)
from langchain_core.output_parsers import StrOutputParser
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
import logging
from flask import jsonify , Response
import json

# CSV file paths
DATASETS = {
    "Blocks Data": os.path.join("CSV DATA", "blocks_with_names.csv"),
    "Tournament Data": os.path.join("CSV DATA", "tours_events_players.csv"),
}

# LLM configuration
llm = ChatGroq(
    groq_api_key="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",
    model_name="llama3-8b-8192"
)

# Helper functions
def get_csv_preview(file_path, num_rows=2):
    try:
        df = pd.read_csv(file_path)
        return df.head(num_rows).to_string()
    except Exception as e:
        raise Exception(f"Error reading CSV file: {str(e)}")

def get_column_info(file_path):
    try:
        df = pd.read_csv(file_path)
        return [{"name": col, "type": str(df[col].dtype), "sample_values": df[col].head(1).tolist()} for col in df.columns]
    except Exception as e:
        raise Exception(f"Error getting column info: {str(e)}")

def extract_multiple_queries(text):
    pattern = r"(?:SQL Query:|Query:)\s*(.*?)(?=(?:\n(?:SQL Query:|Query:)|$))"
    matches = re.finditer(pattern, text, re.DOTALL)
    queries = [match.group(1).strip() for match in matches]
    if not queries:
        pattern = r"SELECT.*?(?:;|$)"
        matches = re.finditer(pattern, text, re.DOTALL | re.IGNORECASE)
        queries = [match.group(0).strip() for match in matches]
    return queries

def extract_rewritten_question(text):
    patterns = [
        r"Rewritten Question:\s*(.*?)(?=\n|$)",
        r"Here's the rewritten question:\s*(.*?)(?=\n|$)",
        r"Processed question:\s*(.*?)(?=\n|$)"
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()
    return text.strip()

def run_multiple_queries(queries):
    results = []
    for query in queries:
        try:
            result = run_query(query)
            if isinstance(result, pd.DataFrame):
                result = result.to_dict(orient='records')
            results.append({"query": query, "result": result, "status": "success"})
        except Exception as e:
            results.append({"query": query, "result": None, "error": str(e), "status": "error"})
    return results


# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detailed logs
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Initialize Firebase Admin
try:
    initialize_app()
    logging.info("Firebase Admin initialized successfully.")
except Exception as e:
    logging.error("Failed to initialize Firebase Admin.", exc_info=True)

# CSV file paths
DATASETS = {
    "Blocks Data": os.path.join("CSV DATA", "blocks_with_names.csv"),
    "Tournament Data": os.path.join("CSV DATA", "tours_events_players.csv"),
}

# LLM configuration
llm = ChatGroq(
    groq_api_key="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",
    model_name="llama3-70b-8192"
)
logging.info("LLM (ChatGroq) configured.")

# Prompt templates
preprocess_template = """Given the following user question and database information, rewrite the question to be more precise and aligned with the actual data structure.

Original Question: {question}

Available Columns and Their Information:
{column_info}

Data Preview:
{csv_preview}

Important: Start your response with "Rewritten Question:" followed by the rewritten question.
"""

sql_template = """Based on the table schema and CSV preview below, write SQL queries that would answer the user's question. Format each query starting with "SQL Query:" on a new line.

Schema:
{schema}

CSV Preview:
{csv_previews}

Question: {processed_question}

Important: Each query must start with "SQL Query:" and be properly formatted SQL.
"""

analysis_template = """Analyze the following query results in relation to the original question:

Original Question: {original_question}
Processed Question: {processed_question}

Query Results:
{query_results}

Provide a comprehensive analysis of the results, including any patterns or insights found in the data.
"""

preprocess_prompt = ChatPromptTemplate.from_template(preprocess_template)
sql_prompt = ChatPromptTemplate.from_template(sql_template)
analysis_prompt = ChatPromptTemplate.from_template(analysis_template)
logging.info("Prompt templates initialized.")

# Define CORS headers
CORS_HEADERS = {
    'Access-Control-Allow-Origin':   '*',  # Replace '*' with your frontend URL in production, e.g., 'http://localhost:3000'
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '3600'
}

# Define the HTTPS function with manual CORS handling and detailed logging
@https_fn.on_request()
def process_question(request: https_fn.Request) -> https_fn.Response:
    logging.info(f"Received {request.method} request.")

    # Log request headers
    logging.debug(f"Request headers: {dict(request.headers)}")

    # Handle preflight (OPTIONS) requests
    if request.method == 'OPTIONS':
        logging.info("Handling CORS preflight (OPTIONS) request.")
        return https_fn.Response('', status=204, headers=CORS_HEADERS)

    # Set CORS headers for actual requests
    headers = CORS_HEADERS.copy()

    try:
        if request.method != 'POST':
            logging.warning(f"Unsupported HTTP method: {request.method}. Only POST is allowed.")
            return https_fn.Response("Method Not Allowed", status=405, headers=headers)

        # Parse JSON body
        data = request.get_json(silent=True)
        logging.debug(f"Request JSON payload: {data}")

        if not data:
            logging.warning("No JSON payload found in the request.")
            return https_fn.Response({"error": "Invalid JSON"}, status=400, headers=headers)

        question = data.get('question')
        theme = data.get('theme')

        logging.info(f"Processing question: '{question}' with theme: '{theme}'.")

        if not question or not theme:
            logging.warning("Missing 'question' or 'theme' in the request.")
            return https_fn.Response(
                {"error": "Missing question or theme"},
                status=400,
                headers=headers
            )

        if theme not in DATASETS:
            logging.warning(f"Invalid theme selected: '{theme}'. Available themes: {list(DATASETS.keys())}")
            return https_fn.Response(
                {"error": "Invalid theme selected"},
                status=400,
                headers=headers
            )

        file_path = DATASETS[theme]
        logging.info(f"Selected file path for theme '{theme}': {file_path}")

        if not os.path.exists(file_path):
            logging.error(f"File not found for theme: {theme}. Expected at {file_path}")
            return https_fn.Response(
                {"error": f"File not found for theme: {theme}"},
                status=404,
                headers=headers
            )

        path_to_db = "data.db"
        if os.path.exists(path_to_db):
            os.remove(path_to_db)
            logging.info(f"Existing database '{path_to_db}' removed.")

        # Convert CSV to SQLite
        with open(file_path, 'rb') as file:
            csv_to_sqlite(file, path_to_db)
            logging.info(f"CSV file '{file_path}' converted to SQLite database '{path_to_db}'.")

        # Prepare preprocess input
        preprocess_input = {
            "question": question,
            "column_info": get_column_info(file_path),
            "csv_preview": get_csv_preview(file_path)
        }
        logging.debug(f"Preprocess input: {preprocess_input}")

        # Invoke LLM for preprocessing
        preprocess_response = (
            preprocess_prompt
            | llm
            | StrOutputParser()
        ).invoke(preprocess_input)
        logging.info("Preprocessing completed.")

        processed_question = extract_rewritten_question(preprocess_response)
        logging.debug(f"Processed question: '{processed_question}'.")

        # Prepare SQL input
        sql_input = {
            "schema": get_schema(5),
            "csv_previews": get_csv_preview(file_path),
            "processed_question": processed_question
        }
        logging.debug(f"SQL input: {sql_input}")

        # Invoke LLM for SQL generation
        sql_response = (
            sql_prompt
            | llm
            | StrOutputParser()
        ).invoke(sql_input)
        logging.info("SQL query generation completed.")

        queries = extract_multiple_queries(sql_response)
        logging.debug(f"Extracted queries: {queries}")

        if not queries:
            logging.warning("No valid SQL queries generated.")
            return https_fn.Response(
                {"error": "No valid SQL queries generated"},
                status=400,
                headers=headers
            )

        # Run multiple queries
        query_results = run_multiple_queries(queries)
        logging.info(f"Executed {len(query_results)} queries.")

        # Prepare query results text for analysis
        query_results_text = "\n".join([
            f"Query {i+1}: {qr['query']}\nResult: " +
            str((qr['result'] if qr['status'] == 'success' else f"Error: {str(qr.get('error', 'Unknown error'))}"))
            for i, qr in enumerate(query_results)
        ])


        logging.debug(f"Query results text: {query_results_text}")

        # Prepare analysis input
        analysis_input = {
            "original_question": question,
            "processed_question": processed_question,
            "query_results": query_results_text
        }
        logging.debug(f"Analysis input: {analysis_input}")

        # Invoke LLM for analysis
        analysis_response = (
            analysis_prompt
            | llm
            | StrOutputParser()
        ).invoke(analysis_input)
        logging.info("Analysis generation completed.")

        # Prepare the final response data
        response_data = {
            "processed_question": processed_question,
            "queries": queries,
            "query_results": query_results,
            "analysis": analysis_response
        }
        logging.debug(f"Response data: {response_data}")
        logging.info(f"Response data: {response_data}")

        # Log successful processing
        logging.info("Request processed successfully. Sending response.")
        response_data = {
                    "processed_question": processed_question,
                    "queries": queries,
                    "query_results": query_results,
                    "analysis": analysis_response
                }
       # return https_fn.Response(json.dumps(response_data), status=200, headers="application/json")
        return Response(
    response=json.dumps(response_data),  # Serialize the response data
    status=200,
    headers=CORS_HEADERS  # Correct way to set headers
)
    
        #     response=response_data,
        #     status=200,
        #     headers=headers  # Includes both CORS and Content-Type headers
        # )
    

    except Exception as e:
        logging.error("An unexpected error occurred during request processing.", exc_info=True)
        return https_fn.Response(
            {"error": str(e)},
            status=500,
            headers=headers
        )
