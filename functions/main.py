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

# Initialize Firebase Admin
initialize_app()

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

# Define CORS headers to allow all origins
CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',  # Allow all origins
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '3600'
}

# Define the HTTPS function
@https_fn.on_request()
def process_question(request: https_fn.Request) -> https_fn.Response:
        # Handle CORS preflight request
        if request.method == 'OPTIONS':
            return https_fn.Response('', status=204, headers=CORS_HEADERS)
        
        # Set CORS headers for actual request
        headers = CORS_HEADERS.copy()
        
    # try:
        if request.method != 'POST':
            return https_fn.Response("Method Not Allowed", status=405, headers=headers)
        
        data = request.get_json(silent=True)
        if not data:
            return https_fn.Response("Invalid JSON", status=400, headers=headers)

        question = data.get('question')
        theme = data.get('theme')

        if not question or not theme:
            return https_fn.Response(
                {"error": "Missing question or theme"},
                status=400,
                headers=headers
            )

        if theme not in DATASETS:
            return https_fn.Response(
                {"error": "Invalid theme selected"},
                status=400,
                headers=headers
            )

        file_path = DATASETS[theme]
        if not os.path.exists(file_path):
            return https_fn.Response(
                {"error": f"File not found for theme: {theme}"},
                status=404,
                headers=headers
            )

        path_to_db = "data.db"
        if os.path.exists(path_to_db):
            os.remove(path_to_db)

        with open(file_path, 'rb') as file:
            csv_to_sqlite(file, path_to_db)

        preprocess_input = {
            "question": question,
            "column_info": get_column_info(file_path),
            "csv_preview": get_csv_preview(file_path)
        }

        preprocess_response = (
            preprocess_prompt
            | llm
            | StrOutputParser()
        ).invoke(preprocess_input)
        
        processed_question = extract_rewritten_question(preprocess_response)

        sql_input = {
            "schema": get_schema(5),
            "csv_previews": get_csv_preview(file_path),
            "processed_question": processed_question
        }

        sql_response = (
            sql_prompt
            | llm
            | StrOutputParser()
        ).invoke(sql_input)
        
        queries = extract_multiple_queries(sql_response)
        if not queries:
            return https_fn.Response(
                {"error": "No valid SQL queries generated"},
                status=400,
                headers=headers
            )

        query_results = run_multiple_queries(queries)

        # Step 4: Analysis
        query_results_text = "\n".join([
            f"Query {i+1}: {qr['query']}\nResult: {qr['result'] if qr['status'] == 'success' else f'Error: {qr.get('error')}'}"
            for i, qr in enumerate(query_results)
        ])

        analysis_input = {
            "original_question": question,
            "processed_question": processed_question,
            "query_results": query_results_text
        }

        analysis_response = (
            analysis_prompt
            | llm
            | StrOutputParser()
        ).invoke(analysis_input)

        response_data = {
            "processed_question": processed_question,
            "queries": queries,
            "query_results": query_results,
            "analysis": analysis_response
        }

        return https_fn.Response(
            response_data,
            status=200,
            headers=headers
        )

    # except Exception as e:
    #     error_response = {
    #         "error": str(e),
    #     }
    #     return https_fn.Response(
    #         error_response,
    #         status=500,
    #         headers=headers
    #     )
