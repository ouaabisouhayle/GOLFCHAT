from flask import Flask, request, jsonify
import pandas as pd
import os
from SQL_langchain_functions import (
    csv_to_sqlite,
    get_schema,
    run_query
)
import re
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate

app = Flask(__name__)

# Define paths for CSVs
DATASETS = {
    "Blocks Data": os.path.join("CSV DATA", "blocks_with_names.csv"),
    "Tournament Data": os.path.join("CSV DATA", "tours_events_players.csv"),
}

# LLM configuration
llm = ChatGroq(
    groq_api_key="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",
    model_name="llama3-70b-8192"
)

# Function helpers
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
    # Look for SQL queries in the text with improved pattern matching
    pattern = r"(?:SQL Query:|Query:)\s*(.*?)(?=(?:\n(?:SQL Query:|Query:)|$))"
    matches = re.finditer(pattern, text, re.DOTALL)
    queries = [match.group(1).strip() for match in matches]
    
    # If no queries found with the pattern, try to extract anything that looks like SQL
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
            # Convert the query result to a list of dictionaries for JSON serialization
            result = run_query(query)
            if isinstance(result, pd.DataFrame):
                result = result.to_dict(orient='records')
            results.append({"query": query, "result": result, "status": "success"})
        except Exception as e:
            results.append({"query": query, "result": None, "error": str(e), "status": "error"})
    return results

# Templates for LLM prompts
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

@app.route('/process', methods=['POST'])
def process_question():
    try:
        data = request.json
        question = data.get('question')
        theme = data.get('theme')

        if not question or not theme:
            return jsonify({"error": "Missing question or theme"}), 400

        if theme not in DATASETS:
            return jsonify({"error": "Invalid theme selected"}), 400

        file_path = DATASETS[theme]

        if not os.path.exists(file_path):
            return jsonify({"error": f"File not found for theme: {theme}"}), 404

        # Load the database
        path_to_db = "data.db"
        if os.path.exists(path_to_db):
            os.remove(path_to_db)
        
        with open(file_path, 'rb') as file:
            csv_to_sqlite(file, path_to_db)

        # Step 1: Preprocess the question
        preprocess_input = {
            "question": question,
            "column_info": get_column_info(file_path),
            "csv_preview": get_csv_preview(file_path)
        }
        preprocess_response = preprocess_chain = (
            preprocess_prompt 
            | llm 
            | StrOutputParser()
        ).invoke(preprocess_input)
        
        processed_question = extract_rewritten_question(preprocess_response)

        # Step 2: Generate SQL queries
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
            return jsonify({"error": "No valid SQL queries generated"}), 400

        # Step 3: Execute queries and get results
        query_results = run_multiple_queries(queries)

        # Step 4: Generate analysis
        query_results_text = "\n".join([
            f"Query {i+1}: {qr['query']}\nResult: {qr['result'] if qr['status'] == 'success' else f'Error: {qr.get('error', 'Unknown error')}'}"
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

        return jsonify({
            "processed_question": processed_question,
            "queries": queries,
            "query_results": query_results,
            "analysis": analysis_response
        })

    except Exception as e:
        return jsonify({
            "error": str(e),
            "stack_trace": str(e.__traceback__)
        }), 500

if __name__ == "__main__":
    app.run(debug=True)