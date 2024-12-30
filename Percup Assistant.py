import streamlit as st
import pandas as pd
from SQL_langchain_functions import (
    csv_to_sqlite,
    get_schema,
    run_query,
    extract_sql_query
)
import re
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
import os
from PIL import Image

def get_csv_preview(file_path, num_rows=2):
    """Get a preview of the CSV file including headers and first few rows"""
    df = pd.read_csv(file_path)
    preview = df.head(num_rows).to_string()
    return preview

def get_column_info(file_path):
    """Get detailed information about columns including names and data types"""
    df = pd.read_csv(file_path)
    column_info = []
    for column in df.columns:
        dtype = str(df[column].dtype)
        sample_values = df[column].head(1).tolist()
        column_info.append({
            "name": column,
            "type": dtype,
            "sample_values": sample_values
        })
    return column_info

def get_theme_file(theme, base_directory):
    """Return the appropriate file path for the selected theme"""
    theme_files = {
        "Blocks Data": os.path.join(base_directory, "CSV DATA/blocks_with_names.csv"),
        "Tournament Data": os.path.join(base_directory, "CSV DATA/tours_events_players.csv"),
        "Player Data": os.path.join(base_directory, "CSV DATA/players_info.csv")
    }
    return theme_files.get(theme)

def extract_multiple_queries(text):
    """Extract all SQL queries from the text"""
    pattern = r"SQL Query:\s*(.*?)(?=(?:\nSQL Query:|$))"
    matches = re.finditer(pattern, text, re.DOTALL)
    queries = []
    
    for match in matches:
        query = match.group(1).strip()
        if query:
            query = re.sub(r'```sql|```', '', query).strip()
            query = re.split(r'\n\n|(?=This query)', query)[0].strip()
            queries.append(query)
    
    return queries

def extract_rewritten_question(text):
    """Extract just the rewritten question from the preprocessing output"""
    # Look for the question after "Rewritten Question:" or similar patterns
    patterns = [
        r"Rewritten Question:\s*(.*?)(?=\n|$)",
        r"Here's the rewritten question:\s*(.*?)(?=\n|$)",
        r"Processed question:\s*(.*?)(?=\n|$)"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()
    
    # If no pattern matches, return the last non-empty line as a fallback
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    return lines[-1] if lines else text.strip()

def run_multiple_queries(queries):
    """Run multiple SQL queries and return their results"""
    results = []
    for query in queries:
        try:
            result = run_query(query)
            results.append({"query": query, "result": result})
        except Exception as e:
            results.append({"query": query, "error": str(e)})
    return results

def main():
    base_directory = os.getcwd()
    
    # Question preprocessing template
    preprocess_template = """Given the following user question and database information, rewrite the question to be more precise and aligned with the actual data structure.
    the perckup value column is the one responsable for the perckup points , the columns percuppointsxxxx (xxxx is a year ) are just perdiction , use them for only prediction questions
     Original Question: {question}
    
    Available Columns and Their Information:
    {column_info}
    
    Data Preview:
    {csv_preview}
    
    Important: Start your response with "Rewritten Question:" followed by the rewritten question.
    Make sure the rewritten question uses the correct column names and is clear and concise."""
    
    # SQL query generation template
    sql_template = """Based on the table schema and CSV preview below, write SQL queries that would answer the user's question.
    Write each query starting with "SQL Query:" on a new line. Make sure to write only valid SQL syntax without any additional text or markdown.
    
    Schema:
    {schema}
    
    CSV Preview (first 5 rows):
    {csv_previews}

    Question: {processed_question}"""
    
    # Analysis template
    analysis_template = """Based on the question and query results below, write a comprehensive analysis:
    
    Original Question: {original_question}
    Processed Question: {processed_question}
    Queries and Results: {query_results}
    
    Provide a detailed analysis of all the results."""
    
    preprocess_prompt = ChatPromptTemplate.from_template(preprocess_template)
    sql_prompt = ChatPromptTemplate.from_template(sql_template)
    analysis_prompt = ChatPromptTemplate.from_template(analysis_template)

    model = st.sidebar.selectbox(
        'Choose a model',
        ['llama3-70b-8192','mixtral-8x7b-32768','llama3-8b-8192','gemma-7b-it']
    )

    theme = st.sidebar.selectbox(
        'Choose your dataset',
        ['Blocks Data', 'Tournament Data', 'Player Data']
    )

    llm = ChatGroq(
        groq_api_key="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", 
        model_name=model
    )

    # Create the chains
    preprocess_chain = (
        RunnablePassthrough.assign(
            column_info=lambda x: get_column_info(get_theme_file(theme, base_directory)),
            csv_preview=lambda x: get_csv_preview(get_theme_file(theme, base_directory))
        )
        | preprocess_prompt
        | llm
        | StrOutputParser()
    )

    sql_chain = (
        RunnablePassthrough.assign(
            schema=get_schema,
            csv_previews=lambda _: get_csv_preview(get_theme_file(theme, base_directory))
        )
        | sql_prompt
        | llm
        | StrOutputParser()
    )

    analysis_chain = (
        analysis_prompt 
        | llm 
        | StrOutputParser()
    )

    st.title("Hello! I'm PERCUP Data assistant!")
    st.write(f"I can help answer your questions about {theme}")
   
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    file_path = get_theme_file(theme, base_directory)
    
    if file_path:
        try:
            path_to_db = "data.db"
            if os.path.exists(path_to_db):
                os.remove(path_to_db)
            
            with open(file_path, 'rb') as file:
                csv_to_sqlite(file, path_to_db)
            
            st.sidebar.success(f'{theme} loaded successfully')
            
            if st.sidebar.checkbox("Show data preview"):
                st.sidebar.markdown(f"### {theme} Preview")
                st.sidebar.code(get_csv_preview(file_path))
            
            placeholder_text = {
                "Blocks Data": "Ask about Minecraft blocks (e.g., 'What are the most common block types?')",
                "Tournament Data": "Ask about tournaments (e.g., 'Show me the highest scores in events')",
                "Player Data": "Ask about players (e.g., 'Who are the top performing players?')"
            }

            if prompt := st.chat_input(placeholder_text[theme]):
                st.chat_message("user").write(prompt)
                st.session_state.messages.append({"role": "user", "content": prompt})
                
                with st.chat_message("assistant"):
                    # Preprocess the question
                    preprocess_response = preprocess_chain.invoke({"question": prompt})
                    processed_question = extract_rewritten_question(preprocess_response)
                    st.write("Processed Question:")
                    st.info(processed_question)
                    
                    # Get SQL queries using processed question
                    query_response = sql_chain.invoke({"processed_question": processed_question})
                    queries = extract_multiple_queries(query_response)
                    
                    # Run all queries
                    query_results = run_multiple_queries(queries)
                    
                    # Display all queries and their results
                    for i, qr in enumerate(query_results, 1):
                        st.write(f"Query {i}:")
                        st.code(qr["query"], language="sql")
                        st.write(f"Result {i}:")
                        if "error" in qr:
                            st.error(f"Error executing query: {qr['error']}")
                        else:
                            st.write(qr["result"])
                        st.write("---")
                    
                    # Generate analysis of all results
                    query_results_text = "\n".join([
                        f"Query {i+1}: {qr['query']}\nResult: {qr.get('result', f'Error: {qr.get('error', 'Unknown error')}')})"
                        for i, qr in enumerate(query_results)
                    ])
                    
                    analysis = analysis_chain.invoke({
                        "original_question": prompt,
                        "processed_question": processed_question,
                        "query_results": query_results_text
                    })
                    
                    st.write("Analysis:")
                    st.write(analysis)
                    
                    st.session_state.messages.append({
                        "role": "assistant", 
                        "content": f"Processed Question: {processed_question}\n\nQueries: {query_response}\n\nAnalysis: {analysis}"
                    })
                    
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            st.error(f"Please ensure the file exists at: {file_path}")
    else:
        st.error("Selected theme configuration not found")

if __name__ == "__main__":
    main()