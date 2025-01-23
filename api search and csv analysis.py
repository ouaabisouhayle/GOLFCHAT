from flask import Flask, request, jsonify
import os
from langchain.agents.agent_types import AgentType
from langchain.agents import initialize_agent, load_tools
from langchain_experimental.agents import create_csv_agent
from langchain_openai import OpenAI
from langchain_groq import ChatGroq
import io
import contextlib
import re

app = Flask(__name__)

os.environ['SERPER_API_KEY'] = 'be68b0be71fc685a1a203544bc2407787d485ae6'

def format_analysis_output(output):
    # Remove ANSI escape codes
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    cleaned_output = ansi_escape.sub('', output)
    
    # Split into sections
    sections = []
    current_section = []
    
    for line in cleaned_output.split('\n'):
        if line.strip():
            if line.startswith('Thought:'):
                if current_section:
                    sections.append('\n'.join(current_section))
                current_section = [line]
            elif line.startswith('Action:'):
                if current_section:
                    sections.append('\n'.join(current_section))
                current_section = [line]
            elif line.startswith('Action Input:'):
                current_section.append(line)
            elif line.startswith('Final Answer:'):
                if current_section:
                    sections.append('\n'.join(current_section))
                current_section = [line]
            else:
                current_section.append(line)
    
    if current_section:
        sections.append('\n'.join(current_section))
    
    # Format sections
    formatted_output = []
    for section in sections:
        if section.strip():
            if section.startswith('Thought:'):
                formatted_output.append('💭 Reasoning:\n' + section.replace('Thought:', '').strip())
            elif section.startswith('Action:'):
                formatted_output.append('🔧 Step:\n' + section.strip())
            elif section.startswith('Final Answer:'):
                formatted_output.append('✅ Conclusion:\n' + section.replace('Final Answer:', '').strip())
            else:
                formatted_output.append(section.strip())
    
    return '\n\n'.join(formatted_output)

def capture_output(func):
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        result = func()
    return result, output.getvalue()

def get_file_path(selected_file, base_directory="GOLFCHAT"):
    theme_files = {
        "blocks": os.path.join(base_directory, "functions/CSV DATA/blocks_with_names.csv"),
        "tournament": os.path.join(base_directory, "functions/CSV DATA/tours_events_players_cleaned.csv"),
    }
    return theme_files.get(selected_file)

def create_search_agent():
    llm = ChatGroq(
        groq_api_key="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",
        model_name='llama3-8b-8192',
        temperature=0.05
    )
    tools = load_tools(["google-serper"], llm=llm)
    return initialize_agent(tools, llm, agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION, verbose=True)

def create_data_agent(file_path):
    return create_csv_agent(
        OpenAI(
            api_key="sk-ee552cf5b3874eaabcb09afaa6962164",
            base_url="https://api.deepseek.com/beta",
            model='deepseek-chat'
        ),
        file_path,
        verbose=True,
        allow_dangerous_code=True
    )

@app.route('/query', methods=['POST'])
def handle_query():
    data = request.json
    if not data or 'question' not in data or 'theme' not in data:
        return jsonify({'error': 'Missing required parameters'}), 400

    try:
        if data['theme'] == 'internet':
            agent = create_search_agent()
            context = """
        Instructions:
        1. Search the internet for relevant, up-to-date information
        2. Focus on providing accurate facts and statistics
        3. Include sources when possible
        4. Provide comprehensive but concise answers
        """
        else:
            file_path = get_file_path(data['theme'])
            if not file_path or not os.path.exists(file_path):
                return jsonify({'error': 'Invalid theme or file not found'}), 400
            agent = create_data_agent(file_path)
            context = """
            Instructions:
            1. Focus on providing clear, numerical insights and statistics
            2. Present information in a structured, easy-to-read format
            3. Include relevant metrics and counts when applicable
            4. Do not create any visualizations or graphs
            5. When comparing data, use clear numerical comparisons
            6. Provide specific examples from the data when relevant
            7. Ensure use of the 'python_repl_ast' action for execution
            """

        def run_agent():
            return agent.run(data['question'] + context)

        response, analysis_output = capture_output(run_agent)
        formatted_analysis = format_analysis_output(analysis_output)
        
        return jsonify({
            'response': response,
            'analysis_details': formatted_analysis
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)