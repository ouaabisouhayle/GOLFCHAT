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

def get_file_path(selected_file, base_directory=""):
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
        allow_dangerous_code=True,
        max_iterations=15
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
            Context: You are a data analysis agent with multiple action capabilities.

Core Objectives:
1. Strictly use 'python_repl_ast' action for all Python code execution (put it alone in the action with no other text)
2. Provide clear, numerical insights from the dataset
3. Analyze data systematically and methodically

Execution Requirements:
- MANDATORY: Use 'python_repl_ast' as the ONLY execution environment
- Break down analysis into discrete, logical steps
- Prioritize numerical precision and statistical insights
- Avoid data visualization

Analysis Guidelines:
- Extract key metrics and statistical summaries
- Compare data points with clear numerical context
- Highlight significant patterns or anomalies
- Present findings in a structured, concise format

Prohibited Actions:
- No graphical representations
- No external library imports beyond standard Python
- No speculative or unsubstantiated claims

Reporting Format:
- Use explicit numerical comparisons
- Include specific data-driven examples
- Organize insights hierarchically
- Provide context for each statistical observation
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