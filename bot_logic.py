import os
import logging
import pandas as pd
from botbuilder.core import ActivityHandler, MessageFactory, TurnContext
from botbuilder.dialogs import Dialog, DialogSet, DialogTurnStatus, WaterfallDialog, WaterfallStepContext
from botbuilder.dialogs.prompts import TextPrompt, PromptOptions

from sqlalchemy import create_engine, inspect, text
from langchain_community.utilities import SQLDatabase
from langchain_openai.chat_models import AzureChatOpenAI

# Logging configuration
logging.basicConfig(level=logging.INFO)

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Azure OpenAI client initialization
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_API_TYPE = os.getenv("AZURE_API_TYPE")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
AZURE_DEPLOYMENT_NAME = os.getenv("AZURE_DEPLOYMENT_NAME")
MODEL_NAME = "gpt-4-32k"  # Example model name, replace with your specific model name if different

llm = AzureChatOpenAI(
    openai_api_version=OPENAI_API_VERSION,
    openai_api_key=AZURE_OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    openai_api_type=AZURE_API_TYPE,
    deployment_name=AZURE_DEPLOYMENT_NAME,
    model_name=MODEL_NAME,
    temperature=0
)

# Database constants
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_DB = "postgres"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "admin"
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
TABLE_NAME = "employees"

# Function to create a database connection
def create_database_connection() -> SQLDatabase:
    """Creates a database connection."""
    logging.info("Creating database connection")
    return SQLDatabase.from_uri(DATABASE_URL)

# Function to format the response without HTML tags
def format_response(response: str) -> str:
    """Formats the response to plain text."""
    if response.strip().startswith('['):
        try:
            df = pd.read_json(response)
            return df.to_string(index=False)
        except ValueError:
            pass
    return response.replace('<br>', '\n').replace('<p>', '').replace('</p>', '')

# Function to handle user query
def handle_query(user_question: str, db: SQLDatabase, llm: AzureChatOpenAI):
    if user_question:
        try:
            detailed_prompt = elaborate_user_input(user_question, db, llm)
            sql_query = create_sql_query_from_response(detailed_prompt, db)
            logging.info(f"Generated SQL Query: {sql_query}")  # Print the SQL query to the console
            result = execute_sql_query(sql_query, db)
            prepared_response = prepare_final_response(user_question, result, llm)
            formatted_response = format_response(prepared_response)
            return formatted_response
        except ValueError as ve:
            logging.error(f"SQL Error: {ve}")
            return "There was an issue with generating a valid SQL query. Please revise your question or contact support."
        except Exception as e:
            logging.error(f"Error processing question: {e}")
            return "Failed to process your question. Please try again."

# Function to check if the input is a greeting
def is_greeting(user_input: str) -> bool:
    greetings = ["hi", "hello", "hey", "good morning", "good afternoon", "good evening"]
    return user_input.lower().strip() in greetings

# Function to elaborate user input
def elaborate_user_input(user_question: str, db: SQLDatabase, llm: AzureChatOpenAI) -> str:
    prompt = f"Understand the user's actual requirements from the user questions and elaborate in detail using pre-trained knowledge, contextual understanding, and any other available information:\n\n{user_question}"
    response = llm.invoke(prompt)
    return response.content.strip()

# Function to create and execute SQL query
def create_sql_query_from_response(response: str, db: SQLDatabase) -> str:
    engine = create_engine(DATABASE_URL)
    inspector = inspect(engine)
    columns = [col["name"] for col in inspector.get_columns(TABLE_NAME)]
    prompt = f"You are an agent designed to interact with a SQL database. Create a syntactically correct SQL query to run for the '{TABLE_NAME}' table using these columns: {', '.join(columns)} based on the following response: {response}. Only return the SQL query itself."
    query_response = llm.invoke(prompt)
    sql_query = query_response.content.strip()

    # Ensure SQL query is valid and secure
    if "SELECT" in sql_query.upper() and TABLE_NAME in sql_query:
        column_mapping = {col: f'"{col}"' for col in columns}
        for unquoted, quoted in column_mapping.items():
            sql_query = sql_query.replace(unquoted, quoted)
        return sql_query
    else:
        logging.error("Invalid SQL generated: " + sql_query)
        raise ValueError("Generated SQL is not valid. Please check the AI's response.")

# Function to execute SQL query
def execute_sql_query(query: str, db: SQLDatabase) -> pd.DataFrame:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as connection:
        result = pd.read_sql(text(query), connection)
    return result

# Function to prepare final response
def prepare_final_response(user_question: str, result: pd.DataFrame, llm: AzureChatOpenAI) -> str:
    prompt = f"You are a chatbot. Based on the user question and retrieved data, prepare a summarised and accurate response. Do ask the user if they have follow-up questions.\n\nUser Question: {user_question}\n\nRetrieved Data:\n{result.to_dict()}"
    response = llm.invoke(prompt)
    return response.content.strip()

class MyBot(ActivityHandler):
    def __init__(self, conversation_state, user_state):
        self.conversation_state = conversation_state
        self.user_state = user_state
        self.dialog_state = self.conversation_state.create_property("DialogState")
        self.dialogs = DialogSet(self.dialog_state)
        self.dialogs.add(TextPrompt("TextPrompt"))
        self.dialogs.add(WaterfallDialog("mainDialog", [self.intro_step, self.act_step, self.final_step]))

    async def on_members_added_activity(self, members_added, turn_context: TurnContext):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity("Hello! How can I assist you with the employees database today?")

    async def on_message_activity(self, turn_context: TurnContext):
        dialog_context = await self.dialogs.create_context(turn_context)
        results = await dialog_context.continue_dialog()

        if results.status == DialogTurnStatus.Empty:
            await dialog_context.begin_dialog("mainDialog")

        await self.conversation_state.save_changes(turn_context)
        await self.user_state.save_changes(turn_context)

    async def intro_step(self, step_context: WaterfallStepContext):
        user_question = step_context.context.activity.text
        logging.info(f"Intro step user question: {user_question}")
        db = create_database_connection()
        if is_greeting(user_question):
            response = "Hello! How can I assist you with the employees database today?"
        else:
            response = handle_query(user_question, db, llm)
        await step_context.context.send_activity(MessageFactory.text(response))
        return await step_context.end_dialog()

    async def act_step(self, step_context: WaterfallStepContext):
        user_question = step_context.result
        logging.info(f"Act step user question: {user_question}")
        db = create_database_connection()
        response = handle_query(user_question, db, llm)
        await step_context.context.send_activity(MessageFactory.text(response))
        return await step_context.next(None)

    async def final_step(self, step_context: WaterfallStepContext):
        return await step_context.end_dialog()
