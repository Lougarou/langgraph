#https://github.com/langchain-ai/langgraph/issues/142

from langchain.globals import set_debug
# /# set_debug(True)

from langgraph.checkpoint.memory import MemorySaver
from esdbclient import EventStoreDBClient
from langchain_core.tools import tool
from langgraph.prebuilt import ToolNode

from langgraph.graph import StateGraph, START, END, MessagesState
from typing import Annotated, Literal
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
import uuid
import pandas as pd
from IPython.display import Image, display
# Connection to a local LLM hosted on Ollama
model = ChatOpenAI(model_name="llama3.2",
                   openai_api_base="http://localhost:11434/v1",
                   openai_api_key="ollama",
                   max_tokens=1024,
                   temperature=0.0,
                   verbose=True).bind_tools([
                    #TODO: add our cool ESDB tool here
                ])


def call_model(state):
    return state

def human_feedback(state):
    feedback = input("User feedback requested: ")
    state['messages'].append(HumanMessage(content=feedback))
    return state

def call_model(state: MessagesState):
    print("LLM is processing the request...")
    response = model.invoke(state['messages'])
    # Prebuilt Toolnode outputs to state['messages']
    if 'tool_calls' in response.additional_kwargs:
        print("LLM needs to call a tool...")
    else:

        print("Not calling a tool")
    return {"messages": [response]}

def get_similar_ticket_from_vector_db(state):
    return state

def get_similar_github_issues_from_vector_db(state):
    return state

def get_possible_issues_by_version(state):
    return state

def diagnose_stats_file(state):
    return state

def output_suggestion(state):
    return state



# Build graph
builder = StateGraph(MessagesState)
# Logic
builder.add_edge(START, "human feedback")
builder.add_edge("human feedback", "LLM Agent")


checkpointer = MemorySaver()
# Add
graph = builder.compile(checkpointer=checkpointer)