#https://github.com/langchain-ai/langgraph/issues/142
import IPython
from langchain.globals import set_debug
# set_debug(True)
from kurrentdb_memory_saver_prototype import KurrentDBSaver
from langgraph.checkpoint.memory import MemorySaver
from esdbclient import EventStoreDBClient
from langchain_core.tools import tool
from langgraph.prebuilt import ToolNode
import operator
from langgraph.graph import StateGraph, START, END, MessagesState
from typing import Annotated, Literal, TypedDict
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from datetime import datetime
import random
import time
import requests
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

def random_delay(func):
    def wrapper(*args, **kwargs):
        time.sleep(random.randint(1, 5))
        return func(*args, **kwargs)  # Call the original function
    return wrapper

def highlight_ui(node: str):
    url = "http://localhost:5000/update"  # Change this if your Flask app is running on a different host/port
    payload = {
        "nodes": [node],  # Replace with the actual node ID(s) you want to highlight
        "message": node + " called..."
    }
    response = requests.post(url, json=payload)
    return response

from typing import Sequence, AnyStr
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages

def add_strings(
    left: AnyStr,
    right: AnyStr,
) -> AnyStr:
    return left + right
class State(TypedDict):
    # The operator.add reducer fn makes this append-only
    updates: Annotated[Sequence[AnyStr], add_strings]
    messages: Annotated[Sequence[BaseMessage], add_messages]

def call_model(state: State):
    highlight_ui("LLM Agent")
    return state

def human_feedback(state: State):
    highlight_ui("human feedback")
    # feedback = input("User feedback requested: ")
    feedback = "skipping step for checkpointer demo"
    state['messages'].append(HumanMessage(content=feedback))
    return state

def call_model(state: MessagesState):
    highlight_ui("LLM Agent")
    print("LLM is processing the request...")
    response = model.invoke(state['messages'])
    return {"messages": [response]}


@random_delay
def metadata(state: State):
    highlight_ui("find metadata")
    return state

@random_delay
def get_similar_ticket_from_vector_db(state: State):
    highlight_ui("get similar ticket from vector db")
    return state

@random_delay
def get_similar_changelog_from_vector_db(state: State):
    highlight_ui("use changelog to find bugs from vector db")
    return state

@random_delay
def diagnose_stats_file(state: State):
    highlight_ui("diagnose stats file")
    return state

@random_delay
def compile_analysis(state: State):
    highlight_ui("compile analysis")
    return state

@random_delay
def decide_next_action(state: State):
    highlight_ui("decide next action")
    return state

@random_delay
def output_suggestion(state: State):
    highlight_ui("output suggestion")
    return state

# Build graph

builder = StateGraph(MessagesState)
builder.add_edge(START, "human feedback")

subgraph_deep_dive = StateGraph(MessagesState)
subgraph_deep_dive.add_node("find metadata", metadata)
subgraph_deep_dive.add_node("compile analysis", compile_analysis)
subgraph_deep_dive.add_node("diagnose stats file", diagnose_stats_file)
subgraph_deep_dive.add_node("get similar ticket from vector db", get_similar_ticket_from_vector_db)
subgraph_deep_dive.add_node("use changelog to find bugs from vector db", get_similar_changelog_from_vector_db)

subgraph_deep_dive.add_edge("find metadata", "diagnose stats file")
subgraph_deep_dive.add_edge("find metadata", "get similar ticket from vector db")
subgraph_deep_dive.add_edge("find metadata", "use changelog to find bugs from vector db")
subgraph_deep_dive.add_edge("diagnose stats file", "compile analysis")
subgraph_deep_dive.add_edge("get similar ticket from vector db", "compile analysis")
subgraph_deep_dive.add_edge("use changelog to find bugs from vector db", "compile analysis")
subgraph_deep_dive.set_entry_point("find metadata")
subgraph_deep_dive.set_finish_point("compile analysis")
subgraph = subgraph_deep_dive.compile()

builder.add_node("analytics subgraph", subgraph)
builder.add_node("human feedback", human_feedback)
builder.add_node("LLM Agent", call_model)
builder.add_node("decide next action", decide_next_action)
builder.add_node("output suggestion", output_suggestion)
builder.add_edge("human feedback", "LLM Agent")
builder.add_edge("human feedback", "analytics subgraph")
builder.add_edge("analytics subgraph", "decide next action")
builder.add_edge("LLM Agent", "decide next action")
builder.add_edge("decide next action", "output suggestion")

builder.set_entry_point("human feedback")
builder.set_finish_point("output suggestion")
checkpointer = MemorySaver()
# Add
esdb_client = EventStoreDBClient(
    uri="esdb://localhost:2113?Tls=false"
)

kurrentdb_checkpointer = KurrentDBSaver(esdb_client)
graph = builder.compile(checkpointer=kurrentdb_checkpointer)
kurrentdb_checkpointer.set_max_count(5, thread_id=42)

messages = {"messages": [
    SystemMessage(content="I am a useful support engineer.")
]}
# NORMAL RUN
result = graph.invoke(
    messages,
    config={"configurable": {"thread_id": 42}}
)

#Replay graph
# result = graph.invoke(
#     messages,
#     config={"configurable": {"thread_id": 42, "checkpoint": "1eff37aa-935a-69ae-8003-703961051a99"}}
# )

#visualize graph
# print(graph.get_graph(xray=True).draw_ascii())
# print(subgraph.get_graph().draw_ascii())
# from IPython.display import Image
# Image(graph.get_graph(xray=True).draw_mermaid_png(output_file_path="graph.png"))


#HISTORY
# config = {"configurable": {"thread_id": "42"}}
# for state in graph.get_state_history(config):
#     print(state)
#     print("--")

#HOT PATH
# kurrentdb_checkpointer.hot_path(thread_id=42)


esdb_client.close()