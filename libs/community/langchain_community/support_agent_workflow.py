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
def execution_time(func):
    def wrapper(*args, **kwargs):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Get current local time
        print(f"{current_time}: {func.__name__}")
        return func(*args, **kwargs)  # Call the original function

    return wrapper

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
    return state

def human_feedback(state: State):
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

@execution_time
def metadata(state: State):
    return state

@execution_time
def get_similar_ticket_from_vector_db(state: State):
    return state

@execution_time
def get_similar_changelog_from_vector_db(state: State):
    return state

@execution_time
def diagnose_stats_file(state: State):
    return state

@execution_time
def compile_analysis(state: State):
    return state

@execution_time
def decide_next_action(state: State):
    return state

@execution_time
def output_suggestion(state: State):
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
# graph = builder.compile(checkpointer=MemorySaver())

messages = {"messages": [
    SystemMessage(content="I think I have a memory leak")
]}

result = graph.invoke(
    messages,
    config={"configurable": {"thread_id": 42}}
)

#visualize graph

# print(graph.get_graph(xray=True).draw_ascii())
# print(subgraph.get_graph().draw_ascii())
# from IPython.display import Image
# Image(graph.get_graph(xray=True).draw_mermaid_png(output_file_path="graph.png"))


esdb_client.close()