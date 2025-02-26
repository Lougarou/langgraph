#https://github.com/langchain-ai/langgraph/issues/142
import IPython
# from langchain.globals import set_debug
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
model = ChatOpenAI(#model_name="llama3.2",
                   model_name="deepseek-r1",
                   openai_api_base="http://localhost:11434/v1",
                   openai_api_key="ollama",
                   max_tokens=1024,
                   temperature=0.5,
                   verbose=True).bind_tools([
                    #TODO: add our cool ESDB tool here
                ])

def random_delay(func):
    def wrapper(*args, **kwargs):
        time.sleep(random.randint(1, 3))
        return func(*args, **kwargs)  # Call the original function
    return wrapper

def highlight_ui(node: str, message: str = ""):
    url = "http://localhost:5000/update"  # Change this if your Flask app is running on a different host/port
    if message == "":
        node + " called..."
    payload = {
        "nodes": [node],  # Replace with the actual node ID(s) you want to highlight
        "message": message
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
    if right is None:
        right = []
    if left is None:
        left = []
    for el in right:
        if el not in left:
            left.append(el)
    return left

class State(TypedDict):
    updates: Annotated[Sequence[AnyStr], add_strings]
    messages: Annotated[Sequence[BaseMessage], add_messages]

def call_model(state: State):
    highlight_ui("Call LLM")
    return state

def human_feedback(state: State):
    highlight_ui("human feedback", "waiting for user question")
    feedback = input("How can I help you?: ")
    if "updates" not in state:
        state["updates"] = []
    state["updates"].append(feedback)
    # feedback = "skipping step for checkpointer demo"
    state['messages'].append(HumanMessage(content=feedback))
    return state

def call_model(state: MessagesState):
    # response = model.invoke(state['messages'])
    highlight_ui("Call LLM", "Not calling LLM for now")
    return state
    # return {"messages": [response]}


@random_delay
def metadata(state: State):
    highlight_ui("find metadata")
    return state

@random_delay
def get_similar_ticket_from_vector_db(state: State):
    highlight_ui("get similar ticket from vector db")
    from support_agent_workflow_vector_ticket_search import search
    if len(state['updates']) == 0:
        return state
    user_query = state['updates'][-1]
    ticket_id, response, score = search(query=user_query)
    response = "BACKGROUND KNOWLEDGE on question. Use this to formulate a response: "
    state['messages'].append(SystemMessage(content=response))
    response = "END OF BACKGROUND KNOWLEDGE."
    highlight_ui("get similar ticket from vector db",
                 "Found ticket: https://eventstore.freshdesk.com/a/tickets/"+str(ticket_id)
                 + " Similarity score: "+str(score))

    return state

@random_delay
def get_similar_changelog_from_vector_db(state: State):
    from support_agent_workflow_changelog import search_faiss
    if len(state['updates']) == 0:
        return state
    user_query = state['updates'][-1]
    results = search_faiss(user_query)
    knowledge = "Add the following to your solution. Suggest to check the following links: "
    for result in results:
        knowledge = knowledge + result + "\n"
    state['messages'].append(SystemMessage(content=knowledge))
    highlight_ui("use changelog to find bugs from vector db", knowledge)
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
    state['messages'].append(HumanMessage(content="Write a solution in HTML Formatting as if you are replying to a customer and break the answer into smaller paragraphs <p> . Use all the knowledge you have in context give the HUMAN a solution to his question. Add links of freshdesk and github pull request at the end under More Information."))
    response = model.invoke(state['messages'])
    print("SOLUTION: ")
    print(response)
    solution = ""
    """
    for message in response:
        solution = solution + message + "\n"
    highlight_ui("output suggestion", solution)
    """
    highlight_ui("output suggestion", response.content)
    return {"messages": [response]}

# Build graph

builder = StateGraph(State)
builder.add_edge(START, "human feedback")

subgraph_deep_dive = StateGraph(State)
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
builder.add_node("Call LLM", call_model)
builder.add_node("decide next action", decide_next_action)
builder.add_node("output suggestion", output_suggestion)
builder.add_edge("human feedback", "Call LLM")
builder.add_edge("human feedback", "analytics subgraph")
builder.add_edge("analytics subgraph", "decide next action")
builder.add_edge("Call LLM", "decide next action")
builder.add_edge("decide next action", "output suggestion")

builder.set_entry_point("human feedback")
builder.set_finish_point("output suggestion")

# Add
esdb_client = EventStoreDBClient(
    uri="esdb://localhost:2113?Tls=false"
)

kurrentdb_checkpointer = KurrentDBSaver(esdb_client)
# checkpointer = MemorySaver()
graph = builder.compile(checkpointer=kurrentdb_checkpointer)
# kurrentdb_checkpointer.set_max_count(5, thread_id=42)

messages = {"messages": [
    SystemMessage(content="Reply as a customer support engineer for EventStoreDB or KurrentDB. "
                          +"Build your response based on the context you have gathered from the user."
                          "The user will ask a question next and you will gather everything in your context to give a solution."
                           "Format all output as HTML."),
]}
# NORMAL RUN
result = graph.invoke(
    messages,
    config={"configurable": {"thread_id": 42}}
)

# Replay a state
# result = graph.invoke(
#     messages,
#     config={"configurable": {"thread_id": 42, "checkpoint": "1eff4344-d342-63bd-8002-be5bd3fc9ca7"}}
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