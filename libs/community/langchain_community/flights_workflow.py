from typing import Annotated, Literal, TypedDict
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph, MessagesState
from langgraph.prebuilt import ToolNode
from langchain_community.tools.eventstoredb.tool import EventStoreDBTool
from langchain.globals import set_debug

set_debug(True)

from esdbclient import EventStoreDBClient

esdb_client = EventStoreDBClient(
    uri="esdb://localhost:2113?Tls=false"
)


# Define the tools for the agent to use
@tool
def search(query: str):
    """Call to surf the web."""
    print("Search was called!")
    # This is a placeholder, but don't tell the LLM that...
    if "sf" in query.lower() or "san francisco" in query.lower():
        return "It's 60 degrees and foggy."
    return "It's 90 degrees and sunny."


@tool
def hotel(query: str):
    """ Call to find an available hotel"""
    print("hotel function was called!")
    if "hotel" in query.lower():
        return "Hilton is available"
    return "no hotels available"


@tool
def call_eventstore_on_flights(query: str):
    """Call to find flight status details"""
    return EventStoreDBTool(esdb_client=esdb_client).invoke(query)


tools = [search, hotel, call_eventstore_on_flights]

tool_node = ToolNode(tools)

# model = ChatAnthropic(model="claude-3-5-sonnet-20240620", temperature=0).bind_tools(tools)
model = ChatOpenAI(model_name="llama3.2",
                   openai_api_base="http://localhost:11434/v1",
                   openai_api_key="ollama",
                   max_tokens=1024,
                   temperature=0.0,
                   verbose=True).bind_tools(tools)


# Define the function that determines whether to continue or not
def should_continue(state: MessagesState) -> Literal["tools", END]:
    messages = state['messages']
    last_message = messages[-1]
    # If the LLM makes a tool call, then we route to the "tools" node
    if last_message.tool_calls:
        return "tools"
    # Otherwise, we stop (reply to the user)
    return END


# Define the function that calls the model
def call_model(state: MessagesState):
    messages = state['messages']
    response = model.invoke(messages)
    # We return a list, because this will get added to the existing list
    return {"messages": [response]}


# Define a new graph
workflow = StateGraph(MessagesState)

# Define the two nodes we will cycle between
workflow.add_node("agent", call_model)
workflow.add_node("tools", tool_node)

# Set the entrypoint as `agent`
# This means that this node is the first one called
workflow.add_edge(START, "agent")

# We now add a conditional edge
workflow.add_conditional_edges(
    # First, we define the start node. We use `agent`.
    # This means these are the edges taken after the `agent` node is called.
    "agent",
    # Next, we pass in the function that will determine which node is called next.
    should_continue,
)

# We now add a normal edge from `tools` to `agent`.
# This means that after `tools` is called, `agent` node is called next.
workflow.add_edge("tools", 'agent')

# Initialize memory to persist state between graph runs
checkpointer = MemorySaver()

# Finally, we compile it!
# This compiles it into a LangChain Runnable,
# meaning you can use it as you would any other runnable.
# Note that we're (optionally) passing the memory when compiling the graph
app = workflow.compile(checkpointer=checkpointer)

messages = {"messages": [
    SystemMessage(content="eventstoredb_query should be called with tool_input as parameter instead of query."),
    HumanMessage(content="Can you call EventStoreDB API with stream name flights-lokhesh")
]}

# Use the Runnable
final_state = app.invoke(
    {"messages": [HumanMessage(content="Can you call EventStoreDB API with stream name flights-lokhesh")]},
    config={"configurable": {"thread_id": 42}}
)
print(final_state["messages"][-1].content)
