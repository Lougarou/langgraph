from langchain.agents import AgentType, initialize_agent

from langchain_community.tools.eventstoredb.tool import EventStoreDBTool
# from langchain_community.tools.yahoo_finance_news import YahooFinanceNewsTool
from langchain_openai import ChatOpenAI

# llm = ChatOpenAI(model_name="Mistral-Nemo-Instruct-2407-Q2_K",
#                   openai_api_base="http://localhost:8080/v1",
#                   openai_api_key="sk-xxx",
#                   max_tokens=1024,
#                   temperature=0.7,
#                 verbose= True)
llm = ChatOpenAI(model_name="llama3.2",
                  openai_api_base="http://localhost:11434/v1",
                  openai_api_key="ollama",
                  max_tokens=1024,
                  temperature=0.7,
                verbose= True)

# tools = [YahooFinanceNewsTool()]
# class garbage_client():
#     def get_stream(self, stream_name):
#         return "404 Stream not found: "+stream_name
# esdb = EventStoreDBTool(esdb_client=garbage_client())
# esdb.esdb_client = garbage_client()


from esdbclient import EventStoreDBClient
esdb_client = EventStoreDBClient(
    uri="esdb://localhost:2113?Tls=false"
)

additional_information = (" The stream has flight details."
                          "The 'booked' event type means that a flight was booked."
                          "The 'delayed event type means that a flight was delayed.")

tools = [EventStoreDBTool(esdb_client=esdb_client)]

agent_chain = initialize_agent(
    tools,
    llm,
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True,
    handle_parsing_errors=True
)
# query = "Call EventStoreDB API to look up stream name 'flights-lokhesh'." + additional_information
# print(query)
# agent_chain.invoke(
#     query
# )
cust_name = "lokhesh"
agent_chain.invoke(f"What is the final status of flights-{cust_name}")

esdb_client.close() #close connection

# question = "How old are you?"
#
# messages = [
#     SystemMessage(
#         content=f"""
#         You are an assistant that answers math questions. If you need to calculate, use the calculator tool.\n\nQuestion: {question}\nAnswer:
#         """
#     ),
#     HumanMessage(content=question),
# ]

# response = llm.invoke(messages)
# print(response.content)