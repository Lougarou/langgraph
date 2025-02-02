import json
from typing import Optional, Any, Type
from langchain_core.callbacks import CallbackManagerForToolRun
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool
from pydantic import ConfigDict
from pydantic import BaseModel, Field


class EventStoreDBInput(BaseModel):
    """Input for the EventStoreDB tool."""

    query: str = Field(description="Event Store stream to look up")

class EventStoreDBTool(BaseTool):
    """Base tool for querying a EventStoreDB API."""

    name: str = "eventstoredb_query"

    esdb_client: Any # Pass esdbclient
    structured: bool # Whether the output should be JSON or not

    description: str = """\
    Input to this tool is one single word stream name that needs to be read from EventStoreDB.
    EventStoreDB is a NoSQL database which uses streams to store data.
    If the stream is not found then return a 404 stream not found error.
    If another error happens return a 500 error.
    \
    """

    args_schema: Type[BaseModel] = EventStoreDBInput

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    def _run(
        self,
        tool_input: str,
        config: RunnableConfig,
        run_manager: Optional[CallbackManagerForToolRun] = None,#enables tracing
        **kwargs  # Accept additional keyword arguments
    ) -> str:
        # esdb_client = config["esdb_client"]

        events = self.esdb_client.get_stream(
                stream_name=tool_input,
                resolve_links=True
        )
        if (self.structured == False):
            result = "Start of stream: "
            for event in events:
                result += "An event of type: "+event.type+" has occurred with details: "+event.data.decode("utf-8")+". Then "
            result += " End of stream. NO FURTHER ACTION REQUIRED."
        else:
            result = events

        # return json.dumps({"story": result}, indent=2) #json.dumps(result, indent=2)
        return result

    # TODO: error handling
    # TODO: implement async
    #TODO: implement streaming