#TODO: Issue it looks like Langchain has code in the 'core' libs that do memory checkpointing and not maintained
# inside the langchain community project. This means that we will need to first open a discussion with the maintainers
# and then once approved we can make a PR. A draft PR would help make our case though. They did accept a PR from SQLite
# Source: https://github.com/langchain-ai/langgraph/tree/c0db7f4d098982cf7c34600b4ed7d177c0b68b5a/libs/checkpoint
# Where to contribute: https://github.com/langchain-ai/langgraph/tree/c0db7f4d098982cf7c34600b4ed7d177c0b68b5a/libs
# We most likely need to implement asynchronous support to get accepted (should not be a blocker, will just take more
# time to implement) but that could also be an Enterprise Feature
# Checkpoint size can be a problem if it gets more than 16mb (max supported by KurrentDB). We might need to design the
# checkpoint ids to point a unique stream which we then build like a read model. Not sure how the writes will be done
# to make sure they are in 16mb chunks.
# Design concern: Implementing pending intermediate writes might be a challenge.


# from langchain_community.chat_models import ChatOpenAI
# llm = ChatOpenAI(model_name="llama3.2",
#                   openai_api_base="http://localhost:11434/v1",
#                   openai_api_key="ollama",
#                   max_tokens=1024,
#                   temperature=0.7,
#                 verbose= True)


import sqlite3

import esdbclient.exceptions
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph
from esdbclient.exceptions import (
    DiscoveryFailed,
    FollowerNotFound,
    GrpcError,
    LeaderNotFound,
    NodeIsNotLeader,
    NotFound,
    ReadOnlyReplicaNotFound,
    ServiceUnavailable,
)
def test_run_list_checkpoints():
    pass
    # with SqliteSaver.from_conn_string(":memory:") as memory:
    # config = {"configurable": {"thread_id": "1"}}
    # checkpoints = list(memory.list(config, limit=2))
    # print(checkpoints)
    # kpointTuple(...), CheckpointTuple(...)]
    #
    # config = {"configurable": {"thread_id": "1"}}
    # before = {"configurable": {"checkpoint_id": "1ef4f797-8335-6428-8001-8a1503f9b875"}}
    # with SqliteSaver.from_conn_string(":memory:") as memory:
    # ..  # Run a graph, then list the checkpoints
    # checkpoints = list(memory.list(config, before=before))
    # print(checkpoints)




import random
import sqlite3
import threading
from typing import Any, AsyncIterator, Dict, Iterator, Optional, Sequence, Tuple

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.checkpoint.serde.types import ChannelProtocol


from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    SerializerProtocol,
    get_checkpoint_id,
)
_AIO_ERROR_MSG = (
    "Asynchronous checkpointer is only available in the Enterprise version of KurrentDB Checkpointer. "
    "Find out more at https://www.kurrent.io/talk_to_expert"
)

"""
put - Store a checkpoint with its configuration and metadata.
.put_writes - Store intermediate writes linked to a checkpoint (i.e. pending writes).
.get_tuple - Fetch a checkpoint tuple using for a given configuration (thread_id and thread_ts).
.list - List checkpoints that match a given configuration and filter criteria.
"""

from esdbclient import EventStoreDBClient, NewEvent, StreamState
from collections import defaultdict
class KurrentDBSaver(BaseCheckpointSaver[str]):
    """A KurrentDB-based checkpoint saver.
    Requirements:
    - by_category system projections enabled
    - $ce-thread stream should be empty
    thread-checkpoint_id streams are used to keep checkpoints of each thread
    """
    client: EventStoreDBClient

    writes: defaultdict[ #TODO: find a way to implement this inside KurrentDB
        tuple[str, str, str],
        dict[tuple[str, int], tuple[str, str, tuple[str, bytes], str]],
    ]
    def __init__(
        self,
        client: EventStoreDBClient,
        *,
        serde: Optional[SerializerProtocol] = None,
        factory: type[defaultdict] = defaultdict,
    ) -> None:
        super().__init__(serde=serde)
        self.jsonplus_serde = JsonPlusSerializer()
        self.client = client
        self.lock = threading.Lock()
        self.writes = factory(dict)

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        print("invoked get tuple")
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "") #TODO: implement parent and namespace
        checkpoint_id = get_checkpoint_id(config)
        thread_id = config["configurable"]["thread_id"]
        try:
            checkpoints_events = self.client.get_stream(
                stream_name="thread-" + thread_id,
                resolve_links=True,
                backwards=True
            )
        except esdbclient.exceptions.NotFound as e:
            print(e)
            return None #no checkpoint found

        for event in checkpoints_events:
            checkpoint = self.jsonplus_serde.loads(event.data)
            metadata = self.jsonplus_serde.loads(event.metadata)
            if checkpoint_id is None: #just return latest checkpoint
                return CheckpointTuple(
                {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint["id"],
                    }
                },
                checkpoint,
                metadata,
                None, #TODO: need to implement pending writes
                None, #TODO: need to implement parent checkpoint
            )
            elif checkpoint["id"] == checkpoint_id:
                return CheckpointTuple(
                {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint["id"],
                    }
                },
                checkpoint,
                metadata,
                None, #TODO: need to implement pending writes
                None, #TODO: need to implement parent checkpoint
            )
        raise Exception("Could not find checkpoint")

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        print("invoked list")
        if filter is not None or before is not None or limit is not None:
            raise NotImplementedError("Filtering, before, and limit are not supported yet")

        #Read thread category stream $ce-thread
        #this will give us all thread streams and then we can read those to find the checkpoints
        streams_events = self.client.get_stream(
            stream_name="$ce-thread",
            resolve_links=True
        )
        for event in streams_events:
            thread_id = event.stream_name.split("-")[1]
            config = self.jsonplus_serde.loads(self.client.get_stream(
                stream_name="config-"+thread_id,
                resolve_links=True
            )[0].data)
            print(thread_id,config)

            checkpoint =  self.jsonplus_serde.loads(event.data)
            metadata = self.jsonplus_serde.loads(event.metadata)

            yield CheckpointTuple(
                {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": config['configurable']["checkpoint_ns"],
                        "checkpoint_id": checkpoint['id'],
                    }
                },
                checkpoint,
                metadata,
                None, #TODO: need to implement pending writes
                None, #TODO: need to implement parent checkpoint
            )

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        print("invoked put")
        """
        Store a checkpoint with its configuration and metadata.
        TODO: Implement error handling
        """

        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"]["checkpoint_ns"]

        serialized_checkpoint = JsonPlusSerializer().dumps(checkpoint)
        serialized_metadata = JsonPlusSerializer().dumps(metadata)
        serialized_config = JsonPlusSerializer().dumps(config)
        checkpoint_event = NewEvent(
            type="langgraph_checkpoint",
            data=serialized_checkpoint,
            metadata=serialized_metadata,
            content_type='application/octet-stream',
        )
        self.client.append_to_stream(
            stream_name=f"thread-{thread_id}",
            events=[checkpoint_event],
            current_version=StreamState.ANY #TODO: check versioning
        )
        config_event = NewEvent(
            type="langgraph_checkpoint_config",
            data=serialized_config,
            content_type='application/octet-stream',
        )
        self.client.append_to_stream(
            stream_name=f"config-{thread_id}",
            events=[config_event],
            current_version=StreamState.ANY #TODO: check versioning
        )
        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        print("invoked put writes")
        """TODO: current implentation is in memory taken from the MemorySaver.
        This needs to be implemented in KurrentDB.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = config["configurable"]["checkpoint_id"]
        outer_key = (thread_id, checkpoint_ns, checkpoint_id)
        outer_writes_ = self.writes.get(outer_key)
        for idx, (c, v) in enumerate(writes):
            inner_key = (task_id, WRITES_IDX_MAP.get(c, idx))
            if inner_key[1] >= 0 and outer_writes_ and inner_key in outer_writes_:
                continue

            self.writes[outer_key][inner_key] = (
                task_id,
                c,
                self.serde.dumps_typed(v),
                task_path,
            )

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        raise NotImplementedError(_AIO_ERROR_MSG)

    async def alist(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[CheckpointTuple]:
        raise NotImplementedError(_AIO_ERROR_MSG)
        yield

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        raise NotImplementedError(_AIO_ERROR_MSG)

    def get_next_version(self, current: Optional[str], channel: ChannelProtocol) -> str:
        print("invoked get next version")
        """Generate the next version ID for a channel.

        This method creates a new version identifier for a channel based on its current version.

        Args:
            current (Optional[str]): The current version identifier of the channel.
            channel (BaseChannel): The channel being versioned.

        Returns:
            str: The next version identifier, which is guaranteed to be monotonically increasing.
        """
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(current.split(".")[0])
        next_v = current_v + 1
        next_h = random.random()
        return f"{next_v:032}.{next_h:016}"


def test_put_checkpoint():

    esdb_client = EventStoreDBClient(
        uri="esdb://localhost:2113?Tls=false"
    )

    memory = KurrentDBSaver(esdb_client)
    config = {"configurable": {"thread_id": "1", "checkpoint_ns": ""}}
    checkpoint = {"ts": "2024-05-04T06:32:42.235444+00:00", "id": "1ef4f797-8335-6428-8001-8a1503f9b875",
                  "channel_values": {"key": "value"}}
    saved_config = memory.put(config, checkpoint, {"source": "input", "step": 1, "writes": {"key": "value"}}, {})
    print(saved_config)

def test_list_checkpoints():
    esdb_client = EventStoreDBClient(
        uri="esdb://localhost:2113?Tls=false"
    )
    memory = KurrentDBSaver(esdb_client)
    config = {"configurable": {"thread_id": "1"}}
    checkpoints = list(memory.list(config))
    print(checkpoints)


def test_run_graph():
    esdb_client = EventStoreDBClient(
        uri="esdb://localhost:2113?Tls=false"
    )

    memory = KurrentDBSaver(esdb_client)

    builder = StateGraph(int)
    builder.add_node("add_one", lambda x: x + 1)
    builder.set_entry_point("add_one")
    builder.set_finish_point("add_one")

    # graph = builder.compile(checkpointer=MemorySaver())
    graph = builder.compile(checkpointer=memory)
    config = {"configurable": {"thread_id": "1"}}
    graph.get_state(config)
    print(graph.get_state(config))
    result = graph.invoke(3, config)
    graph.get_state(config)


def test_get_checkpoint():
    esdb_client = EventStoreDBClient(
        uri="esdb://localhost:2113?Tls=false"
    )
    memory = KurrentDBSaver(esdb_client)
    config = {"configurable": {"thread_id": "1"}}
    checkpoint_tuple = memory.get_tuple(config)
    print(checkpoint_tuple)

def test_get_tuple():
    esdb_client = EventStoreDBClient(
        uri="esdb://localhost:2113?Tls=false"
    )
    memory = KurrentDBSaver(esdb_client)

    config = {"configurable": {"thread_id": "1"}}
    checkpoint_tuple = memory.get_tuple(config)
    print(checkpoint_tuple)

    # with checkpoint_id
    config = {
        "configurable": {
            "thread_id": "1",
            "checkpoint_ns": "",
            "checkpoint_id": "1ef4f797-8335-6428-8001-8a1503f9b875",
        }
    }
    checkpoint_tuple = memory.get_tuple(config)
    print(checkpoint_tuple)

# test_put_checkpoint()
# test_list_checkpoints()
# test_get_checkpoint()
# test_get_tuple()
test_run_graph()
