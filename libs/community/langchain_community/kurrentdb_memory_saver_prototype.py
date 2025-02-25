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
import json

import esdbclient.exceptions
from langgraph.graph import StateGraph
import random
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
    get_checkpoint_id
)
_AIO_ERROR_MSG = (
    "Asynchronous checkpointer is only available in the Enterprise version of KurrentDB Checkpointer. "
    "Find out more at https://www.kurrent.io/talk_to_expert"
)
import pandas as pd
import matplotlib.pyplot as plt

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
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "") #TODO: implement parent and namespace
        checkpoint_id = get_checkpoint_id(config)
        thread_id = config["configurable"]["thread_id"]
        try:
            checkpoints_events = self.client.get_stream(
                stream_name="thread-" + str(thread_id),
                resolve_links=True,
                backwards=True
            )
        except esdbclient.exceptions.NotFound as e:
            # print(e)
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
            checkpoint =  self.jsonplus_serde.loads(event.data)
            metadata = self.jsonplus_serde.loads(event.metadata)

            parent_checkpoint_id = None
            if "checkpoint_ns" in checkpoint and checkpoint["checkpoint_ns"] is not None:
                if checkpoint["checkpoint_ns"] != config["configurable"]["checkpoint_ns"]:
                    continue
                else:
                    parent_checkpoint_id = checkpoint["checkpoint_ns"]

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
        """
        Store a checkpoint with its configuration and metadata.
        TODO: Implement error handling
        """
        # c = checkpoint.copy()
        # c.pop("pending_sends")  # type: ignore[misc]

        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"]["checkpoint_ns"]
        checkpoint["checkpoint_ns"] = checkpoint_ns
        serialized_checkpoint = self.jsonplus_serde.dumps(checkpoint)
        serialized_metadata = self.jsonplus_serde.dumps(metadata)

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

    def hot_path(self, thread_id: int):
        try:
            checkpoints_events = self.client.get_stream(
                stream_name="thread-" + str(thread_id),
                resolve_links=True,
                backwards=False #read forwards
            )
            time_map = {}
            for event in checkpoints_events:
                checkpoint = self.jsonplus_serde.loads(event.data)
                # metadata = self.jsonplus_serde.loads(event.metadata)
                if "channel_versions" in checkpoint:
                    for el in checkpoint["channel_versions"]:
                        if el not in time_map or "start:" in el:
                            time_map[el] = event.recorded_at
            # Sort events by datetime only
            events = sorted(time_map.items(), key=lambda x: x[1])

            # Compute time differences between consecutive events
            execution_times = []
            previous_key, previous_time = events[0]
            for key, current_time in events[1:]:
                execution_time = (current_time - previous_time).total_seconds()
                execution_times.append((previous_key, key, execution_time))
                previous_key, previous_time = key, current_time

            df = pd.DataFrame(execution_times,
                              columns=['Previous Event', 'Current Event', 'Execution Time (seconds)'])
            print(df)

            # Plot Pie Chart
            plt.figure(figsize=(8, 8))
            plt.pie(df['Execution Time (seconds)'], labels=df['Current Event'], autopct='%1.1f%%', startangle=140)
            plt.title('Execution Time Distribution')
            plt.show()

        except Exception as e:
            print(f"Error: {e}")
            return None
    def set_max_count(self, max_count: int, thread_id: int) -> None:
        #TODO: lots of sanity checks and merging metadata
        stream_name = "thread-" + str(thread_id)
        # metadata = self.client.get_stream_metadata(stream_name=stream_name)
        metadata = {"$maxCount": max_count}
        self.client.set_stream_metadata(
            stream_name=stream_name,
            metadata=metadata,
        )
    def set_max_age(self, max_count: int, thread_id) -> None:
        raise NotImplementedError(_AIO_ERROR_MSG)

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

def test_subgraph():
    esdb_client = EventStoreDBClient(
        uri="esdb://localhost:2113?Tls=false"
    )

    memory = KurrentDBSaver(esdb_client)

    builder = StateGraph(int)
    builder.add_node("add_one", lambda x: x + 1)
    builder.set_entry_point("add_one")

    subgraph_builder = StateGraph(int)
    subgraph_builder.add_node("add_two", lambda x: x + 2)
    subgraph_builder.set_entry_point("add_two")
    subgraph_builder.set_finish_point("add_two")
    # subgraph = subgraph_builder.compile(checkpointer=MemorySaver())
    subgraph = subgraph_builder.compile(checkpointer=memory)
    builder.add_node("subgraph", subgraph)
    builder.add_edge("add_one", "subgraph")
    builder.set_finish_point("subgraph")


    graph = builder.compile(checkpointer=memory)
    # graph = builder.compile(checkpointer=MemorySaver())

    config = {"configurable": {"thread_id": "1"}}

    result = graph.invoke(3, config)
    for state in graph.get_state_history(config):
        print(state)
        print("--")
    print("result: ", result)
    graph.get_state(config)

def test_hot_path():
    esdb_client = EventStoreDBClient(
        uri="esdb://localhost:2113?Tls=false"
    )
    memory = KurrentDBSaver(esdb_client)
    memory.hot_path(esdb_client, 42)

# test_put_checkpoint()
# test_list_checkpoints()
# test_get_checkpoint()
# test_get_tuple()
# test_run_graph()
# test_subgraph()
# test_hot_path()