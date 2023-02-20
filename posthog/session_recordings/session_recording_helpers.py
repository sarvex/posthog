import dataclasses
import json
from collections import defaultdict
from typing import Any, DefaultDict, Dict, Generator, List, Optional

from sentry_sdk.api import capture_exception, capture_message

from posthog.models import utils
from posthog.models.session_recording.metadata import RecordingSnapshotsData, SnapshotDataTaggedWithWindowId
from posthog.session_recordings.session_recording_data import (
    RRWEB_MAP_EVENT_TYPE,
    compress_to_string,
    decompress,
    get_events_summary_from_snapshot_data,
)

Event = Dict[str, Any]


# Session Recording Clickhouse helpers
# Due to the way we store items in Kafka/Clickhouse, we need to compress and chunk the session recording data before sending it to Kafka.


def preprocess_session_recording_events_for_clickhouse(events: List[Event]) -> List[Event]:
    result = []
    snapshots_by_session_and_window_id = defaultdict(list)
    for event in events:
        if is_unchunked_snapshot(event):
            session_id = event["properties"]["$session_id"]
            window_id = event["properties"].get("$window_id")
            snapshots_by_session_and_window_id[(session_id, window_id)].append(event)
        else:
            result.append(event)

    for _, snapshots in snapshots_by_session_and_window_id.items():
        result.extend(list(compress_and_chunk_snapshots(snapshots)))

    return result


def compress_and_chunk_snapshots(events: List[Event], chunk_size=512 * 1024) -> Generator[Event, None, None]:
    data_list = [event["properties"]["$snapshot_data"] for event in events]
    session_id = events[0]["properties"]["$session_id"]
    has_full_snapshot = any(snapshot_data["type"] == RRWEB_MAP_EVENT_TYPE.FullSnapshot for snapshot_data in data_list)
    window_id = events[0]["properties"].get("$window_id")

    compressed_data = compress_to_string(json.dumps(data_list))

    id = str(utils.UUIDT())
    chunks = chunk_string(compressed_data, chunk_size)

    for index, chunk in enumerate(chunks):
        yield {
            **events[0],
            "properties": {
                **events[0]["properties"],
                "$session_id": session_id,
                "$window_id": window_id,
                # If it is the first chunk we include all events
                "$snapshot_data": {
                    "chunk_id": id,
                    "chunk_index": index,
                    "chunk_count": len(chunks),
                    "data": chunk,
                    "compression": "gzip-base64",
                    "has_full_snapshot": has_full_snapshot,
                    # We only store this field on the first chunk as it contains all events, not just this chunk
                    "events_summary": get_events_summary_from_snapshot_data(data_list) if index == 0 else None,
                },
            },
        }


def chunk_string(string: str, chunk_length: int) -> List[str]:
    """Split a string into chunk_length-sized elements. Reversal operation: `''.join()`."""
    return [string[0 + offset : chunk_length + offset] for offset in range(0, len(string), chunk_length)]


def is_unchunked_snapshot(event: Dict) -> bool:
    try:
        is_snapshot = event["event"] == "$snapshot"
    except KeyError:
        raise ValueError('All events must have the event name field "event"!')
    except TypeError:
        raise ValueError(f"All events must be dictionaries not '{type(event).__name__}'!")
    try:
        return is_snapshot and "chunk_id" not in event["properties"]["$snapshot_data"]
    except KeyError:
        capture_exception()
        raise ValueError('$snapshot events must contain property "$snapshot_data"!')


def decompress_chunked_snapshot_data(
    team_id: int,
    session_recording_id: str,
    all_recording_events: List[SnapshotDataTaggedWithWindowId],
    limit: Optional[int] = None,
    offset: int = 0,
    return_only_activity_data: bool = False,
) -> RecordingSnapshotsData:
    """
    Before data is stored in clickhouse, it is compressed and then chunked. This function
    gets back to the original data by unchunking the events and then decompressing the data.

    If limit + offset is provided, then it will paginate the decompression by chunks (not by events, because
    you can't decompress an incomplete chunk).

    Depending on the size of the recording, this function can return a lot of data. To decrease the
    memory used, you should either use the pagination parameters or pass in 'return_only_activity_data' which
    drastically reduces the size of the data returned if you only want the activity data (used for metadata calculation)
    """

    if len(all_recording_events) == 0:
        return RecordingSnapshotsData(has_next=False, snapshot_data_by_window_id={})

    snapshot_data_by_window_id = defaultdict(list)

    # Handle backward compatibility to the days of uncompressed and unchunked snapshots
    if "chunk_id" not in all_recording_events[0]["snapshot_data"]:
        paginated_list = paginate_list(all_recording_events, limit, offset)
        for event in paginated_list.paginated_list:
            snapshot_data_by_window_id[event["window_id"]].append(
                get_events_summary_from_snapshot_data([event["snapshot_data"]])[0]
                if return_only_activity_data
                else event["snapshot_data"]
            )
        return RecordingSnapshotsData(
            has_next=paginated_list.has_next, snapshot_data_by_window_id=snapshot_data_by_window_id
        )

    # Split decompressed recording events into their chunks
    chunks_collector: DefaultDict[str, List[SnapshotDataTaggedWithWindowId]] = defaultdict(list)
    for event in all_recording_events:
        chunks_collector[event["snapshot_data"]["chunk_id"]].append(event)

    # Paginate the list of chunks
    paginated_chunk_list = paginate_list(list(chunks_collector.values()), limit, offset)

    has_next = paginated_chunk_list.has_next
    chunk_list: List[List[SnapshotDataTaggedWithWindowId]] = paginated_chunk_list.paginated_list

    # Decompress the chunks and split the resulting events by window_id
    for chunks in chunk_list:
        if len(chunks) != chunks[0]["snapshot_data"]["chunk_count"]:
            capture_message(
                "Did not find all session recording chunks! Team: {}, Session: {}, Chunk-id: {}. Found {} of {} expected chunks".format(
                    team_id,
                    session_recording_id,
                    chunks[0]["snapshot_data"]["chunk_id"],
                    len(chunks),
                    chunks[0]["snapshot_data"]["chunk_count"],
                )
            )
            continue

        b64_compressed_data = "".join(
            chunk["snapshot_data"]["data"] for chunk in sorted(chunks, key=lambda c: c["snapshot_data"]["chunk_index"])
        )
        decompressed_data = json.loads(decompress(b64_compressed_data))

        # Decompressed data can be large, and in metadata calculations, we only care if the event is "active"
        # This pares down the data returned, so we're not passing around a massive object
        if return_only_activity_data:
            events_with_only_activity_data = get_events_summary_from_snapshot_data(decompressed_data)
            snapshot_data_by_window_id[chunks[0]["window_id"]].extend(events_with_only_activity_data)
        else:
            snapshot_data_by_window_id[chunks[0]["window_id"]].extend(decompressed_data)
    return RecordingSnapshotsData(has_next=has_next, snapshot_data_by_window_id=snapshot_data_by_window_id)


@dataclasses.dataclass
class PaginatedList:
    has_next: bool
    paginated_list: List


def paginate_list(list_to_paginate: List, limit: Optional[int], offset: int) -> PaginatedList:
    if not limit:
        has_next = False
        paginated_list = list_to_paginate[offset:]
    elif offset + limit < len(list_to_paginate):
        has_next = True
        paginated_list = list_to_paginate[offset : offset + limit]
    else:
        has_next = False
        paginated_list = list_to_paginate[offset:]
    return PaginatedList(has_next=has_next, paginated_list=paginated_list)
