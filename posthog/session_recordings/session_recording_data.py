import base64
import gzip
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional


from posthog.models.session_recording.metadata import (
    RecordingSegment,
    SessionRecordingEventSummary,
    SnapshotData,
    WindowId,
)


### Session Recording Data
# These are generic helpers for working with the raw RRweb data, regardless of how it is stored

FULL_SNAPSHOT = 2


# NOTE: For reference here are some helpful enum mappings from rrweb
# https://github.com/rrweb-io/rrweb/blob/master/packages/rrweb/src/types.ts

# event.type
class RRWEB_MAP_EVENT_TYPE:
    DomContentLoaded = 0
    Load = 1
    FullSnapshot = 2
    IncrementalSnapshot = 3
    Meta = 4
    Custom = 5
    Plugin = 6


# event.data.source
class RRWEB_MAP_EVENT_DATA_SOURCE:
    Mutation = 0
    MouseMove = 1
    MouseInteraction = 2
    Scroll = 3
    ViewportResize = 4
    Input = 5
    TouchMove = 6
    MediaInteraction = 7
    StyleSheetRule = 8
    CanvasMutation = 9
    Font = 1
    Log = 1
    Drag = 1
    StyleDeclaration = 1
    Selection = 1


# event.data.type
class RRWEB_MAP_EVENT_DATA_TYPE:
    MouseUp = 0
    MouseDown = 1
    Click = 2
    ContextMenu = 3
    DblClick = 4
    Focus = 5
    Blur = 6
    TouchStart = 7
    TouchMove_Departed = 8
    TouchEnd = 9
    TouchCancel = 10


# List of properties from the event payload we care about for our uncompressed `events_summary`
# NOTE: We should keep this as minimal as possible
EVENT_SUMMARY_DATA_INCLUSIONS = [
    "type",
    "source",
    "tag",
    "plugin",
    "href",
    "width",
    "height",
    "payload.href",
    "payload.level",
]


Event = Dict[str, Any]

ACTIVITY_THRESHOLD_SECONDS = 10


def compress_to_string(json_string: str) -> str:
    compressed_data = gzip.compress(json_string.encode("utf-16", "surrogatepass"))
    return base64.b64encode(compressed_data).decode("utf-8")


def decompress(base64data: str) -> str:
    compressed_bytes = base64.b64decode(base64data)
    return gzip.decompress(compressed_bytes).decode("utf-16", "surrogatepass")


def is_active_event(event: SessionRecordingEventSummary) -> bool:
    """
    Determines which rr-web events are "active" - meaning user generated
    """
    active_rr_web_sources = [
        1,  # MouseMove,
        2,  # MouseInteraction,
        3,  # Scroll,
        4,  # ViewportResize,
        5,  # Input,
        6,  # TouchMove,
        7,  # MediaInteraction,
        12,  # Drag,
    ]
    return event["type"] == 3 and event["data"].get("source") in active_rr_web_sources


def parse_snapshot_timestamp(timestamp: int):
    return datetime.fromtimestamp(timestamp / 1000, timezone.utc)


def get_active_segments_from_event_list(
    event_list: List[SessionRecordingEventSummary],
    window_id: WindowId,
    activity_threshold_seconds=ACTIVITY_THRESHOLD_SECONDS,
) -> List[RecordingSegment]:
    """
    Processes a list of events for a specific window_id to determine
    the segments of the recording where the user is "active". And active segment ends
    when there isn't another active event for activity_threshold_seconds seconds
    """
    active_event_timestamps = [event["timestamp"] for event in event_list if is_active_event(event)]

    active_recording_segments: List[RecordingSegment] = []
    current_active_segment: Optional[RecordingSegment] = None
    for current_timestamp_int in active_event_timestamps:
        current_timestamp = parse_snapshot_timestamp(current_timestamp_int)
        # If the time since the last active event is less than the threshold, continue the existing segment
        if current_active_segment and (current_timestamp - current_active_segment["end_time"]) <= timedelta(
            seconds=activity_threshold_seconds
        ):
            current_active_segment["end_time"] = current_timestamp

        # Otherwise, start a new segment
        else:
            if current_active_segment:
                active_recording_segments.append(current_active_segment)
            current_active_segment = RecordingSegment(
                start_time=current_timestamp, end_time=current_timestamp, window_id=window_id, is_active=True
            )

    # Add the active last segment if it hasn't already been added
    if current_active_segment and (
        len(active_recording_segments) == 0 or active_recording_segments[-1] != current_active_segment
    ):
        active_recording_segments.append(current_active_segment)

    return active_recording_segments


def get_events_summary_from_snapshot_data(snapshot_data: List[SnapshotData]) -> List[SessionRecordingEventSummary]:
    """
    Extract a minimal representation of the snapshot data events for easier querying.
    'data' and 'data.payload' values are included as long as they are strings or numbers
    and in the inclusion list to keep the payload minimal
    """
    events_summary = []

    for event in snapshot_data:
        if "timestamp" not in event or "type" not in event:
            continue

        # Get all top level data values
        data = {
            key: value
            for key, value in event.get("data", {}).items()
            if type(value) in [str, int] and key in EVENT_SUMMARY_DATA_INCLUSIONS
        }
        # Some events have a payload, some values of which we want
        if event.get("data", {}).get("payload"):
            # Make sure the payload is a dict before we access it
            if isinstance(event["data"]["payload"], dict):
                data["payload"] = {
                    key: value
                    for key, value in event["data"]["payload"].items()
                    if type(value) in [str, int] and f"payload.{key}" in EVENT_SUMMARY_DATA_INCLUSIONS
                }

        events_summary.append(
            SessionRecordingEventSummary(
                timestamp=event["timestamp"],
                type=event["type"],
                data=data,
            )
        )

    # No guarantees are made about order so we sort here to be sure
    events_summary.sort(key=lambda x: x["timestamp"])

    return events_summary


def generate_inactive_segments_for_range(
    range_start_time: datetime,
    range_end_time: datetime,
    last_active_window_id: WindowId,
    start_and_end_times_by_window_id: Dict[WindowId, RecordingSegment],
    is_first_segment: bool = False,
    is_last_segment: bool = False,
) -> List[RecordingSegment]:
    """
    Given the start and end times of a known period of inactivity,
    this function will try create recording segments to fill the gap based on the
    start and end times of the given window_ids
    """

    window_ids_by_start_time = sorted(
        start_and_end_times_by_window_id, key=lambda x: start_and_end_times_by_window_id[x]["start_time"]
    )

    # Order of window_ids to use for generating inactive segments. Start with the window_id of the
    # last active segment, then try the other window_ids in order of start_time
    window_id_priority_list: List[WindowId] = [last_active_window_id] + window_ids_by_start_time

    inactive_segments: List[RecordingSegment] = []
    current_time = range_start_time

    for window_id in window_id_priority_list:
        window_start_time = start_and_end_times_by_window_id[window_id]["start_time"]
        window_end_time = start_and_end_times_by_window_id[window_id]["end_time"]
        if window_end_time > current_time and current_time < range_end_time:
            # Add/subtract a millisecond to make sure the segments don't exactly overlap
            segment_start_time = max(window_start_time, current_time)
            segment_end_time = min(window_end_time, range_end_time)
            inactive_segments.append(
                RecordingSegment(
                    start_time=segment_start_time, end_time=segment_end_time, window_id=window_id, is_active=False
                )
            )
            current_time = min(segment_end_time, window_end_time)

    # Ensure segments don't exactly overlap. This makes the corresponding player logic simpler
    for index, segment in enumerate(inactive_segments):
        if (index == 0 and segment["start_time"] == range_start_time and not is_first_segment) or (
            index > 0 and segment["start_time"] == inactive_segments[index - 1]["end_time"]
        ):
            segment["start_time"] = segment["start_time"] + timedelta(milliseconds=1)

        if index == len(inactive_segments) - 1 and segment["end_time"] == range_end_time and not is_last_segment:
            segment["end_time"] = segment["end_time"] - timedelta(milliseconds=1)

    return inactive_segments
