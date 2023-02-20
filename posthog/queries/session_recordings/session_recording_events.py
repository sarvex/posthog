import json
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union, cast

from statshog.defaults.django import statsd

from posthog import settings
from posthog.client import sync_execute
from posthog.models import Team
from posthog.models.session_recording.metadata import (
    RecordingMetadata,
    RecordingSnapshotsData,
    SessionRecordingEvent,
    SessionRecordingEventSummary,
    SnapshotDataTaggedWithWindowId,
    WindowId,
)
from posthog.redis import get_client
from posthog.session_recordings.session_recording_data import get_metadata_from_events_summary
from posthog.session_recordings.session_recording_helpers import decompress_chunked_snapshot_data

RECORDINGS_DATA_KEY = "@posthog/recordings/"


def get_key(team_id: int, session_id: str):
    return f"{RECORDINGS_DATA_KEY}{team_id}/{session_id}"


class SessionRecordingEvents:
    _session_recording_id: str
    _recording_start_time: Optional[datetime]
    _team: Team

    def __init__(self, session_recording_id: str, team: Team, recording_start_time: Optional[datetime] = None) -> None:
        self._session_recording_id = session_recording_id
        self._team = team
        self._recording_start_time = recording_start_time

    _recording_snapshot_query = """
        SELECT {fields}
        FROM session_recording_events
        PREWHERE
            team_id = %(team_id)s
            AND session_id = %(session_id)s
            {date_clause}
        ORDER BY timestamp
        {limit_param}
    """

    def get_recording_snapshot_date_clause(self) -> Tuple[str, Dict]:
        if self._recording_start_time:
            # If we can, we want to limit the time range being queried.
            # Theoretically, we shouldn't have to look before the recording start time,
            # but until we straighten out the recording start time logic, we should have a buffer
            return (
                """
                    AND toTimeZone(toDateTime(timestamp, 'UTC'), %(timezone)s) >= toDateTime(%(start_time)s, %(timezone)s) - INTERVAL 1 DAY
                    AND toTimeZone(toDateTime(timestamp, 'UTC'), %(timezone)s) <= toDateTime(%(start_time)s, %(timezone)s) + INTERVAL 2 DAY
            """,
                {"start_time": self._recording_start_time, "timezone": self._team.timezone},
            )
        return ("", {})

    def _query_recording_snapshots(self, include_snapshots=False) -> List[SessionRecordingEvent]:
        fields = ["session_id", "window_id", "distinct_id", "timestamp", "events_summary"]
        if include_snapshots:
            fields.append("snapshot_data")

        date_clause, date_clause_params = self.get_recording_snapshot_date_clause()
        query = self._recording_snapshot_query.format(date_clause=date_clause, fields=", ".join(fields), limit_param="")

        response = sync_execute(
            query, {"team_id": self._team.id, "session_id": self._session_recording_id, **date_clause_params}
        )

        return [
            SessionRecordingEvent(
                session_id=columns[0],
                window_id=columns[1],
                distinct_id=columns[2],
                timestamp=columns[3],
                events_summary=[json.loads(x) for x in columns[4]] if columns[4] else [],
                snapshot_data=json.loads(columns[5]) if len(columns) > 5 else None,
            )
            for columns in response
        ]

    def _query_recording_snapshots_redis(self, limit=-1, offset=0) -> List[SessionRecordingEvent]:
        key = get_key(self._team.id, self._session_recording_id)
        snapshots = get_client().lrange(key, offset, limit)

        items = sorted([json.loads(x) for x in snapshots], key=lambda x: x["snapshot"]["timestamp"])

        return [
            SessionRecordingEvent(
                session_id=self._session_recording_id,
                window_id=item["window_id"],
                distinct_id=item.get("distinct_id", "unknown"),
                timestamp=item["snapshot"].get("timestamp"),
                events_summary=[],
                snapshot_data=item["snapshot"],
            )
            for item in items
        ]

    def get_snapshots(self, limit, offset) -> Optional[RecordingSnapshotsData]:
        all_snapshots = [
            SnapshotDataTaggedWithWindowId(
                window_id=recording_snapshot["window_id"], snapshot_data=recording_snapshot["snapshot_data"]
            )
            for recording_snapshot in self._query_recording_snapshots(include_snapshots=True)
        ]
        decompressed = decompress_chunked_snapshot_data(
            self._team.pk, self._session_recording_id, all_snapshots, limit, offset
        )

        if decompressed["snapshot_data_by_window_id"] == {}:
            return None

        decompressed["storage"] = "clickhouse"
        return decompressed

    def get_metadata(self) -> Optional[RecordingMetadata]:
        snapshots = self._query_recording_snapshots(include_snapshots=False)

        if len(snapshots) == 0:
            return None

        distinct_id = snapshots[0]["distinct_id"]

        events_summary_by_window_id = self._get_events_summary_by_window_id(snapshots)

        if events_summary_by_window_id:
            # If all snapshots contain the new events_summary field...
            statsd.incr("session_recordings.metadata_parsed_from_events_summary")
            metadata = get_metadata_from_events_summary(events_summary_by_window_id)
        else:
            # ... otherwise use the legacy method
            snapshots = self._query_recording_snapshots(include_snapshots=True)
            statsd.incr("session_recordings.metadata_parsed_from_snapshot_data")
            metadata = self._get_metadata_from_snapshot_data(snapshots)

        metadata["distinct_id"] = cast(str, distinct_id)
        metadata["storage"] = "clickhouse"
        return metadata

    def _get_events_summary_by_window_id(
        self, snapshots: List[SessionRecordingEvent]
    ) -> Optional[Dict[WindowId, List[SessionRecordingEventSummary]]]:
        """
        For a list of snapshots, group all the events_summary by window_id.
        If any of them are missing this field, we return empty to fallback to old parsing method
        """
        events_summary_by_window_id: Dict[WindowId, List[SessionRecordingEventSummary]] = {}

        for snapshot in snapshots:
            if snapshot["window_id"] not in events_summary_by_window_id:
                events_summary_by_window_id[snapshot["window_id"]] = []

            events_summary_by_window_id[snapshot["window_id"]].extend(
                [cast(SessionRecordingEventSummary, x) for x in snapshot["events_summary"]]
            )

        for window_id in events_summary_by_window_id:
            events_summary_by_window_id[window_id].sort(key=lambda x: x["timestamp"])

        # If any of the snapshots are missing the events_summary field, we fallback to the old parsing method
        if any(len(x) == 0 for x in events_summary_by_window_id.values()):
            return None

        return events_summary_by_window_id

    def _get_metadata_from_snapshot_data(self, snapshots: List[SessionRecordingEvent]) -> RecordingMetadata:
        """
        !Deprecated!
        This method supports parsing of events_summary info for entries that were created before this field was added

        """
        all_snapshots: List[SnapshotDataTaggedWithWindowId] = [
            SnapshotDataTaggedWithWindowId(window_id=snapshot["window_id"], snapshot_data=snapshot["snapshot_data"])
            for snapshot in snapshots
        ]

        decompressed_recording_data = decompress_chunked_snapshot_data(
            self._team.pk, self._session_recording_id, all_snapshots, return_only_activity_data=True
        )

        events_summary_by_window_id = {
            window_id: cast(List[SessionRecordingEventSummary], event_list)
            for window_id, event_list in decompressed_recording_data["snapshot_data_by_window_id"].items()
        }

        return get_metadata_from_events_summary(events_summary_by_window_id)

    def get_all(self) -> Optional[Union[RecordingSnapshotsData, RecordingMetadata]]:
        # For the new hot storage method, we load all data every time. This may change later but for now it basically bypasses the other options

        # Note we can't do a simple limit offset as the events are currently unordered...
        snapshots = self._query_recording_snapshots_redis()

        if not snapshots:
            return None

        distinct_id = snapshots[0]["distinct_id"]
        snapshot_data_by_window_id = defaultdict(list)

        for item in snapshots:
            if not snapshot_data_by_window_id.get(item["window_id"]):
                snapshot_data_by_window_id[item["window_id"]] = []

            snapshot_data_by_window_id[item["window_id"]].append(item["snapshot_data"])

        metadata = get_metadata_from_events_summary(snapshot_data_by_window_id)

        response = {**metadata, "snapshot_data_by_window_id": snapshot_data_by_window_id, "has_next": False}
        response["distinct_id"] = cast(str, distinct_id)

        return response

    # Ingestion write directly to Redis as a sort of hot-cache. Until the recording is "finished" it will remain here
    @staticmethod
    def ingest(team_id: int, events: dict) -> None:
        # NOTE: WE shouldn't use the global redis client here but for local PoC its fine
        pipe = get_client().pipeline()

        for event in events:
            if event["event"] == "$snapshot":
                distinct_id = event["properties"]["distinct_id"]
                session_id = event["properties"]["$session_id"]
                window_id = event["properties"].get("$window_id")
                snapshot_data = event["properties"]["$snapshot_data"]

                # NOTE: Do we need distinctId here?
                payload = {
                    "window_id": window_id,
                    "distinct_id": distinct_id,
                    "snapshot": snapshot_data,
                }

                key = get_key(team_id, session_id)
                pipe.lpush(key, json.dumps(payload))
                # NOTE: NX is only supported by redis 7+
                # TODO: Make sure redis is 7+
                # pipe.expire(key, settings.SESSION_RECORDINGS_HOT_STORAGE_TTL_SECONDS, nx=True)
                pipe.expire(key, settings.SESSION_RECORDINGS_HOT_STORAGE_TTL_SECONDS)

        pipe.execute()
