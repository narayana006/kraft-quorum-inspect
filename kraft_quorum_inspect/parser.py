"""Parse the text printed by ``kafka-metadata-quorum describe``.

Two output shapes are understood:

``describe --status`` prints ``Key: value`` lines::

    ClusterId:              q1Sh-9_ISia_zwGINzRvyQ
    LeaderId:               2
    ...
    CurrentVoters:          [{"id": 1, "endpoints": ["CONTROLLER://c1:9093"]}, ...]

Kafka 3.3 to 3.8 print the voter and observer lists as plain ids (``[1,2,3]``);
3.9 and later print one JSON-like object per node.

``describe --replication`` prints a tab-separated table whose header starts
with ``NodeId``. Kafka 3.9 and later add a ``DirectoryId`` column. With
``--human-readable`` the two timestamp columns hold ``<n> ms ago`` instead of
epoch milliseconds.

A single input may contain both outputs, in any order, mixed with log lines.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

ZERO_DIRECTORY_ID = "AAAAAAAAAAAAAAAAAAAAAA"

_STATUS_KEYS = {
    "ClusterId",
    "LeaderId",
    "LeaderEpoch",
    "HighWatermark",
    "MaxFollowerLag",
    "MaxFollowerLagTimeMs",
    "CurrentVoters",
    "CurrentObservers",
}
_STATUS_LINE = re.compile(r"^\s*([A-Za-z]+):\s*(.*?)\s*$")
_AGO = re.compile(r"^(\d+)\s*ms ago$")
_CELL_SPLIT = re.compile(r"\t|\s{2,}")
_ROLES = {"leader", "follower", "observer"}


class ParseError(ValueError):
    """The input does not look like kafka-metadata-quorum output."""


@dataclass
class Member:
    """A node as listed in CurrentVoters / CurrentObservers."""

    node_id: int
    directory_id: str | None = None
    endpoints: list[str] = field(default_factory=list)


@dataclass
class Status:
    cluster_id: str | None = None
    leader_id: int | None = None
    leader_epoch: int | None = None
    high_watermark: int | None = None
    max_follower_lag: int | None = None
    max_follower_lag_time_ms: int | None = None
    voters: list[Member] = field(default_factory=list)
    observers: list[Member] = field(default_factory=list)


@dataclass
class ReplicaRow:
    """One row of the --replication table.

    A timestamp is kept either as epoch milliseconds (``*_timestamp``) or as
    an age (``*_ago_ms``), depending on whether --human-readable was used.
    Both are None when the tool printed -1, meaning the leader has no value.
    """

    node_id: int
    role: str
    directory_id: str | None = None
    log_end_offset: int | None = None
    lag: int | None = None
    last_fetch_timestamp: int | None = None
    last_fetch_ago_ms: int | None = None
    last_caught_up_timestamp: int | None = None
    last_caught_up_ago_ms: int | None = None


@dataclass
class Snapshot:
    status: Status | None = None
    replication: list[ReplicaRow] | None = None


def _to_int(text: str) -> int | None:
    try:
        return int(text)
    except ValueError:
        return None


def _parse_members(value: str) -> list[Member]:
    try:
        items = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ParseError(f"cannot read node list {value!r}: {exc}") from exc
    if not isinstance(items, list):
        raise ParseError(f"expected a list of nodes, got {value!r}")
    members = []
    for item in items:
        if isinstance(item, int):
            members.append(Member(node_id=item))
        elif isinstance(item, dict) and isinstance(item.get("id"), int):
            members.append(
                Member(
                    node_id=item["id"],
                    directory_id=item.get("directoryId"),
                    endpoints=list(item.get("endpoints", [])),
                )
            )
        else:
            raise ParseError(f"unexpected node entry {item!r}")
    return members


def _clean_cluster_id(value: str) -> str:
    # Kafka 3.7 prints the Scala Option wrapper: "Some(5L6g3nShT-eMCtK--X86sw)".
    match = re.fullmatch(r"Some\((.*)\)", value)
    return match.group(1) if match else value


def _apply_status_field(status: Status, key: str, value: str) -> None:
    if key == "ClusterId":
        status.cluster_id = _clean_cluster_id(value)
    elif key == "LeaderId":
        status.leader_id = _to_int(value)
    elif key == "LeaderEpoch":
        status.leader_epoch = _to_int(value)
    elif key == "HighWatermark":
        status.high_watermark = _to_int(value)
    elif key == "MaxFollowerLag":
        status.max_follower_lag = _to_int(value)
    elif key == "MaxFollowerLagTimeMs":
        status.max_follower_lag_time_ms = _to_int(value)
    elif key == "CurrentVoters":
        status.voters = _parse_members(value)
    elif key == "CurrentObservers":
        status.observers = _parse_members(value)


def _parse_time(cell: str) -> tuple[int | None, int | None]:
    """Return (epoch_ms, ago_ms); exactly one is set unless the cell is -1."""
    ago = _AGO.match(cell)
    if ago:
        return None, int(ago.group(1))
    value = _to_int(cell)
    if value is None or value < 0:
        return None, None
    return value, None


def _parse_row(header: list[str], cells: list[str]) -> ReplicaRow | None:
    if len(cells) != len(header):
        return None
    data = dict(zip(header, cells))
    node_id = _to_int(data.get("NodeId", ""))
    role = data.get("Status", "").lower()
    if node_id is None or role not in _ROLES:
        return None
    log_end = _to_int(data.get("LogEndOffset", ""))
    fetch_ts, fetch_ago = _parse_time(data.get("LastFetchTimestamp", "-1"))
    caught_ts, caught_ago = _parse_time(data.get("LastCaughtUpTimestamp", "-1"))
    directory_id = data.get("DirectoryId")
    return ReplicaRow(
        node_id=node_id,
        role=role,
        directory_id=None if directory_id in (None, ZERO_DIRECTORY_ID) else directory_id,
        log_end_offset=None if log_end is None or log_end < 0 else log_end,
        lag=_to_int(data.get("Lag", "")),
        last_fetch_timestamp=fetch_ts,
        last_fetch_ago_ms=fetch_ago,
        last_caught_up_timestamp=caught_ts,
        last_caught_up_ago_ms=caught_ago,
    )


def _split_cells(line: str) -> list[str]:
    return [cell.strip() for cell in _CELL_SPLIT.split(line.strip()) if cell.strip()]


def parse(text: str) -> Snapshot:
    """Parse status output, replication output, or both, from one string."""
    status: Status | None = None
    rows: list[ReplicaRow] | None = None
    header: list[str] | None = None

    for line in text.splitlines():
        if not line.strip():
            continue
        cells = _split_cells(line)
        if cells and cells[0] == "NodeId":
            header = cells
            rows = [] if rows is None else rows
            continue
        if header is not None:
            row = _parse_row(header, cells)
            if row is not None:
                rows.append(row)
                continue
        match = _STATUS_LINE.match(line)
        if match and match.group(1) in _STATUS_KEYS:
            status = status or Status()
            _apply_status_field(status, match.group(1), match.group(2))

    if status is None and rows is None:
        raise ParseError(
            "no kafka-metadata-quorum output found "
            "(expected 'describe --status' fields or a 'describe --replication' table)"
        )
    return Snapshot(status=status, replication=rows)


def merge(snapshots: list[Snapshot]) -> Snapshot:
    """Combine snapshots parsed from separate inputs; later inputs win."""
    merged = Snapshot()
    for snap in snapshots:
        if snap.status is not None:
            merged.status = snap.status
        if snap.replication is not None:
            merged.replication = snap.replication
    return merged
