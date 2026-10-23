from pathlib import Path

import pytest

from kraft_quorum_inspect.parser import ParseError, merge, parse

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def load(name: str) -> str:
    return (EXAMPLES / name).read_text()


def test_status_fields_from_kafka_4_capture():
    status = parse(load("healthy-status.txt")).status
    assert status.cluster_id == "q1Sh-9_ISia_zwGINzRvyQ"
    assert status.leader_id == 2
    assert status.leader_epoch == 1
    assert status.high_watermark == 336
    assert status.max_follower_lag == 0
    assert status.max_follower_lag_time_ms == 341
    assert [v.node_id for v in status.voters] == [1, 2, 3]
    assert status.voters[0].endpoints == ["CONTROLLER://controller-1:9093"]
    assert status.voters[0].directory_id is None  # static quorum prints none
    assert [o.node_id for o in status.observers] == [103, 102, 101]
    assert status.observers[0].directory_id == "WUzpLRq-4YHTNLW0Wpwv2A"


def test_status_only_input_has_no_replication():
    snap = parse(load("healthy-status.txt"))
    assert snap.replication is None


def test_replication_table_with_directory_id_column():
    rows = parse(load("healthy-replication.txt")).replication
    assert [(r.node_id, r.role) for r in rows] == [
        (2, "leader"), (1, "follower"), (3, "follower"),
        (103, "observer"), (102, "observer"), (101, "observer"),
    ]
    leader = rows[0]
    assert leader.log_end_offset == 339 and leader.lag == 0
    assert leader.last_caught_up_timestamp == 1794287829091
    assert leader.directory_id is None  # all-zero id is treated as absent
    assert rows[3].directory_id == "WUzpLRq-4YHTNLW0Wpwv2A"


def test_combined_input_yields_both_parts():
    snap = parse(load("healthy.txt"))
    assert snap.status.leader_id == 2
    assert len(snap.replication) == 6


def test_minus_one_values_become_none():
    rows = parse(load("after-leader-failover.txt")).replication
    down = next(r for r in rows if r.node_id == 1)
    assert down.log_end_offset is None
    assert down.last_fetch_timestamp is None and down.last_fetch_ago_ms is None
    assert down.lag == 482


def test_human_readable_timestamps():
    rows = parse(load("follower-stopped-human-readable.txt")).replication
    stopped = next(r for r in rows if r.node_id == 3)
    assert stopped.last_fetch_ago_ms == 12917
    assert stopped.last_caught_up_ago_ms == 13407
    assert stopped.last_fetch_timestamp is None


def test_kafka_3_7_format():
    snap = parse(load("kafka-3.7-single-node.txt"))
    assert snap.status.cluster_id == "5L6g3nShT-eMCtK--X86sw"  # "Some(...)" unwrapped
    assert [v.node_id for v in snap.status.voters] == [1]
    assert snap.status.observers == []
    row = snap.replication[0]  # no DirectoryId column before 3.9
    assert (row.node_id, row.role, row.log_end_offset, row.directory_id) == (1, "leader", 37, None)


def test_voters_with_directory_ids_as_printed_for_dynamic_quorums():
    text = (
        'LeaderId: 1\n'
        'CurrentVoters: [{"id": 1, "directoryId": "KlvRXWMRQbCYpKDXvEz7xw", '
        '"endpoints": ["CONTROLLER://c1:9093", "CONTROLLER_SSL://c1:9094"]}]\n'
        'CurrentObservers: []\n'
    )
    voter = parse(text).status.voters[0]
    assert voter.directory_id == "KlvRXWMRQbCYpKDXvEz7xw"
    assert voter.endpoints == ["CONTROLLER://c1:9093", "CONTROLLER_SSL://c1:9094"]


def test_log_lines_around_output_are_ignored():
    noisy = (
        "[2026-11-10 05:14:53,510] WARN Couldn't resolve server controller-2:9093 "
        "from bootstrap.servers as DNS resolution failed for controller-2 "
        "(org.apache.kafka.clients.ClientUtils)\n" + load("healthy.txt") + "\nsome trailing line\n"
    )
    snap = parse(noisy)
    assert snap.status.leader_id == 2 and len(snap.replication) == 6


def test_table_pasted_with_spaces_instead_of_tabs():
    spaced = load("follower-stopped-human-readable.txt").replace("\t", "  ")
    rows = parse(spaced).replication
    assert len(rows) == 6 and rows[2].last_fetch_ago_ms == 12917


def test_unrelated_text_raises():
    with pytest.raises(ParseError):
        parse("Error: connection refused\n")
    with pytest.raises(ParseError):
        parse("")


def test_malformed_voter_list_raises():
    with pytest.raises(ParseError):
        parse("CurrentVoters: [1,2,\n")


def test_merge_combines_separate_files():
    merged = merge([parse(load("healthy-status.txt")), parse(load("healthy-replication.txt"))])
    assert merged.status is not None and len(merged.replication) == 6
