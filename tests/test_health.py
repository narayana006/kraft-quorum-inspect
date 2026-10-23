from pathlib import Path

from kraft_quorum_inspect.health import CRITICAL, OK, WARNING, evaluate
from kraft_quorum_inspect.parser import parse

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def report_for(name: str, **kwargs):
    return evaluate(parse((EXAMPLES / name).read_text()), **kwargs)


def status_text(voters: str, leader: int = 1, lag: int = 0, lag_ms: int = 0) -> str:
    return (
        f"ClusterId: abc\nLeaderId: {leader}\nLeaderEpoch: 7\nHighWatermark: 100\n"
        f"MaxFollowerLag: {lag}\nMaxFollowerLagTimeMs: {lag_ms}\n"
        f"CurrentVoters: {voters}\nCurrentObservers: []\n"
    )


def replication_text(rows: list[tuple]) -> str:
    lines = ["NodeId\tLogEndOffset\tLag\tLastFetchTimestamp\tLastCaughtUpTimestamp\tStatus"]
    lines += ["\t".join(str(c) for c in row) for row in rows]
    return "\n".join(lines) + "\n"


def test_healthy_three_voter_quorum():
    report = report_for("healthy.txt")
    assert report.status == OK and report.exit_code == 0
    assert (report.voter_count, report.majority, report.healthy_voters) == (3, 2, 3)
    assert report.failures_tolerated == 1
    assert report.findings == []
    assert report.leader_id == 2
    assert report.voters[0].endpoints == ["CONTROLLER://controller-2:9093"]
    assert len(report.observers) == 3


def test_stopped_follower_is_flagged_by_time_behind_leader():
    report = report_for("follower-stopped.txt")
    assert report.status == WARNING and report.exit_code == 1
    assert report.healthy_voters == 2 and report.failures_tolerated == 0
    stopped = next(n for n in report.voters if n.node_id == 3)
    assert stopped.healthy is False
    assert stopped.lag == 23
    assert stopped.behind_leader_ms == 1794287844420 - 1794287832669
    assert "One more controller failure loses the majority" in report.findings[0].message


def test_stopped_follower_in_human_readable_output():
    report = report_for("follower-stopped-human-readable.txt")
    stopped = next(n for n in report.voters if n.node_id == 3)
    assert stopped.behind_leader_ms == 13407 - 6
    assert report.status == WARNING and report.leader_id == 2  # leader taken from the table


def test_voter_never_seen_by_new_leader_is_unreachable():
    report = report_for("after-leader-failover.txt")
    down = next(n for n in report.voters if n.node_id == 1)
    assert down.healthy is False
    assert down.problems[0].startswith("unreachable")
    assert report.status == WARNING and report.failures_tolerated == 0


def test_status_only_uses_summary_lag_fields():
    report = report_for("after-leader-failover-status.txt")
    assert report.has_replication_detail is False
    assert report.status == WARNING
    assert report.healthy_voters == 2 and report.failures_tolerated == 0
    assert all(n.healthy is None for n in report.voters)
    assert any("At least one voter is behind" in f.message for f in report.findings)


def test_status_only_healthy():
    report = report_for("healthy-status.txt")
    assert report.status == OK and report.failures_tolerated == 1
    assert [n.role for n in report.voters] == ["follower", "leader", "follower"]


def test_single_voter_has_no_fault_tolerance():
    report = report_for("single-node.txt")
    assert report.status == WARNING
    assert (report.voter_count, report.majority, report.failures_tolerated) == (1, 1, 0)
    assert "no fault tolerance" in report.findings[0].message


def test_five_voters_one_down_still_tolerates_one_more():
    now = 1_000_000
    text = status_text("[1,2,3,4,5]") + replication_text(
        [(1, 500, 0, now, now, "Leader")]
        + [(i, 500, 0, now - 200, now - 200, "Follower") for i in (2, 3, 4)]
        + [(5, -1, 501, -1, -1, "Follower")]
    )
    report = evaluate(parse(text))
    assert report.status == WARNING
    assert (report.healthy_voters, report.majority, report.failures_tolerated) == (4, 3, 1)
    assert not any("loses the majority" in f.message for f in report.findings)
    assert any(f.message.startswith("Voter 5") for f in report.findings)


def test_majority_lost_is_critical():
    now = 1_000_000
    text = status_text("[1,2,3]") + replication_text(
        [(1, 500, 0, now, now, "Leader"), (2, -1, 501, -1, -1, "Follower"), (3, -1, 501, -1, -1, "Follower")]
    )
    report = evaluate(parse(text))
    assert report.status == CRITICAL and report.exit_code == 2
    assert "Only 1 of 3 voters are healthy" in report.findings[0].message


def test_no_leader_is_critical():
    report = evaluate(parse(status_text("[1,2,3]", leader=-1)))
    assert report.status == CRITICAL
    assert "no leader" in report.findings[0].message


def test_record_lag_threshold():
    now = 1_000_000
    text = status_text("[1,2,3]") + replication_text(
        [(1, 5000, 0, now, now, "Leader"), (2, 5000, 0, now - 100, now - 100, "Follower"),
         (3, 3000, 2000, now - 100, now - 100, "Follower")]
    )
    assert evaluate(parse(text)).status == WARNING
    assert evaluate(parse(text), max_lag=5000).status == OK


def test_time_threshold_is_configurable():
    assert report_for("follower-stopped.txt", max_lag_ms=60_000).status == OK


def test_even_voter_count_adds_a_note_without_changing_status():
    now = 1_000_000
    text = status_text("[1,2,3,4]") + replication_text(
        [(1, 10, 0, now, now, "Leader")] + [(i, 10, 0, now, now, "Follower") for i in (2, 3, 4)]
    )
    report = evaluate(parse(text))
    assert report.status == OK and report.failures_tolerated == 1
    assert any("odd number" in f.message for f in report.findings)


def test_lagging_observer_warns_but_does_not_reduce_voter_count():
    now = 1_000_000
    text = status_text("[1,2,3]") + replication_text(
        [(1, 10, 0, now, now, "Leader"), (2, 10, 0, now, now, "Follower"), (3, 10, 0, now, now, "Follower"),
         (101, 2, 8, now - 90_000, now - 90_000, "Observer")]
    )
    report = evaluate(parse(text))
    assert report.status == WARNING and report.healthy_voters == 3
    assert report.findings[0].message.startswith("Observer 101")


def test_to_dict_is_json_ready():
    data = report_for("healthy.txt").to_dict()
    assert data["exit_code"] == 0 and data["status"] == "ok"
    assert data["voters"][0]["node_id"] == 2
