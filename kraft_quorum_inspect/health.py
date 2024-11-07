"""Turn a parsed snapshot into a health report."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

from .parser import ReplicaRow, Snapshot

OK, WARNING, CRITICAL = "ok", "warning", "critical"
EXIT_CODES = {OK: 0, WARNING: 1, CRITICAL: 2}

DEFAULT_MAX_LAG = 1000
DEFAULT_MAX_LAG_MS = 5000


@dataclass
class Node:
    node_id: int
    role: str
    directory_id: str | None = None
    endpoints: list[str] = field(default_factory=list)
    log_end_offset: int | None = None
    lag: int | None = None
    behind_leader_ms: int | None = None
    healthy: bool | None = None  # None: no per-node replication data
    problems: list[str] = field(default_factory=list)


@dataclass
class Finding:
    severity: str
    message: str


@dataclass
class Report:
    status: str
    cluster_id: str | None
    leader_id: int | None
    leader_epoch: int | None
    high_watermark: int | None
    voter_count: int
    majority: int
    healthy_voters: int
    failures_tolerated: int
    has_replication_detail: bool
    voters: list[Node]
    observers: list[Node]
    findings: list[Finding]

    @property
    def exit_code(self) -> int:
        return EXIT_CODES[self.status]

    def to_dict(self) -> dict:
        data = asdict(self)
        data["exit_code"] = self.exit_code
        return data


def _behind_leader_ms(row: ReplicaRow, leader: ReplicaRow | None) -> int | None:
    """How much older this node's last caught-up time is than the leader's."""
    if leader is None:
        return None
    if row.last_caught_up_timestamp is not None and leader.last_caught_up_timestamp is not None:
        return max(0, leader.last_caught_up_timestamp - row.last_caught_up_timestamp)
    if row.last_caught_up_ago_ms is not None and leader.last_caught_up_ago_ms is not None:
        return max(0, row.last_caught_up_ago_ms - leader.last_caught_up_ago_ms)
    return None


def _node_from_row(row: ReplicaRow, leader: ReplicaRow | None, max_lag: int, max_lag_ms: int) -> Node:
    node = Node(
        node_id=row.node_id,
        role=row.role,
        directory_id=row.directory_id,
        log_end_offset=row.log_end_offset,
        lag=row.lag,
    )
    if row.role == "leader":
        node.behind_leader_ms = 0
        node.healthy = True
        return node

    node.behind_leader_ms = _behind_leader_ms(row, leader)
    never_fetched = (
        row.log_end_offset is None
        or (row.last_fetch_timestamp is None and row.last_fetch_ago_ms is None)
    )
    if never_fetched:
        node.problems.append("unreachable: the leader has not seen a fetch from it")
    else:
        if row.lag is not None and row.lag > max_lag:
            node.problems.append(f"lag of {row.lag} records exceeds {max_lag}")
        if node.behind_leader_ms is not None and node.behind_leader_ms > max_lag_ms:
            node.problems.append(
                f"last caught up {node.behind_leader_ms} ms before the leader (limit {max_lag_ms} ms)"
            )
    node.healthy = not node.problems
    return node


def evaluate(
    snapshot: Snapshot,
    max_lag: int = DEFAULT_MAX_LAG,
    max_lag_ms: int = DEFAULT_MAX_LAG_MS,
) -> Report:
    status, rows = snapshot.status, snapshot.replication
    findings: list[Finding] = []
    voters: list[Node] = []
    observers: list[Node] = []

    leader_row = next((r for r in rows or [] if r.role == "leader"), None)
    leader_id = status.leader_id if status and status.leader_id is not None else None
    if leader_id is None and leader_row is not None:
        leader_id = leader_row.node_id

    if rows is not None:
        for row in rows:
            node = _node_from_row(row, leader_row, max_lag, max_lag_ms)
            (observers if row.role == "observer" else voters).append(node)

    # Fill in endpoints / directory ids, or build the node lists, from --status.
    if status is not None:
        by_id = {n.node_id: n for n in voters + observers}
        for member, role, target in [(m, "voter", voters) for m in status.voters] + [
            (m, "observer", observers) for m in status.observers
        ]:
            node = by_id.get(member.node_id)
            if node is None:
                if rows is not None:
                    continue  # the replication table is the more detailed source
                if role == "voter":
                    role = "leader" if member.node_id == leader_id else "follower"
                node = Node(node_id=member.node_id, role=role)
                target.append(node)
            node.endpoints = member.endpoints
            node.directory_id = node.directory_id or member.directory_id

    voter_count = len(voters)
    majority = voter_count // 2 + 1 if voter_count else 0
    has_leader = leader_id is not None and leader_id >= 0

    if rows is not None:
        unhealthy = [n for n in voters if n.healthy is False]
        healthy_voters = voter_count - len(unhealthy)
        for node in unhealthy:
            findings.append(Finding(WARNING, f"Voter {node.node_id}: {'; '.join(node.problems)}."))
    else:
        # Only the summary is available: it says whether some voter is behind,
        # not which one. Treat that as one unhealthy voter.
        healthy_voters = voter_count
        lag = status.max_follower_lag if status else None
        lag_ms = status.max_follower_lag_time_ms if status else None
        behind = []
        if lag is not None and lag > max_lag:
            behind.append(f"MaxFollowerLag is {lag} records (limit {max_lag})")
        if lag_ms is not None and lag_ms > max_lag_ms:
            behind.append(f"MaxFollowerLagTimeMs is {lag_ms} (limit {max_lag_ms})")
        if lag_ms == -1 and voter_count > 1:
            behind.append("MaxFollowerLagTimeMs is -1: the leader has no caught-up time for a voter")
        if behind and voter_count > 1:
            healthy_voters -= 1
            findings.append(
                Finding(
                    WARNING,
                    f"At least one voter is behind the leader: {'; '.join(behind)}. "
                    "Supply 'describe --replication' output to see which.",
                )
            )

    failures_tolerated = max(0, healthy_voters - majority)

    if not has_leader:
        findings.insert(0, Finding(CRITICAL, "The quorum has no leader (LeaderId is -1 or missing)."))
    if voter_count == 0:
        findings.append(Finding(CRITICAL, "No voters found in the input."))
    elif healthy_voters < majority:
        findings.insert(
            0,
            Finding(
                CRITICAL,
                f"Only {healthy_voters} of {voter_count} voters are healthy; "
                f"a majority needs {majority}. The quorum cannot commit metadata changes.",
            ),
        )
    elif failures_tolerated == 0:
        if voter_count < 3:
            reason = f"with {voter_count} voter{'s' if voter_count > 1 else ''} there is no fault tolerance"
        else:
            reason = f"{healthy_voters} of {voter_count} voters are healthy and a majority needs {majority}"
        findings.insert(
            0,
            Finding(WARNING, f"One more controller failure loses the majority: {reason}."),
        )

    if voter_count >= 2 and voter_count % 2 == 0:
        findings.append(
            Finding(
                OK,
                f"{voter_count} voters tolerate no more failures than {voter_count - 1}; "
                "an odd number of controllers is the usual choice.",
            )
        )
    for node in observers:
        if node.healthy is False:
            findings.append(Finding(WARNING, f"Observer {node.node_id}: {'; '.join(node.problems)}."))

    severities = {f.severity for f in findings}
    overall = CRITICAL if CRITICAL in severities else WARNING if WARNING in severities else OK

    return Report(
        status=overall,
        cluster_id=status.cluster_id if status else None,
        leader_id=leader_id,
        leader_epoch=status.leader_epoch if status else None,
        high_watermark=status.high_watermark if status else None,
        voter_count=voter_count,
        majority=majority,
        healthy_voters=healthy_voters,
        failures_tolerated=failures_tolerated,
        has_replication_detail=rows is not None,
        voters=voters,
        observers=observers,
        findings=findings,
    )
