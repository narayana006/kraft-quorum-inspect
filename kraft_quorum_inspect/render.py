"""Plain-text rendering of a Report."""

from __future__ import annotations

from .health import OK, Node, Report


def _cell(value) -> str:
    return "-" if value is None else str(value)


def _state(node: Node) -> str:
    if node.healthy is None:
        return "unknown"
    if node.healthy:
        return "ok"
    return "UNREACHABLE" if any(p.startswith("unreachable") for p in node.problems) else "LAGGING"


def _table(nodes: list[Node]) -> list[str]:
    header = ["ID", "ROLE", "LOG END", "LAG", "BEHIND LEADER", "STATE", "ENDPOINT"]
    rows = [header]
    for node in nodes:
        behind = None if node.behind_leader_ms is None else f"{node.behind_leader_ms} ms"
        rows.append(
            [
                str(node.node_id),
                node.role,
                _cell(node.log_end_offset),
                _cell(node.lag),
                _cell(behind),
                _state(node),
                ", ".join(node.endpoints) or "-",
            ]
        )
    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
    return ["  " + "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)).rstrip() for row in rows]


def render_text(report: Report) -> str:
    lines = [f"KRaft quorum: {report.status.upper()}"]
    lines.append(f"  Cluster id      {_cell(report.cluster_id)}")
    leader = "none" if report.leader_id is None or report.leader_id < 0 else f"node {report.leader_id}"
    lines.append(f"  Leader          {leader} (epoch {_cell(report.leader_epoch)})")
    lines.append(f"  High watermark  {_cell(report.high_watermark)}")
    basis = "" if report.has_replication_detail else " (estimated from the status summary)"
    lines.append(
        f"  Voters          {report.voter_count} total, {report.healthy_voters} healthy{basis}, "
        f"majority {report.majority}, tolerates {report.failures_tolerated} more failure"
        f"{'' if report.failures_tolerated == 1 else 's'}"
    )
    if report.voters:
        lines += ["", "Voters"] + _table(report.voters)
    if report.observers:
        lines += ["", "Observers"] + _table(report.observers)
    lines.append("")
    if report.findings:
        lines.append("Findings")
        for finding in report.findings:
            label = "NOTE" if finding.severity == OK else finding.severity.upper()
            lines.append(f"  [{label}] {finding.message}")
    else:
        lines.append("No problems found.")
    return "\n".join(lines)
