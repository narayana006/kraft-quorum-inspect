import io
import json
import os
import stat
from pathlib import Path

import pytest

from kraft_quorum_inspect import cli

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


class FakeTty(io.StringIO):
    def isatty(self):
        return True


def run(argv, stdin_text=""):
    out, err = io.StringIO(), io.StringIO()
    code = cli.main(argv, stdin=io.StringIO(stdin_text), out=out, err=err)
    return code, out.getvalue(), err.getvalue()


def example(name):
    return str(EXAMPLES / name)


def make_fake_tool(tmp_path, body):
    tool = tmp_path / "kafka-metadata-quorum.sh"
    tool.write_text("#!/bin/sh\n" + body)
    tool.chmod(tool.stat().st_mode | stat.S_IXUSR)
    return tool


def test_text_report_for_healthy_file():
    code, out, _ = run([example("healthy.txt")])
    assert code == 0
    assert out.startswith("KRaft quorum: OK")
    assert "node 2 (epoch 1)" in out
    assert "No problems found." in out


def test_exit_code_and_warning_for_degraded_quorum():
    code, out, _ = run([example("after-leader-failover.txt")])
    assert code == 1
    assert "UNREACHABLE" in out
    assert "[WARNING] One more controller failure loses the majority" in out


def test_reads_stdin_when_no_files():
    code, out, _ = run([], stdin_text=(EXAMPLES / "healthy.txt").read_text())
    assert code == 0 and "KRaft quorum: OK" in out


def test_two_files_are_merged():
    code, out, _ = run([example("healthy-status.txt"), example("healthy-replication.txt")])
    assert code == 0 and "CONTROLLER://controller-1:9093" in out and "Observers" in out


def test_json_output():
    code, out, _ = run(["--json", example("follower-stopped.txt")])
    data = json.loads(out)
    assert code == 1 and data["exit_code"] == 1 and data["status"] == "warning"
    assert data["failures_tolerated"] == 0
    assert [v["node_id"] for v in data["voters"] if v["healthy"] is False] == [3]


def test_threshold_flag_changes_result():
    code, _, _ = run(["--max-lag-ms", "60000", example("follower-stopped.txt")])
    assert code == 0


def test_missing_file_is_exit_3():
    code, out, err = run(["/no/such/file.txt"])
    assert code == 3 and out == "" and "cannot read" in err


def test_garbage_input_is_exit_3():
    code, _, err = run([], stdin_text="Connection to node -1 could not be established\n")
    assert code == 3 and "no kafka-metadata-quorum output found" in err


def test_no_input_on_a_terminal_is_a_usage_error():
    with pytest.raises(SystemExit) as exc:
        cli.main([], stdin=FakeTty(), out=io.StringIO(), err=io.StringIO())
    assert exc.value.code == 2


def test_watch_rejects_stdin():
    with pytest.raises(SystemExit):
        run(["--watch"], stdin_text="x")


def test_live_mode_invokes_tool_for_status_and_replication(tmp_path):
    log = tmp_path / "calls.log"
    tool = make_fake_tool(
        tmp_path,
        f'echo "$@" >> {log}\n'
        f'case "$*" in\n'
        f'  *--status*) cat {example("healthy-status.txt")} ;;\n'
        f'  *--replication*) cat {example("healthy-replication.txt")} ;;\n'
        f'esac\n',
    )
    code, out, _ = run(["--bootstrap-controller", "ctrl1:9093", "--command-config", "client.properties",
                        "--kafka-tool", str(tool)])
    assert code == 0 and "KRaft quorum: OK" in out
    assert log.read_text().splitlines() == [
        "--bootstrap-controller ctrl1:9093 --command-config client.properties describe --status",
        "--bootstrap-controller ctrl1:9093 --command-config client.properties describe --replication",
    ]


def test_live_mode_finds_tool_through_kafka_home(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    make_fake_tool(bin_dir, f'cat {example("single-node.txt")}\n')
    monkeypatch.setenv("KAFKA_HOME", str(tmp_path))
    code, out, _ = run(["--bootstrap-server", "localhost:9092"])
    assert code == 1 and "no fault tolerance" in out


def test_live_mode_tool_failure_is_exit_3(tmp_path):
    tool = make_fake_tool(tmp_path, 'echo "Timed out waiting for a node assignment" >&2\nexit 1\n')
    code, _, err = run(["--bootstrap-controller", "ctrl1:9093", "--kafka-tool", str(tool)])
    assert code == 3 and "exited with 1: Timed out waiting for a node assignment" in err


def test_live_mode_without_tool_explains_how_to_fix(monkeypatch):
    monkeypatch.delenv("KAFKA_HOME", raising=False)
    monkeypatch.setenv("PATH", os.devnull)
    code, _, err = run(["--bootstrap-controller", "ctrl1:9093"])
    assert code == 3 and "kafka-metadata-quorum not found" in err


def test_watch_rereads_file_and_stops_after_count(tmp_path, monkeypatch):
    target = tmp_path / "quorum.txt"
    target.write_text((EXAMPLES / "healthy.txt").read_text())
    sleeps = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        target.write_text((EXAMPLES / "after-leader-failover.txt").read_text())

    monkeypatch.setattr(cli.time, "sleep", fake_sleep)
    out = io.StringIO()
    args = cli.build_parser().parse_args(["--watch", "--count", "2", "--interval", "0.5", "--json", str(target)])
    code = cli.watch(args, io.StringIO(), out, io.StringIO(), sleep=fake_sleep)
    lines = [json.loads(line) for line in out.getvalue().splitlines()]
    assert [line["status"] for line in lines] == ["ok", "warning"]
    assert all("checked_at" in line for line in lines)
    assert sleeps == [0.5] and code == 1


def test_watch_keeps_going_after_an_input_error(tmp_path):
    target = tmp_path / "quorum.txt"  # does not exist on the first pass

    def fake_sleep(_):
        target.write_text((EXAMPLES / "healthy.txt").read_text())

    out, err = io.StringIO(), io.StringIO()
    args = cli.build_parser().parse_args(["--watch", "--count", "2", str(target)])
    code = cli.watch(args, io.StringIO(), out, err, sleep=fake_sleep)
    assert code == 0 and "cannot read" in err.getvalue() and "KRaft quorum: OK" in out.getvalue()
