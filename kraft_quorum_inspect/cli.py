"""Command-line entry point."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone

from . import __version__
from .health import DEFAULT_MAX_LAG, DEFAULT_MAX_LAG_MS, Report, evaluate
from .parser import ParseError, Snapshot, merge, parse
from .render import render_text

EXIT_ERROR = 3
TOOL_NAMES = ("kafka-metadata-quorum.sh", "kafka-metadata-quorum")


class InputError(Exception):
    """Input could not be obtained (missing file, tool failure, ...)."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kraft-quorum-inspect",
        description="Show the health of a Kafka KRaft controller quorum from "
        "'kafka-metadata-quorum describe' output.",
        epilog="Exit codes: 0 healthy, 1 warning, 2 critical, 3 input could not be read.",
    )
    parser.add_argument(
        "files",
        nargs="*",
        metavar="FILE",
        help="saved output of 'describe --status' and/or 'describe --replication'; "
        "'-' or no argument reads stdin",
    )
    live = parser.add_argument_group("live mode (runs kafka-metadata-quorum for you)")
    live.add_argument("--bootstrap-controller", metavar="HOST:PORT[,...]", help="controller endpoints")
    live.add_argument("--bootstrap-server", metavar="HOST:PORT[,...]", help="broker endpoints")
    live.add_argument("--command-config", metavar="FILE", help="client properties file passed to the tool")
    live.add_argument(
        "--kafka-tool",
        metavar="PATH",
        help="path to kafka-metadata-quorum(.sh); default: $KAFKA_HOME/bin, then PATH",
    )
    live.add_argument("--tool-timeout", type=float, default=30.0, metavar="SECONDS", help="default: 30")
    parser.add_argument("--max-lag", type=int, default=DEFAULT_MAX_LAG, metavar="RECORDS",
                        help=f"flag a node more than this many records behind (default {DEFAULT_MAX_LAG})")
    parser.add_argument("--max-lag-ms", type=int, default=DEFAULT_MAX_LAG_MS, metavar="MS",
                        help="flag a node whose last caught-up time trails the leader's by more "
                        f"than this (default {DEFAULT_MAX_LAG_MS})")
    parser.add_argument("--json", action="store_true", help="print JSON instead of text")
    parser.add_argument("--watch", action="store_true", help="repeat until interrupted")
    parser.add_argument("--interval", type=float, default=5.0, metavar="SECONDS",
                        help="seconds between refreshes in watch mode (default 5)")
    parser.add_argument("--count", type=int, default=0, metavar="N",
                        help="in watch mode, stop after N refreshes (default: run until Ctrl-C)")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def find_tool(explicit: str | None) -> str:
    if explicit:
        if not os.path.isfile(explicit):
            raise InputError(f"--kafka-tool {explicit}: no such file")
        return explicit
    kafka_home = os.environ.get("KAFKA_HOME")
    if kafka_home:
        for name in TOOL_NAMES:
            candidate = os.path.join(kafka_home, "bin", name)
            if os.path.isfile(candidate):
                return candidate
    for name in TOOL_NAMES:
        found = shutil.which(name)
        if found:
            return found
    raise InputError(
        "kafka-metadata-quorum not found. Set KAFKA_HOME, add Kafka's bin directory to PATH, "
        "or pass --kafka-tool. You can also pipe saved output into this command."
    )


def run_tool(args: argparse.Namespace, mode: str) -> str:
    tool = find_tool(args.kafka_tool)
    command = [tool]
    if args.bootstrap_controller:
        command += ["--bootstrap-controller", args.bootstrap_controller]
    else:
        command += ["--bootstrap-server", args.bootstrap_server]
    if args.command_config:
        command += ["--command-config", args.command_config]
    command += ["describe", mode]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=args.tool_timeout)
    except subprocess.TimeoutExpired as exc:
        raise InputError(f"{' '.join(command)} timed out after {args.tool_timeout:g}s") from exc
    except OSError as exc:
        raise InputError(f"cannot run {tool}: {exc}") from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip().splitlines()
        raise InputError(
            f"{' '.join(command)} exited with {result.returncode}"
            + (f": {detail[-1]}" if detail else "")
        )
    return result.stdout


def read_snapshot(args: argparse.Namespace, stdin) -> Snapshot:
    if args.bootstrap_controller or args.bootstrap_server:
        return merge([parse(run_tool(args, "--status")), parse(run_tool(args, "--replication"))])
    snapshots = []
    for name in args.files or ["-"]:
        if name == "-":
            text = stdin.read()
        else:
            try:
                with open(name, encoding="utf-8") as handle:
                    text = handle.read()
            except OSError as exc:
                raise InputError(f"cannot read {name}: {exc.strerror}") from exc
        try:
            snapshots.append(parse(text))
        except ParseError as exc:
            raise InputError(f"{'stdin' if name == '-' else name}: {exc}") from exc
    return merge(snapshots)


def emit(report: Report, args: argparse.Namespace, out, checked_at: str | None = None) -> None:
    if args.json:
        data = report.to_dict()
        if checked_at:
            data["checked_at"] = checked_at
        # One object per line in watch mode so the stream can be consumed line by line.
        print(json.dumps(data, indent=None if args.watch else 2), file=out, flush=True)
    else:
        if checked_at:
            print(f"--- {checked_at} ---", file=out)
        print(render_text(report), file=out, flush=True)


def inspect_once(args: argparse.Namespace, stdin, out, checked_at: str | None = None) -> int:
    snapshot = read_snapshot(args, stdin)
    report = evaluate(snapshot, max_lag=args.max_lag, max_lag_ms=args.max_lag_ms)
    emit(report, args, out, checked_at)
    return report.exit_code


def watch(args: argparse.Namespace, stdin, out, err, sleep=time.sleep) -> int:
    code = EXIT_ERROR
    iteration = 0
    try:
        while True:
            iteration += 1
            now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            try:
                code = inspect_once(args, stdin, out, checked_at=now)
            except (InputError, ParseError) as exc:
                # Keep watching: during an incident the tool itself may fail for a while.
                code = EXIT_ERROR
                print(f"--- {now} ---\nerror: {exc}", file=err, flush=True)
            if args.count and iteration >= args.count:
                return code
            if not args.json:
                print(file=out)
            sleep(args.interval)
    except KeyboardInterrupt:
        return code


def main(argv: list[str] | None = None, stdin=None, out=None, err=None) -> int:
    stdin = stdin or sys.stdin
    out = out or sys.stdout
    err = err or sys.stderr
    parser = build_parser()
    args = parser.parse_args(argv)

    live = bool(args.bootstrap_controller or args.bootstrap_server)
    if args.bootstrap_controller and args.bootstrap_server:
        parser.error("use either --bootstrap-controller or --bootstrap-server, not both")
    if live and args.files:
        parser.error("give either files or a bootstrap address, not both")
    reads_stdin = not live and (not args.files or "-" in args.files)
    if args.watch and reads_stdin:
        parser.error("--watch needs a bootstrap address or files to re-read; it cannot re-read stdin")
    if reads_stdin and not args.files and stdin.isatty():
        parser.error("no input: pass files, pipe output in, or use --bootstrap-controller")
    if args.interval <= 0:
        parser.error("--interval must be positive")

    if args.watch:
        return watch(args, stdin, out, err)
    try:
        return inspect_once(args, stdin, out)
    except (InputError, ParseError) as exc:
        print(f"kraft-quorum-inspect: error: {exc}", file=err)
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
