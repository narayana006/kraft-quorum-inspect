# kraft-quorum-inspect

Show the health of a Kafka KRaft controller quorum at a glance: leader, epoch, voters, observers and replication lag, with a warning when the quorum is one failure away from losing its majority.

**Status:** working. It reads the output of `kafka-metadata-quorum describe --status` and `--replication` from files or stdin, or runs that tool for you. Text and JSON output, alerting exit codes and watch mode all work today. It talks to Kafka only through the `kafka-metadata-quorum` tool; there is no built-in Kafka client.

**Kafka versions:** parses the format printed by Kafka 3.3 through 3.8 (voters as plain ids) and by 3.9 and later (voters with endpoints and directory ids, extra `DirectoryId` column). Checked against real captures from Kafka 3.7.0 and 4.4.0, which are in [`examples/`](examples/).

## Install

Python 3.10 or newer, no dependencies.

```bash
git clone https://github.com/narayana006/kraft-quorum-inspect.git
cd kraft-quorum-inspect
pip install .
```

## Quick start

Try it on a capture from a healthy three-controller cluster:

```console
$ kraft-quorum-inspect examples/healthy.txt
KRaft quorum: OK
  Cluster id      q1Sh-9_ISia_zwGINzRvyQ
  Leader          node 2 (epoch 1)
  High watermark  336
  Voters          3 total, 3 healthy, majority 2, tolerates 1 more failure

Voters
  ID  ROLE      LOG END  LAG  BEHIND LEADER  STATE  ENDPOINT
  2   leader    339      0    0 ms           ok     CONTROLLER://controller-2:9093
  1   follower  339      0    486 ms         ok     CONTROLLER://controller-1:9093
  3   follower  339      0    484 ms         ok     CONTROLLER://controller-3:9093

Observers
  ID   ROLE      LOG END  LAG  BEHIND LEADER  STATE  ENDPOINT
  103  observer  339      0    485 ms         ok     -
  102  observer  339      0    484 ms         ok     -
  101  observer  339      0    488 ms         ok     -

No problems found.
```

And on a capture taken after the leader was stopped and another controller took over:

```console
$ kraft-quorum-inspect examples/after-leader-failover.txt
KRaft quorum: WARNING
  Cluster id      q1Sh-9_ISia_zwGINzRvyQ
  Leader          node 2 (epoch 3)
  High watermark  477
  Voters          3 total, 2 healthy, majority 2, tolerates 0 more failures

Voters
  ID  ROLE      LOG END  LAG  BEHIND LEADER  STATE        ENDPOINT
  2   leader    481      0    0 ms           ok           CONTROLLER://controller-2:9093
  1   follower  -        482  -              UNREACHABLE  CONTROLLER://controller-1:9093
  3   follower  481      0    20 ms          ok           CONTROLLER://controller-3:9093

Observers
  ID   ROLE      LOG END  LAG  BEHIND LEADER  STATE  ENDPOINT
  103  observer  481      0    23 ms          ok     -
  102  observer  481      0    25 ms          ok     -
  101  observer  481      0    19 ms          ok     -

Findings
  [WARNING] One more controller failure loses the majority: 2 of 3 voters are healthy and a majority needs 2.
  [WARNING] Voter 1: unreachable: the leader has not seen a fetch from it.
$ echo $?
1
```

`BEHIND LEADER` is how much older a node's last caught-up time is than the leader's. A few hundred milliseconds is normal on an idle cluster, because followers fetch about twice a second.

## Usage

### From saved output or a pipe

Give it one or more files, or pipe the tool's output in. A file may hold the `--status` output, the `--replication` output, or both; several inputs are merged. Log lines mixed into the output are ignored.

```bash
kafka-metadata-quorum.sh --bootstrap-controller ctrl1:9093 describe --replication | kraft-quorum-inspect
kraft-quorum-inspect status.txt replication.txt
```

`--replication --human-readable` output (`123 ms ago`) is understood too.

With only `--status` output there is no per-node detail, so the report falls back to the `MaxFollowerLag` and `MaxFollowerLagTimeMs` summary fields and says so:

```console
$ kraft-quorum-inspect examples/after-leader-failover-status.txt
KRaft quorum: WARNING
  Cluster id      q1Sh-9_ISia_zwGINzRvyQ
  Leader          node 2 (epoch 3)
  High watermark  477
  Voters          3 total, 2 healthy (estimated from the status summary), majority 2, tolerates 0 more failures

Voters
  ID  ROLE      LOG END  LAG  BEHIND LEADER  STATE    ENDPOINT
  1   follower  -        -    -              unknown  CONTROLLER://controller-1:9093
  2   leader    -        -    -              unknown  CONTROLLER://controller-2:9093
  3   follower  -        -    -              unknown  CONTROLLER://controller-3:9093

Observers
  ID   ROLE      LOG END  LAG  BEHIND LEADER  STATE    ENDPOINT
  103  observer  -        -    -              unknown  -
  102  observer  -        -    -              unknown  -
  101  observer  -        -    -              unknown  -

Findings
  [WARNING] One more controller failure loses the majority: 2 of 3 voters are healthy and a majority needs 2.
  [WARNING] At least one voter is behind the leader: MaxFollowerLagTimeMs is -1: the leader has no caught-up time for a voter. Supply 'describe --replication' output to see which.
```

### Live

Point it at the cluster and it runs `kafka-metadata-quorum describe --status` and `describe --replication` itself:

```bash
kraft-quorum-inspect --bootstrap-controller ctrl1:9093
kraft-quorum-inspect --bootstrap-server broker1:9092 --command-config client.properties
```

The tool is looked up in `$KAFKA_HOME/bin`, then on `PATH`, as `kafka-metadata-quorum.sh` or `kafka-metadata-quorum`. Use `--kafka-tool PATH` to name it explicitly and `--tool-timeout SECONDS` (default 30) to bound each call. The bootstrap address and `--command-config` are passed straight through, so security settings go in that properties file as usual.

### Watch mode

```bash
kraft-quorum-inspect --bootstrap-controller ctrl1:9093 --watch
```

Repeats every 5 seconds (`--interval SECONDS`) until Ctrl-C, or for `--count N` refreshes. Each report is preceded by a UTC timestamp. If the underlying tool fails, the error is printed and watching continues. Watch mode also works with files, which are re-read each time; it cannot be used with stdin. The exit code is that of the last refresh.

This is one refresh from a live cluster about 20 seconds after controller 1 was stopped:

```console
$ kraft-quorum-inspect --bootstrap-controller controller-2:9093,controller-3:9093 --watch --interval 2 --count 2
--- 2026-11-10T05:27:29Z ---
KRaft quorum: WARNING
  Cluster id      q1Sh-9_ISia_zwGINzRvyQ
  Leader          node 3 (epoch 4)
  High watermark  367
  Voters          3 total, 2 healthy, majority 2, tolerates 0 more failures

Voters
  ID  ROLE      LOG END  LAG  BEHIND LEADER  STATE    ENDPOINT
  3   leader    370      0    0 ms           ok       CONTROLLER://controller-3:9093
  1   follower  333      37   18619 ms       LAGGING  CONTROLLER://controller-1:9093
  2   follower  370      0    42 ms          ok       CONTROLLER://controller-2:9093

Observers
  ID   ROLE      LOG END  LAG  BEHIND LEADER  STATE  ENDPOINT
  101  observer  370      0    42 ms          ok     -
  102  observer  370      0    42 ms          ok     -
  103  observer  370      0    43 ms          ok     -

Findings
  [WARNING] One more controller failure loses the majority: 2 of 3 voters are healthy and a majority needs 2.
  [WARNING] Voter 1: last caught up 18619 ms before the leader (limit 5000 ms).
...
```

### JSON

`--json` prints one object (one per line in watch mode, with a `checked_at` field). Top-level fields:

| Field | Meaning |
|---|---|
| `status`, `exit_code` | `ok`/`warning`/`critical` and the matching exit code |
| `cluster_id`, `leader_id`, `leader_epoch`, `high_watermark` | from `describe --status`; `null` if that output was not supplied |
| `voter_count`, `majority`, `healthy_voters`, `failures_tolerated` | the quorum arithmetic |
| `has_replication_detail` | `false` when only `--status` output was available |
| `voters`, `observers` | per node: `node_id`, `role`, `directory_id`, `endpoints`, `log_end_offset`, `lag`, `behind_leader_ms`, `healthy`, `problems` |
| `findings` | list of `severity` and `message` |

```console
$ kraft-quorum-inspect --json examples/follower-stopped-human-readable.txt | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['status'], d['failures_tolerated'], d['voters'][2])"
warning 0 {'node_id': 3, 'role': 'follower', 'directory_id': None, 'endpoints': [], 'log_end_offset': 347, 'lag': 26, 'behind_leader_ms': 13401, 'healthy': False, 'problems': ['last caught up 13401 ms before the leader (limit 5000 ms)']}
```

### Exit codes

| Code | Meaning |
|---|---|
| 0 | healthy |
| 1 | warning: a voter is behind or unreachable, or one more controller failure would lose the majority |
| 2 | critical: no leader, or fewer healthy voters than a majority |
| 3 | the input could not be read or parsed, or the Kafka tool failed |

Usage errors exit with 2, as usual for argparse.

### How health is decided

A voter counts as unhealthy when the replication table shows any of:

- `LogEndOffset` or `LastFetchTimestamp` of `-1`: the current leader has not seen a fetch from it.
- `Lag` greater than `--max-lag` records (default 1000).
- a last caught-up time more than `--max-lag-ms` behind the leader's (default 5000).

With `N` voters a majority is `N/2 + 1` (integer division). `failures_tolerated` is healthy voters minus that majority. Zero means the next controller failure stops the quorum, which is a warning; this includes any cluster with one or two voters. Below zero is critical. Lagging observers (brokers) produce a warning but do not affect the quorum arithmetic. An even number of voters adds a note only.

## Features

- [x] Current leader, epoch and high watermark
- [x] Voter and observer list with lag per node
- [x] Warns when the quorum is one failure from losing majority
- [x] Watch mode
- [x] JSON output and an exit code for alerting

## Limitations

- It only knows what `kafka-metadata-quorum` prints. If that tool cannot reach the cluster, you get exit code 3 and the tool's last line of output, not a diagnosis.
- "Unhealthy" is inferred from lag and timestamps reported by the leader. A stopped follower looks the same as a badly lagging one (`LAGGING`) until a leader change resets its row to `-1` (`UNREACHABLE`).
- The default thresholds are judgment calls, not Kafka defaults. Tune `--max-lag` and `--max-lag-ms` for your cluster.
- Live mode was exercised against a local Kafka 4.4.0 Docker cluster through a wrapper script, and in the tests with a fake tool. It has not been run against a secured (TLS/SASL) cluster.
- The newer voter format with `directoryId` (dynamic quorums) is covered by a hand-written test case based on the Kafka source, not by a real capture: the example clusters use a static quorum.
- No Windows testing; `.bat` tool names are not searched for.

## Development

```bash
pip install -e ".[test]"
pytest
```

## Contributing

Bug reports with the exact tool output that was misread are the most useful contribution: attach it (redacted if needed) and say which Kafka version printed it. Pull requests are welcome; please add a test, ideally with a capture under `examples/`.

## Author

Built and maintained by [Narayana Vemburaj](https://github.com/narayana006).

## License

[MIT](LICENSE)
