# Example inputs

All of these are unedited output of `kafka-metadata-quorum.sh describe`, captured from
local Docker clusters. The Kafka 4.4.0 captures come from a cluster of three controllers
(node ids 1-3, static quorum) and three brokers (node ids 101-103).

| File | Kafka | Contents | Situation |
|---|---|---|---|
| `healthy.txt` | 4.4.0 | `--status` then `--replication` | All three controllers up. |
| `healthy-status.txt`, `healthy-replication.txt` | 4.4.0 | the two halves of `healthy.txt` | Same. |
| `follower-stopped.txt` | 4.4.0 | `--status` then `--replication` | Follower controller 3 stopped about 10 seconds earlier; the leader still reports its last known offsets. |
| `follower-stopped-human-readable.txt` | 4.4.0 | `--replication --human-readable` | Same situation, a few seconds later. |
| `after-leader-failover.txt` | 4.4.0 | `--status` then `--replication` | The previous leader (node 1) was stopped and node 2 took over; the new leader has never heard from node 1, so its row shows `-1`. |
| `after-leader-failover-status.txt` | 4.4.0 | `--status` only | Same. |
| `single-node.txt` | 4.4.0 | `--status` then `--replication` | One combined broker/controller. |
| `kafka-3.7-single-node.txt` | 3.7.0 | `--status` then `--replication` | Older format: voters as plain ids, no `DirectoryId` column, `ClusterId` wrapped in `Some(...)`. |
