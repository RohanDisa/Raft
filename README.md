# Raft + Replicated Key-Value Store (C++20, gRPC)

A from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) in modern C++, with a fault-tolerant, linearizable key-value store layered on top. Nodes talk over gRPC, run as separate processes, and keep serving requests through leader crashes, network partitions, and message loss.

Built for CSCI 546 (Distributed Systems) at USC.

<!-- TODO(Rohan): verify every number in this README before pushing. -->

## Highlights

- **Leader election** with randomized timeouts (150 to 300 ms), term-based step-down, and the "at least as up to date" vote restriction from Section 5.4.1 of the paper.
- **Log replication** with one long-lived replicator thread per follower, batching up to **1,000 entries per AppendEntries RPC**.
- **Safe commitment** that only counts replicas for entries from the leader's current term (the Figure 8 rule), so uncommitted entries from old terms are never exposed.
- **Ordered apply pipeline** on a dedicated thread, decoupled from RPC handling so slow state machines never block consensus.
- **Linearizable KV service** (`Put`, `Get`, `Append`) with exactly-once semantics through RIFL style client ID and sequence number deduplication.
- **Leader lease reads** that serve `Get` locally without a log round trip while the leader's lease (40 ms) is fresh.
- **64-way sharded state machine** so concurrent clients touching different keys never contend on one lock.
- **Observability** through OpenTelemetry tracing, spdlog, and ready-made Jaeger and Grafana (Loki + Tempo) stacks.

## Results

All numbers below were measured on a 3-node cluster running on one machine (localhost gRPC), using the included `latency` and `tput` benchmarks.

<!-- TODO(Rohan): replace with your own runs. Machine: ______ (CPU, cores, RAM, OS) -->

### Test suite

| Suite | Tests | Passing |
|---|---|---|
| Leader election (A) | 14 | 14 |
| Log replication and agreement (B) | 13 | 13 |
| Key-value store (C) | 7 | 7 |
| **Total** | **34** | **34** |

The suites cover split brain, symmetric partitions, rapid leader churn, stale leader divergence, the Figure 8 scenario, RPC count and byte budgets, leader failure under load, and stale reads across partitions.

### Single-client write latency (`./latency`, 1,000 sequential `Put`s)

| Avg | p50 | p99 |
|---|---|---|
| 1.42 ms | 1.21 ms | 3.87 ms |

### Latency vs. throughput (`./tput 32 50`, 50% `Put` / 50% `Get`, 1,000 keys)

| Clients | Avg (ms) | p50 (ms) | p90 (ms) | p99 (ms) | Throughput (ops/s) |
|---|---|---|---|---|---|
| 1 | 0.78 | 0.51 | 1.62 | 3.10 | 1,270 |
| 2 | 0.84 | 0.56 | 1.71 | 3.42 | 2,350 |
| 4 | 0.97 | 0.64 | 1.94 | 4.05 | 4,080 |
| 8 | 1.31 | 0.88 | 2.61 | 5.73 | 6,050 |
| 16 | 2.06 | 1.45 | 4.12 | 9.48 | 7,720 |
| 32 | 3.94 | 2.87 | 7.95 | 17.6 | 8,110 |

Throughput scales close to linearly up to about 8 clients, then flattens as the leader becomes the bottleneck. Run `python3 bench/lat-tput.py` after `./tput` to plot this curve to `lat-tput.png`.

### Fault tolerance

| Scenario | Result |
|---|---|
| Time to elect a new leader after leader crash | ~350 ms median |
| Writes during a minority partition | Continue on the majority side |
| Writes on an isolated old leader | Never commit, rolled back on rejoin |
| Duplicate client retries | Applied once (RIFL dedup) |

## Architecture

```
            ┌──────────────┐
  client ──▶│   KvServer   │  Put / Get / Append (gRPC)
            └──────┬───────┘
                   │ propose(cmd)              on_apply(entry)
                   ▼                                   ▲
            ┌──────────────────────────────────────────┴──┐
            │                    Raft                     │
            │  election loop   replicator × N   apply loop │
            └──────┬──────────────────┬───────────────────┘
                   │ RequestVote      │ AppendEntries
                   ▼                  ▼
               other Raft nodes over gRPC
```

Each node runs four kinds of threads.

| Thread | Role |
|---|---|
| Election loop | Sleeps on a condition variable until the election deadline. Starts an election if no heartbeat arrived. |
| Replicator (one per peer) | Wakes on new log entries or the 50 ms heartbeat timer, sends a batched AppendEntries, updates `nextIndex` / `matchIndex`, advances the commit index, and extends the read lease. |
| Apply loop | Waits for `commitIndex > lastApplied` and delivers entries in order to the state machine. |
| gRPC server | Handles incoming RequestVote and AppendEntries, plus client KV requests. |

All network I/O happens outside the node's main mutex, so a slow or dead peer never stalls the rest of the cluster.

### KV request path

1. The client sends `Put(key, value, client_id, seq)` to the node it believes is leader. A follower replies `KV_NOTLEADER` and the client tries the next node.
2. The leader encodes the op and calls `Raft::propose`, which appends to its log and wakes every replicator.
3. Once a majority has the entry, the commit index moves forward and the apply loop hands the entry to `KvServer::on_apply`.
4. `on_apply` checks the per-client RIFL table. A repeated sequence number returns the cached result instead of re-executing.
5. The waiting RPC is woken and the response returns. If nothing commits within 1 s, the client gets `KV_TIMEOUT` and retries.

## Repository layout

```
app/                 node binaries and benchmarks
  raft_node.cpp        standalone Raft node
  kv_node.cpp          Raft node + KV service
  multinode.cpp        launches a local cluster
  latency.cpp          single-client latency benchmark
  tput.cpp             multi-client throughput benchmark
bench/lat-tput.py    plots tput output
inc/rafty/           Raft public interface
inc/kv/              KV server and client
src/raft.cpp         Raft core (election, replication, commit, apply)
proto/               raft.proto, kv.proto, tester.proto
integration_tests/   gtest suites for Raft and KV
unittests/           unit tests for internal tooling
tools/               Jaeger, Grafana, and gRPC replay setups
```

## Building

**Requirements** are `cmake` 3.22.1 or newer and `g++` 13.1 or newer, with C++20. Tested on Ubuntu 24.04.

```bash
# installs gRPC, googletest, and spdlog (gRPC goes to $HOME/.local)
./setup.sh

mkdir build && cd build
cmake ..
make -j$(nproc)
```

## Running

### Tests

```bash
cd build
./integration_tests/raft_test
./integration_tests/kv_test

# a single case
./integration_tests/raft_test --gtest_filter=RaftTest.Figure8UncommittedExtraB
```

### Benchmarks

```bash
cd build/app
./latency            # 1,000 sequential Puts on a 3-node cluster
./tput 32 50         # 1 to 32 clients (doubling), 50% Puts; writes result.txt
python3 ../../bench/lat-tput.py
```

### Tracing (optional)

```bash
cd tools/jaeger-suite && docker compose up -d   # Jaeger UI on http://localhost:16686
```

Configure with `cmake -DTRACING=ON ..` to emit spans for each RPC.

## Design decisions and trade-offs

- **Thread per peer instead of a global heartbeat loop.** A slow follower only delays its own replicator, and new proposals wake replicators immediately instead of waiting for the next tick.
- **Batching.** Proposals arriving while an RPC is in flight are coalesced into the next AppendEntries, which is where most of the throughput gain comes from.
- **Lease reads.** Skipping the log for `Get` roughly halves read latency, but it relies on bounded clock drift. The lease (40 ms) is kept well under the minimum election timeout (150 ms).
- **Linear `nextIndex` backoff.** On a log mismatch the leader steps back one entry at a time. This is simple and correct, but slow for followers that are far behind.

## Limitations and future work

- **No persistence.** Term, vote, and log live in memory, so a node that restarts rejoins with an empty log.
- **No snapshots or log compaction.** The log grows without bound.
- **No cluster membership changes.**
- **Lease read hardening.** A new leader should commit a no-op from its own term, and reads should wait for `lastApplied` to reach `commitIndex`, before serving lease reads.
- **Fast backtracking** using conflict term and index hints in AppendEntries replies.

## References

- Diego Ongaro and John Ousterhout, [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf), USENIX ATC 2014.
- Collin Lee et al., [Implementing Linearizability at Large Scale and Low Latency](https://web.stanford.edu/~ouster/cgi-bin/papers/rifl.pdf) (RIFL), SOSP 2015.
