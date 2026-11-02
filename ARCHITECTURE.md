# DistriLab A2 — How the system works

This document explains the design in plain terms: what each process does, which
messages travel between them, and how failures are detected and recovered.

---

## 1. The processes

```
                         +-------------------+
          register,      |  Bootstrap Node   |   "phone book": who is in the cluster,
          lease renew    |  (directory only) |   who is the coordinator
        +--------------> +-------------------+ <-------------+
        |                                                    | GetCoordinator,
        |                                                    | ListWorkers
  +-----+------+   overlay link   +------------+             |
  |  Worker 1  |------------------|  Worker 2  |        +----+-----+
  +-----+------+                  +-----+------+        |  Client  |  (GUI or CLI,
        |  \                     /      |               +----+-----+   many at once)
        |   \   +------------+  /       |                    |
        |    +--|  Worker 5  |-+        |   SubmitJob,       |
        |       | COORDINATOR|<---------+---WatchJob (stream)+
        |       +-----+------+          |
  +-----+------+      |          +------+-----+
  |  Worker 3  |------+----------|  Worker 4  |
  +------------+                 +------------+
```

| Process | Runs | Job |
|---|---|---|
| **Bootstrap Node** | 1 | Directory service: gives a joining worker a unique id and some random peers; tells clients where the coordinator is. Never computes, never votes. |
| **Worker** | many (any computers) | Computes subtasks. Exactly one worker at a time is also the **coordinator**. |
| **Client** | many | GUI/CLI. Submits jobs and shows live progress. |

Every process is a separate JVM started on its own (`java -cp target\distrilab.jar ...`). The `.bat`
"demo" scripts only start several of them on one laptop for convenience. On several
computers you start each worker on its own machine with
`--bootstrap=<bootstrap-ip>:50050`.

## 2. Topology: the unstructured overlay

* A joining worker asks the Bootstrap Node for **2 random active workers** and links to
  both (links are bidirectional). Later workers may link to it too, so workers end up
  with **different numbers of neighbours** (2, 3, 4 ...) and no fixed structure.
* Each worker pings its neighbours every 2 s. A neighbour that misses 2 pings is removed,
  and if a worker drops below 2 neighbours it asks the Bootstrap Node for new random
  peers (**overlay repair**).
* Why at least 2 links? In Assignment 1 every worker linked to only one peer, which forms
  a tree. In a tree, one crashed worker splits the network, so the next election misses
  workers. With 2+ random links and repair, the overlay stays connected.

## 3. All gRPC services (contracts in `src/main/proto`)

| Service (file) | Served by | RPCs | Used for |
|---|---|---|---|
| `BootstrapService` (bootstrap.proto) | Bootstrap | Register, Deregister, Heartbeat (lease), GetPeers, ListWorkers, ReportFailure, ReportCoordinator, GetCoordinator | membership and discovery |
| `PeerService` (peer.proto) | every worker | AddNeighbour, Heartbeat, GetStatus | overlay links, failure detection |
| `ElectionService` (election.proto) | every worker | Election, Coordinator | leader election |
| `TaskService` (task.proto) | every worker | Execute (**server-streaming**) | run a subtask, stream progress and result |
| `ReplicationService` (replication.proto) | every worker | Replicate, FetchState | copy job state so a new coordinator can recover it |
| `JobService` (job.proto) | every worker (only the coordinator accepts jobs) | SubmitJob, WatchJob (**server-streaming**), LocateJob, WhoIsCoordinator, GetClusterStatus, CancelJob | client API |

Every call has a **deadline**. Errors arrive as `StatusRuntimeException` with a status
code: `UNAVAILABLE` = cannot connect (process dead), `DEADLINE_EXCEEDED` = too slow, and so on.

## 4. Message flows

### 4.1 A worker joins
1. Worker → Bootstrap `Register(requested id or 0, host, port)`. Bootstrap replies with
   the **assigned id** (the next unused id; ids are never reused), 2 random peers and the
   last known coordinator.
2. Worker → each peer `AddNeighbour(self)`; the peer adds it and replies with *its*
   view of the coordinator.
3. The worker checks the coordinator with a direct `Heartbeat` before trusting it.
4. From then on: lease renewal every 2 s, neighbour pings every 2 s, and the coordinator
   heartbeats it every 1 s.

### 4.2 Leader election (wave / echo algorithm)
Triggered when there is no coordinator (start-up), when the coordinator failed, or when
the coordinator has assigned 5 jobs (term limit).

```
initiator W1                 W2                    W3
   | Election(id,term) ---->  |                      |
   |                          | Election(id,term) -> |
   |                          |                      | (W3 forwards to its other neighbours)
   |                          | <- best in sub-tree  |
   | <- best(W2 sub-tree)     |                      |
   |   best = lowest JAC, tie -> highest id          |
   | Coordinator(term, winner) flooded to everyone ...
```
* Each election has a unique **election id**. A worker records the ids it has seen; if
  the same id arrives again (via another path, since the overlay has cycles) it answers
  `duplicate=true` immediately and **does not process or forward it again**.
* Replies flow back along the path the message came, merging the best candidate
  (`lowest JAC, ties -> highest id`), so the initiator learns the best of **all
  reachable workers**. The log line `finished: 5 workers took part [1, 2, 3, 4, 5]`
  shows this.
* The initiator floods a `Coordinator(term, winner)` announcement, also with duplicate
  suppression. Every worker applies one rule (`ClusterState.adopt`): a higher term
  wins; an older term is ignored; two different winners for the same term (two
  elections ran at once) are resolved with the same lowest-JAC / highest-id rule, so
  everyone converges on one coordinator.
* A random 0–800 ms back-off before starting means that usually only one worker starts
  an election after a failure.
* Safety: a worker that can still reach a live coordinator **rejects** an election
  started by someone else. It double-checks with a direct ping, so one worker's false
  suspicion cannot replace a healthy coordinator.

**JAC**: each worker's Job Allocation Counter; +1 each time it, as coordinator,
distributes a job to other workers (`jacCounting=job`, configurable to `subtask`).
After a coordinator assigns 5 jobs, its JAC is higher than everyone else's, so the
next election picks someone else.

### 4.3 A job from submission to result
1. Client → Bootstrap `GetCoordinator`, then → coordinator `WhoIsCoordinator` to confirm.
2. Client → coordinator `SubmitJob(job_id = new UUID, type, data)`.
3. Coordinator: checks it is the coordinator and the term has room (5 jobs), validates
   the input, **replicates the job to at least one other worker** (write-ahead), then
   replies "accepted".
4. The job is split into `workers × 4` equal subtasks (e.g. PRIMESUM 1..1000 over 4 →
   [1..250] [251..500] [501..750] [751..1000]). Each subtask has a unique id
   (`job_id#index`) and is assigned round-robin.
5. Coordinator → worker `TaskService.Execute(subtask)`: a **server stream**. The worker
   computes on its thread pool and streams `progress(units done / total)` every 150 ms
   (plus a keep-alive every 2 s), then `result`.
6. Each finished subtask is recorded once and replicated. When all are done, the
   coordinator combines them (max / sum / count) and replicates "COMPLETED".
7. Client → coordinator `WatchJob(job_id)`: a **server stream** of `JobProgress`
   (percent from completed work units, per-worker breakdown, events), pushed every
   250 ms while something changes. That drives the progress bar and the details dialog.

### 4.4 Concurrency
* Workers: a fixed pool of compute threads runs many subtasks (from many jobs and
  clients) in parallel; gRPC handles network calls on separate threads, so a busy worker
  still answers heartbeats.
* Coordinator: each job is a `JobRun` object whose methods are `synchronized`
  (callbacks from many workers update it); maps are `ConcurrentHashMap`; counters are
  `AtomicInteger`; the term counter has its own lock.
* Client: one `JobTracker` thread per job, so many jobs run concurrently; Swing is only
  updated on the event thread (Swing timers and `invokeLater`).

## 5. Failure detection: active vs slow vs failed

| Mechanism | Who | What |
|---|---|---|
| **Heartbeats** | coordinator → every worker, every 1 s, deadline 1.5 s | 2 misses → **SUSPECT**, 4 misses → **DEAD** |
| **Coordinator watch** | every worker | no coordinator heartbeat for 4 s → direct check → if that fails too: coordinator failed → election |
| **Task stream** | coordinator ↔ worker | a broken `Execute` stream (`UNAVAILABLE`) is immediate evidence; the subtask is reassigned at once |
| **Progress stall** | coordinator | no progress or keep-alive for 30 s while the worker still answers heartbeats → backup copy elsewhere |
| **Leases** | Bootstrap | a worker that stops renewing for 6 s is removed from the directory |

* **Active**: answers heartbeats and streams progress.
* **Slow**: misses a couple of heartbeats (overloaded, long GC pause, slow network). It
  becomes SUSPECT: it **keeps** its running subtasks but receives no new ones. If it
  recovers it is ALIVE again; nothing is lost or duplicated.
* **Failed**: misses 4 heartbeats in a row, or its connection is refused. It becomes
  DEAD and its unfinished subtasks are reassigned. If it was only very slow and later
  sends a result anyway, that result is **ignored**, because the subtask is already
  recorded (first result wins, by unique task id).

## 6. Recovery

### 6.1 Worker failure
Coordinator: stream error or DEAD → the worker's running subtasks get a new attempt on
the least-loaded live worker (`attempt` number + 1). The progress bar drops a little
(the dead worker's partial work is lost) and then continues. Results stay correct
because each subtask counts exactly once. A new worker that joins appears in the
coordinator's membership within a second and receives new and reassigned subtasks.

### 6.2 Coordinator failure
1. Workers stop receiving heartbeats, verify with a direct ping, and start an election
   for **term + 1** (the dead coordinator cannot take part; the lowest-JAC live worker wins).
2. The new coordinator **pulls the replicated state** from every worker
   (`FetchState`) and merges it.
3. For every unfinished job whose owner is dead, it **adopts** the job: it keeps all
   completed subtask results (they were replicated) and recomputes only the missing ones.
4. The client's progress stream broke when the old coordinator died. The client asks
   any worker `LocateJob(job_id)`, finds the new owner, and resumes `WatchJob`. If no
   worker knows the job (it crashed before being replicated), the client resubmits it
   with the **same job id**; the cluster deduplicates, so it never runs twice.

### 6.3 What state must survive a coordinator failure (and how)

| State | Where it survives |
|---|---|
| Job input (type, numbers / range) and how it was split | replicated to other workers **before** the client is told "accepted" |
| Which subtasks are complete, and their results | replicated after each subtask finishes |
| Final result / failed / cancelled | replicated when the job ends |
| Who owns each job, and in which term | replicated; a newer term wins |
| Current term and coordinator | every worker (announcements); Bootstrap keeps the latest |
| Membership | Bootstrap (leases) and the new coordinator's heartbeats |
| Each worker's JAC | each worker itself; reported in heartbeats and elections |
| Running (incomplete) subtasks | **not** saved: recomputed (cheap compared with replicating every progress tick) |

Merging replicas never conflicts: completed subtasks are a grow-only set (a result is
deterministic, so any copy is correct), a finished state beats an unfinished one, and
otherwise the newer (term, version) wins.

### 6.4 The 5-job term limit
After accepting its 5th job, the coordinator closes the term (further submissions get
"term ended, retry") and starts an election itself (`reason = TERM_LIMIT`). Its own JAC
is now high, so another worker wins. Jobs already running keep running on the old
coordinator (it still owns them); if it later fails, the new coordinator adopts them.

## 7. Why these choices (and the alternatives)

| Choice | Why | Alternatives considered |
|---|---|---|
| Heartbeats + deadlines + suspicion levels | simple, cheap, distinguishes slow from dead | a single timeout (cannot tell slow from dead); phi-accrual detector (more complex) |
| Wave/echo election over the overlay | works on any unstructured topology; considers all reachable workers; meets the JAC rule | Bully (needs everyone to know everyone); Ring (needs a ring); Raft (much more complex; elects by log freshness, not JAC) |
| Replicate job state to all workers | any worker can become coordinator and recover | single backup (lost if both fail); checkpoint files (not shared between computers); ZooKeeper/etcd (an extra service) |
| Unique task ids + first result wins | makes reassignment safe; no double counting | locking or two-phase commit (slow, complex) |
| Client-generated job ids + idempotent submit | safe resubmission after a crash | exactly-once delivery (impossible in general) |
| Bootstrap with leases | fast membership, no probing | A1's TCP probe of every worker on every request (slow); static peer lists; UDP broadcast (LAN only) |

## 8. Why gRPC instead of Java RMI
* **Language-neutral contracts** (`.proto`): clients could be written in Python, Go and
  so on; RMI is Java-only.
* **HTTP/2**: many calls share one connection; **streaming** in both directions
  (we use server streaming for task progress and job progress); RMI has no streaming.
* **Protocol Buffers**: compact binary encoding, explicit schema, backward-compatible
  evolution; RMI uses Java serialization (larger, version-fragile, security issues).
* **Deadlines, cancellation and status codes** are built in; with RMI, a hung call
  blocks forever unless you configure socket timeouts.
* No RMI registry; works through firewalls and proxies like normal HTTP/2 traffic.

## 9. The Bootstrap Node
* **Purpose**: solves first contact. A new worker or client must find at least one
  live member, and the Bootstrap Node is a well-known address for that. It also hands out
  unique ids and keeps membership through leases.
* **Not a single point of failure for computation**: if it crashes, running workers keep
  computing, electing and recovering. Every worker keeps its own directory of the peers it
  has heard of (from the Bootstrap Node, overlay links, heartbeats and elections), and every
  worker introduces itself to a newly elected coordinator, so even a coordinator elected
  while the Bootstrap Node is down knows the whole cluster. Only new joins are affected.
  A running client remembers the workers it has seen; a new client can be given backup
  worker addresses with `--contacts=host:port,...`.
* **Without it**: a static list of seed addresses in a config file, UDP broadcast/multicast
  discovery on a LAN, or a DHT (e.g. Kademlia) with known seed nodes.

## 10. Performance notes (answers to "why was A1 slow?")
* PRIMESUM uses a **segmented sieve of Eratosthenes** (1..10^9 in about 3 s on one core),
  where A1 trial-divided every number (about 30 min).
* PRIMECOUNT uses a **deterministic Miller–Rabin** test (correct for all 64-bit numbers,
  fast for 18-digit values).
* Sums use an overflow-safe accumulator (BigInteger when needed): sums of primes beyond
  ~2·10^10 do not fit in a `long`.
* Membership comes from leases, not from probing every worker on every request.
