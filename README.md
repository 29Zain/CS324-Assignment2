# DistriLab Distributed Computing Cluster — CS324 Assignment 2 (gRPC)

A fault-tolerant distributed computing cluster in Java. Worker processes form an
unstructured overlay network, elect a coordinator, and split jobs from many clients
across all workers. All communication uses **gRPC with Protocol Buffers**.

| | |
|---|---|
| Job types | `MAX(numbers)`, `PRIMESUM(start, end)`, `PRIMECOUNT(numbers)` |
| New in A2 | gRPC everywhere, live progress bars (gRPC streaming) with a per-worker breakdown, worker failure tolerance, coordinator failure tolerance with state recovery |
| Platform | Windows 10 / 11 (the Java code itself runs on any OS) |

> Built as a three-person team project for CS324 (Distributed Computing) at the
> University of the South Pacific, 2026. Team members' names are left out for privacy.

> How the system works (topology, messages, election, failure detection, recovery)
> is explained in **ARCHITECTURE.md**.

---

## 1. Requirements

* **JDK 21** or newer: `java -version`
* **Apache Maven 3.9** or newer: `mvn -v`
* An internet connection for the **first** build (Maven downloads gRPC and Protocol Buffers).

## 2. Project layout

```text
├── src/main/
│   ├── proto/                     gRPC contracts (*.proto) - compiled to Java during the build
│   └── java/
│       ├── bootstrap/             Bootstrap Node   (main class: bootstrap.BootstrapNode)
│       │     BootstrapNode.java     registration, unique ids, leases, peer lists
│       ├── worker/                Worker Node      (main class: worker.Worker)
│       │     Worker.java            start-up, settings, gRPC server
│       │     Overlay.java           neighbour links, pings, overlay repair
│       │     Election.java          wave/echo election, terms, cluster view
│       │     FailureDetector.java   heartbeats (ALIVE/SUSPECT/DEAD), coordinator monitor
│       │     Coordinator.java       job splitting, scheduling, reassignment, progress streams
│       │     Replication.java       replicated job table, recovery after coordinator crash
│       │     TaskServiceImpl.java   runs subtasks and streams their progress
│       ├── client/                Client GUI / CLI (main class: client.Client)
│       │     Client.java            start-up, settings, CLI, CSV loading, data generator
│       │     ClusterClient.java     finds the coordinator, submits and watches jobs, recovery
│       │     ClientGUI.java         Swing GUI (job cards, per-worker breakdown, cluster view)
│       └── common/                code shared by all three
│             Jobs.java              MAX / PRIMESUM / PRIMECOUNT jobs and subtasks
│             Primes.java            sieve, Miller-Rabin, overflow-safe sums
│             Net.java               gRPC channels, host:port, proto conversion
│             Config.java, Log.java  configuration files / logging
├── src/test/java/test/            unit tests (JUnit 5): DistriLabTest.java
├── config/                        bootstrap.properties, worker.properties, client.properties
├── data/                          test datasets with known answers (see data/DATASETS.md)
├── scripts/                       Windows .bat scripts: build, start, stop, crash, tests
├── pom.xml                        Maven build
├── README.md
└── ARCHITECTURE.md                design and fault-tolerance explanation
```

The Bootstrap Node, worker and client are separate programs in separate packages. Each
is started as its own process; they only share the `common` package and the generated
gRPC code (package `proto`).

## 3. Build

Double-click **`scripts\build.bat`**, or in a terminal in the project folder:

```cmd
mvn clean package
```

This generates the gRPC code from `src\main\proto`, compiles everything, runs the unit
tests and creates **`target\distrilab.jar`** (one runnable jar with all three programs).

## 4. Run on ONE computer (demonstration)

Double-click **`scripts\demo-local.bat`** (or run `scripts\demo-local.bat 5`).

It opens the Bootstrap Node, then 5 workers **one at a time** (1 s apart), each in its own
window, then the client GUI. Close everything with **`scripts\stop-all.bat`**.

> The demo script is only a convenience for one laptop. Each part is an independent
> program and can be started on its own, as shown next.

## 5. Run each part manually (one or many computers)

Start the parts **in this order**, from the project folder.

**Step 1: Bootstrap Node** (one computer, e.g. `192.168.1.50`):
```cmd
java -cp target\distrilab.jar bootstrap.BootstrapNode --port=50050
```
or `scripts\start-bootstrap.bat`

**Step 2: Workers** (one command per worker, on any computer):
```cmd
java -cp target\distrilab.jar worker.Worker --bootstrap=192.168.1.50:50050
```
or `scripts\start-worker.bat --bootstrap=192.168.1.50:50050`

* Without `--id`, the Bootstrap Node assigns the next unused id. Ids are never reused, so
  a new worker started after Worker-2 crashed becomes Worker-6, not Worker-2.
* `--id=3` requests a specific id (rejected while a live worker still has it).
* `--port=50053` sets the port (default: any free port).
* `--host=192.168.1.51` sets the address other computers use to reach this worker. It is
  detected automatically when the Bootstrap Node is on another computer.

Several workers can run on one computer; each just needs its own port.

**Step 3: Client(s)** (any computer, as many as you like):
```cmd
java -cp target\distrilab.jar client.Client --bootstrap=192.168.1.50:50050
```
or `scripts\start-client.bat --bootstrap=192.168.1.50:50050`

**Several computers:** copy the project folder (with `target\distrilab.jar`, `config` and
`data`) to each computer, and allow inbound TCP on port 50050 and the worker ports in the
firewall. Fixed worker ports (`--port`) make firewall rules easier.

## 6. Configuration

Every setting is in `config\*.properties` and can be overridden on the command line with
`--name=value`. Nothing (ports, hosts, timeouts, limits) is hard-coded.

| Worker setting | Default | Meaning |
|---|---|---|
| `bootstrap` | `localhost:50050` | Bootstrap Node address |
| `id`, `port`, `host` | auto | Identity and address of this worker |
| `heartbeatIntervalMs` | 1000 | Coordinator → worker heartbeat period |
| `suspectAfterMisses` / `deadAfterMisses` | 2 / 4 | Missed heartbeats before SUSPECT / DEAD |
| `coordinatorTimeoutMs` | 4000 | Coordinator silence before workers check it and elect a new one |
| `initialNeighbours` / `minNeighbours` | 2 / 2 | Overlay links per worker |
| `startupGraceMs`, `minWorkersForFirstElection` | 4000, 1 | When the first election starts |
| `maxJobsPerTerm` | 5 | Jobs per coordinator term |
| `jacCounting` | `job` | `job` = +1 per job given to other workers, `subtask` = +1 per subtask |
| `subtasksPerWorker` | 4 | A job is split into workers × this equal-sized subtasks |
| `maxInFlightPerWorker` | 2 | Subtasks of one job a worker runs at once; it gets the next one when it finishes |
| `stallTimeoutMs` | 30000 | No progress for this long → backup copy on another worker |
| `replicationSyncAcks` | 1 | Replicas that must store a new job before it is accepted |
| `computeThreads` | CPU cores | Threads used for computation |

Client: `bootstrap`, plus optional `contacts` (worker addresses to use if the Bootstrap
Node is down, e.g. `--contacts=192.168.1.51:50051,192.168.1.52:50052`).

## 7. Using the client

**GUI:**
1. Choose **MAX**, **PRIMESUM** or **PRIMECOUNT**.
2. Enter data: type numbers, **Load CSV...**, or **Generate...** a large random dataset.
   After loading, the panel shows how many values were loaded and the smallest/largest
   value. For PRIMESUM, enter a range or use a preset (1–1M, 1–100M, 1–1B).
3. Set **Copies** to submit several jobs at once, then click **Submit job**.
4. Each job gets a card with a **live progress bar** based on completed work on the
   cluster. **Click the card or progress bar** to see the per-worker breakdown (subtasks,
   progress and health of every worker) and the event timeline. **Cancel** stops a job.
5. The **Cluster** tab shows the coordinator, term, jobs in this term, and each worker's
   health, JAC, load and neighbours. The **Activity log** tab records every event.

**Command line** (useful for testing):
```cmd
java -cp target\distrilab.jar client.Client --cli --type=PRIMESUM --start=1 --end=1000000000 --copies=2
java -cp target\distrilab.jar client.Client --cli --type=MAX --file=data\numbers_100k.csv
java -cp target\distrilab.jar client.Client --cli --type=PRIMECOUNT --numbers=2,3,4,5,6,7
java -cp target\distrilab.jar client.Client --status
java -cp target\distrilab.jar client.Client --generate --count=1000000 --min=1 --max=1000000000000 --out=data\generated_1m.csv
```

## 8. Fault-tolerance scenarios (how to demonstrate them)

Start the cluster with `scripts\demo-local.bat` and submit a long job, e.g. PRIMESUM
1 – 10,000,000,000 (or several copies of 1 – 1,000,000,000).

| Scenario | How | What you will see |
|---|---|---|
| **Worker crash** | Close a worker window, or `scripts\kill-worker.bat 3` | The coordinator logs `FAILURE DETECTED ... -> DEAD`; the worker's subtasks are reassigned; the progress bar dips slightly and continues; the result is still correct. |
| **Coordinator crash** | Close the window of the worker shown as COORDINATOR | Workers detect the silence, double-check, and elect a new coordinator (term + 1). It pulls the replicated job table and **adopts** the job, reusing completed subtasks. The client card shows RECOVERING, then continues to the correct result. |
| **Slow / frozen worker** | Resource Monitor → CPU tab → right-click the worker's `java.exe` → *Suspend Process* (later *Resume Process*) | After 2 missed heartbeats it is SUSPECT (keeps its work, gets no new work); resumed quickly → ALIVE again. Suspended longer than 4 heartbeats → DEAD and its work moves; its late result is ignored (no double counting). |
| **New worker during a job** | `scripts\start-worker.bat --bootstrap=localhost:50050` | It registers (next unused id), links to 2 random workers and starts receiving subtasks of the running job within seconds. |
| **5-job term limit** | Submit 6 jobs (Copies = 6) | After the 5th job the coordinator steps down and a new election runs; the 6th job is accepted by the new coordinator. Running jobs finish normally. |
| **Two clients, huge jobs** | Two client windows, each PRIMESUM 1 – 1,000,000,000 | Both finish in seconds with 24,739,512,092,254,535. |
| **Bootstrap Node down** | Close the Bootstrap window | Running workers keep computing, electing and recovering (each worker keeps its own peer directory); running clients keep working. |
| **Bad input** | Letters, empty fields, start > end, values beyond 64 bits, ranges beyond 10^15 | A clear message; nothing crashes. Very large ranges ask for confirmation and can be cancelled. |

## 9. Tests

**Unit tests:** `mvn test` (also run by `scripts\build.bat`). They cover primality
(including Carmichael numbers and 64-bit primes), job splitting and combining,
overflow-safe sums, the Bootstrap registry (unique ids, leases), the election rule and
term handling, replicated-state merging, configuration parsing and CSV loading.

**Automated fault-tolerance tests:** `scripts\fault-tests.bat` (about 5 minutes) starts
a cluster in the background and checks, against known answers: PRIMESUM 1..10^9, two
concurrent clients, MAX/PRIMECOUNT on the CSV files, a worker crash mid-job, a
coordinator crash mid-job, a new worker joining mid-job, and the 5-job term limit.
Results go to `fault-tests-results.txt`, process logs to `logs\fault-tests\`.

## 10. Troubleshooting

| Problem | Fix |
|---|---|
| `target\distrilab.jar not found` | Run `scripts\build.bat` first. |
| `Address already in use` | Another process uses the port: run `scripts\stop-all.bat`, or choose another `--port`. |
| Worker says `Bootstrap Node ... not reachable` | Start the Bootstrap Node first; check its IP/port and the firewall. |
| `Worker id N is already used` | A live worker has that id (or its lease has not expired yet, max 6 s): omit `--id` or wait. |
| Workers on other PCs cannot reach each other | Check `--host` is the LAN IP (not `localhost`) and the firewall allows the ports. |
| First build fails to download | Maven needs internet access for the first build. |
