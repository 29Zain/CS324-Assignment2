package worker;

import common.*;
import common.Config.ConfigException;
import common.Net.*;
import io.grpc.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import proto.*;
import worker.Coordinator.*;
import worker.Election.*;
import worker.FailureDetector.*;
import worker.Overlay.*;
import worker.Replication.*;

/**
 * Worker Node program: start-up, settings, registration with the Bootstrap Node and the
 * gRPC server that hosts all worker services.
 */
public final class Worker {

    private Worker() {
    }

    /** Thread factory for named daemon threads ("name-1", "name-2", ...). */
    static ThreadFactory daemonThreads(String name) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, name + "-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    /** Entry point: java -cp target\distrilab.jar worker.Worker [options] */
    public static void main(String[] args) {
        WorkerMain.main(args);
    }

    /**
     * Starts one worker process. Each worker is started separately, on any computer:
     *
     * <pre>
     *   java -cp target\distrilab.jar worker.Worker --bootstrap=192.168.1.50:50050
     *   java -cp target\distrilab.jar worker.Worker --bootstrap=localhost:50050 --id=3 --port=50053
     * </pre>
     * Without --id the Bootstrap Node assigns the next unused id; without --port a free
     * port is chosen. All other settings come from config/worker.properties.
     */
    public static final class WorkerMain {

        private WorkerMain() {
        }

        public static void main(String[] args) {
            // An unexpected error in a background thread is logged instead of silently killing that thread.
            Thread.setDefaultUncaughtExceptionHandler((thread, error) ->
                    Log.get("WORKER").error("Unexpected error in thread " + thread.getName(), error));
            WorkerSettings settings;
            try {
                settings = WorkerSettings.from(Config.load(WorkerSettings.DEFAULTS, "worker.properties", args));
            } catch (ConfigException e) {
                System.err.println("Configuration error: " + e.getMessage());
                System.err.println("Usage: java -cp target/distrilab.jar worker.Worker [--bootstrap=host:port] [--id=N] [--port=P] "
                        + "[--host=advertised-address] [--config=file]");
                System.exit(2);
                return;
            }
            Log.init("W?", settings.debug());
            Log log = Log.get("WORKER");
            WorkerNode node = new WorkerNode(settings);
            try {
                node.start();
                node.awaitTermination();
            } catch (IllegalStateException e) {
                log.warn("Cannot start: " + e.getMessage());
                node.stop();
                System.exit(1);
            } catch (IOException e) {
                log.warn("Cannot start the worker server (port in use?)", e);
                node.stop();
                System.exit(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Composition root of a worker process: creates the subsystems, wires them together
     * and runs the start-up sequence. Every subsystem lives in its own class:
     *
     * <pre>
     *   OverlayManager      neighbours, links, repair             (overlay)
     *   ElectionManager     wave election + COORDINATOR flood     (election)
     *   CoordinatorMonitor  detects a failed coordinator          (membership)
     *   MembershipTracker   heartbeats -> ALIVE/SUSPECT/DEAD      (membership)
     *   TaskServiceImpl     executes subtasks, streams progress   (task)
     *   ReplicationManager  replicates / recovers job state       (replication)
     *   JobManager          coordinator: admission, scheduling    (jobs)
     * </pre>
     */
    public static final class WorkerNode {

        private static final Log LOG = Log.get("WORKER");
        private static final long REGISTER_RETRY_MS = 2000;

        private final WorkerSettings settings;
        private final ChannelPool pool = new ChannelPool();
        private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4, daemonThreads("scheduler"));
        private final ExecutorService executor = Executors.newCachedThreadPool(daemonThreads("worker-io"));
        private final ClusterState state = new ClusterState();
        private final BootstrapClient bootstrap;
        private final OverlayManager overlay;
        private final ElectionManager election;
        private final MembershipTracker tracker;
        private final ReplicaStore replicas = new ReplicaStore();
        private final ReplicationManager replication;
        private final TaskServiceImpl tasks;
        private final JobManager jobs;
        private final CoordinatorMonitor monitor;

        private Server server;
        private boolean coordinatorRoleActive;

        public WorkerNode(WorkerSettings settings) {
            this.settings = settings;
            this.bootstrap = new BootstrapClient(settings.bootstrap(), pool, settings.rpcTimeoutMs());
            this.overlay = new OverlayManager(state, pool, bootstrap, scheduler, settings.minNeighbours(),
                    settings.neighbourPingMs(), settings.rpcTimeoutMs());
            this.election = new ElectionManager(state, overlay, pool, executor, settings.electionTimeoutMs(),
                    settings.electionJitterMs(), settings.rpcTimeoutMs());
            this.tracker = new MembershipTracker(state, pool, bootstrap, scheduler, settings.heartbeatIntervalMs(),
                    settings.heartbeatTimeoutMs(), settings.suspectAfterMisses(), settings.deadAfterMisses());
            this.replication = new ReplicationManager(state, pool, replicas, this::replicaTargets,
                    settings.rpcTimeoutMs(), settings.replicationSyncAcks());
            this.tasks = new TaskServiceImpl(settings.computeThreads(), scheduler);
            this.jobs = new JobManager(state, tracker, replicas, replication, pool, settings, scheduler, executor,
                    election::isAlive, this::selfSummary, () -> election.initiate(ElectionReason.TERM_LIMIT));
            this.monitor = new CoordinatorMonitor(state, election, bootstrap, pool, scheduler,
                    settings.coordinatorTimeoutMs(), settings.startupGraceMs(), settings.minWorkersForFirstElection());
            tracker.addListener(jobs);
            state.addListener((previous, current) -> onCoordinatorChanged(current));
        }

        /** Start-up sequence: server, registration, overlay join, coordinator discovery. */
        public void start() throws IOException, InterruptedException {
            int port = settings.port() > 0 ? settings.port() : NetUtil.findFreePort();
            server = ServerBuilder.forPort(port)
                    .maxInboundMessageSize(ChannelPool.MAX_MESSAGE_BYTES)
                    .addService(new PeerServiceImpl(state, overlay, tasks::activeSubtasks, tasks::completedSubtasks,
                            jobs::jobsInTerm, settings.maxJobsPerTerm()))
                    .addService(new ElectionServiceImpl(election))
                    .addService(tasks)
                    .addService(new ReplicationServiceImpl(replicas, state))
                    .addService(new JobServiceImpl(jobs, state))
                    .build()
                    .start();

            String host = advertisedHost();
            RegisterResponse registration = registerWithRetry(host, port);
            NodeInfo self = registration.getAssigned();
            state.setSelf(self);
            Log.setProcessTag("W" + self.getId());
            LOG.info("Worker-" + self.getId() + " started at " + host + ":" + port + " (Bootstrap Node "
                    + settings.bootstrap() + ", " + settings.computeThreads() + " compute threads)");

            long renewEvery = Math.min(settings.leaseRenewMs(), Math.max(500, registration.getLeaseTimeoutMs() / 3));
            scheduler.scheduleWithFixedDelay(this::renewLease, renewEvery, renewEvery, TimeUnit.MILLISECONDS);

            List<NeighbourReply> replies = overlay.join(registration.getPeersList());
            overlay.start();
            discoverCoordinator(registration.getCoordinator(), replies);
            jobs.start();
            monitor.start();

            Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "shutdown"));
        }

        private String advertisedHost() {
            if (!settings.advertisedHost().isBlank()) {
                return settings.advertisedHost();
            }
            return settings.bootstrap().isLoopback() ? "localhost" : NetUtil.detectLanAddress(settings.bootstrap().host());
        }

        private RegisterResponse registerWithRetry(String host, int port) throws InterruptedException {
            boolean warned = false;
            while (true) {
                try {
                    return bootstrap.register(settings.requestedId(), host, port);
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
                        throw new IllegalStateException(e.getStatus().getDescription());
                    }
                    if (!warned) {
                        LOG.warn("Bootstrap Node " + settings.bootstrap() + " not reachable (" + e.getStatus().getCode()
                                + "); retrying every " + REGISTER_RETRY_MS / 1000 + " s ...");
                        warned = true;
                    }
                    Thread.sleep(REGISTER_RETRY_MS);
                }
            }
        }

        /** Lease renewal; re-registers with the same id if the Bootstrap Node forgot us. */
        private void renewLease() {
            try {
                bootstrap.renewLease(state.selfId()).filter(known -> !known).ifPresent(unknown -> {
                    try {
                        bootstrap.register(state.selfId(), state.self().getHost(), state.self().getPort());
                        LOG.info("Re-registered with the Bootstrap Node as Worker-" + state.selfId());
                        if (state.isCoordinator()) {
                            bootstrap.reportCoordinator(state.coordinator());
                        }
                    } catch (StatusRuntimeException e) {
                        LOG.warn("Re-registration failed: " + e.getStatus().getDescription());
                    }
                });
            } catch (RuntimeException e) {
                LOG.warn("Lease renewal failed", e);
            }
        }

        /**
         * A new worker learns the current coordinator from the Bootstrap Node and from its new
         * neighbours, and verifies it directly before trusting it. If none is found, the
         * CoordinatorMonitor starts an election after the start-up grace period.
         */
        private void discoverCoordinator(CoordinatorInfo fromBootstrap, List<NeighbourReply> neighbourReplies) {
            // Highest known term wins; on a tie the first candidate (Bootstrap Node first) is kept.
            CoordinatorInfo best = Stream.concat(Stream.of(fromBootstrap),
                            neighbourReplies.stream().map(NeighbourReply::getCoordinator))
                    .filter(CoordinatorInfo::getKnown)
                    .max(Comparator.comparingInt(CoordinatorInfo::getTerm))
                    .orElse(null);
            if (best == null) {
                LOG.info("No coordinator known yet - an election will start after the start-up grace period ("
                        + settings.startupGraceMs() + " ms)");
                return;
            }
            try {
                CoordinatorInfo confirmed = heartbeat(best.getNode()).getCoordinator();
                if (confirmed.getKnown() && confirmed.getNode().getId() == best.getNode().getId()) {
                    state.adopt(confirmed);
                    LOG.info("Joined the cluster under coordinator " + Protos.label(confirmed.getNode())
                            + " (term " + confirmed.getTerm() + ")");
                } else {
                    LOG.info(Protos.shortLabel(best.getNode()) + " is no longer coordinator; waiting for the current one");
                }
            } catch (StatusRuntimeException e) {
                LOG.info("Last known coordinator " + Protos.shortLabel(best.getNode()) + " is unreachable");
            }
        }

        /** Starts/stops the coordinator role whenever the elected coordinator changes. */
        private void onCoordinatorChanged(CoordinatorInfo current) {
            if (current.getKnown()) {
                LOG.info("************************************************************");
                LOG.info("COORDINATOR AGREED: term " + current.getTerm() + " -> " + Protos.label(current.getNode())
                        + " (JAC at election " + current.getJac() + ")");
                LOG.info("************************************************************");
            }
            executor.execute(this::syncCoordinatorRole);
            if (current.getKnown() && current.getNode().getId() != state.selfId()) {
                executor.execute(() -> introduceTo(current.getNode()));
            }
        }

        /**
         * Introduces this worker to a newly elected coordinator, so the coordinator knows
         * every worker even if it was elected while the Bootstrap Node was down.
         */
        private void introduceTo(NodeInfo coordinator) {
            try {
                heartbeat(coordinator);
            } catch (StatusRuntimeException e) {
                // the coordinator monitor will notice if it is really gone
            }
        }

        /** Sends a plain heartbeat (identifying this worker) to {@code node}. */
        private HeartbeatReply heartbeat(NodeInfo node) {
            return PeerServiceGrpc.newBlockingStub(pool.get(node))
                    .withDeadlineAfter(settings.rpcTimeoutMs(), TimeUnit.MILLISECONDS)
                    .heartbeat(HeartbeatRequest.newBuilder().setFrom(state.self()).build());
        }

        private synchronized void syncCoordinatorRole() {
            boolean shouldLead = state.isCoordinator();
            if (shouldLead && !coordinatorRoleActive) {
                coordinatorRoleActive = true;
                LOG.info(">>> This worker is now the COORDINATOR for term " + state.term() + " <<<");
                bootstrap.reportCoordinator(state.coordinator());
                jobs.updateTracker();
                jobs.onBecameCoordinator();
            } else if (!shouldLead && coordinatorRoleActive) {
                coordinatorRoleActive = false;
                LOG.info("No longer coordinator (now term " + state.term() + "); jobs already running here will finish");
                jobs.updateTracker();
            } else if (shouldLead) {
                bootstrap.reportCoordinator(state.coordinator());
            }
        }

        /** Workers that receive replicated state: those the failure detector considers alive, else all known. */
        private List<NodeInfo> replicaTargets() {
            if (tracker.isActive()) {
                return tracker.aliveWorkers();
            }
            return bootstrap.listWorkers().orElse(overlay.neighbours());
        }

        private WorkerSummary selfSummary() {
            return WorkerSummary.newBuilder()
                    .setNode(state.self())
                    .setHealth(WorkerHealth.ALIVE)
                    .setJac(state.jac())
                    .setActiveSubtasks(tasks.activeSubtasks())
                    .addAllNeighbourIds(overlay.neighbourIds())
                    .setIsCoordinator(state.isCoordinator())
                    .build();
        }

        /** Graceful stop (Ctrl+C / window closed): leave the cluster cleanly. */
        public void stop() {
            LOG.info("Shutting down Worker-" + state.selfId());
            if (state.hasSelf()) {
                bootstrap.deregister(state.selfId());
            }
            if (server != null) {
                server.shutdownNow();
            }
            tasks.shutdown();
            scheduler.shutdownNow();
            executor.shutdownNow();
            pool.close();
        }

        public void awaitTermination() throws InterruptedException {
            server.awaitTermination();
        }
    }

    /**
     * Every tunable value of a worker, read from config/worker.properties and the
     * command line (nothing is hard-coded in the logic classes).
     */
    public record WorkerSettings(
            int requestedId,
            int port,
            String advertisedHost,
            HostPort bootstrap,
            int computeThreads,
            long rpcTimeoutMs,
            long leaseRenewMs,
            long heartbeatIntervalMs,
            int suspectAfterMisses,
            int deadAfterMisses,
            long coordinatorTimeoutMs,
            int initialNeighbours,
            int minNeighbours,
            long neighbourPingMs,
            long startupGraceMs,
            int minWorkersForFirstElection,
            long electionTimeoutMs,
            long electionJitterMs,
            int maxJobsPerTerm,
            int subtasksPerWorker,
            int maxInFlightPerWorker,
            long stallTimeoutMs,
            int maxAttemptsPerSubtask,
            int replicationSyncAcks,
            String jacCounting,
            boolean debug) {

        /** Built-in defaults; config files and command-line arguments override them. */
        public static final Map<String, String> DEFAULTS = Map.ofEntries(
                Map.entry("id", "0"),
                Map.entry("port", "0"),
                Map.entry("host", ""),
                Map.entry("bootstrap", "localhost:50050"),
                Map.entry("computeThreads", "0"),
                Map.entry("rpcTimeoutMs", "2000"),
                Map.entry("leaseRenewMs", "2000"),
                Map.entry("heartbeatIntervalMs", "1000"),
                Map.entry("suspectAfterMisses", "2"),
                Map.entry("deadAfterMisses", "4"),
                Map.entry("coordinatorTimeoutMs", "4000"),
                Map.entry("initialNeighbours", "2"),
                Map.entry("minNeighbours", "2"),
                Map.entry("neighbourPingMs", "2000"),
                Map.entry("startupGraceMs", "4000"),
                Map.entry("minWorkersForFirstElection", "1"),
                Map.entry("electionTimeoutMs", "8000"),
                Map.entry("electionJitterMs", "800"),
                Map.entry("maxJobsPerTerm", "5"),
                Map.entry("subtasksPerWorker", "4"),
                Map.entry("maxInFlightPerWorker", "2"),
                Map.entry("stallTimeoutMs", "30000"),
                Map.entry("maxAttemptsPerSubtask", "8"),
                Map.entry("replicationSyncAcks", "1"),
                Map.entry("jacCounting", "job"),
                Map.entry("debug", "false"));

        public static WorkerSettings from(Config c) {
            HostPort bootstrap = HostPort.parse(c.getString("bootstrap"));
            int threads = c.getIntInRange("computeThreads", 0, 256);
            if (threads == 0) {
                threads = Math.max(1, Runtime.getRuntime().availableProcessors());
            }
            String jac = c.getString("jacCounting").toLowerCase();
            if (!jac.equals("job") && !jac.equals("subtask")) {
                throw new ConfigException("jacCounting must be 'job' or 'subtask' but was '" + jac + "'");
            }
            WorkerSettings s = new WorkerSettings(
                    c.getIntInRange("id", 0, 1_000_000),
                    c.getIntInRange("port", 0, 65535),
                    c.getString("host", ""),
                    bootstrap,
                    threads,
                    c.getPositiveLong("rpcTimeoutMs"),
                    c.getPositiveLong("leaseRenewMs"),
                    c.getPositiveLong("heartbeatIntervalMs"),
                    c.getIntInRange("suspectAfterMisses", 1, 100),
                    c.getIntInRange("deadAfterMisses", 1, 100),
                    c.getPositiveLong("coordinatorTimeoutMs"),
                    c.getIntInRange("initialNeighbours", 1, 20),
                    c.getIntInRange("minNeighbours", 1, 20),
                    c.getPositiveLong("neighbourPingMs"),
                    c.getPositiveLong("startupGraceMs"),
                    c.getIntInRange("minWorkersForFirstElection", 1, 1000),
                    c.getPositiveLong("electionTimeoutMs"),
                    c.getLong("electionJitterMs"),
                    c.getIntInRange("maxJobsPerTerm", 1, 1000),
                    c.getIntInRange("subtasksPerWorker", 1, 64),
                    c.getIntInRange("maxInFlightPerWorker", 1, 64),
                    c.getPositiveLong("stallTimeoutMs"),
                    c.getIntInRange("maxAttemptsPerSubtask", 1, 100),
                    c.getIntInRange("replicationSyncAcks", 0, 100),
                    jac,
                    c.getBoolean("debug", false));
            if (s.deadAfterMisses < s.suspectAfterMisses) {
                throw new ConfigException("deadAfterMisses must be >= suspectAfterMisses");
            }
            if (s.coordinatorTimeoutMs <= s.heartbeatIntervalMs) {
                throw new ConfigException("coordinatorTimeoutMs must be larger than heartbeatIntervalMs");
            }
            return s;
        }

        /** How long a heartbeat may take before it counts as missed. */
        public long heartbeatTimeoutMs() {
            return Math.max(300, heartbeatIntervalMs * 3 / 2);
        }
    }

    /**
     * Worker-side access to the Bootstrap Node. Every call has a deadline, and a
     * Bootstrap outage is tolerated: methods return empty results and the worker keeps
     * running with what it already knows.
     */
    public static final class BootstrapClient {

        private static final Log LOG = Log.get("BOOTSTRAP-CLIENT");

        private final HostPort address;
        private final ChannelPool pool;
        private final long timeoutMs;
        private volatile boolean reachable = true;

        public BootstrapClient(HostPort address, ChannelPool pool, long timeoutMs) {
            this.address = address;
            this.pool = pool;
            this.timeoutMs = timeoutMs;
        }

        public HostPort address() {
            return address;
        }

        private BootstrapServiceGrpc.BootstrapServiceBlockingStub stub() {
            return BootstrapServiceGrpc.newBlockingStub(pool.get(address.host(), address.port()))
                    .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
        }

        /** Registration errors are thrown so the caller can retry or stop (duplicate id). */
        public RegisterResponse register(int requestedId, String host, int port) {
            RegisterResponse response = stub().register(RegisterRequest.newBuilder()
                    .setRequestedId(requestedId).setHost(host).setPort(port).build());
            markReachable();
            return response;
        }

        public Optional<Boolean> renewLease(int id) {
            return call("renew lease", () -> stub().heartbeat(NodeRef.newBuilder().setId(id).build()))
                    .map(LeaseReply::getKnown);
        }

        public Optional<List<NodeInfo>> listWorkers() {
            return call("list workers", () -> stub().listWorkers(Empty.getDefaultInstance()).getPeersList());
        }

        public List<NodeInfo> randomPeers(int requesterId, int count, Collection<Integer> exclude) {
            return call("get peers", () -> stub().getPeers(PeerRequest.newBuilder()
                    .setRequesterId(requesterId).setCount(count).addAllExcludeIds(exclude).build()).getPeersList())
                    .orElse(List.of());
        }

        public void reportFailure(int id) {
            call("report failure", () -> stub().reportFailure(NodeRef.newBuilder().setId(id).build()));
        }

        public void reportCoordinator(CoordinatorInfo info) {
            call("report coordinator", () -> stub().reportCoordinator(info));
        }

        public Optional<CoordinatorInfo> coordinator() {
            return call("get coordinator", () -> stub().getCoordinator(Empty.getDefaultInstance()));
        }

        public void deregister(int id) {
            call("deregister", () -> stub().deregister(NodeRef.newBuilder().setId(id).build()));
        }

        private <T> Optional<T> call(String what, Supplier<T> action) {
            try {
                T value = action.get();
                markReachable();
                return Optional.of(value);
            } catch (StatusRuntimeException e) {
                if (reachable) {
                    reachable = false;
                    LOG.warn("Bootstrap Node at " + address + " unreachable (" + what + ": "
                            + e.getStatus().getCode() + "). Continuing with the cluster state already known.");
                }
                return Optional.empty();
            }
        }

        private void markReachable() {
            if (!reachable) {
                reachable = true;
                LOG.info("Bootstrap Node at " + address + " is reachable again");
            }
        }
    }
}
