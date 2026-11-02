package bootstrap;

import common.*;
import common.Config.ConfigException;
import common.Net.Protos;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.LongSupplier;
import proto.*;

/**
 * Bootstrap Node program: the well-known entry point where workers register (unique ids,
 * leases) and where workers and clients find peers and the current coordinator.
 */
public final class BootstrapNode {

    private BootstrapNode() {
    }

    /** Entry point: java -cp target\distrilab.jar bootstrap.BootstrapNode [options] */
    public static void main(String[] args) {
        BootstrapMain.main(args);
    }

    /**
     * Starts the Bootstrap Node.
     *
     * <pre>java -cp target\distrilab.jar bootstrap.BootstrapNode [--port=50050] [--config=config/bootstrap.properties]</pre>
     *
     * The Bootstrap Node is only a directory: workers register here to get a unique id
     * and a few random peers, and clients ask it where the coordinator is. It never
     * computes and never votes, and running workers keep working if it goes down.
     */
    public static final class BootstrapMain {

        private static final Map<String, String> DEFAULTS = Map.of(
                "port", "50050",
                "leaseTimeoutMs", "6000",
                "peersOnJoin", "2",
                "debug", "false");

        private BootstrapMain() {
        }

        public static void main(String[] args) {
            // An unexpected error in a background thread is logged instead of silently killing that thread.
            Thread.setDefaultUncaughtExceptionHandler((thread, error) ->
                    Log.get("BOOTSTRAP").error("Unexpected error in thread " + thread.getName(), error));
            Config config;
            try {
                config = Config.load(DEFAULTS, "bootstrap.properties", args);
            } catch (ConfigException e) {
                System.err.println("Configuration error: " + e.getMessage());
                System.exit(2);
                return;
            }
            Log.init("BOOTSTRAP", config.getBoolean("debug", false));
            Log log = Log.get("BOOTSTRAP");

            try {
                int port = config.getIntInRange("port", 1, 65535);
                int leaseTimeoutMs = config.getIntInRange("leaseTimeoutMs", 1000, 600_000);
                int peersOnJoin = config.getIntInRange("peersOnJoin", 1, 10);

                WorkerRegistry registry = new WorkerRegistry(leaseTimeoutMs, System::currentTimeMillis);
                Server server = ServerBuilder.forPort(port)
                        .addService(new BootstrapServiceImpl(registry, peersOnJoin, leaseTimeoutMs))
                        .build()
                        .start();

                ScheduledExecutorService reaper = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "lease-reaper");
                    t.setDaemon(true);
                    return t;
                });
                reaper.scheduleWithFixedDelay(() -> registry.expireNow().forEach(member ->
                        log.info("Lease of Worker-" + member.id() + " expired -> removed (active workers: "
                                + registry.size() + ")")), 1, 1, TimeUnit.SECONDS);

                log.info("Bootstrap Node listening on port " + port + " (lease timeout " + leaseTimeoutMs
                        + " ms, " + peersOnJoin + " peers per joining worker)");

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    reaper.shutdownNow();
                    server.shutdownNow();
                }, "shutdown"));
                server.awaitTermination();
            } catch (ConfigException e) {
                log.warn("Configuration error: " + e.getMessage());
                System.exit(2);
            } catch (IOException e) {
                log.warn("Could not start the Bootstrap Node (is the port already in use?)", e);
                System.exit(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** gRPC front-end of the Bootstrap Node; all state lives in {@link WorkerRegistry}. */
    public static class BootstrapServiceImpl extends BootstrapServiceGrpc.BootstrapServiceImplBase {

        private static final Log LOG = Log.get("BOOTSTRAP");

        private final WorkerRegistry registry;
        private final int peersOnJoin;
        private final int leaseTimeoutMs;

        public BootstrapServiceImpl(WorkerRegistry registry, int peersOnJoin, int leaseTimeoutMs) {
            this.registry = registry;
            this.peersOnJoin = peersOnJoin;
            this.leaseTimeoutMs = leaseTimeoutMs;
        }

        @Override
        public void register(RegisterRequest request, StreamObserver<RegisterResponse> response) {
            if (request.getHost().isBlank() || request.getPort() <= 0) {
                response.onError(Status.INVALID_ARGUMENT.withDescription("host and port are required").asRuntimeException());
                return;
            }
            try {
                WorkerRegistry.Member member = registry.register(request.getRequestedId(), request.getHost(), request.getPort());
                List<WorkerRegistry.Member> peers = registry.randomPeers(peersOnJoin, List.of(member.id()));
                RegisterResponse.Builder reply = RegisterResponse.newBuilder()
                        .setAssigned(toNode(member))
                        .setCoordinator(coordinatorInfo())
                        .setLeaseTimeoutMs(leaseTimeoutMs);
                peers.forEach(peer -> reply.addPeers(toNode(peer)));
                LOG.info("Registered Worker-" + member.id() + " at " + member.host() + ":" + member.port()
                        + " -> peers " + peers.stream().map(WorkerRegistry.Member::id).toList()
                        + " (active workers: " + registry.size() + ")");
                response.onNext(reply.build());
                response.onCompleted();
            } catch (DuplicateWorkerException e) {
                LOG.warn("Rejected registration: " + e.getMessage());
                response.onError(Status.ALREADY_EXISTS.withDescription(e.getMessage()).asRuntimeException());
            }
        }

        @Override
        public void deregister(NodeRef request, StreamObserver<Ack> response) {
            removeAndAck(request, response, "left the cluster", "");
        }

        @Override
        public void heartbeat(NodeRef request, StreamObserver<LeaseReply> response) {
            reply(response, LeaseReply.newBuilder().setKnown(registry.renew(request.getId())).build());
        }

        @Override
        public void getPeers(PeerRequest request, StreamObserver<PeerList> response) {
            List<Integer> exclude = new ArrayList<>(request.getExcludeIdsList());
            exclude.add(request.getRequesterId());
            reply(response, peerList(registry.randomPeers(request.getCount(), exclude)));
        }

        @Override
        public void listWorkers(Empty request, StreamObserver<PeerList> response) {
            reply(response, peerList(registry.all()));
        }

        @Override
        public void reportFailure(NodeRef request, StreamObserver<Ack> response) {
            removeAndAck(request, response, "reported unreachable -> removed",
                    ". It will be re-added if it is actually alive.");
        }

        @Override
        public void reportCoordinator(CoordinatorInfo request, StreamObserver<Ack> response) {
            NodeInfo node = request.getNode();
            boolean accepted = registry.reportCoordinator(new WorkerRegistry.CoordinatorRecord(
                    node.getId(), node.getHost(), node.getPort(), request.getTerm(), request.getJac()));
            if (accepted) {
                LOG.info("Coordinator for term " + request.getTerm() + " is Worker-" + node.getId());
            }
            reply(response, ack(accepted));
        }

        @Override
        public void getCoordinator(Empty request, StreamObserver<CoordinatorInfo> response) {
            reply(response, coordinatorInfo());
        }

        private CoordinatorInfo coordinatorInfo() {
            return registry.liveCoordinator()
                    .map(c -> Protos.coordinator(Protos.node(c.id(), c.host(), c.port()), c.term(), c.jac()))
                    .orElse(Protos.unknownCoordinator());
        }

        /** Removes a worker, logs "Worker-N {event} (active workers: M){note}" and acks whether it was known. */
        private void removeAndAck(NodeRef request, StreamObserver<Ack> response, String event, String note) {
            Optional<WorkerRegistry.Member> removed = registry.remove(request.getId());
            removed.ifPresent(m -> LOG.info("Worker-" + m.id() + " " + event + " (active workers: " + registry.size()
                    + ")" + note));
            reply(response, ack(removed.isPresent()));
        }

        private static PeerList peerList(List<WorkerRegistry.Member> members) {
            PeerList.Builder list = PeerList.newBuilder();
            members.forEach(m -> list.addPeers(toNode(m)));
            return list.build();
        }

        private static Ack ack(boolean ok) {
            return Ack.newBuilder().setOk(ok).build();
        }

        private static NodeInfo toNode(WorkerRegistry.Member member) {
            return Protos.node(member.id(), member.host(), member.port());
        }

        private static <T> void reply(StreamObserver<T> response, T value) {
            response.onNext(value);
            response.onCompleted();
        }
    }

    /**
     * The Bootstrap Node's table of active workers (pure logic, no networking).
     *
     * <p>Membership is lease based: a worker stays registered only while it keeps
     * renewing its lease. This replaces Assignment 1's slow TCP probing of every
     * worker on every request.
     *
     * <p>Worker ids are unique: an id that belongs to a live worker is rejected, and
     * automatically assigned ids are never reused, so a failed worker's identity can
     * never be confused with a new worker.
     */
    public static class WorkerRegistry {

        /** A registered worker. */
        public record Member(int id, String host, int port, long lastSeenMs) {
            Member renewed(long now) {
                return new Member(id, host, port, now);
            }

            private boolean isAt(String otherHost, int otherPort) {
                return host.equals(otherHost) && port == otherPort;
            }
        }

        /** The coordinator most recently reported by a worker. */
        public record CoordinatorRecord(int id, String host, int port, int term, int jac) {
        }

        private final Map<Integer, Member> members = new ConcurrentHashMap<>();
        private final Set<Integer> usedIds = ConcurrentHashMap.newKeySet();
        private final long leaseTimeoutMs;
        private final LongSupplier clock;
        private volatile CoordinatorRecord coordinator;

        public WorkerRegistry(long leaseTimeoutMs, LongSupplier clock) {
            this.leaseTimeoutMs = leaseTimeoutMs;
            this.clock = clock;
        }

        /**
         * Registers a worker.
         *
         * @param requestedId 0 to auto-assign the next unused id
         * @return the registered member (with its final id)
         * @throws DuplicateWorkerException if the requested id belongs to a live worker
         */
        public synchronized Member register(int requestedId, String host, int port) throws DuplicateWorkerException {
            if (requestedId < 0) {
                throw new IllegalArgumentException("Worker id must be positive");
            }
            long now = clock.getAsLong();
            expire(now);
            int id = requestedId > 0 ? requestedId : nextFreeId();
            if (requestedId > 0) {
                Member existing = members.get(id);
                if (existing != null && !existing.isAt(host, port)) {
                    throw new DuplicateWorkerException(id, existing.host() + ":" + existing.port());
                }
            }
            // The address now belongs to this process; drop stale entries of a crashed process on it.
            members.values().removeIf(m -> m.id() != id && m.isAt(host, port));
            Member member = new Member(id, host, port, now);
            members.put(id, member);
            usedIds.add(id);
            return member;
        }

        private int nextFreeId() {
            return usedIds.stream().mapToInt(Integer::intValue).max().orElse(0) + 1;
        }

        /** Lease renewal; returns false if the worker is unknown and must register again. */
        public boolean renew(int id) {
            Member renewed = members.computeIfPresent(id, (key, member) -> member.renewed(clock.getAsLong()));
            return renewed != null;
        }

        public Optional<Member> remove(int id) {
            return Optional.ofNullable(members.remove(id));
        }

        /** Removes workers whose lease ran out; returns the removed members. */
        public synchronized List<Member> expire(long now) {
            List<Member> expired = members.values().stream()
                    .filter(member -> now - member.lastSeenMs() > leaseTimeoutMs)
                    .toList();
            expired.forEach(member -> members.remove(member.id()));
            return expired;
        }

        public List<Member> expireNow() {
            return expire(clock.getAsLong());
        }

        public boolean isActive(int id) {
            return members.containsKey(id);
        }

        public List<Member> all() {
            return members.values().stream().sorted(Comparator.comparingInt(Member::id)).toList();
        }

        /** Up to {@code count} random active workers, excluding the given ids. */
        public List<Member> randomPeers(int count, Collection<Integer> excludeIds) {
            Set<Integer> excluded = new HashSet<>(excludeIds);
            List<Member> candidates = new ArrayList<>(members.values().stream()
                    .filter(member -> !excluded.contains(member.id()))
                    .toList());
            Collections.shuffle(candidates);
            return candidates.subList(0, Math.min(Math.max(count, 0), candidates.size()));
        }

        /** Accepts the report if it is for the same or a newer term than the one stored. */
        public synchronized boolean reportCoordinator(CoordinatorRecord record) {
            CoordinatorRecord current = coordinator;
            if (current == null || record.term() >= current.term()) {
                coordinator = record;
                return true;
            }
            return false;
        }

        /** The last reported coordinator, only if it is still an active worker. */
        public Optional<CoordinatorRecord> liveCoordinator() {
            return Optional.ofNullable(coordinator).filter(current -> isActive(current.id()));
        }

        public int size() {
            return members.size();
        }
    }

    /** Raised when a worker tries to register with an id that a live worker already uses. */
    public static class DuplicateWorkerException extends Exception {

        public DuplicateWorkerException(int id, String usedBy) {
            super("Worker id " + id + " is already used by an active worker at " + usedBy
                    + ". Choose another id or omit --id to get one assigned automatically.");
        }
    }
}
