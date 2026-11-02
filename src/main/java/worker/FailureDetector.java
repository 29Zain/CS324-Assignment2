package worker;

import com.google.common.util.concurrent.*;
import common.Log;
import common.Net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import proto.*;
import worker.Election.*;
import worker.Worker.BootstrapClient;

/**
 * Failure detection: heartbeat-based health of workers (ALIVE / SUSPECT / DEAD) on the
 * coordinator, and coordinator-silence detection on every worker.
 */
public final class FailureDetector {

    private FailureDetector() {
    }

    /**
     * Failure-detector verdict for a worker.
     * <ul>
     *   <li>ALIVE   - answering heartbeats.</li>
     *   <li>SUSPECT - missed a few heartbeats; it may just be slow or overloaded, so its
     *                 running work is NOT taken away yet, but it gets no new work.</li>
     *   <li>DEAD    - missed many heartbeats in a row (or its connection is refused);
     *                 its unfinished work is reassigned.</li>
     * </ul>
     */
    public enum Health {
        ALIVE, SUSPECT, DEAD, UNKNOWN;

        public WorkerHealth toProto() {
            return switch (this) {
                case ALIVE -> WorkerHealth.ALIVE;
                case SUSPECT -> WorkerHealth.SUSPECT;
                case DEAD -> WorkerHealth.DEAD;
                case UNKNOWN -> WorkerHealth.HEALTH_UNSPECIFIED;
            };
        }
    }

    /**
     * Heartbeat-based failure detector run by the coordinator (and by any former
     * coordinator that still owns running jobs).
     *
     * <p>Every {@code heartbeatIntervalMs} it sends a Heartbeat RPC to every worker known to
     * the Bootstrap Node. Each heartbeat has a deadline. Consecutive misses move a worker
     * ALIVE -> SUSPECT -> DEAD. Heartbeats are answered by gRPC threads, not by the compute
     * threads, so a worker busy with a heavy subtask still answers: "busy" is not mistaken
     * for "dead". Only when a worker stops answering entirely is it declared DEAD and its
     * work reassigned.
     */
    public static final class MembershipTracker {

        private static final Log LOG = Log.get("FAILURE-DETECTOR");
        private static final long FORGET_DEAD_AFTER_MS = 60_000;

        /** Notified when a worker is declared dead or comes back. */
        public interface Listener {
            void onWorkerDead(NodeInfo worker);

            void onWorkerAlive(NodeInfo worker);
        }

        /** What the detector knows about one worker. */
        public static final class Member {
            private volatile NodeInfo node;
            private volatile Health health = Health.ALIVE;
            private volatile int misses;
            private volatile HeartbeatReply lastReply;
            private volatile long deadSinceMs;

            Member(NodeInfo node) {
                this.node = node;
            }

            public NodeInfo node() {
                return node;
            }

            public Health health() {
                return health;
            }

            public HeartbeatReply lastReply() {
                return lastReply;
            }
        }

        private final ClusterState state;
        private final ChannelPool pool;
        private final BootstrapClient bootstrap;
        private final ScheduledExecutorService scheduler;
        private final long intervalMs;
        private final long timeoutMs;
        private final int suspectAfter;
        private final int deadAfter;
        private final Map<Integer, Member> members = new ConcurrentHashMap<>();
        private final List<Listener> listeners = new CopyOnWriteArrayList<>();
        private ScheduledFuture<?> task;

        public MembershipTracker(ClusterState state, ChannelPool pool, BootstrapClient bootstrap,
                                 ScheduledExecutorService scheduler, long intervalMs, long timeoutMs,
                                 int suspectAfter, int deadAfter) {
            this.state = state;
            this.pool = pool;
            this.bootstrap = bootstrap;
            this.scheduler = scheduler;
            this.intervalMs = intervalMs;
            this.timeoutMs = timeoutMs;
            this.suspectAfter = suspectAfter;
            this.deadAfter = deadAfter;
        }

        public void addListener(Listener listener) {
            listeners.add(listener);
        }

        public synchronized boolean isActive() {
            return task != null;
        }

        public synchronized void activate() {
            if (task == null) {
                refreshMembership();
                task = scheduler.scheduleWithFixedDelay(this::tick, 0, intervalMs, TimeUnit.MILLISECONDS);
                LOG.info("Heartbeat monitoring started (every " + intervalMs + " ms, SUSPECT after " + suspectAfter
                        + " misses, DEAD after " + deadAfter + ")");
            }
        }

        public synchronized void deactivate() {
            if (task != null) {
                task.cancel(false);
                task = null;
                LOG.info("Heartbeat monitoring stopped");
            }
        }

        private void tick() {
            try {
                refreshMembership();
                for (Member member : members.values()) {
                    if (member.node.getId() != state.selfId()) {
                        ping(member);
                    }
                }
                forgetLongDead();
            } catch (RuntimeException e) {
                LOG.warn("Heartbeat round failed", e);
            }
        }

        /** Adds workers registered at the Bootstrap Node that we do not track yet. */
        private void refreshMembership() {
            if (state.hasSelf()) {
                members.computeIfAbsent(state.selfId(), id -> new Member(state.self()));
            }
            bootstrap.listWorkers().ifPresent(list -> list.forEach(node -> {
                state.rememberPeer(node);
                track(node);
            }));
            // Works even while the Bootstrap Node is down: everyone we have heard of is monitored.
            state.knownPeers().forEach(this::track);
        }

        /** Starts tracking a worker (or updates its address if it re-registered elsewhere). */
        public void track(NodeInfo node) {
            if (node.getId() <= 0) {
                return;
            }
            Member member = members.computeIfAbsent(node.getId(), id -> new Member(node));
            if (!member.node.equals(node)) {
                member.node = node;
            }
        }

        private void ping(Member member) {
            NodeInfo node = member.node;
            HeartbeatRequest request = HeartbeatRequest.newBuilder()
                    .setFrom(state.self())
                    .setFromCoordinator(state.isCoordinator())
                    .setCoordinator(state.coordinator())
                    .build();
            Futures.addCallback(
                    PeerServiceGrpc.newFutureStub(pool.get(node))
                            .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                            .heartbeat(request),
                    new FutureCallback<HeartbeatReply>() {
                        @Override
                        public void onSuccess(HeartbeatReply reply) {
                            onHeartbeatOk(member, reply);
                        }

                        @Override
                        public void onFailure(Throwable error) {
                            onHeartbeatMissed(member);
                        }
                    },
                    MoreExecutors.directExecutor());
        }

        private void onHeartbeatOk(Member member, HeartbeatReply reply) {
            Health previous;
            synchronized (member) {
                previous = member.health;
                member.misses = 0;
                member.lastReply = reply;
                member.health = Health.ALIVE;
            }
            if (previous == Health.DEAD) {
                LOG.info(Protos.label(member.node) + " is answering again -> ALIVE (rejoined)");
                listeners.forEach(l -> l.onWorkerAlive(member.node));
            } else if (previous == Health.SUSPECT) {
                LOG.info(Protos.shortLabel(member.node) + " answered again -> ALIVE (it was only slow)");
            }
            // A worker that knows a newer coordinator tells us we are out of date.
            if (reply.getCoordinator().getKnown() && reply.getCoordinator().getTerm() > state.term()) {
                state.adopt(reply.getCoordinator());
            }
        }

        private void onHeartbeatMissed(Member member) {
            Health previous;
            Health next;
            int misses;
            synchronized (member) {
                previous = member.health;
                misses = ++member.misses;
                if (misses >= deadAfter) {
                    next = Health.DEAD;
                } else if (misses >= suspectAfter && previous == Health.ALIVE) {
                    next = Health.SUSPECT;
                } else {
                    next = previous;
                }
                member.health = next;
                if (next == Health.DEAD && previous != Health.DEAD) {
                    member.deadSinceMs = System.currentTimeMillis();
                }
            }
            if (next == Health.SUSPECT && previous == Health.ALIVE) {
                LOG.warn(Protos.shortLabel(member.node) + " missed " + misses
                        + " heartbeats -> SUSPECT (may be slow; keeps its work, gets no new work)");
            } else if (next == Health.DEAD && previous != Health.DEAD) {
                LOG.warn("FAILURE DETECTED: " + Protos.label(member.node) + " missed " + misses
                        + " heartbeats in a row -> DEAD");
                pool.invalidate(member.node);
                state.forgetPeer(member.node.getId());
                bootstrap.reportFailure(member.node.getId());
                listeners.forEach(l -> l.onWorkerDead(member.node));
            }
        }

        /** Called when an RPC to the worker just failed (e.g. its task stream broke). */
        public void suspect(NodeInfo node, String reason) {
            Member member = members.get(node.getId());
            if (member == null || node.getId() == state.selfId()) {
                return;
            }
            boolean changed = false;
            synchronized (member) {
                if (member.health == Health.ALIVE) {
                    member.health = Health.SUSPECT;
                    member.misses = Math.max(member.misses, suspectAfter);
                    changed = true;
                }
            }
            if (changed) {
                LOG.warn(Protos.shortLabel(node) + " -> SUSPECT (" + reason + ")");
            }
        }

        private void forgetLongDead() {
            long now = System.currentTimeMillis();
            members.values().removeIf(m -> m.health == Health.DEAD && now - m.deadSinceMs > FORGET_DEAD_AFTER_MS);
        }

        /** Workers that may receive new work, ordered by id. */
        public List<NodeInfo> aliveWorkers() {
            List<NodeInfo> alive = members.values().stream()
                    .filter(member -> member.health == Health.ALIVE)
                    .map(Member::node)
                    .collect(Collectors.toCollection(ArrayList::new));
            if (state.hasSelf() && alive.stream().noneMatch(n -> n.getId() == state.selfId())) {
                alive.add(state.self());
            }
            alive.sort(Comparator.comparingInt(NodeInfo::getId));
            return alive;
        }

        public Health health(int id) {
            if (id == state.selfId()) {
                return Health.ALIVE;
            }
            Member member = members.get(id);
            return member == null ? Health.UNKNOWN : member.health;
        }

        public List<Member> members() {
            List<Member> list = new ArrayList<>(members.values());
            list.sort(Comparator.comparingInt(m -> m.node.getId()));
            return list;
        }
    }

    /**
     * Runs on every worker that is not the coordinator. The coordinator sends each worker a
     * heartbeat every second; if none arrives for {@code coordinatorTimeoutMs}, this monitor
     * double-checks with a direct ping (so a single delayed packet is not treated as a crash)
     * and, if that also fails, declares the coordinator failed and starts an election.
     *
     * <p>It also starts the very first election once the cluster has had time to form
     * (startup grace period and optional minimum number of workers).
     */
    public static final class CoordinatorMonitor {

        private static final Log LOG = Log.get("COORD-MONITOR");

        private final ClusterState state;
        private final ElectionManager election;
        private final BootstrapClient bootstrap;
        private final ChannelPool pool;
        private final ScheduledExecutorService scheduler;
        private final long coordinatorTimeoutMs;
        private final long startupGraceMs;
        private final int minWorkersForFirstElection;
        private final long startedAtMs = System.currentTimeMillis();
        private volatile boolean announcedWaiting;

        public CoordinatorMonitor(ClusterState state, ElectionManager election, BootstrapClient bootstrap,
                                  ChannelPool pool, ScheduledExecutorService scheduler, long coordinatorTimeoutMs,
                                  long startupGraceMs, int minWorkersForFirstElection) {
            this.state = state;
            this.election = election;
            this.bootstrap = bootstrap;
            this.pool = pool;
            this.scheduler = scheduler;
            this.coordinatorTimeoutMs = coordinatorTimeoutMs;
            this.startupGraceMs = startupGraceMs;
            this.minWorkersForFirstElection = minWorkersForFirstElection;
        }

        public void start() {
            scheduler.scheduleWithFixedDelay(this::check, 1000, 1000, TimeUnit.MILLISECONDS);
        }

        private void check() {
            try {
                if (!state.hasSelf() || state.isCoordinator() || election.isRunning()) {
                    return;
                }
                CoordinatorInfo coordinator = state.coordinator();
                if (!coordinator.getKnown()) {
                    if (state.term() > 0 || startupGateOpen()) {
                        election.initiate(state.term() == 0
                                ? ElectionReason.STARTUP
                                : ElectionReason.COORDINATOR_FAILED);
                    }
                    return;
                }
                if (state.millisSinceCoordinatorContact() < coordinatorTimeoutMs) {
                    return;
                }
                if (election.isAlive(coordinator.getNode())) {
                    state.markCoordinatorContact(); // alive, it just has not heartbeated us yet
                    return;
                }
                LOG.warn("FAILURE DETECTED: coordinator " + Protos.label(coordinator.getNode()) + " (term "
                        + coordinator.getTerm() + ") sent no heartbeat for " + state.millisSinceCoordinatorContact()
                        + " ms and a direct check failed -> starting election");
                pool.invalidate(coordinator.getNode());
                bootstrap.reportFailure(coordinator.getNode().getId());
                state.clearCoordinator(coordinator.getNode().getId());
                election.initiate(ElectionReason.COORDINATOR_FAILED);
            } catch (RuntimeException e) {
                LOG.warn("Coordinator check failed", e);
            }
        }

        /**
         * The first election waits for the startup grace period and (optionally) for a minimum
         * number of registered workers, so the first term is elected among the workers that
         * are starting together rather than by the first worker alone.
         */
        private boolean startupGateOpen() {
            long elapsed = System.currentTimeMillis() - startedAtMs;
            if (elapsed < startupGraceMs) {
                return false;
            }
            int registered = bootstrap.listWorkers().map(List::size).orElse(1);
            boolean enough = registered >= minWorkersForFirstElection;
            boolean waitedTooLong = elapsed >= startupGraceMs * 5;
            if (!enough && !waitedTooLong && !announcedWaiting) {
                announcedWaiting = true;
                LOG.info("Waiting for at least " + minWorkersForFirstElection + " workers before the first election ("
                        + registered + " registered)");
            }
            return enough || waitedTooLong;
        }
    }
}
