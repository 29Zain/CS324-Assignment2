package worker;

import com.google.common.util.concurrent.ListenableFuture;
import common.Log;
import common.Net.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import proto.*;
import worker.Overlay.OverlayManager;

/**
 * Coordinator election (wave/echo over the overlay): lowest JAC wins, ties go to the highest
 * id; also holds this worker's view of the cluster (term, coordinator, known peers).
 */
public final class Election {

    private Election() {
    }

    /**
     * This worker's view of the cluster: its own identity, the current term, who the
     * coordinator is, and its Job Allocation Counter (JAC).
     *
     * <p>All coordinator changes go through {@link #adopt}, which applies one rule
     * everywhere (announcements, heartbeats, gossip):
     * <ul>
     *   <li>a higher term always wins;</li>
     *   <li>a lower term is stale and ignored;</li>
     *   <li>two different coordinators for the same term (two elections ran at once) are
     *       resolved deterministically: lowest JAC, then highest id. Every worker applies
     *       the same rule, so all workers end up agreeing on one coordinator.</li>
     * </ul>
     */
    public static final class ClusterState {

        public enum AdoptResult { CHANGED, SAME, STALE }

        /** Notified (outside the lock) whenever the coordinator or term changes. */
        public interface Listener {
            void onCoordinatorChanged(CoordinatorInfo previous, CoordinatorInfo current);
        }

        private final AtomicInteger jac = new AtomicInteger();
        /** Every worker this node has heard of (from the Bootstrap Node, links, heartbeats, elections). */
        private final Map<Integer, NodeInfo> knownPeers = new ConcurrentHashMap<>();
        private final List<Listener> listeners = new CopyOnWriteArrayList<>();

        private volatile NodeInfo self;
        private int term;
        private CoordinatorInfo coordinator = Protos.unknownCoordinator();
        private volatile long lastCoordinatorContactMs = System.currentTimeMillis();

        public void setSelf(NodeInfo node) {
            this.self = node;
        }

        public NodeInfo self() {
            return self;
        }

        public boolean hasSelf() {
            return self != null;
        }

        public int selfId() {
            NodeInfo node = self;
            return node == null ? -1 : node.getId();
        }

        /**
         * Remembers a worker. The directory lets the cluster keep working without the
         * Bootstrap Node: a new coordinator can still reach every worker it has heard of.
         */
        public void rememberPeer(NodeInfo node) {
            if (node != null && node.getId() > 0 && node.getPort() > 0 && node.getId() != selfId()) {
                knownPeers.put(node.getId(), node);
            }
        }

        public void forgetPeer(int id) {
            knownPeers.remove(id);
        }

        public List<NodeInfo> knownPeers() {
            return new ArrayList<>(knownPeers.values());
        }

        public void addListener(Listener listener) {
            listeners.add(listener);
        }

        public synchronized int term() {
            return term;
        }

        public synchronized CoordinatorInfo coordinator() {
            return coordinator;
        }

        public synchronized boolean isCoordinator() {
            return coordinator.getKnown() && self != null && coordinator.getNode().getId() == self.getId();
        }

        public int jac() {
            return jac.get();
        }

        public int incrementJac() {
            return jac.incrementAndGet();
        }

        /** This worker as an election candidate. */
        public Candidate selfCandidate() {
            return Candidate.newBuilder().setNode(self).setJac(jac.get()).build();
        }

        /** True if candidate (jacA, idA) beats (jacB, idB): lowest JAC, then highest id. */
        public static boolean isBetter(int jacA, int idA, int jacB, int idB) {
            return jacA < jacB || (jacA == jacB && idA > idB);
        }

        public static Candidate better(Candidate a, Candidate b) {
            if (b == null || !b.hasNode()) {
                return a;
            }
            if (a == null || !a.hasNode()) {
                return b;
            }
            return isBetter(a.getJac(), a.getNode().getId(), b.getJac(), b.getNode().getId()) ? a : b;
        }

        /** Applies a coordinator announcement/observation (see class comment for the rule). */
        public AdoptResult adopt(CoordinatorInfo candidate) {
            if (candidate == null || !candidate.getKnown() || !candidate.hasNode()) {
                return AdoptResult.STALE;
            }
            CoordinatorInfo previous;
            AdoptResult result;
            synchronized (this) {
                previous = coordinator;
                if (candidate.getTerm() < term) {
                    result = AdoptResult.STALE;
                } else if (candidate.getTerm() == term && coordinator.getKnown()) {
                    if (coordinator.getNode().getId() == candidate.getNode().getId()) {
                        result = AdoptResult.SAME;
                    } else if (isBetter(candidate.getJac(), candidate.getNode().getId(),
                            coordinator.getJac(), coordinator.getNode().getId())) {
                        result = AdoptResult.CHANGED;
                    } else {
                        result = AdoptResult.STALE;
                    }
                } else {
                    result = AdoptResult.CHANGED;
                }
                if (result == AdoptResult.CHANGED) {
                    term = candidate.getTerm();
                    coordinator = candidate;
                    lastCoordinatorContactMs = System.currentTimeMillis();
                }
            }
            if (result == AdoptResult.CHANGED) {
                fire(previous, candidate);
            }
            return result;
        }

        /** Forgets the coordinator after it was detected as failed (the term number is kept). */
        public void clearCoordinator(int failedId) {
            CoordinatorInfo previous;
            synchronized (this) {
                if (!coordinator.getKnown() || coordinator.getNode().getId() != failedId) {
                    return;
                }
                previous = coordinator;
                coordinator = Protos.unknownCoordinator();
            }
            fire(previous, Protos.unknownCoordinator());
        }

        public void markCoordinatorContact() {
            lastCoordinatorContactMs = System.currentTimeMillis();
        }

        public long millisSinceCoordinatorContact() {
            return System.currentTimeMillis() - lastCoordinatorContactMs;
        }

        private void fire(CoordinatorInfo previous, CoordinatorInfo current) {
            for (Listener listener : listeners) {
                listener.onCoordinatorChanged(previous, current);
            }
        }
    }

    /**
     * Custom wave (echo) leader election over the unstructured overlay.
     *
     * <h2>ELECTION phase</h2>
     * The initiator sends ELECTION to all neighbours in parallel. A worker receiving an
     * election id for the first time records it, forwards it to all its other neighbours,
     * waits for their replies and answers its sender with the best candidate of its
     * sub-tree (itself included). A worker that has already seen the id answers
     * {@code duplicate=true} straight away, so no worker processes an election twice and
     * cycles in the overlay cannot cause endless forwarding. When all replies are back,
     * the initiator knows the best candidate among ALL reachable workers:
     * lowest JAC, ties broken by highest id.
     *
     * <h2>COORDINATOR phase</h2>
     * The initiator floods a COORDINATOR announcement (term + winner) through the overlay,
     * again with duplicate suppression. Every worker applies {@link ClusterState#adopt}.
     *
     * <h2>Safety</h2>
     * <ul>
     *   <li>Terms only increase; announcements for old terms are ignored.</li>
     *   <li>A worker that can still reach a live coordinator rejects an election started by
     *       someone else (it double-checks with a direct ping instead of trusting one
     *       worker's suspicion).</li>
     *   <li>If two elections run at once and produce different winners for the same term,
     *       the deterministic tie rule makes every worker pick the same one.</li>
     * </ul>
     */
    public static final class ElectionManager {

        private static final Log LOG = Log.get("ELECTION");
        private static final long FORWARD_MARGIN_MS = 150;
        private static final int SEEN_CACHE_SIZE = 2_000;

        private final ClusterState state;
        private final OverlayManager overlay;
        private final ChannelPool pool;
        private final ExecutorService executor;
        private final long electionTimeoutMs;
        private final long jitterMs;
        private final long rpcTimeoutMs;

        private final AtomicBoolean running = new AtomicBoolean(false);
        private final Map<String, Boolean> seenElections = boundedSet();
        private final Map<String, Boolean> seenAnnouncements = boundedSet();

        public ElectionManager(ClusterState state, OverlayManager overlay, ChannelPool pool, ExecutorService executor,
                               long electionTimeoutMs, long jitterMs, long rpcTimeoutMs) {
            this.state = state;
            this.overlay = overlay;
            this.pool = pool;
            this.executor = executor;
            this.electionTimeoutMs = electionTimeoutMs;
            this.jitterMs = jitterMs;
            this.rpcTimeoutMs = rpcTimeoutMs;
        }

        private static Map<String, Boolean> boundedSet() {
            return Collections.synchronizedMap(new LinkedHashMap<>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > SEEN_CACHE_SIZE;
                }
            });
        }

        public boolean isRunning() {
            return running.get();
        }

        /** Starts an election in the background (at most one at a time per worker). */
        public void initiate(ElectionReason reason) {
            if (!running.compareAndSet(false, true)) {
                return;
            }
            executor.execute(() -> {
                try {
                    runElection(reason);
                } catch (RuntimeException e) {
                    LOG.warn("Election failed", e);
                } finally {
                    running.set(false);
                }
            });
        }

        // ------------------------------------------------------------------ initiator side

        private void runElection(ElectionReason reason) {
            int termAtStart = state.term();
            if (reason != ElectionReason.TERM_LIMIT && jitterMs > 0) {
                // Random back-off: if several workers notice the failure at the same moment,
                // usually only one of them actually starts an election.
                sleep(ThreadLocalRandom.current().nextLong(jitterMs + 1));
                if (state.coordinator().getKnown() && state.term() > termAtStart) {
                    LOG.info("Another election already chose " + Protos.shortLabel(state.coordinator().getNode())
                            + " for term " + state.term() + " - not starting a new one");
                    return;
                }
            }
            int term = state.term() + 1;
            String electionId = "E" + term + "-W" + state.selfId() + "-" + UUID.randomUUID().toString().substring(0, 8);
            seenElections.put(electionId, Boolean.TRUE);

            LOG.info("Starting election " + electionId + " for term " + term + " (reason: " + reason
                    + ", my JAC=" + state.jac() + ", neighbours " + overlay.neighbourIds() + ")");

            ElectionRequest request = ElectionRequest.newBuilder()
                    .setElectionId(electionId)
                    .setTerm(term)
                    .setInitiatorId(state.selfId())
                    .setSender(state.self())
                    .setReason(reason)
                    .build();

            ElectionReply outcome = collect(request, -1, Deadline.after(electionTimeoutMs, TimeUnit.MILLISECONDS));
            if (outcome.getRejected()) {
                LOG.info("Election " + electionId + " aborted: a live coordinator still exists ("
                        + Protos.shortLabel(outcome.getCurrent().getNode()) + ", term " + outcome.getCurrent().getTerm() + ")");
                state.adopt(outcome.getCurrent());
                return;
            }
            Candidate best = outcome.getBest();

            LOG.info("Election " + electionId + " finished: " + outcome.getParticipantsCount() + " workers took part "
                    + outcome.getParticipantsList() + " -> winner " + Protos.shortLabel(best.getNode()) + " (JAC=" + best.getJac() + ")");
            announce(term, best);
        }

        // ------------------------------------------------------------------ ELECTION handling

        /** Handles an incoming ELECTION message (runs on a gRPC server thread). */
        public ElectionReply onElection(ElectionRequest request) {
            state.rememberPeer(request.getSender());
            if (seenElections.putIfAbsent(request.getElectionId(), Boolean.TRUE) != null) {
                LOG.debug("Duplicate election " + request.getElectionId() + " from Worker-" + request.getSender().getId()
                        + " ignored");
                return ElectionReply.newBuilder().setDuplicate(true).build();
            }
            CoordinatorInfo current = state.coordinator();
            if (shouldReject(request, current)) {
                LOG.info("Rejected election " + request.getElectionId() + " from Worker-" + request.getInitiatorId()
                        + ": coordinator " + Protos.shortLabel(current.getNode()) + " (term " + current.getTerm()
                        + ") is still alive");
                return ElectionReply.newBuilder().setRejected(true).setCurrent(current).build();
            }

            LOG.info("Election " + request.getElectionId() + " (term " + request.getTerm() + ") received from Worker-"
                    + request.getSender().getId() + " -> forwarding to " + overlay.neighbourIds());

            Deadline incoming = Context.current().getDeadline();
            long remaining = incoming == null ? electionTimeoutMs : incoming.timeRemaining(TimeUnit.MILLISECONDS);
            Deadline childDeadline = Deadline.after(Math.max(50, remaining - FORWARD_MARGIN_MS), TimeUnit.MILLISECONDS);

            ElectionRequest forwarded = request.toBuilder().setSender(state.self()).build();
            return collect(forwarded, request.getSender().getId(), childDeadline);
        }

        /**
         * Reject when the term is already decided, or when the current coordinator is still
         * alive and the election was not started by the coordinator itself (TERM_LIMIT).
         */
        private boolean shouldReject(ElectionRequest request, CoordinatorInfo current) {
            if (!current.getKnown()) {
                return false;
            }
            if (current.getTerm() >= request.getTerm()) {
                return true;
            }
            if (current.getNode().getId() == request.getInitiatorId()) {
                return false; // the coordinator itself stepped down (5-job term limit)
            }
            if (state.isCoordinator()) {
                return true; // I am the coordinator and obviously alive
            }
            return isAlive(current.getNode());
        }

        /**
         * Forwards the request and combines this worker's candidacy with its sub-trees' replies
         * (best candidate + participants); a rejection is passed back to the initiator as is.
         */
        private ElectionReply collect(ElectionRequest request, int excludeId, Deadline deadline) {
            Candidate best = state.selfCandidate();
            Set<Integer> participants = new TreeSet<>(List.of(state.selfId()));
            for (ElectionReply reply : forward(request, excludeId, deadline)) {
                if (reply.getRejected()) {
                    return reply;
                }
                if (!reply.getDuplicate()) {
                    best = ClusterState.better(best, reply.getBest());
                    participants.addAll(reply.getParticipantsList());
                }
            }
            return ElectionReply.newBuilder().setBest(best).addAllParticipants(participants).build();
        }

        /** Sends the request to every neighbour except {@code excludeId} in parallel and collects the replies. */
        private List<ElectionReply> forward(ElectionRequest request, int excludeId, Deadline deadline) {
            Map<NodeInfo, ListenableFuture<ElectionReply>> calls = new LinkedHashMap<>();
            for (NodeInfo neighbour : overlay.neighbours()) {
                if (neighbour.getId() == excludeId) {
                    continue;
                }
                calls.put(neighbour, ElectionServiceGrpc.newFutureStub(pool.get(neighbour))
                        .withDeadline(deadline)
                        .election(request));
            }
            List<ElectionReply> replies = new ArrayList<>();
            for (Map.Entry<NodeInfo, ListenableFuture<ElectionReply>> call : calls.entrySet()) {
                try {
                    replies.add(call.getValue().get());
                } catch (ExecutionException e) {
                    Status status = Status.fromThrowable(e.getCause());
                    if (status.getCode() == Status.Code.UNAVAILABLE) {
                        overlay.neighbourFailed(call.getKey(), "unreachable during election");
                    } else {
                        LOG.warn(Protos.shortLabel(call.getKey()) + " did not answer the election ("
                                + status.getCode() + ") - its sub-tree is skipped");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            return replies;
        }

        // ------------------------------------------------------------------ COORDINATOR handling

        private void announce(int term, Candidate leader) {
            String announcementId = "A" + term + "-W" + leader.getNode().getId() + "-"
                    + UUID.randomUUID().toString().substring(0, 8);
            seenAnnouncements.put(announcementId, Boolean.TRUE);
            CoordinatorAnnouncement announcement = CoordinatorAnnouncement.newBuilder()
                    .setAnnouncementId(announcementId)
                    .setTerm(term)
                    .setLeader(leader)
                    .setSender(state.self())
                    .build();
            state.adopt(toInfo(announcement));
            floodAnnouncement(announcement, -1);
            // Also tell the winner directly, in case it is not one of our neighbours.
            if (leader.getNode().getId() != state.selfId() && !overlay.isNeighbour(leader.getNode().getId())) {
                sendAnnouncement(leader.getNode(), announcement);
            }
        }

        /** Handles an incoming COORDINATOR announcement. */
        public Ack onCoordinator(CoordinatorAnnouncement announcement) {
            state.rememberPeer(announcement.getSender());
            state.rememberPeer(announcement.getLeader().getNode());
            if (seenAnnouncements.putIfAbsent(announcement.getAnnouncementId(), Boolean.TRUE) != null) {
                return Ack.newBuilder().setOk(true).setMessage("duplicate").build();
            }
            ClusterState.AdoptResult result = state.adopt(toInfo(announcement));
            if (result == ClusterState.AdoptResult.STALE) {
                LOG.info("Ignored stale COORDINATOR announcement (term " + announcement.getTerm() + ", "
                        + Protos.shortLabel(announcement.getLeader().getNode()) + "); current term " + state.term()
                        + " leader " + (state.coordinator().getKnown()
                        ? Protos.shortLabel(state.coordinator().getNode()) : "none"));
                return Ack.newBuilder().setOk(false).setMessage("stale").build();
            }
            floodAnnouncement(announcement.toBuilder().setSender(state.self()).build(), announcement.getSender().getId());
            return Ack.newBuilder().setOk(true).build();
        }

        private void floodAnnouncement(CoordinatorAnnouncement announcement, int excludeId) {
            for (NodeInfo neighbour : overlay.neighbours()) {
                if (neighbour.getId() != excludeId) {
                    sendAnnouncement(neighbour, announcement);
                }
            }
        }

        private void sendAnnouncement(NodeInfo target, CoordinatorAnnouncement announcement) {
            executor.execute(() -> {
                try {
                    ElectionServiceGrpc.newBlockingStub(pool.get(target))
                            .withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                            .coordinator(announcement);
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
                        overlay.neighbourFailed(target, "unreachable during COORDINATOR flood");
                    }
                }
            });
        }

        private static CoordinatorInfo toInfo(CoordinatorAnnouncement announcement) {
            return Protos.coordinator(announcement.getLeader().getNode(), announcement.getTerm(),
                    announcement.getLeader().getJac());
        }

        /** Direct liveness check of a node (one heartbeat with a short deadline). */
        public boolean isAlive(NodeInfo node) {
            try {
                PeerServiceGrpc.newBlockingStub(pool.get(node))
                        .withDeadlineAfter(Math.min(rpcTimeoutMs, 1000), TimeUnit.MILLISECONDS)
                        .heartbeat(HeartbeatRequest.newBuilder().setFrom(state.self()).build());
                return true;
            } catch (StatusRuntimeException e) {
                return false;
            }
        }

        private static void sleep(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** gRPC binding of the election protocol; the logic is in {@link ElectionManager}. */
    public static final class ElectionServiceImpl extends ElectionServiceGrpc.ElectionServiceImplBase {

        private final ElectionManager election;

        public ElectionServiceImpl(ElectionManager election) {
            this.election = election;
        }

        @Override
        public void election(ElectionRequest request, StreamObserver<ElectionReply> response) {
            response.onNext(election.onElection(request));
            response.onCompleted();
        }

        @Override
        public void coordinator(CoordinatorAnnouncement request, StreamObserver<Ack> response) {
            response.onNext(election.onCoordinator(request));
            response.onCompleted();
        }
    }
}
