package worker;

import common.Log;
import common.Net.*;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import proto.*;
import worker.Election.ClusterState;
import worker.Worker.BootstrapClient;

/**
 * Unstructured overlay network: each worker keeps links to at least two random neighbours,
 * pings them and repairs the overlay when a neighbour disappears.
 */
public final class Overlay {

    private Overlay() {
    }

    /**
     * Maintains this worker's links in the unstructured overlay network.
     *
     * <p>A joining worker links to a few <b>random</b> active workers given by the
     * Bootstrap Node (default 2), and other workers link to it later, so workers end
     * up with different numbers of neighbours. Links are bidirectional.
     *
     * <p>Assignment 1 linked each worker to only ONE peer, which forms a tree: losing
     * one worker split the network and an election then missed workers. With at least
     * two links per worker, plus repair when a neighbour dies, the overlay stays connected.
     */
    public static final class OverlayManager {

        private static final Log LOG = Log.get("OVERLAY");
        private static final int MISSES_BEFORE_REMOVAL = 2;

        private final ClusterState state;
        private final ChannelPool pool;
        private final BootstrapClient bootstrap;
        private final ScheduledExecutorService scheduler;
        private final int minNeighbours;
        private final long pingIntervalMs;
        private final long rpcTimeoutMs;

        private final Map<Integer, NodeInfo> neighbours = new ConcurrentHashMap<>();
        private final Map<Integer, Integer> missedPings = new ConcurrentHashMap<>();

        public OverlayManager(ClusterState state, ChannelPool pool, BootstrapClient bootstrap,
                              ScheduledExecutorService scheduler, int minNeighbours,
                              long pingIntervalMs, long rpcTimeoutMs) {
            this.state = state;
            this.pool = pool;
            this.bootstrap = bootstrap;
            this.scheduler = scheduler;
            this.minNeighbours = minNeighbours;
            this.pingIntervalMs = pingIntervalMs;
            this.rpcTimeoutMs = rpcTimeoutMs;
        }

        /** Links to the random peers received at registration. Returns their replies. */
        public List<NeighbourReply> join(List<NodeInfo> peers) {
            List<NeighbourReply> replies = peers.stream().map(this::link).filter(Objects::nonNull).toList();
            if (peers.isEmpty()) {
                LOG.info("No other workers yet - this is the first worker of the overlay");
            }
            return replies;
        }

        public void start() {
            scheduler.scheduleWithFixedDelay(this::maintain, pingIntervalMs, pingIntervalMs, TimeUnit.MILLISECONDS);
        }

        /** Asks {@code peer} to add us as a neighbour; on success we add it too (bidirectional link). */
        private NeighbourReply link(NodeInfo peer) {
            if (peer.getId() == state.selfId()) {
                return null;
            }
            try {
                NeighbourReply reply = PeerServiceGrpc.newBlockingStub(pool.get(peer))
                        .withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                        .addNeighbour(state.self());
                if (reply.getAccepted()) {
                    state.rememberPeer(peer);
                    neighbours.put(peer.getId(), peer);
                    missedPings.remove(peer.getId());
                    LOG.info("Linked with " + Protos.label(peer) + " -> neighbours " + neighbourIds());
                    return reply;
                }
            } catch (StatusRuntimeException e) {
                LOG.warn("Could not link with " + Protos.label(peer) + " (" + e.getStatus().getCode() + ")");
                bootstrap.reportFailure(peer.getId());
            }
            return null;
        }

        /** Called when another worker links to us. */
        public void acceptNeighbour(NodeInfo peer) {
            if (peer.getId() == state.selfId()) {
                return;
            }
            state.rememberPeer(peer);
            NodeInfo previous = neighbours.put(peer.getId(), peer);
            missedPings.remove(peer.getId());
            if (previous == null || !previous.equals(peer)) {
                LOG.info(Protos.label(peer) + " linked to us -> neighbours " + neighbourIds());
            }
        }

        /** Periodic neighbour liveness check + overlay repair. */
        private void maintain() {
            try {
                for (NodeInfo neighbour : neighbours()) {
                    ping(neighbour);
                }
                repairIfNeeded();
            } catch (RuntimeException e) {
                LOG.warn("Overlay maintenance failed", e);
            }
        }

        private void ping(NodeInfo neighbour) {
            try {
                HeartbeatReply reply = PeerServiceGrpc.newBlockingStub(pool.get(neighbour))
                        .withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                        .heartbeat(HeartbeatRequest.newBuilder()
                                .setFrom(state.self())
                                .setFromCoordinator(false)
                                .setCoordinator(state.coordinator())
                                .build());
                missedPings.remove(neighbour.getId());
                // Gossip: a neighbour may know a newer coordinator than we do.
                if (reply.getCoordinator().getKnown() && reply.getCoordinator().getTerm() > state.term()) {
                    state.adopt(reply.getCoordinator());
                }
            } catch (StatusRuntimeException e) {
                int misses = missedPings.merge(neighbour.getId(), 1, Integer::sum);
                if (misses >= MISSES_BEFORE_REMOVAL) {
                    removeNeighbour(neighbour, "missed " + misses + " neighbour pings");
                }
            }
        }

        /** Immediately drops a neighbour that is known to be unreachable. */
        public void neighbourFailed(NodeInfo neighbour, String reason) {
            removeNeighbour(neighbour, reason); // no-op if it is not (or no longer) a neighbour
        }

        private void removeNeighbour(NodeInfo neighbour, String reason) {
            if (neighbours.remove(neighbour.getId()) != null) {
                missedPings.remove(neighbour.getId());
                pool.invalidate(neighbour);
                bootstrap.reportFailure(neighbour.getId());
                LOG.warn("Removed neighbour " + Protos.label(neighbour) + " (" + reason + ") -> neighbours " + neighbourIds());
            }
        }

        /** Keeps at least {@code minNeighbours} links by asking the Bootstrap Node for new random peers. */
        private void repairIfNeeded() {
            if (!state.hasSelf() || neighbours.size() >= minNeighbours) {
                return;
            }
            List<Integer> exclude = new ArrayList<>(neighbours.keySet());
            exclude.add(state.selfId());
            List<NodeInfo> candidates = bootstrap.randomPeers(state.selfId(), minNeighbours - neighbours.size(), exclude);
            for (NodeInfo candidate : candidates) {
                if (link(candidate) != null) {
                    LOG.info("Overlay repaired: new link to " + Protos.shortLabel(candidate));
                }
            }
        }

        public List<NodeInfo> neighbours() {
            List<NodeInfo> list = new ArrayList<>(neighbours.values());
            list.sort(Comparator.comparingInt(NodeInfo::getId));
            return list;
        }

        public List<Integer> neighbourIds() {
            return neighbours().stream().map(NodeInfo::getId).toList();
        }

        public boolean isNeighbour(int id) {
            return neighbours.containsKey(id);
        }
    }

    /** Answers overlay link requests, heartbeats and status queries from other workers. */
    public static final class PeerServiceImpl extends PeerServiceGrpc.PeerServiceImplBase {

        private final ClusterState state;
        private final OverlayManager overlay;
        private final IntSupplier activeSubtasks;
        private final LongSupplier completedSubtasks;
        private final IntSupplier jobsInTerm;
        private final int maxJobsPerTerm;

        public PeerServiceImpl(ClusterState state, OverlayManager overlay, IntSupplier activeSubtasks,
                               LongSupplier completedSubtasks, IntSupplier jobsInTerm, int maxJobsPerTerm) {
            this.state = state;
            this.overlay = overlay;
            this.activeSubtasks = activeSubtasks;
            this.completedSubtasks = completedSubtasks;
            this.jobsInTerm = jobsInTerm;
            this.maxJobsPerTerm = maxJobsPerTerm;
        }

        @Override
        public void addNeighbour(NodeInfo request, StreamObserver<NeighbourReply> response) {
            overlay.acceptNeighbour(request);
            response.onNext(NeighbourReply.newBuilder().setAccepted(true).setCoordinator(state.coordinator()).build());
            response.onCompleted();
        }

        /**
         * Heartbeat from the coordinator (or from an overlay neighbour). A heartbeat from a
         * coordinator also tells us who leads which term, so a worker that missed the
         * COORDINATOR flood still learns the leader.
         */
        @Override
        public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatReply> response) {
            if (request.hasFrom()) {
                state.rememberPeer(request.getFrom());
            }
            if (request.getFromCoordinator() && request.getCoordinator().getKnown()) {
                ClusterState.AdoptResult result = state.adopt(request.getCoordinator());
                if (result != ClusterState.AdoptResult.STALE
                        && request.getCoordinator().getNode().getId() == request.getFrom().getId()) {
                    state.markCoordinatorContact();
                }
            }
            response.onNext(HeartbeatReply.newBuilder()
                    .setNodeId(state.selfId())
                    .setTerm(state.term())
                    .setJac(state.jac())
                    .setActiveSubtasks(activeSubtasks.getAsInt())
                    .setCoordinator(state.coordinator())
                    .addAllNeighbourIds(overlay.neighbourIds())
                    .build());
            response.onCompleted();
        }

        @Override
        public void getStatus(Empty request, StreamObserver<NodeStatus> response) {
            NodeStatus.Builder status = NodeStatus.newBuilder()
                    .setTerm(state.term())
                    .setJac(state.jac())
                    .setIsCoordinator(state.isCoordinator())
                    .setCoordinator(state.coordinator())
                    .addAllNeighbourIds(overlay.neighbourIds())
                    .setActiveSubtasks(activeSubtasks.getAsInt())
                    .setCompletedSubtasks(completedSubtasks.getAsLong())
                    .setJobsInTerm(jobsInTerm.getAsInt())
                    .setMaxJobsPerTerm(maxJobsPerTerm);
            if (state.hasSelf()) {
                status.setNode(state.self());
            }
            response.onNext(status.build());
            response.onCompleted();
        }
    }
}
