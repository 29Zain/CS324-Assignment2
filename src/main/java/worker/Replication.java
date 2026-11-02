package worker;

import com.google.common.util.concurrent.*;
import common.Jobs.JobKind;
import common.Log;
import common.Net.*;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import proto.*;
import worker.Election.ClusterState;

/**
 * Replication of the coordinator's job table: before a job is accepted it is copied to other
 * workers (write-ahead, at least one acknowledgement), so a newly elected coordinator can
 * recover the jobs and their completed subtasks after the old coordinator crashes.
 */
public final class Replication {

    private Replication() {
    }

    /**
     * The replicated record of one job: everything a new coordinator needs to resume it.
     *
     * <p>Merging two copies never conflicts:
     * <ul>
     *   <li>completed subtasks are a grow-only set (union), because a subtask's result is
     *       deterministic - whoever computed it, the value is the same;</li>
     *   <li>a finished state (COMPLETED / FAILED / CANCELLED) beats an unfinished one;</li>
     *   <li>otherwise the copy with the newer (owner term, version) wins.</li>
     * </ul>
     */
    public static final class ReplicatedJob {

        public enum Phase {
            QUEUED, RUNNING, RECOVERING, COMPLETED, FAILED, CANCELLED;

            public boolean isFinished() {
                return this == COMPLETED || this == FAILED || this == CANCELLED;
            }
        }

        /** Input of the job (immutable). */
        public record Spec(String jobId, String clientId, JobKind kind, long[] numbers, long rangeStart,
                           long rangeEnd, int subtaskCount, long submittedAtMs) {
        }

        /** A finished subtask. */
        public record Part(int index, String value, int workerId, long units) {
        }

        /** The coordinator responsible for the job, and the term in which it took it. */
        public record Owner(int id, String host, int port, int term) {
        }

        private final String jobId;
        private Spec spec;
        private Owner owner;
        private Phase phase;
        private final Map<Integer, Part> parts = new TreeMap<>();
        private String result = "";
        private String error = "";
        private long version;
        private long finishedAtMs;

        public ReplicatedJob(String jobId, Spec spec, Owner owner, Phase phase, long version) {
            this.jobId = jobId;
            this.spec = spec;
            this.owner = owner;
            this.phase = phase;
            this.version = version;
        }

        public synchronized void merge(ReplicatedJob other) {
            if (!jobId.equals(other.jobId)) {
                throw new IllegalArgumentException("Cannot merge different jobs");
            }
            ReplicatedJob o = other.copy();
            if (spec == null && o.spec != null) {
                spec = o.spec;
            }
            for (Part part : o.parts.values()) {
                parts.putIfAbsent(part.index(), part);
            }
            boolean otherNewer = compare(o.owner, o.version, owner, version) > 0;
            boolean onlyOtherFinished = o.phase.isFinished() && !phase.isFinished();
            boolean onlyThisFinished = phase.isFinished() && !o.phase.isFinished();
            if (onlyOtherFinished || (!onlyThisFinished && otherNewer)) {
                takeStateFrom(o);
            }
        }

        private void takeStateFrom(ReplicatedJob o) {
            owner = o.owner;
            phase = o.phase;
            result = o.result;
            error = o.error;
            version = o.version;
            finishedAtMs = o.finishedAtMs;
        }

        private static int compare(Owner a, long versionA, Owner b, long versionB) {
            int termA = a == null ? -1 : a.term();
            int termB = b == null ? -1 : b.term();
            if (termA != termB) {
                return Integer.compare(termA, termB);
            }
            return Long.compare(versionA, versionB);
        }

        public synchronized ReplicatedJob copy() {
            ReplicatedJob copy = new ReplicatedJob(jobId, spec, owner, phase, version);
            copy.parts.putAll(parts);
            copy.result = result;
            copy.error = error;
            copy.finishedAtMs = finishedAtMs;
            return copy;
        }

        public synchronized void addPart(Part part) {
            parts.putIfAbsent(part.index(), part);
        }

        public synchronized void finish(Phase finalPhase, String finalResult, String finalError, long atMs) {
            phase = finalPhase;
            result = finalResult == null ? "" : finalResult;
            error = finalError == null ? "" : finalError;
            finishedAtMs = atMs;
        }

        public String jobId() {
            return jobId;
        }

        public synchronized Spec spec() {
            return spec;
        }

        public synchronized Owner owner() {
            return owner;
        }

        public synchronized void setOwner(Owner newOwner) {
            owner = newOwner;
        }

        public synchronized Phase phase() {
            return phase;
        }

        public synchronized void setPhase(Phase newPhase) {
            phase = newPhase;
        }

        public synchronized Map<Integer, Part> parts() {
            return new TreeMap<>(parts);
        }

        public synchronized String result() {
            return result;
        }

        public synchronized String error() {
            return error;
        }

        public synchronized long version() {
            return version;
        }

        public synchronized void setVersion(long newVersion) {
            version = newVersion;
        }

        public synchronized long finishedAtMs() {
            return finishedAtMs;
        }
    }

    /**
     * Every worker's local copy of the replicated job table. The coordinator writes to it
     * through replication; a newly elected coordinator reads it to resume work.
     */
    public static final class ReplicaStore {

        private static final int MAX_FINISHED_JOBS = 500;

        private final Map<String, ReplicatedJob> jobs = new ConcurrentHashMap<>();

        /** Merges a replicated record (or delta) into the local copy. */
        public void apply(ReplicatedJob incoming) {
            jobs.merge(incoming.jobId(), incoming.copy(), (existing, update) -> {
                existing.merge(update);
                return existing;
            });
            trimFinished();
        }

        public Optional<ReplicatedJob> get(String jobId) {
            return Optional.ofNullable(jobs.get(jobId)).map(ReplicatedJob::copy);
        }

        public List<ReplicatedJob> all() {
            List<ReplicatedJob> list = new ArrayList<>();
            jobs.values().forEach(j -> list.add(j.copy()));
            return list;
        }

        public List<ReplicatedJob> unfinished() {
            return all().stream().filter(j -> !j.phase().isFinished()).toList();
        }

        public int size() {
            return jobs.size();
        }

        /** Keeps memory bounded: drops the oldest finished jobs beyond a limit. */
        private void trimFinished() {
            if (jobs.size() <= MAX_FINISHED_JOBS) {
                return;
            }
            jobs.values().stream()
                    .filter(j -> j.phase().isFinished())
                    .sorted(Comparator.comparingLong(ReplicatedJob::finishedAtMs))
                    .limit(jobs.size() - MAX_FINISHED_JOBS)
                    .map(ReplicatedJob::jobId)
                    .toList()
                    .forEach(jobs::remove);
        }
    }

    /** Converts replicated job records between the domain model and protobuf messages. */
    public static final class ReplicationMapper {

        private ReplicationMapper() {
        }

        public static JobRecord toProto(ReplicatedJob job) {
            JobRecord.Builder record = JobRecord.newBuilder()
                    .setJobId(job.jobId())
                    .setStatus(toProto(job.phase()))
                    .setResult(job.result())
                    .setError(job.error())
                    .setVersion(job.version())
                    .setFinishedAtMs(job.finishedAtMs());
            ReplicatedJob.Owner owner = job.owner();
            if (owner != null) {
                record.setOwner(Protos.node(owner.id(), owner.host(), owner.port())).setOwnerTerm(owner.term());
            }
            ReplicatedJob.Spec spec = job.spec();
            if (spec != null) {
                record.setSpec(JobSpec.newBuilder()
                        .setJobId(spec.jobId())
                        .setClientId(spec.clientId())
                        .setType(Protos.toProto(spec.kind()))
                        .addAllNumbers(Protos.asIterable(spec.numbers()))
                        .setRangeStart(spec.rangeStart())
                        .setRangeEnd(spec.rangeEnd())
                        .setSubtaskCount(spec.subtaskCount())
                        .setSubmittedAtMs(spec.submittedAtMs()));
            }
            for (ReplicatedJob.Part part : job.parts().values()) {
                record.addCompleted(CompletedSubtask.newBuilder()
                        .setIndex(part.index()).setValue(part.value())
                        .setWorkerId(part.workerId()).setUnits(part.units()));
            }
            return record.build();
        }

        public static ReplicatedJob fromProto(JobRecord record) {
            ReplicatedJob.Spec spec = null;
            if (record.hasSpec()) {
                JobSpec s = record.getSpec();
                spec = new ReplicatedJob.Spec(s.getJobId(), s.getClientId(), Protos.fromProto(s.getType()),
                        Protos.toArray(s.getNumbersList()), s.getRangeStart(), s.getRangeEnd(),
                        s.getSubtaskCount(), s.getSubmittedAtMs());
            }
            ReplicatedJob.Owner owner = null;
            if (record.hasOwner()) {
                NodeInfo node = record.getOwner();
                owner = new ReplicatedJob.Owner(node.getId(), node.getHost(), node.getPort(), record.getOwnerTerm());
            }
            ReplicatedJob job = new ReplicatedJob(record.getJobId(), spec, owner, fromProto(record.getStatus()),
                    record.getVersion());
            for (CompletedSubtask part : record.getCompletedList()) {
                job.addPart(new ReplicatedJob.Part(part.getIndex(), part.getValue(), part.getWorkerId(), part.getUnits()));
            }
            if (job.phase().isFinished()) {
                job.finish(job.phase(), record.getResult(), record.getError(), record.getFinishedAtMs());
            }
            return job;
        }

        public static JobStatus toProto(ReplicatedJob.Phase phase) {
            return switch (phase) {
                case QUEUED -> JobStatus.QUEUED;
                case RUNNING -> JobStatus.RUNNING;
                case RECOVERING -> JobStatus.RECOVERING;
                case COMPLETED -> JobStatus.COMPLETED;
                case FAILED -> JobStatus.FAILED;
                case CANCELLED -> JobStatus.CANCELLED;
            };
        }

        public static ReplicatedJob.Phase fromProto(JobStatus status) {
            return switch (status) {
                case QUEUED -> ReplicatedJob.Phase.QUEUED;
                case RECOVERING -> ReplicatedJob.Phase.RECOVERING;
                case COMPLETED -> ReplicatedJob.Phase.COMPLETED;
                case FAILED -> ReplicatedJob.Phase.FAILED;
                case CANCELLED -> ReplicatedJob.Phase.CANCELLED;
                default -> ReplicatedJob.Phase.RUNNING;
            };
        }
    }

    /**
     * Pushes job state changes from the coordinator to the other workers, and pulls the
     * replicas back when a new coordinator takes over.
     *
     * <p>When a job is accepted, the coordinator waits until at least
     * {@code syncAcks} other workers have stored it before confirming to the client
     * (a write-ahead step): a crash right after "accepted" can then never lose the job.
     * Later updates (finished subtasks) are sent asynchronously.
     */
    public static final class ReplicationManager {

        private static final Log LOG = Log.get("REPLICATION");

        private final ClusterState state;
        private final ChannelPool pool;
        private final ReplicaStore store;
        private final Supplier<List<NodeInfo>> targets;
        private final long rpcTimeoutMs;
        private final int syncAcks;

        public ReplicationManager(ClusterState state, ChannelPool pool, ReplicaStore store,
                                  Supplier<List<NodeInfo>> targets, long rpcTimeoutMs, int syncAcks) {
            this.state = state;
            this.pool = pool;
            this.store = store;
            this.targets = targets;
            this.rpcTimeoutMs = rpcTimeoutMs;
            this.syncAcks = syncAcks;
        }

        /**
         * Stores the record locally and sends it to the other workers.
         *
         * @param waitForAcks true to block until {@code syncAcks} replicas confirmed (or timeout)
         */
        public void replicate(ReplicatedJob job, boolean waitForAcks) {
            store.apply(job);
            List<NodeInfo> replicas = otherWorkers();
            if (replicas.isEmpty()) {
                return;
            }
            ReplicationBatch batch = ReplicationBatch.newBuilder()
                    .setFrom(state.self())
                    .setTerm(state.term())
                    .addJobs(ReplicationMapper.toProto(job))
                    .build();
            int needed = waitForAcks ? Math.min(syncAcks, replicas.size()) : 0;
            CountDownLatch acks = new CountDownLatch(needed);
            for (NodeInfo replica : replicas) {
                // ROOT context: replication must finish even after the client request that
                // triggered it (e.g. SubmitJob) has returned.
                ListenableFuture<Ack> call;
                Context previous = Context.ROOT.attach();
                try {
                    call = ReplicationServiceGrpc.newFutureStub(pool.get(replica))
                            .withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                            .replicate(batch);
                } finally {
                    Context.ROOT.detach(previous);
                }
                Futures.addCallback(call, new FutureCallback<Ack>() {
                    @Override
                    public void onSuccess(Ack ack) {
                        acks.countDown();
                    }

                    @Override
                    public void onFailure(Throwable error) {
                        LOG.debug("Replica " + Protos.shortLabel(replica) + " did not store job " + job.jobId());
                    }
                }, MoreExecutors.directExecutor());
            }
            if (needed > 0) {
                try {
                    if (!acks.await(rpcTimeoutMs + 500, TimeUnit.MILLISECONDS)) {
                        LOG.warn("Job " + job.jobId() + " confirmed by fewer than " + needed
                                + " replica(s); continuing anyway");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        /** Pulls the replicated state of all reachable workers into the local store. */
        public int fetchFromAll() {
            Map<NodeInfo, ListenableFuture<StateSnapshot>> calls = new LinkedHashMap<>();
            for (NodeInfo replica : otherWorkers()) {
                calls.put(replica, ReplicationServiceGrpc.newFutureStub(pool.get(replica))
                        .withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                        .fetchState(Empty.getDefaultInstance()));
            }
            int records = 0;
            int answered = 0;
            for (Map.Entry<NodeInfo, ListenableFuture<StateSnapshot>> call : calls.entrySet()) {
                try {
                    StateSnapshot snapshot = call.getValue().get();
                    answered++;
                    applyAll(store, snapshot.getJobsList());
                    records += snapshot.getJobsCount();
                } catch (ExecutionException e) {
                    LOG.debug("Could not fetch state from " + Protos.shortLabel(call.getKey()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            LOG.info("Merged replicated state from " + answered + " worker(s): " + records + " record(s), "
                    + store.unfinished().size() + " unfinished job(s)");
            return records;
        }

        private List<NodeInfo> otherWorkers() {
            return targets.get().stream().filter(node -> node.getId() != state.selfId()).toList();
        }
    }

    /** Merges received job records into the local replica store, in order. */
    private static void applyAll(ReplicaStore store, List<JobRecord> records) {
        records.forEach(record -> store.apply(ReplicationMapper.fromProto(record)));
    }

    /** Stores replicated job records sent by the coordinator and serves them back on request. */
    public static final class ReplicationServiceImpl extends ReplicationServiceGrpc.ReplicationServiceImplBase {

        private final ReplicaStore store;
        private final ClusterState state;

        public ReplicationServiceImpl(ReplicaStore store, ClusterState state) {
            this.store = store;
            this.state = state;
        }

        @Override
        public void replicate(ReplicationBatch request, StreamObserver<Ack> response) {
            applyAll(store, request.getJobsList());
            response.onNext(Ack.newBuilder().setOk(true).build());
            response.onCompleted();
        }

        @Override
        public void fetchState(Empty request, StreamObserver<StateSnapshot> response) {
            StateSnapshot.Builder snapshot = StateSnapshot.newBuilder();
            if (state.hasSelf()) {
                snapshot.setFrom(state.self());
            }
            store.all().forEach(job -> snapshot.addJobs(ReplicationMapper.toProto(job)));
            response.onNext(snapshot.build());
            response.onCompleted();
        }
    }
}
