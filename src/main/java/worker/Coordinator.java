package worker;

import common.Jobs.*;
import common.Jobs.Job;
import common.Log;
import common.Net.*;
import io.grpc.*;
import io.grpc.stub.*;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
import proto.*;
import worker.Election.ClusterState;
import worker.FailureDetector.*;
import worker.Replication.*;
import worker.Worker.WorkerSettings;

/**
 * The coordinator role: accepts jobs from clients, splits them into subtasks, schedules them
 * on workers (pull-based, with reassignment on failure), streams progress to clients and
 * enforces the job-acceptance-count (JAC) rules and the 5-jobs-per-term limit.
 */
public final class Coordinator {

    private Coordinator() {
    }

    /** The proto node of a job owner. */
    private static NodeInfo ownerNode(ReplicatedJob.Owner owner) {
        return Protos.node(owner.id(), owner.host(), owner.port());
    }

    /** Sends a single response and completes the stream. */
    private static <T> void reply(StreamObserver<T> response, T value) {
        response.onNext(value);
        response.onCompleted();
    }

    /**
     * What a running job needs from the rest of the worker. Keeping this as an interface
     * keeps {@link JobRun} (scheduling logic) independent of networking details.
     */
    public interface JobEnvironment {

        NodeInfo self();

        /** Workers that may receive new subtasks (ALIVE only), including this one. */
        List<NodeInfo> assignableWorkers();

        Health healthOf(int workerId);

        /** Reports that an RPC to {@code worker} just failed. */
        void suspect(NodeInfo worker, String reason);

        /** Sends a subtask to a worker; the callbacks receive its progress and result. */
        DispatchHandle dispatch(NodeInfo worker, String jobId, Subtask subtask, int attempt, DispatchCallbacks callbacks);

        /** JAC accounting hook. */
        void onSubtaskDispatched(NodeInfo worker);

        /** Replicates a job state change to the other workers (asynchronously). */
        void replicate(ReplicatedJob delta);

        /** Handle used to cancel a running remote subtask. */
        @FunctionalInterface
        interface DispatchHandle {
            void cancel();
        }

        /** Events of one remote subtask execution. */
        interface DispatchCallbacks {
            void onProgress(long unitsDone);

            void onResult(String value);

            void onFailure(Throwable error);

            void onCompleted();
        }
    }

    /**
     * How the Job Allocation Counter (JAC) is incremented. The JAC only counts work this
     * worker assigns to OTHER workers while it is the coordinator.
     *
     * <ul>
     *   <li>{@code job} (default): +1 for every job the coordinator distributes to at least one
     *       other worker ("assigns a job to another worker").</li>
     *   <li>{@code subtask}: +1 for every subtask sent to another worker (A1 behaviour).</li>
     * </ul>
     */
    public static abstract class JacPolicy {

        protected final ClusterState state;

        protected JacPolicy(ClusterState state) {
            this.state = state;
        }

        public static JacPolicy create(String mode, ClusterState state) {
            return "subtask".equals(mode) ? new PerSubtask(state) : new PerJob(state);
        }

        public abstract void onJobDistributed(boolean usedOtherWorkers);

        public abstract void onSubtaskDispatched(NodeInfo target);

        protected boolean counts() {
            return state.isCoordinator();
        }

        /** +1 per distributed job. */
        static final class PerJob extends JacPolicy {
            PerJob(ClusterState state) {
                super(state);
            }

            @Override
            public void onJobDistributed(boolean usedOtherWorkers) {
                if (usedOtherWorkers && counts()) {
                    state.incrementJac();
                }
            }

            @Override
            public void onSubtaskDispatched(NodeInfo target) {
                // not counted in this mode
            }
        }

        /** +1 per subtask sent to another worker. */
        static final class PerSubtask extends JacPolicy {
            PerSubtask(ClusterState state) {
                super(state);
            }

            @Override
            public void onJobDistributed(boolean usedOtherWorkers) {
                // not counted in this mode
            }

            @Override
            public void onSubtaskDispatched(NodeInfo target) {
                if (target.getId() != state.selfId() && counts()) {
                    state.incrementJac();
                }
            }
        }
    }

    /** A job request was refused (not the coordinator, term ended, invalid input ...). */
    public static class JobRejectedException extends Exception {

        private final Status.Code code;

        public JobRejectedException(Status.Code code, String message) {
            super(message);
            this.code = code;
        }

        public Status toStatus() {
            return Status.fromCode(code).withDescription(getMessage());
        }
    }

    /**
     * Runtime state of one job on its owner (the coordinator that accepted or adopted it).
     *
     * <p>Each subtask has a unique task id (job id + index) and is PENDING, RUNNING or DONE.
     * Every (re)assignment is a new numbered <i>attempt</i>. The first result recorded for a
     * subtask wins; any later result for the same subtask (e.g. from a worker that was only
     * slow and whose work had already been reassigned) is ignored. That makes reassignment
     * safe: work is never counted twice.
     *
     * <p>All methods are synchronized: gRPC callbacks from many workers update one job.
     */
    static final class JobRun {

        private static final Log LOG = Log.get("JOB");
        private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");
        private static final int MAX_EVENTS = 60;

        private enum TaskState { PENDING, RUNNING, DONE }

        /** One execution of a subtask on one worker. */
        private static final class Attempt {
            final int number;
            final NodeInfo worker;
            final boolean backup;
            volatile long unitsDone;
            volatile long lastUpdateMs;
            JobEnvironment.DispatchHandle handle;

            Attempt(int number, NodeInfo worker, boolean backup, long now) {
                this.number = number;
                this.worker = worker;
                this.backup = backup;
                this.lastUpdateMs = now;
            }
        }

        private static final class SubtaskState {
            final Subtask subtask;
            TaskState state = TaskState.PENDING;
            String result;
            int doneBy = -1;
            int attempts;
            final Map<Integer, Attempt> running = new LinkedHashMap<>();

            SubtaskState(Subtask subtask) {
                this.subtask = subtask;
            }
        }

        private final Job job;
        private final ReplicatedJob.Spec spec;
        private final ReplicatedJob.Owner owner;
        private final JobEnvironment env;
        private final SubtaskState[] subtasks;
        private final int maxAttempts;
        private final int maxInFlightPerWorker;
        private final long totalUnits;
        private final Deque<String> events = new ArrayDeque<>();
        private final Set<Integer> workersUsed = new HashSet<>();

        private ReplicatedJob.Phase phase = ReplicatedJob.Phase.QUEUED;
        private String result = "";
        private String error = "";
        private long finishedAtMs;
        private long changeVersion;
        private long replicationVersion;

        JobRun(Job job, ReplicatedJob.Spec spec, ReplicatedJob.Owner owner, JobEnvironment env,
               Map<Integer, ReplicatedJob.Part> alreadyDone, int maxAttempts, int maxInFlightPerWorker,
               long baseReplicationVersion) {
            this.job = job;
            this.spec = spec;
            this.owner = owner;
            this.env = env;
            this.maxAttempts = maxAttempts;
            this.maxInFlightPerWorker = maxInFlightPerWorker;
            this.replicationVersion = baseReplicationVersion;
            List<Subtask> split = job.split(spec.subtaskCount());
            this.subtasks = new SubtaskState[split.size()];
            long units = 0;
            for (int i = 0; i < split.size(); i++) {
                subtasks[i] = new SubtaskState(split.get(i));
                units += split.get(i).units();
                ReplicatedJob.Part part = alreadyDone.get(i);
                if (part != null) {
                    subtasks[i].state = TaskState.DONE;
                    subtasks[i].result = part.value();
                    subtasks[i].doneBy = part.workerId();
                }
            }
            this.totalUnits = units;
        }

        String jobId() {
            return job.id();
        }

        synchronized ReplicatedJob.Phase phase() {
            return phase;
        }

        synchronized boolean isFinished() {
            return phase.isFinished();
        }

        synchronized long finishedAtMs() {
            return finishedAtMs;
        }

        synchronized long changeVersion() {
            return changeVersion;
        }

        synchronized int subtaskCount() {
            return subtasks.length;
        }

        synchronized boolean usedOtherWorkers() {
            return workersUsed.stream().anyMatch(id -> id != env.self().getId());
        }

        /**
         * Starts the job. Subtasks are handed out on demand: every worker gets at most
         * {@code maxInFlightPerWorker} subtasks of this job at a time and receives the next one
         * when it finishes one. The subtasks themselves are equal-sized, so the work is divided
         * evenly; faster workers simply take more of them, and a worker that joins while the
         * job runs starts receiving subtasks immediately.
         */
        synchronized void start(List<NodeInfo> workers) {
            phase = ReplicatedJob.Phase.RUNNING;
            List<NodeInfo> pool = workers.isEmpty() ? List.of(env.self()) : workers;
            event("Split into " + subtasks.length + " equal subtasks; up to " + maxInFlightPerWorker
                    + " at a time per worker, workers " + pool.stream().map(NodeInfo::getId).toList());
            fillSlots();
            if (allDone()) {
                finish();
            }
            changed();
        }

        /** Gives queued subtasks to workers that have a free slot (least loaded first). */
        private void fillSlots() {
            if (phase.isFinished()) {
                return;
            }
            List<NodeInfo> workers = env.assignableWorkers();
            if (workers.isEmpty()) {
                workers = List.of(env.self());
            }
            Map<Integer, Integer> load = currentLoad();
            for (SubtaskState st : subtasks) {
                if (st.state != TaskState.PENDING) {
                    continue;
                }
                NodeInfo target = null;
                for (NodeInfo worker : workers) {
                    int used = load.getOrDefault(worker.getId(), 0);
                    if (used < maxInFlightPerWorker
                            && (target == null || used < load.getOrDefault(target.getId(), 0))) {
                        target = worker;
                    }
                }
                if (target == null) {
                    return; // every worker is busy; the rest stays queued
                }
                dispatch(st, target, false);
                load.merge(target.getId(), 1, Integer::sum);
            }
        }

        private Map<Integer, Integer> currentLoad() {
            Map<Integer, Integer> load = new TreeMap<>();
            for (SubtaskState st : subtasks) {
                for (Attempt attempt : st.running.values()) {
                    load.merge(attempt.worker.getId(), 1, Integer::sum);
                }
            }
            return load;
        }

        private void dispatch(SubtaskState st, NodeInfo worker, boolean backup) {
            if (phase.isFinished()) {
                return;
            }
            int number = ++st.attempts;
            if (number > maxAttempts) {
                fail("Subtask #" + st.subtask.index() + " could not be completed after " + maxAttempts + " attempts");
                return;
            }
            Attempt attempt = new Attempt(number, worker, backup, System.currentTimeMillis());
            st.running.put(number, attempt);
            st.state = TaskState.RUNNING;
            workersUsed.add(worker.getId());
            env.onSubtaskDispatched(worker);
            final int index = st.subtask.index();
            attempt.handle = env.dispatch(worker, job.id(), st.subtask, number, new JobEnvironment.DispatchCallbacks() {
                @Override
                public void onProgress(long unitsDone) {
                    progress(index, number, unitsDone);
                }

                @Override
                public void onResult(String value) {
                    completed(index, number, value);
                }

                @Override
                public void onFailure(Throwable error) {
                    failed(index, number, error);
                }

                @Override
                public void onCompleted() {
                    failed(index, number, null);
                }
            });
            changed();
        }

        private synchronized void progress(int index, int attemptNumber, long unitsDone) {
            Attempt attempt = subtasks[index].running.get(attemptNumber);
            if (attempt != null) {
                attempt.unitsDone = unitsDone;
                attempt.lastUpdateMs = System.currentTimeMillis();
                changed();
            }
        }

        private synchronized void completed(int index, int attemptNumber, String value) {
            SubtaskState st = subtasks[index];
            Attempt attempt = st.running.remove(attemptNumber);
            if (attempt == null || phase.isFinished()) {
                return;
            }
            if (st.state == TaskState.DONE) {
                event("Duplicate result for subtask #" + index + " from " + Protos.shortLabel(attempt.worker)
                        + " ignored (already completed by Worker-" + st.doneBy + ")");
                return;
            }
            st.state = TaskState.DONE;
            st.result = value;
            st.doneBy = attempt.worker.getId();
            cancelAttempts(st);
            if (attempt.backup) {
                event("Backup copy of subtask #" + index + " on " + Protos.shortLabel(attempt.worker) + " finished first");
            }
            ReplicatedJob delta = delta();
            delta.addPart(new ReplicatedJob.Part(index, value, st.doneBy, st.subtask.units()));
            env.replicate(delta);
            if (allDone()) {
                finish();
            } else {
                fillSlots();
            }
            changed();
        }

        /** Attempt ended with an error (or ended without a result). */
        private synchronized void failed(int index, int attemptNumber, Throwable error) {
            SubtaskState st = subtasks[index];
            Attempt attempt = st.running.remove(attemptNumber);
            if (attempt == null || st.state == TaskState.DONE || phase.isFinished()) {
                return; // already handled, or we cancelled it ourselves
            }
            String reason = error == null ? "stream closed without a result" : Status.fromThrowable(error).getCode().name();
            env.suspect(attempt.worker, "subtask stream failed: " + reason);
            if (st.running.isEmpty()) {
                st.state = TaskState.PENDING;
                event(Protos.shortLabel(attempt.worker) + " failed while running subtask #" + index + " (" + reason
                        + ") -> subtask returned to the queue for reassignment");
            }
            fillSlots();
            changed();
        }

        /** A worker was declared DEAD: reassign everything it was running. */
        synchronized void workerDead(int workerId) {
            if (phase.isFinished()) {
                return;
            }
            List<Integer> moved = new ArrayList<>();
            for (SubtaskState st : subtasks) {
                if (st.state != TaskState.RUNNING) {
                    continue;
                }
                List<Attempt> onDead = st.running.values().stream()
                        .filter(a -> a.worker.getId() == workerId).toList();
                for (Attempt attempt : onDead) {
                    st.running.remove(attempt.number);
                    cancel(attempt);
                }
                if (!onDead.isEmpty() && st.running.isEmpty()) {
                    st.state = TaskState.PENDING;
                    moved.add(st.subtask.index());
                }
            }
            if (!moved.isEmpty()) {
                event("Worker-" + workerId + " declared DEAD -> subtasks " + moved + " returned to the queue");
            }
            fillSlots();
            changed();
        }

        /**
         * A subtask whose attempts sent no update for {@code stallMs} although the worker is
         * still alive gets a backup copy on another worker; whichever finishes first wins.
         */
        synchronized void checkStalls(long now, long stallMs) {
            if (phase.isFinished()) {
                return;
            }
            fillSlots(); // also hands queued work to workers that joined since the last check
            for (SubtaskState st : subtasks) {
                if (st.state != TaskState.RUNNING || st.running.size() != 1) {
                    continue;
                }
                Attempt only = st.running.values().iterator().next();
                if (now - only.lastUpdateMs > stallMs) {
                    NodeInfo backup = chooseWorker(Set.of(only.worker.getId()));
                    if (backup.getId() != only.worker.getId()) {
                        event(Protos.shortLabel(only.worker) + " is slow: no progress on subtask #" + st.subtask.index()
                                + " for " + (now - only.lastUpdateMs) / 1000 + " s -> backup copy on "
                                + Protos.shortLabel(backup));
                        only.lastUpdateMs = now;
                        dispatch(st, backup, true);
                    }
                }
            }
        }

        synchronized void cancel(String reason) {
            if (phase.isFinished()) {
                return;
            }
            terminate(ReplicatedJob.Phase.CANCELLED, reason, "Job cancelled: ");
            changed();
        }

        /** Least-loaded assignable worker, avoiding {@code avoid} when possible. */
        private NodeInfo chooseWorker(Set<Integer> avoid) {
            List<NodeInfo> candidates = new ArrayList<>(env.assignableWorkers());
            List<NodeInfo> preferred = candidates.stream().filter(n -> !avoid.contains(n.getId())).toList();
            List<NodeInfo> pool = preferred.isEmpty() ? candidates : preferred;
            if (pool.isEmpty()) {
                return env.self();
            }
            Map<Integer, Integer> load = currentLoad();
            return pool.stream()
                    .min(Comparator.comparingInt((NodeInfo n) -> load.getOrDefault(n.getId(), 0))
                            .thenComparingInt(NodeInfo::getId))
                    .orElse(env.self());
        }

        /** Cancels and forgets every running attempt of {@code st}. */
        private void cancelAttempts(SubtaskState st) {
            st.running.values().forEach(this::cancel);
            st.running.clear();
        }

        private void cancel(Attempt attempt) {
            if (attempt.handle != null) {
                attempt.handle.cancel();
            }
        }

        private boolean allDone() {
            return Arrays.stream(subtasks).allMatch(st -> st.state == TaskState.DONE);
        }

        private void finish() {
            List<String> partials = Arrays.stream(subtasks).map(st -> st.result).toList();
            try {
                result = job.combine(partials);
                phase = ReplicatedJob.Phase.COMPLETED;
                event("Completed: result = " + result);
            } catch (RuntimeException e) {
                phase = ReplicatedJob.Phase.FAILED;
                error = "Could not combine partial results: " + e.getMessage();
                event(error);
            }
            finishedAtMs = System.currentTimeMillis();
            replicateFinal();
        }

        private void fail(String message) {
            terminate(ReplicatedJob.Phase.FAILED, message, "Job failed: ");
        }

        /** Cancels every running attempt and ends the job in {@code finalPhase} with {@code reason}. */
        private void terminate(ReplicatedJob.Phase finalPhase, String reason, String eventPrefix) {
            for (SubtaskState st : subtasks) {
                cancelAttempts(st);
            }
            phase = finalPhase;
            error = reason;
            finishedAtMs = System.currentTimeMillis();
            event(eventPrefix + reason);
            replicateFinal();
        }

        private void replicateFinal() {
            ReplicatedJob delta = delta();
            delta.finish(phase, result, error, finishedAtMs);
            env.replicate(delta);
        }

        private ReplicatedJob delta() {
            return new ReplicatedJob(job.id(), null, owner, phase, ++replicationVersion);
        }

        private void event(String message) {
            events.addLast(LocalTime.now().format(TIME) + "  " + message);
            while (events.size() > MAX_EVENTS) {
                events.removeFirst();
            }
            LOG.info("[" + job.id() + "] " + message);
        }

        /** Adds an event from outside (e.g. "recovered after coordinator failure"). */
        synchronized void note(String message) {
            event(message);
            changed();
        }

        private void changed() {
            changeVersion++;
        }

        /** Current progress, including the per-worker breakdown, for the client. */
        synchronized JobProgress snapshot() {
            long unitsDone = 0;
            int done = 0;
            Map<Integer, WorkerProgress.Builder> perWorker = new TreeMap<>();
            for (SubtaskState st : subtasks) {
                long units = st.subtask.units();
                if (st.state == TaskState.DONE) {
                    done++;
                    unitsDone += units;
                    WorkerProgress.Builder w = worker(perWorker, st.doneBy);
                    w.setSubtasksAssigned(w.getSubtasksAssigned() + 1)
                            .setSubtasksDone(w.getSubtasksDone() + 1)
                            .setUnitsDone(w.getUnitsDone() + units)
                            .setUnitsAssigned(w.getUnitsAssigned() + units);
                } else if (st.state == TaskState.RUNNING) {
                    long best = 0;
                    for (Attempt attempt : st.running.values()) {
                        best = Math.max(best, Math.min(attempt.unitsDone, units));
                        WorkerProgress.Builder w = worker(perWorker, attempt.worker.getId());
                        w.setSubtasksAssigned(w.getSubtasksAssigned() + 1)
                                .setSubtasksRunning(w.getSubtasksRunning() + 1)
                                .setUnitsDone(w.getUnitsDone() + Math.min(attempt.unitsDone, units))
                                .setUnitsAssigned(w.getUnitsAssigned() + units);
                    }
                    unitsDone += best;
                }
            }
            double percent = phase == ReplicatedJob.Phase.COMPLETED ? 100.0
                    : totalUnits == 0 ? 0.0 : Math.min(100.0, 100.0 * unitsDone / totalUnits);
            long end = phase.isFinished() ? finishedAtMs : System.currentTimeMillis();
            JobProgress.Builder progress = JobProgress.newBuilder()
                    .setJobId(job.id())
                    .setType(Protos.toProto(job.kind()))
                    .setStatus(ReplicationMapper.toProto(phase))
                    .setPercent(percent)
                    .setUnitsDone(phase == ReplicatedJob.Phase.COMPLETED ? totalUnits : unitsDone)
                    .setUnitsTotal(totalUnits)
                    .setSubtasksDone(done)
                    .setSubtasksTotal(subtasks.length)
                    .setResult(result)
                    .setError(error)
                    .setOwner(ownerNode(owner))
                    .setTerm(owner.term())
                    .setElapsedMs(Math.max(0, end - spec.submittedAtMs()))
                    .addAllEvents(events);
            perWorker.forEach((id, w) -> progress.addWorkers(w.setHealth(env.healthOf(id).toProto())));
            return progress.build();
        }

        private static WorkerProgress.Builder worker(Map<Integer, WorkerProgress.Builder> map, int id) {
            return map.computeIfAbsent(id, k -> WorkerProgress.newBuilder().setWorkerId(k));
        }
    }

    /**
     * Coordinator-side job management.
     *
     * <ul>
     *   <li><b>Admission</b>: only the coordinator accepts jobs, at most {@code maxJobsPerTerm}
     *       (5) per term; the 5th job closes the term and triggers a new election.</li>
     *   <li><b>Idempotency</b>: jobs are identified by a client-generated id; submitting the
     *       same id again never runs the job twice.</li>
     *   <li><b>Write-ahead replication</b>: a job is replicated to other workers before the
     *       client is told it was accepted.</li>
     *   <li><b>Recovery</b>: when a coordinator (or any job owner) fails, the current
     *       coordinator <i>adopts</i> its unfinished jobs from the replicas and only redoes
     *       the subtasks that had not completed.</li>
     *   <li><b>Progress</b>: clients watching a job get a snapshot pushed every 250 ms
     *       whenever something changed.</li>
     * </ul>
     */
    public static final class JobManager implements JobEnvironment, MembershipTracker.Listener {

        private static final Log LOG = Log.get("COORDINATOR");
        private static final long PUBLISH_EVERY_MS = 250;
        private static final long MAINTENANCE_EVERY_MS = 2000;
        private static final long KEEP_FINISHED_MS = 10 * 60_000;

        private final ClusterState state;
        private final MembershipTracker tracker;
        private final ReplicaStore replicas;
        private final ReplicationManager replication;
        private final ChannelPool pool;
        private final WorkerSettings settings;
        private final ScheduledExecutorService scheduler;
        private final ExecutorService executor;
        private final JacPolicy jacPolicy;
        private final Predicate<NodeInfo> pinger;
        private final Supplier<WorkerSummary> selfSummary;
        private final Runnable onTermLimit;

        private final Map<String, JobRun> runs = new ConcurrentHashMap<>();
        private final Map<String, List<Watcher>> watchers = new ConcurrentHashMap<>();
        private final Object termLock = new Object();
        private final Object adoptLock = new Object();
        private int counterTerm = -1;
        private int jobsInTerm;
        private boolean termClosed;

        public JobManager(ClusterState state, MembershipTracker tracker, ReplicaStore replicas,
                          ReplicationManager replication, ChannelPool pool, WorkerSettings settings,
                          ScheduledExecutorService scheduler, ExecutorService executor, Predicate<NodeInfo> pinger,
                          Supplier<WorkerSummary> selfSummary, Runnable onTermLimit) {
            this.state = state;
            this.tracker = tracker;
            this.replicas = replicas;
            this.replication = replication;
            this.pool = pool;
            this.settings = settings;
            this.scheduler = scheduler;
            this.executor = executor;
            this.pinger = pinger;
            this.selfSummary = selfSummary;
            this.onTermLimit = onTermLimit;
            this.jacPolicy = JacPolicy.create(settings.jacCounting(), state);
        }

        public void start() {
            scheduler.scheduleWithFixedDelay(this::publishProgress, PUBLISH_EVERY_MS, PUBLISH_EVERY_MS, TimeUnit.MILLISECONDS);
            scheduler.scheduleWithFixedDelay(this::maintenance, MAINTENANCE_EVERY_MS, MAINTENANCE_EVERY_MS,
                    TimeUnit.MILLISECONDS);
        }

        // =========================================================== admission

        public JobAccepted submit(JobRequest request) throws JobRejectedException {
            String jobId = request.getJobId().trim();
            if (jobId.isEmpty()) {
                throw new JobRejectedException(Status.Code.INVALID_ARGUMENT, "job_id is required");
            }
            JobRun existing = runs.get(jobId);
            if (existing != null) {
                return accepted(existing, 0);
            }
            Optional<ReplicatedJob> known = replicas.get(jobId);
            if (known.isPresent()) {
                return acceptedFromReplica(known.get());
            }
            if (!state.isCoordinator()) {
                throw notCoordinator();
            }
            JobKind kind;
            try {
                kind = Protos.fromProto(request.getType());
            } catch (IllegalArgumentException e) {
                throw new JobRejectedException(Status.Code.INVALID_ARGUMENT, "Unknown job type " + request.getType());
            }
            long[] numbers = Protos.toArray(request.getNumbersList());
            Job job = JobFactory.create(jobId, kind, numbers, request.getRangeStart(), request.getRangeEnd());
            try {
                job.validate();
            } catch (InvalidJobException e) {
                throw new JobRejectedException(Status.Code.INVALID_ARGUMENT, e.getMessage());
            }

            int number;
            int term;
            boolean lastOfTerm;
            synchronized (termLock) {
                syncTermCounter();
                if (!state.isCoordinator()) {
                    throw notCoordinator();
                }
                if (termClosed) {
                    throw new JobRejectedException(Status.Code.FAILED_PRECONDITION, "Term " + counterTerm + " has ended ("
                            + settings.maxJobsPerTerm() + "/" + settings.maxJobsPerTerm()
                            + " jobs assigned); a new coordinator is being elected - retry shortly");
                }
                number = ++jobsInTerm;
                lastOfTerm = number >= settings.maxJobsPerTerm();
                termClosed = lastOfTerm;
                term = counterTerm;
            }

            List<NodeInfo> workers = assignableWorkers();
            int subtaskCount = Math.max(1, Math.min(job.maxUsefulSubtasks(), workers.size() * settings.subtasksPerWorker()));
            ReplicatedJob.Spec spec = new ReplicatedJob.Spec(jobId, request.getClientId(), kind, numbers,
                    request.getRangeStart(), request.getRangeEnd(), subtaskCount, System.currentTimeMillis());
            ReplicatedJob.Owner me = selfOwner(term);
            // Register the run locally BEFORE replicating, so the periodic orphan check can
            // never mistake this brand-new job for an orphan and adopt it a second time.
            JobRun run = new JobRun(job, spec, me, this, Map.of(), settings.maxAttemptsPerSubtask(),
                    settings.maxInFlightPerWorker(), 1);
            runs.put(jobId, run);
            replication.replicate(new ReplicatedJob(jobId, spec, me, ReplicatedJob.Phase.RUNNING, 1), true);
            LOG.info("Accepted job " + jobId + ": " + job.describe() + " from client " + request.getClientId()
                    + " - job " + number + "/" + settings.maxJobsPerTerm() + " of term " + term);
            updateTracker();
            run.start(workers);
            jacPolicy.onJobDistributed(run.usedOtherWorkers());
            LOG.info("JAC of Worker-" + state.selfId() + " is now " + state.jac());

            if (lastOfTerm) {
                LOG.info("Term " + term + " reached its limit of " + settings.maxJobsPerTerm()
                        + " jobs -> stepping down and starting a new election (running jobs continue)");
                executor.execute(onTermLimit);
            }
            return accepted(run, number);
        }

        private JobRejectedException notCoordinator() {
            String hint = state.coordinator().getKnown()
                    ? " Current coordinator: " + Protos.label(state.coordinator().getNode())
                    : " No coordinator is known right now (election in progress).";
            return new JobRejectedException(Status.Code.FAILED_PRECONDITION,
                    "Worker-" + state.selfId() + " is not the coordinator." + hint);
        }

        private void syncTermCounter() {
            int term = state.term();
            if (term != counterTerm) {
                counterTerm = term;
                jobsInTerm = 0;
                termClosed = false;
            }
        }

        public int jobsInTerm() {
            synchronized (termLock) {
                syncTermCounter();
                return state.isCoordinator() ? jobsInTerm : 0;
            }
        }

        private JobAccepted accepted(JobRun run, int number) {
            return JobAccepted.newBuilder()
                    .setJobId(run.jobId())
                    .setOwner(state.self())
                    .setTerm(state.term())
                    .setSubtaskCount(run.subtaskCount())
                    .setJobNumberInTerm(number)
                    .build();
        }

        private JobAccepted acceptedFromReplica(ReplicatedJob job) {
            if (!job.phase().isFinished()) {
                adoptIfOrphaned(job);
            }
            JobRun run = runs.get(job.jobId());
            if (run != null) {
                return accepted(run, 0);
            }
            JobAccepted.Builder reply = JobAccepted.newBuilder().setJobId(job.jobId()).setTerm(state.term());
            if (job.owner() != null) {
                reply.setOwner(ownerNode(job.owner()));
            }
            if (job.spec() != null) {
                reply.setSubtaskCount(job.spec().subtaskCount());
            }
            return reply.build();
        }

        private ReplicatedJob.Owner selfOwner(int term) {
            NodeInfo self = state.self();
            return new ReplicatedJob.Owner(self.getId(), self.getHost(), self.getPort(), term);
        }

        // =========================================================== recovery

        /** Called when this worker becomes coordinator: pull replicas, then adopt orphaned jobs. */
        public void onBecameCoordinator() {
            executor.execute(() -> {
                replication.fetchFromAll();
                adoptOrphans();
            });
        }

        /** Adopts every unfinished job whose owner is no longer alive. */
        public void adoptOrphans() {
            if (!state.isCoordinator()) {
                return;
            }
            for (ReplicatedJob job : replicas.unfinished()) {
                adoptIfOrphaned(job);
            }
        }

        private void adoptIfOrphaned(ReplicatedJob job) {
            synchronized (adoptLock) {
                if (!state.isCoordinator() || runs.containsKey(job.jobId())) {
                    return;
                }
                ReplicatedJob.Owner owner = job.owner();
                if (owner != null && owner.id() != state.selfId() && ownerAlive(owner)) {
                    return;
                }
                adopt(job, owner);
            }
        }

        private boolean ownerAlive(ReplicatedJob.Owner owner) {
            Health health = tracker.health(owner.id());
            if (health == Health.ALIVE) {
                return true;
            }
            return health != Health.DEAD && pinger.test(ownerNode(owner));
        }

        private void adopt(ReplicatedJob job, ReplicatedJob.Owner previous) {
            ReplicatedJob.Spec spec = job.spec();
            if (spec == null) {
                LOG.warn("Cannot recover job " + job.jobId() + ": no replica holds its input data");
                return;
            }
            Job domainJob = JobFactory.create(spec.jobId(), spec.kind(), spec.numbers(), spec.rangeStart(), spec.rangeEnd());
            ReplicatedJob.Owner me = selfOwner(state.term());
            ReplicatedJob takeover = job.copy();
            takeover.setOwner(me);
            takeover.setPhase(ReplicatedJob.Phase.RUNNING);
            takeover.setVersion(job.version() + 1);
            replication.replicate(takeover, true);

            Map<Integer, ReplicatedJob.Part> done = job.parts();
            JobRun run = new JobRun(domainJob, spec, me, this, done, settings.maxAttemptsPerSubtask(),
                    settings.maxInFlightPerWorker(), job.version() + 1);
            runs.put(job.jobId(), run);
            String from = previous == null ? "an unknown owner" : "Worker-" + previous.id();
            run.note("RECOVERED by Worker-" + state.selfId() + " (term " + state.term() + ") after " + from
                    + " failed: " + done.size() + "/" + spec.subtaskCount() + " subtasks were already complete and are"
                    + " reused; " + (spec.subtaskCount() - done.size()) + " will be recomputed");
            updateTracker();
            run.start(assignableWorkers());
        }

        @Override
        public void onWorkerDead(NodeInfo worker) {
            for (JobRun run : runs.values()) {
                run.workerDead(worker.getId());
            }
            executor.execute(this::adoptOrphans);
        }

        @Override
        public void onWorkerAlive(NodeInfo worker) {
            // nothing to do: the worker is simply available for new subtasks again
        }

        // =========================================================== maintenance

        private void maintenance() {
            try {
                long now = System.currentTimeMillis();
                for (JobRun run : runs.values()) {
                    run.checkStalls(now, settings.stallTimeoutMs());
                }
                adoptOrphans();
                updateTracker();
                runs.values().removeIf(r -> r.isFinished() && now - r.finishedAtMs() > KEEP_FINISHED_MS
                        && !watchers.containsKey(r.jobId()));
            } catch (RuntimeException e) {
                LOG.warn("Job maintenance failed", e);
            }
        }

        /** Heartbeat monitoring runs while this worker is coordinator or still owns running jobs. */
        public void updateTracker() {
            boolean needed = state.isCoordinator() || hasActiveJobs();
            if (needed && !tracker.isActive()) {
                tracker.activate();
            } else if (!needed && tracker.isActive()) {
                tracker.deactivate();
            }
        }

        public boolean hasActiveJobs() {
            return runs.values().stream().anyMatch(r -> !r.isFinished());
        }

        // =========================================================== progress streaming

        /** One client stream watching a job. */
        private static final class Watcher {
            final ServerCallStreamObserver<JobProgress> stream;
            long sentVersion = -1;
            boolean closed;

            Watcher(ServerCallStreamObserver<JobProgress> stream) {
                this.stream = stream;
            }

            synchronized void send(JobProgress progress, long version, boolean last) {
                if (closed || stream.isCancelled()) {
                    closed = true;
                    return;
                }
                try {
                    stream.onNext(progress);
                    sentVersion = version;
                    if (last) {
                        stream.onCompleted();
                        closed = true;
                    }
                } catch (RuntimeException e) {
                    closed = true;
                }
            }
        }

        public void watch(String jobId, StreamObserver<JobProgress> observer) {
            ServerCallStreamObserver<JobProgress> stream = (ServerCallStreamObserver<JobProgress>) observer;
            JobRun run = runs.get(jobId);
            if (run != null) {
                Watcher watcher = new Watcher(stream);
                List<Watcher> list = watchers.computeIfAbsent(jobId, k -> new CopyOnWriteArrayList<>());
                list.add(watcher);
                stream.setOnCancelHandler(() -> list.remove(watcher));
                watcher.send(run.snapshot(), run.changeVersion(), run.isFinished());
                return;
            }
            Optional<ReplicatedJob> replica = replicas.get(jobId);
            if (replica.isEmpty()) {
                stream.onError(Status.NOT_FOUND.withDescription("Unknown job " + jobId).asRuntimeException());
            } else if (replica.get().phase().isFinished()) {
                reply(stream, finalSnapshot(replica.get()));
            } else {
                ReplicatedJob.Owner owner = replica.get().owner();
                stream.onError(Status.FAILED_PRECONDITION.withDescription("Job " + jobId + " is owned by "
                        + (owner == null ? "an unknown worker" : "Worker-" + owner.id())).asRuntimeException());
            }
        }

        private void publishProgress() {
            try {
                for (Map.Entry<String, List<Watcher>> entry : watchers.entrySet()) {
                    JobRun run = runs.get(entry.getKey());
                    List<Watcher> list = entry.getValue();
                    list.removeIf(w -> w.closed);
                    if (run == null || list.isEmpty()) {
                        if (list.isEmpty()) {
                            watchers.remove(entry.getKey(), list);
                        }
                        continue;
                    }
                    long version = run.changeVersion();
                    boolean finished = run.isFinished();
                    JobProgress snapshot = null;
                    for (Watcher watcher : list) {
                        if (watcher.sentVersion != version || finished) {
                            if (snapshot == null) {
                                snapshot = run.snapshot();
                            }
                            watcher.send(snapshot, version, finished);
                        }
                    }
                }
            } catch (RuntimeException e) {
                LOG.warn("Progress publishing failed", e);
            }
        }

        private JobProgress finalSnapshot(ReplicatedJob job) {
            JobProgress.Builder progress = JobProgress.newBuilder()
                    .setJobId(job.jobId())
                    .setStatus(ReplicationMapper.toProto(job.phase()))
                    .setPercent(job.phase() == ReplicatedJob.Phase.COMPLETED ? 100.0 : 0.0)
                    .setResult(job.result())
                    .setError(job.error())
                    .setSubtasksDone(job.parts().size());
            if (job.spec() != null) {
                progress.setType(Protos.toProto(job.spec().kind()))
                        .setSubtasksTotal(job.spec().subtaskCount())
                        .setElapsedMs(Math.max(0, job.finishedAtMs() - job.spec().submittedAtMs()));
            }
            if (job.owner() != null) {
                progress.setOwner(ownerNode(job.owner()))
                        .setTerm(job.owner().term());
            }
            return progress.build();
        }

        // =========================================================== queries

        public JobLocation locate(String jobId) {
            JobRun run = runs.get(jobId);
            if (run != null) {
                return JobLocation.newBuilder().setFound(true).setOwner(state.self())
                        .setStatus(ReplicationMapper.toProto(run.phase())).build();
            }
            return replicas.get(jobId).map(job -> {
                JobLocation.Builder location = JobLocation.newBuilder().setFound(true)
                        .setStatus(ReplicationMapper.toProto(job.phase())).setResult(job.result());
                if (job.owner() != null) {
                    location.setOwner(ownerNode(job.owner()));
                }
                return location.build();
            }).orElse(JobLocation.newBuilder().setFound(false).build());
        }

        public void cancel(String jobId) throws JobRejectedException {
            JobRun run = runs.get(jobId);
            if (run == null) {
                throw new JobRejectedException(Status.Code.FAILED_PRECONDITION, "Job " + jobId + " is not owned by Worker-"
                        + state.selfId());
            }
            run.cancel("cancelled by the client");
        }

        public ClusterStatus clusterStatus() {
            ClusterStatus.Builder status = ClusterStatus.newBuilder()
                    .setCoordinator(state.coordinator())
                    .setJobsInTerm(jobsInTerm())
                    .setMaxJobsPerTerm(settings.maxJobsPerTerm())
                    .setRunningJobs((int) runs.values().stream().filter(r -> !r.isFinished()).count());
            int coordinatorId = state.coordinator().getKnown() ? state.coordinator().getNode().getId() : -1;
            if (tracker.isActive()) {
                for (MembershipTracker.Member member : tracker.members()) {
                    if (member.node().getId() == state.selfId()) {
                        status.addWorkers(selfSummary.get());
                        continue;
                    }
                    WorkerSummary.Builder summary = WorkerSummary.newBuilder()
                            .setNode(member.node())
                            .setHealth(member.health().toProto())
                            .setIsCoordinator(member.node().getId() == coordinatorId);
                    if (member.lastReply() != null) {
                        summary.setJac(member.lastReply().getJac())
                                .setActiveSubtasks(member.lastReply().getActiveSubtasks())
                                .addAllNeighbourIds(member.lastReply().getNeighbourIdsList());
                    }
                    status.addWorkers(summary);
                }
            } else {
                status.addWorkers(selfSummary.get());
            }
            return status.build();
        }

        // =========================================================== JobEnvironment

        @Override
        public NodeInfo self() {
            return state.self();
        }

        @Override
        public List<NodeInfo> assignableWorkers() {
            return tracker.isActive() ? tracker.aliveWorkers() : List.of(state.self());
        }

        @Override
        public Health healthOf(int workerId) {
            return tracker.health(workerId);
        }

        @Override
        public void suspect(NodeInfo worker, String reason) {
            tracker.suspect(worker, reason);
        }

        @Override
        public void onSubtaskDispatched(NodeInfo worker) {
            jacPolicy.onSubtaskDispatched(worker);
        }

        @Override
        public void replicate(ReplicatedJob delta) {
            executor.execute(() -> replication.replicate(delta, false));
        }

        @Override
        public DispatchHandle dispatch(NodeInfo worker, String jobId, Subtask subtask, int attempt,
                                       DispatchCallbacks callbacks) {
            SubtaskRequest request = SubtaskRequest.newBuilder()
                    .setJobId(jobId)
                    .setIndex(subtask.index())
                    .setAttempt(attempt)
                    .setType(Protos.toProto(subtask.kind()))
                    .addAllNumbers(Protos.asIterable(subtask.numbers()))
                    .setRangeStart(subtask.rangeStart())
                    .setRangeEnd(subtask.rangeEnd())
                    .setCoordinator(state.self())
                    .build();
            AtomicReference<ClientCallStreamObserver<SubtaskRequest>> call = new AtomicReference<>();
            // Start the call in the ROOT context: when a subtask is dispatched while handling a
            // client's SubmitJob request, it must not inherit that request's context, otherwise
            // gRPC cancels the subtask as soon as the SubmitJob response has been sent.
            Context.ROOT.run(() -> TaskServiceGrpc.newStub(pool.get(worker)).execute(request,
                    new ClientResponseObserver<SubtaskRequest, SubtaskUpdate>() {
                        @Override
                        public void beforeStart(ClientCallStreamObserver<SubtaskRequest> requestStream) {
                            call.set(requestStream);
                        }

                        @Override
                        public void onNext(SubtaskUpdate update) {
                            if (update.hasResult()) {
                                callbacks.onResult(update.getResult().getValue());
                            } else if (update.hasProgress()) {
                                callbacks.onProgress(update.getProgress().getUnitsDone());
                            }
                        }

                        @Override
                        public void onError(Throwable error) {
                            callbacks.onFailure(error);
                        }

                        @Override
                        public void onCompleted() {
                            callbacks.onCompleted();
                        }
                    }));
            return () -> {
                ClientCallStreamObserver<SubtaskRequest> stream = call.get();
                if (stream != null) {
                    stream.cancel("cancelled by coordinator", null);
                }
            };
        }
    }

    /** Client-facing API, served by every worker (only the coordinator accepts new jobs). */
    public static final class JobServiceImpl extends JobServiceGrpc.JobServiceImplBase {

        private final JobManager jobs;
        private final ClusterState state;

        public JobServiceImpl(JobManager jobs, ClusterState state) {
            this.jobs = jobs;
            this.state = state;
        }

        @Override
        public void submitJob(JobRequest request, StreamObserver<JobAccepted> response) {
            try {
                reply(response, jobs.submit(request));
            } catch (JobRejectedException e) {
                response.onError(e.toStatus().asRuntimeException());
            }
        }

        @Override
        public void watchJob(WatchRequest request, StreamObserver<JobProgress> response) {
            jobs.watch(request.getJobId(), response);
        }

        @Override
        public void locateJob(WatchRequest request, StreamObserver<JobLocation> response) {
            reply(response, jobs.locate(request.getJobId()));
        }

        @Override
        public void whoIsCoordinator(Empty request, StreamObserver<CoordinatorInfo> response) {
            reply(response, state.coordinator());
        }

        @Override
        public void getClusterStatus(Empty request, StreamObserver<ClusterStatus> response) {
            reply(response, jobs.clusterStatus());
        }

        @Override
        public void cancelJob(WatchRequest request, StreamObserver<Ack> response) {
            try {
                jobs.cancel(request.getJobId());
                reply(response, Ack.newBuilder().setOk(true).build());
            } catch (JobRejectedException e) {
                response.onError(e.toStatus().asRuntimeException());
            }
        }
    }
}
