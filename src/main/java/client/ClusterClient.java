package client;

import common.Jobs.JobKind;
import common.Net.*;
import io.grpc.*;
import io.grpc.stub.*;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
import java.util.stream.*;
import proto.*;

/**
 * Client-side access to the cluster. The client only needs the Bootstrap Node's
 * address; it finds the coordinator from there (or, if the Bootstrap Node is down,
 * by asking the workers it already knows).
 */
public final class ClusterClient implements AutoCloseable {

    private final HostPort bootstrap;
    private final ChannelPool pool = new ChannelPool();
    private final long rpcTimeoutMs;
    private volatile List<NodeInfo> knownWorkers;
    private volatile CoordinatorInfo lastCoordinator = CoordinatorInfo.getDefaultInstance();

    public ClusterClient(HostPort bootstrap, long rpcTimeoutMs) {
        this(bootstrap, List.of(), rpcTimeoutMs);
    }

    /**
     * @param contacts worker addresses to try if the Bootstrap Node is down (optional)
     */
    public ClusterClient(HostPort bootstrap, List<HostPort> contacts, long rpcTimeoutMs) {
        this.bootstrap = bootstrap;
        this.rpcTimeoutMs = rpcTimeoutMs;
        this.knownWorkers = contacts.stream()
                .map(contact -> NodeInfo.newBuilder().setHost(contact.host()).setPort(contact.port()).build())
                .toList();
    }

    public HostPort bootstrapAddress() {
        return bootstrap;
    }

    private JobServiceGrpc.JobServiceBlockingStub jobs(NodeInfo node) {
        return JobServiceGrpc.newBlockingStub(pool.get(node)).withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS);
    }

    private BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrapService() {
        return BootstrapServiceGrpc.newBlockingStub(pool.get(bootstrap.host(), bootstrap.port()))
                .withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /** Calls one node's JobService; if that fails, drops the node's channel and returns empty. */
    private <T> Optional<T> ask(NodeInfo node, Function<JobServiceGrpc.JobServiceBlockingStub, T> call) {
        try {
            return Optional.of(call.apply(jobs(node)));
        } catch (StatusRuntimeException e) {
            pool.invalidate(node);
            return Optional.empty();
        }
    }

    private Optional<CoordinatorInfo> whoIsCoordinator(NodeInfo node) {
        return ask(node, stub -> stub.whoIsCoordinator(Empty.getDefaultInstance()));
    }

    private static WatchRequest watchRequest(String jobId) {
        return WatchRequest.newBuilder().setJobId(jobId).build();
    }

    /**
     * Workers registered at the Bootstrap Node. If the Bootstrap Node is down, the client
     * falls back to the workers it already knows (from earlier lookups, the cluster status,
     * or the configured contacts), so it keeps working.
     */
    public List<NodeInfo> workers() {
        try {
            List<NodeInfo> list = bootstrapService().listWorkers(Empty.getDefaultInstance()).getPeersList();
            if (!list.isEmpty()) {
                knownWorkers = list;
            }
            return list.isEmpty() ? knownWorkers : list;
        } catch (StatusRuntimeException e) {
            return knownWorkers;
        }
    }

    /** Finds the current coordinator and confirms it with the coordinator itself. */
    public Optional<CoordinatorInfo> findCoordinator() {
        try {
            Optional<CoordinatorInfo> confirmed = confirm(bootstrapService().getCoordinator(Empty.getDefaultInstance()));
            if (confirmed.isPresent()) {
                return confirmed;
            }
        } catch (StatusRuntimeException e) {
            // Bootstrap Node unavailable: ask the workers instead
        }
        // The highest term wins (the first one on a tie).
        return candidates().stream()
                .flatMap(worker -> whoIsCoordinator(worker).stream())
                .filter(CoordinatorInfo::getKnown)
                .max(Comparator.comparingLong(CoordinatorInfo::getTerm))
                .flatMap(this::confirm);
    }

    private List<NodeInfo> candidates() {
        List<NodeInfo> list = new ArrayList<>(workers());
        if (lastCoordinator.getKnown() && list.stream().noneMatch(n -> n.getId() == lastCoordinator.getNode().getId())) {
            list.add(lastCoordinator.getNode());
        }
        return list;
    }

    /** A node is the coordinator only if it says so itself. A confirmed coordinator is remembered. */
    private Optional<CoordinatorInfo> confirm(CoordinatorInfo info) {
        NodeInfo node = info.getNode();
        boolean confirmed = info.getKnown() && whoIsCoordinator(node)
                .filter(self -> self.getKnown() && self.getNode().getId() == node.getId())
                .isPresent();
        if (confirmed) {
            lastCoordinator = info;
        }
        return confirmed ? Optional.of(info) : Optional.empty();
    }

    public JobAccepted submit(NodeInfo coordinator, JobRequest request) {
        // Large jobs take a moment to replicate before they are accepted.
        return JobServiceGrpc.newBlockingStub(pool.get(coordinator))
                .withDeadlineAfter(Math.max(rpcTimeoutMs * 5, 15_000), TimeUnit.MILLISECONDS)
                .submitJob(request);
    }

    /** Asks the coordinator, then every other worker, who currently owns a job. */
    public Optional<JobLocation> locate(String jobId) {
        WatchRequest request = watchRequest(jobId);
        return Stream.concat(findCoordinator().map(CoordinatorInfo::getNode).stream(), candidates().stream())
                .flatMap(node -> ask(node, stub -> stub.locateJob(request)).stream())
                .filter(JobLocation::getFound)
                .findFirst();
    }

    /**
     * Opens the server-streaming progress feed of a job. Returns a handle that cancels
     * the stream.
     */
    public Runnable watch(NodeInfo owner, String jobId, Consumer<JobProgress> onProgress,
                          Consumer<Throwable> onError, Runnable onDone) {
        AtomicReference<ClientCallStreamObserver<WatchRequest>> call = new AtomicReference<>();
        JobServiceGrpc.newStub(pool.get(owner)).watchJob(watchRequest(jobId),
                new ClientResponseObserver<WatchRequest, JobProgress>() {
                    @Override
                    public void beforeStart(ClientCallStreamObserver<WatchRequest> requestStream) {
                        call.set(requestStream);
                    }

                    @Override
                    public void onNext(JobProgress value) {
                        onProgress.accept(value);
                    }

                    @Override
                    public void onError(Throwable error) {
                        onError.accept(error);
                    }

                    @Override
                    public void onCompleted() {
                        onDone.run();
                    }
                });
        return () -> {
            ClientCallStreamObserver<WatchRequest> stream = call.get();
            if (stream != null) {
                stream.cancel("client stopped watching", null);
            }
        };
    }

    public void cancel(NodeInfo owner, String jobId) {
        jobs(owner).cancelJob(watchRequest(jobId));
    }

    public Optional<ClusterStatus> clusterStatus() {
        Optional<CoordinatorInfo> coordinator = findCoordinator();
        if (coordinator.isEmpty()) {
            return Optional.empty();
        }
        try {
            ClusterStatus status = jobs(coordinator.get().getNode()).getClusterStatus(Empty.getDefaultInstance());
            List<NodeInfo> seen = status.getWorkersList().stream().map(WorkerSummary::getNode).toList();
            if (!seen.isEmpty()) {
                knownWorkers = seen; // remembered in case the Bootstrap Node goes down
            }
            return Optional.of(status);
        } catch (StatusRuntimeException e) {
            return Optional.empty();
        }
    }

    public void forget(NodeInfo node) {
        pool.invalidate(node);
    }

    @Override
    public void close() {
        pool.close();
    }

    /**
     * Follows one job from submission to its result, surviving failures:
     * <ol>
     *   <li>find the coordinator and submit (retrying while an election is in progress);</li>
     *   <li>watch the job's progress stream on its owner;</li>
     *   <li>if the stream breaks (owner crashed) or goes silent, locate the job again - the
     *       new coordinator adopts it - and resume watching;</li>
     *   <li>if no worker knows the job at all, resubmit it with the SAME job id (the
     *       cluster deduplicates, so it can never run twice).</li>
     * </ol>
     */
    public static final class JobTracker implements Runnable {

        private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");
        private static final long SILENCE_TIMEOUT_MS = 15_000;
        private static final long SUBMIT_GIVE_UP_MS = 180_000;
        private static final long RETRY_DELAY_MS = 1500;
        private static final int NOT_FOUND_BEFORE_RESUBMIT = 4;

        private final ClusterClient client;
        private final JobRequest request;
        private final JobKind kind;
        private final String description;
        private final long submittedAtMs = System.currentTimeMillis();
        private final List<Consumer<JobView>> listeners = new CopyOnWriteArrayList<>();
        private final List<String> notes = new CopyOnWriteArrayList<>();

        private volatile ClientJobStatus status = ClientJobStatus.SUBMITTING;
        private volatile String message = "Looking for the coordinator...";
        private volatile JobProgress progress;
        private volatile long finishedAtMs;
        private volatile NodeInfo owner;
        private volatile boolean cancelRequested;

        public JobTracker(ClusterClient client, JobRequest request, JobKind kind, String description) {
            this.client = client;
            this.request = request;
            this.kind = kind;
            this.description = description;
        }

        public String jobId() {
            return request.getJobId();
        }

        public void addListener(Consumer<JobView> listener) {
            listeners.add(listener);
        }

        public void removeListener(Consumer<JobView> listener) {
            listeners.remove(listener);
        }

        public JobView view() {
            return new JobView(request.getJobId(), kind, description, status, message, progress,
                    List.copyOf(notes), submittedAtMs, finishedAtMs);
        }

        /** Asks the job's owner to cancel it. */
        public void requestCancel() {
            cancelRequested = true;
            NodeInfo current = owner;
            if (current != null) {
                try {
                    client.cancel(current, request.getJobId());
                } catch (StatusRuntimeException e) {
                    note("Cancel request failed: " + e.getStatus().getDescription());
                }
            }
        }

        @Override
        public void run() {
            try {
                owner = submit();
                int notFound = 0;
                while (owner != null && !status.isFinished()) {
                    String lost = watchUntilFinishedOrLost(owner);
                    if (lost == null || status.isFinished()) {
                        break;
                    }
                    update(ClientJobStatus.RECOVERING, "Lost contact with " + Protos.shortLabel(owner) + " (" + lost
                            + ") - locating the job");
                    note("Lost contact with job owner " + Protos.shortLabel(owner) + ": " + lost);
                    sleep(RETRY_DELAY_MS);
                    Optional<JobLocation> location = client.locate(request.getJobId());
                    if (location.isPresent()) {
                        notFound = 0;
                        JobLocation found = location.get();
                        if (found.getStatus() == JobStatus.COMPLETED) {
                            finishFromLocation(found);
                            break;
                        }
                        if (found.hasOwner() && found.getOwner().getId() != owner.getId()) {
                            note("Job is now owned by " + Protos.shortLabel(found.getOwner()));
                        }
                        if (found.hasOwner()) {
                            owner = found.getOwner();
                        }
                    } else if (++notFound >= NOT_FOUND_BEFORE_RESUBMIT) {
                        note("No worker knows this job any more - resubmitting it with the same job id");
                        owner = submit();
                        notFound = 0;
                    }
                }
            } catch (RuntimeException e) {
                failWithoutNote("Client error: " + e.getMessage());
            }
        }

        /** Submits until a coordinator accepts the job; returns its owner, or null if it was rejected. */
        private NodeInfo submit() {
            long giveUpAt = System.currentTimeMillis() + SUBMIT_GIVE_UP_MS;
            while (System.currentTimeMillis() < giveUpAt && !cancelRequested) {
                Optional<CoordinatorInfo> coordinator = client.findCoordinator();
                if (coordinator.isEmpty()) {
                    update(ClientJobStatus.WAITING, "No coordinator available (election in progress?) - retrying");
                    sleep(RETRY_DELAY_MS);
                    continue;
                }
                NodeInfo node = coordinator.get().getNode();
                try {
                    update(ClientJobStatus.SUBMITTING, "Submitting to coordinator " + Protos.shortLabel(node)
                            + " (term " + coordinator.get().getTerm() + ")");
                    JobAccepted accepted = client.submit(node, request);
                    NodeInfo jobOwner = accepted.hasOwner() ? accepted.getOwner() : node;
                    note("Accepted by " + Protos.shortLabel(node) + " (term " + accepted.getTerm() + ")"
                            + (accepted.getJobNumberInTerm() > 0 ? ", job " + accepted.getJobNumberInTerm()
                            + " of this term" : "") + ", " + accepted.getSubtaskCount() + " subtasks");
                    update(ClientJobStatus.RUNNING, "Running on the cluster");
                    return jobOwner;
                } catch (StatusRuntimeException e) {
                    Status.Code code = e.getStatus().getCode();
                    if (code == Status.Code.INVALID_ARGUMENT) {
                        failWithoutNote(e.getStatus().getDescription());
                        return null;
                    }
                    update(ClientJobStatus.WAITING, describe(e) + " - retrying");
                    if (code == Status.Code.UNAVAILABLE) {
                        client.forget(node); // the coordinator itself is unreachable
                    }
                    sleep(RETRY_DELAY_MS);
                }
            }
            failWithoutNote(cancelRequested ? "Cancelled before submission" : "Gave up: no coordinator accepted the job");
            return null;
        }

        /**
         * Watches the progress stream. Returns null when the job finished, otherwise a reason
         * why the stream was lost (owner unreachable, stream silent, job moved elsewhere).
         */
        private String watchUntilFinishedOrLost(NodeInfo node) {
            BlockingQueue<Object> events = new LinkedBlockingQueue<>();
            Object done = new Object();
            Runnable cancel = client.watch(node, request.getJobId(), events::add, events::add, () -> events.add(done));
            try {
                while (true) {
                    Object event = events.poll(SILENCE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    if (event == null) {
                        cancel.run();
                        return "no progress update for " + SILENCE_TIMEOUT_MS / 1000 + " s";
                    }
                    if (event instanceof JobProgress p) {
                        onProgress(p);
                        if (status.isFinished()) {
                            return null;
                        }
                    } else if (event instanceof Throwable error) {
                        return describe(error);
                    } else if (event == done) {
                        return status.isFinished() ? null : "stream closed";
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cancel.run();
                return "interrupted";
            }
        }

        private void onProgress(JobProgress p) {
            progress = p;
            switch (p.getStatus()) {
                case COMPLETED -> finish(ClientJobStatus.COMPLETED, "Result: " + p.getResult());
                case FAILED -> finish(ClientJobStatus.FAILED, p.getError());
                case CANCELLED -> finish(ClientJobStatus.CANCELLED, p.getError());
                default -> {
                    status = ClientJobStatus.RUNNING;
                    message = String.format("%.1f%% | %d/%d subtasks | owner Worker-%d (term %d)", p.getPercent(),
                            p.getSubtasksDone(), p.getSubtasksTotal(), p.getOwner().getId(), p.getTerm());
                    publish();
                }
            }
        }

        private void finishFromLocation(JobLocation location) {
            JobProgress.Builder p = progress == null ? JobProgress.newBuilder() : progress.toBuilder();
            progress = p.setJobId(request.getJobId()).setStatus(JobStatus.COMPLETED).setPercent(100)
                    .setResult(location.getResult()).build();
            finish(ClientJobStatus.COMPLETED, "Result: " + location.getResult());
        }

        private void finish(ClientJobStatus finalStatus, String text) {
            finishedAtMs = System.currentTimeMillis();
            status = finalStatus;
            message = text;
            note(finalStatus + ": " + text);
            publish();
        }

        /** Marks the job FAILED on the client side (rejected, gave up or client error), without adding a note. */
        private void failWithoutNote(String text) {
            update(ClientJobStatus.FAILED, text);
            finishedAtMs = System.currentTimeMillis();
            publish();
        }

        private static String describe(Throwable error) {
            Status status = Status.fromThrowable(error);
            String description = status.getDescription();
            return status.getCode() + (description == null || description.isBlank() ? "" : ": " + description);
        }

        private void update(ClientJobStatus newStatus, String text) {
            status = newStatus;
            message = text;
            publish();
        }

        private void note(String text) {
            notes.add(LocalTime.now().format(TIME) + "  " + text);
        }

        private void publish() {
            JobView view = view();
            for (Consumer<JobView> listener : listeners) {
                listener.accept(view);
            }
        }

        private static void sleep(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        /** Builds the gRPC request for a job. */
        public static JobRequest buildRequest(String jobId, String clientId, JobKind kind, long[] numbers,
                                              long start, long end) {
            return JobRequest.newBuilder()
                    .setJobId(jobId)
                    .setClientId(clientId)
                    .setType(Protos.toProto(kind))
                    .addAllNumbers(Protos.asIterable(numbers))
                    .setRangeStart(start)
                    .setRangeEnd(end)
                    .build();
        }
    }

    /**
     * Immutable snapshot of one tracked job for the GUI/CLI.
     *
     * @param progress last progress received from the cluster (null before the first update)
     * @param notes    client-side events, e.g. "lost contact with Worker-5, re-locating job"
     */
    public record JobView(String jobId, JobKind kind, String description, ClientJobStatus status, String message,
                          JobProgress progress, List<String> notes, long submittedAtMs, long finishedAtMs) {

        public double percent() {
            if (status == ClientJobStatus.COMPLETED) {
                return 100.0;
            }
            return progress == null ? 0.0 : progress.getPercent();
        }

        public String result() {
            return progress == null ? "" : progress.getResult();
        }

        public long elapsedMs() {
            long end = finishedAtMs > 0 ? finishedAtMs : System.currentTimeMillis();
            return end - submittedAtMs;
        }

        public String shortId() {
            return jobId.length() > 8 ? jobId.substring(0, 8) : jobId;
        }
    }

    /** Job status as seen by the client (includes client-side states such as SUBMITTING). */
    public enum ClientJobStatus {
        SUBMITTING, WAITING, RUNNING, RECOVERING, COMPLETED, FAILED, CANCELLED;

        public boolean isFinished() {
            return this == COMPLETED || this == FAILED || this == CANCELLED;
        }
    }
}
