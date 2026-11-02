package worker;

import common.Jobs.*;
import common.Log;
import common.Net.Protos;
import io.grpc.Status;
import io.grpc.stub.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import proto.*;

/**
 * Executes subtasks sent by a coordinator.
 *
 * <p>Several subtasks (from the same or different jobs and clients) run concurrently on a
 * fixed pool of compute threads. While a subtask runs, the worker streams real progress
 * (units processed so far) back to the coordinator; a keep-alive update is also sent
 * every {@value #KEEPALIVE_MS} ms so the coordinator can tell "working" from "stuck".
 * Cancelled calls (e.g. a duplicate attempt that lost the race) stop computing early.
 */
public final class TaskServiceImpl extends TaskServiceGrpc.TaskServiceImplBase {

    private static final Log LOG = Log.get("TASK");
    private static final long PROGRESS_EVERY_MS = 150;
    private static final long KEEPALIVE_MS = 2000;

    private final ExecutorService computePool;
    private final ScheduledExecutorService scheduler;
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicLong completed = new AtomicLong();

    public TaskServiceImpl(int threads, ScheduledExecutorService scheduler) {
        this.computePool = Executors.newFixedThreadPool(threads, computeThreadFactory());
        this.scheduler = scheduler;
    }

    private static ThreadFactory computeThreadFactory() {
        ThreadFactory daemonThreads = Worker.daemonThreads("compute");
        return runnable -> {
            Thread thread = daemonThreads.newThread(runnable);
            // Lower priority so heartbeats and RPCs stay responsive under heavy computation.
            thread.setPriority(Thread.NORM_PRIORITY - 1);
            return thread;
        };
    }

    public int activeSubtasks() {
        return active.get();
    }

    public long completedSubtasks() {
        return completed.get();
    }

    @Override
    public void execute(SubtaskRequest request, StreamObserver<SubtaskUpdate> responseObserver) {
        ServerCallStreamObserver<SubtaskUpdate> stream = (ServerCallStreamObserver<SubtaskUpdate>) responseObserver;
        AtomicBoolean cancelled = new AtomicBoolean(false);
        stream.setOnCancelHandler(() -> cancelled.set(true));

        Subtask subtask;
        try {
            subtask = SubtaskFactory.create(Protos.fromProto(request.getType()), request.getIndex(),
                    Protos.toArray(request.getNumbersList()), request.getRangeStart(), request.getRangeEnd());
        } catch (IllegalArgumentException e) {
            stream.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        String taskId = request.getJobId() + "#" + request.getIndex() + " (attempt " + request.getAttempt() + ")";
        ProgressStream progress = new ProgressStream(stream, cancelled, subtask.units());
        ScheduledFuture<?> keepAlive = scheduler.scheduleAtFixedRate(progress::sendLatest,
                KEEPALIVE_MS, KEEPALIVE_MS, TimeUnit.MILLISECONDS);
        progress.sendLatest(); // tells the coordinator the subtask was received (0%)

        computePool.execute(() -> {
            active.incrementAndGet();
            long started = System.currentTimeMillis();
            LOG.info("Running subtask " + taskId + ": " + subtask.kind() + ", "
                    + String.format("%,d", subtask.units()) + " units (from coordinator Worker-"
                    + request.getCoordinator().getId() + ")");
            try {
                String value = subtask.execute(progress::report, cancelled::get);
                long millis = System.currentTimeMillis() - started;
                keepAlive.cancel(false);
                progress.finish(SubtaskResult.newBuilder()
                        .setValue(value).setUnits(subtask.units()).setComputeMs(millis).build());
                completed.incrementAndGet();
                LOG.info("Finished subtask " + taskId + " in " + millis + " ms -> " + abbreviate(value));
            } catch (TaskCancelledException e) {
                LOG.info("Subtask " + taskId + " cancelled by the coordinator");
            } catch (RuntimeException e) {
                LOG.warn("Subtask " + taskId + " failed", e);
                progress.fail(Status.INTERNAL.withDescription("Subtask failed: " + e.getMessage()));
            } finally {
                keepAlive.cancel(false);
                active.decrementAndGet();
            }
        });
    }

    private static String abbreviate(String value) {
        return value.length() > 30 ? value.substring(0, 27) + "..." : value;
    }

    public void shutdown() {
        computePool.shutdownNow();
    }

    /** Thread-safe wrapper that throttles progress messages on one response stream. */
    private static final class ProgressStream {
        private final ServerCallStreamObserver<SubtaskUpdate> stream;
        private final AtomicBoolean cancelled;
        private final long total;
        private volatile long done;
        private long lastSentMs;
        private boolean closed;

        ProgressStream(ServerCallStreamObserver<SubtaskUpdate> stream, AtomicBoolean cancelled, long total) {
            this.stream = stream;
            this.cancelled = cancelled;
            this.total = total;
        }

        void report(long unitsDone, long unitsTotal) {
            done = unitsDone;
            long now = System.currentTimeMillis();
            synchronized (this) {
                if (now - lastSentMs < PROGRESS_EVERY_MS) {
                    return;
                }
            }
            sendLatest();
        }

        synchronized void sendLatest() {
            if (closed || cancelled.get()) {
                return;
            }
            lastSentMs = System.currentTimeMillis();
            try {
                stream.onNext(SubtaskUpdate.newBuilder()
                        .setProgress(SubtaskProgress.newBuilder().setUnitsDone(done).setUnitsTotal(total))
                        .build());
            } catch (RuntimeException e) {
                closed = true; // the coordinator went away; the computation will be cancelled
                cancelled.set(true);
            }
        }

        void finish(SubtaskResult result) {
            close(() -> {
                stream.onNext(SubtaskUpdate.newBuilder().setResult(result).build());
                stream.onCompleted();
            });
        }

        void fail(Status status) {
            close(() -> stream.onError(status.asRuntimeException()));
        }

        /** Sends the final message(s) of the stream, at most once and only if it is still open. */
        private synchronized void close(Runnable sendFinal) {
            if (closed || cancelled.get()) {
                return;
            }
            closed = true;
            try {
                sendFinal.run();
            } catch (RuntimeException e) {
                // finish: coordinator disconnected after we finished; it will reassign if it needs to
                // fail: already closed
            }
        }
    }
}
