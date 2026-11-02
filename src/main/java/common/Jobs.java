package common;

import common.Primes.BigSum;
import java.math.BigInteger;
import java.util.*;

/**
 * Job model shared by client, coordinator and workers: the job types (MAX, PRIMESUM,
 * PRIMECOUNT), how each is validated, split into subtasks and how partial results are combined.
 */
public final class Jobs {

    private Jobs() {
    }

    /** The job types the cluster supports. */
    public enum JobKind {
        MAX("Largest value in a list of numbers"),
        PRIMESUM("Sum of all primes in a range [start, end]"),
        PRIMECOUNT("How many values in a list are prime");

        private final String description;

        JobKind(String description) {
            this.description = description;
        }

        public String description() {
            return description;
        }

        /** True for jobs whose input is a list of numbers (MAX, PRIMECOUNT). */
        public boolean usesNumberList() {
            return this != PRIMESUM;
        }
    }

    /**
     * A computational job submitted by a client.
     *
     * <p>Each job type knows three things, implemented by its subclass:
     * <ol>
     *   <li>how to <b>split</b> itself into subtasks (as evenly as possible),</li>
     *   <li>how many <b>work units</b> it contains (used for real progress),</li>
     *   <li>how to <b>combine</b> the partial results of its subtasks.</li>
     * </ol>
     * The coordinator works only with this abstract type, so adding a new job type
     * does not require changing the scheduling, fault-tolerance or progress code.
     */
    public static abstract class Job {

        private final String id;
        private final JobKind kind;

        protected Job(String id, JobKind kind) {
            this.id = id;
            this.kind = kind;
        }

        public String id() {
            return id;
        }

        public JobKind kind() {
            return kind;
        }

        /** Rejects inputs the cluster cannot process (with a message for the user). */
        public abstract void validate() throws InvalidJobException;

        /** Total amount of work; progress = completed units / total units. */
        public abstract long totalUnits();

        /**
         * Splits the job into {@code count} subtasks whose sizes differ by at most one unit.
         * The split is deterministic: any coordinator splitting the same job with the same
         * count produces identical subtasks (needed for recovery after coordinator failure).
         */
        public abstract List<Subtask> split(int count);

        /** Combines the partial results of all subtasks into the final answer. */
        public abstract String combine(List<String> partialResults);

        /** Short description for logs and the GUI, e.g. "PRIMESUM [1 .. 1,000,000]". */
        public abstract String describe();

        /** Upper bound for the number of subtasks (cannot have more subtasks than units). */
        public int maxUsefulSubtasks() {
            return (int) Math.max(1, Math.min(totalUnits(), Integer.MAX_VALUE));
        }

        /**
         * Start offsets for splitting all {@link #totalUnits()} into {@code count} near-equal parts
         * (limited to 1 .. {@link #maxUsefulSubtasks()} parts; the number of parts is
         * {@code length - 1}). The first {@code total % parts} parts get one extra unit
         * (e.g. 10 into 4 = 3,3,2,2).
         */
        protected long[] evenBoundaries(int count) {
            int parts = Math.max(1, Math.min(count, maxUsefulSubtasks()));
            long total = totalUnits();
            long[] bounds = new long[parts + 1];
            long base = total / parts;
            long remainder = total % parts;
            for (int i = 0; i < parts; i++) {
                bounds[i + 1] = bounds[i] + base + (i < remainder ? 1 : 0);
            }
            return bounds;
        }
    }

    /** Base class for jobs whose input is a list of numbers (MAX and PRIMECOUNT). */
    public static abstract class ListJob extends Job {

        /** Hard limit so a single request cannot exhaust a worker's memory. */
        public static final int MAX_VALUES = 20_000_000;

        private final long[] numbers;

        protected ListJob(String id, JobKind kind, long[] numbers) {
            super(id, kind);
            this.numbers = numbers == null ? new long[0] : numbers;
        }

        @Override
        public void validate() throws InvalidJobException {
            if (numbers.length == 0) {
                throw new InvalidJobException(kind() + " needs at least one number.");
            }
            if (numbers.length > MAX_VALUES) {
                throw new InvalidJobException(kind() + " supports at most " + MAX_VALUES + " values (got "
                        + numbers.length + ").");
            }
        }

        @Override
        public long totalUnits() {
            return numbers.length;
        }

        @Override
        public List<Subtask> split(int count) {
            long[] bounds = evenBoundaries(count);
            int parts = bounds.length - 1;
            List<Subtask> subtasks = new ArrayList<>(parts);
            for (int i = 0; i < parts; i++) {
                long[] slice = Arrays.copyOfRange(numbers, (int) bounds[i], (int) bounds[i + 1]);
                subtasks.add(createSubtask(i, slice));
            }
            return subtasks;
        }

        /** Creates the subtask type matching this job (polymorphic factory method). */
        protected abstract Subtask createSubtask(int index, long[] slice);

        @Override
        public String describe() {
            return kind() + " of " + String.format("%,d", numbers.length) + " values";
        }
    }

    /** MAX(numbers): the largest value of an unsorted list. */
    public static class MaxJob extends ListJob {

        public MaxJob(String id, long[] numbers) {
            super(id, JobKind.MAX, numbers);
        }

        @Override
        protected Subtask createSubtask(int index, long[] slice) {
            return new MaxSubtask(index, slice);
        }

        @Override
        public String combine(List<String> partialResults) {
            return Long.toString(partialResults.stream().mapToLong(Long::parseLong).reduce(Long.MIN_VALUE, Math::max));
        }
    }

    /** PRIMECOUNT(numbers): how many values of an unsorted list are prime. */
    public static class PrimeCountJob extends ListJob {

        public PrimeCountJob(String id, long[] numbers) {
            super(id, JobKind.PRIMECOUNT, numbers);
        }

        @Override
        protected Subtask createSubtask(int index, long[] slice) {
            return new PrimeCountSubtask(index, slice);
        }

        @Override
        public String combine(List<String> partialResults) {
            return Long.toString(partialResults.stream().mapToLong(Long::parseLong).sum());
        }
    }

    /**
     * PRIMESUM(start, end): sum of all primes p with start &lt;= p &lt;= end.
     *
     * <p>The range is split into equal-length sub-ranges, e.g. PRIMESUM(1, 1000) on 4
     * subtasks = [1..250] [251..500] [501..750] [751..1000]. The result is exact for any
     * size (BigInteger), so very large ranges cannot overflow.
     */
    public static class PrimeSumJob extends Job {

        /** Largest supported end of range (10^15). Base primes up to sqrt(10^15) fit in memory. */
        public static final long MAX_END = 1_000_000_000_000_000L;

        private final long start;
        private final long end;

        public PrimeSumJob(String id, long start, long end) {
            super(id, JobKind.PRIMESUM);
            this.start = start;
            this.end = end;
        }

        /** Primes are positive, so negative parts of the range can be skipped. */
        private long effectiveStart() {
            return Math.max(start, 0);
        }

        @Override
        public void validate() throws InvalidJobException {
            if (start > end) {
                throw new InvalidJobException("PRIMESUM start (" + start + ") must be <= end (" + end + ").");
            }
            if (end > MAX_END) {
                throw new InvalidJobException(String.format(
                        "PRIMESUM end must be <= %,d (10^15); got %,d.", MAX_END, end));
            }
        }

        @Override
        public long totalUnits() {
            long lo = effectiveStart();
            return end >= lo ? end - lo + 1 : 0;
        }

        @Override
        public List<Subtask> split(int count) {
            if (totalUnits() == 0) {
                return List.of(new PrimeSumSubtask(0, 1, 0)); // empty range -> sum is 0
            }
            long[] bounds = evenBoundaries(count);
            int parts = bounds.length - 1;
            long lo = effectiveStart();
            List<Subtask> subtasks = new ArrayList<>(parts);
            for (int i = 0; i < parts; i++) {
                subtasks.add(new PrimeSumSubtask(i, lo + bounds[i], lo + bounds[i + 1] - 1));
            }
            return subtasks;
        }

        @Override
        public String combine(List<String> partialResults) {
            return partialResults.stream().map(BigInteger::new).reduce(BigInteger.ZERO, BigInteger::add).toString();
        }

        @Override
        public String describe() {
            return String.format("PRIMESUM [%,d .. %,d]", start, end);
        }
    }

    /** Creates the right {@link Job} subclass for a job kind. */
    public static final class JobFactory {

        private JobFactory() {
        }

        public static Job create(String id, JobKind kind, long[] numbers, long start, long end) {
            return switch (kind) {
                case MAX -> new MaxJob(id, numbers);
                case PRIMECOUNT -> new PrimeCountJob(id, numbers);
                case PRIMESUM -> new PrimeSumJob(id, start, end);
            };
        }
    }

    /** A job request that cannot be executed (empty list, bad range, too large ...). */
    public static class InvalidJobException extends Exception {

        public InvalidJobException(String message) {
            super(message);
        }
    }

    /**
     * One piece of a job that a single worker executes. Subclasses implement the
     * actual computation; they report real progress (units processed) while running.
     */
    public static abstract class Subtask {

        /** How often (in units) long loops report progress and check for cancellation. */
        protected static final int REPORT_EVERY = 4096;

        private final int index;
        private final JobKind kind;

        protected Subtask(int index, JobKind kind) {
            this.index = index;
            this.kind = kind;
        }

        /** Position of this subtask inside its job (0-based); job id + index = unique task id. */
        public int index() {
            return index;
        }

        public JobKind kind() {
            return kind;
        }

        /** Number of work units in this subtask. */
        public abstract long units();

        /**
         * Runs the computation.
         *
         * @return the partial result as a decimal string
         * @throws TaskCancelledException if {@code cancel} is triggered while running
         */
        public abstract String execute(ProgressListener progress, CancellationToken cancel);

        /** Input numbers (list jobs); empty for range jobs. */
        public long[] numbers() {
            return new long[0];
        }

        /** Range start (range jobs); 0 for list jobs. */
        public long rangeStart() {
            return 0;
        }

        /** Range end (range jobs); 0 for list jobs. */
        public long rangeEnd() {
            return 0;
        }
    }

    /** A subtask that processes a slice of a number list. */
    public static abstract class ListSubtask extends Subtask {

        private final long[] numbers;

        protected ListSubtask(int index, JobKind kind, long[] numbers) {
            super(index, kind);
            this.numbers = numbers;
        }

        @Override
        public long[] numbers() {
            return numbers;
        }

        @Override
        public long units() {
            return numbers.length;
        }
    }

    /** Finds the largest value in its slice. */
    public static class MaxSubtask extends ListSubtask {

        public MaxSubtask(int index, long[] numbers) {
            super(index, JobKind.MAX, numbers);
        }

        @Override
        public String execute(ProgressListener progress, CancellationToken cancel) {
            long[] values = numbers();
            if (values.length == 0) {
                throw new IllegalArgumentException("MAX subtask has no values");
            }
            long max = Long.MIN_VALUE;
            for (int i = 0; i < values.length; i++) {
                if (values[i] > max) {
                    max = values[i];
                }
                if ((i + 1) % (REPORT_EVERY * 64) == 0) {
                    cancel.throwIfCancelled();
                    progress.onProgress(i + 1L, values.length);
                }
            }
            progress.onProgress(values.length, values.length);
            return Long.toString(max);
        }
    }

    /** Counts the primes in its slice using the deterministic Miller-Rabin test. */
    public static class PrimeCountSubtask extends ListSubtask {

        public PrimeCountSubtask(int index, long[] numbers) {
            super(index, JobKind.PRIMECOUNT, numbers);
        }

        @Override
        public String execute(ProgressListener progress, CancellationToken cancel) {
            long[] values = numbers();
            long count = 0;
            for (int i = 0; i < values.length; i++) {
                if (Primes.isPrime(values[i])) {
                    count++;
                }
                if ((i + 1) % REPORT_EVERY == 0) {
                    cancel.throwIfCancelled();
                    progress.onProgress(i + 1L, values.length);
                }
            }
            progress.onProgress(values.length, values.length);
            return Long.toString(count);
        }
    }

    /**
     * Sums the primes in [start, end] with a segmented sieve of Eratosthenes over odd numbers only.
     *
     * <p>Instead of testing every number separately (Assignment 1: ~30 minutes for
     * 1..10^9), the range is processed in windows of {@value #SEGMENT} numbers: the
     * multiples of each base prime &lt;= sqrt(end) are crossed out, and what remains
     * is prime. 1..10^9 takes a few seconds on one core.
     */
    public static class PrimeSumSubtask extends Subtask {

        static final int SEGMENT = 1 << 20;

        private final long start;
        private final long end;

        public PrimeSumSubtask(int index, long start, long end) {
            super(index, JobKind.PRIMESUM);
            this.start = start;
            this.end = end;
        }

        @Override
        public long rangeStart() {
            return start;
        }

        @Override
        public long rangeEnd() {
            return end;
        }

        @Override
        public long units() {
            return end >= start ? end - start + 1 : 0;
        }

        @Override
        public String execute(ProgressListener progress, CancellationToken cancel) {
            long total = units();
            BigSum sum = new BigSum();
            long lo = Math.max(start, 2);
            if (end < lo) {
                progress.onProgress(total, total);
                return "0";
            }
            if (lo == 2) {
                sum.add(2); // the only even prime; the sieve below only looks at odd numbers
                lo = 3;
            }
            long firstOdd = (lo & 1) == 0 ? lo + 1 : lo;
            int[] basePrimes = Primes.basePrimes((int) Primes.isqrt(end));
            // composite[i] describes the odd number segStart + 2*i, so each window covers 2*SEGMENT numbers.
            boolean[] composite = new boolean[SEGMENT];

            for (long segStart = firstOdd; segStart <= end; segStart += 2L * SEGMENT) {
                cancel.throwIfCancelled();
                long segEnd = Math.min(segStart + 2L * (SEGMENT - 1), end);
                int length = (int) ((segEnd - segStart) / 2 + 1);
                Arrays.fill(composite, 0, length, false);

                for (int i = 1; i < basePrimes.length; i++) { // basePrimes[0] == 2 is skipped
                    long p = basePrimes[i];
                    long square = p * p;
                    if (square > segEnd) {
                        break;
                    }
                    long first = Math.max(square, ((segStart + p - 1) / p) * p);
                    if ((first & 1) == 0) {
                        first += p; // multiples alternate even/odd; only odd ones are in the window
                    }
                    for (long index = (first - segStart) / 2; index < length; index += p) {
                        composite[(int) index] = true;
                    }
                }
                for (int i = 0; i < length; i++) {
                    if (!composite[i]) {
                        sum.add(segStart + 2L * i);
                    }
                }
                progress.onProgress(Math.min(total, segEnd - start + 1), total);
            }
            progress.onProgress(total, total);
            return sum.toString();
        }
    }

    /** Rebuilds a subtask on the worker from the data received over the network. */
    public static final class SubtaskFactory {

        private SubtaskFactory() {
        }

        public static Subtask create(JobKind kind, int index, long[] numbers, long start, long end) {
            return switch (kind) {
                case MAX -> new MaxSubtask(index, numbers);
                case PRIMECOUNT -> new PrimeCountSubtask(index, numbers);
                case PRIMESUM -> new PrimeSumSubtask(index, start, end);
            };
        }
    }

    /** Receives real progress from a running subtask (units processed so far). */
    @FunctionalInterface
    public interface ProgressListener {

        ProgressListener NONE = (done, total) -> { };

        void onProgress(long unitsDone, long unitsTotal);
    }

    /** Lets a long computation stop early, e.g. when the coordinator cancels a duplicate attempt. */
    @FunctionalInterface
    public interface CancellationToken {

        CancellationToken NONE = () -> false;

        boolean isCancelled();

        /** Throws {@link TaskCancelledException} if cancellation was requested. */
        default void throwIfCancelled() {
            if (isCancelled()) {
                throw new TaskCancelledException();
            }
        }
    }

    /** Raised inside a subtask when its computation has been cancelled. */
    public static class TaskCancelledException extends RuntimeException {

        public TaskCancelledException() {
            super("Subtask was cancelled");
        }
    }
}
