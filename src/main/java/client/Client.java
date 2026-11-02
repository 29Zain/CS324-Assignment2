package client;

import client.ClientGUI.ClientWindow;
import client.ClusterClient.*;
import common.*;
import common.Config.ConfigException;
import common.Jobs.JobKind;
import common.Net.HostPort;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.function.LongConsumer;
import javax.swing.SwingUtilities;
import proto.*;

/**
 * Client program: start-up, settings, command-line mode, CSV loading and dataset generation.
 */
public final class Client {

    private Client() {
    }

    /** Entry point: java -cp target\distrilab.jar client.Client [options] */
    public static void main(String[] args) {
        ClientMain.main(args);
    }

    /**
     * Starts the client. Without arguments the GUI opens; with --cli, --status or --generate
     * it runs in the terminal (see {@link CliRunner}).
     *
     * <pre>java -cp target\distrilab.jar client.Client --bootstrap=192.168.1.50:50050</pre>
     */
    public static final class ClientMain {

        private ClientMain() {
        }

        public static void main(String[] args) {
            // An unexpected error in a background thread is logged instead of silently killing that thread.
            Thread.setDefaultUncaughtExceptionHandler((thread, error) ->
                    Log.get("CLIENT").error("Unexpected error in thread " + thread.getName(), error));
            Config config;
            ClientSettings settings;
            try {
                config = Config.load(ClientSettings.DEFAULTS, "client.properties", args);
                settings = ClientSettings.from(config);
            } catch (ConfigException e) {
                System.err.println("Configuration error: " + e.getMessage());
                System.exit(2);
                return;
            }
            Log.init("CLIENT", config.getBoolean("debug", false));
            boolean headless = config.getBoolean("cli", false) || config.getBoolean("status", false)
                    || config.getBoolean("generate", false);
            if (headless) {
                System.exit(new CliRunner(config, settings).run());
                return;
            }
            SwingUtilities.invokeLater(() -> ClientWindow.open(settings));
        }
    }

    /** Client settings (config/client.properties + command line). */
    public record ClientSettings(HostPort bootstrap, List<HostPort> contacts, long rpcTimeoutMs, long statusRefreshMs,
                                 String clientId, String dataDirectory, boolean darkTheme) {

        public static final Map<String, String> DEFAULTS = Map.of(
                "bootstrap", "localhost:50050",
                "contacts", "",
                "rpcTimeoutMs", "3000",
                "statusRefreshMs", "2000",
                "dataDirectory", "data",
                "darkTheme", "false");

        public static ClientSettings from(Config config) {
            return new ClientSettings(
                    HostPort.parse(config.getString("bootstrap")),
                    parseContacts(config.getString("contacts", "")),
                    config.getPositiveLong("rpcTimeoutMs"),
                    config.getPositiveLong("statusRefreshMs"),
                    config.getString("clientId", "client-" + UUID.randomUUID().toString().substring(0, 6)),
                    config.getString("dataDirectory"),
                    config.getBoolean("darkTheme", false));
        }

        /** "host:port,host:port" - workers to try when the Bootstrap Node is unreachable. */
        private static List<HostPort> parseContacts(String text) {
            return Arrays.stream(text.split(","))
                    .filter(part -> !part.isBlank())
                    .map(part -> HostPort.parse(part.trim()))
                    .toList();
        }
    }

    /**
     * Headless client, useful for scripted tests:
     * <pre>
     *   --cli --type=PRIMESUM --start=1 --end=1000000000 [--copies=2]
     *   --cli --type=MAX --file=data/numbers_100k.csv
     *   --cli --type=PRIMECOUNT --numbers=2,3,4,5,6,7
     *   --status
     *   --generate --count=1000000 --min=1 --max=1000000000000 --out=data/numbers_1m.csv [--seed=42]
     * </pre>
     */
    public static final class CliRunner {

        private final Config config;
        private final ClientSettings settings;

        public CliRunner(Config config, ClientSettings settings) {
            this.config = config;
            this.settings = settings;
        }

        private ClusterClient connect() {
            return new ClusterClient(settings.bootstrap(), settings.contacts(), settings.rpcTimeoutMs());
        }

        public int generate() throws IOException {
            int count = config.getIntInRange("count", 1, 50_000_000);
            long min = config.getLong("min");
            long max = config.getLong("max");
            long seed = config.has("seed") ? config.getLong("seed") : 42;
            Path out = Path.of(config.getString("out"));
            LoadedData data = DataGenerator.generate(out, count, min, max, seed,
                    done -> System.out.printf("  generated %,d values%n", done));
            System.out.println("Wrote " + out.toAbsolutePath() + ": " + data.summary());
            return 0;
        }

        public int status() {
            try (ClusterClient client = connect()) {
                ClusterStatus status = client.clusterStatus().orElse(null);
                if (status == null) {
                    System.out.println("No coordinator reachable (workers registered: " + client.workers().size() + ")");
                    return 1;
                }
                System.out.printf("Coordinator: Worker-%d (term %d), jobs this term %d/%d, running jobs %d%n",
                        status.getCoordinator().getNode().getId(), status.getCoordinator().getTerm(),
                        status.getJobsInTerm(), status.getMaxJobsPerTerm(), status.getRunningJobs());
                for (WorkerSummary w : status.getWorkersList()) {
                    System.out.printf("  Worker-%-3d %-22s %-8s JAC=%-4d active=%-3d neighbours=%s%s%n",
                            w.getNode().getId(), w.getNode().getHost() + ":" + w.getNode().getPort(), w.getHealth(),
                            w.getJac(), w.getActiveSubtasks(), w.getNeighbourIdsList(),
                            w.getIsCoordinator() ? "  <- coordinator" : "");
                }
                return 0;
            }
        }

        public int runJobs() throws IOException, DataFormatException, InterruptedException {
            JobKind kind = JobKind.valueOf(config.getString("type").toUpperCase());
            long[] numbers = new long[0];
            long start = 0;
            long end = 0;
            String description;
            if (kind.usesNumberList()) {
                LoadedData data = config.has("file")
                        ? CsvLoader.loadFile(Path.of(config.getString("file")))
                        : CsvLoader.parseText(config.getString("numbers"), "command line");
                numbers = data.values();
                description = kind + " of " + data.summary();
            } else {
                start = config.getLong("start");
                end = config.getLong("end");
                description = String.format("PRIMESUM [%,d .. %,d]", start, end);
            }
            int copies = config.has("copies") ? config.getIntInRange("copies", 1, 50) : 1;

            try (ClusterClient client = connect()) {
                List<JobTracker> trackers = new ArrayList<>();
                List<Thread> threads = new ArrayList<>();
                Map<String, Integer> notesPrinted = new HashMap<>();
                for (int i = 0; i < copies; i++) {
                    String jobId = UUID.randomUUID().toString();
                    JobTracker tracker = new JobTracker(client,
                            JobTracker.buildRequest(jobId, settings.clientId(), kind, numbers, start, end), kind, description);
                    trackers.add(tracker);
                    Thread thread = new Thread(tracker, "job-" + jobId.substring(0, 8));
                    thread.start();
                    threads.add(thread);
                    System.out.println("Submitted job " + tracker.view().shortId() + ": " + description);
                }
                boolean running = true;
                while (running) {
                    Thread.sleep(1000);
                    running = false;
                    for (JobTracker tracker : trackers) {
                        JobView view = tracker.view();
                        printNewNotes(view, notesPrinted);
                        if (!view.status().isFinished()) {
                            running = true;
                            System.out.printf("[job %s] %-10s %6.2f%%  %s%n", view.shortId(), view.status(),
                                    view.percent(), view.message());
                        }
                    }
                }
                for (Thread thread : threads) {
                    thread.join();
                }
                boolean allOk = true;
                for (JobTracker tracker : trackers) {
                    JobView view = tracker.view();
                    printNewNotes(view, notesPrinted);
                    System.out.printf("[job %s] %s in %.2f s -> %s%n", view.shortId(), view.status(),
                            view.elapsedMs() / 1000.0, view.status() == ClientJobStatus.COMPLETED ? view.result() : view.message());
                    allOk &= view.status() == ClientJobStatus.COMPLETED;
                }
                return allOk ? 0 : 1;
            }
        }

        private static void printNewNotes(JobView view, Map<String, Integer> printed) {
            int already = printed.getOrDefault(view.jobId(), 0);
            for (int i = already; i < view.notes().size(); i++) {
                System.out.println("[job " + view.shortId() + "] " + view.notes().get(i));
            }
            printed.put(view.jobId(), view.notes().size());
        }

        /** Runs the requested CLI command; returns the process exit code. */
        public int run() {
            try {
                if (config.getBoolean("generate", false)) {
                    return generate();
                }
                if (config.getBoolean("status", false)) {
                    return status();
                }
                return runJobs();
            } catch (ConfigException | IllegalArgumentException e) {
                System.err.println("Invalid arguments: " + e.getMessage());
                return 2;
            } catch (DataFormatException | IOException e) {
                System.err.println("Input error: " + e.getMessage());
                return 2;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return 1;
            }
        }
    }

    /**
     * Reads numbers from CSV/text: values may be separated by commas, semicolons, spaces,
     * tabs or new lines. A non-numeric first line is treated as a header and skipped.
     * Invalid values are reported with their line number instead of crashing.
     */
    public static final class CsvLoader {

        private CsvLoader() {
        }

        public static LoadedData loadFile(Path file) throws IOException, DataFormatException {
            try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                return parse(reader, file.getFileName().toString());
            }
        }

        public static LoadedData parseText(String text, String source) throws DataFormatException {
            try (BufferedReader reader = new BufferedReader(new StringReader(text))) {
                return parse(reader, source);
            } catch (IOException e) {
                throw new DataFormatException("Could not read input: " + e.getMessage());
            }
        }

        private static LoadedData parse(BufferedReader reader, String source) throws IOException, DataFormatException {
            long[] values = new long[1024];
            int count = 0;
            int lineNumber = 0;
            boolean skippedHeader = false;
            boolean seenData = false;
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                String trimmed = line.strip();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                String[] tokens = trimmed.split("[,;\\s]+");
                if (!seenData && !skippedHeader && !looksNumeric(tokens)) {
                    skippedHeader = true; // e.g. "value" or "start,end"
                    continue;
                }
                for (String token : tokens) {
                    if (token.isEmpty()) {
                        continue;
                    }
                    if (count == values.length) {
                        values = Arrays.copyOf(values, values.length * 2);
                    }
                    values[count++] = parseValue(token, lineNumber);
                    seenData = true;
                }
            }
            return LoadedData.of(Arrays.copyOf(values, count), source, lineNumber, skippedHeader);
        }

        private static boolean looksNumeric(String[] tokens) {
            return Arrays.stream(tokens).anyMatch(token -> !token.isEmpty() && token.matches("[+-]?[0-9_]+"));
        }

        private static long parseValue(String token, int line) throws DataFormatException {
            String cleaned = token.replace("_", "");
            if (!cleaned.matches("[+-]?[0-9]+")) {
                throw new DataFormatException("Line " + line + ": '" + token + "' is not a whole number");
            }
            try {
                return Long.parseLong(cleaned);
            } catch (NumberFormatException e) {
                throw new DataFormatException("Line " + line + ": " + token
                        + " is outside the supported 64-bit range (-9,223,372,036,854,775,808 .. 9,223,372,036,854,775,807)");
            }
        }
    }

    /** Numbers loaded from a CSV file, a generator or manual input, with a summary for the GUI. */
    public record LoadedData(long[] values, String source, long min, long max, int lineCount, boolean skippedHeader) {

        public static LoadedData of(long[] values, String source, int lineCount, boolean skippedHeader) {
            // For no values min/max stay Long.MAX_VALUE/Long.MIN_VALUE.
            LongSummaryStatistics stats = Arrays.stream(values).summaryStatistics();
            return new LoadedData(values, source, stats.getMin(), stats.getMax(), lineCount, skippedHeader);
        }

        public int count() {
            return values.length;
        }

        /** e.g. "1,000,000 values · min 3 · max 9,999,999,967 · numbers_1m.csv" */
        public String summary() {
            if (values.length == 0) {
                return "no values";
            }
            return String.format("%,d values · min %,d · max %,d · %s", values.length, min, max, source);
        }

        /** First few values for a preview. */
        public String preview(int limit) {
            StringBuilder text = new StringBuilder();
            for (int i = 0; i < Math.min(limit, values.length); i++) {
                if (i > 0) {
                    text.append(", ");
                }
                text.append(values[i]);
            }
            if (values.length > limit) {
                text.append(", ... (").append(String.format("%,d", values.length - limit)).append(" more)");
            }
            return text.toString();
        }
    }

    /**
     * Generates large test datasets with a known size and range, e.g.
     * {@code --generate --count=1000000 --min=1 --max=1000000000000 --out=data/numbers_1m.csv}.
     * The same seed always produces the same file, so results can be compared between runs.
     */
    public static final class DataGenerator {

        private DataGenerator() {
        }

        public static LoadedData generate(Path out, int count, long min, long max, long seed, LongConsumer progress)
                throws IOException {
            if (count <= 0 || min > max) {
                throw new IllegalArgumentException("count must be > 0 and min <= max");
            }
            SplittableRandom random = new SplittableRandom(seed);
            long[] values = new long[count];
            Path parent = out.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            try (BufferedWriter writer = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {
                for (int i = 0; i < count; i++) {
                    long value = max == Long.MAX_VALUE && min == Long.MIN_VALUE
                            ? random.nextLong()
                            : min + (long) (random.nextDouble() * ((double) max - (double) min + 1.0));
                    value = Math.max(min, Math.min(max, value));
                    values[i] = value;
                    writer.write(Long.toString(value));
                    writer.write((i + 1) % 10 == 0 || i == count - 1 ? "\n" : ",");
                    if (progress != null && (i + 1) % 100_000 == 0) {
                        progress.accept(i + 1L);
                    }
                }
            }
            return LoadedData.of(values, out.getFileName().toString(), (count + 9) / 10, false);
        }
    }

    /** Input data that cannot be parsed (reported to the user with a line number). */
    public static class DataFormatException extends Exception {

        public DataFormatException(String message) {
            super(message);
        }
    }
}
