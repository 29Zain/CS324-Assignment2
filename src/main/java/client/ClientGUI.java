package client;

import client.Client.*;
import client.ClusterClient.*;
import com.formdev.flatlaf.*;
import common.Jobs.*;
import java.awt.*;
import java.awt.event.*;
import java.io.File;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;
import javax.swing.*;
import javax.swing.Timer;
import javax.swing.border.Border;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.table.*;
import javax.swing.text.BadLocationException;
import proto.*;

/**
 * Swing GUI of the client: job submission, live progress cards (click for the per-worker
 * breakdown), cluster view and activity log.
 */
public final class ClientGUI {

    private ClientGUI() {
    }

    /** Thread factory for named daemon threads (they never keep the JVM alive). */
    private static ThreadFactory daemonThreads(String name) {
        return r -> {
            Thread thread = new Thread(r, name);
            thread.setDaemon(true);
            return thread;
        };
    }

    /**
     * Runs {@code task} on a SwingWorker background thread, then hands its result (or the
     * cause of its failure) to the given callback on the EDT.
     */
    private static <T> void inBackground(Callable<T> task, Consumer<T> onSuccess, Consumer<Throwable> onFailure) {
        new SwingWorker<T, Void>() {
            @Override
            protected T doInBackground() throws Exception {
                return task.call();
            }

            @Override
            protected void done() {
                try {
                    onSuccess.accept(get());
                } catch (ExecutionException e) {
                    onFailure.accept(e.getCause());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }.execute();
    }

    /**
     * Main client window:
     * <pre>
     *  +--------------------------------------------------------------+
     *  | DistriLab Cluster Client          Coordinator Worker-5 term 2|
     *  +----------------+---------------------------------------------+
     *  | 1. Operation   |  Jobs (one card per job, live progress bars)|
     *  | 2. Input data  |                                             |
     *  | 3. Submit      +---------------------------------------------+
     *  |                |  [Cluster] [Activity log]                   |
     *  +----------------+---------------------------------------------+
     * </pre>
     */
    public static final class ClientWindow extends JFrame {

        private final ClientSettings settings;
        private final ClusterClient client;
        private final ActivityLog log = new ActivityLog();
        private final JobsPanel jobs;
        private final ClusterPanel cluster;
        private final JLabel headline = new JLabel(" ");
        private final ExecutorService jobThreads = Executors.newCachedThreadPool(daemonThreads("job-tracker"));
        private final Map<String, ClientJobStatus> lastStatus = new ConcurrentHashMap<>();
        private final Map<String, Integer> notesLogged = new ConcurrentHashMap<>();

        public static void open(ClientSettings settings) {
            Theme.install(settings.darkTheme());
            new ClientWindow(settings).setVisible(true);
        }

        private ClientWindow(ClientSettings settings) {
            super("DistriLab Client - " + settings.clientId());
            this.settings = settings;
            this.client = new ClusterClient(settings.bootstrap(), settings.contacts(), settings.rpcTimeoutMs());
            this.jobs = new JobsPanel(this::openDetails);
            this.cluster = new ClusterPanel(client, settings.statusRefreshMs(),
                    text -> SwingUtilities.invokeLater(() -> headline.setText(text)));

            setDefaultCloseOperation(EXIT_ON_CLOSE);
            // Fit the window to the screen (laptop screens are often only 768-900 px high).
            Rectangle screen = GraphicsEnvironment.getLocalGraphicsEnvironment().getMaximumWindowBounds();
            setSize(Math.min(1360, screen.width - 40), Math.min(880, screen.height - 20));
            setMinimumSize(new Dimension(Math.min(1000, screen.width), Math.min(620, screen.height)));
            setLocationRelativeTo(null);

            JPanel header = new JPanel(new BorderLayout());
            header.setBorder(Theme.padding(10, 16));
            JLabel title = Theme.boldLabel("DistriLab Cluster Client", 19f);
            JLabel subtitle = new JLabel("Bootstrap Node " + settings.bootstrap() + "   ·   client id "
                    + settings.clientId() + "   ·   gRPC");
            subtitle.setForeground(Theme.GREY);
            JPanel titles = new JPanel(new BorderLayout());
            titles.add(title, BorderLayout.NORTH);
            titles.add(subtitle, BorderLayout.SOUTH);
            header.add(titles, BorderLayout.WEST);
            headline.setFont(Theme.bold(headline.getFont(), 14f));
            header.add(headline, BorderLayout.EAST);

            SubmitPanel submit = new SubmitPanel(this::submitJobs, log, settings.dataDirectory());
            JTabbedPane tabs = new JTabbedPane();
            tabs.addTab("Cluster", cluster);
            tabs.addTab("Activity log", log);

            JSplitPane right = new JSplitPane(JSplitPane.VERTICAL_SPLIT, jobs, tabs);
            right.setResizeWeight(0.62);
            right.setBorder(null);

            setLayout(new BorderLayout());
            add(header, BorderLayout.NORTH);
            add(new JScrollPane(submit), BorderLayout.WEST);
            add(right, BorderLayout.CENTER);

            addWindowListener(new WindowAdapter() {
                @Override
                public void windowClosing(WindowEvent e) {
                    cluster.stop();
                    jobThreads.shutdownNow();
                    client.close();
                }
            });
            log.info("Client started. Bootstrap Node: " + settings.bootstrap());
            // Give the job list ~60 % of the height once the window has its real size.
            addComponentListener(new ComponentAdapter() {
                private boolean placed;

                @Override
                public void componentShown(ComponentEvent e) {
                    placeDivider();
                }

                @Override
                public void componentResized(ComponentEvent e) {
                    placeDivider();
                }

                private void placeDivider() {
                    if (!placed && right.getHeight() > 0) {
                        placed = true;
                        right.setDividerLocation(0.6);
                    }
                }
            });
        }

        /** Creates one tracker (and thread) per job copy; each job has its own unique id. */
        private void submitJobs(JobKind kind, long[] numbers, long start, long end, String description, int copies) {
            for (int i = 0; i < copies; i++) {
                String jobId = UUID.randomUUID().toString();
                JobTracker tracker = new JobTracker(client,
                        JobTracker.buildRequest(jobId, settings.clientId(), kind, numbers, start, end), kind, description);
                tracker.addListener(this::logChanges);
                jobs.add(tracker);
                log.info("Submitting job #" + jobId.substring(0, 8) + ": " + description);
                jobThreads.execute(tracker);
            }
        }

        /** Writes status changes and job notes into the activity log. */
        private void logChanges(JobView view) {
            ClientJobStatus previous = lastStatus.put(view.jobId(), view.status());
            int logged = notesLogged.getOrDefault(view.jobId(), 0);
            for (int i = logged; i < view.notes().size(); i++) {
                log.info("#" + view.shortId() + "  " + view.notes().get(i).substring(10));
            }
            notesLogged.put(view.jobId(), view.notes().size());
            if (previous != view.status() && view.status() != ClientJobStatus.RUNNING) {
                log.info("#" + view.shortId() + "  " + view.status() + ": " + view.message());
            }
        }

        private void openDetails(JobTracker tracker) {
            new JobDetailsDialog(this, tracker).setVisible(true);
        }
    }

    /** Colours, fonts and look-and-feel used across the client GUI (plus small component factories). */
    static final class Theme {

        static final Color BLUE = new Color(0x2F6FEB);
        static final Color GREEN = new Color(0x1F9D55);
        static final Color AMBER = new Color(0xD9822B);
        static final Color RED = new Color(0xD64545);
        static final Color GREY = new Color(0x8A8F98);
        static final Color PURPLE = new Color(0x7C4DDB);

        private Theme() {
        }

        static void install(boolean dark) {
            if (dark) {
                FlatDarkLaf.setup();
            } else {
                FlatLightLaf.setup();
            }
            UIManager.put("Component.arc", 10);
            UIManager.put("Button.arc", 10);
            UIManager.put("ProgressBar.arc", 10);
            UIManager.put("TextComponent.arc", 8);
            UIManager.put("ScrollBar.width", 12);
            UIManager.put("Table.showHorizontalLines", true);
            UIManager.put("TabbedPane.showTabSeparators", true);
        }

        static Color statusColor(ClientJobStatus status) {
            return switch (status) {
                case RUNNING -> BLUE;
                case RECOVERING, WAITING -> AMBER;
                case SUBMITTING -> PURPLE;
                case COMPLETED -> GREEN;
                case FAILED -> RED;
                case CANCELLED -> GREY;
            };
        }

        static Color healthColor(WorkerHealth health) {
            return switch (health) {
                case ALIVE -> GREEN;
                case SUSPECT -> AMBER;
                case DEAD -> RED;
                default -> GREY;
            };
        }

        static Font mono(int size) {
            return new Font(Font.MONOSPACED, Font.PLAIN, size);
        }

        static Font bold(Font base, float size) {
            return base.deriveFont(Font.BOLD, size);
        }

        static Border padding(int vertical, int horizontal) {
            return BorderFactory.createEmptyBorder(vertical, horizontal, vertical, horizontal);
        }

        /** Label in the default font made bold at the given size. */
        static JLabel boldLabel(String text, float size) {
            JLabel label = new JLabel(text);
            label.setFont(bold(label.getFont(), size));
            return label;
        }

        static JButton button(String text, ActionListener action) {
            JButton button = new JButton(text);
            button.addActionListener(action);
            return button;
        }

        /** Adds a "caption: field" pair to a two-column grid form. */
        static void addRow(JPanel form, String caption, Component field) {
            form.add(new JLabel(caption));
            form.add(field);
        }

        /** Small coloured label used for statuses and job types. */
        static JLabel chip(String text, Color color) {
            JLabel label = new JLabel();
            label.setFont(bold(label.getFont(), label.getFont().getSize2D()));
            recolorChip(label, text, color);
            return label;
        }

        static void recolorChip(JLabel chip, String text, Color color) {
            chip.setText(text);
            chip.setForeground(color);
            chip.setBorder(BorderFactory.createCompoundBorder(
                    BorderFactory.createLineBorder(color, 1, true), padding(1, 7)));
        }

        /** Shows a job's status on its chip and its progress on the bar, both in the status colour. */
        static void showStatus(JobView view, JLabel chip, JProgressBar bar) {
            recolorChip(chip, view.status().name(), statusColor(view.status()));
            bar.setForeground(statusColor(view.status()));
            bar.setValue((int) Math.round(view.percent() * 10));
        }

        static String formatMillis(long ms) {
            if (ms < 1000) {
                return ms + " ms";
            }
            if (ms < 60_000) {
                return String.format("%.1f s", ms / 1000.0);
            }
            return String.format("%d min %02d s", ms / 60_000, (ms / 1000) % 60);
        }
    }

    /** Timestamped activity log shown at the bottom of the window (thread-safe). */
    static final class ActivityLog extends JScrollPane {

        private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");
        private static final int MAX_LINES = 2000;

        private final JTextArea area = new JTextArea();

        ActivityLog() {
            area.setEditable(false);
            area.setFont(Theme.mono(12));
            area.setLineWrap(false);
            setViewportView(area);
        }

        void info(String message) {
            String line = LocalTime.now().format(TIME) + "  " + message + "\n";
            SwingUtilities.invokeLater(() -> {
                area.append(line);
                trim();
                area.setCaretPosition(area.getDocument().getLength());
            });
        }

        private void trim() {
            int excess = area.getLineCount() - MAX_LINES;
            if (excess > 0) {
                try {
                    area.getDocument().remove(0, area.getLineEndOffset(excess - 1));
                } catch (BadLocationException e) {
                    area.setText("");
                }
            }
        }
    }

    /** Left-hand panel: choose an operation, enter or load data, and submit jobs. */
    static final class SubmitPanel extends JPanel {

        /** Receives a validated job from the panel. */
        interface SubmitHandler {
            void submit(JobKind kind, long[] numbers, long start, long end, String description, int copies);
        }

        private static final String LIST_CARD = "list";
        /** Ranges larger than this ask for confirmation before they are submitted. */
        private static final long LARGE_RANGE = 100_000_000_000L;
        private static final String RANGE_CARD = "range";

        private final SubmitHandler handler;
        private final ActivityLog log;
        private final File dataDirectory;
        private final Map<JobKind, JToggleButton> kindButtons = new EnumMap<>(JobKind.class);
        private final JLabel kindDescription = new JLabel();
        private final CardLayout inputCards = new CardLayout();
        private final JPanel inputPanel = new JPanel(inputCards);
        private final JTextArea manualInput = new JTextArea(7, 24);
        private final JLabel dataSummary = new JLabel(" ");
        private final JTextField rangeStart = new JTextField("1", 12);
        private final JTextField rangeEnd = new JTextField("1000000", 12);
        private final JSpinner copies = new JSpinner(new SpinnerNumberModel(1, 1, 10, 1));
        private final JButton submitButton = new JButton("Submit job");
        private JobKind kind = JobKind.PRIMESUM;
        private LoadedData loaded;

        SubmitPanel(SubmitHandler handler, ActivityLog log, String dataDirectory) {
            this.handler = handler;
            this.log = log;
            this.dataDirectory = new File(dataDirectory);
            setLayout(new BorderLayout());
            setBorder(Theme.padding(14, 14));
            setPreferredSize(new Dimension(370, 600));

            JPanel content = new JPanel();
            content.setLayout(new BoxLayout(content, BoxLayout.Y_AXIS));
            content.add(section("1. Operation"));
            content.add(kindSelector());
            content.add(Box.createVerticalStrut(6));
            kindDescription.setForeground(Theme.GREY);
            content.add(new JComponentWrapper(kindDescription));
            content.add(Box.createVerticalStrut(14));
            content.add(section("2. Input data"));
            inputPanel.add(listCard(), LIST_CARD);
            inputPanel.add(rangeCard(), RANGE_CARD);
            inputPanel.setAlignmentX(Component.LEFT_ALIGNMENT);
            content.add(inputPanel);
            content.add(Box.createVerticalStrut(14));
            content.add(section("3. Submit"));
            content.add(submitRow());
            add(content, BorderLayout.NORTH);

            selectKind(JobKind.PRIMESUM);
        }

        private static JLabel section(String title) {
            JLabel label = Theme.boldLabel(title, 14f);
            label.setBorder(BorderFactory.createEmptyBorder(0, 0, 6, 0));
            label.setAlignmentX(Component.LEFT_ALIGNMENT);
            return label;
        }

        /** Keeps a component left-aligned inside a BoxLayout. */
        private static final class JComponentWrapper extends JPanel {
            JComponentWrapper(Component component) {
                super(new FlowLayout(FlowLayout.LEFT, 0, 0));
                add(component);
                setAlignmentX(Component.LEFT_ALIGNMENT);
            }
        }

        private JPanel kindSelector() {
            JPanel row = new JPanel(new GridLayout(1, 3, 6, 0));
            row.setAlignmentX(Component.LEFT_ALIGNMENT);
            ButtonGroup group = new ButtonGroup();
            for (JobKind k : JobKind.values()) {
                JToggleButton button = new JToggleButton(k.name());
                button.addActionListener(e -> selectKind(k));
                group.add(button);
                kindButtons.put(k, button);
                row.add(button);
            }
            row.setMaximumSize(new Dimension(Integer.MAX_VALUE, 34));
            return row;
        }

        private void selectKind(JobKind newKind) {
            kind = newKind;
            kindButtons.get(newKind).setSelected(true);
            kindDescription.setText(newKind.description());
            inputCards.show(inputPanel, newKind.usesNumberList() ? LIST_CARD : RANGE_CARD);
        }

        private JPanel listCard() {
            JPanel card = new JPanel(new BorderLayout(0, 6));
            manualInput.setLineWrap(true);
            manualInput.setWrapStyleWord(true);
            manualInput.setFont(Theme.mono(12));
            manualInput.setToolTipText("Type numbers separated by commas, spaces or new lines");
            card.add(new JLabel("Numbers (comma, space or line separated):"), BorderLayout.NORTH);
            card.add(new JScrollPane(manualInput), BorderLayout.CENTER);

            JPanel south = new JPanel(new BorderLayout(0, 6));
            JPanel buttons = new JPanel(new GridLayout(1, 3, 6, 0));
            buttons.add(Theme.button("Load CSV...", e -> chooseCsv(false)));
            buttons.add(Theme.button("Generate...",
                    e -> GenerateDialog.show(this, dataDirectory, log, this::useLoadedData)));
            buttons.add(Theme.button("Clear", e -> clearLoadedData()));
            south.add(buttons, BorderLayout.NORTH);
            dataSummary.setForeground(Theme.GREY);
            south.add(dataSummary, BorderLayout.CENTER);
            card.add(south, BorderLayout.SOUTH);
            return card;
        }

        private JPanel rangeCard() {
            JPanel card = new JPanel();
            card.setLayout(new BoxLayout(card, BoxLayout.Y_AXIS));
            JPanel fields = new JPanel(new GridLayout(2, 2, 6, 6));
            Theme.addRow(fields, "Range start:", rangeStart);
            Theme.addRow(fields, "Range end:", rangeEnd);
            fields.setAlignmentX(Component.LEFT_ALIGNMENT);
            fields.setMaximumSize(new Dimension(Integer.MAX_VALUE, 64));
            card.add(fields);
            card.add(Box.createVerticalStrut(8));

            JPanel presets = new JPanel(new GridLayout(1, 4, 6, 0));
            presets.add(preset("1 - 1M", 1, 1_000_000));
            presets.add(preset("1 - 100M", 1, 100_000_000));
            presets.add(preset("1 - 1B", 1, 1_000_000_000));
            JButton fromCsv = Theme.button("CSV...", e -> chooseCsv(true));
            fromCsv.setToolTipText("Load start and end from the first two values of a CSV file");
            presets.add(fromCsv);
            presets.setAlignmentX(Component.LEFT_ALIGNMENT);
            presets.setMaximumSize(new Dimension(Integer.MAX_VALUE, 32));
            card.add(presets);
            card.add(Box.createVerticalStrut(6));
            JLabel hint = new JLabel(String.format("<html>Supported: 0 .. %,d (10^15). Sums of any size are exact.</html>",
                    PrimeSumJob.MAX_END));
            hint.setForeground(Theme.GREY);
            hint.setAlignmentX(Component.LEFT_ALIGNMENT);
            card.add(hint);
            return card;
        }

        private JButton preset(String text, long start, long end) {
            return Theme.button(text, e -> {
                rangeStart.setText(Long.toString(start));
                rangeEnd.setText(Long.toString(end));
            });
        }

        private JPanel submitRow() {
            JPanel row = new JPanel(new BorderLayout(8, 0));
            JPanel copiesPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 4, 0));
            copiesPanel.add(new JLabel("Copies:"));
            copies.setToolTipText("Submit several identical jobs at the same time (concurrency test)");
            copiesPanel.add(copies);
            row.add(copiesPanel, BorderLayout.WEST);
            submitButton.putClientProperty("JButton.buttonType", "default");
            submitButton.setFont(Theme.bold(submitButton.getFont(), 14f));
            submitButton.addActionListener(e -> submit());
            row.add(submitButton, BorderLayout.CENTER);
            row.setAlignmentX(Component.LEFT_ALIGNMENT);
            row.setMaximumSize(new Dimension(Integer.MAX_VALUE, 40));
            return row;
        }

        // ------------------------------------------------------------------ data loading

        private void chooseCsv(boolean forRange) {
            JFileChooser chooser = new JFileChooser(dataDirectory.isDirectory() ? dataDirectory : new File("."));
            chooser.setFileFilter(new FileNameExtensionFilter("CSV / text files", "csv", "txt"));
            if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) {
                return;
            }
            Path file = chooser.getSelectedFile().toPath();
            dataSummary.setText("Loading " + file.getFileName() + " ...");
            inBackground(() -> CsvLoader.loadFile(file), data -> {
                if (forRange) {
                    applyRange(data);
                } else {
                    useLoadedData(data);
                }
            }, cause -> {
                String message = cause instanceof DataFormatException ? cause.getMessage()
                        : "Could not read " + file.getFileName() + ": " + cause.getMessage();
                dataSummary.setText(" ");
                error(message);
            });
        }

        private void applyRange(LoadedData data) {
            if (data.count() < 2) {
                error("The CSV file must contain at least two values (start, end).");
                return;
            }
            rangeStart.setText(Long.toString(data.values()[0]));
            rangeEnd.setText(Long.toString(data.values()[1]));
            log.info("Loaded PRIMESUM range from " + data.source());
        }

        private void useLoadedData(LoadedData data) {
            loaded = data;
            manualInput.setText(data.preview(200));
            manualInput.setEditable(false);
            manualInput.setCaretPosition(0);
            dataSummary.setText("<html><b>" + String.format("%,d", data.count()) + " values</b> loaded from "
                    + data.source() + "<br>min " + String.format("%,d", data.min()) + " · max "
                    + String.format("%,d", data.max()) + (data.skippedHeader() ? " · header skipped" : "") + "</html>");
            log.info("Loaded data: " + data.summary());
        }

        private void clearLoadedData() {
            loaded = null;
            manualInput.setEditable(true);
            manualInput.setText("");
            dataSummary.setText(" ");
        }

        // ------------------------------------------------------------------ submission

        private void submit() {
            int count = (Integer) copies.getValue();
            if (kind.usesNumberList()) {
                LoadedData data = loaded;
                if (data == null) {
                    try {
                        data = CsvLoader.parseText(manualInput.getText(), "manual input");
                    } catch (DataFormatException e) {
                        error(e.getMessage());
                        return;
                    }
                }
                if (data.count() == 0) {
                    error("Enter some numbers or load a CSV file for " + kind + ".");
                    return;
                }
                if (data.count() > ListJob.MAX_VALUES) {
                    error(String.format("At most %,d values are supported per job.", ListJob.MAX_VALUES));
                    return;
                }
                handler.submit(kind, data.values(), 0, 0, kind + " of " + data.summary(), count);
            } else {
                long start;
                long end;
                try {
                    start = parseNumber(rangeStart.getText(), "Range start");
                    end = parseNumber(rangeEnd.getText(), "Range end");
                } catch (DataFormatException e) {
                    error(e.getMessage());
                    return;
                }
                if (start > end) {
                    error("Range start must be less than or equal to range end.");
                    return;
                }
                if (end > PrimeSumJob.MAX_END) {
                    error(String.format("Range end must be at most %,d (10^15).", PrimeSumJob.MAX_END));
                    return;
                }
                long size = end - Math.max(start, 0);
                if (size > LARGE_RANGE && !confirm(String.format("PRIMESUM over %,d numbers can take a long time "
                        + "(about 1 s per billion numbers per CPU core in the cluster). You can cancel it at any time. "
                        + "Submit anyway?", size))) {
                    return;
                }
                handler.submit(kind, new long[0], start, end, String.format("PRIMESUM [%,d .. %,d]", start, end), count);
            }
        }

        private static long parseNumber(String text, String field) throws DataFormatException {
            String cleaned = text.trim().replace(",", "").replace("_", "").replace(" ", "");
            if (!cleaned.matches("[+-]?[0-9]+")) {
                throw new DataFormatException(field + " must be a whole number.");
            }
            try {
                return Long.parseLong(cleaned);
            } catch (NumberFormatException e) {
                throw new DataFormatException(field + " is too large for a 64-bit number.");
            }
        }

        private boolean confirm(String message) {
            return JOptionPane.showConfirmDialog(this, message, "Large job", JOptionPane.YES_NO_OPTION,
                    JOptionPane.WARNING_MESSAGE) == JOptionPane.YES_OPTION;
        }

        private void error(String message) {
            JOptionPane.showMessageDialog(this, message, "Invalid input", JOptionPane.WARNING_MESSAGE);
        }
    }

    /** Small dialog that generates a large random CSV dataset and loads it. */
    static final class GenerateDialog {

        private GenerateDialog() {
        }

        static void show(JComponent parent, File dataDirectory, ActivityLog log, Consumer<LoadedData> onLoaded) {
            JTextField count = new JTextField("1000000");
            JTextField min = new JTextField("1");
            JTextField max = new JTextField("1000000000000");
            JTextField seed = new JTextField("42");
            JTextField file = new JTextField(new File(dataDirectory, "generated_1m.csv").getPath());
            JPanel form = new JPanel(new GridLayout(5, 2, 6, 6));
            Theme.addRow(form, "How many values:", count);
            Theme.addRow(form, "Smallest value:", min);
            Theme.addRow(form, "Largest value:", max);
            Theme.addRow(form, "Random seed:", seed);
            Theme.addRow(form, "Save as:", file);
            int choice = JOptionPane.showConfirmDialog(parent, form, "Generate test data", JOptionPane.OK_CANCEL_OPTION,
                    JOptionPane.PLAIN_MESSAGE);
            if (choice != JOptionPane.OK_OPTION) {
                return;
            }
            int n;
            long lo;
            long hi;
            long s;
            try {
                n = Integer.parseInt(withoutCommas(count));
                lo = Long.parseLong(withoutCommas(min));
                hi = Long.parseLong(withoutCommas(max));
                s = Long.parseLong(seed.getText().trim());
                if (n <= 0 || n > 20_000_000 || lo > hi) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                JOptionPane.showMessageDialog(parent, "Use 1 .. 20,000,000 values and smallest <= largest.",
                        "Invalid input", JOptionPane.WARNING_MESSAGE);
                return;
            }
            Path out = Path.of(file.getText().trim());
            log.info("Generating " + String.format("%,d", n) + " values into " + out + " ...");
            inBackground(() -> DataGenerator.generate(out, n, lo, hi, s, null), onLoaded,
                    cause -> JOptionPane.showMessageDialog(parent, "Could not generate data: " + cause.getMessage(),
                            "Error", JOptionPane.ERROR_MESSAGE));
        }

        private static String withoutCommas(JTextField field) {
            return field.getText().trim().replace(",", "");
        }
    }

    /** Scrollable list of job cards (newest first), refreshed several times per second. */
    static final class JobsPanel extends JPanel {

        private final JPanel list = new JPanel();
        private final List<JobCard> cards = new ArrayList<>();
        private final JLabel counts = new JLabel("No jobs yet - submit one on the left.");
        private final Consumer<JobTracker> openDetails;

        JobsPanel(Consumer<JobTracker> openDetails) {
            this.openDetails = openDetails;
            setLayout(new BorderLayout());

            JPanel header = new JPanel(new BorderLayout());
            header.setBorder(Theme.padding(10, 12));
            JLabel title = Theme.boldLabel("Jobs", 15f);
            JPanel left = new JPanel(new BorderLayout(10, 0));
            left.add(title, BorderLayout.WEST);
            counts.setForeground(Theme.GREY);
            left.add(counts, BorderLayout.CENTER);
            header.add(left, BorderLayout.WEST);
            header.add(Theme.button("Clear finished", e -> clearFinished()), BorderLayout.EAST);
            add(header, BorderLayout.NORTH);

            list.setLayout(new BoxLayout(list, BoxLayout.Y_AXIS));
            JPanel holder = new JPanel(new BorderLayout());
            holder.add(list, BorderLayout.NORTH);
            holder.add(Box.createVerticalGlue(), BorderLayout.CENTER);
            JScrollPane scroll = new JScrollPane(holder);
            scroll.getVerticalScrollBar().setUnitIncrement(16);
            scroll.setBorder(null);
            add(scroll, BorderLayout.CENTER);

            new Timer(300, e -> refresh()).start();
        }

        void add(JobTracker tracker) {
            JobCard card = new JobCard(tracker, openDetails);
            cards.add(0, card);
            list.add(card, 0);
            relayout();
        }

        private void clearFinished() {
            cards.removeIf(card -> {
                if (card.isFinished()) {
                    list.remove(card);
                    return true;
                }
                return false;
            });
            relayout();
        }

        /** Lays the card list out again after cards were added or removed. */
        private void relayout() {
            list.revalidate();
            list.repaint();
            refresh();
        }

        private void refresh() {
            int running = 0;
            int finished = 0;
            for (JobCard card : cards) {
                card.refresh();
                if (card.isFinished()) {
                    finished++;
                } else {
                    running++;
                }
            }
            if (!cards.isEmpty()) {
                counts.setText(running + " running · " + finished + " finished");
            }
        }
    }

    /**
     * One submitted job: description, status, a progress bar driven by completed work on
     * the cluster, the result and the elapsed time. Clicking the card (or "Details")
     * opens the worker-level breakdown.
     */
    static final class JobCard extends JPanel {

        private final JobTracker tracker;
        private final JLabel title = new JLabel();
        private final JLabel status = Theme.chip("SUBMITTING", Theme.PURPLE);
        private final JProgressBar bar = new JProgressBar(0, 1000);
        private final JLabel result = new JLabel(" ");
        private final JLabel meta = new JLabel(" ");
        private final JLabel message = new JLabel(" ");
        private final JButton cancel = new JButton("Cancel");

        JobCard(JobTracker tracker, Consumer<JobTracker> openDetails) {
            this.tracker = tracker;
            setLayout(new BorderLayout(0, 6));
            setBorder(BorderFactory.createCompoundBorder(
                    BorderFactory.createMatteBorder(0, 0, 1, 0, UIManager.getColor("Separator.foreground")),
                    Theme.padding(10, 12)));
            setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));

            JPanel top = new JPanel(new BorderLayout(8, 0));
            top.setOpaque(false);
            title.setFont(Theme.bold(title.getFont(), 13.5f));
            top.add(title, BorderLayout.CENTER);
            top.add(status, BorderLayout.EAST);

            bar.setStringPainted(true);
            bar.setPreferredSize(new Dimension(100, 22));

            JPanel info = new JPanel(new GridLayout(3, 1, 0, 2));
            info.setOpaque(false);
            result.setFont(Theme.mono(13));
            meta.setForeground(Theme.GREY);
            message.setForeground(Theme.GREY);
            info.add(result);
            info.add(meta);
            info.add(message);

            JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 6, 0));
            buttons.setOpaque(false);
            JButton details = Theme.button("Details", e -> openDetails.accept(tracker));
            cancel.addActionListener(e -> {
                cancel.setEnabled(false);
                Thread thread = new Thread(tracker::requestCancel, "cancel-" + tracker.jobId());
                thread.setDaemon(true);
                thread.start();
            });
            buttons.add(details);
            buttons.add(cancel);

            JPanel bottom = new JPanel(new BorderLayout());
            bottom.setOpaque(false);
            bottom.add(info, BorderLayout.CENTER);
            bottom.add(buttons, BorderLayout.EAST);

            add(top, BorderLayout.NORTH);
            add(bar, BorderLayout.CENTER);
            add(bottom, BorderLayout.SOUTH);

            MouseAdapter opener = new MouseAdapter() {
                @Override
                public void mouseClicked(MouseEvent e) {
                    openDetails.accept(tracker);
                }
            };
            addMouseListener(opener);
            bar.addMouseListener(opener);
            bar.setToolTipText("Click for the per-worker breakdown");
            refresh();
        }

        boolean isFinished() {
            return tracker.view().status().isFinished();
        }

        /** Re-reads the tracker's latest view (called by a Swing timer). */
        void refresh() {
            JobView view = tracker.view();
            title.setText(view.description() + "   #" + view.shortId());
            Theme.showStatus(view, status, bar);
            JobProgress p = view.progress();
            String subtasks = p == null || p.getSubtasksTotal() == 0 ? ""
                    : String.format(" · %d/%d subtasks", p.getSubtasksDone(), p.getSubtasksTotal());
            bar.setString(String.format("%.1f%%%s", view.percent(), subtasks));

            result.setText(switch (view.status()) {
                case COMPLETED -> "Result: " + view.result();
                case FAILED -> "Failed";
                case CANCELLED -> "Cancelled";
                default -> "Result: (running)";
            });
            StringBuilder metaText = new StringBuilder("Elapsed ").append(Theme.formatMillis(view.elapsedMs()));
            if (p != null && p.hasOwner()) {
                metaText.append("   ·   Owner Worker-").append(p.getOwner().getId()).append(" (term ").append(p.getTerm())
                        .append(")");
            }
            meta.setText(metaText.toString());
            boolean resultShownAbove = view.status() == ClientJobStatus.COMPLETED;
            message.setText(resultShownAbove || view.message() == null ? " " : view.message());
            message.setForeground(switch (view.status()) {
                case RECOVERING, WAITING -> Theme.AMBER;
                case FAILED -> Theme.RED;
                default -> Theme.GREY;
            });
            cancel.setVisible(!view.status().isFinished());
        }

        @Override
        public Dimension getMaximumSize() {
            return new Dimension(Integer.MAX_VALUE, getPreferredSize().height);
        }
    }

    /**
     * Worker-level breakdown of one job: which worker ran how many subtasks, how far each
     * one is, the health of each worker, and the timeline of events (reassignments,
     * coordinator changes ...). Updates live while open.
     */
    static final class JobDetailsDialog extends JDialog {

        private final JobTracker tracker;
        private final JLabel status = Theme.chip("", Theme.GREY);
        private final JProgressBar overall = new JProgressBar(0, 1000);
        private final JTextField result = new JTextField();
        private final JLabel owner = new JLabel();
        private final JLabel elapsed = new JLabel();
        private final JLabel units = new JLabel();
        private final WorkerTableModel model = new WorkerTableModel();
        private final JTextArea events = new JTextArea();
        private final Timer timer;
        private int lastEventCount = -1;

        JobDetailsDialog(JFrame parent, JobTracker tracker) {
            super(parent, "Job details", false);
            this.tracker = tracker;
            setSize(880, 640);
            setLocationRelativeTo(parent);

            JPanel header = new JPanel(new BorderLayout(0, 8));
            header.setBorder(Theme.padding(12, 14));
            JLabel title = Theme.boldLabel(tracker.view().description() + "   #" + tracker.view().shortId(), 15f);
            JPanel titleRow = new JPanel(new BorderLayout());
            titleRow.add(title, BorderLayout.CENTER);
            titleRow.add(status, BorderLayout.EAST);
            header.add(titleRow, BorderLayout.NORTH);
            overall.setStringPainted(true);
            overall.setPreferredSize(new Dimension(100, 26));
            header.add(overall, BorderLayout.CENTER);

            JPanel facts = new JPanel(new GridLayout(2, 2, 12, 4));
            result.setEditable(false);
            result.setFont(Theme.mono(13));
            facts.add(labelled("Result", result));
            facts.add(labelled("Owner", owner));
            facts.add(labelled("Elapsed", elapsed));
            facts.add(labelled("Work units", units));
            header.add(facts, BorderLayout.SOUTH);

            JTable table = new JTable(model);
            table.setRowHeight(26);
            table.setFillsViewportHeight(true);
            table.getColumnModel().getColumn(1).setCellRenderer(new HealthRenderer());
            table.getColumnModel().getColumn(4).setCellRenderer(new BarRenderer());
            table.getColumnModel().getColumn(4).setPreferredWidth(220);
            JScrollPane tableScroll = new JScrollPane(table);
            tableScroll.setBorder(BorderFactory.createTitledBorder("Worker breakdown"));

            events.setEditable(false);
            events.setFont(Theme.mono(12));
            JScrollPane eventScroll = new JScrollPane(events);
            eventScroll.setBorder(BorderFactory.createTitledBorder("Timeline (cluster events and client notes)"));

            JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, tableScroll, eventScroll);
            split.setResizeWeight(0.5);
            split.setBorder(Theme.padding(0, 10));

            setLayout(new BorderLayout());
            add(header, BorderLayout.NORTH);
            add(split, BorderLayout.CENTER);

            timer = new Timer(400, e -> refresh());
            timer.start();
            addWindowListener(new WindowAdapter() {
                @Override
                public void windowClosed(WindowEvent e) {
                    timer.stop();
                }
            });
            setDefaultCloseOperation(DISPOSE_ON_CLOSE);
            refresh();
        }

        private static JPanel labelled(String caption, Component value) {
            JPanel panel = new JPanel(new BorderLayout(8, 0));
            JLabel label = new JLabel(caption + ":");
            label.setForeground(Theme.GREY);
            label.setPreferredSize(new Dimension(80, 20));
            panel.add(label, BorderLayout.WEST);
            panel.add(value, BorderLayout.CENTER);
            return panel;
        }

        private void refresh() {
            JobView view = tracker.view();
            Theme.showStatus(view, status, overall);
            JobProgress p = view.progress();
            overall.setString(String.format("%.2f%%%s", view.percent(), p == null ? ""
                    : String.format(" · %d of %d subtasks done", p.getSubtasksDone(), p.getSubtasksTotal())));
            String text = view.status().isFinished() && !view.result().isEmpty() ? view.result()
                    : view.status().isFinished() ? view.message() : "(running)";
            if (!result.getText().equals(text)) {
                result.setText(text);
            }
            elapsed.setText(Theme.formatMillis(view.elapsedMs()));
            if (p != null) {
                owner.setText(p.hasOwner() ? "Worker-" + p.getOwner().getId() + " (" + p.getOwner().getHost() + ":"
                        + p.getOwner().getPort() + "), term " + p.getTerm() : "-");
                units.setText(String.format("%,d / %,d", p.getUnitsDone(), p.getUnitsTotal()));
                model.setRows(p.getWorkersList());
            }
            List<String> lines = new ArrayList<>();
            if (p != null) {
                lines.addAll(p.getEventsList());
            }
            lines.add("---- client ----");
            lines.addAll(view.notes());
            if (lines.size() != lastEventCount) {
                lastEventCount = lines.size();
                events.setText(String.join("\n", lines));
                events.setCaretPosition(events.getDocument().getLength());
            }
        }

        /** Rows = workers that took part in the job. */
        private static final class WorkerTableModel extends RowsTableModel<WorkerProgress> {
            WorkerTableModel() {
                super("Worker", "Health", "Subtasks done / assigned", "Running", "Progress", "Units done / assigned");
            }

            @Override
            Object value(WorkerProgress w, int column) {
                return switch (column) {
                    case 0 -> "Worker-" + w.getWorkerId();
                    case 1 -> w.getHealth();
                    case 2 -> w.getSubtasksDone() + " / " + w.getSubtasksAssigned();
                    case 3 -> w.getSubtasksRunning();
                    case 4 -> w.getUnitsAssigned() == 0 ? 0.0 : 100.0 * w.getUnitsDone() / w.getUnitsAssigned();
                    default -> String.format("%,d / %,d", w.getUnitsDone(), w.getUnitsAssigned());
                };
            }
        }

        private static final class BarRenderer extends JProgressBar implements TableCellRenderer {
            BarRenderer() {
                super(0, 1000);
                setStringPainted(true);
                setForeground(Theme.BLUE);
            }

            @Override
            public Component getTableCellRendererComponent(JTable table, Object value, boolean selected, boolean focus,
                                                           int row, int column) {
                double percent = (Double) value;
                setValue((int) Math.round(percent * 10));
                setString(String.format("%.1f%%", percent));
                return this;
            }
        }
    }

    /**
     * Live view of the cluster from the coordinator's failure detector: every worker, its
     * health (ALIVE / SUSPECT / DEAD), JAC, current load and overlay neighbours.
     */
    static final class ClusterPanel extends JPanel {

        private final ClusterClient client;
        private final JLabel summary = new JLabel("Connecting...");
        private final WorkersModel model = new WorkersModel();
        private final ScheduledExecutorService refresher =
                Executors.newSingleThreadScheduledExecutor(daemonThreads("cluster-status"));
        private final Consumer<String> headline;

        ClusterPanel(ClusterClient client, long refreshMs, Consumer<String> headline) {
            this.client = client;
            this.headline = headline;
            setLayout(new BorderLayout(0, 6));
            setBorder(Theme.padding(8, 10));
            summary.setFont(Theme.bold(summary.getFont(), 13f));
            add(summary, BorderLayout.NORTH);
            JTable table = new JTable(model);
            table.setRowHeight(24);
            table.setFillsViewportHeight(true);
            table.getColumnModel().getColumn(3).setCellRenderer(new HealthRenderer());
            add(new JScrollPane(table), BorderLayout.CENTER);
            refresher.scheduleWithFixedDelay(this::refresh, 0, refreshMs, TimeUnit.MILLISECONDS);
        }

        private void refresh() {
            try {
                Optional<ClusterStatus> status = client.clusterStatus();
                if (status.isPresent()) {
                    ClusterStatus s = status.get();
                    long alive = s.getWorkersList().stream().filter(w -> w.getHealth() == WorkerHealth.ALIVE).count();
                    String text = String.format("Coordinator: Worker-%d (term %d)   ·   Jobs this term: %d/%d   ·   "
                                    + "Workers: %d alive of %d   ·   Running jobs: %d",
                            s.getCoordinator().getNode().getId(), s.getCoordinator().getTerm(), s.getJobsInTerm(),
                            s.getMaxJobsPerTerm(), alive, s.getWorkersCount(), s.getRunningJobs());
                    SwingUtilities.invokeLater(() -> {
                        summary.setText(text);
                        summary.setForeground(null);
                        model.setRows(s.getWorkersList());
                        headline.accept("Coordinator Worker-" + s.getCoordinator().getNode().getId() + " · term "
                                + s.getCoordinator().getTerm());
                    });
                } else {
                    List<NodeInfo> registered = client.workers();
                    String text = "No coordinator reachable right now (election in progress, or cluster not started). "
                            + registered.size() + " worker(s) registered at the Bootstrap Node.";
                    SwingUtilities.invokeLater(() -> {
                        summary.setText(text);
                        summary.setForeground(Theme.AMBER);
                        headline.accept("No coordinator");
                    });
                }
            } catch (RuntimeException e) {
                SwingUtilities.invokeLater(() -> summary.setText("Status unavailable: " + e.getMessage()));
            }
        }

        void stop() {
            refresher.shutdownNow();
        }

        private static final class WorkersModel extends RowsTableModel<WorkerSummary> {
            WorkersModel() {
                super("Worker", "Address", "Role", "Health", "JAC", "Active subtasks", "Neighbours");
            }

            @Override
            Object value(WorkerSummary w, int column) {
                return switch (column) {
                    case 0 -> "Worker-" + w.getNode().getId();
                    case 1 -> w.getNode().getHost() + ":" + w.getNode().getPort();
                    case 2 -> w.getIsCoordinator() ? "COORDINATOR" : "worker";
                    case 3 -> w.getHealth();
                    case 4 -> w.getJac();
                    case 5 -> w.getActiveSubtasks();
                    default -> w.getNeighbourIdsList().toString();
                };
            }
        }
    }

    /** Read-only table model over a list of rows with fixed column names. */
    private abstract static class RowsTableModel<T> extends AbstractTableModel {
        private final String[] columns;
        private List<T> rows = List.of();

        RowsTableModel(String... columns) {
            this.columns = columns;
        }

        void setRows(List<T> newRows) {
            rows = newRows;
            fireTableDataChanged();
        }

        /** Value of one cell of the given row. */
        abstract Object value(T row, int column);

        @Override
        public int getRowCount() {
            return rows.size();
        }

        @Override
        public int getColumnCount() {
            return columns.length;
        }

        @Override
        public String getColumnName(int column) {
            return columns[column];
        }

        @Override
        public Object getValueAt(int row, int column) {
            return value(rows.get(row), column);
        }
    }

    /** Shows a worker's health as a coloured dot and name (used by both worker tables). */
    private static final class HealthRenderer extends DefaultTableCellRenderer {
        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean selected, boolean focus,
                                                       int row, int column) {
            WorkerHealth health = (WorkerHealth) value;
            String text = health == WorkerHealth.HEALTH_UNSPECIFIED ? "UNKNOWN" : health.name();
            Component c = super.getTableCellRendererComponent(table, "● " + text, selected, focus, row, column);
            c.setForeground(Theme.healthColor(health));
            return c;
        }
    }
}
