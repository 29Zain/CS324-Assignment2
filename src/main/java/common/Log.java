package common;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.*;

/**
 * Small wrapper around java.util.logging that prints one readable line per event:
 * <pre>12:30:01.123 [W3] [ELECTION] Worker-5 elected for term 2</pre>
 * Every subsystem logs under its own tag so the console tells the story of
 * elections, heartbeats, failures and recoveries during a demonstration.
 */
public final class Log {

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static volatile String processTag = "--";
    /** Strong reference: java.util.logging forgets settings of loggers that are garbage collected. */
    private static final Logger GRPC_LOGGER = Logger.getLogger("io.grpc");

    private final Logger logger;
    private final String tag;

    private Log(String tag) {
        this.tag = tag;
        this.logger = Logger.getLogger("distrilab." + tag);
    }

    /** Configures console output once per process. */
    public static void init(String processTagValue, boolean debug) {
        processTag = processTagValue;
        Logger root = Logger.getLogger("");
        for (Handler handler : root.getHandlers()) {
            root.removeHandler(handler);
        }
        Level level = debug ? Level.FINE : Level.INFO;
        ConsoleHandler console = new ConsoleHandler();
        console.setFormatter(new LineFormatter());
        console.setLevel(level);
        root.addHandler(console);
        root.setLevel(level);
        // gRPC/Netty are noisy at INFO when connections drop; failures are reported by our own code.
        GRPC_LOGGER.setLevel(Level.SEVERE);
    }

    /** Changes the process tag (e.g. once a worker learns its assigned id). */
    public static void setProcessTag(String processTagValue) {
        processTag = processTagValue;
    }

    public static Log get(String tag) {
        return new Log(tag);
    }

    public void info(String message) {
        log(Level.INFO, message, null);
    }

    public void warn(String message) {
        log(Level.WARNING, message, null);
    }

    public void warn(String message, Throwable error) {
        log(Level.WARNING, message + " (" + describe(error) + ")", null);
    }

    public void error(String message, Throwable error) {
        log(Level.SEVERE, message, error);
    }

    public void debug(String message) {
        log(Level.FINE, message, null);
    }

    /** Logs under this subsystem's tag, e.g. "[ELECTION] ..."; {@code error} may be null. */
    private void log(Level level, String message, Throwable error) {
        logger.log(level, "[" + tag + "] " + message, error);
    }

    /** Short, human readable description of an exception for log lines. */
    public static String describe(Throwable error) {
        if (error == null) {
            return "unknown error";
        }
        String message = error.getMessage();
        return error.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    /** One line per record: time, process, level (if not INFO) and message. */
    private static final class LineFormatter extends Formatter {
        @Override
        public String format(LogRecord record) {
            StringBuilder line = new StringBuilder();
            line.append(LocalTime.now().format(TIME)).append(" [").append(processTag).append("] ");
            if (record.getLevel() != Level.INFO) {
                line.append(record.getLevel().getName()).append(' ');
            }
            line.append(formatMessage(record)).append(System.lineSeparator());
            if (record.getThrown() != null) {
                line.append("    ").append(describe(record.getThrown())).append(System.lineSeparator());
            }
            return line.toString();
        }
    }
}
