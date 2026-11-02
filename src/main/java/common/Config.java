package common;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.function.Function;

/**
 * Configuration for one component (bootstrap, worker or client).
 *
 * <p>Values are resolved in this order (later wins):
 * <ol>
 *   <li>defaults built into the component</li>
 *   <li>the properties file ({@code config/worker.properties} etc., or {@code --config=path})</li>
 *   <li>command-line arguments in the form {@code --key=value} or {@code --key value}</li>
 * </ol>
 * This keeps ports, hosts, timeouts and limits out of the source code, so the
 * same jar can be started on any computer without recompiling.
 */
public final class Config {

    private final Map<String, String> values = new TreeMap<>();

    private Config() {
    }

    /**
     * Builds a configuration from defaults, an optional properties file and CLI args.
     *
     * @param defaults        built-in defaults for the component
     * @param defaultFileName file looked up in ./config/ when --config is not given
     * @param args            command-line arguments
     */
    public static Config load(Map<String, String> defaults, String defaultFileName, String[] args) {
        Config config = of(defaults);

        Map<String, String> cli = parseArgs(args);
        Path file = cli.containsKey("config")
                ? Path.of(cli.get("config"))
                : Path.of("config", defaultFileName);
        if (Files.isRegularFile(file)) {
            config.values.putAll(readProperties(file));
            config.values.put("config.file", file.toAbsolutePath().toString());
        } else if (cli.containsKey("config")) {
            throw new ConfigException("Config file not found: " + file.toAbsolutePath());
        }
        config.values.putAll(cli);
        return config;
    }

    /** Creates a configuration directly from a map (used by tests). */
    public static Config of(Map<String, String> values) {
        Config config = new Config();
        config.values.putAll(values);
        return config;
    }

    /** Parses {@code --key=value}, {@code --key value} and bare {@code --flag} arguments. */
    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> parsed = new TreeMap<>();
        if (args == null) {
            return parsed;
        }
        for (int i = 0; i < args.length; i++) {
            String arg = args[i].trim();
            if (!arg.startsWith("--") || arg.length() <= 2) {
                throw new ConfigException("Unexpected argument '" + arg + "'. Use --key=value.");
            }
            String body = arg.substring(2);
            int eq = body.indexOf('=');
            if (eq >= 0) {
                parsed.put(body.substring(0, eq).trim(), body.substring(eq + 1).trim());
            } else if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                parsed.put(body, args[++i].trim());
            } else {
                parsed.put(body, "true");
            }
        }
        return parsed;
    }

    private static Map<String, String> readProperties(Path file) {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(file)) {
            properties.load(in);
        } catch (IOException e) {
            throw new ConfigException("Cannot read config file " + file + ": " + e.getMessage(), e);
        }
        Map<String, String> map = new TreeMap<>();
        for (String name : properties.stringPropertyNames()) {
            map.put(name, properties.getProperty(name).trim());
        }
        return map;
    }

    public boolean has(String key) {
        String value = values.get(key);
        return value != null && !value.isBlank();
    }

    public String getString(String key) {
        if (!has(key)) {
            throw new ConfigException("Missing required setting '" + key + "'");
        }
        return values.get(key);
    }

    public String getString(String key, String fallback) {
        return has(key) ? values.get(key) : fallback;
    }

    public int getInt(String key) {
        return parse(key, Integer::parseInt, "an integer");
    }

    public long getLong(String key) {
        return parse(key, Long::parseLong, "a number");
    }

    /** Parses a required setting, reporting what was {@code expected} if it is malformed. */
    private <T> T parse(String key, Function<String, T> parser, String expected) {
        String raw = getString(key);
        try {
            return parser.apply(raw);
        } catch (NumberFormatException e) {
            throw new ConfigException("Setting '" + key + "' must be " + expected + " but was '" + raw + "'", e);
        }
    }

    public boolean getBoolean(String key, boolean fallback) {
        return has(key) ? Boolean.parseBoolean(values.get(key)) : fallback;
    }

    /** Integer setting that must lie in [min, max]. */
    public int getIntInRange(String key, int min, int max) {
        int value = getInt(key);
        if (value < min || value > max) {
            throw new ConfigException("Setting '" + key + "' must be between " + min + " and " + max
                    + " but was " + value);
        }
        return value;
    }

    /** Positive long setting, e.g. a timeout in milliseconds. */
    public long getPositiveLong(String key) {
        long value = getLong(key);
        if (value <= 0) {
            throw new ConfigException("Setting '" + key + "' must be > 0 but was " + value);
        }
        return value;
    }

    @Override
    public String toString() {
        return values.toString();
    }

    /** Thrown when a configuration value is missing or invalid. */
    public static class ConfigException extends RuntimeException {

        public ConfigException(String message) {
            super(message);
        }

        public ConfigException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
