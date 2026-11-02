package common;

import common.Config.ConfigException;
import common.Jobs.JobKind;
import io.grpc.*;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import proto.*;

/**
 * Networking helpers shared by all programs: gRPC channel pool, host:port parsing and
 * conversion between Protocol Buffer messages and the job model.
 */
public final class Net {

    private Net() {
    }

    /**
     * Reuses one gRPC channel (one HTTP/2 connection) per remote address instead of
     * opening a new connection for every call. A channel to a node that has failed is
     * discarded with {@link #invalidate}, so a restarted node is reconnected immediately.
     */
    public static final class ChannelPool implements AutoCloseable {

        /** Large jobs (millions of values) are sent in one message, so raise gRPC's 4 MB default. */
        public static final int MAX_MESSAGE_BYTES = 512 * 1024 * 1024;

        private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

        public ManagedChannel get(NodeInfo node) {
            return get(node.getHost(), node.getPort());
        }

        public ManagedChannel get(String host, int port) {
            return channels.computeIfAbsent(key(host, port), k -> ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .maxInboundMessageSize(MAX_MESSAGE_BYTES)
                    .build());
        }

        /** Drops the cached channel to a node (called after the node is suspected to have failed). */
        public void invalidate(NodeInfo node) {
            invalidate(node.getHost(), node.getPort());
        }

        public void invalidate(String host, int port) {
            ManagedChannel channel = channels.remove(key(host, port));
            if (channel != null) {
                // Graceful: calls that other threads still have in flight on this channel
                // complete (or fail) on their own; new calls get a fresh connection.
                channel.shutdown();
            }
        }

        private static String key(String host, int port) {
            return host + ":" + port;
        }

        @Override
        public void close() {
            for (ManagedChannel channel : channels.values()) {
                channel.shutdownNow();
            }
            for (ManagedChannel channel : channels.values()) {
                try {
                    channel.awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            channels.clear();
        }
    }

    /** A "host:port" address, e.g. the Bootstrap Node address given on the command line. */
    public record HostPort(String host, int port) {

        public static HostPort parse(String text) {
            if (text == null || text.isBlank()) {
                throw new ConfigException("Address is empty; expected host:port");
            }
            String value = text.trim();
            int colon = value.lastIndexOf(':');
            if (colon <= 0 || colon == value.length() - 1) {
                throw new ConfigException("Invalid address '" + value + "'; expected host:port, e.g. 192.168.1.50:50050");
            }
            try {
                int port = Integer.parseInt(value.substring(colon + 1));
                if (port < 1 || port > 65535) {
                    throw new ConfigException("Port out of range in '" + value + "'");
                }
                return new HostPort(value.substring(0, colon), port);
            } catch (NumberFormatException e) {
                throw new ConfigException("Invalid port in '" + value + "'", e);
            }
        }

        public boolean isLoopback() {
            return host.equalsIgnoreCase("localhost") || host.startsWith("127.");
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }
    }

    /** Network helpers for choosing a host name and a free port. */
    public static final class NetUtil {

        private NetUtil() {
        }

        /**
         * The local IP address used to reach {@code target}. When the cluster runs on several
         * computers, a worker must advertise this LAN address (not "localhost") to the others.
         */
        public static String detectLanAddress(String target) {
            try (DatagramSocket socket = new DatagramSocket()) {
                // UDP "connect" sends nothing; it only selects the outgoing interface.
                socket.connect(InetAddress.getByName(target), 9);
                String address = socket.getLocalAddress().getHostAddress();
                if (!address.startsWith("0.")) {
                    return address;
                }
            } catch (IOException e) {
                // fall through to the next strategy
            }
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (IOException e) {
                return "localhost";
            }
        }

        /** Asks the operating system for a free TCP port. */
        public static int findFreePort() {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                return socket.getLocalPort();
            } catch (IOException e) {
                throw new IllegalStateException("No free port available", e);
            }
        }
    }

    /** Conversions between the generated protobuf types and the plain Java domain types. */
    public static final class Protos {

        private Protos() {
        }

        public static JobType toProto(JobKind kind) {
            // Both enums use the same constant names.
            return JobType.valueOf(kind.name());
        }

        public static JobKind fromProto(JobType type) {
            try {
                return JobKind.valueOf(type.name());
            } catch (IllegalArgumentException e) {
                // JOB_TYPE_UNSPECIFIED, UNRECOGNIZED or a type this version does not know
                throw new IllegalArgumentException("Unknown job type: " + type);
            }
        }

        public static long[] toArray(List<Long> values) {
            return values.stream().mapToLong(Long::longValue).toArray();
        }

        public static Iterable<Long> asIterable(long[] values) {
            return () -> Arrays.stream(values).iterator();
        }

        public static NodeInfo node(int id, String host, int port) {
            return NodeInfo.newBuilder().setId(id).setHost(host).setPort(port).build();
        }

        /** "Worker-3 (192.168.1.51:50053)" */
        public static String label(NodeInfo node) {
            return "Worker-" + node.getId() + " (" + node.getHost() + ":" + node.getPort() + ")";
        }

        public static String shortLabel(NodeInfo node) {
            return "Worker-" + node.getId();
        }

        public static CoordinatorInfo coordinator(NodeInfo node, int term, int jac) {
            return CoordinatorInfo.newBuilder().setKnown(true).setNode(node).setTerm(term).setJac(jac).build();
        }

        public static CoordinatorInfo unknownCoordinator() {
            return CoordinatorInfo.newBuilder().setKnown(false).build();
        }
    }
}
