package test;

import bootstrap.BootstrapNode.*;
import client.Client.*;
import common.*;
import common.Config.ConfigException;
import common.Jobs.*;
import common.Net.Protos;
import common.Primes.BigSum;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import proto.CoordinatorInfo;
import worker.Election.ClusterState;
import worker.Replication.*;
import static org.junit.jupiter.api.Assertions.*;

/** All unit tests, grouped by the part of the system they test. */
class DistriLabTest {

    @Nested
    class ClusterStateTest {

        private static CoordinatorInfo leader(int id, int term, int jac) {
            return Protos.coordinator(Protos.node(id, "h", 50050 + id), term, jac);
        }

        @Test
        void electionRuleLowestJacThenHighestId() {
            assertTrue(ClusterState.isBetter(0, 1, 3, 5), "lower JAC wins");
            assertTrue(ClusterState.isBetter(2, 5, 2, 4), "equal JAC: higher id wins");
            assertFalse(ClusterState.isBetter(2, 4, 2, 5));
        }

        @Test
        void higherTermWinsAndLowerTermIsStale() {
            ClusterState state = new ClusterState();
            state.setSelf(Protos.node(1, "h", 50051));
            assertEquals(ClusterState.AdoptResult.CHANGED, state.adopt(leader(5, 1, 0)));
            assertEquals(ClusterState.AdoptResult.SAME, state.adopt(leader(5, 1, 0)));
            assertEquals(ClusterState.AdoptResult.CHANGED, state.adopt(leader(4, 2, 7)));
            assertEquals(ClusterState.AdoptResult.STALE, state.adopt(leader(5, 1, 0)));
            assertEquals(4, state.coordinator().getNode().getId());
        }

        @Test
        void concurrentElectionsForTheSameTermConverge() {
            ClusterState a = new ClusterState();
            ClusterState b = new ClusterState();
            // the two announcements arrive in opposite orders on two workers
            a.adopt(leader(3, 2, 0));
            a.adopt(leader(4, 2, 0));
            b.adopt(leader(4, 2, 0));
            b.adopt(leader(3, 2, 0));
            assertEquals(4, a.coordinator().getNode().getId());
            assertEquals(4, b.coordinator().getNode().getId());
        }

        @Test
        void clearingKeepsTheTerm() {
            ClusterState state = new ClusterState();
            state.adopt(leader(5, 3, 0));
            state.clearCoordinator(5);
            assertFalse(state.coordinator().getKnown());
            assertEquals(3, state.term());
        }
    }

    @Nested
    class ConfigTest {

        @Test
        void commandLineOverridesDefaults() {
            Config config = Config.load(Map.of("port", "1", "name", "a"), "does-not-exist.properties",
                    new String[]{"--port=50051", "--flag", "--name", "b"});
            assertEquals(50051, config.getInt("port"));
            assertEquals("b", config.getString("name"));
            assertTrue(config.getBoolean("flag", false));
        }

        @Test
        void invalidValuesAreReported() {
            Config config = Config.of(Map.of("port", "abc", "threads", "500"));
            assertThrows(ConfigException.class, () -> config.getInt("port"));
            assertThrows(ConfigException.class, () -> config.getIntInRange("threads", 0, 256));
            assertThrows(ConfigException.class, () -> config.getString("missing"));
            assertThrows(ConfigException.class, () -> Config.load(Map.of(), "x", new String[]{"positional"}));
        }
    }

    @Nested
    class CsvLoaderTest {

        @Test
        void acceptsMixedSeparatorsCommentsAndHeader() throws Exception {
            LoadedData data = CsvLoader.parseText("value\n1, 2;3\n\n4\t5 6\n# comment\n-7,9223372036854775807", "t");
            assertEquals(8, data.count());
            assertEquals(-7, data.min());
            assertEquals(Long.MAX_VALUE, data.max());
            assertTrue(data.skippedHeader());
        }

        @Test
        void reportsBadValuesWithLineNumbers() {
            DataFormatException bad = assertThrows(DataFormatException.class, () -> CsvLoader.parseText("1,2\n3,abc", "t"));
            assertTrue(bad.getMessage().contains("Line 2"));
            assertThrows(DataFormatException.class, () -> CsvLoader.parseText("99999999999999999999", "t"));
        }

        @Test
        void generatedFilesLoadBackIdentically(@TempDir Path dir) throws Exception {
            LoadedData generated = DataGenerator.generate(dir.resolve("g.csv"), 12_345, -100, 100, 1, null);
            LoadedData loaded = CsvLoader.loadFile(dir.resolve("g.csv"));
            assertEquals(generated.count(), loaded.count());
            assertEquals(generated.max(), loaded.max());
            assertEquals(generated.min(), loaded.min());
        }
    }

    @Nested
    class JobSplittingTest {

        private static String run(Job job, int subtasks) {
            List<String> partials = new ArrayList<>();
            for (Subtask subtask : job.split(subtasks)) {
                partials.add(subtask.execute(ProgressListener.NONE, CancellationToken.NONE));
            }
            return job.combine(partials);
        }

        @Test
        void primeSumSplitMatchesSpecificationExample() {
            List<Subtask> parts = new PrimeSumJob("j", 1, 1000).split(4);
            assertEquals(4, parts.size());
            assertEquals(1, parts.get(0).rangeStart());
            assertEquals(250, parts.get(0).rangeEnd());
            assertEquals(251, parts.get(1).rangeStart());
            assertEquals(1000, parts.get(3).rangeEnd());
        }

        @Test
        void primeSumKnownValues() {
            assertEquals("37550402023", run(new PrimeSumJob("j", 1, 1_000_000), 7));
            assertEquals("17", run(new PrimeSumJob("j", -50, 10), 3));
            assertEquals("0", run(new PrimeSumJob("j", -50, -1), 3));
            assertEquals("2", run(new PrimeSumJob("j", 2, 2), 5));
        }

        @Test
        void primeSumIsIndependentOfTheNumberOfSubtasks() {
            String expected = run(new PrimeSumJob("j", 999_000, 1_234_567), 1);
            for (int n = 2; n <= 40; n += 3) {
                assertEquals(expected, run(new PrimeSumJob("j", 999_000, 1_234_567), n), "subtasks=" + n);
            }
        }

        @Test
        void listSplitIsAsEvenAsPossible() {
            long[] numbers = new long[10];
            List<Subtask> parts = new MaxJob("j", numbers).split(4);
            assertEquals(List.of(3L, 3L, 2L, 2L), parts.stream().map(Subtask::units).toList());
            assertEquals(10, new MaxJob("j", numbers).split(50).size(), "never more subtasks than values");
        }

        @Test
        void maxAndPrimeCount() {
            long[] numbers = {12, 45, 7, 998, 231, 4, 17, 89, 3, 500, -5, Long.MAX_VALUE - 1};
            assertEquals(Long.toString(Long.MAX_VALUE - 1), run(new MaxJob("j", numbers), 5));
            assertEquals("4", run(new PrimeCountJob("j", numbers), 5)); // 7, 17, 89, 3
        }

        @Test
        void invalidJobsAreRejected() {
            assertThrows(InvalidJobException.class, () -> new MaxJob("j", new long[0]).validate());
            assertThrows(InvalidJobException.class, () -> new PrimeSumJob("j", 10, 1).validate());
            assertThrows(InvalidJobException.class, () -> new PrimeSumJob("j", 1, PrimeSumJob.MAX_END + 1).validate());
        }

        @Test
        void cancellationStopsAComputation() {
            Subtask subtask = new PrimeSumJob("j", 1, 50_000_000).split(1).get(0);
            assertThrows(TaskCancelledException.class, () -> subtask.execute(ProgressListener.NONE, () -> true));
        }

        @Test
        void progressIsReportedUpToTheTotal() {
            long[] last = new long[2];
            Subtask subtask = new PrimeSumJob("j", 1, 3_000_000).split(1).get(0);
            subtask.execute((done, total) -> {
                last[0] = done;
                last[1] = total;
            }, CancellationToken.NONE);
            assertEquals(last[1], last[0]);
            assertTrue(last[1] > 0);
        }
    }

    @Nested
    class PrimesTest {

        @Test
        void smallNumbersMatchTrialDivision() {
            for (long n = -10; n < 20_000; n++) {
                assertEquals(trialDivision(n), Primes.isPrime(n), "n=" + n);
            }
        }

        @Test
        void largePrimesAreRecognised() {
            assertTrue(Primes.isPrime(1_000_000_007L));
            assertTrue(Primes.isPrime(1_000_000_000_000_000_003L));
            assertTrue(Primes.isPrime(9_223_372_036_854_775_783L)); // largest prime below 2^63
        }

        @Test
        void compositesAndCarmichaelNumbersAreRejected() {
            assertFalse(Primes.isPrime(561));                       // Carmichael number
            assertFalse(Primes.isPrime(3_215_031_751L));            // strong pseudoprime to bases 2,3,5,7
            assertFalse(Primes.isPrime(3_825_123_056_546_413_051L)); // strong pseudoprime to bases up to 23
            assertFalse(Primes.isPrime(Long.MAX_VALUE));
        }

        @Test
        void basePrimesAreCorrect() {
            int[] primes = Primes.basePrimes(30);
            assertEquals(10, primes.length);
            assertEquals(29, primes[9]);
        }

        @Test
        void bigSumNeverOverflows() {
            BigSum sum = new BigSum();
            for (int i = 0; i < 5; i++) {
                sum.add(Long.MAX_VALUE);
            }
            assertEquals(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(5)), sum.value());
        }

        private static boolean trialDivision(long n) {
            if (n < 2) {
                return false;
            }
            for (long d = 2; d * d <= n; d++) {
                if (n % d == 0) {
                    return false;
                }
            }
            return true;
        }
    }

    @Nested
    class ReplicatedJobTest {

        private static final ReplicatedJob.Spec SPEC =
                new ReplicatedJob.Spec("j", "c", JobKind.PRIMESUM, new long[0], 1, 100, 4, 0);
        private static final ReplicatedJob.Owner OLD = new ReplicatedJob.Owner(5, "h", 1, 2);
        private static final ReplicatedJob.Owner NEW = new ReplicatedJob.Owner(4, "h", 2, 3);

        @Test
        void outOfOrderDeltasMergeToTheSameState() {
            ReplicatedJob first = new ReplicatedJob("j", SPEC, OLD, ReplicatedJob.Phase.RUNNING, 1);
            ReplicatedJob part0 = new ReplicatedJob("j", null, OLD, ReplicatedJob.Phase.RUNNING, 2);
            part0.addPart(new ReplicatedJob.Part(0, "10", 5, 25));
            ReplicatedJob part2 = new ReplicatedJob("j", null, OLD, ReplicatedJob.Phase.RUNNING, 3);
            part2.addPart(new ReplicatedJob.Part(2, "30", 3, 25));

            ReplicaStore store = new ReplicaStore();
            store.apply(part2);
            store.apply(first);
            store.apply(part0);
            ReplicatedJob merged = store.get("j").orElseThrow();
            assertNotNull(merged.spec());
            assertEquals(2, merged.parts().size());
            assertEquals(3, merged.version());
        }

        @Test
        void newerOwnerWinsAndFinishedStateIsFinal() {
            ReplicaStore store = new ReplicaStore();
            store.apply(new ReplicatedJob("j", SPEC, OLD, ReplicatedJob.Phase.RUNNING, 7));
            store.apply(new ReplicatedJob("j", null, NEW, ReplicatedJob.Phase.RUNNING, 1)); // adopted in term 3
            assertEquals(4, store.get("j").orElseThrow().owner().id());
            store.apply(new ReplicatedJob("j", null, OLD, ReplicatedJob.Phase.RUNNING, 99)); // stale owner
            assertEquals(4, store.get("j").orElseThrow().owner().id());

            ReplicatedJob done = new ReplicatedJob("j", null, NEW, ReplicatedJob.Phase.RUNNING, 2);
            done.finish(ReplicatedJob.Phase.COMPLETED, "60", "", 1);
            store.apply(done);
            store.apply(new ReplicatedJob("j", null, NEW, ReplicatedJob.Phase.RUNNING, 50));
            assertEquals(ReplicatedJob.Phase.COMPLETED, store.get("j").orElseThrow().phase());
            assertEquals("60", store.get("j").orElseThrow().result());
        }

        @Test
        void protobufRoundTripKeepsEverything() {
            ReplicatedJob job = new ReplicatedJob("j", new ReplicatedJob.Spec("j", "c", JobKind.MAX, new long[]{3, -1, 9},
                    0, 0, 2, 5), OLD, ReplicatedJob.Phase.RUNNING, 4);
            job.addPart(new ReplicatedJob.Part(1, "9", 2, 2));
            ReplicatedJob copy = ReplicationMapper.fromProto(ReplicationMapper.toProto(job));
            assertEquals(3, copy.spec().numbers().length);
            assertEquals(-1, copy.spec().numbers()[1]);
            assertEquals("9", copy.parts().get(1).value());
            assertEquals(5, copy.owner().id());
            assertEquals(2, copy.owner().term());
        }
    }

    @Nested
    class WorkerRegistryTest {

        private final AtomicLong clock = new AtomicLong(1_000);
        private final WorkerRegistry registry = new WorkerRegistry(5_000, clock::get);

        @Test
        void autoAssignedIdsAreNeverReused() throws Exception {
            assertEquals(1, registry.register(0, "h", 1).id());
            assertEquals(2, registry.register(0, "h", 2).id());
            registry.remove(2);
            assertEquals(3, registry.register(0, "h", 3).id(), "a dead worker's id is not handed out again");
        }

        @Test
        void duplicateLiveIdIsRejected() throws Exception {
            registry.register(5, "h", 1);
            assertThrows(DuplicateWorkerException.class, () -> registry.register(5, "h", 2));
            registry.register(5, "h", 1); // same process re-registering is fine
        }

        @Test
        void leasesExpire() throws Exception {
            registry.register(1, "h", 1);
            registry.register(2, "h", 2);
            clock.addAndGet(3_000);
            assertTrue(registry.renew(1));
            clock.addAndGet(3_000);
            List<WorkerRegistry.Member> expired = registry.expireNow();
            assertEquals(1, expired.size());
            assertEquals(2, expired.get(0).id());
            assertFalse(registry.renew(2), "an expired worker must register again");
        }

        @Test
        void coordinatorRecordKeepsHighestTermAndOnlyLiveWorkers() throws Exception {
            registry.register(3, "h", 3);
            registry.reportCoordinator(new WorkerRegistry.CoordinatorRecord(3, "h", 3, 2, 0));
            assertFalse(registry.reportCoordinator(new WorkerRegistry.CoordinatorRecord(1, "h", 1, 1, 0)));
            assertEquals(3, registry.liveCoordinator().orElseThrow().id());
            registry.remove(3);
            assertTrue(registry.liveCoordinator().isEmpty());
        }

        @Test
        void randomPeersExcludeRequester() throws Exception {
            for (int i = 1; i <= 5; i++) {
                registry.register(i, "h", i);
            }
            List<WorkerRegistry.Member> peers = registry.randomPeers(2, List.of(1));
            assertEquals(2, peers.size());
            assertTrue(peers.stream().noneMatch(m -> m.id() == 1));
        }
    }
}
