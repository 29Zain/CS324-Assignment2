package common;

import java.math.BigInteger;
import java.util.Arrays;

/**
 * Prime number utilities used by the PRIMESUM and PRIMECOUNT jobs.
 *
 * <ul>
 *   <li>{@link #isPrime(long)} - deterministic Miller-Rabin test. With the first
 *       12 primes as bases it is proven correct for every 64-bit number, and it is
 *       fast even for 18-digit values (trial division would need ~10^9 steps).</li>
 *   <li>{@link #basePrimes(int)} - classic sieve of Eratosthenes up to sqrt(limit).</li>
 *   <li>Segmented sieving (see PrimeSumSubtask) uses the base primes to cross out
 *       composites in fixed-size windows, so a range of a billion numbers is processed
 *       in seconds with constant memory.</li>
 * </ul>
 */
public final class Primes {

    /** Bases that make Miller-Rabin deterministic for all n < 3.3 * 10^24. */
    private static final long[] MR_BASES = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37};

    /** Above this value a*b may overflow a long, so BigInteger is used. */
    private static final long SAFE_MULTIPLY_LIMIT = 3_037_000_499L; // floor(sqrt(Long.MAX_VALUE))

    private static int[] cachedBasePrimes = {2};
    private static int cachedLimit = 2;

    private Primes() {
    }

    public static boolean isPrime(long n) {
        if (n < 2) {
            return false;
        }
        for (long p : MR_BASES) {
            if (n == p) {
                return true;
            }
            if (n % p == 0) {
                return false;
            }
        }
        // n - 1 = d * 2^s with d odd
        long d = n - 1;
        int s = Long.numberOfTrailingZeros(d);
        d >>= s;
        for (long a : MR_BASES) {
            if (!passesMillerRabinRound(a, d, s, n)) {
                return false;
            }
        }
        return true;
    }

    private static boolean passesMillerRabinRound(long a, long d, int s, long n) {
        long x = powMod(a % n, d, n);
        if (x == 1 || x == n - 1) {
            return true;
        }
        for (int r = 1; r < s; r++) {
            x = mulMod(x, x, n);
            if (x == n - 1) {
                return true;
            }
        }
        return false;
    }

    static long powMod(long base, long exponent, long mod) {
        long result = 1;
        long b = base % mod;
        long e = exponent;
        while (e > 0) {
            if ((e & 1) == 1) {
                result = mulMod(result, b, mod);
            }
            b = mulMod(b, b, mod);
            e >>= 1;
        }
        return result;
    }

    static long mulMod(long a, long b, long mod) {
        if (a <= SAFE_MULTIPLY_LIMIT && b <= SAFE_MULTIPLY_LIMIT) {
            return (a * b) % mod;
        }
        return BigInteger.valueOf(a).multiply(BigInteger.valueOf(b)).mod(BigInteger.valueOf(mod)).longValue();
    }

    /**
     * All primes &lt;= limit using the sieve of Eratosthenes. Results are cached
     * because every PRIMESUM subtask of a job needs the same base primes.
     */
    public static synchronized int[] basePrimes(int limit) {
        if (limit <= cachedLimit) {
            int count = upperBoundIndex(cachedBasePrimes, limit);
            return Arrays.copyOf(cachedBasePrimes, count);
        }
        boolean[] composite = new boolean[limit + 1];
        int count = 0;
        int[] primes = new int[Math.max(16, (int) (1.3 * limit / Math.max(1.0, Math.log(limit))) + 16)];
        for (int i = 2; i <= limit; i++) {
            if (!composite[i]) {
                if (count == primes.length) {
                    primes = Arrays.copyOf(primes, primes.length * 2);
                }
                primes[count++] = i;
                for (long j = (long) i * i; j <= limit; j += i) {
                    composite[(int) j] = true;
                }
            }
        }
        cachedBasePrimes = Arrays.copyOf(primes, count);
        cachedLimit = limit;
        return cachedBasePrimes.clone();
    }

    /** Number of entries in the sorted array that are &lt;= value. */
    private static int upperBoundIndex(int[] sorted, int value) {
        int index = Arrays.binarySearch(sorted, value);
        return index >= 0 ? index + 1 : -index - 1;
    }

    /** floor(sqrt(n)) for n &gt;= 0 without floating point rounding errors. */
    public static long isqrt(long n) {
        if (n < 0) {
            throw new IllegalArgumentException("negative: " + n);
        }
        long r = (long) Math.sqrt((double) n);
        while (r * r > n) {
            r--;
        }
        while ((r + 1) * (r + 1) <= n) {
            r++;
        }
        return r;
    }

    /**
     * Overflow-safe running sum.
     *
     * <p>The sum of all primes up to ~2 * 10^10 already exceeds Long.MAX_VALUE, which
     * silently produced wrong answers in Assignment 1. This class adds in a fast
     * {@code long} and spills into a {@link BigInteger} only when an addition would
     * overflow, so it stays fast and is always exact.
     */
    public static final class BigSum {

        private long small;
        private BigInteger big = BigInteger.ZERO;

        public void add(long value) {
            long result = small + value;
            // Overflow happened iff both operands have the same sign and the result's sign differs.
            if (((small ^ result) & (value ^ result)) < 0) {
                big = big.add(BigInteger.valueOf(small));
                small = value;
            } else {
                small = result;
            }
        }

        public BigInteger value() {
            return big.add(BigInteger.valueOf(small));
        }

        @Override
        public String toString() {
            return value().toString();
        }
    }
}
