/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.metrics;

import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.MonotonicClockTranslation;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.DEFAULT_BUCKET_COUNT;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.LANDMARK_RESET_INTERVAL_IN_NS;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.findIndex;

/** Deterministic reservoir observations; the launcher supplies logging. No wall-clock throughput claims. */
public final class HistogramWidthOverTime
{
    private static final long[] OFFSETS = EstimatedHistogram.newOffsets(DEFAULT_BUCKET_COUNT, false);
    private static final String[] COHORTS = { "never", "once", "late420", "late900", "late1740", "perminute",
                                             "persecond", "burst100", "hot32", "stopped32" };
    private static volatile Object observed;

    public static void main(String[] args)
    {
        Options options = Options.parse(args);
        if (options.verifyOnly) { verify(); return; }
        if (options.propertyOnly) { properties(); return; }
        run(options);
    }

    private static void run(Options options)
    {
        Clock clock = new Clock();
        int perCohort = options.tables / COHORTS.length;
        CompactDecayingEstimatedHistogramReservoir[][] reservoirs = new CompactDecayingEstimatedHistogramReservoir[COHORTS.length][];
        long[][] ledgers = new long[COHORTS.length][OFFSETS.length + 1];
        long[] eventCounts = new long[COHORTS.length];
        long[] values = options.spread ? Arrays.copyOfRange(OFFSETS, 40, 52) : new long[]{ 1000 };
        for (int cohort = 0; cohort < COHORTS.length; cohort++)
        {
            reservoirs[cohort] = new CompactDecayingEstimatedHistogramReservoir[perCohort];
            for (int table = 0; table < perCohort; table++)
                reservoirs[cohort][table] = compact(clock);
        }
        System.out.printf(Locale.ROOT, "# tables=%d seconds=%d spread=%s values=%s cells_per_logical_array=%d%n",
                          options.tables, options.seconds, options.spread, Arrays.toString(values), OFFSETS.length + 1);
        System.out.println("# Synthetic identical histories within cohorts; one reservoir per logical table; single writer; stripes=2.");
        System.out.println("# Widths classify one logical cumulative array and one active decay stripe per histogram, including all-zero arrays.");
        System.out.println("# They are minimum sufficient widths, not current sparse-page or OTel widen-only backing widths.");
        System.out.println("# At each second, updates precede observations. Pre raw/cells precede getSnapshot; cumulative values from that snapshot apply to both phases.");
        System.out.println("# Normalized counts and snapshot landmark are unavailable (-1) before scraping. No samples between scheduled checkpoints.");
        System.out.println("# Every checkpoint includes a scrape, including extra boundary checkpoints; these observations can reset the landmark.");
        System.out.println("# A zero logical array can still have allocated cells. Reservoir graphs include shared offset and injected-clock objects.");
        System.out.println("sample,seconds,cohort,phase,tables,raw_max,raw_zero,raw_byte,raw_short,raw_int,raw_long,cumulative_max,cumulative_total,cumulative_zero,cumulative_byte,cumulative_short,cumulative_int,cumulative_long,normalized_max,allocated_cells,landmark_min_ns,landmark_max_ns");
        System.out.println("memory,seconds,cohort,phase,tables,reservoir_graph_bytes,marginal_bytes,shared_fixed_bytes");
        for (int second = 0; second <= options.seconds; second++)
        {
            clock.time = TimeUnit.SECONDS.toNanos(second);
            for (int cohort = 0; cohort < COHORTS.length; cohort++)
            {
                int events = events(cohort, second);
                for (int event = 0; event < events; event++)
                {
                    long value = values[(int) (eventCounts[cohort] % values.length)];
                    ledgers[cohort][findIndex(OFFSETS, value)]++;
                    eventCounts[cohort]++;
                    for (CompactDecayingEstimatedHistogramReservoir reservoir : reservoirs[cohort])
                        reservoir.update(value);
                }
            }
            if (checkpoint(second) || second == options.seconds)
                sample(second, options.seconds, reservoirs, ledgers, values);
        }
        observed = null;
        System.out.println("# width_over_time=PASS");
    }

    private static int events(int cohort, int second)
    {
        switch (cohort)
        {
            case 0: return 0;
            case 1: return second == 0 ? 1 : 0;
            case 2: return second == 420 ? 1 : 0;
            case 3: return second == 900 ? 1 : 0;
            case 4: return second == 1740 ? 1 : 0;
            case 5: return second % 60 == 0 ? 1 : 0;
            case 6: return 1;
            case 7: return second % 300 == 0 ? 100 : 0;
            case 8: return 32;
            case 9: return second < 60 ? 32 : 0;
            default: throw new AssertionError(cohort);
        }
    }

    private static boolean checkpoint(int second)
    {
        return second % 60 == 0 || second == 1801 || second == 3602 || second == 5403
               || second == 32767 || second == 32768 || second == 65535 || second == 65536;
    }

    private static boolean memoryCheckpoint(int second, int last)
    {
        return second == 0 || second == 420 || second == 900 || second == 1740 || second == 1800
               || second == 1801 || second == 3602 || second == 5403 || second == last;
    }

    private static void sample(int second, int last, CompactDecayingEstimatedHistogramReservoir[][] reservoirs,
                               long[][] ledgers, long[] values)
    {
        Stats allPre = new Stats(), allPost = new Stats();
        for (int cohort = 0; cohort < reservoirs.length; cohort++)
        {
            CompactDecayingEstimatedHistogramReservoir[] group = reservoirs[cohort];
            Stats pre = new Stats(), post = new Stats();
            long[] preMax = new long[group.length];
            int[] preCells = new int[group.length];
            for (int table = 0; table < group.length; table++)
            {
                preMax[table] = rawMax(group[table], values);
                preCells[table] = group[table].allocatedCounterCells();
            }
            if (memoryCheckpoint(second, last))
                memory(second, COHORTS[cohort], "pre", group);
            for (int table = 0; table < group.length; table++)
            {
                CompactDecayingEstimatedHistogramReservoir reservoir = group[table];
                EstimatedHistogramReservoirSnapshot snapshot = snapshot(reservoir);
                long[] cumulative = snapshot.getValues();
                if (!Arrays.equals(cumulative, ledgers[cohort]))
                    throw new AssertionError("Cumulative ledger mismatch second=" + second + " cohort=" + cohort + " table=" + table);
                if (reservoir.isContended() || reservoir.allocatedStripeCount() > 1)
                    throw new AssertionError("Single-owner fixture unexpectedly has multiple active stripes");
                pre.add(preMax[table], cumulative, -1, preCells[table], -1);
                post.add(rawMax(reservoir, values), cumulative, max(snapshot.decayingBuckets),
                         reservoir.allocatedCounterCells(), snapshot.getSnapshotLandmark());
            }
            pre.print(second, COHORTS[cohort], "pre");
            post.print(second, COHORTS[cohort], "post");
            allPre.merge(pre);
            allPost.merge(post);
            if (memoryCheckpoint(second, last))
                memory(second, COHORTS[cohort], "post", group);
        }
        allPre.print(second, "all", "pre");
        allPost.print(second, "all", "post");
    }

    private static long rawMax(CompactDecayingEstimatedHistogramReservoir reservoir, long[] values)
    {
        long max = 0;
        for (long value : values)
        {
            long[] stripes = reservoir.decayingStripeValues(value);
            if (stripes[1] != 0)
                throw new AssertionError("Secondary stripe is populated");
            if (stripes[0] < 0)
                throw new AssertionError("Weighted counter overflow");
            max = Math.max(max, stripes[0]);
        }
        return max;
    }

    private static void memory(int second, String cohort, String phase, CompactDecayingEstimatedHistogramReservoir[] roots)
    {
        Object[] fewer = Arrays.copyOf(roots, roots.length - 1, Object[].class);
        observed = new Object[]{ roots, fewer };
        long full = ObjectSizes.measureDeep(roots) - ObjectSizes.measure(roots);
        long less = ObjectSizes.measureDeep(fewer) - ObjectSizes.measure(fewer);
        long marginal = roots.length > 1 ? full - less : -1;
        long shared = roots.length > 1 ? full - roots.length * marginal : -1;
        System.out.printf(Locale.ROOT, "memory,%d,%s,%s,%d,%d,%d,%d%n", second, cohort, phase, roots.length, full, marginal, shared);
    }

    private static int width(long max)
    {
        if (max < 0) throw new AssertionError("Negative count");
        return max == 0 ? 0 : max <= Byte.MAX_VALUE ? 1 : max <= Short.MAX_VALUE ? 2 : max <= Integer.MAX_VALUE ? 3 : 4;
    }

    private static long max(long[] values)
    {
        long result = 0;
        for (long value : values)
        {
            if (value < 0) throw new AssertionError("Negative bucket count");
            result = Math.max(result, value);
        }
        return result;
    }

    private static CompactDecayingEstimatedHistogramReservoir compact(Clock clock)
    {
        return new CompactDecayingEstimatedHistogramReservoir(false, DEFAULT_BUCKET_COUNT, 2, clock, LANDMARK_RESET_INTERVAL_IN_NS);
    }

    private static EstimatedHistogramReservoirSnapshot snapshot(CompactDecayingEstimatedHistogramReservoir reservoir)
    {
        return (EstimatedHistogramReservoirSnapshot) reservoir.getSnapshot();
    }

    private static void verify()
    {
        for (int seconds : new int[]{ 420, 900, 1740 })
        {
            Pair pair = new Pair();
            pair.clock.time = TimeUnit.SECONDS.toNanos(seconds);
            pair.update(1000);
            long raw = rawMax(pair.compact, new long[]{ 1000 });
            EstimatedHistogramReservoirSnapshot snapshot = pair.check();
            if (snapshot.getSnapshotLandmark() != 0 || max(snapshot.decayingBuckets) != 1 || raw <= 127)
                throw new AssertionError("Late-first-write weighted/normalized distinction failed at " + seconds);
            if (seconds == 420 && width(raw) != 2 || seconds == 900 && width(raw) != 3 || seconds == 1740 && width(raw) != 3)
                throw new AssertionError("Unexpected late-first-write width at " + seconds + ": " + raw);
        }
        Pair mapping = new Pair();
        for (long value : OFFSETS) mapping.update(value);
        mapping.update(0);
        mapping.update(Long.MAX_VALUE);
        for (long value : OFFSETS)
            if (rawMax(mapping.compact, new long[]{ value }) != mapping.ledger[findIndex(OFFSETS, value)])
                throw new AssertionError("Raw bucket access mismatch for " + value);
        if (rawMax(mapping.compact, new long[]{ Long.MAX_VALUE }) != 1)
            throw new AssertionError("Raw overflow bucket access mismatch");
        mapping.check();
        for (boolean updateFirst : new boolean[]{ false, true })
        {
            Pair pair = new Pair();
            pair.update(1000);
            pair.clock.time = LANDMARK_RESET_INTERVAL_IN_NS;
            if (pair.check().getSnapshotLandmark() != 0)
                throw new AssertionError("Reset must be strictly after the interval");
            pair.clock.time++;
            if (updateFirst) pair.update(1000);
            if (pair.check().getSnapshotLandmark() != pair.clock.time)
                throw new AssertionError("Missing reset after interval");
        }
        System.out.println("# width_verify=PASS late_first_writes=3 strict_reset_paths=2 raw_bucket_mapping=all-plus-overflow");
    }

    private static void properties()
    {
        long steps = 0;
        for (int seed = 0; seed < 16; seed++)
        {
            Random random = new Random(0x7769647468L + seed);
            Pair pair = new Pair();
            for (int step = 0; step < 2000; step++)
            {
                pair.clock.time += random.nextBoolean() ? TimeUnit.SECONDS.toNanos(random.nextInt(121)) : random.nextInt(3);
                int count = random.nextInt(33);
                for (int i = 0; i < count; i++)
                    pair.update(OFFSETS[40 + random.nextInt(12)]);
                if (random.nextInt(100) == 0)
                {
                    pair.compact.clear();
                    pair.legacy.clear();
                    Arrays.fill(pair.ledger, 0);
                }
                pair.check();
                steps++;
            }
        }
        System.out.println("# width_properties=PASS seeds=16 steps=" + steps);
    }

    private static final class Pair
    {
        final Clock clock = new Clock();
        final CompactDecayingEstimatedHistogramReservoir compact = compact(clock);
        final DecayingEstimatedHistogramReservoir legacy = new DecayingEstimatedHistogramReservoir(false, DEFAULT_BUCKET_COUNT, 1,
                                                                                                   clock, LANDMARK_RESET_INTERVAL_IN_NS);
        final long[] ledger = new long[OFFSETS.length + 1];

        void update(long value)
        {
            compact.update(value);
            legacy.update(value);
            ledger[findIndex(OFFSETS, value)]++;
        }

        EstimatedHistogramReservoirSnapshot check()
        {
            EstimatedHistogramReservoirSnapshot actual = snapshot(compact);
            EstimatedHistogramReservoirSnapshot expected = (EstimatedHistogramReservoirSnapshot) legacy.getSnapshot();
            if (!Arrays.equals(actual.getValues(), ledger) || !Arrays.equals(expected.getValues(), ledger)
                || !Arrays.equals(actual.decayingBuckets, expected.decayingBuckets)
                || actual.getSnapshotLandmark() != expected.getSnapshotLandmark())
                throw new AssertionError("Reservoir mismatch at nanos=" + clock.time);
            return actual;
        }
    }

    private static final class Stats
    {
        long tables, rawMax, cumulativeMax, cumulativeTotal, normalizedMax = -1, cells;
        long landmarkMin = Long.MAX_VALUE, landmarkMax = -1;
        final long[] rawWidths = new long[5], cumulativeWidths = new long[5];

        void add(long raw, long[] cumulative, long normalized, long allocated, long landmark)
        {
            tables++;
            rawMax = Math.max(rawMax, raw);
            rawWidths[width(raw)]++;
            long max = max(cumulative);
            cumulativeMax = Math.max(cumulativeMax, max);
            cumulativeWidths[width(max)]++;
            for (long count : cumulative) cumulativeTotal += count;
            normalizedMax = Math.max(normalizedMax, normalized);
            cells += allocated;
            landmarkMin = Math.min(landmarkMin, landmark);
            landmarkMax = Math.max(landmarkMax, landmark);
        }

        void merge(Stats other)
        {
            tables += other.tables;
            rawMax = Math.max(rawMax, other.rawMax);
            cumulativeMax = Math.max(cumulativeMax, other.cumulativeMax);
            cumulativeTotal += other.cumulativeTotal;
            normalizedMax = Math.max(normalizedMax, other.normalizedMax);
            cells += other.cells;
            landmarkMin = Math.min(landmarkMin, other.landmarkMin);
            landmarkMax = Math.max(landmarkMax, other.landmarkMax);
            for (int i = 0; i < 5; i++) { rawWidths[i] += other.rawWidths[i]; cumulativeWidths[i] += other.cumulativeWidths[i]; }
        }

        void print(int second, String cohort, String phase)
        {
            System.out.printf(Locale.ROOT, "sample,%d,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                              second, cohort, phase, tables, rawMax, rawWidths[0], rawWidths[1], rawWidths[2], rawWidths[3], rawWidths[4],
                              cumulativeMax, cumulativeTotal, cumulativeWidths[0], cumulativeWidths[1], cumulativeWidths[2],
                              cumulativeWidths[3], cumulativeWidths[4], normalizedMax, cells, landmarkMin, landmarkMax);
        }
    }

    private static final class Clock implements MonotonicClock
    {
        long time;
        public long now() { return time; }
        public long error() { return 0; }
        public MonotonicClockTranslation translate() { throw new UnsupportedOperationException(); }
        public boolean isAfter(long instant) { return time > instant; }
        public boolean isAfter(long now, long instant) { return now > instant; }
    }

    private static final class Options
    {
        int tables = 1000, seconds = 7200;
        boolean spread, verifyOnly, propertyOnly;

        static Options parse(String[] args)
        {
            Options options = new Options();
            for (int i = 0; i < args.length; i++)
            {
                switch (args[i])
                {
                    case "--spread": options.spread = true; break;
                    case "--verify-only": options.verifyOnly = true; break;
                    case "--property-only": options.propertyOnly = true; break;
                    case "--tables":
                    case "--seconds":
                        String key = args[i];
                        if (++i == args.length) throw new IllegalArgumentException("Missing " + key + " value");
                        int value = Integer.parseInt(args[i]);
                        if (key.equals("--tables")) options.tables = value; else options.seconds = value;
                        break;
                    default: throw new IllegalArgumentException("Unknown option " + args[i]);
                }
            }
            if (options.tables < 10 || options.tables > 1000 || options.tables % 10 != 0 || options.seconds < 0 || options.seconds > 86400)
                throw new IllegalArgumentException("Tables must be10..1000 in multiples of10; seconds must be0..86400");
            if (options.verifyOnly && options.propertyOnly) throw new IllegalArgumentException("Choose one verification mode");
            return options;
        }
    }
}
