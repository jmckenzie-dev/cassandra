/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.quicktheories.core.RandomnessSource;
import org.quicktheories.impl.Constraint;

/**
 * Provides a more uniform generator w/a longer period, replacability, and some convenience methods around seeds for reproducibility.
 */
// TODO: Upgrade to the latest version of Math3 and add in RandomAdapter to get a Random based on same seed / algo: https://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/org/apache/commons/math3/random/RandomAdaptor.html
@SuppressWarnings("JavadocReference")
public final class RandomHelpers
{
    private static final Logger logger = LoggerFactory.getLogger(RandomHelpers.class);
    private static long seed = Clock.Global.currentTimeMillis();
    private static RandomGenerator rg = new MersenneTwister(seed);
    private static final RandomnessSource srs = simpleRandomnessSource();

    public static final String USER_SEED = "cassandra.test.seed";
    @Nullable private static Long userSeed;

    @SuppressWarnings("unused")
    public static void replaceRandomGenerator(RandomGenerator newGen)
    {
        rg = newGen;
    }

    static
    {
        if (System.getenv(USER_SEED) != null)
            userSeed = Long.parseLong(System.getenv(USER_SEED));
    }

    /**
     * Will set the seed to the user defined value in env var {@link #USER_SEED} if set, otherwise current time in millis
     * @return true if set to user specified seed, false if using clock
     */
    public static boolean setSeed()
    {
        if (!maybeSetToUserSeed())
        {
            setSeed(Clock.Global.currentTimeMillis());
            return false;
        }
        return true;
    }

    @SuppressWarnings("unused")
    public static void setUserSeed(long value)
    {
        userSeed = value;
        maybeSetToUserSeed();
    }

    public static boolean maybeSetToUserSeed()
    {
        if (userSeed != null)
        {
            setSeed(userSeed);
            return true;
        }
        return false;
    }

    public static void setSeed(long newSeed)
    {
        seed = newSeed;
        rg.setSeed(seed);
    }

    public static void printSeed(String context)
    {
        logger.info("Random seed for context: " + context + ": " + seed);
    }

    public static long getSeed()
    {
        return seed;
    }

    public static String makeRandomStringBounded(int maxLength)
    {
        return makeRandomString(nextInt(maxLength));
    }

    // Creates a random string
    public static String makeRandomString(int length)
    {
        char[] chars = new char[length];
        for (int i = 0; i < length; ++i)
            chars[i++] = (char) ('a' + rg.nextInt('z' - 'a' + 1));
        return new String(chars);
    }

    public static long nextLong()
    {
        return rg.nextLong();
    }

    public static float nextFloat()
    {
        return rg.nextFloat();
    }

    public static int nextInt()
    {
        return rg.nextInt();
    }

    public static int nextInt(int bound)
    {
        return rg.nextInt(bound);
    }

    public static int randomFromRange(int min, int max)
    {
        Preconditions.checkArgument(max > min);
        int result = min;
        int toAdd = Math.round(nextFloat() * (max - min));
        result += toAdd;
        return result;
    }

    public static void nextBytes(byte[] buffer)
    {
        rg.nextBytes(buffer);
    }

    public static UUID nextUUID()
    {
        return Generators.UUID_RANDOM_GEN.generate(srs);
    }

    /**
     * TODO: Add Gen<BigInteger> support into {@link AbstractTypeGenerators#PRIMITIVE_TYPE_DATA_GENS} and call that, similar to {@link #nextUUID}
     * @param bounds Max number of bytes for the BigInteger
     */
    public static BigInteger nextBoundedBigInteger(int bounds)
    {
        return nextBigInteger(nextInt(bounds));
    }

    public static BigInteger nextBigInteger(int numBytes)
    {
        Preconditions.checkArgument(numBytes >= 0);
        if (numBytes == 0)
            return BigInteger.ZERO;

        ByteBuffer bb = ByteBuffer.wrap(new byte[numBytes]);
        for (int i = 0; i < numBytes / 4; i++)
            bb.putInt(nextInt());
        return new BigInteger(bb.array());
    }

    /**
     * TODO: Add Gen<BigDecimal> support into {@link AbstractTypeGenerators#PRIMITIVE_TYPE_DATA_GENS} and call that, similar to {@link #nextUUID}
     * @param digitBound Max number of digits to have on left side of decimal, forced to a minimum of 1 by this method
     * @param fractionalBound Max number of digits to have on right side of decimal, forced to a minimum of 1 by this method
     */
    public static BigDecimal nextBoundedBigDecimal(int digitBound, int fractionalBound)
    {
        Preconditions.checkArgument(digitBound > 0);
        Preconditions.checkArgument(fractionalBound > 0);
        int digit = nextInt(digitBound);
        int fraction = nextInt(fractionalBound);
        return nextBigDecimal(digit == 0 ? 1 : digit, fraction == 0 ? 1 : fraction);
    }

    /**
     * TODO: Add in support for digits == 0 and/or fractional == 0
     * @param digits Number of digits to left of decimal, min 1
     * @param fractional Number of digits to the right of decimal, min 1
     */
    public static BigDecimal nextBigDecimal(int digits, int fractional)
    {
        Preconditions.checkArgument(digits > 0);
        Preconditions.checkArgument(fractional > 0);
        char[] buffer = new char[digits + fractional + 1];
        int idx = 0;
        while (idx < digits + fractional + 1)
        {
            buffer[idx] = idx != digits
                ? (char)(nextInt(10) + '0')
                : '.';
            ++idx;
        }
        return new BigDecimal(buffer);
    }

    private static RandomnessSource simpleRandomnessSource()
    {
        // Minimal RandomnessSource backed by our MersenneTwister so quicktheories Gens (e.g. UUID_RANDOM_GEN)
        // can be driven from the same seeded generator. Stock quicktheories (trunk) has no SimpleRandomnessSource.
        return new RandomnessSource()
        {
            public long next(Constraint constraint)
            {
                // Callers here (Generators.UUID_RANDOM_GEN) use Constraint.none(), i.e. the full long domain,
                // so a raw draw from our seeded generator is sufficient. Constraint bounds are package-private
                // and not accessible from here, so we don't attempt to honor narrower ranges.
                return rg.nextLong();
            }

            public org.quicktheories.core.DetatchedRandomnessSource detach()
            {
                throw new UnsupportedOperationException();
            }

            public void registerFailedAssumption() {}
        };
    }

    public static <T> void shuffle(List<T> list)
    {
        for (int i = 0; i < list.size(); i++) {
            int r = nextInt(list.size());
            T temp = list.get(r);
            list.set(r, list.get(i));
            list.set(i, temp);
        }
    }
}