/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.memtable;

import org.junit.Test;

import org.apache.cassandra.db.memtable.IdleMemtableFlusher.AdmissionBudget;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IdleFlushAdmissionBudgetTest
{
    @Test
    public void countCreditRefillsAndDoesNotAccumulateAcrossLongSilence()
    {
        AdmissionBudget budget = new AdmissionBudget(2, 100, 0);
        assertTrue(budget.available(0));
        budget.charge(0);
        assertTrue(budget.available(0));
        budget.charge(0);
        assertFalse(budget.available(0));
        assertFalse(budget.available(499_999_999));
        assertTrue(budget.available(500_000_000));
        budget.charge(0);
        assertFalse(budget.available(500_000_000));
        assertTrue(budget.available(100_000_000_000L));
        budget.charge(0);
        assertTrue(budget.available(100_000_000_000L));
        budget.charge(0);
        assertFalse(budget.available(100_000_000_000L));
    }

    @Test
    public void oversizedFlushRepaysDebtAndRequiresPositiveByteCredit()
    {
        AdmissionBudget budget = new AdmissionBudget(100, 100, 0);
        assertTrue(budget.available(0));
        budget.charge(350);
        assertFalse(budget.available(0));
        assertFalse(budget.available(2_500_000_000L));
        assertTrue(budget.available(2_500_000_001L));
        budget.charge(100);
        assertFalse(budget.available(2_500_000_001L));
        assertTrue(budget.available(100_000_000_000L));
        budget.charge(100);
        assertFalse(budget.available(100_000_000_000L));
    }

    @Test
    public void monotonicClockWrapAndBackwardObservations()
    {
        long start = Long.MAX_VALUE - 100;
        AdmissionBudget budget = new AdmissionBudget(1, 100, start);
        budget.charge(100);
        assertFalse(budget.available(start - 1));
        assertFalse(budget.available(start));
        assertTrue(budget.available(start + 1_000_000_000L));
    }

    @Test
    public void extremeSizeRemainsDebtWithoutOverflow()
    {
        AdmissionBudget budget = new AdmissionBudget(Integer.MAX_VALUE, 1, 0);
        budget.charge(Long.MAX_VALUE);
        assertFalse(budget.available(Long.MAX_VALUE));
        AdmissionBudget fast = new AdmissionBudget(Integer.MAX_VALUE, Long.MAX_VALUE, 0);
        fast.charge(Long.MAX_VALUE);
        assertFalse(fast.available(0));
        assertTrue(fast.available(1_000_000_000));
    }

    @Test
    public void invalidRatesAreRejected()
    {
        for (int rate : new int[] { 0, -1, Integer.MIN_VALUE })
            assertThatThrownBy(() -> new AdmissionBudget(rate, 1, 0)).isInstanceOf(IllegalArgumentException.class);
        for (double rate : new double[] { 0, -1, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY })
            assertThatThrownBy(() -> new AdmissionBudget(1, rate, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
