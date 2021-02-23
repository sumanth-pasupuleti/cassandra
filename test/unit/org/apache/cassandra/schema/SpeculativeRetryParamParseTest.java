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

package org.apache.cassandra.schema;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertEquals;

public class SpeculativeRetryParamParseTest
{
    @Test
    public void successfulParsetest()
    {
        assertEquals(SpeculativeRetryParam.none(), SpeculativeRetryParam.fromString("NONE"));
        assertEquals(SpeculativeRetryParam.none(), SpeculativeRetryParam.fromString("None"));
        assertEquals(SpeculativeRetryParam.none(), SpeculativeRetryParam.fromString("none"));
        assertEquals(SpeculativeRetryParam.always(), SpeculativeRetryParam.fromString("ALWAYS"));
        assertEquals(SpeculativeRetryParam.always(), SpeculativeRetryParam.fromString("Always"));
        assertEquals(SpeculativeRetryParam.always(), SpeculativeRetryParam.fromString("always"));
        assertEquals(SpeculativeRetryParam.custom(121.1), SpeculativeRetryParam.fromString("121.1ms"));
        assertEquals(SpeculativeRetryParam.custom(21.7), SpeculativeRetryParam.fromString("21.7MS"));
        assertEquals(SpeculativeRetryParam.percentile(21.1), SpeculativeRetryParam.fromString("21.1percentile"));
        assertEquals(SpeculativeRetryParam.percentile(10.0), SpeculativeRetryParam.fromString("10PERCENTILE"));
    }

    @Test(expected = ConfigurationException.class)
    public void failedParsetestForEmptyValue()
    {
        SpeculativeRetryParam.fromString("");
    }

    @Test(expected = ConfigurationException.class)
    public void failedParsetestForNegativePercentile()
    {
        SpeculativeRetryParam.fromString("-0.1PERCENTILE");
    }

    @Test(expected = ConfigurationException.class)
    public void failedParsetestForOver100Percentile()
    {
        SpeculativeRetryParam.fromString("100.1PERCENTILE");
    }

    @Test(expected = ConfigurationException.class)
    public void failedParsetestForInvalidString()
    {
        SpeculativeRetryParam.fromString("xyzms");
    }

    @Test(expected = ConfigurationException.class)
    public void failedParsetestForShortFormPercentilie()
    {
        SpeculativeRetryParam.fromString("21.7p");
    }
}