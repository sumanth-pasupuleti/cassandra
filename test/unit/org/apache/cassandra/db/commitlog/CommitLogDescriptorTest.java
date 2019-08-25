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

package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileSegmentInputStream;
import org.apache.cassandra.net.MessagingService;

public class CommitLogDescriptorTest
{
    ParameterizedClass compression;

    @Before
    public void setup()
    {
        Map<String,String> params = new HashMap<>();
        compression = new ParameterizedClass(LZ4Compressor.class.getName(), params);
    }

    @Test
    public void testVersions()
    {
        Assert.assertTrue(CommitLogDescriptor.isValid("CommitLog-1340512736956320000.log"));
        Assert.assertTrue(CommitLogDescriptor.isValid("CommitLog-2-1340512736956320000.log"));
        Assert.assertFalse(CommitLogDescriptor.isValid("CommitLog--1340512736956320000.log"));
        Assert.assertFalse(CommitLogDescriptor.isValid("CommitLog--2-1340512736956320000.log"));
        Assert.assertFalse(CommitLogDescriptor.isValid("CommitLog-2-1340512736956320000-123.log"));

        Assert.assertEquals(1340512736956320000L, CommitLogDescriptor.fromFileName("CommitLog-2-1340512736956320000.log").id);

        Assert.assertEquals(MessagingService.current_version, new CommitLogDescriptor(1340512736956320000L, null).getMessagingVersion());
        String newCLName = "CommitLog-" + CommitLogDescriptor.current_version + "-1340512736956320000.log";
        Assert.assertEquals(MessagingService.current_version, CommitLogDescriptor.fromFileName(newCLName).getMessagingVersion());
    }

    // migrated from CommitLogTest
    private void testDescriptorPersistence(CommitLogDescriptor desc) throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        CommitLogDescriptor.writeHeader(buf, desc);
        long length = buf.position();
        // Put some extra data in the stream.
        buf.putDouble(0.1);
        buf.flip();
        FileDataInput input = new FileSegmentInputStream(buf, "input", 0);
        CommitLogDescriptor read = CommitLogDescriptor.readHeader(input);
        Assert.assertEquals("Descriptor length", length, input.getFilePointer());
        Assert.assertEquals("Descriptors", desc, read);
    }

    // migrated from CommitLogTest
    @Test
    public void testDescriptorPersistence() throws IOException
    {
        testDescriptorPersistence(new CommitLogDescriptor(11, null));
        testDescriptorPersistence(new CommitLogDescriptor(CommitLogDescriptor.current_version, 13, null));
        testDescriptorPersistence(new CommitLogDescriptor(CommitLogDescriptor.current_version, 15, null));
        testDescriptorPersistence(new CommitLogDescriptor(CommitLogDescriptor.current_version, 17, new ParameterizedClass("LZ4Compressor", null)));
        testDescriptorPersistence(new CommitLogDescriptor(CommitLogDescriptor.current_version, 19,
                                                          new ParameterizedClass("StubbyCompressor", ImmutableMap.of("parameter1", "value1", "flag2", "55", "argument3", "null")
                                                          )));
    }

    // migrated from CommitLogTest
    @Test
    public void testDescriptorInvalidParametersSize() throws IOException
    {
        Map<String, String> params = new HashMap<>();
        for (int i=0; i<65535; ++i)
            params.put("key"+i, Integer.toString(i, 16));
        try {
            CommitLogDescriptor desc = new CommitLogDescriptor(CommitLogDescriptor.current_version,
                                                               21,
                                                               new ParameterizedClass("LZ4Compressor", params));

            ByteBuffer buf = ByteBuffer.allocate(1024000);
            CommitLogDescriptor.writeHeader(buf, desc);
            Assert.fail("Parameter object too long should fail on writing descriptor.");
        } catch (ConfigurationException e)
        {
            // correct path
        }
    }

    @Test
    public void constructParametersString_NoCompression()
    {
        String json = CommitLogDescriptor.constructParametersString(null, Collections.emptyMap());
        Assert.assertFalse(json.contains(CommitLogDescriptor.COMPRESSION_CLASS_KEY));

        json = CommitLogDescriptor.constructParametersString(null, Collections.emptyMap());
        Assert.assertFalse(json.contains(CommitLogDescriptor.COMPRESSION_CLASS_KEY));
    }

    @Test
    public void constructParametersString_WithCompression()
    {
        String json = CommitLogDescriptor.constructParametersString(compression, Collections.emptyMap());
        Assert.assertTrue(json.contains(CommitLogDescriptor.COMPRESSION_CLASS_KEY));
    }

    @Test
    public void writeAndReadHeader_NoCompression() throws IOException
    {
        CommitLogDescriptor descriptor = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null);
        ByteBuffer buffer = ByteBuffer.allocate(16 * 1024);
        CommitLogDescriptor.writeHeader(buffer, descriptor);
        buffer.flip();
        FileSegmentInputStream dataInput = new FileSegmentInputStream(buffer, null, 0);
        CommitLogDescriptor result = CommitLogDescriptor.readHeader(dataInput);
        Assert.assertNotNull(result);
        Assert.assertNull(result.compression);
    }

    @Test
    public void writeAndReadHeader_OnlyCompression() throws IOException
    {
        CommitLogDescriptor descriptor = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression);
        ByteBuffer buffer = ByteBuffer.allocate(16 * 1024);
        CommitLogDescriptor.writeHeader(buffer, descriptor);
        buffer.flip();
        FileSegmentInputStream dataInput = new FileSegmentInputStream(buffer, null, 0);
        CommitLogDescriptor result = CommitLogDescriptor.readHeader(dataInput);
        Assert.assertNotNull(result);
        Assert.assertEquals(compression, result.compression);
    }

    @Test
    public void equals_NoCompression()
    {
        CommitLogDescriptor desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null);
        Assert.assertEquals(desc1, desc1);

        CommitLogDescriptor desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null);
        Assert.assertEquals(desc1, desc2);

        desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null);
        Assert.assertEquals(desc1, desc1);
        desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null);
        Assert.assertEquals(desc1, desc2);

        desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null);
        Assert.assertEquals(desc1, desc1);
        desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null);
        Assert.assertEquals(desc1, desc2);
    }

    @Test
    public void equals_OnlyCompression()
    {
        CommitLogDescriptor desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression);
        Assert.assertEquals(desc1, desc1);

        CommitLogDescriptor desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression);
        Assert.assertEquals(desc1, desc2);

        desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression);
        Assert.assertEquals(desc1, desc1);
        desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression);
        Assert.assertEquals(desc1, desc2);

        desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression);
        Assert.assertEquals(desc1, desc1);
        desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression);
        Assert.assertEquals(desc1, desc2);
    }

    @Test
    public void equals_OnlyEncryption()
    {
        CommitLogDescriptor desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null);
        Assert.assertEquals(desc1, desc1);

        CommitLogDescriptor desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null);
        Assert.assertEquals(desc1, desc2);

        desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null);
        Assert.assertEquals(desc1, desc1);
        desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null);
        Assert.assertEquals(desc1, desc2);

        desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null);
        Assert.assertEquals(desc1, desc1);
        desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, null);
        Assert.assertEquals(desc1, desc2);
    }

    /**
     * Shouldn't have both enabled in real life, but ensure they are correct, nonetheless
     */
    @Test
    public void equals_BothCompressionAndEncryption()
    {
        CommitLogDescriptor desc1 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression);
        Assert.assertEquals(desc1, desc1);

        CommitLogDescriptor desc2 = new CommitLogDescriptor(CommitLogDescriptor.current_version, 1, compression);
        Assert.assertEquals(desc1, desc2);
    }
}
