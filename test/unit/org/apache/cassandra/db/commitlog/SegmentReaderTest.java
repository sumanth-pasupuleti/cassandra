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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Random;
import java.util.function.BiFunction;

import javax.crypto.Cipher;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogSegmentReader.CompressedSegmenter;
import org.apache.cassandra.db.commitlog.CommitLogSegmentReader.SyncSegment;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SegmentReaderTest
{
    static final Random random = new Random();

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.forceStaticInitialization();
    }

    @Test
    public void compressedSegmenter_LZ4() throws IOException
    {
        compressedSegmenter(LZ4Compressor.create(Collections.emptyMap()));
    }

    @Test
    public void compressedSegmenter_Snappy() throws IOException
    {
        compressedSegmenter(SnappyCompressor.create(null));
    }

    @Test
    public void compressedSegmenter_Deflate() throws IOException
    {
        compressedSegmenter(DeflateCompressor.create(null));
    }

    private void compressedSegmenter(ICompressor compressor) throws IOException
    {
        int rawSize = (1 << 15) - 137;
        ByteBuffer plainTextBuffer = compressor.preferredBufferType().allocate(rawSize);
        byte[] b = new byte[rawSize];
        random.nextBytes(b);
        plainTextBuffer.put(b);
        plainTextBuffer.flip();

        int uncompressedHeaderSize = 4;  // need to add in the plain text size to the block we write out
        int length = compressor.initialCompressedBufferLength(rawSize);
        ByteBuffer compBuffer = ByteBufferUtil.ensureCapacity(null, length + uncompressedHeaderSize, true, compressor.preferredBufferType());
        compBuffer.putInt(rawSize);
        compressor.compress(plainTextBuffer, compBuffer);
        compBuffer.flip();

        File compressedFile = FileUtils.createTempFile("compressed-segment-", ".log");
        compressedFile.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(compressedFile);
        fos.getChannel().write(compBuffer);
        fos.close();

        try (RandomAccessReader reader = RandomAccessReader.open(compressedFile))
        {
            CompressedSegmenter segmenter = new CompressedSegmenter(compressor, reader);
            int fileLength = (int) compressedFile.length();
            SyncSegment syncSegment = segmenter.nextSegment(0, fileLength);
            FileDataInput fileDataInput = syncSegment.input;
            ByteBuffer fileBuffer = readBytes(fileDataInput, rawSize);

            plainTextBuffer.flip();
            Assert.assertEquals(plainTextBuffer, fileBuffer);

            // CompressedSegmenter includes the Sync header length in the syncSegment.endPosition (value)
            Assert.assertEquals(rawSize, syncSegment.endPosition - CommitLogSegment.SYNC_MARKER_SIZE);
        }
    }

    private ByteBuffer readBytes(FileDataInput input, int len)
    {
        byte[] buf = new byte[len];
        try
        {
            input.readFully(buf);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return ByteBuffer.wrap(buf);
    }
}
