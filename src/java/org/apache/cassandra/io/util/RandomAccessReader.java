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
package org.apache.cassandra.io.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.FSReadError;

public class RandomAccessReader extends AbstractDataInput implements FileDataInput
{
    public static final long CACHE_FLUSH_INTERVAL_IN_BYTES = (long) Math.pow(2, 27); // 128mb

    // default buffer size, 64Kb
    public static final int DEFAULT_BUFFER_SIZE = 65536;

    // absolute filesystem path to the file
    private final String filePath;

    // buffer which will cache file blocks
    protected ByteBuffer buffer;

    // `bufferOffset` is the offset of the beginning of the buffer
    // `markedPointer` folds the offset of the last file mark
    protected long bufferOffset, markedPointer;

    protected boolean initialized = false;

    // channel linked with the file, used to retrieve data and force updates.
    protected final SeekableByteChannel channel;

    private final long fileLength;

    protected final PoolingSegmentedFile owner;

    protected RandomAccessReader(File file, int bufferSize, PoolingSegmentedFile owner) throws FileNotFoundException
    {
        this.owner = owner;

        filePath = file.getAbsolutePath();

        try
        {
            channel = Files.newByteChannel(file.toPath(), StandardOpenOption.READ);
        }
        catch (IOException e)
        {
            throw new FileNotFoundException(filePath);
        }

        // allocating required size of the buffer
        if (bufferSize <= 0)
            throw new IllegalArgumentException("bufferSize must be positive");
        buffer = ByteBuffer.allocate(bufferSize);

        // we can cache file length in read-only mode
        try
        {
            fileLength = channel.size();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    public static RandomAccessReader open(File file, PoolingSegmentedFile owner)
    {
        return open(file, DEFAULT_BUFFER_SIZE, owner);
    }

    public static RandomAccessReader open(File file)
    {
        return open(file, DEFAULT_BUFFER_SIZE, null);
    }

    @VisibleForTesting
    static RandomAccessReader open(File file, int bufferSize, PoolingSegmentedFile owner)
    {
        try
        {
            return new RandomAccessReader(file, bufferSize, owner);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static RandomAccessReader open(SequentialWriter writer)
    {
        return open(new File(writer.getPath()), DEFAULT_BUFFER_SIZE, null);
    }

    // channel extends FileChannel, impl SeekableByteChannel.  Safe to cast.
    public FileChannel getChannel()
    {
        return (FileChannel)channel;
    }

    /**
     * Read data from file starting from current currentOffset to populate buffer.
     */
    protected void reBuffer()
    {
        resetBuffer();

        try
        {
            if (bufferOffset >= channel.size())
            {
                buffer.flip();
                return;
            }

            channel.position(bufferOffset); // setting channel position

            int read = 0;

            while (read < buffer.array().length)
            {
                int n = channel.read(buffer);

                if (n < 0)
                    break;
                read += n;
            }

            initialized = true;
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
        buffer.flip();
    }

    @Override
    public long getFilePointer()
    {
        return current();
    }

    protected long current()
    {
        // Protect against checking len after RAR is closed
        return buffer == null? bufferOffset : bufferOffset + buffer.position();
    }

    public String getPath()
    {
        return filePath;
    }

    public int getTotalBufferSize()
    {
        return buffer.capacity();
    }

    public void reset()
    {
        seek(markedPointer);
    }

    public long bytesPastMark()
    {
        long bytes = current() - markedPointer;
        assert bytes >= 0;
        return bytes;
    }

    public FileMark mark()
    {
        markedPointer = current();
        return new BufferedRandomAccessFileMark(markedPointer);
    }

    public void reset(FileMark mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        seek(((BufferedRandomAccessFileMark) mark).pointer);
    }

    public long bytesPastMark(FileMark mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        long bytes = current() - ((BufferedRandomAccessFileMark) mark).pointer;
        assert bytes >= 0;
        return bytes;
    }

    /**
     * @return true if there is no more data to read
     */
    public boolean isEOF()
    {
        return getFilePointer() == length();
    }

    public long bytesRemaining()
    {
        return length() - getFilePointer();
    }

    protected void resetBuffer()
    {
        bufferOffset += buffer.position();
        buffer.clear();
    }

    @Override
    public void close()
    {
        if (owner == null || buffer == null)
        {
            // The buffer == null check is so that if the pool owner has deallocated us, calling close()
            // will re-call deallocate rather than recycling a deallocated object.
            // I'd be more comfortable if deallocate didn't have to handle being idempotent like that,
            // but RandomAccessFile.close will call AbstractInterruptibleChannel.close which will
            // re-call RAF.close -- in this case, [C]RAR.close since we are overriding that.
            deallocate();
        }
        else
        {
            owner.recycle(this);
        }
    }

    public void deallocate()
    {
        buffer = null; // makes sure we don't use this after it's ostensibly closed

        try
        {
            channel.close();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" + "filePath='" + filePath + "')";
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class BufferedRandomAccessFileMark implements FileMark
    {
        final long pointer;

        public BufferedRandomAccessFileMark(long pointer)
        {
            this.pointer = pointer;
        }
    }

    @Override
    public void seek(long newPosition)
    {
        if (newPosition < 0)
            throw new IllegalArgumentException("new position should not be negative");

        if (newPosition > length()) // it is save to call length() in read-only mode
            throw new IllegalArgumentException(String.format("unable to seek to position %d in %s (%d bytes) in read-only mode",
                                                             newPosition, getPath(), length()));

        if (newPosition > current() + buffer.remaining() || newPosition < bufferOffset || !initialized)
        {
            // Set current location to newPosition and clear buffer so reBuffer calculates from newPosition
            bufferOffset = newPosition;
            buffer.clear();
            reBuffer();
        }

        buffer.position((int) (newPosition - bufferOffset));
    }

    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read()
    {
        if (buffer == null)
            throw new AssertionError("Attempted to read from closed RAR");

        if (isEOF())
            return -1; // required by RandomAccessFile

        if (!buffer.hasRemaining() || !initialized)
            reBuffer();

        return (int)buffer.get() & 0xff;
    }

    @Override
    public int read(byte[] buffer)
    {
        return read(buffer, 0, buffer.length);
    }

    @Override
    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read(byte[] buff, int offset, int length)
    {
        if (buffer == null)
            throw new AssertionError("Attempted to read from closed RAR");

        if (length == 0)
            return 0;

        if (isEOF())
            return -1;

        if (!buffer.hasRemaining() || !initialized)
            reBuffer();

        int toCopy = Math.min(length, buffer.remaining());
        buffer.get(buff, offset, toCopy);
        return toCopy;
    }

    public ByteBuffer readBytes(int length) throws EOFException
    {
        assert length >= 0 : "buffer length should not be negative: " + length;

        if (length > fileLength) {
            throw new EOFException();
        }

        ByteBuffer clone = ByteBuffer.allocate(length);
        int read = 0;
        try
        {
            if (!buffer.hasRemaining() || !initialized)
                reBuffer();

            while (buffer.hasRemaining() && read < length)
            {
                // Copy out remainder of what buffer has available and reBuffer
                if (length - read >= buffer.remaining())
                {
                    int start = clone.position();
                    clone.put(buffer);
                    read += clone.position() - start;
                    reBuffer();
                }
                // copy out a subset of the buffer - exit condition
                else
                {
                    int toCopy = clone.remaining();
                    clone.put(buffer.array(), buffer.position(), toCopy);

                    read += toCopy;
                    buffer.position(buffer.position() + toCopy);
                }
            }
            if (read < length) {
                throw new EOFException();
            }
        }
        catch (EOFException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new FSReadError(e, filePath);
        }
        clone.flip();
        return clone;
    }

    public long length()
    {
        return fileLength;
    }

    public long getPosition()
    {
        return bufferOffset + buffer.position();
    }

    protected void seekInternal(long position)
    {
        seek(position);
    }

    /**
    * Reads a line of text form the current position in this file. A line is
    * represented by zero or more characters followed by {@code '\n'}, {@code
    * '\r'}, {@code "\r\n"} or the end of file marker. The string does not
    * include the line terminating sequence.
    * <p>
    * Blocks until a line terminating sequence has been read, the end of the
    * file is reached or an exception is thrown.
    *
    * @return the contents of the line or {@code null} if no characters have
    *         been read before the end of the file has been reached.
    * @throws java.io.IOException
    *             if this file is closed or another I/O error occurs.
    */
    public final String readLine() throws IOException {
        StringBuilder line = new StringBuilder(80); // Typical line length
        boolean foundTerminator = false;
        long unreadPosition = 0;
        while (true) {
            int nextByte = read();
            switch (nextByte) {
                case -1:
                    return line.length() != 0 ? line.toString() : null;
                case (byte) '\r':
                    if (foundTerminator) {
                        seekInternal(unreadPosition);
                        return line.toString();
                    }
                    foundTerminator = true;
                    /* Have to be able to peek ahead one byte */
                    unreadPosition = getPosition();
                    break;
                case (byte) '\n':
                    return line.toString();
                default:
                    if (foundTerminator) {
                        seekInternal(unreadPosition);
                        return line.toString();
                    }
                    line.append((char) nextByte);
            }
        }
    }

    public int skipBytes(int n) throws IOException {
        long pos;
        long len;
        long newpos;

        if (n <= 0) {
            return 0;
        }
        pos = getPosition();
        len = length();
        newpos = pos + n;
        if (newpos > len) {
            newpos = len;
        }
        seek(newpos);

        /* return the actual number of bytes skipped */
        return (int) (newpos - pos);
    }
}
