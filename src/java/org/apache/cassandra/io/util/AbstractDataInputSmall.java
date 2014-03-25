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

public abstract class AbstractDataInputSmall extends AbstractDataInput implements DataInput
{
    protected abstract void seekInternal(int position);
    protected abstract int getPosition();

    /*
    !! DataInput methods below are copied from the implementation in Apache Harmony RandomAccessFile.
    */

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
        int unreadPosition = 0;
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
}
