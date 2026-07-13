/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.cache;

import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.spi.cache.BlobSource;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * View of the last {@code maxLength} bytes of a file, anchored at the storage object's actual
 * end, used to populate tail cache entries. {@link #length()} is an upper bound: the content is
 * shorter when the storage object holds fewer bytes than requested (for example when the file
 * length reported to the reader is a metadata hint that does not match the storage object).
 * The tail is fetched from storage once and memoized, so populating a multi-page cache entry
 * performs a single remote read.
 */
final class TailBlobSource
        implements BlobSource
{
    private final TrinoInputFile file;
    private final int maxLength;
    private byte[] tail;

    TailBlobSource(TrinoInputFile file, int maxLength)
    {
        this.file = requireNonNull(file, "file is null");
        this.maxLength = maxLength;
    }

    @Override
    public long length()
    {
        return maxLength;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        byte[] tail = loadTail();
        if (position < 0 || position > tail.length - length) {
            throw new EOFException("Cannot read %s bytes at %s. Tail of %s holds %s bytes".formatted(length, position, file.location(), tail.length));
        }
        System.arraycopy(tail, toIntExact(position), buffer, offset, length);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length)
            throws IOException
    {
        byte[] tail = loadTail();
        int readSize = min(length, tail.length);
        System.arraycopy(tail, tail.length - readSize, buffer, offset, readSize);
        return readSize;
    }

    private byte[] loadTail()
            throws IOException
    {
        if (tail == null) {
            byte[] buffer = new byte[maxLength];
            int read;
            try (TrinoInput input = file.newInput()) {
                read = input.readTail(buffer, 0, maxLength);
            }
            tail = read == maxLength ? buffer : Arrays.copyOf(buffer, read);
        }
        return tail;
    }

    @Override
    public String toString()
    {
        return file.location().toString();
    }
}
