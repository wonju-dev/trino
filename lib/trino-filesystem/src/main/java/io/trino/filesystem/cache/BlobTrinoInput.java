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

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.CacheKey;
import io.trino.spi.metrics.Metrics;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class BlobTrinoInput
        implements TrinoInput
{
    private final Blob blob;
    private final BlobCache cache;
    private final CacheKey key;
    private final TrinoInputFile file;
    private final AtomicLong tailCachedSize = new AtomicLong();
    private final AtomicLong tailLoadedSize = new AtomicLong();
    private boolean closed;

    BlobTrinoInput(Blob blob, BlobCache cache, CacheKey key, TrinoInputFile file)
    {
        this.blob = requireNonNull(blob, "blob is null");
        this.cache = requireNonNull(cache, "cache is null");
        this.key = requireNonNull(key, "key is null");
        this.file = requireNonNull(file, "file is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        blob.read(position, buffer, offset, length);
    }

    /**
     * Tails of blobs holding the storage object's complete content are served directly from the
     * blob. Otherwise tails are cached as separate entries under a derived key component,
     * populated from the storage object's actual end: the entry's content is the true tail, and
     * its length is at most the requested size — smaller when the length reported for the file
     * is a hint that overstates the storage object.
     */
    @Override
    public int readTail(byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(offset, length, buffer.length);
        int readSize = toIntExact(min(blob.length(), length));
        if (readSize == 0) {
            return 0;
        }
        if (blob.hasAllContent()) {
            blob.read(blob.length() - readSize, buffer, offset, readSize);
            return readSize;
        }
        TailBlobSource tailSource = new TailBlobSource(file, readSize);
        try (Blob tail = cache.get(key.append("tail#" + readSize), tailSource)) {
            int read = toIntExact(min(tail.length(), readSize));
            try {
                tail.read(0, buffer, offset, read);
                return read;
            }
            catch (EOFException e) {
                // The cache reported the requested size but the storage object holds fewer bytes.
                // The tail source memoized the actual tail while the entry was being populated, so
                // serve it from there instead of re-reading remotely
                int fallbackRead = tailSource.readTail(buffer, offset, readSize);
                tailLoadedSize.addAndGet(fallbackRead);
                return fallbackRead;
            }
            finally {
                tailCachedSize.addAndGet(tail.cachedSize());
                tailLoadedSize.addAndGet(tail.loadedSize());
            }
        }
    }

    @Override
    public Metrics getMetrics()
    {
        return new Metrics(ImmutableMap.of(
                "bytesReadFromCache", new LongCount(blob.cachedSize() + tailCachedSize.get()),
                "bytesReadExternally", new LongCount(blob.loadedSize() + tailLoadedSize.get())));
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            blob.close();
        }
        catch (Exception e) {
            throw new IOException("Could not close cached blob", e);
        }
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input closed: " + blob);
        }
    }

    @Override
    public String toString()
    {
        return blob.toString();
    }
}
