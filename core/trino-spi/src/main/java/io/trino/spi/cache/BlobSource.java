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
package io.trino.spi.cache;

import java.io.Closeable;
import java.io.IOException;

/**
 * Lazy source of bytes backing a cached blob. Used by {@link BlobCache} to
 * populate a cache entry on miss.
 */
public interface BlobSource
        extends Closeable
{
    /**
     * Length of the content to cache. May be a caller-supplied hint (for example a length
     * recorded in table metadata) that does not match the underlying storage object.
     */
    long length()
            throws IOException;

    /**
     * Reads exactly {@code length} bytes at {@code position} of the underlying object into
     * {@code buffer} at {@code offset}, or fails without a partial read when the range is
     * not fully available.
     */
    void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException;

    /**
     * Read up to {@code length} bytes anchored at the actual end of the underlying storage
     * object and return the number of bytes actually read, which is smaller than {@code length}
     * when the object holds fewer bytes. {@link #length()} may be a caller-supplied hint that
     * does not match the storage object — file footers are read against lengths advertised by
     * table metadata, which some writers have historically gotten wrong — so implementations
     * must anchor this read at the object's true end (for example with an HTTP suffix-range
     * request) instead of computing an offset from {@link #length()}.
     */
    int readTail(byte[] buffer, int offset, int length)
            throws IOException;

    /**
     * Releases any resources held for reading. The blob cache closes the source once an entry
     * is fully cached; a blob reading through to the source closes it when the blob is closed.
     */
    @Override
    default void close()
            throws IOException
    {}
}
