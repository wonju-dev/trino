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
package io.trino.blob.cache.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import com.google.inject.Inject;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheKey;
import io.trino.spi.cache.NoopBlob;
import org.weakref.jmx.Managed;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class MemoryBlobCache
        implements BlobCache
{
    private final Cache<CacheKey, Entry> cache;
    private final int maxContentLengthBytes;
    private final AtomicLong largeFileSkippedCount = new AtomicLong();

    /**
     * {@code complete} is true when {@code data} is the storage object's complete content:
     * the load saw no more bytes than the reported length, so the tail-anchored read covered
     * the object from position 0.
     */
    private record Entry(Slice data, boolean complete) {}

    @Inject
    public MemoryBlobCache(MemoryBlobCacheConfig config)
    {
        this(config.getCacheTtl(), config.getMaxSize(), config.getMaxContentLength());
    }

    private MemoryBlobCache(Duration expireAfterWrite, DataSize maxSize, DataSize maxContentLength)
    {
        checkArgument(maxContentLength.compareTo(DataSize.of(1, GIGABYTE)) <= 0, "maxContentLength must be less than or equal to 1GB");
        this.cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(maxSize.toBytes())
                .weigher((Weigher<CacheKey, Entry>) (key, value) -> toIntExact(estimatedSizeOf(key.components(), SizeOf::estimatedSizeOf) + value.data().getRetainedSize()))
                .expireAfterWrite(expireAfterWrite.toMillis(), TimeUnit.MILLISECONDS)
                .shareNothingWhenDisabled()
                .recordStats()
                .build();
        this.maxContentLengthBytes = toIntExact(maxContentLength.toBytes());
    }

    @Override
    public Blob get(CacheKey key, BlobSource source)
            throws IOException
    {
        requireNonNull(key, "key is null");
        requireNonNull(source, "source is null");
        try {
            Entry cachedEntry = getOrLoad(key, source);
            if (cachedEntry == null) {
                // The pass-through blob owns the source and closes it with the blob
                return new NoopBlob(source);
            }
            source.close();
            return new MemoryBlob(cachedEntry.data(), cachedEntry.complete());
        }
        catch (Throwable e) {
            // The cache owns the source until a blob is returned, so it must not stay open
            // when this method fails
            closeAllSuppress(e, source);
            throwIfInstanceOf(e, IOException.class);
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void invalidate(CacheKey prefix)
    {
        requireNonNull(prefix, "prefix is null");
        List<CacheKey> matching = cache.asMap().keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .collect(toImmutableList());
        cache.invalidateAll(matching);
    }

    @Managed
    public void flushCache()
    {
        cache.invalidateAll();
    }

    @Managed
    public long getHitCount()
    {
        return cache.stats().hitCount();
    }

    @Managed
    public long getMissCount()
    {
        return cache.stats().missCount();
    }

    @Managed
    public long getRequestCount()
    {
        return cache.stats().requestCount();
    }

    @Managed
    public long getLargeFileSkippedCount()
    {
        return largeFileSkippedCount.get();
    }

    @VisibleForTesting
    boolean isCached(CacheKey key)
    {
        return cache.getIfPresent(key) != null;
    }

    private Entry getOrLoad(CacheKey key, BlobSource source)
            throws IOException
    {
        Entry cached = cache.getIfPresent(key);
        if (cached != null) {
            return cached;
        }
        long length = source.length();
        if (length > maxContentLengthBytes) {
            largeFileSkippedCount.incrementAndGet();
            return null;
        }
        try {
            return cache.get(key, () -> load(source, length));
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IOException(cause);
        }
    }

    private static Entry load(BlobSource source, long length)
            throws IOException
    {
        // Populate via tail-anchored read so callers (such as Iceberg's use_file_size_from_metadata)
        // that pass a length overstating the storage object still get the whole object: the tail of
        // an object shorter than the requested length is the entire object, correctly anchored at
        // position 0. The extra byte detects the opposite mismatch.
        byte[] buffer = new byte[toIntExact(length) + 1];
        int read = source.readTail(buffer, 0, buffer.length);
        if (read <= length) {
            // Trim to the bytes actually read: wrapping the probe buffer would retain the whole
            // length+1 array (the weigher charges its retained size), so a short entry from an
            // overstated length hint would evict far more than its real size
            return new Entry(Slices.wrappedBuffer(Arrays.copyOf(buffer, read)), true);
        }
        // The storage object is longer than the reported length, so the tail-anchored bytes do not
        // start at position 0: serve the declared range read from the head instead. The entry holds
        // only the object's head, so it must not serve tail reads.
        byte[] head = new byte[toIntExact(length)];
        source.readFully(0, head, 0, head.length);
        return new Entry(Slices.wrappedBuffer(head), false);
    }
}
