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

import com.google.inject.Inject;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobCacheManager;
import io.trino.spi.cache.CacheCapability;
import io.trino.spi.cache.CacheKey;
import io.trino.spi.catalog.CatalogName;

import java.util.Set;

import static io.trino.spi.cache.CacheCapability.LOW_LATENCY;
import static java.util.Objects.requireNonNull;

public class InMemoryBlobCacheManager
        implements BlobCacheManager
{
    private final MemoryBlobCache sharedCache;

    @Inject
    public InMemoryBlobCacheManager(MemoryBlobCache sharedCache)
    {
        this.sharedCache = requireNonNull(sharedCache, "sharedCache is null");
    }

    @Override
    public boolean hasCapability(CacheCapability capability)
    {
        return capability == LOW_LATENCY;
    }

    @Override
    public BlobCache create(CatalogName catalog, Set<CacheCapability> capabilities)
    {
        // keys arrive with the catalog name as their first component, so catalogs can share the cache
        requireNonNull(catalog, "catalog is null");
        return sharedCache;
    }

    @Override
    public void drop(CatalogName catalog)
    {
        sharedCache.invalidate(CacheKey.of(catalog.toString()));
    }

    @Override
    public void shutdown()
    {
        sharedCache.flushCache();
    }
}
