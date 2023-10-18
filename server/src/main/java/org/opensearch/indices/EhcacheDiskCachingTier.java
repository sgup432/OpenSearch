/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.PooledExecutionServiceConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.event.EventType;
import org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration;
import org.opensearch.common.ExponentiallyWeightedMovingAverage;
import org.opensearch.common.cache.RemovalListener;
import org.ehcache.Cache;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

public class EhcacheDiskCachingTier implements DiskCachingTier<IndicesRequestCache.Key, BytesReference>, RemovalListener<IndicesRequestCache.Key, BytesReference> {

    public static HashMap<String, PersistentCacheManager> cacheManagers = new HashMap<>();
    // Because of the way test cases are set up, each node may try to instantiate several disk caching tiers.
    // Only one of them will be used, but there will be initialization errors when multiple cache managers try to
    // use the same file path and create/get caches with the same alias. We resolve this with a static reference
    // to a cache manager, which is populated if it is null and reused if it is not.
    // (See https://stackoverflow.com/questions/53756412/ehcache-org-ehcache-statetransitionexception-persistence-directory-already-lo)
    // To deal with IT cases, we need to create a manager per node, as otherwise nodes will try to reuse the same manager,
    // so we get the correct cache manager by looking up the node ID in this map.
    // I don't think any of this can happen in production, because nodes shouldn't share a JVM,
    // and they should only instantiate their services once? But it's best to resolve it anyway.

    private PersistentCacheManager cacheManager; // This is the manager this tier will actually use
    private Cache<EhcacheKey, BytesReference> cache;
    public final static String BASE_DISK_CACHE_FP = "disk_cache_tier";
    // Placeholder. this should probably be defined somewhere else, since we need to change security.policy based on its value
    // To accomodate test setups, where multiple nodes may exist on the same filesystem, we add the node ID to the end of this
    // These will be subfolders of BASE_DISK_CACHE_FP
    private final String diskCacheFP; // the one to use for this node
    private RemovalListener<IndicesRequestCache.Key, BytesReference> removalListener;
    private ExponentiallyWeightedMovingAverage getTimeMillisEWMA;
    private static final double GET_TIME_EWMA_ALPHA  = 0.3; // This is the value used elsewhere in OpenSearch
    private static final int MIN_WRITE_THREADS = 0;
    private static final int MAX_WRITE_THREADS = 4; // Max number of threads for the PooledExecutionService which handles writes
    private static final String cacheAlias = "diskTier";
    private CounterMetric count; // number of entries in cache
    private final EhcacheEventListener listener;
    private final IndicesRequestCache indicesRequestCache; // only used to create new Keys
    private final String nodeId;
    // private RBMIntKeyLookupStore keystore;
    // private CacheTierPolicy[] policies;
    // private IndicesRequestCacheDiskTierPolicy policy;

    public EhcacheDiskCachingTier(
        long maxWeightInBytes,
        long maxKeystoreWeightInBytes,
        IndicesRequestCache indicesRequestCache,
        String nodeId
    ) {
        this.count = new CounterMetric();
        this.listener = new EhcacheEventListener(this, this);
        this.indicesRequestCache = indicesRequestCache;
        this.nodeId = nodeId;
        this.diskCacheFP = PathUtils.get(BASE_DISK_CACHE_FP, nodeId).toString();
        // I know this function warns it shouldn't often be used, we can fix it to use the roots once we pick a final FP

        getManager();
        try {
            cacheManager.destroyCache(cacheAlias);
        } catch (Exception e) {
            System.out.println("Unable to destroy cache!!");
            e.printStackTrace();
            // do actual logging later
        }
        createCache(maxWeightInBytes);
        this.getTimeMillisEWMA = new ExponentiallyWeightedMovingAverage(GET_TIME_EWMA_ALPHA, 10);

        // this.keystore = new RBMIntKeyLookupStore((int) Math.pow(2, 28), maxKeystoreWeightInBytes);
        // this.policies = new CacheTierPolicy[]{ new IndicesRequestCacheTookTimePolicy(settings, clusterSettings) };
        // this.policy = new IndicesRequestCacheDiskTierPolicy(this.policies, true);
    }

    public void getManager() {
        // based on https://stackoverflow.com/questions/53756412/ehcache-org-ehcache-statetransitionexception-persistence-directory-already-lo
        // resolving double-initialization issue when using OpenSearchSingleNodeTestCase
        PersistentCacheManager oldCacheManager = cacheManagers.get(nodeId);
        if (oldCacheManager != null) {
            try {
                try {
                    oldCacheManager.close();
                } catch (IllegalStateException e) {
                    System.out.println("Cache was uninitialized, skipping close() and moving to destroy()");
                }
                oldCacheManager.destroy();
            } catch (Exception e) {
                System.out.println("Was unable to destroy existing cache manager");
                e.printStackTrace();
                // actual logging later
            }
        }
        PooledExecutionServiceConfiguration threadConfig = PooledExecutionServiceConfigurationBuilder.newPooledExecutionServiceConfigurationBuilder()
            .defaultPool("default", MIN_WRITE_THREADS, MAX_WRITE_THREADS)
            .build();

        cacheManagers.put(nodeId,
            CacheManagerBuilder.newCacheManagerBuilder()
            .using(threadConfig)
            .with(CacheManagerBuilder.persistence(diskCacheFP)
            ).build(true)
        );
        this.cacheManager = cacheManagers.get(nodeId);
    }

    private void createCache(long maxWeightInBytes) {
        // our EhcacheEventListener should receive events every time an entry is changed
        CacheEventListenerConfigurationBuilder listenerConfig = CacheEventListenerConfigurationBuilder
            .newEventListenerConfiguration(listener,
                EventType.EVICTED,
                EventType.EXPIRED,
                EventType.REMOVED,
                EventType.UPDATED,
                EventType.CREATED)
            .ordered().asynchronous();
        // ordered() has some performance penalty as compared to unordered(), we can also use synchronous()

        cache = cacheManager.createCache(cacheAlias,
            CacheConfigurationBuilder.newCacheConfigurationBuilder(
                    EhcacheKey.class, BytesReference.class, ResourcePoolsBuilder.newResourcePoolsBuilder().disk(maxWeightInBytes, MemoryUnit.B, false))
                .withService(listenerConfig));
    }

    @Override
    public BytesReference get(IndicesRequestCache.Key key)  {
        // I don't think we need to do the future stuff as the cache is threadsafe

        // if (keystore.contains(key.hashCode()) {
        long now = System.nanoTime();
        BytesReference value = null;
        try {
            value = cache.get(new EhcacheKey(key));
        } catch (IOException ignored) { // do smth with this later
        }
        double tookTimeMillis = ((double) (System.nanoTime() - now)) / 1000000;
        getTimeMillisEWMA.addValue(tookTimeMillis);
        return value;
        // }
        // return null;
    }

    @Override
    public void put(IndicesRequestCache.Key key, BytesReference value) {
        // No need to get old value, this is handled by EhcacheEventListener.

        // CheckDataResult policyResult = policy.checkData(value)
        // if (policyResult.isAccepted()) {
        try {
            cache.put(new EhcacheKey(key), value);
        } catch (IOException ignored) { // do smth with this later
        }
        // keystore.add(key.hashCode());
        // else { do something with policyResult.deniedReason()? }
        // }
    }

    @Override
    public BytesReference computeIfAbsent(IndicesRequestCache.Key key, TieredCacheLoader<IndicesRequestCache.Key, BytesReference> loader) throws Exception {
        return null; // should not need to fill out, Cache.computeIfAbsent is always used
    }

    @Override
    public void invalidate(IndicesRequestCache.Key key) {
        // keep keystore check to avoid unneeded disk seek
        // RemovalNotification is handled by EhcacheEventListener

        // if (keystore.contains(key.hashCode()) {
        try {
            cache.remove(new EhcacheKey(key));
        } catch (IOException ignored) { // do smth with this later
        }
        // keystore.remove(key.hashCode());
        // }
    }

    @Override
    public BytesReference compute(IndicesRequestCache.Key key, TieredCacheLoader<IndicesRequestCache.Key, BytesReference> loader) throws Exception {
        return null; // should not need to fill out, Cache.compute is always used
    }

    @Override
    public void setRemovalListener(RemovalListener<IndicesRequestCache.Key, BytesReference> removalListener) {
        this.removalListener = removalListener; // this is passed the spillover strategy, same as on-heap
    }

    @Override
    public void invalidateAll() {
        // can we just wipe the cache and start over? Or do we need to create a bunch of RemovalNotifications?
    }

    @Override
    public Iterable<IndicesRequestCache.Key> keys() {
        // ehcache doesn't provide a method like this, because it might be a huge number of keys that consume all
        // the memory. Do we want this method for disk tier?
        return Collections::emptyIterator;
    }

    @Override
    public int count() {
        return (int) count.count();
    }

    protected void countInc() {
        count.inc();
    }
    protected void countDec() {
        count.dec();
    }

    @Override
    public TierType getTierType() {
        return TierType.DISK;
    }

    @Override
    public void onRemoval(RemovalNotification<IndicesRequestCache.Key, BytesReference> notification) {
        removalListener.onRemoval(notification);
    }

    @Override
    public double getTimeMillisEWMA() {
        return getTimeMillisEWMA.getAverage();
    }

    public IndicesRequestCache.Key convertEhcacheKeyToOriginal(EhcacheKey eKey) throws IOException {
        BytesStreamInput is = new BytesStreamInput();
        byte[] bytes = eKey.getBytes();
        is.readBytes(bytes, 0, bytes.length);
        try {
            return indicesRequestCache.new Key(is);
        } catch (Exception e) {
            System.out.println("Was unable to reconstruct EhcacheKey into Key");
            e.printStackTrace();
            // actual logging later
        }
        return null;
    }

    @Override
    public void close() {
        // Should be called after each test
        cacheManager.removeCache(cacheAlias);
        cacheManager.close();
    }

    public void destroy() throws Exception {
        // Close the cache and delete any persistent data associated with it
        // Might also be appropriate after standalone tests
        cacheManager.close();
        cacheManager.destroy();
    }
}
