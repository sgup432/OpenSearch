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
import org.ehcache.core.internal.statistics.DefaultStatisticsService;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.statistics.TierStatistics;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.opensearch.common.ExponentiallyWeightedMovingAverage;
import org.opensearch.common.cache.RemovalListener;
import org.ehcache.Cache;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;

public class EhcacheDiskCachingTier implements DiskCachingTier<IndicesRequestCache.Key, BytesReference>, RemovalListener<IndicesRequestCache.Key, BytesReference> {
    // & Writeable.Reader<K> ?

    public static PersistentCacheManager cacheManager;
    private Cache<EhcacheKey, BytesReference> cache;
    //private final Class<K> keyType; // These are needed to pass to newCacheConfigurationBuilder
    //private final Class<EhcacheKey<K>> ehcacheKeyType;
    //private final Class<V> valueType;
    public final static String DISK_CACHE_FP = "disk_cache_tier"; // this should probably be defined somewhere else since we need to change security.policy based on its value
    private RemovalListener<IndicesRequestCache.Key, BytesReference> removalListener;
    private ExponentiallyWeightedMovingAverage getTimeMillisEWMA;
    private static final double GET_TIME_EWMA_ALPHA  = 0.3; // This is the value used elsewhere in OpenSearch
    private static final int MIN_WRITE_THREADS = 0;
    private static final int MAX_WRITE_THREADS = 4; // Max number of threads for the PooledExecutionService which handles writes
    private static final String cacheAlias = "diskTier";
    private final boolean isPersistent;
    private CounterMetric count; // number of entries in cache
    private final EhcacheEventListener listener;
    private final IndicesRequestCache indicesRequestCache; // only used to create new Keys
    // private RBMIntKeyLookupStore keystore;
    // private CacheTierPolicy[] policies;
    // private IndicesRequestCacheDiskTierPolicy policy;

    public EhcacheDiskCachingTier(
        boolean isPersistent,
        long maxWeightInBytes,
        long maxKeystoreWeightInBytes,
        IndicesRequestCache indicesRequestCache
        //Class<K> keyType,
        //Class<EhcacheKey<K>> ehcacheKeyType,
        //Class<V> valueType
    ) {
        this.isPersistent = isPersistent;
        //this.keyType = keyType;
        //this.ehcacheKeyType = ehcacheKeyType;
        //this.valueType = valueType;
        this.count = new CounterMetric();
        this.listener = new EhcacheEventListener(this, this);
        this.indicesRequestCache = indicesRequestCache;

        getManager();
        getOrCreateCache(isPersistent, maxWeightInBytes);
        this.getTimeMillisEWMA = new ExponentiallyWeightedMovingAverage(GET_TIME_EWMA_ALPHA, 10);

        // this.keystore = new RBMIntKeyLookupStore((int) Math.pow(2, 28), maxKeystoreWeightInBytes);
        // this.policies = new CacheTierPolicy[]{ new IndicesRequestCacheTookTimePolicy(settings, clusterSettings) };
        // this.policy = new IndicesRequestCacheDiskTierPolicy(this.policies, true);
    }

    public static void getManager() {
        // based on https://stackoverflow.com/questions/53756412/ehcache-org-ehcache-statetransitionexception-persistence-directory-already-lo
        // resolving double-initialization issue when using OpenSearchSingleNodeTestCase
        if (cacheManager == null) {
            PooledExecutionServiceConfiguration threadConfig = PooledExecutionServiceConfigurationBuilder.newPooledExecutionServiceConfigurationBuilder()
                .defaultPool("default", MIN_WRITE_THREADS, MAX_WRITE_THREADS)
                .build();

            cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
                .using(threadConfig)
                .with(CacheManagerBuilder.persistence(DISK_CACHE_FP)
                ).build(true);
        }
    }

    private void getOrCreateCache(boolean isPersistent, long maxWeightInBytes) {
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

        try {
            cache = cacheManager.createCache(cacheAlias,
                CacheConfigurationBuilder.newCacheConfigurationBuilder(
                        EhcacheKey.class, BytesReference.class, ResourcePoolsBuilder.newResourcePoolsBuilder().disk(maxWeightInBytes, MemoryUnit.B, isPersistent))
                    .withService(listenerConfig));
        } catch (IllegalArgumentException e) {
            // Thrown when the cache already exists, which may happen in test cases
            cache = cacheManager.getCache(cacheAlias, EhcacheKey.class, BytesReference.class);
        }
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

    public double getTimeMillisEWMA() {
        return getTimeMillisEWMA.getAverage();
    }

    public boolean isPersistent() {
        return isPersistent;
    }

    public IndicesRequestCache.Key convertEhcacheKeyToOriginal(EhcacheKey eKey) throws IOException {
        BytesStreamInput is = new BytesStreamInput();
        byte[] bytes = eKey.getBytes();
        is.readBytes(bytes, 0, bytes.length);
        // we somehow have to use the Reader thing in the Writeable interface
        // otherwise its not generic
        try {
            return indicesRequestCache.new Key(is);
        } catch (Exception e) {
            System.out.println("Was unable to reconstruct EhcacheKey into Key");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {
        // Call this method after each test, otherwise the directory will stay locked and you won't be able to
        // initialize another IndicesRequestCache (for example in the next test that runs)
        cacheManager.removeCache(cacheAlias);
        cacheManager.close();
    }

    public void destroy() throws Exception {
        // Close the cache and delete any persistent data associated with it
        // Might also be appropriate after standalone tests
        cacheManager.destroy();
    }
}
