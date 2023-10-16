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
import org.opensearch.common.ExponentiallyWeightedMovingAverage;
import org.opensearch.common.cache.RemovalListener;
import org.ehcache.Cache;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;

public class EhcacheDiskCachingTier<K extends Writeable, V> implements DiskCachingTier<K, V>, RemovalListener<K, V> {

    private final PersistentCacheManager cacheManager;
    private final Cache<EhcacheKey, V> cache;
    private final Class<K> keyType; // These are needed to pass to newCacheConfigurationBuilder
    //private final Class<EhcacheKey<K>> ehcacheKeyType;
    private final Class<V> valueType;
    public final static String DISK_CACHE_FP = "disk_cache_tier"; // this should probably be defined somewhere else since we need to change security.policy based on its value
    private RemovalListener<K, V> removalListener;
    private StatisticsService statsService; // non functional
    private ExponentiallyWeightedMovingAverage getTimeMillisEWMA;
    private static final double GET_TIME_EWMA_ALPHA  = 0.3; // This is the value used elsewhere in OpenSearch
    private static final int MIN_WRITE_THREADS = 0;
    private static final int MAX_WRITE_THREADS = 4; // Max number of threads for the PooledExecutionService which handles writes
    private static final String cacheAlias = "diskTier";
    private final boolean isPersistent;
    private CounterMetric count; // number of entries in cache
    private final EhcacheEventListener<K, V> listener;
    // private RBMIntKeyLookupStore keystore;
    // private CacheTierPolicy[] policies;
    // private IndicesRequestCacheDiskTierPolicy policy;

    public EhcacheDiskCachingTier(
        boolean isPersistent,
        long maxWeightInBytes,
        long maxKeystoreWeightInBytes,
        Class<K> keyType,
        //Class<EhcacheKey<K>> ehcacheKeyType,
        Class<V> valueType
    ) {
        this.isPersistent = isPersistent;
        this.keyType = keyType;
        //this.ehcacheKeyType = ehcacheKeyType;
        this.valueType = valueType;
        this.count = new CounterMetric();
        this.listener = new EhcacheEventListener<K, V>(this, this);
        statsService = new DefaultStatisticsService();

        // our EhcacheEventListener should receive events every time an entry is changed
        CacheEventListenerConfigurationBuilder listenerConfig = CacheEventListenerConfigurationBuilder
            .newEventListenerConfiguration(listener,
                EventType.EVICTED,
                EventType.EXPIRED,
                EventType.REMOVED,
                EventType.UPDATED,
                EventType.CREATED)
            .ordered().asynchronous(); // ordered() has some performance penalty as compared to unordered(), we can also use synchronous()

        PooledExecutionServiceConfiguration threadConfig = PooledExecutionServiceConfigurationBuilder.newPooledExecutionServiceConfigurationBuilder()
            .defaultPool("default", MIN_WRITE_THREADS, MAX_WRITE_THREADS)
            .build();

        this.cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
            .using(statsService) // https://stackoverflow.com/questions/40453859/how-to-get-ehcache-3-1-statistics
            .using(threadConfig)
            .with(CacheManagerBuilder.persistence(DISK_CACHE_FP))
            .withCache(cacheAlias, CacheConfigurationBuilder.newCacheConfigurationBuilder(
                EhcacheKey.class, valueType, ResourcePoolsBuilder.newResourcePoolsBuilder().disk(maxWeightInBytes, MemoryUnit.B, isPersistent))
                .withService(listenerConfig)
            ).build(true);
        this.cache = cacheManager.getCache(cacheAlias, EhcacheKey.class, valueType);
        this.getTimeMillisEWMA = new ExponentiallyWeightedMovingAverage(GET_TIME_EWMA_ALPHA, 10);

        // this.keystore = new RBMIntKeyLookupStore((int) Math.pow(2, 28), maxKeystoreWeightInBytes);
        // this.policies = new CacheTierPolicy[]{ new IndicesRequestCacheTookTimePolicy(settings, clusterSettings) };
        // this.policy = new IndicesRequestCacheDiskTierPolicy(this.policies, true);
    }

    @Override
    public V get(K key) throws IOException {
        // I don't think we need to do the future stuff as the cache is threadsafe

        // if (keystore.contains(key.hashCode()) {
        long now = System.nanoTime();
        V value = cache.get(new EhcacheKey(key));
        double tookTimeMillis = ((double) (System.nanoTime() - now)) / 1000000;
        getTimeMillisEWMA.addValue(tookTimeMillis);
        return value;
        // }
        // return null;
    }

    @Override
    public void put(K key, V value) throws IOException {
        // No need to get old value, this is handled by EhcacheEventListener.

        // CheckDataResult policyResult = policy.checkData(value)
        // if (policyResult.isAccepted()) {
        cache.put(new EhcacheKey(key), value);
        // keystore.add(key.hashCode());
        // else { do something with policyResult.deniedReason()? }
        // }
    }

    @Override
    public V computeIfAbsent(K key, TieredCacheLoader<K, V> loader) throws Exception {
        return null; // should not need to fill out, Cache.computeIfAbsent is always used
    }

    @Override
    public void invalidate(K key) throws IOException {
        // keep keystore check to avoid unneeded disk seek
        // RemovalNotification is handled by EhcacheEventListener

        // if (keystore.contains(key.hashCode()) {
        cache.remove(new EhcacheKey(key));
        // keystore.remove(key.hashCode());
        // }
    }

    @Override
    public V compute(K key, TieredCacheLoader<K, V> loader) throws Exception {
        return null; // should not need to fill out, Cache.compute is always used
    }

    @Override
    public void setRemovalListener(RemovalListener<K, V> removalListener) {
        this.removalListener = removalListener; // this is passed the spillover strategy, same as on-heap
    }

    @Override
    public void invalidateAll() {
        // can we just wipe the cache and start over? Or do we need to create a bunch of RemovalNotifications?
    }

    @Override
    public Iterable<K> keys() {
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
    public void onRemoval(RemovalNotification<K, V> notification) {
        removalListener.onRemoval(notification);
    }

    public double getTimeMillisEWMA() {
        return getTimeMillisEWMA.getAverage();
    }

    public boolean isPersistent() {
        return isPersistent;
    }

    public K convertEhcacheKeyToOriginal(EhcacheKey eKey) throws IOException {
        BytesStreamInput is = new BytesStreamInput();
        byte[] bytes = eKey.getBytes();
        is.readBytes(bytes, 0, bytes.length);
        // we somehow have to use the Reader thing in the Writeable interface
        // otherwise its not generic
    }

    @Override
    public void close() {
        // Call this method after each test, otherwise the directory will stay locked and you won't be able to
        // initialize another IndicesRequestCache (for example in the next test that runs)
        cacheManager.removeCache(cacheAlias);
        cacheManager.close();
    }
}
