/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.PooledExecutionServiceConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.internal.statistics.DefaultStatisticsService;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.statistics.CacheStatistics;
import org.ehcache.core.statistics.TierStatistics;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration;
import org.opensearch.common.ExponentiallyWeightedMovingAverage;
import org.opensearch.common.cache.RemovalListener;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;

import java.util.Collections;

public class EhcacheDiskCachingTier<K, V> implements CachingTier<K, V>, RemovalListener<K, V> {

    private CacheManager cacheManager;
    private final Cache<K, V> cache;

    private final Class<K> keyType; // I think these are needed to pass to newCacheConfigurationBuilder
    private final Class<V> valueType;
    private final String DISK_CACHE_FP = "disk_cache_tier"; // this should probably be defined somewhere else since we need to change security.policy based on its value
    private RemovalListener<K, V> removalListener;
    private final CacheStatistics cacheStats;
    private ExponentiallyWeightedMovingAverage getTimeMillisEWMA;
    private static final double GET_TIME_EWMA_ALPHA  = 0.3; // This is the value used elsewhere in OpenSearch
    private static final int MIN_WRITE_THREADS = 0;
    private static final int MAX_WRITE_THREADS = 4; // Max number of threads for the PooledExecutionService which handles writes
    // private RBMIntKeyLookupStore keystore;
    // private CacheTierPolicy[] policies;
    // private IndicesRequestCacheDiskTierPolicy policy;

    public EhcacheDiskCachingTier(long maxWeightInBytes, long maxKeystoreWeightInBytes, Class<K> keyType, Class<V> valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
        String cacheAlias = "diskTier";
        StatisticsService statsService = new DefaultStatisticsService();

        // our EhcacheEventListener should receive events every time an entry is removed, but not when it's created
        CacheEventListenerConfigurationBuilder listenerConfig = CacheEventListenerConfigurationBuilder
            .newEventListenerConfiguration(new EhcacheEventListener(this),
                EventType.EVICTED,
                EventType.EXPIRED,
                EventType.REMOVED,
                EventType.UPDATED)
            .ordered().asynchronous(); // ordered() has some performance penalty as compared to unordered(), we can also use synchronous()

        PooledExecutionServiceConfiguration threadConfig = PooledExecutionServiceConfigurationBuilder.newPooledExecutionServiceConfigurationBuilder()
            .defaultPool("default", MIN_WRITE_THREADS, MAX_WRITE_THREADS)
            .build();

        this.cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
            .using(statsService)
            .using(threadConfig)
            .with(CacheManagerBuilder.persistence(DISK_CACHE_FP))
            .withCache(cacheAlias, CacheConfigurationBuilder.newCacheConfigurationBuilder(
                keyType, valueType, ResourcePoolsBuilder.newResourcePoolsBuilder().disk(maxWeightInBytes, MemoryUnit.B, true))
                .withService(listenerConfig) // stackoverflow shows .add(), but IDE says this is deprecated. idk
            ).build(true);
        this.cache = cacheManager.getCache(cacheAlias, keyType, valueType);
        this.cacheStats = statsService.getCacheStatistics(cacheAlias);
        this.getTimeMillisEWMA = new ExponentiallyWeightedMovingAverage(GET_TIME_EWMA_ALPHA, 10);

        // try and feed it an OpenSearch threadpool rather than its default ExecutionService?

        // this.keystore = new RBMIntKeyLookupStore((int) Math.pow(2, 28), maxKeystoreWeightInBytes);
        // this.policies = new CacheTierPolicy[]{ new IndicesRequestCacheTookTimePolicy(settings, clusterSettings) };
        // this.policy = new IndicesRequestCacheDiskTierPolicy(this.policies, true);
    }

    @Override
    public V get(K key) {
        // do we need to do the future stuff? I don't think so?

        // if (keystore.contains(key.hashCode()) {
        long now = System.nanoTime();
        V value = cache.get(key);
        double tookTimeMillis = ((double) (System.nanoTime() - now)) / 1000000;
        getTimeMillisEWMA.addValue(tookTimeMillis);
        return value;
        // }
        // return null;

    }

    @Override
    public void put(K key, V value) {
        // No need to get old value, this is handled by EhcacheEventListener.

        // CheckDataResult policyResult = policy.checkData(value)
        // if (policyResult.isAccepted()) {
        cache.put(key, value);
        // keystore.add(key.hashCode());
        // else { do something with policyResult.deniedReason()? }
        // }
    }

    @Override
    public V computeIfAbsent(K key, TieredCacheLoader<K, V> loader) throws Exception {
        return null; // should not need to fill out, Cache.computeIfAbsent is always used
    }

    @Override
    public void invalidate(K key) {
        // keep keystore check to avoid unneeded disk seek
        // RemovalNotification is handled by EhcacheEventListener

        // if (keystore.contains(key.hashCode()) {
        cache.remove(key);
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
        // this might be an expensive disk-seek call. Might be better to keep track ourselves?
        return (int) getTierStats().getMappings();
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

    private TierStatistics getTierStats() {
        return cacheStats.getTierStatistics().get("Disk");
    }

    public long getHits() {
        return getTierStats().getHits();
    }

    public long getMisses() {
        return getTierStats().getMisses();
    }

    public long getWeightBytes() {
        return getTierStats().getOccupiedByteSize();
    }

    public long getEvictions() {
        return getTierStats().getEvictions();
    }

    public double getHitRatio() {
        TierStatistics ts = getTierStats();
        long hits = ts.getHits();
        return hits / (hits + ts.getMisses());
    }

    public void close() {
        // Call this method after each test, otherwise the directory will stay locked and you won't be able to
        // initialize another IndicesRequestCache
        cacheManager.close();
    }


    // See https://stackoverflow.com/questions/45827753/listenerobject-not-found-in-imports-for-ehcache-3 for API reference
    // it's not actually documented by ehcache :(
    // This class is used to get the old value from mutating calls to the cache, and it uses those to create a RemovalNotification
    private class EhcacheEventListener implements CacheEventListener<Object, Object> { // try making these specific, but i dont think itll work
        private RemovalListener<K, V> removalListener;
        EhcacheEventListener(RemovalListener<K, V> removalListener) {
            this.removalListener = removalListener;
        }
        @Override
        public void onEvent(CacheEvent<?, ?> event) {
            // send a RemovalNotification
            K key = (K) event.getKey(); // I think these casts should be ok?
            V oldValue = (V) event.getOldValue();
            V newValue = (V) event.getNewValue();
            EventType eventType = event.getType();
            RemovalReason reason;
            switch (eventType) {
                case EVICTED:
                    reason = RemovalReason.EVICTED; // why is there both RemovalReason.EVICTED and RemovalReason.CAPACITY?
                    break;
                case EXPIRED:
                case REMOVED:
                    reason = RemovalReason.INVALIDATED;
                    // this is probably fine for EXPIRED. We use cache.remove() to invalidate keys, but this might overlap with RemovalReason.EXPLICIT?
                    break;
                case UPDATED:
                    reason = RemovalReason.REPLACED;
                    break;
                default:
                    reason = null;
            }
            // we don't subscribe to CREATED type, which is the only other option
            removalListener.onRemoval(new RemovalNotification<K, V>(key, oldValue, reason));
        }
    }
}
