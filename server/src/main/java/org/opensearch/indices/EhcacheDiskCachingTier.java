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
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.internal.statistics.DefaultStatisticsService;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.statistics.CacheStatistics;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.opensearch.common.cache.RemovalListener;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;

import java.util.Collections;

public class EhcacheDiskCachingTier<K, V> implements CachingTier<K, V>, RemovalListener<K, V> {

    private CacheManager cacheManager;
    private Cache<K, V> cache;

    private final Class<K> keyType; // I think these are needed to pass to newCacheConfigurationBuilder
    private final Class<V> valueType;
    private final String DISK_CACHE_FP = "disk_cache_tier";
    private RemovalListener<K, V> removalListener;
    private final StatisticsService statsService;
    private final CacheStatistics cacheStats;
    // private RBMIntKeyLookupStore keystore;
    // private CacheTierPolicy[] policies;
    // private IndicesRequestCacheDiskTierPolicy policy;
    //private RemovalListener<K, V> removalListener = notification -> {}; // based on on-heap

    public EhcacheDiskCachingTier(long maxWeightInBytes, long maxKeystoreWeightInBytes, Class<K> keyType, Class<V> valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
        String cacheAlias = "diskTier";
        this.statsService = new DefaultStatisticsService();

        // our EhcacheEventListener should receive events every time an entry is removed for some reason, but not when it's created
        CacheEventListenerConfigurationBuilder listenerConfig = CacheEventListenerConfigurationBuilder
            .newEventListenerConfiguration(new EhcacheEventListener(this),
                EventType.EVICTED,
                EventType.EXPIRED,
                EventType.REMOVED,
                EventType.UPDATED)
            .ordered().asynchronous(); // ordered() has some performance penalty as compared to unordered(), we can also use synchronous()

        this.cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
            .using(statsService)
            .with(CacheManagerBuilder.persistence(DISK_CACHE_FP))
            .withCache(cacheAlias, CacheConfigurationBuilder.newCacheConfigurationBuilder(
                keyType, valueType, ResourcePoolsBuilder.newResourcePoolsBuilder().disk(maxWeightInBytes, MemoryUnit.B, true))
                .withService(listenerConfig)
            ).build(true);
        this.cache = cacheManager.getCache(cacheAlias, keyType, valueType);
        this.cacheStats = statsService.getCacheStatistics(cacheAlias);

        // this.keystore = new RBMIntKeyLookupStore((int) Math.pow(2, 28), maxKeystoreWeightInBytes);
        // this.policies = new CacheTierPolicy[]{ new IndicesRequestCacheTookTimePolicy(settings, clusterSettings) };
        // this.policy = new IndicesRequestCacheDiskTierPolicy(this.policies, true);
    }

    @Override
    public V get(K key) {
        // do we need to do the future stuff? I don't think so?

        // if (keystore.contains(key.hashCode()) {
        return cache.get(key); // do we want to somehow keep track of how long each cache.get takes?
        // }
        // return null;

    }

    @Override
    public void put(K key, V value) {
        // No need to get old value, this is handled by EhcacheEventListener.

        // CheckDataResult policyResult = policy.checkData(value)
        // if (policyResult.isAccepted()) {
        cache.put(key, value);
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
        return (int) cacheStats.getTierStatistics().get("Disk").getMappings();
    }

    @Override
    public TierType getTierType() {
        return TierType.DISK;
    }

    @Override
    public void onRemoval(RemovalNotification<K, V> notification) {
        removalListener.onRemoval(notification);
    }

    // See https://stackoverflow.com/questions/45827753/listenerobject-not-found-in-imports-for-ehcache-3 for API reference
    // This class is used to get the old value from mutating calls to the cache and use those to create a RemovalNotification
    private class EhcacheEventListener implements CacheEventListener<Object, Object> {
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
                    reason = RemovalReason.INVALIDATED; // not sure about this one
                    break;
                case REMOVED:
                    reason = RemovalReason.INVALIDATED; // we use cache.remove() to invalidate keys, but this might overlap with RemovalReason.EXPLICIT?
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
