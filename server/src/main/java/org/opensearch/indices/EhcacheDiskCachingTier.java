/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.opensearch.common.cache.RemovalListener;
import org.ehcache.Cache;
import org.ehcache.CacheManager;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

public class EhcacheDiskCachingTier<K, V> implements CachingTier<K, V> {

    private CacheManager cacheManager;
    private Cache<K, V> cache;
    private final Class<K> keyType; // I think these are needed to pass to newCacheConfigurationBuilder
    private final Class<V> valueType;
    private final String DISK_CACHE_FP = "disk_cache_tier";
    // private RBMIntKeyLookupStore keystore;
    // private CacheTierPolicy[] policies;
    // private IndicesRequestCacheDiskTierPolicy policy;
    private RemovalListener<K, V> removalListener = notification -> {}; // based on on-heap

    public EhcacheDiskCachingTier(long maxWeightInBytes, long maxKeystoreWeightInBytes, Class<K> keyType, Class<V> valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
        String cacheAlias = "diskTier";
        this.cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
            .with(CacheManagerBuilder.persistence(DISK_CACHE_FP))
            .withCache(cacheAlias, CacheConfigurationBuilder.newCacheConfigurationBuilder(
                keyType, valueType, ResourcePoolsBuilder.newResourcePoolsBuilder().disk(maxWeightInBytes, MemoryUnit.B, true)
            )).build(true);
        this.cache = cacheManager.getCache(cacheAlias, keyType, valueType);
        // this.keystore = new RBMIntKeyLookupStore((int) Math.pow(2, 28), maxKeystoreWeightInBytes);
        // this.policies = new CacheTierPolicy[]{ new IndicesRequestCacheTookTimePolicy(settings, clusterSettings) };
        // this.policy = new IndicesRequestCacheDiskTierPolicy(this.policies, true);
    }

    @Override
    public V get(K key) {
        // keystore check goes here
        // if (keystore.contains(key.hashCode())
        // do we need to do the future stuff?
        return cache.get(key); // do we want to somehow keep track of how long each cache.get takes?
        //return null;
    }

    @Override
    public void put(K key, V value) {
        // CheckDataResult policyResult = policy.checkData(value)
        // if (policyResult.isAccepted()) {
        cache.put(key, value);
        // do we need to check for replacement, as onheap does?
        // else { do something with policyResult.deniedReason()? }
    }

    @Override
    public V computeIfAbsent(K key, TieredCacheLoader<K, V> loader) throws Exception {
        return null;
    }

    @Override
    public void invalidate(K key) {}

    @Override
    public V compute(K key, TieredCacheLoader<K, V> loader) throws Exception {
        return null;
    }

    @Override
    public void setRemovalListener(RemovalListener<K, V> removalListener) {}

    @Override
    public void invalidateAll() {}

    @Override
    public Iterable<K> keys() {
        return Collections::emptyIterator;
    }

    @Override
    public int count() {
        return 0;
    }

    @Override
    public TierType getTierType() {
        return TierType.DISK;
    }
}
