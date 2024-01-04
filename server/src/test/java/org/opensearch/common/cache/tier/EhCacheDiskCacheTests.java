/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.StoreAwareCacheRemovalNotification;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

public class EhCacheDiskCacheTests extends OpenSearchSingleNodeTestCase {

    private static final int CACHE_SIZE_IN_BYTES = 1024 * 101;
    private static final String SETTING_PREFIX = "indices.request.cache";

    public void testBasicGetAndPut() throws IOException {
        Settings settings = Settings.builder().build();
        MockEventListener<String, String> mockEventListener = new MockEventListener<>();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            StoreAwareCache<String, String> ehcacheTest = new EhCacheDiskCache.Builder<String, String>().setKeyType(String.class)
                .setValueType(String.class)
                .setSettings(settings)
                .setThreadPoolAlias("ehcacheTest")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setSettingPrefix(SETTING_PREFIX)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setEventListener(mockEventListener)
                .build();
            int randomKeys = randomIntBetween(10, 100);
            Map<String, String> keyValueMap = new HashMap<>();
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                ehcacheTest.put(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                String value = ehcacheTest.get(entry.getKey());
                assertEquals(entry.getValue(), value);
            }
            assertEquals(randomKeys, mockEventListener.enumMap.get(EventType.ON_CACHED).get());
            assertEquals(randomKeys, mockEventListener.enumMap.get(EventType.ON_HIT).get());

            // Validate misses
            int expectedNumberOfMisses = randomIntBetween(10, 200);
            for (int i = 0; i < expectedNumberOfMisses; i++) {
                ehcacheTest.get(UUID.randomUUID().toString());
            }

            assertEquals(expectedNumberOfMisses, mockEventListener.enumMap.get(EventType.ON_MISS).get());
            ehcacheTest.close();
        }
    }

    public void testConcurrentPut() throws Exception {
        Settings settings = Settings.builder().build();
        MockEventListener<String, String> mockEventListener = new MockEventListener<>();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            StoreAwareCache<String, String> ehcacheTest = new EhCacheDiskCache.Builder<String, String>().setKeyType(String.class)
                .setValueType(String.class)
                .setSettings(settings)
                .setThreadPoolAlias("ehcacheTest")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setSettingPrefix(SETTING_PREFIX)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setEventListener(mockEventListener)
                .build();
            int randomKeys = randomIntBetween(20, 100);
            Thread[] threads = new Thread[randomKeys];
            Phaser phaser = new Phaser(randomKeys + 1);
            CountDownLatch countDownLatch = new CountDownLatch(randomKeys);
            Map<String, String> keyValueMap = new HashMap<>();
            int j = 0;
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                threads[j] = new Thread(() -> {
                    phaser.arriveAndAwaitAdvance();
                    ehcacheTest.put(entry.getKey(), entry.getValue());
                    countDownLatch.countDown();
                });
                threads[j].start();
                j++;
            }
            phaser.arriveAndAwaitAdvance(); // Will trigger parallel puts above.
            countDownLatch.await(); // Wait for all threads to finish
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                String value = ehcacheTest.get(entry.getKey());
                assertEquals(entry.getValue(), value);
            }
            assertEquals(randomKeys, mockEventListener.enumMap.get(EventType.ON_CACHED).get());
            ehcacheTest.close();
        }
    }

    public void testEhcacheParallelGets() throws Exception {
        Settings settings = Settings.builder().build();
        MockEventListener<String, String> mockEventListener = new MockEventListener<>();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            StoreAwareCache<String, String> ehcacheTest = new EhCacheDiskCache.Builder<String, String>().setKeyType(String.class)
                .setValueType(String.class)
                .setSettings(settings)
                .setThreadPoolAlias("ehcacheTest")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setSettingPrefix(SETTING_PREFIX)
                .setIsEventListenerModeSync(true) // For accurate count
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setEventListener(mockEventListener)
                .build();
            int randomKeys = randomIntBetween(20, 100);
            Thread[] threads = new Thread[randomKeys];
            Phaser phaser = new Phaser(randomKeys + 1);
            CountDownLatch countDownLatch = new CountDownLatch(randomKeys);
            Map<String, String> keyValueMap = new HashMap<>();
            int j = 0;
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                ehcacheTest.put(entry.getKey(), entry.getValue());
            }
            assertEquals(keyValueMap.size(), ehcacheTest.count());
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                threads[j] = new Thread(() -> {
                    phaser.arriveAndAwaitAdvance();
                    assertEquals(entry.getValue(), ehcacheTest.get(entry.getKey()));
                    countDownLatch.countDown();
                });
                threads[j].start();
                j++;
            }
            phaser.arriveAndAwaitAdvance(); // Will trigger parallel puts above.
            countDownLatch.await(); // Wait for all threads to finish
            assertEquals(randomKeys, mockEventListener.enumMap.get(EventType.ON_HIT).get());
            ehcacheTest.close();
        }
    }

    public void testEhcacheKeyIterator() throws Exception {
        Settings settings = Settings.builder().build();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            StoreAwareCache<String, String> ehcacheTest = new EhCacheDiskCache.Builder<String, String>().setKeyType(String.class)
                .setValueType(String.class)
                .setSettings(settings)
                .setThreadPoolAlias("ehcacheTest")
                .setSettingPrefix(SETTING_PREFIX)
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setEventListener(new MockEventListener<>())
                .build();

            int randomKeys = randomIntBetween(2, 100);
            Map<String, String> keyValueMap = new HashMap<>();
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                ehcacheTest.put(entry.getKey(), entry.getValue());
            }
            Iterator<String> keys = ehcacheTest.keys().iterator();
            int keysCount = 0;
            while (keys.hasNext()) {
                String key = keys.next();
                keysCount++;
                assertNotNull(ehcacheTest.get(key));
            }
            assertEquals(CacheStoreType.DISK, ehcacheTest.getTierType());
            assertEquals(keysCount, randomKeys);
            ehcacheTest.close();
        }
    }

    public void testEvictions() throws Exception {
        Settings settings = Settings.builder().build();
        MockEventListener<String, String> mockEventListener = new MockEventListener<>();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            StoreAwareCache<String, String> ehcacheTest = new EhCacheDiskCache.Builder<String, String>().setKeyType(String.class)
                .setValueType(String.class)
                .setSettings(settings)
                .setThreadPoolAlias("ehcacheTest")
                .setSettingPrefix(SETTING_PREFIX)
                .setIsEventListenerModeSync(true)
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setEventListener(mockEventListener)
                .build();

            // Generate a string with 100 characters
            String value = generateRandomString(100);

            // Trying to generate more than 100kb to cause evictions.
            for (int i = 0; i < 1000; i++) {
                String key = "Key" + i;
                ehcacheTest.put(key, value);
            }
            assertTrue(mockEventListener.enumMap.get(EventType.ON_REMOVAL).get() > 0);
            ehcacheTest.close();
        }
    }

    private static String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder randomString = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int index = (int) (Math.random() * characters.length());
            randomString.append(characters.charAt(index));
        }

        return randomString.toString();
    }

    // TODO: Remove this from here in final PR.
    enum EventType {
        ON_HIT,
        ON_MISS,
        ON_CACHED,
        ON_REMOVAL;
    }

    class MockEventListener<K, V> implements StoreAwareCacheEventListener<K, V> {

        EnumMap<EventType, AtomicInteger> enumMap;

        MockEventListener() {
            enumMap = new EnumMap<>(EventType.class);
            for (EventType eventType : EventType.values()) {
                enumMap.put(eventType, new AtomicInteger());
            }
        }

        @Override
        public void onMiss(K key, CacheStoreType cacheStoreType) {
            assert cacheStoreType.equals(CacheStoreType.DISK);
            AtomicInteger count = enumMap.get(EventType.ON_MISS);
            count.incrementAndGet();
        }

        @Override
        public void onRemoval(StoreAwareCacheRemovalNotification notification) {
            assert notification.getCacheStoreType().equals(CacheStoreType.DISK);
            AtomicInteger count = enumMap.get(EventType.ON_REMOVAL);
            count.incrementAndGet();
        }

        @Override
        public void onHit(K key, V value, CacheStoreType cacheStoreType) {
            assert cacheStoreType.equals(CacheStoreType.DISK);
            AtomicInteger count = enumMap.get(EventType.ON_HIT);
            count.incrementAndGet();
        }

        @Override
        public void onCached(K key, V value, CacheStoreType cacheStoreType) {
            assert cacheStoreType.equals(CacheStoreType.DISK);
            AtomicInteger count = enumMap.get(EventType.ON_CACHED);
            count.incrementAndGet();
        }
    }
}
