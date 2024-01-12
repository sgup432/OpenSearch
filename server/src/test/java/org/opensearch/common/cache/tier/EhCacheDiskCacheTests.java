/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.StoreAwareCacheRemovalNotification;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.instanceOf;

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
            assertEquals(randomKeys, mockEventListener.onCachedCount.get());
            assertEquals(randomKeys, mockEventListener.onHitCount.get());

            // Validate misses
            int expectedNumberOfMisses = randomIntBetween(10, 200);
            for (int i = 0; i < expectedNumberOfMisses; i++) {
                ehcacheTest.get(UUID.randomUUID().toString());
            }

            assertEquals(expectedNumberOfMisses, mockEventListener.onMissCount.get());
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
            assertEquals(randomKeys, mockEventListener.onCachedCount.get());
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
            assertEquals(randomKeys, mockEventListener.onHitCount.get());
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
            assertTrue(mockEventListener.onRemovalCount.get() > 0);
            ehcacheTest.close();
        }
    }

    public void testComputeIfAbsentConcurrently() throws Exception {
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

            int numberOfRequest = randomIntBetween(200, 400);
            String key = UUID.randomUUID().toString();
            String value = "dummy";
            Thread[] threads = new Thread[numberOfRequest];
            Phaser phaser = new Phaser(numberOfRequest + 1);
            CountDownLatch countDownLatch = new CountDownLatch(numberOfRequest);

            List<LoadAwareCacheLoader<String, String>> loadAwareCacheLoaderList = new CopyOnWriteArrayList<>();

            // Try to hit different request with the same key concurrently. Verify value is only loaded once.
            for(int i = 0; i < numberOfRequest; i++) {
                threads[i] = new Thread(() -> {
                    LoadAwareCacheLoader<String, String> loadAwareCacheLoader = new LoadAwareCacheLoader<>() {
                        boolean isLoaded;

                        @Override
                        public boolean isLoaded() {
                            return isLoaded;
                        }

                        @Override
                        public String load(String key) {
                            isLoaded = true;
                            return value;
                        }
                    };
                    loadAwareCacheLoaderList.add(loadAwareCacheLoader);
                    phaser.arriveAndAwaitAdvance();
                    try {
                        assertEquals(value, ehcacheTest.computeIfAbsent(key, loadAwareCacheLoader));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    countDownLatch.countDown();
                });
                threads[i].start();
            }
            phaser.arriveAndAwaitAdvance();
            countDownLatch.await();
            int numberOfTimesValueLoaded = 0;
            for (int i = 0; i < numberOfRequest; i++) {
                if (loadAwareCacheLoaderList.get(i).isLoaded()) {
                    numberOfTimesValueLoaded++;
                }
            }
            assertEquals(1, numberOfTimesValueLoaded);
            assertEquals(0, ((EhCacheDiskCache) ehcacheTest).getCompletableFutureMap().size());
            assertEquals(1, mockEventListener.onMissCount.get());
            assertEquals(1, mockEventListener.onCachedCount.get());
            assertEquals(numberOfRequest - 1, mockEventListener.onHitCount.get());
            ehcacheTest.close();
        }
    }

    public void testComputeIfAbsentConcurrentlyAndThrowsException() throws Exception {
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

            int numberOfRequest = randomIntBetween(200, 400);
            String key = UUID.randomUUID().toString();
            Thread[] threads = new Thread[numberOfRequest];
            Phaser phaser = new Phaser(numberOfRequest + 1);
            CountDownLatch countDownLatch = new CountDownLatch(numberOfRequest);

            List<LoadAwareCacheLoader<String, String>> loadAwareCacheLoaderList = new CopyOnWriteArrayList<>();

            // Try to hit different request with the same key concurrently. Loader throws exception.
            for(int i = 0; i < numberOfRequest; i++) {
                threads[i] = new Thread(() -> {
                    LoadAwareCacheLoader<String, String> loadAwareCacheLoader = new LoadAwareCacheLoader<>() {
                        boolean isLoaded;

                        @Override
                        public boolean isLoaded() {
                            return isLoaded;
                        }

                        @Override
                        public String load(String key) throws Exception {
                            isLoaded = true;
                            throw new RuntimeException("Exception");
                        }
                    };
                    loadAwareCacheLoaderList.add(loadAwareCacheLoader);
                    phaser.arriveAndAwaitAdvance();
                    assertThrows(ExecutionException.class, () -> ehcacheTest.computeIfAbsent(key,
                            loadAwareCacheLoader));
                    countDownLatch.countDown();
                });
                threads[i].start();
            }
            phaser.arriveAndAwaitAdvance();
            countDownLatch.await();

            assertEquals(0, ((EhCacheDiskCache) ehcacheTest).getCompletableFutureMap().size());
            ehcacheTest.close();
        }
    }

    public void testComputeIfAbsentWithNullValueLoading() throws Exception {
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

            int numberOfRequest = randomIntBetween(200, 400);
            String key = UUID.randomUUID().toString();
            Thread[] threads = new Thread[numberOfRequest];
            Phaser phaser = new Phaser(numberOfRequest + 1);
            CountDownLatch countDownLatch = new CountDownLatch(numberOfRequest);

            List<LoadAwareCacheLoader<String, String>> loadAwareCacheLoaderList = new CopyOnWriteArrayList<>();

            // Try to hit different request with the same key concurrently. Loader throws exception.
            for(int i = 0; i < numberOfRequest; i++) {
                threads[i] = new Thread(() -> {
                    LoadAwareCacheLoader<String, String> loadAwareCacheLoader = new LoadAwareCacheLoader<>() {
                        boolean isLoaded;

                        @Override
                        public boolean isLoaded() {
                            return isLoaded;
                        }

                        @Override
                        public String load(String key) throws Exception {
                            isLoaded = true;
                            return null;
                        }
                    };
                    loadAwareCacheLoaderList.add(loadAwareCacheLoader);
                    phaser.arriveAndAwaitAdvance();
                    try {
                        ehcacheTest.computeIfAbsent(key, loadAwareCacheLoader);
                    } catch (Exception ex) {
                        assertThat(ex.getCause(), instanceOf(NullPointerException.class));
                    }
                    assertThrows(ExecutionException.class, () -> ehcacheTest.computeIfAbsent(key,
                        loadAwareCacheLoader));
                    countDownLatch.countDown();
                });
                threads[i].start();
            }
            phaser.arriveAndAwaitAdvance();
            countDownLatch.await();

            assertEquals(0, ((EhCacheDiskCache) ehcacheTest).getCompletableFutureMap().size());
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

        AtomicInteger onMissCount = new AtomicInteger();
        AtomicInteger onHitCount = new AtomicInteger();
        AtomicInteger onCachedCount = new AtomicInteger();
        AtomicInteger onRemovalCount = new AtomicInteger();

        MockEventListener() {
        }

        @Override
        public void onMiss(K key, CacheStoreType cacheStoreType) {
            assert cacheStoreType.equals(CacheStoreType.DISK);
            onMissCount.incrementAndGet();
        }

        @Override
        public void onRemoval(StoreAwareCacheRemovalNotification<K, V> notification) {
            assert notification.getCacheStoreType().equals(CacheStoreType.DISK);
            onRemovalCount.incrementAndGet();
        }

        @Override
        public void onHit(K key, V value, CacheStoreType cacheStoreType) {
            assert cacheStoreType.equals(CacheStoreType.DISK);
            onHitCount.incrementAndGet();
        }

        @Override
        public void onCached(K key, V value, CacheStoreType cacheStoreType) {
            assert cacheStoreType.equals(CacheStoreType.DISK);
            onCachedCount.incrementAndGet();
        }
    }
}
