/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

public class EhCacheDiskCachingTierNewTests extends OpenSearchSingleNodeTestCase  {


    public void testBasicGetAndPut() {
        Settings.Builder settingsBuilder = Settings.builder();
        EhCacheDiskCachingTierNew<String, String> ehCacheDiskCachingTierNew =
            new EhCacheDiskCachingTierNew.Builder<String, String>()
                .setKeyType(String.class)
                .setValueType(String.class)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setSettings(settingsBuilder.build())
                .setThreadPoolAlias("ehcacheTest")
                .setMaximumWeightInBytes(50000)
                .setStoragePath("/tmp/2")
                .build();
        int randomKeys = randomIntBetween(10, 100);
        Map<String, String> keyValueMap = new HashMap<>();
        for (int i = 0; i < randomKeys; i++) {
            keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
        for (Map.Entry<String, String> entry: keyValueMap.entrySet()) {
            ehCacheDiskCachingTierNew.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, String> entry: keyValueMap.entrySet()) {
            String value = ehCacheDiskCachingTierNew.get(entry.getKey());
            assertEquals(entry.getValue(), value);
        }
        ehCacheDiskCachingTierNew.close();
    }

    public void testConcurrentPut() throws InterruptedException {
        Settings.Builder settingsBuilder = Settings.builder();
        EhCacheDiskCachingTierNew<String, String> ehCacheDiskCachingTierNew =
            new EhCacheDiskCachingTierNew.Builder<String, String>()
                .setKeyType(String.class)
                .setValueType(String.class)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setSettings(settingsBuilder.build())
                .setThreadPoolAlias("ehcacheTest")
                .setMaximumWeightInBytes(50000)
                .setStoragePath("/tmp/1")
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
        for (Map.Entry<String, String> entry: keyValueMap.entrySet()) {
            threads[j] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                ehCacheDiskCachingTierNew.put(entry.getKey(), entry.getValue());
                countDownLatch.countDown();
            });
            threads[j].start();
            j++;
        }
        phaser.arriveAndAwaitAdvance(); // Will trigger parallel puts above.
        countDownLatch.await(); // Wait for all threads to finish
        for (Map.Entry<String, String> entry: keyValueMap.entrySet()) {
            String value = ehCacheDiskCachingTierNew.get(entry.getKey());
            assertEquals(entry.getValue(), value);
        }
        ehCacheDiskCachingTierNew.close();
    }

    public void testEhcacheParallelGets() throws InterruptedException {
        Settings.Builder settingsBuilder = Settings.builder();
        EhCacheDiskCachingTierNew<String, String> ehCacheDiskCachingTierNew =
            new EhCacheDiskCachingTierNew.Builder<String, String>()
                .setKeyType(String.class)
                .setValueType(String.class)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setSettings(settingsBuilder.build())
                .setThreadPoolAlias("ehcacheTest")
                .setMaximumWeightInBytes(50000) // 100kb
                .setStoragePath("/tmp/")
                .setIsEventListenerModeSync(true) // For accurate count
                .build();
        ehCacheDiskCachingTierNew.setRemovalListener(removalListener(new AtomicInteger()));
        int randomKeys = randomIntBetween(20, 100);
        Thread[] threads = new Thread[randomKeys];
        Phaser phaser = new Phaser(randomKeys + 1);
        CountDownLatch countDownLatch = new CountDownLatch(randomKeys);
        Map<String, String> keyValueMap = new HashMap<>();
        int j = 0;
        for (int i = 0; i < randomKeys; i++) {
            keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
        for (Map.Entry<String, String> entry: keyValueMap.entrySet()) {
            ehCacheDiskCachingTierNew.put(entry.getKey(), entry.getValue());
        }
        assertEquals(keyValueMap.size(), ehCacheDiskCachingTierNew.count());
        for (Map.Entry<String, String> entry: keyValueMap.entrySet()) {
            threads[j] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                assertEquals(entry.getValue(), ehCacheDiskCachingTierNew.get(entry.getKey()));
                countDownLatch.countDown();
            });
            threads[j].start();
            j++;
        }
        phaser.arriveAndAwaitAdvance(); // Will trigger parallel puts above.
        countDownLatch.await(); // Wait for all threads to finish
        ehCacheDiskCachingTierNew.close();
    }

    public void testEhcacheKeyIterator() throws InterruptedException {
        Settings.Builder settingsBuilder = Settings.builder();
        EhCacheDiskCachingTierNew<String, String> ehCacheDiskCachingTierNew =
            new EhCacheDiskCachingTierNew.Builder<String, String>()
                .setKeyType(String.class)
                .setValueType(String.class)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setSettings(settingsBuilder.build())
                .setThreadPoolAlias("ehcacheTest")
                .setMaximumWeightInBytes(50000) // 100kb
                .setStoragePath("/tmp/")
                .build();

        int randomKeys = randomIntBetween(2, 2);
        Map<String, String> keyValueMap = new HashMap<>();
        for (int i = 0; i < randomKeys; i++) {
            keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
        for (Map.Entry<String, String> entry: keyValueMap.entrySet()) {
            ehCacheDiskCachingTierNew.put(entry.getKey(), entry.getValue());
        }
        Iterator<String> keys = ehCacheDiskCachingTierNew.keys().iterator();
        int keysCount = 0;
        while(keys.hasNext()) {
            String key = keys.next();
            keysCount++;
            assertNotNull(ehCacheDiskCachingTierNew.get(key));
        }
        assertEquals(keysCount, randomKeys);
        ehCacheDiskCachingTierNew.close();
    }

    public void testCompute() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder();
        EhCacheDiskCachingTierNew<String, String> ehCacheDiskCachingTierNew =
            new EhCacheDiskCachingTierNew.Builder<String, String>()
                .setKeyType(String.class)
                .setValueType(String.class)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setSettings(settingsBuilder.build())
                .setThreadPoolAlias("ehcacheTest")
                .setMaximumWeightInBytes(50000) // 100kb
                .setStoragePath("/tmp/")
                .build();
        // For now it is unsupported.
        assertThrows(UnsupportedOperationException.class, () -> ehCacheDiskCachingTierNew.compute("dummy", new TieredCacheLoader<String, String>() {
            @Override
            public String load(String key) throws Exception {
                return "dummy";
            }

            @Override
            public boolean isLoaded() {
                return false;
            }
        }));
        assertThrows(UnsupportedOperationException.class, () -> ehCacheDiskCachingTierNew.computeIfAbsent("dummy",
            new TieredCacheLoader<String, String>() {
            @Override
            public String load(String key) throws Exception {
                return "dummy";
            }

            @Override
            public boolean isLoaded() {
                return false;
            }
        }));
    }

    private RemovalListener<String, String> removalListener(AtomicInteger counter) {
        return notification -> counter.incrementAndGet();
    }
}
