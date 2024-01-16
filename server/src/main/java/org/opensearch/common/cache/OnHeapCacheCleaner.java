/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.threadpool.ThreadPool;

import java.util.Iterator;
import java.util.function.Predicate;

public class OnHeapCacheCleaner<K, V> implements CacheCleaner {

    ThreadPool threadPool;
    TimeValue cleanInterval;
    Predicate<K> predicate;
    ICache<K, V> cache;
    public OnHeapCacheCleaner(ThreadPool threadPool, TimeValue cleanInterval, Predicate<K> predicate,
                              ICache<K, V> cache) {
        this.threadPool = threadPool;
        this.cleanInterval = cleanInterval;
        this.predicate = predicate;
        this.cache = cache;
        threadPool.schedule(this::clean, this.cleanInterval, ThreadPool.Names.SAME);
    }

    @Override
    public void clean() {
        for (Iterator<K> iterator = cache.keys().iterator(); iterator.hasNext();) {
            K key = iterator.next();
            if (predicate.test(key)) {
                iterator.remove();
            }
        }
    }
}
