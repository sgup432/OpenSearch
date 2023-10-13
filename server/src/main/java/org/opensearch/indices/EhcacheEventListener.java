/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.indices.EhcacheDiskCachingTier;

// moved to another file for testing flexibility purposes

public class EhcacheEventListener<K, V> implements CacheEventListener<K, V> { // make it private after debugging
    private RemovalListener<K, V> removalListener;
    private CounterMetric counter;
    EhcacheEventListener(RemovalListener<K, V> removalListener, CounterMetric counter) {
        this.removalListener = removalListener;
        this.counter = counter; // needed to handle count changes
    }
    @Override
    public void onEvent(CacheEvent<? extends K, ? extends V> event) {
        K key = event.getKey();
        V oldValue = event.getOldValue();
        V newValue = event.getNewValue();
        EventType eventType = event.getType();

        System.out.println("I am eventing!!");

        // handle changing count for the disk tier
        if (oldValue == null && newValue != null) {
            counter.inc();
        } else if (oldValue != null && newValue == null) {
            counter.dec();
        } else {
            int j;
        }

        // handle creating a RemovalReason, unless eventType is CREATED
        RemovalReason reason;
        switch (eventType) {
            case CREATED:
                return;
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
        removalListener.onRemoval(new RemovalNotification<K, V>(key, oldValue, reason));
    }
}
