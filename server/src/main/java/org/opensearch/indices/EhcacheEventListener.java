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
import org.opensearch.core.common.bytes.BytesReference;

public class EhcacheEventListener implements CacheEventListener<EhcacheKey, BytesReference> {
    // Receives key-value pairs (EhcacheKey, BytesReference), but must transform into (Key, BytesReference)
    // to send removal notifications
    private final RemovalListener<IndicesRequestCache.Key, BytesReference> removalListener;
    private final EhcacheDiskCachingTier tier;
    EhcacheEventListener(RemovalListener<IndicesRequestCache.Key, BytesReference> removalListener, EhcacheDiskCachingTier tier) {
        this.removalListener = removalListener;
        this.tier = tier; // needed to handle count changes
    }
    @Override
    public void onEvent(CacheEvent<? extends EhcacheKey, ? extends BytesReference> event) {
        EhcacheKey ehcacheKey = event.getKey();
        BytesReference oldValue = event.getOldValue();
        BytesReference newValue = event.getNewValue();
        EventType eventType = event.getType();

        // handle changing count for the disk tier
        if (oldValue == null && newValue != null) {
            tier.countInc();
        } else if (oldValue != null && newValue == null) {
            tier.countDec();
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
        try {
            IndicesRequestCache.Key key = tier.convertEhcacheKeyToOriginal(ehcacheKey);
            removalListener.onRemoval(new RemovalNotification<>(key, oldValue, reason));
        } catch (Exception ignored) {}

    }
}
