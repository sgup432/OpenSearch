/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

public interface DiskCachingTier<K, V> extends CachingTier<K, V> {
    /**
     * Closes the disk tier.
     */
    void close();

    /**
     * Get the EWMA time in milliseconds for a get().
     * @return
     */
    double getTimeMillisEWMA();
}
