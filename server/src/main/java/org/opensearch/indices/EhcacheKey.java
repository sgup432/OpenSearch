/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

public class EhcacheKey implements Serializable {
    // the IndicesRequestCache.Key is not Serializable, but it is Writeable.
    // We use the output stream's bytes in this wrapper class and implement the appropriate interfaces/methods.
    // Unfortunately it's not possible to define this class as EhcacheKey<K> and use that as ehcache keys,
    // because of type erasure. However, the only context EhcacheKey objects would be compared to one another
    // is when they are used for the same cache, so they will always refer to the same K.
    private byte[] bytes;

    public EhcacheKey(Writeable key) throws IOException {
        BytesStreamOutput os = new BytesStreamOutput(); // Should we pass in an expected size? If so, how big?
        key.writeTo(os);
        this.bytes = BytesReference.toBytes(os.bytes());
    }

    public byte[] getBytes() {
        return this.bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof EhcacheKey)) {
            return false;
        }
        EhcacheKey other = (EhcacheKey) o;
        return Arrays.equals(this.bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.bytes);
    }
}
