/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import java.io.Serializable;
import java.util.Objects;

public class DummySerializableKey implements Serializable {
    private Integer i;
    private String s;
    public DummySerializableKey(Integer i, String s) {
        this.i = i;
        this.s = s;
    }

    public int getI() {
        return i;
    }
    public String getS() {
        return s;
    }
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DummySerializableKey)) {
            return false;
        }
        DummySerializableKey other = (DummySerializableKey) o;
        return Objects.equals(this.i, other.i) && this.s.equals(other.s);
    }
    @Override
    public final int hashCode() {
        int result = 11;
        if (i != null) {
            result = 31 * result + i.hashCode();
        }
        if (s != null) {
            result = 31 * result + s.hashCode();
        }
        return result;
    }
}
