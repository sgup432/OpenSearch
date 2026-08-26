/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

/**
 * Generic thread-scoped cancellation hook for long-running CPU work on the search thread that is NOT driven
 * through the Lucene reader and so is not covered by {@code ExitableDirectoryReader}. The query phase binds a
 * check (throwing {@code TaskCancelledException} when the task is cancelled); deep code calls {@link #checkCancelled()}
 * without threading the signal through intermediate APIs. Bind/clear must be paired (clear in a {@code finally}).
 * No-op when unbound, e.g. work on the warmer or concurrent-search slice threads, which do not inherit this ThreadLocal.
 *
 * <p>Current consumer: the in-heap field data / global-ordinals build (which reads the builder, not the reader).
 */
public final class SearchCancellationContext {

    private static final ThreadLocal<Runnable> CURRENT = new ThreadLocal<>();

    private SearchCancellationContext() {}

    /** Bind a cancellation check to the current thread. The check should throw if the task is cancelled. */
    public static void set(Runnable check) {
        CURRENT.set(check);
    }

    /** Remove any cancellation check bound to the current thread. Must be called in a {@code finally}. */
    public static void clear() {
        CURRENT.remove();
    }

    /** Runs the bound check, if any (throws when the task is cancelled). No-op when unbound. */
    public static void checkCancelled() {
        final Runnable check = CURRENT.get();
        if (check != null) {
            check.run();
        }
    }
}
