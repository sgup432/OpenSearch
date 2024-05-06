/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.module.CacheModule;
import org.opensearch.common.cache.service.CacheService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.AbstractBytesReference;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.cache.request.ShardRequestCache;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.node.Node;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.indices.IndicesRequestCache.INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class IndicesRequestCacheTests extends OpenSearchSingleNodeTestCase {
    private ThreadPool getThreadPool() {
        return new ThreadPool(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "default tracer tests").build());
    }

    public void testBasicOperationsCache() throws Exception {
        IndexShard indexShard = createIndex("test").getShard(0);
        ThreadPool threadPool = getThreadPool();
        IndicesRequestCache cache = new IndicesRequestCache(
            Settings.EMPTY,
            (shardId -> Optional.of(new IndicesService.IndexShardCacheEntity(indexShard))),
            new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(),
            threadPool
        );
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);

        // initial cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        ShardRequestCache requestCacheStats = indexShard.requestCache();
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());

        // cache hit
        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = indexShard.requestCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // Closing the cache doesn't modify an already returned CacheEntity
        if (randomBoolean()) {
            reader.close();
        } else {
            indexShard.close("test", true, true); // closed shard but reader is still open
            cache.clear(entity);
        }
        cache.cacheCleanupManager.cleanCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(reader, writer, dir, cache);
        terminate(threadPool);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testBasicOperationsCacheWithFeatureFlag() throws Exception {
        IndexShard indexShard = createIndex("test").getShard(0);
        CacheService cacheService = new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService();
        ThreadPool threadPool = getThreadPool();
        IndicesRequestCache cache = new IndicesRequestCache(
            Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.PLUGGABLE_CACHE, "true").build(),
            (shardId -> Optional.of(new IndicesService.IndexShardCacheEntity(indexShard))),
            cacheService,
            threadPool
        );
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);

        // initial cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        ShardRequestCache requestCacheStats = indexShard.requestCache();
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());

        // cache hit
        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = indexShard.requestCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // Closing the cache doesn't modify an already returned CacheEntity
        if (randomBoolean()) {
            reader.close();
        } else {
            indexShard.close("test", true, true); // closed shard but reader is still open
            cache.clear(entity);
        }
        cache.cacheCleanupManager.cleanCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(reader, writer, dir, cache);
        terminate(threadPool);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testCacheDifferentReaders() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard indexShard = createIndex("test").getShard(0);
        ThreadPool threadPool = getThreadPool();
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, (shardId -> {
            IndexService indexService = null;
            try {
                indexService = indicesService.indexServiceSafe(shardId.getIndex());
            } catch (IndexNotFoundException ex) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(), threadPool);
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        if (randomBoolean()) {
            writer.flush();
            IOUtils.close(writer);
            writer = new IndexWriter(dir, newIndexWriterConfig());
        }
        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));

        // initial cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        ShardRequestCache requestCacheStats = entity.stats();
        assertEquals("foo", value.streamInput().readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        final int cacheSize = requestCacheStats.stats().getMemorySize().bytesAsInt();
        assertEquals(1, cache.numRegisteredCloseListeners());

        // cache the second
        IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        value = cache.getOrCompute(entity, loader, secondReader, termBytes);
        requestCacheStats = entity.stats();
        assertEquals("bar", value.streamInput().readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(2, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > cacheSize + value.length());
        assertEquals(2, cache.numRegisteredCloseListeners());

        secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        value = cache.getOrCompute(secondEntity, loader, secondReader, termBytes);
        requestCacheStats = entity.stats();
        assertEquals("bar", value.streamInput().readString());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(2, cache.count());

        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = entity.stats();
        assertEquals(2, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(2, cache.count());

        // Closing the cache doesn't change returned entities
        reader.close();
        cache.cacheCleanupManager.cleanCache();
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertEquals(cacheSize, requestCacheStats.stats().getMemorySize().bytesAsInt());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // release
        if (randomBoolean()) {
            secondReader.close();
        } else {
            indexShard.close("test", true, true); // closed shard but reader is still open
            cache.clear(secondEntity);
        }
        cache.cacheCleanupManager.cleanCache();
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(secondReader, writer, dir, cache);
        terminate(threadPool);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testCacheCleanupThresholdSettingValidator_Valid_Percentage() {
        String s = IndicesRequestCache.validateStalenessSetting("50%");
        assertEquals("50%", s);
    }

    public void testCacheCleanupThresholdSettingValidator_Valid_Double() {
        String s = IndicesRequestCache.validateStalenessSetting("0.5");
        assertEquals("0.5", s);
    }

    public void testCacheCleanupThresholdSettingValidator_Valid_DecimalPercentage() {
        String s = IndicesRequestCache.validateStalenessSetting("0.5%");
        assertEquals("0.5%", s);
    }

    public void testCacheCleanupThresholdSettingValidator_InValid_MB() {
        assertThrows(IllegalArgumentException.class, () -> { IndicesRequestCache.validateStalenessSetting("50mb"); });
    }

    public void testCacheCleanupThresholdSettingValidator_Invalid_Percentage() {
        assertThrows(IllegalArgumentException.class, () -> { IndicesRequestCache.validateStalenessSetting("500%"); });
    }

    public void testCacheCleanupBasedOnZeroThreshold() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard indexShard = createIndex("test").getShard(0);
        ThreadPool threadPool = getThreadPool();
        Settings settings = Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), "0%").build();
        IndicesRequestCache cache = new IndicesRequestCache(settings, (shardId -> {
            IndexService indexService = null;
            try {
                indexService = indicesService.indexServiceSafe(shardId.getIndex());
            } catch (IndexNotFoundException ex) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), settings).getCacheService(), threadPool);
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        if (randomBoolean()) {
            writer.flush();
            IOUtils.close(writer);
            writer = new IndexWriter(dir, newIndexWriterConfig());
        }
        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));

        // Get 2 entries into the cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        cache.getOrCompute(entity, loader, reader, termBytes);

        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        cache.getOrCompute(entity, loader, reader, termBytes);

        IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        cache.getOrCompute(entity, loader, secondReader, termBytes);

        secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        cache.getOrCompute(secondEntity, loader, secondReader, termBytes);
        assertEquals(2, cache.count());

        // Close the reader, to be enqueued for cleanup
        // 1 out of 2 keys ie 50% are now stale.
        reader.close();
        // cache count should not be affected
        assertEquals(2, cache.count());
        // clean cache with 0% staleness threshold
        cache.cacheCleanupManager.cleanCache();
        // cleanup should remove the stale-key
        assertEquals(1, cache.count());

        IOUtils.close(secondReader, writer, dir, cache);
        terminate(threadPool);
    }

    public void testCacheCleanupBasedOnStaleThreshold_StalenessEqualToThreshold() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard indexShard = createIndex("test").getShard(0);
        ThreadPool threadPool = getThreadPool();
        Settings settings = Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), "0.5").build();
        IndicesRequestCache cache = new IndicesRequestCache(settings, (shardId -> {
            IndexService indexService = null;
            try {
                indexService = indicesService.indexServiceSafe(shardId.getIndex());
            } catch (IndexNotFoundException ex) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), settings).getCacheService(), threadPool);
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        if (randomBoolean()) {
            writer.flush();
            IOUtils.close(writer);
            writer = new IndexWriter(dir, newIndexWriterConfig());
        }
        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));

        // Get 2 entries into the cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        cache.getOrCompute(entity, loader, reader, termBytes);

        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        cache.getOrCompute(entity, loader, reader, termBytes);

        IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        cache.getOrCompute(entity, loader, secondReader, termBytes);

        secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        cache.getOrCompute(secondEntity, loader, secondReader, termBytes);
        assertEquals(2, cache.count());

        // Close the reader, to be enqueued for cleanup
        // 1 out of 2 keys ie 50% are now stale.
        reader.close();
        // cache count should not be affected
        assertEquals(2, cache.count());

        // clean cache with 50% staleness threshold
        cache.cacheCleanupManager.cleanCache();
        // cleanup should have taken effect
        assertEquals(1, cache.count());

        IOUtils.close(secondReader, writer, dir, cache);
        terminate(threadPool);
    }

    public void testStaleCount_OnRemovalNotificationOfStaleKey_DecrementsStaleCount() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard indexShard = createIndex("test", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()).getShard(0);
        ThreadPool threadPool = getThreadPool();
        Settings settings = Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), "0.51").build();
        IndicesRequestCache cache = new IndicesRequestCache(settings, (shardId -> {
            IndexService indexService = null;
            try {
                indexService = indicesService.indexServiceSafe(shardId.getIndex());
            } catch (IndexNotFoundException ex) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), settings).getCacheService(), threadPool);
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        if (randomBoolean()) {
            writer.flush();
            IOUtils.close(writer);
            writer = new IndexWriter(dir, newIndexWriterConfig());
        }
        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));

        // Get 2 entries into the cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        cache.getOrCompute(entity, loader, reader, termBytes);

        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        cache.getOrCompute(entity, loader, reader, termBytes);

        IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        cache.getOrCompute(entity, loader, secondReader, termBytes);

        secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        cache.getOrCompute(secondEntity, loader, secondReader, termBytes);
        assertEquals(2, cache.count());

        RequestCacheStats stats = indexShard.requestCache().stats();
        System.out.println("memory 1 = " + stats.getMemorySizeInBytes());
        System.out.println("misses = " + stats.getMissCount());

        // Close the reader, to be enqueued for cleanup
        reader.close();
        AtomicInteger staleKeysCount = cache.cacheCleanupManager.getStaleKeysCount();
        // 1 out of 2 keys ie 50% are now stale.
        assertEquals(1, staleKeysCount.get());
        // cache count should not be affected
        assertEquals(2, cache.count());
        indexShard.requestCache().stats();
        System.out.println("memory 2 = " + stats.getMemorySizeInBytes());
        System.out.println("misses = " + stats.getMissCount());

        OpenSearchDirectoryReader.DelegatingCacheHelper delegatingCacheHelper =
            (OpenSearchDirectoryReader.DelegatingCacheHelper) secondReader.getReaderCacheHelper();
        String readerCacheKeyId = delegatingCacheHelper.getDelegatingCacheKey().getId();
        IndicesRequestCache.Key key = new IndicesRequestCache.Key(
            ((IndexShard) secondEntity.getCacheIdentity()).shardId(),
            termBytes,
            readerCacheKeyId,
            ((IndexShard) secondEntity.getCacheIdentity()).hashCode()
        );
        cache.invalidate(secondEntity, secondReader, termBytes);
//        cache.onRemoval(new RemovalNotification<IndicesRequestCache.Key, BytesReference>(key, termBytes, RemovalReason.EVICTED));
        staleKeysCount = cache.cacheCleanupManager.getStaleKeysCount();
        // eviction of previous stale key from the cache should decrement staleKeysCount in iRC
        assertEquals(0, staleKeysCount.get());
        stats = indexShard.requestCache().stats();
        System.out.println("memory 3 = " + stats.getMemorySizeInBytes());
        System.out.println("misses = " + stats.getMissCount() + "  count = " + cache.count());

        IOUtils.close(secondReader, writer, dir, cache);
        terminate(threadPool);
    }

//    static class Key implements Accountable {
//        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Key.class);
//
//        public final IndicesRequestCache.CacheEntity entity; // use as identity equality
//        public final IndexReader.CacheKey readerCacheKey;
//        public final BytesReference value;
//
//        Key(IndicesRequestCache.CacheEntity entity, IndexReader.CacheKey readerCacheKey, BytesReference value) {
//            this.entity = entity;
//            this.readerCacheKey = Objects.requireNonNull(readerCacheKey);
//            this.value = value;
//        }
//
//        @Override
//        public long ramBytesUsed() {
//            return BASE_RAM_BYTES_USED + entity.ramBytesUsed() + value.length();
//        }
//
//    }

//    public void testStaleCount_OnRemovalNotificationOfStaleKey_DoesNotDecrementsStaleCount() throws Exception {
//        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
//        IndexShard indexShard = createIndex("test").getShard(0);
//        ThreadPool threadPool = getThreadPool();
//        Settings settings = Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), "0.51").build();
//        IndicesRequestCache cache = new IndicesRequestCache(settings, (shardId -> {
//            IndexService indexService = null;
//            try {
//                indexService = indicesService.indexServiceSafe(shardId.getIndex());
//            } catch (IndexNotFoundException ex) {
//                return Optional.empty();
//            }
//            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
//        }), new CacheModule(new ArrayList<>(), settings).getCacheService(), threadPool);
//        Directory dir = newDirectory();
//        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
//
//        writer.addDocument(newDoc(0, "foo"));
//        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
//        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
//        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
//        if (randomBoolean()) {
//            writer.flush();
//            IOUtils.close(writer);
//            writer = new IndexWriter(dir, newIndexWriterConfig());
//        }
//        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
//        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
//
//        // Get 2 entries into the cache
//        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
//        Loader loader = new Loader(reader, 0);
//        cache.getOrCompute(entity, loader, reader, termBytes);
//
//        entity = new IndicesService.IndexShardCacheEntity(indexShard);
//        loader = new Loader(reader, 0);
//        cache.getOrCompute(entity, loader, reader, termBytes);
//
//        IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
//        loader = new Loader(secondReader, 0);
//        cache.getOrCompute(entity, loader, secondReader, termBytes);
//
//        secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
//        loader = new Loader(secondReader, 0);
//        cache.getOrCompute(secondEntity, loader, secondReader, termBytes);
//        assertEquals(2, cache.count());
//
//        // Close the reader, to be enqueued for cleanup
//        reader.close();
//        AtomicInteger staleKeysCount = cache.cacheCleanupManager.getStaleKeysCount();
//        // 1 out of 2 keys ie 50% are now stale.
//        assertEquals(1, staleKeysCount.get());
//        // cache count should not be affected
//        assertEquals(2, cache.count());
//
//        OpenSearchDirectoryReader.DelegatingCacheHelper delegatingCacheHelper = (OpenSearchDirectoryReader.DelegatingCacheHelper) reader
//            .getReaderCacheHelper();
//        String readerCacheKeyId = delegatingCacheHelper.getDelegatingCacheKey().getId();
//        IndicesRequestCache.Key key = new IndicesRequestCache.Key(
//            ((IndexShard) secondEntity.getCacheIdentity()).shardId(),
//            termBytes,
//            readerCacheKeyId
//        );
//
//        cache.onRemoval(new RemovalNotification<IndicesRequestCache.Key, BytesReference>(key, termBytes, RemovalReason.EVICTED));
//        staleKeysCount = cache.cacheCleanupManager.getStaleKeysCount();
//        // eviction of NON-stale key from the cache should NOT decrement staleKeysCount in iRC
//        assertEquals(1, staleKeysCount.get());
//
//        IOUtils.close(secondReader, writer, dir, cache);
//        terminate(threadPool);
//    }

    public void testCacheCleanupBasedOnStaleThreshold_StalenessGreaterThanThreshold() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard indexShard = createIndex("test").getShard(0);
        ThreadPool threadPool = getThreadPool();
        Settings settings = Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), "0.49").build();
        IndicesRequestCache cache = new IndicesRequestCache(settings, (shardId -> {
            IndexService indexService = null;
            try {
                indexService = indicesService.indexServiceSafe(shardId.getIndex());
            } catch (IndexNotFoundException ex) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), settings).getCacheService(), threadPool);
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        if (randomBoolean()) {
            writer.flush();
            IOUtils.close(writer);
            writer = new IndexWriter(dir, newIndexWriterConfig());
        }
        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));

        // Get 2 entries into the cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        cache.getOrCompute(entity, loader, reader, termBytes);

        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        cache.getOrCompute(entity, loader, reader, termBytes);

        IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        cache.getOrCompute(entity, loader, secondReader, termBytes);

        secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        cache.getOrCompute(secondEntity, loader, secondReader, termBytes);
        assertEquals(2, cache.count());

        // Close the reader, to be enqueued for cleanup
        // 1 out of 2 keys ie 50% are now stale.
        reader.close();
        // cache count should not be affected
        assertEquals(2, cache.count());

        // clean cache with 49% staleness threshold
        cache.cacheCleanupManager.cleanCache();
        // cleanup should have taken effect with 49% threshold
        assertEquals(1, cache.count());

        IOUtils.close(secondReader, writer, dir, cache);
        terminate(threadPool);
    }

    public void testCacheCleanupBasedOnStaleThreshold_StalenessLesserThanThreshold() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard indexShard = createIndex("test").getShard(0);
        ThreadPool threadPool = getThreadPool();
        Settings settings = Settings.builder().put(INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING.getKey(), "51%").build();
        IndicesRequestCache cache = new IndicesRequestCache(settings, (shardId -> {
            IndexService indexService = null;
            try {
                indexService = indicesService.indexServiceSafe(shardId.getIndex());
            } catch (IndexNotFoundException ex) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(), threadPool);
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        if (randomBoolean()) {
            writer.flush();
            IOUtils.close(writer);
            writer = new IndexWriter(dir, newIndexWriterConfig());
        }
        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));

        // Get 2 entries into the cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        cache.getOrCompute(entity, loader, reader, termBytes);

        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        cache.getOrCompute(entity, loader, reader, termBytes);

        IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        cache.getOrCompute(entity, loader, secondReader, termBytes);

        secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        cache.getOrCompute(secondEntity, loader, secondReader, termBytes);
        assertEquals(2, cache.count());

        // Close the reader, to be enqueued for cleanup
        // 1 out of 2 keys ie 50% are now stale.
        reader.close();
        // cache count should not be affected
        assertEquals(2, cache.count());

        // clean cache with 51% staleness threshold
        cache.cacheCleanupManager.cleanCache();
        // cleanup should have been ignored
        assertEquals(2, cache.count());

        IOUtils.close(secondReader, writer, dir, cache);
        terminate(threadPool);
    }

    public void testEviction() throws Exception {
        final ByteSizeValue size;
        {
            IndexShard indexShard = createIndex("test").getShard(0);
            ThreadPool threadPool = getThreadPool();
            IndicesRequestCache cache = new IndicesRequestCache(
                Settings.EMPTY,
                (shardId -> Optional.of(new IndicesService.IndexShardCacheEntity(indexShard))),
                new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(),
                threadPool
            );
            Directory dir = newDirectory();
            IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

            writer.addDocument(newDoc(0, "foo"));
            DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
            TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
            BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
            IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
            Loader loader = new Loader(reader, 0);

            writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
            DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
            IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
            Loader secondLoader = new Loader(secondReader, 0);

            BytesReference value1 = cache.getOrCompute(entity, loader, reader, termBytes);
            assertEquals("foo", value1.streamInput().readString());
            BytesReference value2 = cache.getOrCompute(secondEntity, secondLoader, secondReader, termBytes);
            assertEquals("bar", value2.streamInput().readString());
            size = indexShard.requestCache().stats().getMemorySize();
            IOUtils.close(reader, secondReader, writer, dir, cache);
            terminate(threadPool);
        }
        IndexShard indexShard = createIndex("test1").getShard(0);
        ThreadPool threadPool = getThreadPool();
        IndicesRequestCache cache = new IndicesRequestCache(
            Settings.builder().put(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.getKey(), size.getBytes() + 1 + "b").build(),
            (shardId -> Optional.of(new IndicesService.IndexShardCacheEntity(indexShard))),
            new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(),
            threadPool
        );
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader secondLoader = new Loader(secondReader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "baz"));
        DirectoryReader thirdReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        IndicesService.IndexShardCacheEntity thirddEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader thirdLoader = new Loader(thirdReader, 0);

        BytesReference value1 = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value1.streamInput().readString());
        BytesReference value2 = cache.getOrCompute(secondEntity, secondLoader, secondReader, termBytes);
        assertEquals("bar", value2.streamInput().readString());
        logger.info("Memory size: {}", indexShard.requestCache().stats().getMemorySize());
        BytesReference value3 = cache.getOrCompute(thirddEntity, thirdLoader, thirdReader, termBytes);
        assertEquals("baz", value3.streamInput().readString());
        assertEquals(2, cache.count());
        assertEquals(1, indexShard.requestCache().stats().getEvictions());
        IOUtils.close(reader, secondReader, thirdReader, writer, dir, cache);
        terminate(threadPool);
    }

    public void testClearAllEntityIdentity() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard indexShard = createIndex("test").getShard(0);
        ThreadPool threadPool = getThreadPool();
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, (shardId -> {
            IndexService indexService = null;
            try {
                indexService = indicesService.indexServiceSafe(shardId.getIndex());
            } catch (IndexNotFoundException ex) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(), threadPool);

        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader secondLoader = new Loader(secondReader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "baz"));
        DirectoryReader thirdReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        IndicesService.IndexShardCacheEntity thirddEntity = new IndicesService.IndexShardCacheEntity(createIndex("test1").getShard(0));
        Loader thirdLoader = new Loader(thirdReader, 0);

        BytesReference value1 = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value1.streamInput().readString());
        BytesReference value2 = cache.getOrCompute(secondEntity, secondLoader, secondReader, termBytes);
        assertEquals("bar", value2.streamInput().readString());
        logger.info("Memory size: {}", indexShard.requestCache().stats().getMemorySize());
        BytesReference value3 = cache.getOrCompute(thirddEntity, thirdLoader, thirdReader, termBytes);
        assertEquals("baz", value3.streamInput().readString());
        assertEquals(3, cache.count());
        RequestCacheStats requestCacheStats = entity.stats().stats();
        requestCacheStats.add(thirddEntity.stats().stats());
        final long hitCount = requestCacheStats.getHitCount();
        // clear all for the indexShard Idendity even though is't still open
        cache.clear(randomFrom(entity, secondEntity));
        cache.cacheCleanupManager.cleanCache();
        assertEquals(1, cache.count());
        // third has not been validated since it's a different identity
        value3 = cache.getOrCompute(thirddEntity, thirdLoader, thirdReader, termBytes);
        requestCacheStats = entity.stats().stats();
        requestCacheStats.add(thirddEntity.stats().stats());
        assertEquals(hitCount + 1, requestCacheStats.getHitCount());
        assertEquals("baz", value3.streamInput().readString());

        IOUtils.close(reader, secondReader, thirdReader, writer, dir, cache);
        terminate(threadPool);
    }

    public Iterable<Field> newDoc(int id, String value) {
        return Arrays.asList(
            newField("id", Integer.toString(id), StringField.TYPE_STORED),
            newField("value", value, StringField.TYPE_STORED)
        );
    }

    private static class Loader implements CheckedSupplier<BytesReference, IOException> {

        private final DirectoryReader reader;
        private final int id;
        public boolean loadedFromCache = true;

        Loader(DirectoryReader reader, int id) {
            super();
            this.reader = reader;
            this.id = id;
        }

        @Override
        public BytesReference get() {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                TopDocs topDocs = searcher.search(new TermQuery(new Term("id", Integer.toString(id))), 1);
                assertEquals(1, topDocs.totalHits.value);
                Document document = reader.storedFields().document(topDocs.scoreDocs[0].doc);
                out.writeString(document.get("value"));
                loadedFromCache = false;
                return out.bytes();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public void testInvalidate() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard indexShard = createIndex("test").getShard(0);
        ThreadPool threadPool = getThreadPool();
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, (shardId -> {
            IndexService indexService = null;
            try {
                indexService = indicesService.indexServiceSafe(shardId.getIndex());
            } catch (IndexNotFoundException ex) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(), threadPool);
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);

        // initial cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        ShardRequestCache requestCacheStats = entity.stats();
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());

        // cache hit
        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = entity.stats();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // load again after invalidate
        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        cache.invalidate(entity, reader, termBytes);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = entity.stats();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // release
        if (randomBoolean()) {
            reader.close();
        } else {
            indexShard.close("test", true, true); // closed shard but reader is still open
            cache.clear(entity);
        }
        cache.cacheCleanupManager.cleanCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(reader, writer, dir, cache);
        terminate(threadPool);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testEqualsKey() throws IOException {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        Directory dir = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, config);
        ShardId shardId = new ShardId("foo", "bar", 1);
        ShardId shardId1 = new ShardId("foo1", "bar1", 2);
        IndexReader reader1 = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), shardId);
        String rKey1 = ((OpenSearchDirectoryReader) reader1).getDelegatingCacheHelper().getDelegatingCacheKey().getId();
        writer.addDocument(new Document());
        IndexReader reader2 = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), shardId);
        String rKey2 = ((OpenSearchDirectoryReader) reader2).getDelegatingCacheHelper().getDelegatingCacheKey().getId();
        IOUtils.close(reader1, reader2, writer, dir);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);
        IndicesRequestCache.Key key1 = new IndicesRequestCache.Key(shardId, new TestBytesReference(1), rKey1, shardId.hashCode());
        IndicesRequestCache.Key key2 = new IndicesRequestCache.Key(shardId, new TestBytesReference(1), rKey1, shardId.hashCode());
        IndicesRequestCache.Key key3 = new IndicesRequestCache.Key(shardId1, new TestBytesReference(1), rKey1,
            shardId1.hashCode());
        IndicesRequestCache.Key key4 = new IndicesRequestCache.Key(shardId, new TestBytesReference(1), rKey2, shardId.hashCode());
        IndicesRequestCache.Key key5 = new IndicesRequestCache.Key(shardId, new TestBytesReference(2), rKey2, shardId.hashCode());
        String s = "Some other random object";
        assertEquals(key1, key1);
        assertEquals(key1, key2);
        assertNotEquals(key1, null);
        assertNotEquals(key1, s);
        assertNotEquals(key1, key3);
        assertNotEquals(key1, key4);
        assertNotEquals(key1, key5);
    }

    public void testSerializationDeserializationOfCacheKey() throws Exception {
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        IndexService indexService = createIndex("test");
        IndexShard indexShard = indexService.getShard(0);
        IndicesService.IndexShardCacheEntity shardCacheEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        String readerCacheKeyId = UUID.randomUUID().toString();
        IndicesRequestCache.Key key1 = new IndicesRequestCache.Key(indexShard.shardId(), termBytes, readerCacheKeyId,
            indexShard.hashCode());
        BytesReference bytesReference = null;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            key1.writeTo(out);
            bytesReference = out.bytes();
        }
        StreamInput in = bytesReference.streamInput();

        IndicesRequestCache.Key key2 = new IndicesRequestCache.Key(in);

        assertEquals(readerCacheKeyId, key2.readerCacheKeyId);
        assertEquals(((IndexShard) shardCacheEntity.getCacheIdentity()).shardId(), key2.shardId);
        assertEquals(termBytes, key2.value);

    }

    public void testConcurrentIndexingAndRefresh() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        String index1 = "test1";
        String index2 = "test2";
        IndexShard indexShard = createIndex(index1, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()).getShard(0);
        IndexShard indexShard1 = createIndex(index2, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()).getShard(0);
        ThreadPool threadPool = getThreadPool();
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, (shardId -> {
            IndexService indexService = indicesService.indices.get(shardId.getIndex().getUUID());
            if (indexService == null) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(), threadPool);
        Directory dir = newDirectory();
        Directory dir1 = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
        IndexWriter writer1 = new IndexWriter(dir1, newIndexWriterConfig());


//        List<String> values = new ArrayList<>();
//        List<String>
//        for (int i = 0; i < 1000; i++) {
//
//        }
        writer.addDocument(newDoc(0, "foo"));
        writer1.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), indexShard.shardId());
        DirectoryReader reader1 = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer1), indexShard1.shardId());

        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        IndicesService.IndexShardCacheEntity entity1 = new IndicesService.IndexShardCacheEntity(indexShard1);

        int numberOfThreads = 5;
        Thread[] threads = new Thread[numberOfThreads];
        Map<IndexShard, List<BytesReference>> serializedTermQueriesMap = new ConcurrentHashMap<>();
        Map<IndexShard, BytesReference> valueListMap = new ConcurrentHashMap<>();
        Map<IndexShard, DirectoryReader> readerMap = new ConcurrentHashMap<>();
        Map<IndexShard, IndicesService.IndexShardCacheEntity> map = new HashMap<>();
        map.put(indexShard, entity);
        map.put(indexShard1, entity1);
        readerMap.put(indexShard, reader);
        readerMap.put(indexShard1, reader1);

        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        List<IndexShard> indexShardList = List.of(indexShard, indexShard1);
        CountDownLatch count = new CountDownLatch(numberOfThreads);
        long startTime = System.currentTimeMillis();
        int timeoutInSeconds = 20; // Timeout duration in seconds
        for (int i = 0; i< numberOfThreads; i++) {
            executor.submit(() -> {
                while(true) {
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    if (elapsedTime >= timeoutInSeconds * 1000) {
                        System.out.println("Timeout reached");
                        count.countDown();
                        break;
                    }
                    int position = 0;
                    if (randomBoolean()) {
                        position = 1;
                    }
                    TermQueryBuilder termQuery = new TermQueryBuilder("id", UUID.randomUUID().toString());
                    BytesReference termBytes = null;
                    try {
                        termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    serializedTermQueriesMap.computeIfAbsent(indexShardList.get(position),
                        k -> Collections.synchronizedList(new ArrayList<>())).add(termBytes);
                    Loader loader = new Loader(readerMap.get(indexShardList.get(position)), 0);
                    try {
                        BytesReference value =
                            cache.getOrCompute(map.get(indexShardList.get(position)), loader,
                                readerMap.get(indexShardList.get(position)), termBytes);
                        assertNotNull(value);
                        //assertTrue(loader.loadedFromCache);
                        //System.out.println("index shard stats : " +  indexShardList.get(position).requestCache()
                        // .stats().getMemorySizeInBytes());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    valueListMap.put(indexShardList.get(position), termBytes);
                }
            });
        }
        long startTimeForInvalidationBackground = System.currentTimeMillis();
        int numberOfThreadsForInvalidation = 2;
        ExecutorService executor1 = Executors.newFixedThreadPool(numberOfThreadsForInvalidation);
        CountDownLatch latchForInvalidation = new CountDownLatch(numberOfThreadsForInvalidation);
        AtomicBoolean indexDeleted = new AtomicBoolean(false);
        for (int  i = 0; i < numberOfThreadsForInvalidation; i++) {
            executor.submit(() -> {
               while (true) {
                   long elapsedTime = System.currentTimeMillis() - startTimeForInvalidationBackground;
                   if (elapsedTime >= timeoutInSeconds * 1000) {
                       System.out.println("Timeout reached for invalidation");
                       latchForInvalidation.countDown();
                       break;
                   }
                   if (randomBoolean()) {
                       if (randomBoolean()) {
                           if (randomBoolean()) {
                               System.out.println("Deleting one of the index: " + indexShardList.get(1).shardId().getIndex().getName());
                               if (!indexDeleted.get()) {
                                   System.out.println("Actual deleting");
                                   IndexShard indexShardDeleted = indexShardList.get(1);
                                   IndicesRequestCache.CacheEntity entity2 = map.get(indexShardDeleted);
                                   cache.clear(entity2);
                                   indexDeleted.set(true);
                               }
                           }
                       }
                   }
                   int position = 0;
                   if (randomBoolean() && !indexDeleted.get()) {
                       position = 1;
                   }
                   List<BytesReference> bytesReferenceList = serializedTermQueriesMap.get(indexShardList.get(position));
                   if (bytesReferenceList.size() > 0 ) {
                       BytesReference reference = bytesReferenceList.get(randomIntBetween(0, bytesReferenceList.size() - 1));
                       //System.out.println("refeerenceeeeeee");
                       cache.invalidate(map.get(indexShardList.get(position)),
                           readerMap.get(indexShardList.get(position)), reference);
                       //System.out.println("asdsadsadsa");
                   }
               }
            });
        }
        count.await();
        latchForInvalidation.await();
        //System.out.println("index shard stats : " +  indexShard.requestCache().stats().getMemorySizeInBytes());

        RequestCacheStats requestCacheStats = entity.stats().stats();
        RequestCacheStats requestCacheStats1 = entity1.stats().stats();
        System.out.println("request stats hits for entity1 = " + requestCacheStats.getHitCount() + " miss = " + requestCacheStats.getMissCount() + " memory_size_in_bytes = " + requestCacheStats.getMemorySizeInBytes() + " evictions = " + requestCacheStats.getEvictions());
        System.out.println("request stats hits for entity2 = " + requestCacheStats1.getHitCount() + " miss = " + requestCacheStats1.getMissCount() + " memory_size_in_bytes = " + requestCacheStats1.getMemorySizeInBytes() + " evictions = " + requestCacheStats1.getEvictions());
//        while (true) {
//            executor.submit(() -> {
//                int position = 0;
//                if (randomBoolean()) {
//                    position = 1;
//                }
//                TermQueryBuilder termQuery = new TermQueryBuilder("id", UUID.randomUUID().toString());
//                BytesReference termBytes = null;
//                try {
//                    termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//                serializedTermQueriesMap.put(indexShardList.get(position), termBytes);
//                Loader loader = new Loader(readerMap.get(position), 0);
//                try {
//                    BytesReference value =
//                        cache.getOrCompute(new IndicesService.IndexShardCacheEntity(indexShardList.get(position)), loader,
//                            readerMap.get(position), termBytes);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//                valueListMap.put(indexShardList.get(position), termBytes);
//            });
//        }

        IOUtils.close(reader, writer, reader1, writer1, dir, dir1, cache);
        // initial cache
        //IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);

    }

    public void testComputeIfAbsentConcurrently() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        String index1 = "test1";
        IndexShard indexShard = createIndex(index1, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()).getShard(0);
        ThreadPool threadPool = getThreadPool();
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, (shardId -> {
            IndexService indexService = indicesService.indices.get(shardId.getIndex().getUUID());
            if (indexService == null) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(), threadPool);
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        for (int  i = 0; i < 100; i ++) {
            writer.addDocument(newDoc(i, generateString(randomIntBetween(4, 50))));
        }
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer),indexShard.shardId());

        int numberOfKeys = 30;
        Thread[] threads = new Thread[numberOfKeys];
        Phaser phaser = new Phaser(numberOfKeys + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numberOfKeys); // To wait for all threads to finish.

        //ExecutorService executor1 = Executors.newFixedThreadPool(30);

        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        CounterMetric metric = new CounterMetric();
        for (int i = 0; i < numberOfKeys; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                TermQueryBuilder termQuery = new TermQueryBuilder("id", generateString(randomIntBetween(4, 50)));
                BytesReference termBytes = null;
                try {
                    termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
                    metric.inc(termBytes.length());
                    System.out.println("size = " +  termBytes.length());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                Loader loader = new Loader(reader, finalI);
                try {
                    phaser.arriveAndAwaitAdvance();
                    BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
                    System.out.println("value size = " + value.length());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        RequestCacheStats requestCacheStats = entity.stats().stats();
        System.out.println("request stats hits for entity1 = " + requestCacheStats.getHitCount() + " miss = " + requestCacheStats.getMissCount() + " memory_size_in_bytes = " + requestCacheStats.getMemorySizeInBytes() + " evictions = " + requestCacheStats.getEvictions());
        cache.clear(entity);
        requestCacheStats = entity.stats().stats();
        System.out.println("request stats hits for entity1 = " + requestCacheStats.getHitCount() + " miss = " + requestCacheStats.getMissCount() + " memory_size_in_bytes = " + requestCacheStats.getMemorySizeInBytes() + " evictions = " + requestCacheStats.getEvictions());
        IOUtils.close(reader, writer, dir, cache);
    }

    public void testRemovalWithMultipleIndicesConcurrently() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        int numberOfIndices = 50;
        List<String> indicesList = new ArrayList<>();
        List<IndexShard> indexShardList = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = "test" + i;
            indicesList.add(indexName);
            IndexShard indexShard = createIndex(indexName, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()).getShard(0);
            indexShardList.add(indexShard);
        }
        ThreadPool threadPool = getThreadPool();
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, (shardId -> {
            IndexService indexService = indicesService.indices.get(shardId.getIndex().getUUID());
            if (indexService == null) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(), threadPool);
        Map<IndexShard, DirectoryReader> readerMap = new ConcurrentHashMap<>();
        Map<IndexShard, IndicesService.IndexShardCacheEntity> entityMap = new ConcurrentHashMap<>();
        Map<IndexShard, IndexWriter> writerMap = new ConcurrentHashMap<>();
        int numberOfItems = randomIntBetween(200, 400);
        List<Directory> directories = new ArrayList<>();
        for (int i = 0; i < numberOfIndices; i++) {
            IndexShard  indexShard = indexShardList.get(i);
            entityMap.put(indexShard, new IndicesService.IndexShardCacheEntity(indexShard));
            Directory dir = newDirectory();
            directories.add(dir);
            IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
            for (int j = 0; j < numberOfItems; j++) {
                writer.addDocument(newDoc(j, generateString(randomIntBetween(4, 50))));
            }
            writerMap.put(indexShard, writer);
            DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer),
                indexShard.shardId());
            readerMap.put(indexShard, reader);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(5);

        System.out.println("numberOfItems = " + numberOfItems);
        CountDownLatch latch = new CountDownLatch(numberOfItems);
        Map<IndexShard, List<BytesReference>> termQueryMap = new ConcurrentHashMap<>();
        Map<BytesReference, List<BytesReference>> valueMap = new ConcurrentHashMap<>();
        CounterMetric metric = new CounterMetric();
        List<RemovalNotification<IndicesRequestCache.Key, BytesReference>> removalNotifications =
            Collections.synchronizedList(new ArrayList<>());
        for (int  i = 0; i < numberOfItems; i++) {
            int finalI = i;
            executorService.submit(() -> {
                try {
                    metric.inc(1);
                    int randomIndexPosition = randomIntBetween(0, numberOfIndices - 1);
                    IndexShard indexShard = indexShardList.get(randomIndexPosition);
                    TermQueryBuilder termQuery = new TermQueryBuilder("id", generateString(randomIntBetween(4, 50)));
                    BytesReference termBytes = null;
                    try {
                        termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
                        //metric.inc(termBytes.length());
                        //System.out.println("size = " +  termBytes.length());
                    } catch (IOException e) {
                        System.out.println("exception1 " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    synchronized (termQueryMap) {
                        List<BytesReference> bytesReferenceList = termQueryMap.computeIfAbsent(indexShard, k -> new ArrayList<>());
                        bytesReferenceList.add(termBytes);
                        //termQueryMap.computeIfAbsent(indexShard, k -> new ArrayList<>()).add(termBytes);
                    }

                    Loader loader = new Loader(readerMap.get(indexShard), finalI);
                    try {
                        //phaser.arriveAndAwaitAdvance();
                        BytesReference value = cache.getOrCompute(entityMap.get(indexShard), loader, readerMap.get(indexShard), termBytes);
                        synchronized (valueMap) {
                            List<BytesReference> bytesReferenceList = valueMap.computeIfAbsent(termBytes,
                                k -> new ArrayList<>());
                            bytesReferenceList.add(value);
                        }
                        OpenSearchDirectoryReader.DelegatingCacheHelper delegatingCacheHelper =
                            (OpenSearchDirectoryReader.DelegatingCacheHelper) readerMap.get(indexShard)
                                .getReaderCacheHelper();
                        String readerCacheKeyId = delegatingCacheHelper.getDelegatingCacheKey().getId();
                        removalNotifications.add(new RemovalNotification<>(new IndicesRequestCache.Key(indexShard.shardId(), termBytes, readerCacheKeyId, indexShard.hashCode()), value,
                            RemovalReason.EVICTED));
                    } catch (Exception e) {
                        System.out.println("exception2 " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    latch.countDown();
                } catch (Exception ex) {
                    System.out.println("exceptionssssssssss " + ex.getMessage() + " exception = " + ex);
                }
            });
        }
        System.out.println("Reached here!!!");
        latch.await();
        int actual = 0;
        for (Map.Entry<IndexShard, List<BytesReference>> entry: termQueryMap.entrySet()) {
            List<BytesReference> bytesReferenceList = entry.getValue();
            actual += bytesReferenceList.size();
        }
        assertEquals(numberOfItems, actual);
        getNodeCacheStats(client());


        List<IndexShard> copyIndexShardList = Collections.synchronizedList(new ArrayList<>());
        copyIndexShardList.addAll(indexShardList);
        CountDownLatch latch1 = new CountDownLatch(numberOfItems);
        CounterMetric metric1 = new CounterMetric();

        for (int i = 0; i < indicesList.size(); i++) {
            IndexShard indexShard = indexShardList.get(i);
            System.out.println("memory size for index " +  indexShard.shardId().getIndex().getName() + " size: " + indexShard.requestCache().stats().getMemorySizeInBytes());
        }
        assertEquals(removalNotifications.size(), numberOfItems);
        for (int i = 0; i < removalNotifications.size(); i++) {
            int finalI = i;
            executorService.submit(() -> {
                cache.onRemoval(removalNotifications.get(finalI));
                latch1.countDown();
            });

        }
//        for (int i = 0; i < numberOfItems * 2; i++) {
//            int finalI = i;
//            //executorService.submit(() -> {
//                try {
//                    int randomIndexPosition = randomIntBetween(0, numberOfIndices - 1);
//                    IndexShard indexShard = indexShardList.get(randomIndexPosition);
//                    OpenSearchDirectoryReader.DelegatingCacheHelper delegatingCacheHelper =
//                        (OpenSearchDirectoryReader.DelegatingCacheHelper) readerMap.get(indexShard)
//                            .getReaderCacheHelper();
//                    String readerCacheKeyId = delegatingCacheHelper.getDelegatingCacheKey().getId();
//
//                    List<BytesReference> termBytes = termQueryMap.get(indexShard);
//                    if (termBytes != null && termBytes.size() > 0) {
//                        BytesReference bytesReference = null;
//                        synchronized (termQueryMap) {
//                            int randomIndex = ThreadLocalRandom.current().nextInt(termBytes.size());
//                            bytesReference = termBytes.remove(randomIndex);
//                            // Update the map with the modified list
//                            termQueryMap.put(indexShard, termBytes);
//                        }
//                        List<BytesReference> value = valueMap.get(bytesReference);
//                        BytesReference bytesReference1 = value.get(randomIntBetween(0, value.size() - 1));
//                        RemovalNotification<IndicesRequestCache.Key, BytesReference> removalNotification =
//                            new RemovalNotification<>(new IndicesRequestCache.Key(indexShard.shardId(),
//                                bytesReference, readerCacheKeyId),
//                                bytesReference1, RemovalReason.EVICTED);
//                        cache.onRemoval(removalNotification);
//                    } else {
//                    }
//                    latch1.countDown();
//                } catch (Exception ex) {
//                    System.out.println("Exception: " + ex + " message: " + ex.getMessage());
//                }
//                    //System.out.println("latch countDown = " + latch1.getCount());
//           /// });
//        }


//        // Invalidate
//        for (int i = 0; i < numberOfIndices; i++) {
//            int finalI = i;
//            executorService.submit(() -> {
//                int randomIndexPosition = finalI;
//                IndexShard indexShard = copyIndexShardList.get(randomIndexPosition);
//                indicesService.removeIndex(indexShard.shardId().getIndex(),
//                    IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED, "for testing");
////                List<BytesReference> bytesReferenceList = termQueryMap.get(indexShard);
////                BytesReference bytesReference = bytesReferenceList.get(randomIntBetween(0,
////                    bytesReferenceList.size() - 1));
////                cache.invalidate(entityMap.get(indexShard),
////                    readerMap.get(indexShard), bytesReference);
//                metric1.inc();
//                latch1.countDown();
//            });
//        }
        System.out.println("waiting!!!");
        latch1.await();
        //Thread.sleep(16000);

        for (int i = 0; i < indicesList.size(); i++) {
            IndexShard indexShard = indexShardList.get(i);
            System.out.println("memory size for index " +  indexShard.shardId().getIndex().getName() + " size: " + indexShard.requestCache().stats().getMemorySizeInBytes());
        }

        getNodeCacheStats(client());
        for (int i = 0; i < numberOfIndices; i++) {
            IndexShard indexShard = indexShardList.get(i);
            readerMap.get(indexShard).close();
            writerMap.get(indexShard).close();
            writerMap.get(indexShard).getDirectory().close();
        }
//        directories.stream().forEach(directory -> {
//            try {
//                directory.close();
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        });
        IOUtils.close(cache);
    }

    public void testWithNullShardId() {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        String index1 = "test1";
        IndexShard indexShard = createIndex(index1, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()).getShard(0);
        ThreadPool threadPool = getThreadPool();
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, (shardId -> {
            IndexService indexService = indicesService.indices.get(shardId.getIndex().getUUID());
            if (indexService == null) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(4)));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(), threadPool);

        TermQueryBuilder termQuery = new TermQueryBuilder("id", generateString(randomIntBetween(4, 50)));
        BytesReference termBytes = null;
        try {
            termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        IndicesRequestCache.Key key = new IndicesRequestCache.Key(
            indexShard.shardId(),
            termBytes,
            UUID.randomUUID().toString(),
            indexShard.hashCode()
        );

        cache.onRemoval(new RemovalNotification<>(key, termBytes, RemovalReason.EVICTED));
    }

    public void testCleanCacheWithScenarios() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        String index1 = "test1";
        IndexShard indexShard = createIndex(index1, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()).getShard(0);
        ThreadPool threadPool = getThreadPool();
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, (shardId -> {
            IndexService indexService = indicesService.indices.get(shardId.getIndex().getUUID());
            if (indexService == null) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(), threadPool);

        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), indexShard.shardId());
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        if (randomBoolean()) {
            writer.flush();
            IOUtils.close(writer);
            writer = new IndexWriter(dir, newIndexWriterConfig());
        }
        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), indexShard.shardId());

        // initial cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        ShardRequestCache requestCacheStats = entity.stats();
        assertEquals("foo", value.streamInput().readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        final int cacheSize = requestCacheStats.stats().getMemorySize().bytesAsInt();
        assertEquals(1, cache.numRegisteredCloseListeners());

        // cache the second
        IndicesService.IndexShardCacheEntity secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        value = cache.getOrCompute(entity, loader, secondReader, termBytes);
        requestCacheStats = entity.stats();
        assertEquals("bar", value.streamInput().readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(2, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > cacheSize + value.length());
        assertEquals(2, cache.numRegisteredCloseListeners());

        secondEntity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(secondReader, 0);
        value = cache.getOrCompute(secondEntity, loader, secondReader, termBytes);
        requestCacheStats = entity.stats();
        assertEquals("bar", value.streamInput().readString());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(2, cache.count());

        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        requestCacheStats = entity.stats();
        assertEquals(2, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(2, cache.count());
        System.out.println("memory size = " + entity.stats().stats().getMemorySizeInBytes());

        // Closing the cache doesn't change returned entities
        reader.close();
        cache.cacheCleanupManager.cleanCache();
//        Thread thread = new Thread(() -> {
//
//        });
//        IndicesService.IndexShardCacheEntity finalEntity = entity;
//        Thread thread1 = new Thread(() -> {
//           cache.clear(finalEntity);
//        });

        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        System.out.println("memory size after cleanup = " + entity.stats().stats().getMemorySizeInBytes());
        //assertEquals(cacheSize, requestCacheStats.stats().getMemorySize().bytesAsInt());
        //assertEquals(1, cache.numRegisteredCloseListeners());

        // release
//        if (randomBoolean()) {
//            secondReader.close();
//        } else {
//            indexShard.close("test", true, true); // closed shard but reader is still open
//            cache.clear(secondEntity);
//        }
//        cache.cacheCleanupManager.cleanCache();
//        assertEquals(2, requestCacheStats.stats().getMissCount());
//        assertEquals(0, requestCacheStats.stats().getEvictions());
//        assertTrue(loader.loadedFromCache);
//        assertEquals(0, cache.count());
//        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(secondReader, writer, dir, cache);
        terminate(threadPool);
        //assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testComputeIfAbsentConcurrentlyAndInvalidateSequentially() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        int numberOfIndices = 100;
        List<String> indicesList = new ArrayList<>();
        List<IndexShard> indexShardList = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = "test" + i;
            indicesList.add(indexName);
            IndexShard indexShard = createIndex(indexName, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()).getShard(0);
            indexShardList.add(indexShard);
        }
        ThreadPool threadPool = getThreadPool();
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, (shardId -> {
            IndexService indexService = indicesService.indices.get(shardId.getIndex().getUUID());
            if (indexService == null) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(), threadPool);
        Map<IndexShard, DirectoryReader> readerMap = new ConcurrentHashMap<>();
        Map<IndexShard, IndicesService.IndexShardCacheEntity> entityMap = new ConcurrentHashMap<>();
        Map<IndexShard, IndexWriter> writerMap = new ConcurrentHashMap<>();
        int numberOfItems = randomIntBetween(1000, 4000);
        List<Directory> directories = new ArrayList<>();
        for (int i = 0; i < numberOfIndices; i++) {
            IndexShard  indexShard = indexShardList.get(i);
            entityMap.put(indexShard, new IndicesService.IndexShardCacheEntity(indexShard));
            Directory dir = newDirectory();
            directories.add(dir);
            IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
            for (int j = 0; j < numberOfItems; j++) {
                writer.addDocument(newDoc(j, generateString(randomIntBetween(4, 300))));
            }
            writerMap.put(indexShard, writer);
            DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer),
                indexShard.shardId());
            readerMap.put(indexShard, reader);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(15);


        CountDownLatch latch = new CountDownLatch(numberOfItems);
        Map<IndexShard, List<BytesReference>> termQueryMap = new ConcurrentHashMap<>();
        Map<BytesReference, List<BytesReference>> valueMap = new ConcurrentHashMap<>();
        CounterMetric metric = new CounterMetric();
        List<RemovalNotification<IndicesRequestCache.Key, BytesReference>> removalNotifications =
            Collections.synchronizedList(new ArrayList<>());
        for (int  i = 0; i < numberOfItems; i++) {
            int finalI = i;
            executorService.submit(() -> {
                try {
                    metric.inc(1);
                    int randomIndexPosition = randomIntBetween(0, numberOfIndices - 1);
                    IndexShard indexShard = indexShardList.get(randomIndexPosition);
                    TermQueryBuilder termQuery = new TermQueryBuilder("id", generateString(randomIntBetween(4, 300)));
                    BytesReference termBytes = null;
                    try {
                        termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
                        //metric.inc(termBytes.length());
                        System.out.println("size = " +  termBytes.length());
                    } catch (IOException e) {
                        System.out.println("exception1 " + e.getMessage());
                        throw new RuntimeException(e);
                    }
//                    synchronized (termQueryMap) {
//                        List<BytesReference> bytesReferenceList = termQueryMap.computeIfAbsent(indexShard, k -> new ArrayList<>());
//                        bytesReferenceList.add(termBytes);
//                        //termQueryMap.computeIfAbsent(indexShard, k -> new ArrayList<>()).add(termBytes);
//                    }

                    Loader loader = new Loader(readerMap.get(indexShard), finalI);
                    try {
                        //phaser.arriveAndAwaitAdvance();
                        BytesReference value = cache.getOrCompute(entityMap.get(indexShard), loader, readerMap.get(indexShard), termBytes);
                        System.out.println("value size = " +  value.length());
//                        synchronized (valueMap) {
//                            List<BytesReference> bytesReferenceList = valueMap.computeIfAbsent(termBytes,
//                                k -> new ArrayList<>());
//                            bytesReferenceList.add(value);
//                        }
//                        OpenSearchDirectoryReader.DelegatingCacheHelper delegatingCacheHelper =
//                            (OpenSearchDirectoryReader.DelegatingCacheHelper) readerMap.get(indexShard)
//                                .getReaderCacheHelper();
//                        String readerCacheKeyId = delegatingCacheHelper.getDelegatingCacheKey().getId();
//                        removalNotifications.add(new RemovalNotification<>(new IndicesRequestCache.Key(indexShard.shardId(), termBytes, readerCacheKeyId), value,
//                            RemovalReason.EVICTED));
                    } catch (Exception e) {
                        System.out.println("exception2 " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    latch.countDown();
                } catch (Exception ex) {
                    System.out.println("exceptionssssssssss " + ex.getMessage() + " exception = " + ex);

                }
            });
        }
        System.out.println("Reached here!!!");
        latch.await();

        for (int i = 0; i < indicesList.size(); i++) {
            IndexShard indexShard = indexShardList.get(i);
            System.out.println("memory size for index " +  indexShard.shardId().getIndex().getName() + " size: " + indexShard.requestCache().stats().getMemorySizeInBytes());
        }
        getNodeCacheStats(client());
//        assertEquals(numberOfItems, cache.count());
//        System.out.println("Now invalidating !!!");
//
//        cache.invalidateAll();
//        System.out.println("Invalidation done !!!");
//        for (int i = 0; i < indicesList.size(); i++) {
//            IndexShard indexShard = indexShardList.get(i);
//            System.out.println("memory size for index " +  indexShard.shardId().getIndex().getName() + " size: " + indexShard.requestCache().stats().getMemorySizeInBytes());
//        }
//
//        getNodeCacheStats(client());
        for (int i = 0; i < numberOfIndices; i++) {
            IndexShard indexShard = indexShardList.get(i);
            readerMap.get(indexShard).close();
            writerMap.get(indexShard).close();
            writerMap.get(indexShard).getDirectory().close();
        }
    }

    public void testComputeIfAbsentConcurrentlyAndDeleteIndicesParallely() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        int numberOfIndices = 100;
        List<String> indicesList = new ArrayList<>();
        List<IndexShard> indexShardList = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = "test" + i;
            indicesList.add(indexName);
            IndexShard indexShard = createIndex(indexName, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()).getShard(0);
            indexShardList.add(indexShard);
        }
        ThreadPool threadPool = getThreadPool();
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, (shardId -> {
            IndexService indexService = indicesService.indices.get(shardId.getIndex().getUUID());
            if (indexService == null) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(), threadPool);
        Map<IndexShard, DirectoryReader> readerMap = new ConcurrentHashMap<>();
        Map<IndexShard, IndicesService.IndexShardCacheEntity> entityMap = new ConcurrentHashMap<>();
        Map<IndexShard, IndexWriter> writerMap = new ConcurrentHashMap<>();
        int numberOfItems = randomIntBetween(1000, 4000);
        Map<String, Boolean> booleanMap = new HashMap<>();
        List<Directory> directories = new ArrayList<>();
        for (int i = 0; i < numberOfIndices; i++) {
            IndexShard  indexShard = indexShardList.get(i);
            entityMap.put(indexShard, new IndicesService.IndexShardCacheEntity(indexShard));
            Directory dir = newDirectory();
            directories.add(dir);
            IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
            for (int j = 0; j < numberOfItems; j++) {
                writer.addDocument(newDoc(j, generateString(randomIntBetween(4, 300))));
            }
            writerMap.put(indexShard, writer);
            DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer),
                indexShard.shardId());
            readerMap.put(indexShard, reader);
            booleanMap.put(indexShard.shardId().toString(), false);
        }
        System.out.println("reader map size = " + readerMap.size());

        ExecutorService executorService = Executors.newFixedThreadPool(15);


        CountDownLatch latch = new CountDownLatch(numberOfItems);
        Map<IndexShard, List<BytesReference>> termQueryMap = new ConcurrentHashMap<>();
        Map<BytesReference, List<BytesReference>> valueMap = new ConcurrentHashMap<>();
        CounterMetric metric = new CounterMetric();
        List<RemovalNotification<IndicesRequestCache.Key, BytesReference>> removalNotifications =
            Collections.synchronizedList(new ArrayList<>());
        for (int  i = 0; i < numberOfItems; i++) {
            int finalI = i;
            executorService.submit(() -> {
                try {
                    metric.inc(1);
                    int randomIndexPosition = randomIntBetween(0, numberOfIndices - 1);
                    IndexShard indexShard = indexShardList.get(randomIndexPosition);
                    TermQueryBuilder termQuery = new TermQueryBuilder("id", generateString(randomIntBetween(4, 300)));
                    BytesReference termBytes = null;
                    try {
                        termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
                        //metric.inc(termBytes.length());
                        //System.out.println("size = " +  termBytes.length());
                    } catch (IOException e) {
                        System.out.println("exception1 " + e.getMessage());
                        throw new RuntimeException(e);
                    }
//                    synchronized (termQueryMap) {
//                        List<BytesReference> bytesReferenceList = termQueryMap.computeIfAbsent(indexShard, k -> new ArrayList<>());
//                        bytesReferenceList.add(termBytes);
//                        //termQueryMap.computeIfAbsent(indexShard, k -> new ArrayList<>()).add(termBytes);
//                    }

                    Loader loader = new Loader(readerMap.get(indexShard), finalI);
                    try {
                        //phaser.arriveAndAwaitAdvance();
                        BytesReference value = cache.getOrCompute(entityMap.get(indexShard), loader, readerMap.get(indexShard), termBytes);
//                        synchronized (valueMap) {
//                            List<BytesReference> bytesReferenceList = valueMap.computeIfAbsent(termBytes,
//                                k -> new ArrayList<>());
//                            bytesReferenceList.add(value);
//                        }
//                        OpenSearchDirectoryReader.DelegatingCacheHelper delegatingCacheHelper =
//                            (OpenSearchDirectoryReader.DelegatingCacheHelper) readerMap.get(indexShard)
//                                .getReaderCacheHelper();
//                        String readerCacheKeyId = delegatingCacheHelper.getDelegatingCacheKey().getId();
//                        removalNotifications.add(new RemovalNotification<>(new IndicesRequestCache.Key(indexShard.shardId(), termBytes, readerCacheKeyId), value,
//                            RemovalReason.EVICTED));
                    } catch (Exception e) {
                        System.out.println("exception2 " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    latch.countDown();
                } catch (Exception ex) {
                    System.out.println("exceptionssssssssss " + ex.getMessage() + " exception = " + ex);

                }
            });
        }
        System.out.println("Reached here!!!");
        latch.await();

        for (int i = 0; i < indicesList.size(); i++) {
            IndexShard indexShard = indexShardList.get(i);
            System.out.println("memory size for index " +  indexShard.shardId().getIndex().getName() + " size: " +
            indexShard.requestCache().stats().getMemorySizeInBytes());
        }
        //assertEquals(numberOfItems, cache.count());
        System.out.println("Now deleting all indices parallelly !!!");

        CountDownLatch latch1 = new CountDownLatch(readerMap.size());

        for (Map.Entry<IndexShard, DirectoryReader> entry: readerMap.entrySet()) {
            executorService.submit(() -> {
                try {
                    entry.getValue().close();

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                latch1.countDown();
            });
        }



//        for (int i = 0; i< numberOfIndices; i ++) {
//            int finalI = i;
//            executorService.submit(() -> {
//                IndexShard indexShard = indexShardList.get(finalI);
//                try {
//                    readerMap.get(indexShard).close();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            });
//            latch1.countDown();
//        }
        latch1.await();
        System.out.println("Deleting done and sleeping for 10 seconds!!!");
        Thread.sleep(20000);
//        assertBusy(() -> {
//            assertEquals(numberOfIndices, cache.cacheCleanupManager.keysToClean.size());
//        }, 20, TimeUnit.SECONDS);
//        cache.cacheCleanupManager.cleanCache();

        System.out.println("keysToClean = " +  cache.cacheCleanupManager.keysToClean.size());
        for (Iterator<IndicesRequestCache.CleanupKey> iterator = cache.cacheCleanupManager.keysToClean.iterator(); iterator.hasNext();) {
            IndicesRequestCache.CleanupKey cleanupKey = iterator.next();
            ShardId shardId = ((IndexShard) cleanupKey.entity.getCacheIdentity()).shardId();
            String readerCacheKeyId = cleanupKey.readerCacheKeyId;
            booleanMap.put(shardId.toString(), true);
            //System.out.println("shardId = " + shardId.toString() + " readerCacheKeyId = " + readerCacheKeyId);
            // gdujddtlrkcbete

        }
        int missCount = 0;
        for (Map.Entry<String, Boolean> entry: booleanMap.entrySet()) {
            if (!entry.getValue()) {
                missCount++;
                System.out.println("Doesn't contains this!!! == " + entry.getKey());
                System.out.println("Was it even tried closing = " + cache.listOfShardIdClosed.contains(entry.getKey()));
            }
        }
        System.out.println("miss count = " + missCount);
        //Thread.sleep(10000);

//        for (int i = 0; i < indicesList.size(); i++) {
//            IndexShard indexShard = indexShardList.get(i);
//            System.out.println("memory size for index " +  indexShard.shardId().getIndex().getName() + " size: " + indexShard.requestCache().stats().getMemorySizeInBytes());
//        }
        //Thread.sleep(10000);
        getNodeCacheStats(client());
        for (int i = 0; i < numberOfIndices; i++) {
            IndexShard indexShard = indexShardList.get(i);
            readerMap.get(indexShard).close();
            writerMap.get(indexShard).close();
            writerMap.get(indexShard).getDirectory().close();
        }
    }


    // Keep writing to multiple indices on a separate thread.
    // Keep searching for items for multiple indices on a separate thread.
    // Keep refresh interval low.
    // Keep cache clean interval low.
    public void testWriteSearchParallely() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        int numberOfIndices = 50;
        List<String> indicesList = new ArrayList<>();
        List<IndexShard> indexShardList = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = "test" + i;
            indicesList.add(indexName);
            IndexShard indexShard = createIndex(indexName, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put("index.refresh_interval", "80ms")
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()).getShard(0);
            indexShardList.add(indexShard);
        }
        ThreadPool threadPool = getThreadPool();
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, (shardId -> {
            IndexService indexService = indicesService.indices.get(shardId.getIndex().getUUID());
            if (indexService == null) {
                return Optional.empty();
            }
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(), threadPool);
        Map<IndexShard, DirectoryReader> readerMap = new ConcurrentHashMap<>();
        Map<IndexShard, IndicesService.IndexShardCacheEntity> entityMap = new ConcurrentHashMap<>();
        Map<IndexShard, IndexWriter> writerMap = new ConcurrentHashMap<>();
        int numberOfItems = randomIntBetween(1000, 4000);
        Map<String, Boolean> booleanMap = new HashMap<>();
        List<Directory> directories = new ArrayList<>();
        for (int i = 0; i < numberOfIndices; i++) {
            IndexShard  indexShard = indexShardList.get(i);
            entityMap.put(indexShard, new IndicesService.IndexShardCacheEntity(indexShard));
            Directory dir = newDirectory();
            directories.add(dir);
            IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
            for (int j = 0; j < numberOfItems; j++) {
                writer.addDocument(newDoc(j, generateString(randomIntBetween(4, 300))));
            }
            writerMap.put(indexShard, writer);
            DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer),
                indexShard.shardId());
            readerMap.put(indexShard, reader);
            booleanMap.put(indexShard.shardId().toString(), false);
        }

        ExecutorService searchExecutorServicec = Executors.newFixedThreadPool(3);

        //CountDownLatch latch = new CountDownLatch(numberOfItems);
        Map<IndexShard, List<BytesReference>> termQueryMap = new ConcurrentHashMap<>();
        Map<BytesReference, List<BytesReference>> valueMap = new ConcurrentHashMap<>();
        CounterMetric metric = new CounterMetric();
        List<RemovalNotification<IndicesRequestCache.Key, BytesReference>> removalNotifications =
            Collections.synchronizedList(new ArrayList<>());

        for (int  i = 0; i < numberOfItems; i++) {
            int finalI = i;
            searchExecutorServicec.submit(() -> {
                try {
                    while (true) {
                        metric.inc(1);
                        int randomIndexPosition = randomIntBetween(0, numberOfIndices - 1);
                        IndexShard indexShard = indexShardList.get(randomIndexPosition);
                        TermQueryBuilder termQuery = new TermQueryBuilder("id", generateString(randomIntBetween(4, 300)));
                        BytesReference termBytes = null;
                        try {
                            termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
                            //metric.inc(termBytes.length());
                            //System.out.println("size = " +  termBytes.length());
                        } catch (IOException e) {
                            System.out.println("exception1 " + e.getMessage());
                            throw new RuntimeException(e);
                        }
//                    synchronized (termQueryMap) {
//                        List<BytesReference> bytesReferenceList = termQueryMap.computeIfAbsent(indexShard, k -> new ArrayList<>());
//                        bytesReferenceList.add(termBytes);
//                        //termQueryMap.computeIfAbsent(indexShard, k -> new ArrayList<>()).add(termBytes);
//                    }
                        // if (readerMap.get(indexShard).)
                        Loader loader = new Loader(readerMap.get(indexShard), finalI);
                        try {
                            //phaser.arriveAndAwaitAdvance();
                            BytesReference value = cache.getOrCompute(entityMap.get(indexShard), loader, readerMap.get(indexShard), termBytes);
//                        synchronized (valueMap) {
//                            List<BytesReference> bytesReferenceList = valueMap.computeIfAbsent(termBytes,
//                                k -> new ArrayList<>());
//                            bytesReferenceList.add(value);
//                        }
//                        OpenSearchDirectoryReader.DelegatingCacheHelper delegatingCacheHelper =
//                            (OpenSearchDirectoryReader.DelegatingCacheHelper) readerMap.get(indexShard)
//                                .getReaderCacheHelper();
//                        String readerCacheKeyId = delegatingCacheHelper.getDelegatingCacheKey().getId();
//                        removalNotifications.add(new RemovalNotification<>(new IndicesRequestCache.Key(indexShard.shardId(), termBytes, readerCacheKeyId), value,
//                            RemovalReason.EVICTED));
                        } catch (Exception e) {
                            System.out.println("exception2 " + e.getMessage());
                            throw new RuntimeException(e);
                        }
                    }
                    //latch.countDown();
                } catch (Exception ex) {
                    System.out.println("exceptionssssssssss " + ex.getMessage() + " exception = " + ex);

                }
            });
        }

        ExecutorService writerService = Executors.newFixedThreadPool(2);
        for (int i = 0; i < numberOfIndices; i++) {
            int finalI = i;
            writerService.submit(() -> {
                while (true) {
                    IndexShard indexShard = indexShardList.get(finalI);
                    IndexWriter writer = writerMap.get(indexShard);
                    for (int j = 0; j < numberOfItems; j++) {
                        try {
                            writer.updateDocument(new Term("id", "0"), newDoc(finalI,
                                generateString(randomIntBetween(4, 300))));
                            synchronized (readerMap) {
                                DirectoryReader oldReader = readerMap.get(indexShard);
                                DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer),
                                    indexShard.shardId());
                                readerMap.put(indexShard, reader);
                                oldReader.close();
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    Thread.sleep(10);
                }
            });
        }
        long checkInIntervalSeconds = 10;
        ExecutorService statsService = Executors.newFixedThreadPool(1);
        while (true) {
            getNodeCacheStats(client());
            Thread.sleep(checkInIntervalSeconds * 1000);
        }
    }

    public void testWithAllocatingAndDeletingSameIndexNameOnSameNode() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        String index1 = "test1";
        IndexShard indexShard = createIndex(index1, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()).getShard(0);
        ThreadPool threadPool = getThreadPool();
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, (shardId -> {
            IndexService indexService = indicesService.indices.get(shardId.getIndex().getUUID());
            if (indexService == null) {
                return Optional.empty();
            }
            IndexShard indexShard1 = mock(IndexShard.class);
            when(indexShard1.requestCache()).thenReturn(new ShardRequestCache());
            return Optional.of(new IndicesService.IndexShardCacheEntity(indexShard1));
        }), new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(), threadPool);

        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), indexShard.shardId());
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);

        // initial cache
        IndicesService.IndexShardCacheEntity entity = new IndicesService.IndexShardCacheEntity(indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        RequestCacheStats requestCacheStats =
            client().admin().cluster().prepareNodesStats().execute().actionGet().getNodes().get(0).getIndices().getRequestCache();
        long memorySizeInBytes = requestCacheStats.getMemorySizeInBytes();
        reader.close();
        System.out.println("memory size in bytes = " + requestCacheStats.getMemorySizeInBytes() + " miss = " + requestCacheStats.getMissCount()
            + "hit == " + requestCacheStats.getHitCount() + " eviction = " + requestCacheStats.getEvictions());
        indicesService.indexService(indexShard.indexSettings().getIndex()).removeShard(indexShard.shardId().id(), "force");
        //assertAcked(client().admin().indices().prepareDelete(index1).get());
        requestCacheStats =
            client().admin().cluster().prepareNodesStats().execute().actionGet().getNodes().get(0).getIndices().getRequestCache();
        //assertEquals(memorySizeInBytes, requestCacheStats.getMemorySizeInBytes());
        //IOUtils.close(reader, writer, dir);
//        assertBusy(() -> {
//            assertTrue(cache.cacheCleanupManager.keysToClean.size() > 0);
//        }, 10, TimeUnit.SECONDS);
//        indexShard = createIndex(index1, Settings.builder()
//            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
//            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()).getShard(0);
//        threadPool = getThreadPool();
//        dir = newDirectory();
//        writer = new IndexWriter(dir, newIndexWriterConfig());
//
//        writer.addDocument(newDoc(0, "foo"));
//        reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), indexShard.shardId());
//        termQuery = new TermQueryBuilder("id", "0");
//        termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);


        // initial cache
        entity = new IndicesService.IndexShardCacheEntity(indexShard);
        loader = new Loader(reader, 0);

        //value = cache.getOrCompute(entity, loader, reader, termBytes);
//        requestCacheStats =
//            client().admin().cluster().prepareNodesStats().execute().actionGet().getNodes().get(0).getIndices().getRequestCache();
//        System.out.println("memory size in bytes = " + requestCacheStats.getMemorySizeInBytes() + " miss = " + requestCacheStats.getMissCount()
//            + "hit == " + requestCacheStats.getHitCount() + " eviction = " + requestCacheStats.getEvictions());
        cache.cacheCleanupManager.cleanCache();
        IOUtils.close(reader, writer, dir, cache);
    }


    private static void getNodeCacheStats(Client client) {
        NodesStatsResponse stats = client.admin().cluster().prepareNodesStats().execute().actionGet();
        for (NodeStats stat : stats.getNodes()) {
            if (stat.getNode().isDataNode()) {
                RequestCacheStats stats1 = stat.getIndices().getRequestCache();
                System.out.println("total hits = " + stats1.getHitCount() + " miss = " + stats1.getMissCount()
                    +  " memory = " + stats1.getMemorySizeInBytes() +  " evictions " + stats1.getEvictions());
            }
        }
    }

    public static String generateString(int length) {
        String characters = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder(length);
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(characters.length());
            sb.append(characters.charAt(index));
        }
        return sb.toString();
    }


    private class TestBytesReference extends AbstractBytesReference {

        int dummyValue;

        TestBytesReference(int dummyValue) {
            this.dummyValue = dummyValue;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof TestBytesReference && this.dummyValue == ((TestBytesReference) other).dummyValue;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + dummyValue;
            return result;
        }

        @Override
        public byte get(int index) {
            return 0;
        }

        @Override
        public int length() {
            return 0;
        }

        @Override
        public BytesReference slice(int from, int length) {
            return null;
        }

        @Override
        public BytesRef toBytesRef() {
            return null;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public boolean isFragment() {
            return false;
        }
    }
}
