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
import org.apache.lucene.util.BytesRef;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.module.CacheModule;
import org.opensearch.common.cache.service.CacheService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.AbstractBytesReference;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.unit.ByteSizeValue;
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
import org.opensearch.node.Node;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.indices.IndicesRequestCache.INDICES_CACHE_QUERY_SIZE;
import static org.opensearch.indices.IndicesRequestCache.INDICES_REQUEST_CACHE_STALENESS_THRESHOLD_SETTING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
            threadPool,
            ClusterServiceUtils.createClusterService(threadPool)
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
            threadPool,
            ClusterServiceUtils.createClusterService(threadPool)
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
        }),
            new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(),
            threadPool,
            ClusterServiceUtils.createClusterService(threadPool)
        );
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
        }),
            new CacheModule(new ArrayList<>(), settings).getCacheService(),
            threadPool,
            ClusterServiceUtils.createClusterService(threadPool)
        );
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
        }),
            new CacheModule(new ArrayList<>(), settings).getCacheService(),
            threadPool,
            ClusterServiceUtils.createClusterService(threadPool)
        );
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
        IndexShard indexShard = createIndex("test").getShard(0);
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
        }),
            new CacheModule(new ArrayList<>(), settings).getCacheService(),
            threadPool,
            ClusterServiceUtils.createClusterService(threadPool)
        );
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
        reader.close();
        AtomicInteger staleKeysCount = cache.cacheCleanupManager.getStaleKeysCount();
        // 1 out of 2 keys ie 50% are now stale.
        assertEquals(1, staleKeysCount.get());
        // cache count should not be affected
        assertEquals(2, cache.count());

        OpenSearchDirectoryReader.DelegatingCacheHelper delegatingCacheHelper =
            (OpenSearchDirectoryReader.DelegatingCacheHelper) secondReader.getReaderCacheHelper();
        String readerCacheKeyId = delegatingCacheHelper.getDelegatingCacheKey().getId();
        IndicesRequestCache.Key key = new IndicesRequestCache.Key(indexShard.shardId(), termBytes, readerCacheKeyId, indexShard.hashCode());

        cache.onRemoval(new RemovalNotification<IndicesRequestCache.Key, BytesReference>(key, termBytes, RemovalReason.EVICTED));
        staleKeysCount = cache.cacheCleanupManager.getStaleKeysCount();
        // eviction of previous stale key from the cache should decrement staleKeysCount in iRC
        assertEquals(0, staleKeysCount.get());

        IOUtils.close(secondReader, writer, dir, cache);
        terminate(threadPool);
    }

    public void testStaleCount_OnRemovalNotificationOfStaleKey_DoesNotDecrementsStaleCount() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard indexShard = createIndex("test").getShard(0);
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
        }),
            new CacheModule(new ArrayList<>(), settings).getCacheService(),
            threadPool,
            ClusterServiceUtils.createClusterService(threadPool)
        );
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
        reader.close();
        AtomicInteger staleKeysCount = cache.cacheCleanupManager.getStaleKeysCount();
        // 1 out of 2 keys ie 50% are now stale.
        assertEquals(1, staleKeysCount.get());
        // cache count should not be affected
        assertEquals(2, cache.count());

        OpenSearchDirectoryReader.DelegatingCacheHelper delegatingCacheHelper = (OpenSearchDirectoryReader.DelegatingCacheHelper) reader
            .getReaderCacheHelper();
        String readerCacheKeyId = delegatingCacheHelper.getDelegatingCacheKey().getId();
        IndicesRequestCache.Key key = new IndicesRequestCache.Key(indexShard.shardId(), termBytes, readerCacheKeyId, indexShard.hashCode());

        cache.onRemoval(new RemovalNotification<IndicesRequestCache.Key, BytesReference>(key, termBytes, RemovalReason.EVICTED));
        staleKeysCount = cache.cacheCleanupManager.getStaleKeysCount();
        // eviction of NON-stale key from the cache should NOT decrement staleKeysCount in iRC
        assertEquals(1, staleKeysCount.get());

        IOUtils.close(secondReader, writer, dir, cache);
        terminate(threadPool);
    }

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
        }),
            new CacheModule(new ArrayList<>(), settings).getCacheService(),
            threadPool,
            ClusterServiceUtils.createClusterService(threadPool)
        );
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
        }),
            new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(),
            threadPool,
            ClusterServiceUtils.createClusterService(threadPool)
        );
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
                threadPool,
                ClusterServiceUtils.createClusterService(threadPool)
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
            Settings.builder().put(INDICES_CACHE_QUERY_SIZE.getKey(), size.getBytes() + 1 + "b").build(),
            (shardId -> Optional.of(new IndicesService.IndexShardCacheEntity(indexShard))),
            new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(),
            threadPool,
            ClusterServiceUtils.createClusterService(threadPool)
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
        }),
            new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(),
            threadPool,
            ClusterServiceUtils.createClusterService(threadPool)
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
        }),
            new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(),
            threadPool,
            ClusterServiceUtils.createClusterService(threadPool)
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
        IndicesRequestCache.Key key3 = new IndicesRequestCache.Key(shardId1, new TestBytesReference(1), rKey1, shardId1.hashCode());
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
        IndicesRequestCache.Key key1 = new IndicesRequestCache.Key(
            indexShard.shardId(),
            termBytes,
            readerCacheKeyId,
            indexShard.hashCode()
        );
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

    public void testGetOrComputeConcurrentlyWithMultipleIndices() throws Exception {
        ThreadPool threadPool = getThreadPool();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        int numberOfIndices = randomIntBetween(2, 5);
        List<String> indicesList = new ArrayList<>();
        List<IndexShard> indexShardList = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = "test" + i;
            indicesList.add(indexName);
            IndexShard indexShard = createIndex(
                indexName,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            ).getShard(0);
            indexShardList.add(indexShard);
        }
        // Create a cache with 2kb to cause evictions and test that flow as well.
        IndicesRequestCache cache = new IndicesRequestCache(
            Settings.builder().put(INDICES_CACHE_QUERY_SIZE.getKey(), "2kb").build(),
            (shardId -> {
                IndexService indexService = null;
                try {
                    indexService = indicesService.indexServiceSafe(shardId.getIndex());
                } catch (IndexNotFoundException ex) {
                    return Optional.empty();
                }
                return Optional.of(new IndicesService.IndexShardCacheEntity(indexService.getShard(shardId.id())));
            }),
            new CacheModule(new ArrayList<>(), Settings.EMPTY).getCacheService(),
            threadPool,
            ClusterServiceUtils.createClusterService(threadPool)
        );
        Map<IndexShard, DirectoryReader> readerMap = new ConcurrentHashMap<>();
        Map<IndexShard, IndicesService.IndexShardCacheEntity> entityMap = new ConcurrentHashMap<>();
        Map<IndexShard, IndexWriter> writerMap = new ConcurrentHashMap<>();
        int numberOfItems = randomIntBetween(200, 400);
        for (int i = 0; i < numberOfIndices; i++) {
            IndexShard indexShard = indexShardList.get(i);
            entityMap.put(indexShard, new IndicesService.IndexShardCacheEntity(indexShard));
            Directory dir = newDirectory();
            IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
            for (int j = 0; j < numberOfItems; j++) {
                writer.addDocument(newDoc(j, generateString(randomIntBetween(4, 50))));
            }
            writerMap.put(indexShard, writer);
            DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), indexShard.shardId());
            readerMap.put(indexShard, reader);
        }

        CountDownLatch latch = new CountDownLatch(numberOfItems);
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < numberOfItems; i++) {
            int finalI = i;
            executorService.submit(() -> {
                int randomIndexPosition = randomIntBetween(0, numberOfIndices - 1);
                IndexShard indexShard = indexShardList.get(randomIndexPosition);
                TermQueryBuilder termQuery = new TermQueryBuilder("id", generateString(randomIntBetween(4, 50)));
                BytesReference termBytes = null;
                try {
                    termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                Loader loader = new Loader(readerMap.get(indexShard), finalI);
                try {
                    cache.getOrCompute(entityMap.get(indexShard), loader, readerMap.get(indexShard), termBytes);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            });
        }
        latch.await();
        for (int i = 0; i < numberOfIndices; i++) {
            IndexShard indexShard = indexShardList.get(i);
            IndicesService.IndexShardCacheEntity entity = entityMap.get(indexShard);
            RequestCacheStats stats = entity.stats().stats();
            assertTrue(stats.getMemorySizeInBytes() >= 0);
            assertTrue(stats.getMissCount() >= 0);
            assertTrue(stats.getEvictions() >= 0);
        }
        cache.invalidateAll();
        for (int i = 0; i < numberOfIndices; i++) {
            IndexShard indexShard = indexShardList.get(i);
            IndicesService.IndexShardCacheEntity entity = entityMap.get(indexShard);
            RequestCacheStats stats = entity.stats().stats();
            assertEquals(0, stats.getMemorySizeInBytes());
        }

        for (int i = 0; i < numberOfIndices; i++) {
            IndexShard indexShard = indexShardList.get(i);
            readerMap.get(indexShard).close();
            writerMap.get(indexShard).close();
            writerMap.get(indexShard).getDirectory().close();
        }
        IOUtils.close(cache);
        terminate(threadPool);
        executorService.shutdownNow();
    }

    public static String generateString(int length) {
        String characters = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = randomInt(characters.length() - 1);
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
