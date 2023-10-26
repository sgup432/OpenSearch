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
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.AbstractBytesReference;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.index.IndexService;
import org.opensearch.index.cache.request.ShardRequestCache;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndicesRequestCacheTests extends OpenSearchSingleNodeTestCase {

    public void testBasicOperationsCache() throws Exception {
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, getInstanceFromNode(IndicesService.class));
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        AtomicBoolean indexShard = new AtomicBoolean(true);

        // initial cache
        TestEntity entity = new TestEntity(requestCacheStats, indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());

        // cache hit
        entity = new TestEntity(requestCacheStats, indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
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
            indexShard.set(false); // closed shard but reader is still open
            cache.clear(entity);
        }
        cache.cleanCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(reader, writer, dir, cache);
        assertEquals(0, cache.numRegisteredCloseListeners());
        cache.closeDiskTier();
    }

    public void testAddDirectToEhcache() throws Exception {
        // this test is for debugging serialization issues and can eventually be removed
        // Put breakpoints at line 260 of AbstractOffHeapStore to catch serialization errors
        // that would otherwise fail silently
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        Settings.Builder settingsBuilder = Settings.builder();
        long heapSizeBytes = 1000;
        settingsBuilder.put("indices.requests.cache.size", new ByteSizeValue(heapSizeBytes));
        IndicesRequestCache cache = new IndicesRequestCache(settingsBuilder.build(), getInstanceFromNode(IndicesService.class));

        // set up a key
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        AtomicBoolean indexShard = new AtomicBoolean(true);
        TestEntity entity = new TestEntity(requestCacheStats, indexShard);
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        String rKey = ((OpenSearchDirectoryReader) reader).getDelegatingCacheHelper().getDelegatingCacheKey().getId().toString();
        IndicesRequestCache.Key key = cache.new Key(entity, termBytes, rKey);

        BytesReference value = new BytesArray(new byte[]{0});
        cache.tieredCacheHandler.getDiskCachingTier().put(key, value);

        BytesReference res = cache.tieredCacheHandler.getDiskCachingTier().get(key);
        assertEquals(value, res);
        assertEquals(1, cache.tieredCacheHandler.count(TierType.DISK));

        IOUtils.close(reader, writer, dir, cache);
        cache.closeDiskTier();
    }

    public void testSpillover() throws Exception {
        // fill the on-heap cache until we spill over
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        Settings.Builder settingsBuilder = Settings.builder();
        long heapSizeBytes = 1000; // each of these queries is 131 bytes, so we can fit 7 in the heap cache
        int heapKeySize = 131;
        int maxNumInHeap = 1000 / heapKeySize;
        settingsBuilder.put("indices.requests.cache.size", new ByteSizeValue(heapSizeBytes));
        IndicesRequestCache cache = new IndicesRequestCache(settingsBuilder.build(), getInstanceFromNode(IndicesService.class));

        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        AtomicBoolean indexShard = new AtomicBoolean(true);

        TestEntity entity = new TestEntity(requestCacheStats, indexShard);
        Loader loader = new Loader(reader, 0);
        System.out.println("On-heap cache size at start = " + requestCacheStats.stats().getMemorySizeInBytes());
        BytesReference[] termBytesArr = new BytesReference[maxNumInHeap + 1];

        for (int i = 0; i < maxNumInHeap + 1; i++) {
            TermQueryBuilder termQuery = new TermQueryBuilder("id", String.valueOf(i));
            BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
            String rKey = ((OpenSearchDirectoryReader) reader).getDelegatingCacheHelper().getDelegatingCacheKey().getId().toString();
            termBytesArr[i] = termBytes;
            BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        }
        // get value from disk cache, the first key should have been evicted
        BytesReference firstValue = cache.getOrCompute(entity, loader, reader, termBytesArr[0]);

        assertEquals(maxNumInHeap * heapKeySize, requestCacheStats.stats().getMemorySizeInBytes());
        // TODO: disk weight bytes
        assertEquals(1, requestCacheStats.stats().getEvictions());
        assertEquals(1, requestCacheStats.stats(TierType.DISK).getHitCount());
        assertEquals(maxNumInHeap + 1, requestCacheStats.stats(TierType.DISK).getMissCount());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(maxNumInHeap + 2, requestCacheStats.stats().getMissCount());
        assertEquals(maxNumInHeap, cache.tieredCacheHandler.count(TierType.ON_HEAP));
        assertEquals(1, cache.tieredCacheHandler.count(TierType.DISK));

        // get a value from heap cache, second key should still be there
        BytesReference secondValue = cache.getOrCompute(entity, loader, reader, termBytesArr[1]);
        // get the value on disk cache again
        BytesReference firstValueAgain = cache.getOrCompute(entity, loader, reader, termBytesArr[0]);

        assertEquals(1, requestCacheStats.stats().getEvictions());
        assertEquals(2, requestCacheStats.stats(TierType.DISK).getHitCount());
        assertEquals(maxNumInHeap + 1, requestCacheStats.stats(TierType.DISK).getMissCount());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(maxNumInHeap + 3, requestCacheStats.stats().getMissCount());
        assertEquals(maxNumInHeap, cache.tieredCacheHandler.count(TierType.ON_HEAP));
        assertEquals(1, cache.tieredCacheHandler.count(TierType.DISK));

        IOUtils.close(reader, writer, dir, cache);
        cache.closeDiskTier();
    }

    public void testDiskGetTimeEWMA() throws Exception {
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        Settings.Builder settingsBuilder = Settings.builder();
        long heapSizeBytes = 0; // skip directly to disk cache
        settingsBuilder.put("indices.requests.cache.size", new ByteSizeValue(heapSizeBytes));
        IndicesRequestCache cache = new IndicesRequestCache(settingsBuilder.build(), getInstanceFromNode(IndicesService.class));

        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        AtomicBoolean indexShard = new AtomicBoolean(true);

        TestEntity entity = new TestEntity(requestCacheStats, indexShard);
        Loader loader = new Loader(reader, 0);

        for (int i = 0; i < 50; i++) {
            TermQueryBuilder termQuery = new TermQueryBuilder("id", String.valueOf(i));
            BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
            BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
            // on my machine get time EWMA converges to ~0.025 ms, but it does have an SSD
            //assertTrue(cache.tieredCacheHandler.diskGetTimeMillisEWMA() > 0);
        }

        IOUtils.close(reader, writer, dir, cache);
        cache.closeDiskTier();
    }

    public void testCacheDifferentReaders() throws Exception {
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, getInstanceFromNode(IndicesService.class));
        AtomicBoolean indexShard = new AtomicBoolean(true);
        ShardRequestCache requestCacheStats = new ShardRequestCache();
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
        TestEntity entity = new TestEntity(requestCacheStats, indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
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
        TestEntity secondEntity = new TestEntity(requestCacheStats, indexShard);
        loader = new Loader(secondReader, 0);
        value = cache.getOrCompute(entity, loader, secondReader, termBytes);
        assertEquals("bar", value.streamInput().readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(2, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > cacheSize + value.length());
        assertEquals(2, cache.numRegisteredCloseListeners());

        secondEntity = new TestEntity(requestCacheStats, indexShard);
        loader = new Loader(secondReader, 0);
        value = cache.getOrCompute(secondEntity, loader, secondReader, termBytes);
        assertEquals("bar", value.streamInput().readString());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(2, cache.count());

        entity = new TestEntity(requestCacheStats, indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        assertEquals(2, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(2, cache.count());

        // Closing the cache doesn't change returned entities
        reader.close();
        cache.cleanCache();
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
            indexShard.set(false); // closed shard but reader is still open
            cache.clear(secondEntity);
        }
        cache.cleanCache();
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(secondReader, writer, dir, cache);
        assertEquals(0, cache.numRegisteredCloseListeners());
        cache.closeDiskTier();
    }

    public void testEviction() throws Exception {
        final ByteSizeValue size;
        {
            IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, getInstanceFromNode(IndicesService.class));
            AtomicBoolean indexShard = new AtomicBoolean(true);
            ShardRequestCache requestCacheStats = new ShardRequestCache();
            Directory dir = newDirectory();
            IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

            writer.addDocument(newDoc(0, "foo"));
            DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
            TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
            BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
            TestEntity entity = new TestEntity(requestCacheStats, indexShard);
            Loader loader = new Loader(reader, 0);

            writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
            DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
            TestEntity secondEntity = new TestEntity(requestCacheStats, indexShard);
            Loader secondLoader = new Loader(secondReader, 0);

            BytesReference value1 = cache.getOrCompute(entity, loader, reader, termBytes);
            assertEquals("foo", value1.streamInput().readString());
            BytesReference value2 = cache.getOrCompute(secondEntity, secondLoader, secondReader, termBytes);
            assertEquals("bar", value2.streamInput().readString());
            size = requestCacheStats.stats().getMemorySize();
            IOUtils.close(reader, secondReader, writer, dir, cache);
            cache.closeDiskTier();
        }
        IndicesRequestCache cache = new IndicesRequestCache(
            Settings.builder().put(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.getKey(), size.getBytes() + 1 + "b").build(),
            null
        );
        AtomicBoolean indexShard = new AtomicBoolean(true);
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        TestEntity entity = new TestEntity(requestCacheStats, indexShard);
        Loader loader = new Loader(reader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TestEntity secondEntity = new TestEntity(requestCacheStats, indexShard);
        Loader secondLoader = new Loader(secondReader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "baz"));
        DirectoryReader thirdReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TestEntity thirddEntity = new TestEntity(requestCacheStats, indexShard);
        Loader thirdLoader = new Loader(thirdReader, 0);

        BytesReference value1 = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value1.streamInput().readString());
        BytesReference value2 = cache.getOrCompute(secondEntity, secondLoader, secondReader, termBytes);
        assertEquals("bar", value2.streamInput().readString());
        logger.info("Memory size: {}", requestCacheStats.stats().getMemorySize());
        BytesReference value3 = cache.getOrCompute(thirddEntity, thirdLoader, thirdReader, termBytes);
        assertEquals("baz", value3.streamInput().readString());
        assertEquals(2, cache.count());
        assertEquals(1, requestCacheStats.stats().getEvictions());
        IOUtils.close(reader, secondReader, thirdReader, writer, dir, cache);
        cache.closeDiskTier();
    }

    public void testClearAllEntityIdentity() throws Exception {
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, getInstanceFromNode(IndicesService.class));
        AtomicBoolean indexShard = new AtomicBoolean(true);

        ShardRequestCache requestCacheStats = new ShardRequestCache();
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        TestEntity entity = new TestEntity(requestCacheStats, indexShard);
        Loader loader = new Loader(reader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TestEntity secondEntity = new TestEntity(requestCacheStats, indexShard);
        Loader secondLoader = new Loader(secondReader, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "baz"));
        DirectoryReader thirdReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        AtomicBoolean differentIdentity = new AtomicBoolean(true);
        TestEntity thirddEntity = new TestEntity(requestCacheStats, differentIdentity);
        Loader thirdLoader = new Loader(thirdReader, 0);

        BytesReference value1 = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value1.streamInput().readString());
        BytesReference value2 = cache.getOrCompute(secondEntity, secondLoader, secondReader, termBytes);
        assertEquals("bar", value2.streamInput().readString());
        logger.info("Memory size: {}", requestCacheStats.stats().getMemorySize());
        BytesReference value3 = cache.getOrCompute(thirddEntity, thirdLoader, thirdReader, termBytes);
        assertEquals("baz", value3.streamInput().readString());
        assertEquals(3, cache.count());
        final long hitCount = requestCacheStats.stats().getHitCount();
        // clear all for the indexShard Idendity even though is't still open
        cache.clear(randomFrom(entity, secondEntity));
        cache.cleanCache();
        assertEquals(1, cache.count());
        // third has not been validated since it's a different identity
        value3 = cache.getOrCompute(thirddEntity, thirdLoader, thirdReader, termBytes);
        assertEquals(hitCount + 1, requestCacheStats.stats().getHitCount());
        assertEquals("baz", value3.streamInput().readString());

        IOUtils.close(reader, secondReader, thirdReader, writer, dir, cache);
        cache.closeDiskTier();

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
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY, getInstanceFromNode(IndicesService.class));
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        BytesReference termBytes = XContentHelper.toXContent(termQuery, MediaTypeRegistry.JSON, false);
        AtomicBoolean indexShard = new AtomicBoolean(true);

        // initial cache
        TestEntity entity = new TestEntity(requestCacheStats, indexShard);
        Loader loader = new Loader(reader, 0);
        BytesReference value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(loader.loadedFromCache);
        assertEquals(1, cache.count());

        // cache hit
        entity = new TestEntity(requestCacheStats, indexShard);
        loader = new Loader(reader, 0);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(loader.loadedFromCache);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // load again after invalidate
        entity = new TestEntity(requestCacheStats, indexShard);
        loader = new Loader(reader, 0);
        cache.invalidate(entity, reader, termBytes);
        value = cache.getOrCompute(entity, loader, reader, termBytes);
        assertEquals("foo", value.streamInput().readString());
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
            indexShard.set(false); // closed shard but reader is still open
            cache.clear(entity);
        }
        cache.cleanCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(reader, writer, dir, cache);
        assertEquals(0, cache.numRegisteredCloseListeners());
        cache.closeDiskTier();
    }

    public void testEqualsKey() throws IOException {
        AtomicBoolean trueBoolean = new AtomicBoolean(true);
        AtomicBoolean falseBoolean = new AtomicBoolean(false);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndicesRequestCache indicesRequestCache = indicesService.indicesRequestCache;
        Directory dir = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, config);
        ShardId shardId = new ShardId("foo", "bar", 1);
        IndexReader reader1 = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), shardId);
        String rKey1 = ((OpenSearchDirectoryReader) reader1).getDelegatingCacheHelper().getDelegatingCacheKey().getId().toString();
        writer.addDocument(new Document());
        IndexReader reader2 = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), shardId);
        String rKey2 = ((OpenSearchDirectoryReader) reader2).getDelegatingCacheHelper().getDelegatingCacheKey().getId().toString();
        IOUtils.close(reader1, reader2, writer, dir);
        IndicesRequestCache.Key key1 = indicesRequestCache.new Key(new TestEntity(null, trueBoolean), new TestBytesReference(1), rKey1);
        IndicesRequestCache.Key key2 = indicesRequestCache.new Key(new TestEntity(null, trueBoolean), new TestBytesReference(1), rKey1);
        IndicesRequestCache.Key key3 = indicesRequestCache.new Key(new TestEntity(null, falseBoolean), new TestBytesReference(1), rKey1);
        IndicesRequestCache.Key key4 = indicesRequestCache.new Key(new TestEntity(null, trueBoolean), new TestBytesReference(1), rKey2);
        IndicesRequestCache.Key key5 = indicesRequestCache.new Key(new TestEntity(null, trueBoolean), new TestBytesReference(2), rKey2);
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
        ShardRequestCache shardRequestCache = new ShardRequestCache();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndicesRequestCache indicesRequestCache = indicesService.indicesRequestCache;
        IndexService indexService = createIndex("test");
        IndexShard indexShard = indexService.getShard(0);
        IndicesService.IndexShardCacheEntity shardCacheEntity = indicesService.new IndexShardCacheEntity(indexShard);
        String readerCacheKeyId = UUID.randomUUID().toString();
        IndicesRequestCache.Key key1 = indicesRequestCache.new Key(shardCacheEntity, termBytes, readerCacheKeyId);
        BytesReference bytesReference = null;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            key1.writeTo(out);
            bytesReference = out.bytes();
        }
        StreamInput in = bytesReference.streamInput();

        IndicesRequestCache.Key key2 = indicesRequestCache.new Key(in);

        assertEquals(readerCacheKeyId, key2.readerCacheKeyUniqueId);
        assertEquals(shardCacheEntity.getCacheIdentity(), key2.entity.getCacheIdentity());
        assertEquals(termBytes, key2.value);

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

    private class TestEntity extends AbstractIndexShardCacheEntity implements Serializable {
        private final AtomicBoolean standInForIndexShard;
        private final ShardRequestCache shardRequestCache;

        private TestEntity(ShardRequestCache shardRequestCache, AtomicBoolean standInForIndexShard) {
            this.standInForIndexShard = standInForIndexShard;
            this.shardRequestCache = shardRequestCache;
        }

        @Override
        protected ShardRequestCache stats() {
            return shardRequestCache;
        }

        @Override
        public boolean isOpen() {
            return standInForIndexShard.get();
        }

        @Override
        public Object getCacheIdentity() {
            return standInForIndexShard;
        }

        @Override
        public long ramBytesUsed() {
            return 42;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
