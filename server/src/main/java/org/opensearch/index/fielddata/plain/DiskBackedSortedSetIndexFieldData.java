/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata.plain;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Nullable;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.IndexOrdinalsFieldData;
import org.opensearch.index.fielddata.LeafOrdinalsFieldData;
import org.opensearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortOrder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Disk-backed field data for text fields with fielddata=true.
 * <p>
 * Instead of loading the uninverted data entirely on heap (like {@link PagedBytesIndexFieldData}),
 * this implementation writes the uninverted terms to a temporary MMap-backed Lucene directory
 * as SortedSetDocValues. The resulting field data is accessed via mmap with zero heap overhead
 * (beyond the MMap page table entries managed by the OS).
 *
 * @opensearch.internal
 */
public class DiskBackedSortedSetIndexFieldData extends AbstractIndexOrdinalsFieldData {

    private final double minFrequency, maxFrequency;
    private final int minSegmentSize;

    /**
     * Builder for disk-backed sorted set index field data.
     *
     * @opensearch.internal
     */
    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final double minFrequency, maxFrequency;
        private final int minSegmentSize;
        private final ValuesSourceType valuesSourceType;

        public Builder(String name, double minFrequency, double maxFrequency, int minSegmentSize, ValuesSourceType valuesSourceType) {
            this.name = name;
            this.minFrequency = minFrequency;
            this.maxFrequency = maxFrequency;
            this.minSegmentSize = minSegmentSize;
            this.valuesSourceType = valuesSourceType;
        }

        @Override
        public IndexOrdinalsFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new DiskBackedSortedSetIndexFieldData(
                name,
                valuesSourceType,
                cache,
                breakerService,
                minFrequency,
                maxFrequency,
                minSegmentSize
            );
        }
    }

    public DiskBackedSortedSetIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        IndexFieldDataCache cache,
        CircuitBreakerService breakerService,
        double minFrequency,
        double maxFrequency,
        int minSegmentSize
    ) {
        super(fieldName, valuesSourceType, cache, breakerService, AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION);
        this.minFrequency = minFrequency;
        this.maxFrequency = maxFrequency;
        this.minSegmentSize = minSegmentSize;
    }

    @Override
    public SortField sortField(
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        XFieldComparatorSource.Nested nested,
        boolean reverse
    ) {
        XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
        return new SortField(getFieldName(), source, reverse);
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new IllegalArgumentException("only supported on numeric fields");
    }

    @Override
    public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
        LeafReader reader = context.reader();
        Terms terms = reader.terms(getFieldName());
        if (terms == null) {
            return AbstractLeafOrdinalsFieldData.empty();
        }

        Path tempDir = Files.createTempDirectory("opensearch-fielddata-" + getFieldName());
        MMapDirectory mmapDir = new MMapDirectory(tempDir);
        int maxDoc = reader.maxDoc();

        // --- Disk-spill uninversion with O(maxDoc + uniqueTerms) heap ---
        //
        // The inverted index is term-major: term -> [doc1, doc2, ...].
        // Doc values need doc-major: doc -> [term1, term2, ...].
        // We transpose with bounded heap using two passes over a spill file.
        //
        // Pass 1: Stream (docId, termOrd) pairs to disk. Track count per doc.
        // Heap: termDict (unique terms) + docTermCount[maxDoc].
        //
        // Pass 2: Use docTermCount to compute offsets, then scatter-read the
        // spill file into a doc-ordered file. Finally stream doc-ordered
        // file sequentially into IndexWriter. Heap: offset[maxDoc] only.

        Path spillFile = tempDir.resolve("uninvert.spill");
        List<BytesRef> termDict = new ArrayList<>();
        int[] docTermCount = new int[maxDoc];

        // Pass 1: Write (docId, termOrd) pairs to spill file
        try (DataOutputStream spillOut = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(spillFile), 1 << 16))) {
            TermsEnum termsEnum = filter(terms, terms.iterator(), reader);
            PostingsEnum docsEnum = null;
            int ord = 0;
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                termDict.add(BytesRef.deepCopyOf(term));
                docsEnum = termsEnum.postings(docsEnum, PostingsEnum.NONE);
                for (int docId = docsEnum.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                    spillOut.writeInt(docId);
                    spillOut.writeInt(ord);
                    docTermCount[docId]++;
                }
                ord++;
            }
        }

        // Pass 2: Reorder spill file into doc-major order using counting sort on disk.
        // Compute prefix-sum offsets: offset[d] = position in sorted file where doc d's entries start.
        long[] offsets = new long[maxDoc + 1];
        for (int d = 0; d < maxDoc; d++) {
            offsets[d + 1] = offsets[d] + (long) docTermCount[d] * 8;
        }

        // Write doc-ordered file by scattering entries from spill file
        Path sortedFile = tempDir.resolve("uninvert.sorted");
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(sortedFile.toFile(), "rw")) {
            // Pre-allocate the sorted file
            raf.setLength(offsets[maxDoc]);

            // Track current write position per doc
            long[] writePos = offsets.clone();

            try (DataInputStream spillIn = new DataInputStream(new BufferedInputStream(Files.newInputStream(spillFile), 1 << 16))) {
                long totalEntries = Files.size(spillFile) / 8;
                for (long i = 0; i < totalEntries; i++) {
                    int docId = spillIn.readInt();
                    int termOrd = spillIn.readInt();
                    raf.seek(writePos[docId]);
                    raf.writeInt(termOrd);
                    writePos[docId] += 4;
                }
            }
        }
        Files.delete(spillFile);
        offsets = null;

        // Pass 3: Stream sorted file sequentially into IndexWriter
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        try (
            IndexWriter writer = new IndexWriter(mmapDir, iwc);
            DataInputStream sortedIn = new DataInputStream(new BufferedInputStream(Files.newInputStream(sortedFile), 1 << 16))
        ) {
            for (int docId = 0; docId < maxDoc; docId++) {
                Document doc = new Document();
                int count = docTermCount[docId];
                for (int i = 0; i < count; i++) {
                    int termOrd = sortedIn.readInt();
                    doc.add(new SortedSetDocValuesField(getFieldName(), termDict.get(termOrd)));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();
        }
        Files.delete(sortedFile);

        // Release transient structures
        termDict = null;
        docTermCount = null;

        DirectoryReader dirReader = DirectoryReader.open(mmapDir);
        LeafReader leafReader = dirReader.leaves().get(0).reader();
        return new DiskBackedSortedSetLeafFieldData(leafReader, dirReader, mmapDir, tempDir, getFieldName());
    }

    private TermsEnum filter(Terms terms, TermsEnum iterator, LeafReader reader) throws IOException {
        if (iterator == null) {
            return null;
        }
        int docCount = terms.getDocCount();
        if (docCount == -1) {
            docCount = reader.maxDoc();
        }
        if (docCount >= minSegmentSize) {
            final int minFreq = minFrequency > 1.0 ? (int) minFrequency : (int) (docCount * minFrequency);
            final int maxFreq = maxFrequency > 1.0 ? (int) maxFrequency : (int) (docCount * maxFrequency);
            if (minFreq > 1 || maxFreq < docCount) {
                iterator = new FrequencyFilter(iterator, minFreq, maxFreq);
            }
        }
        return iterator;
    }

    /**
     * Leaf field data backed by mmap'd SortedSetDocValues on disk.
     *
     * @opensearch.internal
     */
    private static class DiskBackedSortedSetLeafFieldData extends AbstractLeafOrdinalsFieldData {

        private final LeafReader reader;
        private final String field;
        private final DirectoryReader dirReader;
        private final MMapDirectory mmapDir;
        private final Path tempDir;

        DiskBackedSortedSetLeafFieldData(LeafReader reader, DirectoryReader dirReader, MMapDirectory mmapDir, Path tempDir, String field) {
            super(DEFAULT_SCRIPT_FUNCTION);
            this.reader = reader;
            this.field = field;
            this.dirReader = dirReader;
            this.mmapDir = mmapDir;
            this.tempDir = tempDir;
        }

        @Override
        public SortedSetDocValues getOrdinalsValues() {
            try {
                return DocValues.getSortedSet(reader, field);
            } catch (IOException e) {
                throw new IllegalStateException("cannot load docvalues from disk-backed fielddata", e);
            }
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }

        @Override
        public void close() {
            try {
                dirReader.close();
                mmapDir.close();
                Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException e) {
                        // best effort cleanup
                    }
                });
            } catch (IOException e) {
                // best effort cleanup
            }
        }
    }

    /**
     * A frequency filter for terms.
     *
     * @opensearch.internal
     */
    private static final class FrequencyFilter extends FilteredTermsEnum {
        private final int minFreq;
        private final int maxFreq;

        FrequencyFilter(TermsEnum delegate, int minFreq, int maxFreq) {
            super(delegate, false);
            this.minFreq = minFreq;
            this.maxFreq = maxFreq;
        }

        @Override
        protected AcceptStatus accept(BytesRef arg0) throws IOException {
            int docFreq = docFreq();
            if (docFreq >= minFreq && docFreq <= maxFreq) {
                return AcceptStatus.YES;
            }
            return AcceptStatus.NO;
        }
    }
}
