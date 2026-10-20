/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.index.query.QueryBuilders.constantScoreQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Verifies the node-wide dynamic switches that enable or disable the request and query caches.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class RequestAndQueryCacheEnablementIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static final String INDEX = "index";

    // The randomizing client varies preference/timeout per node, and both are part of the request cache key,
    // so use a single client for the whole test
    private Client client;

    public RequestAndQueryCacheEnablementIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Before
    public void setUpClient() {
        client = client();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // registers the test-only index.queries.cache.everything setting
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // cache queries on every segment, not only large ones, so a tiny test index is cached
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true)
            .build();
    }

    public void testRequestCacheEnableDisable() throws Exception {
        createSingleShardIndex(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true));
        indexAndMerge();

        searchAggs();
        assertHitsAndMisses(0, 1);
        assertThat(requestCacheStats().getMemorySizeInBytes(), greaterThan(0L));
        searchAggs();
        assertHitsAndMisses(1, 1);

        updateClusterSetting(IndicesRequestCache.INDICES_REQUEST_CACHE_ENABLED_SETTING, false);
        assertBusy(() -> assertThat(requestCacheStats().getMemorySizeInBytes(), equalTo(0L)));
        // the node-wide switch wins over the explicit request parameter
        for (int i = 0; i < 3; i++) {
            assertSearchResponse(aggsSearch().setRequestCache(true).get());
        }
        assertHitsAndMisses(1, 1);
        assertThat(requestCacheStats().getMemorySizeInBytes(), equalTo(0L));

        updateClusterSetting(IndicesRequestCache.INDICES_REQUEST_CACHE_ENABLED_SETTING, true);
        searchAggs();
        assertHitsAndMisses(1, 2);
        assertThat(requestCacheStats().getMemorySizeInBytes(), greaterThan(0L));
        searchAggs();
        assertHitsAndMisses(2, 2);
    }

    public void testQueryCacheEnableDisable() throws Exception {
        createSingleShardIndex(
            Settings.builder()
                // the random index template may disable the query cache for the index
                .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), true)
                .put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true)
                .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), false)
        );
        indexAndMerge();

        assertHitCount(client.prepareSearch(INDEX).setQuery(constantScoreQuery(termQuery("f", "a"))).get(), 1);
        assertThat(queryCacheStats().getQueryCache().getCacheSize(), greaterThan(0L));
        long cacheCount = queryCacheStats().getQueryCache().getCacheCount();

        updateClusterSetting(IndicesQueryCache.INDICES_QUERIES_CACHE_ENABLED_SETTING, false);
        assertBusy(() -> assertThat(queryCacheStats().getQueryCache().getCacheSize(), equalTo(0L)));
        assertHitCount(client.prepareSearch(INDEX).setQuery(constantScoreQuery(termQuery("f", "a"))).get(), 1);
        assertHitCount(client.prepareSearch(INDEX).setQuery(constantScoreQuery(termQuery("f", "b"))).get(), 1);
        assertThat(queryCacheStats().getQueryCache().getCacheSize(), equalTo(0L));
        assertThat(queryCacheStats().getQueryCache().getCacheCount(), equalTo(cacheCount));

        updateClusterSetting(IndicesQueryCache.INDICES_QUERIES_CACHE_ENABLED_SETTING, true);
        assertHitCount(client.prepareSearch(INDEX).setQuery(constantScoreQuery(termQuery("f", "b"))).get(), 1);
        assertThat(queryCacheStats().getQueryCache().getCacheSize(), greaterThan(0L));
    }

    private void createSingleShardIndex(Settings.Builder settings) {
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(INDEX)
                .setMapping("f", "type=keyword")
                .setSettings(
                    settings.put(SETTING_NUMBER_OF_SHARDS, 1)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0)
                        // Disable index refreshing to avoid caches being invalidated mid-test
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(-1))
                )
                .get()
        );
        ensureGreen(INDEX);
    }

    private void indexAndMerge() {
        client.prepareIndex(INDEX).setId("1").setSource("f", "a").get();
        client.prepareIndex(INDEX).setId("2").setSource("f", "b").get();
        refresh(INDEX);
        // Force merge to a single segment so no background merge invalidates caches during the test
        assertThat(client.admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get().getFailedShards(), equalTo(0));
        refresh(INDEX);
    }

    private SearchRequestBuilder aggsSearch() {
        // DFS requests are never cached, so pin the search type rather than let the randomizing client pick it
        return client.prepareSearch(INDEX)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .addAggregation(AggregationBuilders.terms("t").field("f"));
    }

    private void searchAggs() {
        assertSearchResponse(aggsSearch().get());
    }

    private void updateClusterSetting(Setting<Boolean> setting, boolean value) {
        assertAcked(
            client.admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(setting.getKey(), value)).get()
        );
    }

    private void assertHitsAndMisses(long expectedHits, long expectedMisses) {
        RequestCacheStats stats = requestCacheStats();
        // compare together so a failure shows both values
        assertEquals(Arrays.asList(expectedHits, expectedMisses), Arrays.asList(stats.getHitCount(), stats.getMissCount()));
    }

    private RequestCacheStats requestCacheStats() {
        return client.admin().indices().prepareStats(INDEX).clear().setRequestCache(true).get().getTotal().getRequestCache();
    }

    private CommonStats queryCacheStats() {
        return client.admin().indices().prepareStats(INDEX).clear().setQueryCache(true).get().getTotal();
    }
}
