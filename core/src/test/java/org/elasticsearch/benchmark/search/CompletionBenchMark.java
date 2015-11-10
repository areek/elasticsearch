/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.benchmark.search;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.transport.TransportModule;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 */
public class CompletionBenchMark {

    private static final String INDEX_NAME = "index";
    private static final String TYPE_NAME = "type";

    private static Client client;
    private static Node node;

    public static void main(String[] args) throws Exception {
        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "-1")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("path.home", "/tmp/")
                .put(TransportModule.TRANSPORT_TYPE_KEY, "local")
                .build();

        String clusterName = CompletionBenchMark.class.getSimpleName();
        node = nodeBuilder().clusterName(clusterName)
                .settings(settingsBuilder().put(settings))
                .node();

        client = node.client();

        // Delete the index if already present
        try {
            client.admin().indices().prepareDelete(INDEX_NAME).get();
        } catch (IndexNotFoundException e) {
        }

        final Set<String> prefixesUpto1 = new HashSet<>();
        final Set<String> prefixesUpto2 = new HashSet<>();
        final Set<String> prefixesUpto3 = new HashSet<>();
        final Set<String> prefixesUpto4 = new HashSet<>();
        final Set<String> prefixesUpto5 = new HashSet<>();
        try {
            XContentBuilder mapping = jsonBuilder().startObject()
                    .startObject(TYPE_NAME)
                        .startObject("properties")
                            .startObject("title_suggest")
                                .field("type", "completion")
                                //.field()
                            .endObject()
                            .startObject("payload")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                    .endObject();
            client.admin().indices().prepareCreate(INDEX_NAME)
                    .addMapping(TYPE_NAME, mapping)
                    .get();

            // Index docs
            BulkRequestBuilder builder = client.prepareBulk();
            final Path path = Paths.get("/Users/areek/workspace/allCountries.txt");
            try (BufferedReader reader = Files.newBufferedReader(path)) {
                int c = 0;
                String line;
                Map<String, Object> source = new HashMap<>(1);
                Map<String, Object> map = new HashMap<>(2);
                while ((line = reader.readLine()) != null) {
                    String[] splits = line.split("\t");
                    if (splits.length > 0) {
                        int id = Integer.parseInt(splits[0]);
                        String name = splits[1];
                        String payload = splits[2];
                        prefixesUpto1.add(name.substring(0, Math.min(name.length(), 1)));
                        prefixesUpto2.add(name.substring(0, Math.min(name.length(), 2)));
                        prefixesUpto3.add(name.substring(0, Math.min(name.length(), 3)));
                        prefixesUpto4.add(name.substring(0, Math.min(name.length(), 4)));
                        prefixesUpto5.add(name.substring(0, Math.min(name.length(), 5)));
                        map.put("input", name);
                        map.put("weight", id);
                        source.put("title_suggest", map);
                        source.put("payload", payload);
                        builder.add(
                                client.prepareIndex(INDEX_NAME, TYPE_NAME, String.valueOf(c++))
                                        .setSource(source)
                        );
                        map.clear();
                        source.clear();
                        if (builder.numberOfActions() >= 1000) {
                            BulkResponse response = builder.get();
                            if (response.hasFailures()) {
                                System.err.println("--> bulk failed");
                            }
                            builder = client.prepareBulk();
                        }
                    }
                    if (c == 5000000) {
                        break;
                    }
                }
            }
            if (builder.numberOfActions() > 0) {
                BulkResponse response = builder.get();
                if (response.hasFailures()) {
                    System.err.println("--> bulk failed");
                }
            }
        } catch (IndexAlreadyExistsException e) {
            System.out.println("--> Index already exists, ignoring indexing phase, waiting for green");
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
        }
        client.admin().indices().prepareForceMerge().setMaxNumSegments(1).get();
        client.admin().indices().prepareRefresh().execute().actionGet();
        System.out.println("Count: " + client.prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet().getHits().totalHits());
        System.out.println("Index Size: " + client.admin().indices().prepareStats(INDEX_NAME).setCompletionFields("title_suggest").get().getPrimaries().getCompletion().getSize());

        runPerformanceTestsForPrefix(5, 5, prefixesUpto5);
        runPerformanceTestsForPrefix(1, 5, prefixesUpto1);
        runPerformanceTestsForPrefix(2, 5, prefixesUpto2);
        runPerformanceTestsForPrefix(3, 5, prefixesUpto3);
        runPerformanceTestsForPrefix(4, 5, prefixesUpto4);

        client.close();
        node.close();
    }

    private static void runPerformanceTestsForPrefix(int maxLen, final int size, Collection<String> prefixes) {
        runPerformanceTest("PREFIX_QUERY: maxLen: " + maxLen + " size: " + size, prefixes, new Bench() {
            @Override
            public int query(String prefix) {
                return (int) client.prepareSearch(INDEX_NAME).setSize(size).setQuery(QueryBuilders.prefixQuery("title_suggest", prefix)).get().getHits().getTotalHits();
            }
        });

        runPerformanceTest("prefix maxLen: " + maxLen + " size: " + size, prefixes, new Bench() {
            @Override
            public int query(String prefix) {
                return client.prepareSuggest(INDEX_NAME).addSuggestion(
                        SuggestBuilders.completionSuggestion("prefix").field("title_suggest").prefix(prefix).size(size))
                        .get().getSuggest().getSuggestion("prefix").getEntries().get(0).getOptions().size();
            }
        });

        runPerformanceTest("prefix maxLen: " + maxLen + " size: " + size + " payload: true ", prefixes, new Bench() {
            @Override
            public int query(String prefix) {
                return client.prepareSuggest(INDEX_NAME).addSuggestion(
                        SuggestBuilders.completionSuggestion("prefix").field("title_suggest").prefix(prefix).payload("payload").size(size))
                        .get().getSuggest().getSuggestion("prefix").getEntries().get(0).getOptions().size();
            }
        });
    }

    interface Bench {
        int query(String prefix);
    }

    private static void runPerformanceTest(String id, Collection<String> prefixes, Bench bench) {
        BenchmarkResult result = measure(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                int v = 0;
                for (String prefix : prefixes) {
                    v += bench.query(prefix);
                }
                return v;
            }
        });

        System.err.println(
                String.format(Locale.ROOT, "%-15s queries: %d, time[ms]: %s, ~kQPS: %.0f",
                        id,
                        prefixes.size(),
                        result.toString(),
                        prefixes.size() / result.avg));

    }


    private final static int rounds = 15;
    private final static int warmup = 5;

    private static BenchmarkResult measure(Callable<Integer> callable) {
        final double NANOS_PER_MS = 1000000;

        try {
            List<Double> times = new ArrayList<>();
            for (int i = 0; i < warmup + rounds; i++) {
                final long start = System.nanoTime();
                guard = callable.call().intValue();
                times.add((System.nanoTime() - start) / NANOS_PER_MS);
            }
            return new BenchmarkResult(times, warmup, rounds);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);

        }
    }

    /** Guard against opts. */
    @SuppressWarnings("unused")
    private static volatile int guard;

    private static class BenchmarkResult {


        /** Average time per round (ms). */
        public final double avg;

        /**
         * Standard deviation (in milliseconds).
         */
        public final double stddev;

        public BenchmarkResult(List<Double> times, int warmup, int rounds) {
            List<Double> values = times.subList(warmup, times.size());
            double sum = 0;
            double sumSquares = 0;

            for (double l : values) {
                sum += l;
                sumSquares += l * l;
            }

            this.avg = sum / (double) values.size();
            this.stddev = Math.sqrt(sumSquares / (double) values.size() - this.avg * this.avg);
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "%.0f [+- %.2f]",
                    avg, stddev);
        }
    }
}
