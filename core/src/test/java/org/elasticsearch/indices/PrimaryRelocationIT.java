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

package org.elasticsearch.indices;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PrimaryRelocationIT extends ESIntegTestCase {

    @TestLogging("indices.recovery:TRACE,index.shard.service:TRACE,cluster.service:TRACE")
    public void testPrimaryRelocationWhileIndexing() throws Exception {
        logger.info("--> starting [node1] ...");
        final String node_1 = internalCluster().startNode(Settings.EMPTY);
        logger.info("--> creating test index ...");
        client().admin().indices().prepareCreate("test")
                .setSettings(Settings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                )
                .execute().actionGet();

        ensureGreen("test");
        // make sure we have an index on node1
        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1").get();
        assertThat(indexResponse.isCreated(), equalTo(true));
        client().admin().indices().prepareFlush().get();

        logger.info("--> starting [node2] ...");
        final String node_2 = internalCluster().startDataOnlyNode(Settings.EMPTY);
        final String[] nodes = new String[] {node_1, node_2};
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread writer = new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        IndexResponse indexResponse = client(randomFrom(nodes)).prepareIndex("test", "type", "2").setSource("field1", "value2").get();
                        assertThat("deleted document was found", indexResponse.isCreated(), equalTo(true));
                        DeleteResponse deleteResponse = client(randomFrom(nodes)).prepareDelete("test", "type", "2").get();
                        assertThat("indexed document was not found", deleteResponse.isFound(), equalTo(true));
                    } catch (Throwable e) {
                        failure.set(e);
                        stop.set(true);
                        break;
                    }
                }
            }
        };
        writer.start();
        for (int i = 0; !stop.get(); i++) {
            int from = i % 2;
            int to = (from == 0) ? 1 : 0;
            logger.info("--> relocation from {} to {} ", nodes[from], nodes[to]);
            client().admin().cluster().prepareReroute()
                    .add(new MoveAllocationCommand(new ShardId("test", 0), nodes[from], nodes[to]))
                    .execute().actionGet();
            clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForRelocatingShards(0).execute().actionGet();
            assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
            logger.info("--> relocation complete");
        }

        assertFalse(ExceptionsHelper.detailedMessage(failure.get()), true);
    }
}
