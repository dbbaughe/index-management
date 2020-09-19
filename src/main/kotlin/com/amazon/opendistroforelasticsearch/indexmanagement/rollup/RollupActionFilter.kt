/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings
import org.apache.logging.log4j.LogManager
import org.apache.lucene.search.TotalHits
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.search.MultiSearchRequest
import org.elasticsearch.action.search.SearchProgressActionListener
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchShard
import org.elasticsearch.action.search.SearchTask
import org.elasticsearch.action.support.ActionFilter
import org.elasticsearch.action.support.ActionFilterChain
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.io.stream.DelayableWriteable
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.aggregations.InternalAggregations
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram
import org.elasticsearch.search.aggregations.metrics.InternalSum
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.tasks.Task
import java.lang.Exception
import kotlin.system.measureNanoTime
import kotlin.system.measureTimeMillis

class RollupActionFilter(
    val clusterService: ClusterService,
    val indexNameExpressionResolver: IndexNameExpressionResolver
) : ActionFilter {

    private val log = LogManager.getLogger(javaClass)

    override fun order(): Int {
        return Integer.MIN_VALUE
    }

    override fun <Request : ActionRequest, Response : ActionResponse> apply(
        task: Task,
        action: String,
        request: Request,
        listener: ActionListener<Response>,
        chain: ActionFilterChain<Request, Response>
    ) {
//         if (rollupSearchDisabled) return chain.proceed(task, action, request, listener)
        if (request is SearchRequest) {
            log.info("The indices are ${request.indices().map { it.toString() }}")
            // TODO: This seems expensive - this is happening on EVERY request; even ones not going to rollup indices
//            val time = measureNanoTime {
            val indices = request.indices().map { it.toString() }.toTypedArray()
            // indices: ["lo?g*", "test", "another*"]
            val concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterService.state(), request.indicesOptions(), *indices)
            // indices: ["logstash-01", "logstash-02", "test", "another-thing"]
            var hasRollupIndex = false
            var hasNonRollupIndex = false
            for (index in concreteIndices) {
                if (hasRollupIndex && hasNonRollupIndex) break
                val isRollupIndex = RollupSettings.ROLLUP_INDEX.get(clusterService.state().metadata.index(index).settings)

                if (isRollupIndex) hasRollupIndex = true else hasNonRollupIndex = true
            }
            if (hasRollupIndex && hasNonRollupIndex) {
                log.warn("A non-rollup index cannot be searched with a rollup index")
                return listener.onFailure(ElasticsearchStatusException("Rollups are not supported in the same search request as normal indices", RestStatus.BAD_REQUEST))
            }
//            }
            //log.info("The time it took in nanos: $time")
            // always rewrite search request
            //log.info("The request is ${request.source()}, but setting to match all")
//            request.source().query(MatchAllQueryBuilder())
            //log.info("The request is now ${request.source()}")
            if (hasRollupIndex) {
                //            AsyncSearchProgressActionListener progressActionListener = new AsyncSearchProgressActionListener(asyncSearchContext,
                //                    () -> {
                //                    taskManager.unregister(searchTask);
                //                    unregisterChildNode.close();
                //            });
                //            searchTask.setProgressListener(progressActionListener);
                val searchListener = object : SearchProgressActionListener() {
                    override fun onFinalReduce(
                        shards: MutableList<SearchShard>,
                        totalHits: TotalHits,
                        aggs: InternalAggregations,
                        reducePhase: Int
                    ) {
                        aggs.asList().forEach {
                            if (it is InternalDateHistogram) {
                                //it
                            }
                        }
                        log.info("onFinalReduce")
                    }
                    override fun onPartialReduce(
                        shards: MutableList<SearchShard>,
                        totalHits: TotalHits,
                        aggs: DelayableWriteable.Serialized<InternalAggregations>,
                        reducePhase: Int
                    ) {
                        log.info("onPartialReduce")
                    }
                    override fun onQueryResult(shardIndex: Int) {
                        log.info("onQueryResult")
                    }
                    override fun onResponse(response: SearchResponse) {
                        log.info("onResponse")
                    }
                    override fun onFailure(e: Exception) {
                        log.info("onFailure")
                    }
                }
                (task as SearchTask).setProgressListener(searchListener)
            }
            return chain.proceed(task, action, request, listener)
        }

//        // TODO: Handle MultiSearchRequest
//        if (request is MultiSearchRequest) {
//            request.requests().map { it.indices() }
//            request.indicesOptions()
//        }
//
//        // Don't need this
//        if (request is BulkRequest) {
//            val indices = request.requests().map { it.index() to it.indicesOptions() }.toTypedArray()
//            val concreteIndices = indices.map { indexNameExpressionResolver.concreteIndexNames(clusterService.state(), it.second, it.first) }
//            var hasRollupIndex = false
//            var hasNonRollupIndex = false
//            concreteIndices.forEach {
//                it.forEach {index ->
//                    val isRollupIndex = RollupSettings.ROLLUP_INDEX.get(clusterService.state().metadata.index(index).settings)
//                    if (isRollupIndex) hasRollupIndex = true else hasNonRollupIndex = true
//                }
//            }
//            if (hasRollupIndex && hasNonRollupIndex) {
//                log.warn("A non-rollup index cannot be searched with a rollup index")
//                return listener.onFailure(ElasticsearchException("Rollups are not supported in the same search request as normal indices"))
//            }
//        }
        chain.proceed(task, action, request, listener)
    }
}