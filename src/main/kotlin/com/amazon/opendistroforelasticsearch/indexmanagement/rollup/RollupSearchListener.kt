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
import org.apache.lucene.index.IndexReader
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Query
import org.apache.lucene.search.QueryVisitor
import org.apache.lucene.search.ScoreMode
import org.apache.lucene.search.Weight
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.index.query.ParsedQuery
import org.elasticsearch.index.shard.SearchOperationListener
import org.elasticsearch.search.aggregations.AggregatorFactories
import org.elasticsearch.search.aggregations.SearchContextAggregations
import org.elasticsearch.search.internal.SearchContext

class RollupExceptionQuery : Query() {
    override fun createWeight(searcher: IndexSearcher?, scoreMode: ScoreMode?, boost: Float): Weight =
        throw UnsupportedOperationException("Cannot query rollup and normal indices in the same request")
    override fun rewrite(reader: IndexReader?): Query =
        throw UnsupportedOperationException("Cannot query rollup and normal indices in the same request")
    override fun visit(visitor: QueryVisitor?) =
        throw UnsupportedOperationException("Cannot query rollup and normal indices in the same request")
    override fun hashCode(): Int = -1
    override fun equals(other: Any?): Boolean = false
    override fun toString(field: String?): String = "rollup_exception_query"
}

class RollupSearchListener(
    private val clusterService: ClusterService,
    private val indexNameExpressionResolver: IndexNameExpressionResolver
) : SearchOperationListener {

    private val logger = LogManager.getLogger(javaClass)

    override fun onNewContext(context: SearchContext) {
        logger.info("on New Context")
        context.from()
    }

    override fun onPreQueryPhase(searchContext: SearchContext) {

        logger.info("onPreQueryPhase")
        val indices = searchContext.request().indices().map { it.toString() }.toTypedArray()
        val concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterService.state(), searchContext.request().indicesOptions(), *indices)
        logger.info("The concrete indices are ${concreteIndices.map { it.toString() }}")
        val hasNonRollupIndex = concreteIndices.any {
            val isRollupIndex = RollupSettings.ROLLUP_INDEX.get(clusterService.state().metadata.index(it).settings)
            if (!isRollupIndex) logger.warn("A non-rollup index cannot be searched with a rollup index [index=$it]")
            !isRollupIndex
        }

        //logger.info("Max result window ${searchContext.indexShard().indexSettings().maxResultWindow}")
        searchContext.request().aliasFilter.queryBuilder
        if (hasNonRollupIndex) {
            val query = RollupExceptionQuery()
            searchContext.parsedQuery(ParsedQuery(query))
            return
        }

        //searchContext.indexShard().indexSettings().indexMetadata

        //searchContext
        searchContext.aggregations().factories()
        val searchContextAggregations = SearchContextAggregations(searchContext.aggregations().factories(), searchContext.aggregations().multiBucketConsumer())
        //searchContext.aggregations(searchContextAggregations)

        searchContext.aggregations().factories()

        searchContext.aggregations()?.aggregators()?.forEach {
            logger.info("Name of aggregator ${it.name()}")
        }

        //searchContext.aggregations().factories()

        /**************************************************/
        val indexShard = searchContext.indexShard()
        /**************************************************/

        /**************************************************/
        val queryShardContext = searchContext.queryShardContext
        /**************************************************/

        /**************************************************/
        val query = searchContext.query()
        /**************************************************/

        /**************************************************/
        val shardSearchRequest = searchContext.request()
        logger.info("Index routings ${shardSearchRequest.indexRoutings().map { it.toString() }}")
        logger.info("Indices are ${shardSearchRequest.indices().map { it.toString() }}")
        logger.info("IndicesOptions are ${shardSearchRequest.indicesOptions()}")
        /**************************************************/
    }

    override fun onQueryPhase(searchContext: SearchContext, tookInNanos: Long) {
        logger.info("onQueryPhase")
    }
}