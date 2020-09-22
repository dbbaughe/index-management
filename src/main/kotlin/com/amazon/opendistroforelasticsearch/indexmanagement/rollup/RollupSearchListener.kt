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

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetrics
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Histogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Average
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Max
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Min
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Sum
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.ValueCount
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
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.ParsedQuery
import org.elasticsearch.index.shard.SearchOperationListener
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.AggregatorFactories
import org.elasticsearch.search.aggregations.SearchContextAggregations
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder
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

        //putMapping(new MappingMetadata(type, XContentHelper.convertToMap(XContentFactory.xContent(source), source, true)));
        //XContentHelper.convertToMap()
        // TODO: move this into separate class
        val rollupJobs = mutableListOf<Rollup>()
        val rollupy = searchContext.indexShard().indexSettings().indexMetadata.mapping()?.source()
        if (rollupy == null) throw UnsupportedOperationException("Nope")
        val xcp = XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, rollupy.compressedReference(), XContentType.JSON)
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation) // start of block
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, xcp.nextToken(), xcp::getTokenLocation) // _doc
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation) // start of _doc block
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                "_meta" -> {
                    logger.info("in meta")
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
                    while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                        val fieldName = xcp.currentName()
                        xcp.nextToken()

                        when (fieldName) {
                            "rollups" -> {
                                logger.info("in rollups")
                                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
                                while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                                    val fieldName = xcp.currentName()
                                    xcp.nextToken()
                                    logger.info("Parsing $fieldName rollup")
                                    rollupJobs.add(Rollup.parse(xcp))
                                }
                            }
                            else -> xcp.skipChildren()
                        }
                    }
                }
                else -> xcp.skipChildren()
            }
        }
        logger.info("Rollupjobs are $rollupJobs")
        /*
        * currentToken = null
        * NextToken = object -> currenttoken = START_OBJECT
        * NextToken = field -> currentToken = FIELD_NAME and currentName = _doc
        * NextToken = START_OBJECT and currentName = _doc
        * NextToken = FIELD_NAME and _meta
        * NT = START_OBJECT and _meta
        * NT = FIELD_NAME and rollups
        * NT = START_OBJECT and rollups
        * NT = FIELD_NAME and example
        * skipChildren -> undefined
        * current -> FIELD_NAME and exmaple
        * NT = START_OJBECT and example (maybe you cant skip on a FIELD_NAME?)
        * skipChildren
        * current -> END OBJECT and example
        * NT -> END_OBJECT and rollups
        * NT -> END_OBJECT and _meta
        * NT -> FIELD_NAME and dynamic_templates
        * NT -> skip -> NT -> FIELD NAME properties
        * NT -> skip -> NT -> END OBJECT _doc
        * NT -> END OBJECT null
        * NT -> null null
        *
        *
        * */
        // need to parse all rollups and figure out which is best suited to answer the query
        // means we need to know all aggregations/queries included in the request

        // and then what does it really mean for a rollup job to be able to answer a query
        // obviously if query has non-valid aggregation we can reject it, or do we just let it skip the documents?
        // like if you do a terms query on a field that doesnt exist in rollup what happens? in the normal case it wouldnt throw an exception
        // if you search a field that a document doesnt have, it just means the document doesnt fall into the bucket

        //hve a tree of aggregation/subaggregations
        // need to get all types and fields so we can compare to rollup jobs
        // some fields could be multiple types? so dont use the field as the key
        // whats gonna be the best way to compare jobs and aggs?
        // will need to do same for queries

        // crete a map of type of aggregation to field?
        // terms -> product.id, customer.id, city, sku, order_quantity
        // histogram -> price, order_quantity
        // date_histogram -> order_date
        // sum -> price, taxful, taxless, shipping
        // and then use this for verifying rollup jobs?

        // TODO: any issues with replication of job mappings to shards? i.e. can two shards have different _meta mappings and resolve to different
        // types of jobs?

        // once we determine which rollup job to use then what? we need to use that job to
        // what happens if more than one job can answer the questions?
        // could probably make it more performant by using the one with less data (i.e. lower granularity) since they both can still answer
        // we also have to "score" them somehow.. if we eventually want to let queries that dont match 100%
        // i.e. job a matches 80% and job b matches 80% but they match different things which one is better?

        // for now to keep it simple - filter only ones that match 100% of the query/aggs and then use the "newest" created one
        // we'll optimize from there
        // if we change how the matching works then this could be considered a breaking change
        // dont change how matching works on previous versions? keep a versioning schema for internal matching
        // so that all rollup job on v2 still uses v2 matching instead of a new v3 one?
        // does this work in practice? have to do some tests

        // TODO: do we need the WHOLE rollup job in meta? it contains too much information which isnt helpful like enabled time
        // and pageSize etc.

        // TODO: how does this job matching work with roles/security?

        val dimensionTypesToFields = mutableMapOf<String, MutableSet<String>>()
        val metricFieldsToTypes = mutableMapOf<String, MutableSet<String>>()
        getAggregationMetadata(searchContext.request().source().aggregations().aggregatorFactories, dimensionTypesToFields, metricFieldsToTypes)
        logger.info("Dimension types to fields $dimensionTypesToFields")
        logger.info("Metric fields to types $metricFieldsToTypes")

        // for every rollup job in _meta - filter for all jobs that
        // contain all of the types of aggs and contains all of the fields for those aggs
        // TODO: move to helper class
        val matchingRollupJobs = rollupJobs.filter { job -> // TODO: Veeeeeeery inefficient - works for testing tho
            logger.info("Checking job $job")
            // check for ONLY bucketing types of aggregations in dimensions
            // TODO duplicates? in the filteredDimensions list
            dimensionTypesToFields.entries.all { (type, set) -> // set is the set of fields in the aggregation before rewriting
                // TODO: can this miss something in dimensioTypestofields and then its not checked? ie the type is something other than terms, dh, h so we then dont bother to check?
                //   dont think so because then filteredDimensions is empty and the containsAll should fail
                logger.info("checking dimension type $type for $set")
                val filteredDimensions = job.dimensions.filter { it.type.type == type }.map {// looking for dimensions that are of this type, aaah this is trying to find sum in dimensions
                    when (it) {
                        is DateHistogram -> it.targetField
                        is Histogram -> it.targetField
                        is Terms -> it.targetField
                        // TODO: cant throw - have to return early after overwriting parsedquery
                        else -> throw IllegalArgumentException("Found unsupported Dimension during search transformation [${it.type.type}]")
                    }
                }
                logger.info("Filtered dimensions $filteredDimensions")
                filteredDimensions.containsAll(set)
            } && metricFieldsToTypes.entries.all { (field, set) ->
                logger.info("checking metric for field $field for $set")
                // using the GIVEN user query which should be the RAW/SOURCE field we should find the RollupMetrics that contain the same source_field
                val filteredMetrics = job.metrics.filter { it.sourceField == field }.map {// there should only be 1 rollupmetric with a given field? to go from List<List<String>> to just List<String>, maybe remove filter and use findFirst?
                    it.metrics.map { it.type.type }
//                    when (it) {
//                        is Average -> it.targetField
//                        is Max -> it.targetField
//                        is Min -> it.targetField
//                        is Sum -> it.targetField
//                        is ValueCount -> it.targetField
//                        // TODO: cant throw - have to return early after overwriting parsedquery
//                        else -> throw IllegalArgumentException("Found unsupported Metric during search transformation [${it.type.type}]")
//                    }
                }
                logger.info("Filtered metrics $filteredMetrics")
                // so for every field we get the matching rollupmetrics and we get the metric aggs computed for that field
                // then we confirm this rollupmetric contains all the metricaggs the user aggregation wants to perform
                (filteredMetrics.firstOrNull() ?: emptyList()).containsAll(set)
            }

        }

        // TODO: cant throw - have to return early after overwriting parsedquery
        if (matchingRollupJobs.isEmpty()) throw UnsupportedOperationException("Nope")

        val theOneAndOnlyJob = matchingRollupJobs.reduce { accu, curr ->
            if (accu.lastUpdateTime.isAfter(curr.lastUpdateTime)) accu
            else curr
        }

        // now we need to transform the agg

        // find best job --> need to parse all rollup jobs which means EVERY shard is parsing all rollup jobs which is duplicated work
        // this will depend on what we do for queries/aggs not matching EXACTLY whats available
        // so lets first just make it easy and assume we reject any request that cant 100% be fulfilled by rollup data
        // then we want to get similar types -> fields for rollup jobs.. and just confirm query/aggs is a 100% subset of job
        //XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, )
        //                    XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE,
        //                    response.sourceAsBytesRef, XContentType.JSON).use { xcp ->
        //                        rollup = Rollup.parseWithType(xcp, response.id, response.seqNo, response.primaryTerm)
        //                    }




        val aggregationBuilders = searchContext.request().source().aggregations().aggregatorFactories
        rewriteAggregations(theOneAndOnlyJob, aggregationBuilders) // TODO: above returns unmodifiablecollection, is it fine?
        val factories = searchContext.request().source().aggregations().build(searchContext.queryShardContext, null)
        searchContext.aggregations(SearchContextAggregations(factories, searchContext.aggregations().multiBucketConsumer()))


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

    @Throws(UnsupportedOperationException::class)
    private fun rewriteAggregations(job: Rollup, aggBuilders: Collection<AggregationBuilder>) {
        // need the job to know what to rewrite things to
        if (aggBuilders.isEmpty()) return
        aggBuilders.forEach {
            logger.info("name ${it.name}")
            logger.info("it.writeableName ${it.writeableName}")
            logger.info("it.type ${it.type}")
            logger.info("metadata: ${it.metadata}")
            logger.info("it.subAggregations.size ${it.subAggregations.size}")
            when (it) {
                // TODO: seems like valuessourceagg is parent for each of these? can we just use this
                // or do we have to be explicit to make sure nothing unintended gets by
//                is ValuesSourceAggregationBuilder<*> -> {
//                    logger.info("ValuesSourceAggregationBuilder")
//                }
                is TermsAggregationBuilder -> {
                    logger.info("TermsAggregationBuilder")
                    val dim = job.dimensions.find { dimension -> dimension.sourceField == it.field() && dimension.type == Dimension.Type.TERMS } as Terms
                    it.field(dim.targetField)
                }
                is DateHistogramAggregationBuilder -> {
                    logger.info("DateHistogramAggregationBuilder")
                    val dim = job.dimensions.find { dimension -> dimension.sourceField == it.field() && dimension.type == Dimension.Type.DATE_HISTOGRAM } as DateHistogram
                    it.field(dim.targetField)
                }
                is HistogramAggregationBuilder -> {
                    logger.info("HistogramAggregationBuilder")
                    val dim = job.dimensions.find { dimension -> dimension.sourceField == it.field() && dimension.type == Dimension.Type.HISTOGRAM } as Histogram
                    // TODO: add field on dimension? or add FieldDimension and field to that?
                    it.field(dim.targetField)
                }
                is SumAggregationBuilder -> {
                    // TODO: imported wrong Sum instead of metric Sum a few times, make sure that didnt happen anywhere else
                    logger.info("SumAggregationBuilder")
                    // TODO rewrite RollupMetrics to be individual
                    // TODO: the multiple percentiles on one index field here
                    val metric = job.metrics.find { metric -> metric.sourceField == it.field() && metric.metrics.any { m -> m is Sum } } as RollupMetrics
                    it.field("${metric.targetField}.sum") // TODO: hardcoded
                    logger.info("sum field: ${it.field()}")
                }
                is AvgAggregationBuilder -> {
                    logger.info("AvgAggregationBuilder")
                    val metric = job.metrics.find { metric -> metric.sourceField == it.field() && metric.metrics.any { m -> m is Average } } as RollupMetrics
                    it.field("${metric.targetField}.avg")
                    logger.info("avg field: ${it.field()}")
                }
                is MaxAggregationBuilder -> {
                    logger.info("MaxAggregationBuilder")
                    val metric = job.metrics.find { metric -> metric.sourceField == it.field() && metric.metrics.any { m -> m is Max } } as RollupMetrics
                    it.field("${metric.targetField}.max")
                    logger.info("max field: ${it.field()}")
                }
                is MinAggregationBuilder -> {
                    logger.info("MinAggregationBuilder")
                    val metric = job.metrics.find { metric -> metric.sourceField == it.field() && metric.metrics.any { m -> m is Min } } as RollupMetrics
                    it.field("${metric.targetField}.min")
                    logger.info("min field: ${it.field()}")
                }
                is ValueCountAggregationBuilder -> {
                    logger.info("ValueCountAggregationBuilder")
                    val metric = job.metrics.find { metric -> metric.sourceField == it.field() && metric.metrics.any { m -> m is ValueCount } } as RollupMetrics
                    it.field("${metric.targetField}.value_count")
                    logger.info("value_count field: ${it.field()}")
                }
                // do we also throw exception if the job itself does not support the query they are running?
                // most likely as if they try to do a terms query on city and they didnt rollup city.. then it wont work
                else -> throw UnsupportedOperationException("The ${it.type} aggregation is not currently supported in rollups")
            }
            rewriteAggregations(job, it.subAggregations)
        }
    }

    // modifying input parameter... change to return final built output instead?
    // i.e. lowest level returns its own map of sets and each level above combines returned and its own map of sets
    // until final is a return of everything
    @Throws(UnsupportedOperationException::class)
    private fun getAggregationMetadata(
        aggregationBuilders: Collection<AggregationBuilder>,
        dimensionTypesToFields: MutableMap<String, MutableSet<String>>,
        metricFieldsToTypes: MutableMap<String, MutableSet<String>>
    ) {
        aggregationBuilders.forEach {
            when (it) {
                is TermsAggregationBuilder -> {
                    dimensionTypesToFields.computeIfAbsent(it.type) { mutableSetOf() }.add(it.field())
                }
                is DateHistogramAggregationBuilder -> {
                    dimensionTypesToFields.computeIfAbsent(it.type) { mutableSetOf() }.add(it.field())
                }
                is HistogramAggregationBuilder -> {
                    dimensionTypesToFields.computeIfAbsent(it.type) { mutableSetOf() }.add(it.field())
                }
                is SumAggregationBuilder -> {
                    metricFieldsToTypes.computeIfAbsent(it.field()) { mutableSetOf() }.add(it.type)
                }
                is AvgAggregationBuilder -> {
                    metricFieldsToTypes.computeIfAbsent(it.field()) { mutableSetOf() }.add(it.type)
                }
                is MaxAggregationBuilder -> {
                    metricFieldsToTypes.computeIfAbsent(it.field()) { mutableSetOf() }.add(it.type)
                }
                is MinAggregationBuilder -> {
                    metricFieldsToTypes.computeIfAbsent(it.field()) { mutableSetOf() }.add(it.type)
                }
                is ValueCountAggregationBuilder -> {
                    metricFieldsToTypes.computeIfAbsent(it.field()) { mutableSetOf() }.add(it.type)
                }
            }
            if (it.subAggregations.isNotEmpty()) {
                getAggregationMetadata(it.subAggregations, dimensionTypesToFields, metricFieldsToTypes)
            }
        }
    }

    override fun onQueryPhase(searchContext: SearchContext, tookInNanos: Long) {
        logger.info("onQueryPhase")
        // we dont know what ordinal each bucket refers to
//        searchContext.aggregations().aggregators().forEach {
//            when (it) {
//                is BucketsAggregator -> {
//                    it
//                }
//            }
//        }
    }
}

/*
*
* //
//+//        logger.info("searchContext.aggregations().factories.countAggregators() ${searchContext.aggregations().factories().countAggregators()}")
//+//        logger.info("searchContext.aggregations().factories.createTopLevelAggregators(searchContext),map { it.name } ${searchContext.aggregations().factories().createTopLevelAggregators(searchContext).map { it.name() }}")
//+//        logger.info("searchContext.aggregations().factories.createTopLevelAggregators(searchContext),map { it } ${searchContext.aggregations().factories().createTopLevelAggregators(searchContext).map { it.parent() }}")
//+//        val collectors: MutableList<Aggregator> = mutableListOf()
//+//        val aggregators: Array<Aggregator>
//+//        val factories = searchContext.aggregations().factories()
//+//
//+//        aggregators = factories.createTopLevelAggregators(searchContext)
//+//        searchContext.indexShard().searchOperationListener
//+        // A single shard that this will be executed on can have MULTIPLE jobs in it
//+        // So what needs to happen is the query needs to execute only on ONE of the job outputs, i.e. you can't count docs from two different jobs as that would more than likely count
//+        // duplicate documents (unless the user did in fact have completely distinct rollup jobs with no overlap, but we won't bother checking that.. or should we?)
//+        // Let's see.. if we do a query for... terms: "customer_id" and there is only 1 rollup job with customer_id then thats fine, but if there are two rollup jobs on customer_id then we need to make sure we only count
//+        // 1 of them... is that true... if there are 2 rollup jobs both on customer_id and each rollup job did a different metric aggregation then technically it could be fine? except doc counts would get screwed up
//+        // so yeah, only allow this to resolve to 1 rollup job, this means we need to be able to access the rollup job configurations at this point in time
//+//        searchContext.indexShard().indexSettings().indexMetadata.mapping()
//+//        val aggs = searchContext.request().source().aggregations()
//+//        val aggsFactories = aggs.aggregatorFactories
//+//        aggsFactories.forEach {
//+//            logger.info("name ${it.name}")
//+//            logger.info("it.writeableName ${it.writeableName}")
//+//            logger.info("it.type ${it.type}")
//+//            logger.info("metadata: ${it.metadata}")
//+//            logger.info("it.subAggregations.size ${it.subAggregations.size}")
//+//            if (it is ValuesSourceAggregationBuilder<*>) {
//+//                it.field("g.keyword")
//+//                // it.build(...) -> public final AggregatorFactory build(QueryShardContext queryShardContext, AggregatorFactory parent)
//+//
//+//            }
//+//            it.subAggregations.forEach {
//+//                logger.info("subaggs name ${it.name}")
//+//                logger.info("subaggs it.writeableName ${it.writeableName}")
//+//                logger.info("subaggs it.type ${it.type}")
//+//                logger.info("subaggsmetadata: ${it.metadata}")
//+//                logger.info("subaggs it.subAggregations.size ${it.subAggregations.size}")
//+//            }
//+//
//+//            it.bucketCardinality()
//+//            //it.rewrite(QueryRewriteContext)
//+//        }
//+//        val factories = searchContext.request().source().aggregations().build(searchContext.queryShardContext, null)
//+//        searchContext.aggregations(SearchContextAggregations(factories, searchContext.aggregations().multiBucketConsumer()))
//+
//+//        aggregators.forEach {
//+//            if (it !is GlobalAggregator) {
//+//                collectors.add(it)
//+//            }
//+//        }
//+
//+        //searchContext.request()
//+        //searchContext.queryShardContext()
//+        //searchContext.request().source().aggregations().rewrite(...)
//+        //searchContext.request().source().aggregations().build(...)
//+        //searchContext.request().source().aggregations().aggregatorFactories
//+
//+        //public abstract SearchContextAggregations aggregations();
//+        //
//+        //    public abstract SearchContext aggregations(SearchContextAggregations aggregations);
//+
//+        // val factor = searchContext.request().source().aggregations().build(searchContext.queryShardContext, null)
//+        // searchContext.aggregations(SearchContextAggregations(factories))
//+
//+        //       if (source.aggregations() != null) {
//+        //            try {
//+        //                AggregatorFactories factories = source.aggregations().build(queryShardContext, null);
//+        //                context.aggregations(new SearchContextAggregations(factories, multiBucketConsumerService.create()));
//+        //            } catch (IOException e) {
//+        //                throw new AggregationInitializationException("Failed to create aggregators", e);
//+        //            }
//+        //        }
//+
//+//
//+//        searchContext.aggregations().aggregators(aggregators)
//+//
//+//        if (collectors.isNotEmpty()) {
//+//            val collector: Collector = MultiBucketCollector.wrap(collectors)
//+//            (collector as BucketCollector).preCollection()
//+//        }
*
* */