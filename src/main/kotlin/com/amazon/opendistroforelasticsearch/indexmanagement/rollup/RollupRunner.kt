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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.retry
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.stop.StopRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.stop.StopRollupRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Histogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Average
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Max
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Metric
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Min
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Sum
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.ValueCount
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings.Companion.ROLLUP_INGEST_BACKOFF_COUNT
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings.Companion.ROLLUP_INGEST_BACKOFF_MILLIS
import com.amazon.opendistroforelasticsearch.indexmanagement.util._DOC
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestToXContentListener
import org.elasticsearch.script.ScriptService
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder
import org.elasticsearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.InternalAvg
import org.elasticsearch.search.aggregations.metrics.InternalMax
import org.elasticsearch.search.aggregations.metrics.InternalMin
import org.elasticsearch.search.aggregations.metrics.InternalSum
import org.elasticsearch.search.aggregations.metrics.InternalValueCount
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import java.time.ZoneId

object RollupRunner : ScheduledJobRunner,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("RollupRunner")) {

    private val logger = LogManager.getLogger(javaClass)

    private lateinit var clusterService: ClusterService
    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var scriptService: ScriptService
    private lateinit var settings: Settings
    private lateinit var rollupMapperService: RollupMapperService
    @Volatile private lateinit var retryIngestPolicy: BackoffPolicy

    fun registerClusterService(clusterService: ClusterService): RollupRunner {
        this.clusterService = clusterService
        return this
    }

    fun registerClient(client: Client): RollupRunner {
        this.client = client
        return this
    }

    fun registerNamedXContentRegistry(xContentRegistry: NamedXContentRegistry): RollupRunner {
        this.xContentRegistry = xContentRegistry
        return this
    }

    fun registerScriptService(scriptService: ScriptService): RollupRunner {
        this.scriptService = scriptService
        return this
    }

    fun registerSettings(settings: Settings): RollupRunner {
        this.settings = settings
        return this
    }

    fun registerRollupMapperService(rollupMapperService: RollupMapperService): RollupRunner {
        this.rollupMapperService = rollupMapperService
        return this
    }

    fun registerConsumers(): RollupRunner {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ROLLUP_INGEST_BACKOFF_MILLIS, ROLLUP_INGEST_BACKOFF_COUNT) {
                millis, count -> retryIngestPolicy = BackoffPolicy.constantBackoff(millis, count)
        }
        retryIngestPolicy = BackoffPolicy.constantBackoff(ROLLUP_INGEST_BACKOFF_MILLIS.get(settings), ROLLUP_INGEST_BACKOFF_COUNT.get(settings))
        return this
    }

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        if (job !is Rollup) {
            throw IllegalArgumentException("Invalid job type, found ${job.javaClass.simpleName} with id: ${context.jobId}")
        }

        // Determine if a full window has passed before launching a coroutine and locking?

        /* Determine if a full window has passed
        *  How? We want the window to start with the very first document. If we have never run yet how do we know if there is a full window?
        *  Once we have started it's fine as we can just keep increasing from the last window
        *  One thing to do.. GET the first document sorted by the timestamp field and then use that.. kind of sucks but any other way?
        * */

        launch {
            // Get initial metadata and then figure out next window
            // If metadata does not exist then this is first time job is running
            // So we should initialize a metadata by getting the very first document in the index and check its timestamp
            if (job.metadataID == null) {
                // initialize metadata by getting first document sorted by date range field
            } else {
                // check for full window
                val rollupMetadata = getMetaData(job.metadataID)
            }

            // Attempt to acquire lock
            val lock: LockModel? = context.lockService.suspendUntil { acquireLock(job, context, it) }
            if (lock == null) {
                logger.debug("Could not acquire lock for ${job.id}")
            } else {
                runRollupJob(job, context)
                // Release lock
                val released: Boolean = context.lockService.suspendUntil { release(lock, it) }
                if (!released) {
                    logger.debug("Could not release lock for ${job.id}")
                }
            }
        }
    }

    private suspend fun runRollupJob(job: Rollup, context: JobExecutionContext) {
        logger.info("Running rollup job for ${job.id}")
        /* Determine if a full window has passed OR DO IT ABOVE
        *  How? We want the window to start with the very first document. If we have never run yet how do we know if there is a full window?
        *  Once we have started it's fine as we can just keep increasing from the last window
        *  One thing to do.. GET the first document sorted by the timestamp field and then use that.. kind of sucks but any other way?
        * */

        // // While loop hasFullWindow; Get the full window

        // // P1: Get Validations

        // // P1: Save Validations

        // // Get MetaData

        // // // While loop after key Composite aggregations

        // now have a response and we need to convert it into bulk requests into target index
        // if thats successfull then we proceed with next afterkey
//        val response: SearchResponse = client.suspendUntil { search(rollupSearchRequest, it) }
//        response.failedShards // TODO: what do we do if failed shards? can this happen with allow partial results false
//        response.clusters // Think about cross cluster search impact/behavior
//        response.isTerminatedEarly
//        response.isTimedOut
//        response.took // stats
//        response.totalShards // stats?


        rollupMapperService.createRollupTargetIndex(job)
        // TODO: clean up in terms of after key and search request
        val cab = getCompositeAggregationBuilder(job)
        var afterKey: Map<String, Any>? = null
        do {
            afterKey?.let { cab.aggregateAfter(it) }
            val response: SearchResponse = client.suspendUntil { search(getRollupSearchRequest(cab, job.sourceIndex), it) }
            val internalComposite = response.aggregations.get<InternalComposite>("RollupCompositeAggs")
            logger.info("buckets size: ${internalComposite.buckets.size}")
            afterKey = internalComposite.afterKey()
            logger.info("AfterKey is $afterKey")
            // create a class that handles the whole flow of composite -> index -> loop and can spit out an updated metadata?
            var requestsToRetry = convertResponseToRequests(job, internalComposite)
            // what is the actual behavior when bulk requests fails and we run out of retries? we definitely shouldnt move past it as we'd then have incorrect/missed rollup data
            // so I guess just retry retry retry and if at the end we still have failed we just need to redo this whole execution next time
            // how many times should we retry with TOO_MANY_REQUESTS? retrying is probably less expensive than redoing this whole calculation... but maybe the time between executions gives enough time then
            // if someone sets a REALLY high backoff count could we go over the 30 min lock?
            // TODO: move retry out of ISM package
            if (requestsToRetry.isNotEmpty()) {
                retryIngestPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
                    val bulkRequest = BulkRequest().add(requestsToRetry)
                    val bulkResponse: BulkResponse = client.suspendUntil { client.bulk(bulkRequest, it) }
                    val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
                    requestsToRetry = failedResponses.filter { it.status() == RestStatus.TOO_MANY_REQUESTS }
                        .map { bulkRequest.requests()[it.itemId] as IndexRequest }

                    if (requestsToRetry.isNotEmpty()) {
                        val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                        throw ExceptionsHelper.convertToElastic(retryCause)
                    }
                }
            }
        } while (afterKey != null)

        //response.aggregations.get<InternalCOmposite>("RollupCompositeAggs")
        //afterKey: CompositeKEy
        //buckets InternalCOmposite.InternalBucket
        // aggregations InternalAggregation
        //InternalSum
        //how to parse aggs

        // TODO: how can we have an ongoing job have its page size updated during ongoing executions?

        // temporary - just using single execution to bulk index search data and stopping job
        stopJob(job)

        // TODO Have some fake data bulk ingested as compacted data into a target index

        //{ timestamp: <time>, price: <$$$> }
        //^ compact them into hourly?
        context.expectedExecutionTime.toEpochMilli()

        // // // Update MetaData

        // // // If after key not null, renew lock

        // // Update Validations

        // renewLock
    }

    private suspend fun getMetaData(id: String): RollupMetadata? {
        // TODO: theres a difference between returning null because of error and returning null because of not exist
        // unless this function asumes it exists because we do in fact have an ID which shouldnt be possible without one
        // so if its null something went wrong
        // then question is do we perm fail or auto recover somehow
        var rollupMetadata: RollupMetadata? = null
        try {
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, id)
            // TODO: move suspendUntil out of ISM
            val response: GetResponse = client.suspendUntil { get(getRequest, it) }
            val metadataSource = response.sourceAsBytesRef

            withContext(Dispatchers.IO) {
                val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, metadataSource, XContentType.JSON)
                rollupMetadata = RollupMetadata.parse(xcp, response.id, response.seqNo, response.primaryTerm)
            }
        } catch (e: Exception) {
            logger.error(e)
        }

        return rollupMetadata
    }

    /*

    This job is for order date 5 min, city, and only sum on price

          {
        "_index" : "roll",
        "_type" : "_doc",
        "_id" : "aaa$xPfNS6vLDmmCbbl8EiMFdw",
        "_score" : 1.0,
        "_source" : {
          "order_date.date_histogram.timestamp" : 1599091200000,
          "order_date.date_histogram.interval" : "5m",
          "order_date.date_histogram.time_zone" : "UTC",
          "geoip.city_name.terms.value" : "Abu Dhabi",
          "_rollup.version" : 2,
          "order_date.date_histogram._count" : 1,
          "taxless_total_price.sum.value" : 35.96875,
          "geoip.city_name.terms._count" : 1,
          "_rollup.id" : "aaa"
        }
      },



    This job is order date 5 min, city, and every metric on price
    * {
        "_index" : "roll",
        "_type" : "_doc",
        "_id" : "ecom$y4750qnAm7J-5t6sH3Zgcg",
        "_score" : 1.0,
        "_source" : {
          "taxless_total_price.max.value" : 35.96875,
          "taxless_total_price.min.value" : 35.96875,
          "geoip.city_name.terms.value" : "Abu Dhabi",
          "taxless_total_price.sum.value" : 35.96875,
          "order_date.date_histogram.timestamp" : 1599091200000,
          "taxless_total_price.value_count.value" : 1.0,
          "taxless_total_price.avg._count" : 1.0,
          "order_date.date_histogram.interval" : "5m",
          "order_date.date_histogram.time_zone" : "UTC",
          "_rollup.version" : 2,
          "order_date.date_histogram._count" : 1,
          "taxless_total_price.avg.value" : 35.96875,
          "geoip.city_name.terms._count" : 1,
          "_rollup.id" : "ecom"
        }
      },



      this job is for order date 5 min, terms on city and product quantity, histogram on product quantity, and avg on product quantity

            {
        "_index" : "roll2",
        "_type" : "_doc",
        "_id" : "bbbb$dW-CciBySBUCV98OCvimRQ",
        "_score" : 1.0,
        "_source" : {
          "products.quantity.histogram.interval" : 5,
          "products.quantity.avg._count" : 8.0,
          "geoip.city_name.terms.value" : "Abu Dhabi",
          "products.quantity.terms.value" : 1,
          "products.quantity.terms._count" : 4,
          "order_date.date_histogram.timestamp" : 1599091200000,
          "products.quantity.histogram.value" : 0.0,
          "products.quantity.avg.value" : 8.0,
          "order_date.date_histogram.interval" : "5m",
          "order_date.date_histogram.time_zone" : "UTC",
          "_rollup.version" : 2,
          "order_date.date_histogram._count" : 4,
          "products.quantity.histogram._count" : 4,
          "geoip.city_name.terms._count" : 4,
          "_rollup.id" : "bbbb"
        }
      },
    *
    * */

    //TODO missing terms field           "geoip.city_name.terms.value" : null,

    // TODO: how to handle average of averages

    // TODO: doc counts for aggregations are showing the doc counts of the rollup docs and not the raw data which is expected, elastic has a PR for a _doc_count mapping which we might be able to use but its in PR and they could change it
    //  is there a way we can overwrite doc_count? on request/response? https://github.com/elastic/elasticsearch/pull/58339
    //  verified elastic currently gives incorrect doc counts too (they give rolled up doc counts)

    private fun convertResponseToRequests(job: Rollup, internalComposite: InternalComposite): List<DocWriteRequest<*>> {
        val requests = mutableListOf<DocWriteRequest<*>>()
        internalComposite.buckets.forEach {
            // what is the ID we are using - combination of rollup id and dimensions?
            // for now just use unhashed - but we might need to hash depending on length...
            // if we have to hash - how to protect against collisions? do we even worry about them
            // whats sort order of id? do we really allow the same set but different order equate to a different document?
            // perhaps sort them by key and then append values
            val documentId = it.key.entries.sortedBy { it.key }.joinToString { it.value.toString() }
            val mapOfKeyValues = mutableMapOf<String, Any>("rollup.id" to job.id) // TODO: include rollup version?
            logger.info("KeyAsString ${it.keyAsString}")
            logger.info("documentId: $documentId")
            it.key.entries.forEach {
                logger.info("Keys - key: ${it.key} and value: ${it.value}")
                // TODO: it seems composite returns just key values? what happens when your composuite grouping is on same field?
                //  it seems coposite requires the user to enter a key to represent the source which puts the requirement of dealing with conflicts
                //  on the user which is nice - we could do the same in rollup dimensions? require user to enter a string and then not have to deal with
                //  any conflicting fields with different types?
                val type = (job.dimensions.find { dim -> dim.targetField == it.key } as Dimension).type.type // TODO: fix this
                mapOfKeyValues[it.key] = it.value // this doesnt work because you can have multiple types of dimensions on same field, i.e. terms and histogram on number field
                // transform doesnt allow terms on number but rollup does
            }
            logger.info("DocCount ${it.docCount}")
            it.aggregations.forEach {
                when (it) {
                    // TODO handle conflicts? is it possible?
                    is InternalSum -> {
                        logger.info("Sub Aggregation is InternalSum")
                        logger.info("Type is ${it.type} and name is ${it.name} and value is ${it.value}")
                        mapOfKeyValues["${it.name}.${it.type}"] = it.value // TODO: <name>.sum = value change to <name>: { sum: value }
                        // it.name + it.type = it.value          -> taxless_total_price.sum = 35.96875
                        // does this need doc count? or can we just use doc count from above
                        // This should be saved with the above key... or do we need to? since the above key is only 1 per document
                        // so then no we don't need to... just need to save the field and type and value?
                    }
                    is InternalMax -> logger.info("Sub Aggregation is InternalMax")
                    is InternalMin -> logger.info("Sub Aggregation is InternalMin")
                    is InternalValueCount -> logger.info("Sub Aggregation is InternalValueCount")
                    is InternalAvg -> logger.info("Sub Aggregation is InternalAvg")
                    else -> logger.info("Unsupported aggregation")
                }
            }
            logger.info("mapOfKeyValues: $mapOfKeyValues")
            val indexRequest = IndexRequest(job.targetIndex) // do we want to allow users to add routing to the documents?
                .id(documentId)
                .source(mapOfKeyValues) // we shouldnt need to care about version/seqno/primaryterm because this hsould be idempotent
            requests.add(indexRequest)
        }
        return requests
    }

    // just temporary
    private suspend fun stopJob(job: Rollup) {
        val stop = StopRollupRequest(job.id)
        try {
            val response: AcknowledgedResponse = client.suspendUntil { execute(StopRollupAction.INSTANCE, stop, it) }
            logger.info("Stopped job ${response.isAcknowledged}")
        } catch (e: Exception) {
            logger.error("Failed to stop job", e)
        }
    }

    private fun getCompositeAggregationBuilder(job: Rollup): CompositeAggregationBuilder {
        val sources = mutableListOf<CompositeValuesSourceBuilder<*>>()
        job.dimensions.forEach { dimension ->
            when (dimension) {
                is DateHistogram -> {
                    // TODO: this could conflict with other fields - require user to give name instead?
                    // but this goes against rollup which has the same field/names
                    DateHistogramValuesSourceBuilder(dimension.targetField).apply {
                        this.field(dimension.sourceField)
                        this.timeZone(ZoneId.of(dimension.timezone))
                        dimension.calendarInterval?.let { it ->
                            this.calendarInterval(DateHistogramInterval(it))
                        }
                        dimension.fixedInterval?.let { it ->
                            this.fixedInterval(DateHistogramInterval(it))
                        }
                    }.also { it ->
                        sources.add(it)
                    }
                }
                is Terms -> {
                    // TODO: require unique target fields on metrics and dimensions? is it needed on metrics or only dimensions?
                    TermsValuesSourceBuilder(dimension.targetField).apply {
                        this.field(dimension.sourceField)
                    }.also {
                        sources.add(it)
                    }
                }
                is Histogram -> {
                    // TODO is it possible any of these can be done mixed on the same field resulting in conflicts with the keys/names?
                    HistogramValuesSourceBuilder(dimension.targetField).apply {
                        this.field(dimension.sourceField)
                        this.interval(dimension.interval)
                    }
                }
            }
        }
        val cab = CompositeAggregationBuilder("RollupCompositeAggs", sources)
        job.metrics.forEach { metric ->
            metric.sourceField
            metric.metrics // right now strings, needs to be "AVG" -> { "avg": { } }
            metric.metrics.forEach { agg ->
                // TODO is it possible any of these can be done mixed on the same field resulting in conflicts with the keys/names?
                cab.subAggregation(
                    when (agg) {
                        is Average -> AvgAggregationBuilder(metric.targetField).field(metric.sourceField)
                        is Sum -> SumAggregationBuilder(metric.targetField).field(metric.sourceField)
                        is Max -> MaxAggregationBuilder(metric.targetField).field(metric.sourceField)
                        is Min-> MinAggregationBuilder(metric.targetField).field(metric.sourceField)
                        is ValueCount -> ValueCountAggregationBuilder(metric.targetField).field(metric.sourceField)
                        else -> throw UnsupportedOperationException("Found unsupported metric aggregation ${agg.type.type}")
                    }
                )
            }
        }
        return cab
        // TODO: unrelated to this part but for debugging ill want to see raw rollup documents but all my searches will get intercepted and transformed?
        //  only if we reject size > 0... should we? makes sense for user experience but for debugging its a pain
        //  can still have cluster setting to disable search but that means you could impact ongoing searches by cx to debug
    }

    private fun getRollupSearchRequest(cab: CompositeAggregationBuilder, index: String): SearchRequest {
        val searchSourceBuilder = SearchSourceBuilder()
            .trackTotalHits(false)
            .size(0)
            .aggregation(cab)
            .query(MatchAllQueryBuilder())// TODO: range query based on windw
        return SearchRequest(index)
            .source(searchSourceBuilder)
            .allowPartialSearchResults(false) // TODO check this
    }
}