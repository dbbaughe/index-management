package com.amazon.opendistroforelasticsearch.indexmanagement.rollup

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
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
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.script.ScriptService

object RollupRunner : ScheduledJobRunner,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("RollupRunner")) {

    private val logger = LogManager.getLogger(javaClass)

    private lateinit var clusterService: ClusterService
    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var scriptService: ScriptService
    private lateinit var settings: Settings

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

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        if (job !is Rollup) {
            throw IllegalArgumentException("Invalid job type, found ${job.javaClass.simpleName} with id: ${context.jobId}")
        }

        // Determine if a full window has passed before launching a coroutine and locking?

        /* Determine if a full window has passed OR DO IT ABOVE
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
                try {
                    val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, job.metadataID)
                    val response: GetResponse = client.suspendUntil { get(getRequest, it) }
                    val metadataSource = response.sourceAsBytesRef

                    withContext(Dispatchers.IO) {
                        val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, metadataSource, XContentType.JSON)
                        RollupMetadata.parse(xcp, response.id, response.seqNo, response.primaryTerm)
                    }
                } catch (e: Exception) {
                    logger.error(e)
                }
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

        // // // bulkIndex

        // // // Update MetaData

        // // // If after key not null, renew lock

        // // Update Validations

        // renewLock
    }
}