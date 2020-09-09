package com.amazon.opendistroforelasticsearch.indexmanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupRunner
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
import org.apache.logging.log4j.LogManager

object IndexManagementRunner : ScheduledJobRunner {

    private val logger = LogManager.getLogger(javaClass)

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        when (job) {
            is ManagedIndexConfig -> ManagedIndexRunner.runJob(job, context)
            is Rollup -> RollupRunner.runJob(job, context)
            else -> {
                val errorMessage = "Invalid job type, found ${job.javaClass.simpleName} with id: ${context.jobId}"
                logger.error(errorMessage)
                throw IllegalArgumentException(errorMessage)
            }
        }
    }
}