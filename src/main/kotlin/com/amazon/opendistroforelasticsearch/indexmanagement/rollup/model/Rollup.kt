/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.instant
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.optionalTimeField
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimensions.DateHistogramDimension
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimensions.HistogramDimension
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimensions.TermsDimension
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.CronSchedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.ScheduleParser
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.index.seqno.SequenceNumbers
import java.io.IOException
import java.time.Instant

data class Rollup(
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    val enabled: Boolean,
    val schemaVersion: Long,
    val jobSchedule: Schedule,
    val jobLastUpdatedTime: Instant,
    val jobEnabledTime: Instant?,
    val description: String,
    val sourceIndex: String,
    val targetIndex: String,
    val metadataID: String?,
    val roles: List<String>,
    val pageSize: Long,
    val delay: Long,
    val termsDimension: TermsDimension?,
    val dateHistogramDimension: DateHistogramDimension,
    val histogramDimensions: List<HistogramDimension>,
    val metrics: List<RollupMetrics>
) : ScheduledJobParameter, Writeable {

    init {
        if (enabled) {
            requireNotNull(jobEnabledTime) { "jobEnabledTime must be present if the job is enabled" }
        } else {
            require(jobEnabledTime == null) { "jobEnabledTime must not be present if the job is disabled" }
        }
    }

    override fun isEnabled() = enabled

    override fun getName() = id // the id is user chosen and represents the rollup's name

    override fun getEnabledTime() = jobEnabledTime

    override fun getSchedule() = jobSchedule

    override fun getLastUpdateTime() = jobLastUpdatedTime

    override fun getLockDurationSeconds(): Long = 3600L // 1 hour

    @Throws(IOException::class)
    constructor(sin: StreamInput): this(
        id = sin.readString(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        enabled = sin.readBoolean(),
        schemaVersion = sin.readLong(),
        jobSchedule = ScheduleParser.readFrom(sin),
        jobLastUpdatedTime = sin.readInstant(),
        jobEnabledTime = sin.readOptionalInstant(),
        description = sin.readString(),
        sourceIndex = sin.readString(),
        targetIndex = sin.readString(),
        metadataID = sin.readOptionalString(),
        roles = sin.readStringArray().toList(),
        pageSize = sin.readLong(),
        delay = sin.readLong(),
        termsDimension = if (sin.readBoolean()) TermsDimension.readFrom(sin) else null,
        dateHistogramDimension = DateHistogramDimension.readFrom(sin),
        histogramDimensions = sin.readList(::HistogramDimension),
        metrics = sin.readList(::RollupMetrics)
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
                .startObject(ROLLUP_TYPE)
                    .field(ENABLED_FIELD, enabled)
                    .field(SCHEDULE_FIELD, jobSchedule)
                    .optionalTimeField(LAST_UPDATED_TIME_FIELD, jobLastUpdatedTime)
                    .optionalTimeField(ENABLED_TIME_FIELD, jobEnabledTime)
                    .field(DESCRIPTION_FIELD, description)
                    .field(SOURCE_INDEX_FIELD, sourceIndex)
                    .field(TARGET_INDEX_FIELD, targetIndex)
                    .field(METADATA_ID_FIELD, metadataID)
                    .field(ROLES_FIELD, roles.toTypedArray())
                    .field(PAGE_SIZE_FIELD, pageSize)
                    .field(DELAY_FIELD, delay)
                    .startObject(DIMENSIONS_FIELD)
                        .field(TermsDimension.TERMS_FIELD, termsDimension)
                        .field(DateHistogramDimension.DATE_HISTOGRAM_FIELD, dateHistogramDimension)
                        .field(HistogramDimension.HISTOGRAM_FIELD, histogramDimensions.toTypedArray())
                    .endObject()
                    .field(RollupMetrics.METRICS_FIELD, metrics.toTypedArray())
                .endObject()
            .endObject()
        return builder
    }


    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeBoolean(enabled)
        out.writeLong(schemaVersion)
        if (schedule is CronSchedule) {
            out.writeEnum(Schedule.Type.CRON)
        } else {
            out.writeEnum(Schedule.Type.INTERVAL)
        }
        out.writeInstant(jobLastUpdatedTime)
        out.writeOptionalInstant(jobEnabledTime)
        out.writeString(description)
        out.writeString(sourceIndex)
        out.writeString(targetIndex)
        out.writeOptionalString(metadataID)
        out.writeStringArray(roles.toTypedArray())
        out.writeLong(pageSize)
        out.writeLong(delay)
        if (termsDimension == null) {
            out.writeBoolean(false)
        } else {
            out.writeBoolean(true)
            termsDimension.writeTo(out)
        }
        dateHistogramDimension.writeTo(out)
        out.writeCollection(histogramDimensions)
        out.writeCollection(metrics)


    }

    companion object {
        const val ROLLUP_TYPE = "rollup"
        const val NO_ID = ""
        const val ENABLED_FIELD = "enabled"
        const val SCHEMA_VERSION_FIELD = "schema_version"
        const val SCHEDULE_FIELD = "schedule"
        const val LAST_UPDATED_TIME_FIELD = "last_updated_time"
        const val ENABLED_TIME_FIELD = "enabled_time"
        const val DESCRIPTION_FIELD = "description"
        const val SOURCE_INDEX_FIELD = "source_index"
        const val TARGET_INDEX_FIELD = "target_index"
        const val METADATA_ID_FIELD = "metadata_id"
        const val ROLES_FIELD = "roles"
        const val PAGE_SIZE_FIELD = "page_size"
        const val DELAY_FIELD = "delay"
        const val DIMENSIONS_FIELD = "dimensions"

        val test = ParseField("something")


        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): Rollup {
            var schedule: Schedule? = null
            var schemaVersion: Long = IndexUtils.DEFAULT_SCHEMA_VERSION
            var lastUpdatedTime: Instant? = null
            var enabledTime: Instant? = null
            var enabled = true
            var description: String? = null
            var sourceIndex: String? = null
            var targetIndex: String? = null
            var metadataID: String? = null
            val roles = mutableListOf<String>()
            var pageSize: Long? = null
            var delay: Long? = null
            var dateHistogramDimension: DateHistogramDimension? = null
            var termsDimension: TermsDimension? = null
            val histograms = mutableListOf<HistogramDimension>()
            val metrics = mutableListOf<RollupMetrics>()

            println("Inside rollup parse")

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            println("Inside rollup parse about to execute while loop")
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ENABLED_FIELD -> enabled = xcp.booleanValue()
                    SCHEDULE_FIELD -> schedule = ScheduleParser.parse(xcp)
                    SCHEMA_VERSION_FIELD -> schemaVersion = xcp.longValue()
                    ENABLED_TIME_FIELD -> enabledTime = xcp.instant()
                    LAST_UPDATED_TIME_FIELD -> lastUpdatedTime = xcp.instant()
                    DESCRIPTION_FIELD -> description = xcp.text()
                    SOURCE_INDEX_FIELD -> sourceIndex = xcp.text()
                    TARGET_INDEX_FIELD -> targetIndex = xcp.text()
                    METADATA_ID_FIELD -> metadataID = xcp.textOrNull()
                    ROLES_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp::getTokenLocation)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            roles.add(xcp.text())
                        }
                    }
                    PAGE_SIZE_FIELD -> pageSize = xcp.longValue()
                    DELAY_FIELD -> delay = xcp.longValue()
                    DIMENSIONS_FIELD -> {
                        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
                        while (xcp.nextToken() != Token.END_OBJECT) {
                            val dimensionsFieldName = xcp.currentName()
                            xcp.nextToken()

                            when (dimensionsFieldName) {
                                TermsDimension.TERMS_FIELD -> termsDimension = if (xcp.currentToken() == Token.VALUE_NULL) null else TermsDimension.parse(xcp)
                                DateHistogramDimension.DATE_HISTOGRAM_FIELD -> dateHistogramDimension = DateHistogramDimension.parse(xcp)
                                HistogramDimension.HISTOGRAM_FIELD -> {
                                    ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp::getTokenLocation)
                                    while (xcp.nextToken() != Token.END_ARRAY) {
                                        histograms.add(HistogramDimension.parse(xcp))
                                    }
                                }
                                else -> throw IllegalArgumentException("Invalid field: [$dimensionsFieldName] found in Rollup dimensions.")
                            }
                        }
                    }
                    RollupMetrics.METRICS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp::getTokenLocation)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            metrics.add(RollupMetrics.parse(xcp))
                        }
                    }
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Rollup.")
                }
            }

            if (enabled && enabledTime == null) {
                enabledTime = Instant.now()
            } else if (!enabled) {
                enabledTime = null
            }
            return Rollup(
                id,
                seqNo,
                primaryTerm,
                enabled = enabled,
                schemaVersion = schemaVersion,
                jobSchedule = requireNotNull(schedule) { "Rollup schedule is null" },
                jobLastUpdatedTime = requireNotNull(lastUpdatedTime) { "Rollup last updated time is null" },
                jobEnabledTime = enabledTime,
                description = requireNotNull(description) { "Rollup description is null" },
                sourceIndex = requireNotNull(sourceIndex) { "Rollup source index is null" },
                targetIndex = requireNotNull(targetIndex) { "Rollup target index is null" },
                metadataID = metadataID,
                roles = roles.toList(),
                pageSize = requireNotNull(pageSize) { "Rollup page size is null" },
                delay = requireNotNull(delay) { "Rollup delay is null" },
                termsDimension = termsDimension,
                dateHistogramDimension = requireNotNull(dateHistogramDimension) { "Rollup date histogram is required" },
                histogramDimensions = histograms,
                metrics = metrics
            )
        }

        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parseWithType(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): Rollup {
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp::getTokenLocation)
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            val rollup = parse(xcp, id, seqNo, primaryTerm)
            ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            return rollup
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): Rollup = Rollup(sin)
    }
}