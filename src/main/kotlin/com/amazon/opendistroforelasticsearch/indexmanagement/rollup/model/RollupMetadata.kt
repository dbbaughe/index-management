package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.instant
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.index.seqno.SequenceNumbers
import java.io.IOException
import java.time.Instant

data class RollupMetadata(
    val id: String,
    val seqNo: Long,
    val primaryTerm: Long,
    val rollupID: String,
    val afterKey: String?,
    val lastUpdatedTime: Instant,
    val windowStartTime: Instant,
    val windowEndTime: Instant,
    val finished: Boolean,
    val failed: Boolean,
    val failureReason: String?
): ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput): this(
        id = sin.readString(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        rollupID = sin.readString(),
        afterKey = sin.readOptionalString(),
        lastUpdatedTime = sin.readInstant(),
        windowStartTime = sin.readInstant(),
        windowEndTime = sin.readInstant(),
        finished = sin.readBoolean(),
        failed = sin.readBoolean(),
        failureReason = sin.readOptionalString()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(ROLLUP_ID_FIELD, rollupID)
        if (afterKey != null) builder.field(AFTER_KEY_FIELD, afterKey)
        builder
            .field(LAST_UPDATED_FIELD, lastUpdatedTime)
            .field(WINDOW_START_TIME_FIELD, windowStartTime)
            .field(WINDOW_END_TIME_FIELD, windowEndTime)
            .field(FINISHED_FIELD, finished)
            .field(FAILED_FIELD, failed)
        if (failureReason != null) builder.field(FAILURE_REASON, failureReason)
        return builder
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeString(rollupID)
        out.writeOptionalString(afterKey)
        out.writeInstant(lastUpdatedTime)
        out.writeInstant(windowStartTime)
        out.writeInstant(windowEndTime)
        out.writeBoolean(finished)
        out.writeBoolean(failed)
        out.writeOptionalString(failureReason)
    }

    companion object {
        const val NO_ID = ""
        const val ROLLUP_ID_FIELD = "rollup_id"
        const val AFTER_KEY_FIELD = "after_key"
        const val LAST_UPDATED_FIELD = "last_updated"
        const val WINDOW_START_TIME_FIELD = "window_start_time"
        const val WINDOW_END_TIME_FIELD = "window_end_time"
        const val FINISHED_FIELD = "finished"
        const val FAILED_FIELD = "failed"
        const val FAILURE_REASON = "failure_reason"
        const val STATS_FIELD = "stats" // TODO


        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): RollupMetadata {
            var rollupID: String? = null
            var afterKey: String? = null
            var lastUpdatedTime: Instant? = null
            var windowStartTime: Instant? = null
            var windowEndTime: Instant? = null
            var finished = false
            var failed = false
            var failureReason: String? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ROLLUP_ID_FIELD -> rollupID = xcp.text()
                    AFTER_KEY_FIELD -> afterKey = xcp.text()
                    LAST_UPDATED_FIELD -> lastUpdatedTime = xcp.instant()
                    WINDOW_START_TIME_FIELD -> windowStartTime = xcp.instant()
                    WINDOW_END_TIME_FIELD -> windowEndTime = xcp.instant()
                    FINISHED_FIELD -> finished = xcp.booleanValue()
                    FAILED_FIELD -> failed = xcp.booleanValue()
                    FAILURE_REASON -> failureReason = xcp.text()
                }
            }

            return RollupMetadata(
                id,
                seqNo,
                primaryTerm,
                rollupID = requireNotNull(rollupID) { "RollupID must not be null" },
                afterKey = afterKey,
                lastUpdatedTime = requireNotNull(lastUpdatedTime) { "Last updated time must not be null" },
                windowStartTime = requireNotNull(windowStartTime) { "Window start time must not be null" },
                windowEndTime = requireNotNull(windowEndTime) { "Window end time must not be null" },
                finished = finished,
                failed = failed,
                failureReason = failureReason
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput) = RollupMetadata(sin)
    }
}