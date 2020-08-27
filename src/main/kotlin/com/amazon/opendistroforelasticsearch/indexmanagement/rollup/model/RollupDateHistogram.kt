package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model

import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class RollupDateHistogram(
    val field: String,
    val fixedInterval: String?,
    val calendarInterval: String?,
    val timezone: String
): ToXContentObject {

    init {
        require(fixedInterval != null || calendarInterval != null) { "Must specify a fixed or calendar interval" }
        require(fixedInterval == null || calendarInterval == null) { "Can only specify a fixed or calendar interval" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
            .field(FIXED_INTERVAL_FIELD, fixedInterval)
            .field(CALENDAR_INTERVAL_FIELD, calendarInterval)
            .field(DATE_HISTOGRAM_FIELD_FIELD, field)
            .field(DATE_HISTOGRAM_TIMEZONE_FIELD, timezone)
            .endObject()
        return builder
    }

    companion object {
        const val DATE_HISTOGRAM_FIELD = "date_histogram" // TODO: there is prob an official one we can use?
        const val FIXED_INTERVAL_FIELD = "fixed_interval"
        const val CALENDAR_INTERVAL_FIELD = "calendar_interval"
        const val DATE_HISTOGRAM_FIELD_FIELD = "field"
        const val DATE_HISTOGRAM_TIMEZONE_FIELD = "timezone"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): RollupDateHistogram {
            var field: String? = null
            var fixedInterval: String? = null
            var calendarInterval: String? = null
            var timezone = "UTC"

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    FIXED_INTERVAL_FIELD -> fixedInterval = xcp.text()
                    CALENDAR_INTERVAL_FIELD -> calendarInterval = xcp.text()
                    DATE_HISTOGRAM_TIMEZONE_FIELD -> timezone = xcp.text()
                    DATE_HISTOGRAM_FIELD_FIELD -> field = xcp.text()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Rollup terms aggregation.")
                }
            }
            return RollupDateHistogram(
                field = requireNotNull(field) { "Field must not be null in date histogram" },
                fixedInterval = fixedInterval,
                calendarInterval = calendarInterval,
                timezone = timezone
            )
        }
    }
}