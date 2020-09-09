package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimensions

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class HistogramDimension(val field: String, val interval: Long): ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput): this(
        field = sin.readString(),
        interval = sin.readLong()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(HISTOGRAM_FIELD_FIELD, field)
            .field(HISTOGRAM_INTERVAL_FIELD, interval)
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(field)
        out.writeLong(interval)
    }

    companion object {
        const val HISTOGRAM_FIELD = "histogram"
        const val HISTOGRAM_FIELD_FIELD = "field"
        const val HISTOGRAM_INTERVAL_FIELD = "interval"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): HistogramDimension {
            var field: String? = null
            var interval: Long? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    HISTOGRAM_FIELD_FIELD -> field = xcp.text()
                    HISTOGRAM_INTERVAL_FIELD -> interval = xcp.longValue()
                }
            }

            return HistogramDimension(
                requireNotNull(field) { "Field must be set in histogram" },
                requireNotNull(interval) { "Interval must be set in histogram" }
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput) = HistogramDimension(sin)
    }
}