package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model

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

data class RollupMetrics(val field: String, val metrics: List<String>): ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput): this(
        field = sin.readString(),
        metrics = sin.readStringArray().toList()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(METRICS_FIELD_FIELD, field)
            .field(METRICS_METRICS_FIELD, metrics.toTypedArray())
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(field)
        out.writeStringArray(metrics.toTypedArray())
    }

    companion object {
        const val METRICS_FIELD = "metrics"
        const val METRICS_FIELD_FIELD = "field"
        const val METRICS_METRICS_FIELD = "metrics"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): RollupMetrics {
            var field: String? = null
            val metrics = mutableListOf<String>()

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    METRICS_FIELD_FIELD -> field = xcp.text()
                    METRICS_METRICS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp::getTokenLocation)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            metrics.add(xcp.text())
                        }
                    }
                }
            }

            return RollupMetrics(requireNotNull(field) { "Field must not be null in rollup metrics" }, metrics)
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput) = RollupMetrics(sin)
    }
}