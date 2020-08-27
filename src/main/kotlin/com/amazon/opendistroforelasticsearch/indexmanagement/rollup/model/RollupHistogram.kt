package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.State
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class RollupHistogram(val field: String, val interval: Long): ToXContentObject {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
            .endObject()
        return builder
    }

    companion object {
        const val HISTOGRAM_FIELD = "histogram"
        const val HISTOGRAM_FIELD_FIELD = "field"
        const val HISTOGRAM_INTERVAL_FIELD = "interval"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): RollupHistogram {
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

            return RollupHistogram(
                requireNotNull(field) { "Field must be set in histogram" },
                requireNotNull(interval) { "Interval must be set in histogram" }
            )
        }
    }
}