package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model

import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class RollupMetrics(val field: String, val metrics: List<String>) {

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
    }
}