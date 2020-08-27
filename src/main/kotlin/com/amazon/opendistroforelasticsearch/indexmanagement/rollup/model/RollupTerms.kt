package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model

import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class RollupTerms(val fields: List<String>): ToXContentObject {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
            .field(TERMS_FIELDS_FIELD, fields)
            .endObject()
        return builder
    }

    companion object {
        const val TERMS_FIELD = "terms"
        const val TERMS_FIELDS_FIELD = "fields"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): RollupTerms {
            val fields = mutableListOf<String>()

            ensureExpectedToken(
                Token.START_OBJECT,
                xcp.currentToken(),
                xcp::getTokenLocation
            )
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    TERMS_FIELDS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp::getTokenLocation)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            fields.add(xcp.text())
                        }
                    }
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Rollup terms aggregation.")
                }
            }
            return RollupTerms(fields.toList())
        }
    }
}