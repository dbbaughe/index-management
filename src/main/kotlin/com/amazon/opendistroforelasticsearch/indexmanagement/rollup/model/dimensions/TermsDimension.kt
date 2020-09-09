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

data class TermsDimension(val fields: List<String>): ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput): this(
        fields = sin.readStringArray().toList()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
            .field(TERMS_FIELDS_FIELD, fields)
            .endObject()
        return builder
    }

    override fun writeTo(out: StreamOutput) {
        out.writeStringArray(fields.toTypedArray())
    }

    companion object {
        const val TERMS_FIELD = "terms"
        const val TERMS_FIELDS_FIELD = "fields"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): TermsDimension {
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
            return TermsDimension(
                fields.toList()
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput) = TermsDimension(sin)
    }
}