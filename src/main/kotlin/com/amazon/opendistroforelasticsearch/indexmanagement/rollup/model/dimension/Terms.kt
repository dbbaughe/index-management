/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class Terms(val termsSourceField: String, val termsTargetField: String): Dimension(Type.TERMS, termsSourceField, termsTargetField) {

    @Throws(IOException::class)
    constructor(sin: StreamInput): this(
        termsSourceField = sin.readString(),
        termsTargetField = sin.readString()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startObject(type.type)
            .field(TERMS_SOURCE_FIELD_FIELD, termsSourceField)
            .field(TERMS_TARGET_FIELD_FIELD, termsTargetField)
            .endObject()
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(termsSourceField)
    }

    companion object {
        const val TERMS_FIELD = "terms"
        const val TERMS_SOURCE_FIELD_FIELD = "source_field"
        const val TERMS_TARGET_FIELD_FIELD = "target_field"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Terms {
            var termsSourceField: String? = null
            var termsTargetField: String? = null

            ensureExpectedToken(
                Token.START_OBJECT,
                xcp.currentToken(),
                xcp::getTokenLocation
            )
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    TERMS_SOURCE_FIELD_FIELD -> termsSourceField = xcp.text()
                    TERMS_TARGET_FIELD_FIELD -> termsTargetField = xcp.text()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Rollup terms aggregation.")
                }
            }
            if (termsTargetField == null) termsTargetField = termsSourceField // TODO: testing
            return Terms(
                requireNotNull(termsSourceField) { "Source field cannot be null in terms aggregation" },
                requireNotNull(termsTargetField) { "Target field cannot be null in terms aggregation" }
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput) = Terms(sin)
    }
}