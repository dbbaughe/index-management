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

data class Histogram(
    val histogramSourceField: String,
    val histogramTargetField: String,
    val interval: Double
): Dimension(Type.HISTOGRAM, histogramSourceField, histogramTargetField) {

    @Throws(IOException::class)
    constructor(sin: StreamInput): this(
        histogramSourceField = sin.readString(),
        histogramTargetField = sin.readString(),
        interval = sin.readDouble()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startObject(type.type)
            .field(HISTOGRAM_SOURCE_FIELD_FIELD, histogramSourceField)
            .field(HISTOGRAM_TARGET_FIELD_FIELD, histogramTargetField)
            .field(HISTOGRAM_INTERVAL_FIELD, interval)
            .endObject()
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(histogramSourceField)
        out.writeString(histogramTargetField)
        out.writeDouble(interval)
    }

    companion object {
        const val HISTOGRAM_FIELD = "histogram"
        const val HISTOGRAM_SOURCE_FIELD_FIELD = "source_field"
        const val HISTOGRAM_TARGET_FIELD_FIELD = "target_field"
        const val HISTOGRAM_INTERVAL_FIELD = "interval"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Histogram {
            var histogramSourceField: String? = null
            var histogramTargetField: String? = null
            var interval: Double? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    HISTOGRAM_SOURCE_FIELD_FIELD -> histogramSourceField = xcp.text()
                    HISTOGRAM_TARGET_FIELD_FIELD -> histogramTargetField = xcp.text()
                    HISTOGRAM_INTERVAL_FIELD -> interval = xcp.doubleValue()
                }
            }
            if (histogramTargetField == null) histogramTargetField = histogramSourceField
            return Histogram(
                requireNotNull(histogramSourceField) { "Source field must be set in histogram" },
                requireNotNull(histogramTargetField) { "Target field must be set in histogram" },
                requireNotNull(interval) { "Interval must be set in histogram" }
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput) = Histogram(sin)
    }
}