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

data class DateHistogram(
    val field: String,
    val fixedInterval: String?,
    val calendarInterval: String?,
    val timezone: String // TODO: FIX
): Dimension(Type.DATE_HISTOGRAM) {

    init {
        require(fixedInterval != null || calendarInterval != null) { "Must specify a fixed or calendar interval" }
        require(fixedInterval == null || calendarInterval == null) { "Can only specify a fixed or calendar interval" }
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput): this(
        field = sin.readString(),
        fixedInterval = sin.readOptionalString(),
        calendarInterval = sin.readOptionalString(),
        timezone = sin.readString()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .startObject(type.type)
        if (fixedInterval != null) builder.field(FIXED_INTERVAL_FIELD, fixedInterval)
        if (calendarInterval != null) builder.field(CALENDAR_INTERVAL_FIELD, calendarInterval)
        return builder.field(DATE_HISTOGRAM_FIELD_FIELD, field)
            .field(DATE_HISTOGRAM_TIMEZONE_FIELD, timezone)
            .endObject()
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(field)
        out.writeOptionalString(fixedInterval)
        out.writeOptionalString(calendarInterval)
        out.writeString(timezone)
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
        fun parse(xcp: XContentParser): DateHistogram {
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
            return DateHistogram(
                field = requireNotNull(field) { "Field must not be null in date histogram" },
                fixedInterval = fixedInterval,
                calendarInterval = calendarInterval,
                timezone = timezone
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput) = DateHistogram(sin)
    }
}