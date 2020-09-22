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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Average
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Max
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Metric
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Min
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Sum
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.ValueCount
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

// TODO: for config mappings make sure we can safely update the metric mappings to add any future fields if we need to
// TODO: i might want to do multiple of the same metric on the same field - i.e. percentiles of 0, 25, 50, 75, 100 but also percentiles of 0, 33, 66, 100?
data class RollupMetrics(val sourceField: String, val targetField: String, val metrics: List<Metric>): ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput): this(
        sourceField = sin.readString(),
        targetField = sin.readString(),
        metrics = sin.let {
            val metricsList = mutableListOf<Metric>()
            val size = it.readVInt()
            for (i in 0..size) {
                val type = it.readEnum(Metric.Type::class.java)
                metricsList.add(
                    when (requireNotNull(type) { "Metric type cannot be null" }) {
                        Metric.Type.AVERAGE -> Average(it)
                        Metric.Type.MAX -> Max(it)
                        Metric.Type.MIN -> Min(it)
                        Metric.Type.SUM -> Sum(it)
                        Metric.Type.VALUE_COUNT -> ValueCount(it)
                    }
                )
            }
            metricsList.toList()
        }
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(METRICS_SOURCE_FIELD_FIELD, sourceField)
            .field(METRICS_METRICS_FIELD, metrics.toTypedArray())
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(sourceField)
        out.writeString(targetField)
        out.writeVInt(metrics.size)
        for (metric in metrics) {
            out.writeEnum(metric.type)
            when (metric) {
                is Average -> metric.writeTo(out)
                is Max -> metric.writeTo(out)
                is Min -> metric.writeTo(out)
                is Sum -> metric.writeTo(out)
                is ValueCount -> metric.writeTo(out)
            }
        }
        out.writeCollection(metrics)
    }

    companion object {
        const val METRICS_FIELD = "metrics"
        const val METRICS_SOURCE_FIELD_FIELD = "source_field"
        const val METRICS_TARGET_FIELD_FIELD = "target_field"
        const val METRICS_METRICS_FIELD = "metrics"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): RollupMetrics {
            var sourceField: String? = null
            var targetField: String? = null
            val metrics = mutableListOf<Metric>()

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    METRICS_SOURCE_FIELD_FIELD -> sourceField = xcp.text()
                    METRICS_TARGET_FIELD_FIELD -> targetField = xcp.text()
                    METRICS_METRICS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp::getTokenLocation)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            metrics.add(Metric.parse(xcp))
                        }
                    }
                }
            }

            if (targetField == null) targetField = sourceField
            return RollupMetrics(
                sourceField = requireNotNull(sourceField) { "Source field must not be null in rollup metrics" },
                targetField = requireNotNull(targetField) { "Target field must not be null in rollup metrics" },
                metrics = metrics.toList()
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput) = RollupMetrics(sin)
    }
}