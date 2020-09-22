package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric

import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

abstract class Metric(val type: Type) : ToXContentObject, Writeable {

    enum class Type(val type: String) {
        AVERAGE("avg"),
        SUM("sum"),
        MAX("max"),
        MIN("min"),
        VALUE_COUNT("value_count");

        override fun toString(): String {
            return type
        }
    }

// TODO: Would be nice but can't override both here and subclass, should we move this to rollup metrics?
//    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
//        builder.startObject().startObject(type.type)
//        when(this) {
//            is Average -> this.toXContent(builder, params)
//            is Max -> this.toXContent(builder, params)
//            is Min -> this.toXContent(builder, params)
//            is Sum -> this.toXContent(builder, params)
//            is ValueCount -> this.toXContent(builder, params)
//            else -> throw IllegalArgumentException("Invalid metric type: [$this] found in rollup metrics")
//        }
//        return builder.endObject().endObject()
//    }

    companion object {
        @Suppress("ComplexMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Metric {
            var metric: Metric? = null
            // TODO: fix all ensureExpected and Token.START
            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                metric = when (fieldName) {
                    Type.AVERAGE.type -> Average.parse(xcp)
                    Type.MAX.type -> Max.parse(xcp)
                    Type.MIN.type -> Min.parse(xcp)
                    Type.SUM.type -> Sum.parse(xcp)
                    Type.VALUE_COUNT.type -> ValueCount.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid metric type: [$fieldName] found in rollup metrics")
                }
            }

            return requireNotNull(metric) { "Metric is null" }
        }
    }
}