package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric
//TODO license checks
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken

class Average() : Metric(Type.AVERAGE) {
    constructor(sin: StreamInput): this()

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject().startObject(Type.AVERAGE.type).endObject().endObject()
    }

    override fun writeTo(out: StreamOutput) {} // nothing to write

    companion object {
        const val AVERAGE_FIELD = "avg"

        // TODO: this parses just the internal object but then toXContent spits out the whole qualified object
        //  ie {} vs { "avg": {} } should toXContent also just print {} and let Metric deal with the "avg" part?
        //  not sure we can put it in Metric because Metric does not know about all the non-metric fields? or we can just use a when block and call toXContent?
        fun parse(xcp: XContentParser) : Average {
            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            return Average()
        }
    }
}