package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.search.fetch.subphase.FetchSourceContext
import java.io.IOException

class GetRollupRequest : ActionRequest {
    var id: String
    var version: Long
    val method: RestRequest.Method
    val srcContext: FetchSourceContext?

    constructor(
        id: String,
        version: Long,
        method: RestRequest.Method,
        srcContext: FetchSourceContext?
    ) : super() {
        this.id = id
        this.version = version
        this.method = method
        this.srcContext = srcContext
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        version = sin.readLong(),
        method = sin.readEnum(RestRequest.Method::class.java),
        srcContext = if (sin.readBoolean()) FetchSourceContext(sin) else null
    )

    // TODO
    override fun validate(): ActionRequestValidationException? {
        val validationException: ActionRequestValidationException? = null
//        if (rollupID.isBlank()) {
//            validationException = ValidateActions.addValidationError("rollupID is missing", validationException)
//        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeEnum(method)
        if (srcContext == null) {
            out.writeBoolean(false)
        } else {
            out.writeBoolean(true)
            srcContext.writeTo(out)
        }
    }
}