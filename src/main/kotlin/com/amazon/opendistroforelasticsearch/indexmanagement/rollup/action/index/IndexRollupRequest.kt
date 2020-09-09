package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions.addValidationError
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.rest.RestRequest
import java.io.IOException

class IndexRollupRequest : IndexRequest {
    val rollupID: String
    val method: RestRequest.Method
    val rollup: Rollup

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin) {
        rollupID = sin.readString()
        super.setIfSeqNo(sin.readLong())
        super.setIfPrimaryTerm(sin.readLong())
        super.setRefreshPolicy(WriteRequest.RefreshPolicy.readFrom(sin))
        method = sin.readEnum(RestRequest.Method::class.java)
        rollup = Rollup.readFrom(sin)
    }

    constructor(
        rollupID: String,
        seqNo: Long,
        primaryTerm: Long,
        refreshPolicy: WriteRequest.RefreshPolicy,
        method: RestRequest.Method,
        rollup: Rollup
    ) {
        this.rollupID = rollupID
        super.setIfSeqNo(seqNo)
        super.setIfPrimaryTerm(primaryTerm)
        super.setRefreshPolicy(refreshPolicy)
        this.method = method
        this.rollup = rollup
    }

    // TODO
    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (rollupID.isBlank()) {
            validationException = addValidationError("rollupID is missing", validationException)
        }
        return validationException
    }

    fun rollupID(): String = rollupID

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(rollupID)
        out.writeLong(ifSeqNo())
        out.writeLong(ifPrimaryTerm())
        refreshPolicy.writeTo(out)
        out.writeEnum(method)
        rollup.writeTo(out)
    }
}