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


/**
 * A request to delete a rollup job.
 */
class IndexRollupRequest : IndexRequest {
    val rollupID: String
    val seqNo: Long
    val primaryTerm: Long
    val rPolicy: WriteRequest.RefreshPolicy
    val method: RestRequest.Method
//    val rollup: Rollup
    //    val destinationId: String
    //    val seqNo: Long
    //    val primaryTerm: Long
    //    val refreshPolicy: WriteRequest.RefreshPolicy
    //    val method: RestRequest.Method
    //    val destination: Destination

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin) {
        rollupID = sin.readString()
        seqNo = sin.readLong()
        primaryTerm = sin.readLong()
        rPolicy = WriteRequest.RefreshPolicy.readFrom(sin)
        method = sin.readEnum(RestRequest.Method::class.java)
//        rollup = Rollup.readFrom(sin)
    }

    /**
     * Constructs a new delete rollup request for the specified rollupID.
     *
     * @param rollupID The rollupID to delete.
     */
    constructor(
        rollupID: String,
        seqNo: Long,
        primaryTerm: Long,
        refreshPolicy: WriteRequest.RefreshPolicy,
        method: RestRequest.Method,
        rollup: Rollup
    ) {
        this.rollupID = rollupID
        this.seqNo = seqNo
        this.primaryTerm = primaryTerm
        this.rPolicy = refreshPolicy
        this.method = method
//        this.rollup = rollup
    }

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
    }
}