package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.delete

import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions.addValidationError
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import java.io.IOException


/**
 * A request to delete a rollup job.
 */
class DeleteRollupRequest : DeleteRequest {
    val rollupID: String

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin) {
        rollupID = sin.readString()
    }

    /**
     * Constructs a new delete rollup request for the specified rollupID.
     *
     * @param rollupID The rollupID to delete.
     */
    constructor(rollupID: String) {
        this.rollupID = rollupID
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