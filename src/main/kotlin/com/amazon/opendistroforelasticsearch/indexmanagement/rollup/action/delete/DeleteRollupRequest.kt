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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.delete

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
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
        super.index(INDEX_MANAGEMENT_INDEX)
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