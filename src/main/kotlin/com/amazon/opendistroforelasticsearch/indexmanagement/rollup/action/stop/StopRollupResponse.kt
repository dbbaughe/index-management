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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.stop

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.rest.RestStatus
import java.io.IOException

class StopRollupResponse : ActionResponse, ToXContentObject {
    var started: Boolean
    var status: RestStatus

    constructor(
        started: Boolean,
        status: RestStatus
    ) : super() {
        this.started = started
        this.status = status
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        started = sin.readBoolean(),
        status = sin.readEnum(RestStatus::class.java)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(started)
        out.writeEnum(status)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("stopped", started)
            .endObject()
    }
}