/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.refreshanalyzer

import org.elasticsearch.action.support.broadcast.BroadcastRequest
import org.elasticsearch.common.io.stream.StreamInput
import java.io.IOException

class RefreshSearchAnalyzerRequest : BroadcastRequest<RefreshSearchAnalyzerRequest> {
    constructor(vararg indices: String) : super(*indices)

    @Throws(IOException::class)
    constructor(inp: StreamInput) : super(inp)
}
