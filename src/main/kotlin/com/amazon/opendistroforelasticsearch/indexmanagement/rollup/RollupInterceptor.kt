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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup

import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.Transport
import org.elasticsearch.transport.TransportChannel
import org.elasticsearch.transport.TransportInterceptor
import org.elasticsearch.transport.TransportRequest
import org.elasticsearch.transport.TransportRequestHandler
import org.elasticsearch.transport.TransportRequestOptions
import org.elasticsearch.transport.TransportResponse
import org.elasticsearch.transport.TransportResponseHandler

class RollupInterceptor : TransportInterceptor {

    private val log = LogManager.getLogger(javaClass)

    override fun interceptSender(sender: TransportInterceptor.AsyncSender): TransportInterceptor.AsyncSender {
//        log.info("Inside intercept sender")
        return object: TransportInterceptor.AsyncSender {
            override fun <T : TransportResponse> sendRequest(
                connection: Transport.Connection,
                action: String,
                request: TransportRequest,
                options: TransportRequestOptions,
                handler: TransportResponseHandler<T>
            ) {
//                log.info("intercepted sender sendrequest $action")
                sender.sendRequest(connection, action, request, options, handler)
            }
        }
    }

    override fun <T : TransportRequest> interceptHandler(
        action: String,
        executor: String,
        forceExecution: Boolean,
        actualHandler: TransportRequestHandler<T>
    ): TransportRequestHandler<T> {
//        log.info("Inside intercepthandler")
        return object: TransportRequestHandler<T> {
            override fun messageReceived(request: T, channel: TransportChannel, task: Task) {
//                log.info("intercept handler message received ${task.action}")
                actualHandler.messageReceived(request, channel, task);
            }
        }
    }
}