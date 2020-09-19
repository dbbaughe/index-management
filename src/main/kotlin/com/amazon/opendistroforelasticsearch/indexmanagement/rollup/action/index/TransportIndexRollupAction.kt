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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

class TransportIndexRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val indexManagementIndices: IndexManagementIndices,
    val clusterService: ClusterService
) : HandledTransportAction<IndexRollupRequest, IndexRollupResponse>(
    IndexRollupAction.NAME, transportService, actionFilters, ::IndexRollupRequest
) {
    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: IndexRollupRequest, listener: ActionListener<IndexRollupResponse>) {
        IndexRollupHandler(client, listener, request).start()
    }

    inner class IndexRollupHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexRollupResponse>,
        private val request: IndexRollupRequest
    ) {

        fun start() {
            indexManagementIndices.checkAndUpdateISMConfigIndex(ActionListener.wrap(::onCreateMappingsResponse, actionListener::onFailure))
        }

        private fun onCreateMappingsResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Successfully created or updated ${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX} with newest mappings.")
                putRollup()
            } else {
                val message = "Unable to create or update ${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX} with newest mapping."
                log.error(message)
                actionListener.onFailure(ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR))
            }
        }

        private fun putRollup() {
            val rollup = request.rollup.copy(schemaVersion = IndexUtils.indexManagementConfigSchemaVersion)

            val indexRequest = IndexRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
                .setRefreshPolicy(request.refreshPolicy)
                .source(rollup.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .id(request.rollupID)
                .timeout(IndexRequest.DEFAULT_TIMEOUT)
            if (request.ifSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO || request.ifPrimaryTerm() == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
                indexRequest.opType(DocWriteRequest.OpType.CREATE)
            } else {
                indexRequest.setIfSeqNo(request.ifSeqNo())
                    .setIfPrimaryTerm(request.ifPrimaryTerm())
            }
            client.index(indexRequest, object : ActionListener<IndexResponse> {
                override fun onResponse(response: IndexResponse) {
                    if (response.shardInfo.failed > 0) {
                        val failureReasons = response.shardInfo.failures.joinToString(", ") { it.reason() }
                        actionListener.onFailure(ElasticsearchStatusException(failureReasons, response.status()))
                    } else {
                        val status = if (indexRequest.opType() == DocWriteRequest.OpType.CREATE) RestStatus.CREATED else RestStatus.OK
                        actionListener.onResponse(
                            IndexRollupResponse(response.id, response.version,
                                response.seqNo, response.primaryTerm, status, rollup)
                        )
                    }
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(e)
                }
            })
        }
    }
}